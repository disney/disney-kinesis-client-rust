// Copyright 2024-2025 Sam Halliday, Rob Pickerill (The Walt Disney Company)
//
// Licensed under the Tomorrow Open Source Technology License, Version 1.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  https://disneystreaming.github.io/TOST-1.0.txt
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

// this file defines the helper functions for lease management (creation /
// acquisition / deletion), as opposed to the consumption, checkpointing and
// heartbeating of owned shards. We call this "oversee" and it's roughly
// equivalent to the LeaseAssignmentManager.java.

use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use crate::{types::*, Fail, StartStrategy};
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_kinesis::types::Shard;
use futures::future::join_all;
use itertools::Itertools;
use rand::seq::{IndexedRandom, IteratorRandom};

use crate::types::SequenceNumber::{AT_TIMESTAMP, LATEST, TRIM_HORIZON};

// Equivalent to the Java KCL
//
// LeaseManagementConfig.failoverTimeMillis = 10000
//
// and defines the heartbeat timeout. The default refresh rate of the overseer
// is twice this value.
pub(crate) const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) fn update_heartbeats(
    leases: &Vec<Lease>,
    leases_by_key: &mut HashMap<String, Lease>,
    // when we last (locally and monotonically) seen a heartbeat for a lease key
    lease_heartbeats_by_key: &mut HashMap<String, Instant>,
) {
    let active = leases.iter().map(|e| &e.lease_key).collect::<HashSet<_>>();

    // removes leases that don't exist anymore
    leases_by_key.retain(|k, _| active.contains(k));
    lease_heartbeats_by_key.retain(|k, _| active.contains(k));

    let nowish = Instant::now();
    leases.into_iter().for_each({
        |e: &Lease| {
            // progress all the leases that have had heartbeats since we last checked
            if leases_by_key
                .get(&e.lease_key)
                .map(|o| o.lease_counter < e.lease_counter || o.lease_owner != e.lease_owner)
                .unwrap_or(true)
            {
                lease_heartbeats_by_key.insert(e.lease_key.clone(), nowish);
            }
            // update the entry
            leases_by_key.insert(e.lease_key.clone(), e.clone());

            // there's a LOT of string cloning going on here. Every leaseKey exists multiple times. If we want
            // to have everything point to the same underlying data we'd need to use something like Arc<String>
            // or https://docs.rs/internment/latest/internment/
            // at the very least we should probably be using str instead of String
            // everywhere.
            // https://users.rust-lang.org/t/hashmaps-keyed-by-their-own-contents-is-there-a-better-way/120192
        }
    });
}

pub(crate) fn calculate_new_leases(
    start_strategy: &StartStrategy,
    shards: &HashMap<String, Shard>,
    leases_by_key: &HashMap<String, Lease>,
) -> Vec<Lease> {
    // shards that don't have a lease and their parents no longer exist
    let orphans = shards
        .values()
        .into_iter()
        .filter(|s| {
            !leases_by_key.contains_key(&s.shard_id)
                && shard_parent_ids(s).iter().all(|&p| !shards.contains_key(p))
        })
        .collect_vec();

    let seq = match start_strategy {
        StartStrategy::Timestamp(ms) => ExtendedSequenceNumber {
            sequence_number: AT_TIMESTAMP,
            sub_sequence_number: *ms,
        },
        _ => ExtendedSequenceNumber {
            sequence_number: TRIM_HORIZON,
            sub_sequence_number: 0,
        },
    };

    let to_acquire: Vec<(&Shard, ExtendedSequenceNumber)>;
    if !orphans.is_empty() {
        match start_strategy {
            StartStrategy::Latest => {
                // find the most recent decendents (i.e. those that are not
                // parents) and create leases for them. We would like to mark
                // all the ancestors as ended but that introduces a network race
                // condition if we succeed in ending the ancestors but failing
                // to create the LATEST ones. We could safely end them in the
                // next (non-orphan) pass but it's also ok to never create them
                // in the first place (subject to implementation compatibility).
                let seq = ExtendedSequenceNumber {
                    sequence_number: LATEST,
                    sub_sequence_number: 0,
                };
                let parents = shards
                    .values()
                    .into_iter()
                    .flat_map(|s| shard_parent_ids(s))
                    .collect::<HashSet<_>>();
                to_acquire = shards
                    .into_iter()
                    .filter(|&(sid, _)| !leases_by_key.contains_key(sid) && !parents.contains(sid))
                    .map(|(_, s)| (s, seq.clone()))
                    .collect_vec();
            }
            _ => {
                to_acquire = orphans.into_iter().map(|s| (s, seq.clone())).collect_vec();
            }
        }
    } else {
        // tries to replicate the behaviour of the Java KCL, as documented in
        // HierarchicalShardSyncer.determineNewLeasesToCreate but it's worded
        // quite ambiguously, so there may be subtle incompatibilities. To be
        // safe, we choose to do the most conservative thing and only create
        // leases for shards if their parent's leases have ended and they have
        // no descendant leases (for an arbitrary depth).

        // all the unleased shards that are ancestors of the leases.
        let ancestors = leases_by_key
            .keys()
            .into_iter()
            .flat_map(|sid| shards.get(sid))
            .flat_map(|shard| shard_ancestors(shards, shard))
            .map(|s| &s.shard_id)
            .collect::<HashSet<_>>();

        to_acquire = shards
            .into_iter()
            .filter(|&(sid, _)| !leases_by_key.contains_key(sid)) // unleased
            .filter(|&(sid, _)| !ancestors.contains(sid)) // not an ancestor
            .filter(|&(_, s)| {
                // parents are all ENDED or don't exist (but not fully orphaned)
                shard_parent_ids(s)
                    .into_iter()
                    .filter(|&pid| shards.contains_key(pid))
                    .all(|pid| {
                        if let Option::Some(lease) = leases_by_key.get(pid) {
                            lease.checkpoint.is_ended()
                        } else {
                            false
                        }
                    })
            })
            .map(|(_, s)| (s, seq.clone()))
            .collect_vec();
    };
    return to_acquire
        .into_iter()
        .map(|(shard, checkpoint)| Lease::from(&shard, checkpoint))
        .collect_vec();
}

pub(crate) async fn create_new_leases(
    ddb_client: &aws_sdk_dynamodb::client::Client,
    table_name: &String,
    leases: &Vec<Lease>,
) -> Result<(), Fail> {
    let work = leases
        .into_iter()
        .map(|e| {
            ddb_client
                .put_item()
                .table_name(table_name.clone())
                .set_item(Option::Some(From::from(e)))
                .send()
        })
        .collect_vec();

    for result in join_all(work).await {
        // ignore failures where the primary key is violated, it just means
        // another worker wrote it first.
        match result {
            Err(e) => match e.as_service_error() {
                Some(e) if e.is_conditional_check_failed_exception() => (),
                _ => return Err(From::from(e)),
            },
            Ok(_) => (),
        }
    }
    return Ok(());
}

// if a lease exists but there is no corresponding Shard, then it can be
// deleted because it means that the shard fell off its retention period.
//
// if a lease is not ended at this point, it should be logged at an
// escalated level, as it indicates lost data.
//
// note that we could be more aggressive and remove leases that have been ended
// and have children leases, which would marginally improve the performance of
// the list_leases call.
pub(crate) async fn delete_redundant(
    ddb_client: &aws_sdk_dynamodb::client::Client,
    table_name: &String,
    shards: &HashMap<String, Shard>,
    leases_by_key: &HashMap<String, Lease>,
) -> Result<(), Fail> {
    let work = leases_by_key
        .keys()
        .collect::<HashSet<_>>()
        .difference(&shards.keys().collect::<HashSet<_>>())
        .map(|sid| {
            ddb_client
                .delete_item()
                .table_name(table_name.clone())
                .key("leaseKey", AttributeValue::S(sid.to_string()))
                .send()
        })
        .collect_vec();

    for result in join_all(work).await {
        // deleting not existent item is ok
        // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html
        let _ = result?;
    }
    Ok(())
}

pub(crate) fn calculate_acquire(
    worker_identifier: &String,
    leases_to_acquire: usize,
    max_leases: usize,
    leases_by_key: &HashMap<String, Lease>,
    lease_heartbeats_by_key: &HashMap<String, Instant>,
    recently_ended_by_us: &Vec<String>,
    table_name: &String,
    stream_name: &String,
) -> Vec<Lease> {
    // To protect against KCLs that might create leases preemptively we
    // require that the parent lease exists and is ended OR that the parent
    // shard doesn't exist.

    let ended = leases_by_key
        .iter()
        .filter(|(_, v)| v.checkpoint.is_ended())
        .map(|(k, _)| k)
        .collect::<HashSet<_>>();

    let mut active_leases = leases_by_key
        .iter()
        .filter(|&(k, v)| {
            !v.checkpoint.is_ended()
                && leases_by_key.contains_key(k)
                && v.parent_shard_ids(leases_by_key)
                    .into_iter()
                    .filter(|&p| leases_by_key.contains_key(p))
                    .all(|p| ended.contains(p))
        })
        .collect::<HashMap<_, _>>();

    if recently_ended_by_us.len() > 0 {
        // if we recently ended leases, let's just focus on continuing their
        // legacy; other leases can be picked up on the next cycle.
        let parents = recently_ended_by_us.into_iter().collect::<HashSet<_>>();
        let preference = active_leases
            .iter()
            .filter(|(_, &v)| v.lease_owner.is_none() && v.is_child_of(&parents, leases_by_key))
            .map(|(&k, &v)| (k, v))
            .collect::<HashMap<_, _>>();

        if !preference.is_empty() {
            // disables regular lease taking in this pass, as we focus on
            // obtaining the children of closed leases.
            active_leases = preference;
        }
    }

    let nowish = Instant::now();
    let expired = lease_heartbeats_by_key
        .iter()
        .filter(|(_, ts)| nowish.duration_since(**ts) > HEARTBEAT_TIMEOUT)
        .map(|(key, _)| key)
        .collect::<HashSet<_>>();

    // note that we use the source of truth about how many leases we hold, not a
    // locally cached count of workers. This must only count active leases,
    // since it is used to find "rich" workers.
    let shares = active_leases
        .iter()
        .filter(|(_, v)| !expired.contains(&v.lease_key))
        .flat_map(|(_, v)| v.lease_owner.clone())
        .counts();
    let ours = shares.get(worker_identifier).cloned().unwrap_or(0);

    let headroom = max(0, max_leases - ours);

    let mut rng = rand::rng();

    // we could add weights here if we had a preference.
    // we allow picking our own expired leases.
    let to_acquire = active_leases
        .iter()
        .filter(|(&key, &lease)| lease.lease_owner.is_none() || expired.contains(key))
        .map(|(_, &v)| v)
        .choose_multiple(&mut rng, leases_to_acquire)
        .into_iter()
        .take(headroom)
        .collect_vec();

    if !to_acquire.is_empty() {
        return to_acquire.into_iter().cloned().collect_vec();
    }

    // If we are lease poor (less than average), and there's nothing available,
    // then start stealing from workers who are lease rich (more than average).
    // One way of doing this in a stable way is to steal a lease from a worker
    // that has at least 2 leases more than us. If we can't find such a worker,
    // then don't do anything.
    //
    // Be careful about changing this code as it is easy to come up with a lease
    // stealing algorithm that thrashes around the optimal distribution; we want
    // to minimise lost leases.
    if headroom > 0 && !active_leases.is_empty() {
        let correction = if ours == 0 { 1 } else { 0 };
        let avg = shares.values().sum::<usize>() / (shares.len() + correction);
        if ours <= avg {
            let trigger = max(avg + 1, ours + 2);
            let rich: HashSet<String> = shares
                .into_iter()
                .filter(|(_, c)| *c >= trigger)
                .map(|(k, _)| k)
                .collect();

            if !rich.is_empty() {
                tracing::info!(
                    worker = worker_identifier,
                    table = table_name,
                    stream = stream_name,
                    "trying to take from {rich:?}"
                );
            }

            // we could prefer to steal from the richest but that could lead to
            // contention and thrashing, so prefer to choose randomly. It could
            // perhaps be improved by choosing based on kbps to trend towards
            // the fleet average. Recall that it is only stable to steal 1
            // lease.
            if let Some(&target) = active_leases
                .into_values()
                .filter(|lease| match &lease.lease_owner {
                    Some(owner) => rich.contains(owner),
                    _ => false,
                })
                .collect_vec()
                .choose(&mut rand::rng())
            {
                return vec![target.clone()];
            }
        }
    }

    return Vec::new();
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs::File,
        time::{Duration, Instant},
    };

    use crate::types::dupes;
    use crate::types::*;
    use crate::{oversee, StartStrategy};
    // use aws_sdk_kinesis::types::Shard;
    use serde::{de::DeserializeOwned, Deserialize, Serialize};

    // allows assertions to be defined in data, whereby the "shards" and
    // "before" are the input, and the "after" is the expected state of the
    // leases after the change. Note that the exact output isn't always
    // asserted, it might be some property such as the number of leases.
    #[derive(Serialize, Deserialize)]
    struct CreateLeases {
        comment: String,
        strategy: StartStrategy,
        shards: Vec<dupes::Shard>,
        leases: Vec<Lease>,
        to_create: Vec<Lease>,
    }

    // given a bunch of state, calculate the set of leases that can be acquired
    // and how many of them we would expect to see being acquired by this
    // worker. the actual leases to be acquired is always random so we can't do
    // exact equality checks.
    #[derive(Serialize, Deserialize)]
    struct Acquire {
        comment: String,
        leases_to_acquire: usize,
        max_leases: usize,
        worker: String,
        leases: Vec<Lease>,
        heartbeats: HashMap<String, u64>, // shard_id -> age in seconds
        available: Vec<String>,           // shard_ids
        expected: usize,                  // subset of "available" should be this size
        recently_ended_by_us: Vec<String>,
    }

    fn shards(shards: Vec<dupes::Shard>) -> HashMap<String, aws_sdk_kinesis::types::Shard> {
        shards
            .iter()
            .map(|s| (s.shard_id.clone(), From::from(s)))
            .collect()
    }

    fn leases(leases: Vec<Lease>) -> HashMap<String, Lease> {
        leases
            .iter()
            .map(|e| (e.lease_key.clone(), e.clone()))
            .collect()
    }

    fn read_scenarios<T>(filename: &str) -> Vec<T>
    where
        T: DeserializeOwned,
    {
        let scenarios: Vec<T> = serde_json::from_reader(File::open(filename).unwrap()).unwrap(); // test code
        assert_ne!(scenarios.len(), 0);
        scenarios
    }

    #[test]
    fn test_create_new_leases() {
        for test in read_scenarios::<CreateLeases>("tests/create_new_leases.json") {
            let results = oversee::calculate_new_leases(
                &test.strategy,
                &shards(test.shards),
                &leases(test.leases),
            );

            assert_eq!(results.len(), test.to_create.len(), "{}", test.comment);
            for lease in results {
                assert!(
                    test.to_create.contains(&lease),
                    "{} : {:?}",
                    test.comment,
                    lease
                );
            }
        }
    }

    #[test]
    fn test_calculate_acquire() {
        for test in read_scenarios::<Acquire>("tests/calculate_acquire.json") {
            let leases = leases(test.leases);

            let heartbeats: HashMap<_, _> = test
                .heartbeats
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        Instant::now().checked_sub(Duration::from_secs(v)).unwrap(), // test code
                    )
                })
                .collect();

            let results = oversee::calculate_acquire(
                &test.worker,
                test.leases_to_acquire,
                test.max_leases,
                &leases,
                &heartbeats,
                &test.recently_ended_by_us,
                &test.comment,
                &test.comment,
            );
            assert_eq!(results.len(), test.expected, "{}", test.comment);
            for lease in results {
                assert!(
                    test.available.contains(&lease.lease_key),
                    "{}",
                    test.comment
                );
            }
        }
    }
}
