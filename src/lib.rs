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

//! Kinesis Consumer Library compatible with the Java KCLv2.
//!
//! Uses a backing DynamoDB table to consume from a kinesis stream at scale
//! using a distributed consensus algorithm; with an at-least-once delivery
//! guarantee of records (and using best efforts to avoid duplicates), so long
//! as consumers are processing every shard and are not falling behind the
//! retention period of the stream (both situations are visible from cloudwatch
//! metrics).

//#![allow(unused_imports, unreachable_code)]
//#![allow(dead_code, unused_variables)]

pub mod aggregated;
mod errors;
mod lease;
mod oversee;
mod types;
mod waiter;

use async_rwlock::RwLock;
use async_trait::async_trait;
use aws_sdk_cloudwatch::types::MetricDatum;
use aws_sdk_dynamodb::types::{AttributeValue, BillingMode};
use aws_sdk_kinesis::types::Shard;
use aws_smithy_types::DateTime;
use oversee::HEARTBEAT_TIMEOUT;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::{sleep, timeout_at},
};
use tokio_util::sync::CancellationToken;
use types::Lease;
use uuid::Uuid;
use waiter::DynamoDbWaiter;

pub use errors::Fail;
pub use lease::Checkpointer;
pub use types::{ExtendedSequenceNumber, KinesisClientRecord};

/// To be implemented by consumers. This is called every time a lease is
/// acquired, and the user responds with a ShardRecordProcessor that will be be
/// handed records and lifecycle events.
///
/// Note that user code must be infallible, with the user addressing all
/// exceptional situations before returning. If the user wishes to stop
/// consuming records they must issue a shutdown() to the object that they
/// obtained when spawning the consumer client.
#[async_trait]
pub trait ShardRecordProcessorFactory: Send + Sync {
    /// The checkpointer should be retained for use by the processor, it is only
    /// valid for this lease and trying to use it with checkpoints obtained from
    /// other leases will corrupt the lease table.
    async fn initialize(&self, checkpointer: Checkpointer) -> Box<dyn ShardRecordProcessor>;
}

#[async_trait]
pub trait ShardRecordProcessor: Send + Sync {
    /// Called with a non-empty, in order, batch of records obtained from a
    /// single shard. Records may be a mixture of aggregated (i.e. KPL) and
    /// non-aggregated (i.e. regular) payloads.
    ///
    /// It is the user's responsibility to checkpoint the records by sending the
    /// KinesisClientRecord.checkpoint() to the Checkpointer for this shard;
    /// failure to do so will result in all the unchecked records being sent to
    /// the next worker when the lease is lost.
    ///
    /// Every checkpoint incurs a DDB write, so the user is recommended to tune
    /// this according to their application and cost requirements, e.g. every 10
    /// seconds. A checkpoint should not be persisted until all the records up
    /// to that point have been fully processed, or there may be data loss.
    ///
    /// The user should aim to return from this method in a timely fashion as it
    /// will backpressure kinesis, and delay the heartbeating / renewing of the
    /// lease which risks losing the lease to another worker.
    async fn process_records(&self, records: Vec<KinesisClientRecord>, millis_behind_latest: i64);

    /// Called when the lease has been lost, i.e. taken by another worker. At
    /// this point the user may checkpoint work that they have already received,
    /// and is encouraged to do so in a timely fashion to avoid the same records
    /// being processed by the new owner (there is a grace time before the other
    /// worker will start processing records from the checkpoint). No further
    /// action is required and no more records will be sent to this processor.
    async fn lease_lost(&self);

    /// Called when we have consumed all the records in this shard.
    ///
    /// The user must call Checkpointer.end() when they have finished processing
    /// all the outstanding work in this shard and may still send regular
    /// checkpoints during that time.
    ///
    /// This will be called periodically, until the lease is lost (which happens
    /// when the user checkpoints the end).
    ///
    /// Once the end has been checkpointed, the records in this shard will no
    /// longer be accessible to workers, and leases for the children shards will
    /// be created. The worker that checkpoints the end of a lease is given
    /// priority of the children leases, but is not guaranteed them.
    async fn shard_ended(&self);

    /// Called if the user has elsewhere requested that the consumer shuts down.
    /// Once this returns, the lease task will attempt to release the lease.
    async fn shutdown_requested(&self);
}

// should never be cloneable as the UUID must be kept unique
pub struct Client {
    callback: Arc<dyn ShardRecordProcessorFactory>,

    // clients
    cw_client: Arc<aws_sdk_cloudwatch::Client>,
    ddb_client: Arc<aws_sdk_dynamodb::client::Client>,
    kinesis_client: Arc<aws_sdk_kinesis::client::Client>,

    table_name: String,
    stream_name: String,

    // actually a UUID
    worker_identifier: String,

    management_config: ManagementConfig,

    // the idea is to take an Either when EFO support is added
    lease_config: LeaseConfig,

    // for tasks to communicate to the overseer
    sender: UnboundedSender<String>,
    receiver: UnboundedReceiver<String>,

    // tracks which leases we are currently processing
    active: Arc<RwLock<HashSet<String>>>,

    // for user-invoked graceful shutdown
    shutdown: CancellationToken,
}

/// Configuration associated to "polling mode" consumption, as opposed to
/// Enhanced Fan Out.
///
/// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html
#[derive(Clone, Serialize, Deserialize)]
pub enum LeaseConfig {
    PollingConfig {
        /// Minimum value of 1. Maximum value of 10000.
        limit: i32,
    },
}

impl Default for LeaseConfig {
    fn default() -> LeaseConfig {
        LeaseConfig::PollingConfig { limit: 10000 }
    }
}

/// Where to start consuming from shards that are orphaned, i.e. when we do not
/// see leases for the active shards or their parents.
///
/// In practice, this is only relevant for the first deployment of a service,
/// but is also relevant for services that are deployed after an extended period
/// of being offline.
///
/// It is important to understand that this is NOT the starting position for a
/// redeployment of a service, and will not force the consumer to ignore the
/// checkpoints in the lease table. If the goal is to start (re)processing from
/// a specific point in the stream that has been checkpointed from the last
/// deployment, the user can manually drop the lease table when there are no
/// consumers.
///
/// It is recommended that initial deployments are always overscaled (i.e. more
/// consumers than normally necessary) in order to account for the increased
/// traffic from playing back many records in a short time span.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum StartStrategy {
    /// Everything that is in the stream (also called "trim horizon"). This may
    /// incur a lot of catching up.
    #[serde(rename = "TRIM_HORIZON")]
    Oldest,
    /// From a specific unix epoch millis, applies to orphans and their
    /// descendants (i.e. used every time a lease is created, not just on
    /// initial service deployment). Timestamps in the future lead to undefined
    /// behaviour (GetShardIterator doesn't document it).
    #[serde(rename = "AT_TIMESTAMP")]
    Timestamp(i64),
    /// only the most recent shards and records (skipping over orphans to their
    /// most recent decendants).
    #[serde(rename = "LATEST")]
    Latest,
}

/// Configuration concerning the acquisition of leases.
#[derive(Clone, Serialize, Deserialize)]
pub struct ManagementConfig {
    /// the number of leases to grab every time we have capacity to grow. It is
    /// wise to keep this low and tune it based on the rollout speed of the
    /// fleet, since overzealous nodes will ultimately have to give up their
    /// excess (and that takes longer than everybody getting there gradually).
    pub leases_to_acquire: usize,
    /// never try to obtain more than this many leases, but be careful because
    /// if there are not enough workers then some shards can go unassigned.
    pub max_leases: usize,
    /// where to start streaming from orphaned streams (i.e. first use)
    pub start_strategy: StartStrategy,
}

/// returned to the user when consumer is spawned
pub struct Comms {
    shutdown: CancellationToken,
}

impl Comms {
    /// Request that the consumer fully shuts down and waits for all leases to
    /// be released.
    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        self.shutdown.cancelled().await;
    }
}

impl Client {
    /// Creates a new consumer, the next step is to call `run`
    ///
    /// The "table name" is equivalent to the "application name" in the Java SDK
    /// and will be the name of both the dynamodb lease table and the cloudwatch
    /// metrics.
    pub async fn new(
        callback: Arc<dyn ShardRecordProcessorFactory>,
        cw_client: Arc<aws_sdk_cloudwatch::client::Client>,
        ddb_client: Arc<aws_sdk_dynamodb::client::Client>,
        kinesis_client: Arc<aws_sdk_kinesis::client::Client>,
        table_name: String,
        stream_name: String,
        management_config: ManagementConfig,
        lease_config: LeaseConfig,
    ) -> Client {
        let (sender, receiver) = unbounded_channel::<String>();

        return Client {
            callback,
            cw_client,
            ddb_client,
            kinesis_client,
            table_name,
            stream_name,
            worker_identifier: Uuid::new_v4().to_string(),
            management_config,
            lease_config,
            sender,
            receiver,
            active: Arc::new(RwLock::new(HashSet::new())),
            shutdown: CancellationToken::new(),
        };
    }

    async fn validate_dynamodb(&self) -> Result<bool, Fail> {
        let output = self
            .ddb_client
            .describe_table()
            .table_name(&self.table_name)
            .send()
            .await;

        output.map_or_else(
            |e| match e.as_service_error() {
                Some(e) if e.is_resource_not_found_exception() => Ok(false),
                _ => Err(From::from(e)),
            },
            |_| Ok(true),
        )
    }

    async fn create_dynamodb_lease_table(&self) -> Result<(), Fail> {
        let attribute_definitions = aws_sdk_dynamodb::types::AttributeDefinition::builder()
            .attribute_name("leaseKey")
            .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
            .build()
            .unwrap(); // succeeds

        let key_schema = aws_sdk_dynamodb::types::KeySchemaElement::builder()
            .attribute_name("leaseKey")
            .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
            .build()
            .unwrap(); // succeeds

        tracing::info!(
            worker = self.worker_identifier,
            table = self.table_name,
            stream = self.stream_name,
            "creating a fresh lease table"
        );

        let _ = self
            .ddb_client
            .create_table()
            .table_name(&self.table_name)
            .attribute_definitions(attribute_definitions)
            .key_schema(key_schema)
            .on_demand_throughput(aws_sdk_dynamodb::types::OnDemandThroughput::builder().build())
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await?;

        let _ = self
            .ddb_client
            .wait_until_table_exists(&self.table_name, Duration::from_secs(30))
            .await?;
        Ok(())
    }

    /// Spawns the process to stream from kinesis for this worker.
    ///
    /// If the backing DDB table does not exist, one is created.
    ///
    /// A struct is returned that may be used to communicate with the worker
    /// (e.g. to request a shutdown).
    pub async fn run(self) -> Result<Comms, Fail> {
        if !self.validate_dynamodb().await? {
            self.create_dynamodb_lease_table().await?;
        }

        let comms = Comms {
            shutdown: self.shutdown.clone(),
        };
        let worker_identifier = self.worker_identifier.clone();
        let table_name = self.table_name.clone();
        let stream_name = self.stream_name.clone();

        tokio::spawn(async move {
            match self.worker().await {
                Err(e) => tracing::warn!(
                    worker = worker_identifier,
                    table = table_name,
                    stream = stream_name,
                    "consumer failed because of {e:?}"
                ),
                _ => (),
            }
        });

        return Ok(comms);
    }

    async fn worker(mut self) -> Result<(), Fail> {
        let mut leases_by_key = HashMap::new();
        let mut lease_heartbeats_by_key = HashMap::new();
        let mut ended = Vec::new();

        loop {
            if self.shutdown.is_cancelled() {
                tracing::info!(
                    worker = self.worker_identifier,
                    table = self.table_name,
                    stream = self.stream_name,
                    "stopped on request"
                );
                return Ok(());
            }

            let loop_start = Instant::now();
            let _ = self
                .oversee(&mut leases_by_key, &mut lease_heartbeats_by_key, &ended)
                .await?;
            ended.clear();

            // a self imposed throttle even in the case of being woken
            sleep(Duration::from_secs(1)).await;

            // if there is a desire to reduce ddb costs, we could gradually
            // backoff here to a much lower period than the deadline, e.g. every
            // minute or 5 minutes.
            let deadline = loop_start + 2 * HEARTBEAT_TIMEOUT;
            let _ = timeout_at(
                deadline.into(),
                self.receiver
                    .recv_many(&mut ended, self.management_config.max_leases),
            )
            .await;
        }
    }

    // an overseer process checks the full set of shards and leases and decides
    // if we need to acquire another. Leases are grabbed randomly up to a limit,
    // with a delay between each. Once all leases have been assigned to a
    // worker, we can steal a lease from any worker with more than some
    // percentage threshold of our workload (preferring the workers with the
    // most leases). We must be careful not to spawn new processes on split or
    // merged shards unless their parents are fully consumed.
    //
    // Each lease that we own should run in a loop polling from kinesis,
    // writing heartbeats and writing checkpoints, terminating if we have
    // lost the lease, which we will know about from a failed ddb
    // conditional write (which might have been come from the overseer
    // heartbeats).
    //
    // The overseer and lease processes don't need to share any local
    // information, although it makes sense for the lease process to be able
    // to trigger an earlier overseer check such as when we get to the end
    // of a split or merged shard.
    //
    // The caller can call shutdown to halt everything, gracefully, at any
    // time, e.g. by acting on a user signal.
    async fn oversee(
        &self,
        // the last time we seen each lease
        leases_by_key: &mut HashMap<String, Lease>,
        // when we last (locally and monotonically) seen a heartbeat for a lease key
        lease_heartbeats_by_key: &mut HashMap<String, Instant>,
        recently_ended: &Vec<String>,
    ) -> Result<(), Fail> {
        // updating the shards and leases can lead to a race condition where we
        // see new leases before we see the shards, leading us to conclude that
        // the leases are stale. The safest thing is to always poll the leases
        // first, so that the shards are always more recent.
        //
        // if there is a desire to reduce the number of requests to list_shards
        // we could either randomly call list_shards or (do what the java kcl
        // does) and elect a leader based on the workerid (see
        // `DeterministicShuffleShardSyncLeaderDecider`). If we don't have a
        // fresh view of the shards then we can only update the heartbeats and
        // try to acquire new leases, we cannot safely mutate the lease table.
        let leases = list_leases(&self.ddb_client, &self.table_name).await?;
        oversee::update_heartbeats(&leases, leases_by_key, lease_heartbeats_by_key);

        // In KCL the leader is elected through a shared concept by looking at
        // the randomly assigned IDs, but it's fragile because it takes some
        // time to notice when the leader is not making progress. Our approach
        // is for every node to decide randomly if they should be the leader in
        // this turn. If they are the leader then they can create and delete
        // leases, if they are not the leader then they should not mutate the
        // lease table in any way.
        //
        // Every turn we have a 2 in N chance of being the leader, where N is
        // the number of active workers. There's no point in rolling the dice if
        // we have less than 2 workers. Probably we could lift the floor here
        // but it complicates the testing strategy. When we recently ended any
        // shards then we'll definitely want to opt to become the leader because
        // we need to create the children.
        //
        // For maximum compatibility running alongside Java SDK clients we could
        // assume leadership when we would have been the elected leader, but
        // it's not strictly necessary.
        let nowish = Instant::now();
        let workers = lease_heartbeats_by_key
            .iter()
            .filter(|(_, ts)| nowish.duration_since(**ts) <= HEARTBEAT_TIMEOUT)
            .count();
        if !recently_ended.is_empty()
            || workers < 2
            || rand::rng().random_range(0..(workers / 2)) == 0
        {
            // we're a leader in this cycle...
            let shards: HashMap<String, Shard> =
                list_shards(&self.kinesis_client, &self.stream_name)
                    .await?
                    .into_iter()
                    .map(|s| (s.shard_id.clone(), s))
                    .collect();
            assert!(!shards.is_empty()); // should always be at least 1
            self.publish_metrics(&leases, shards.len()).await?;

            // see if we need to create any new leases, noting that they won't be
            // visible to us yet, to keep things simpler (consistency is king).
            let new_leases = oversee::calculate_new_leases(
                &self.management_config.start_strategy,
                &shards,
                leases_by_key,
            );
            oversee::create_new_leases(&self.ddb_client, &self.table_name, &new_leases).await?;

            // mutate the leases map so that we can immediately acquire them. even
            // if we failed to write them, somebody else will have created them.
            new_leases.into_iter().for_each(|e| {
                leases_by_key.insert(e.lease_key.clone(), e);
            });

            // delete leases for shards that no longer exist, if we have an up to date snapshot
            // of the shards.
            oversee::delete_redundant(&self.ddb_client, &self.table_name, &shards, leases_by_key)
                .await?;
        } else {
            self.publish_metrics(&leases, 0).await?;
        }

        // populate the leases that we should try to acquire
        let to_acquire: Vec<Lease> = oversee::calculate_acquire(
            &self.worker_identifier,
            self.management_config.leases_to_acquire,
            self.management_config.max_leases,
            leases_by_key,
            lease_heartbeats_by_key,
            recently_ended,
            &self.table_name,
            &self.stream_name,
        );

        // we can try to acquire expired leases that we last owned (otherwise a
        // sole worker would not be able to recover from an error), but we must
        // first check that we don't already have a lease in progress, otherwise
        // we'd end up stepping on our own toes.
        let mut active = self.active.write().await;
        let to_acquire: Vec<Lease> = to_acquire
            .into_iter()
            .filter(|e| !active.contains(&e.lease_key))
            .collect();

        if !to_acquire.is_empty() {
            tracing::debug!(
                worker = self.worker_identifier,
                table = self.table_name,
                stream = self.stream_name,
                "will try to acquire {} leases",
                to_acquire.len()
            );
        }

        // in Java KCLv3 there is a fleet decision to rebalance (i.e. give up or
        // trade/steal leases) based on the kbps trends. We could try to
        // implement that for improved rebalancing but it makes the big
        // assumption that everything is even between the workers, and the only
        // thing that differs is the amount of data in each shard. c.f.
        // VarianceBasedLeaseAssignmentDecider.java

        // spawns new tasks to handle each shard
        for lease in to_acquire {
            active.insert(lease.lease_key.clone());

            let cw_client = self.cw_client.clone();
            let ddb_client = self.ddb_client.clone();
            let kinesis_client = self.kinesis_client.clone();
            let table_name = self.table_name.clone();
            let stream_name = self.stream_name.clone();
            let worker_identifier = self.worker_identifier.clone();
            let lease_config = self.lease_config.clone();
            let sender = self.sender.clone();
            let shutdown = self.shutdown.child_token();
            let callback = self.callback.clone();
            let active = self.active.clone();

            tokio::spawn(async move {
                let lease_key = lease.lease_key.clone();
                let ended = lease::start(
                    callback,
                    cw_client,
                    ddb_client,
                    kinesis_client,
                    sender,
                    shutdown,
                    table_name.clone(),
                    stream_name.clone(),
                    worker_identifier.clone(),
                    lease_config,
                    lease,
                )
                .await;

                let mut active = active.write().await;
                active.remove(&lease_key);

                match ended {
                    Err(e) => tracing::warn!(
                        worker = worker_identifier,
                        table = table_name,
                        stream = stream_name,
                        "lease task failed because of {e:?}"
                    ),
                    _ => (),
                }
            });
        }

        return Ok(());
    }

    async fn publish_metrics(&self, leases: &Vec<Lease>, total_shards: usize) -> Result<(), Fail> {
        let total_leases = leases.len();
        let unclaimed_leases = leases.iter().filter(|e| e.lease_owner.is_none()).count();
        let us = Some(self.worker_identifier.clone());
        let worker_leases = leases.iter().filter(|e| e.lease_owner == us).count();

        tracing::debug!(
            worker = self.worker_identifier,
            table = self.table_name,
            stream = self.stream_name,
            "total_shards={total_shards}, \
             total_leases={total_leases}, \
             unclaimed_leases={unclaimed_leases}, \
             worker_leases={worker_leases}"
        );

        let nowish = DateTime::from(SystemTime::now());

        let total_leases = MetricDatum::builder()
            .metric_name("total_leases")
            .value(total_leases as f64)
            .timestamp(nowish.clone())
            .build();
        let unclaimed_leases = MetricDatum::builder()
            .metric_name("unclaimed_leases")
            .value(unclaimed_leases as f64)
            .timestamp(nowish.clone())
            .build();
        let worker_leases = MetricDatum::builder()
            .metric_name("worker_leases")
            .value(worker_leases as f64)
            .timestamp(nowish.clone())
            .build();

        let mut req = self
            .cw_client
            .put_metric_data()
            .namespace(&self.table_name)
            .metric_data(total_leases)
            .metric_data(unclaimed_leases)
            .metric_data(worker_leases);

        if total_shards > 0 {
            let total_shards = MetricDatum::builder()
                .metric_name("total_shards")
                .value(total_shards as f64)
                .timestamp(nowish.clone())
                .build();
            req = req.metric_data(total_shards);
        }

        req.send().await?;

        Ok(())
    }
}

async fn list_shards(
    kinesis_client: &aws_sdk_kinesis::client::Client,
    stream_name: &String,
) -> Result<Vec<Shard>, Fail> {
    let mut shards: Vec<Shard> = Vec::new();
    let mut token = None;

    loop {
        let result = kinesis_client
            .list_shards()
            .stream_name(stream_name)
            .set_next_token(token)
            .send()
            .await?;

        token = result.next_token;
        if let Some(batch) = result.shards {
            shards.extend(batch);
        }
        if token.is_none() {
            break;
        }
    }

    return Ok(shards);
}

// consider making it (and its deps) pub, as it might be useful
async fn list_leases(
    ddb_client: &aws_sdk_dynamodb::client::Client,
    table_name: &String,
) -> Result<Vec<Lease>, Fail> {
    let results: Result<Vec<_>, _> = ddb_client
        .scan()
        .table_name(table_name)
        .into_paginator()
        .items()
        .send()
        .collect()
        .await;

    results?
        .iter()
        .map(|item| Lease::try_from(item).map_err(mk_lease_parse_error))
        .collect()
}

// consider making it (and its deps) pub, as it might be useful
async fn get_lease(
    ddb_client: &aws_sdk_dynamodb::client::Client,
    table_name: &String,
    lease_key: &String,
) -> Result<Option<Lease>, Fail> {
    let result = ddb_client
        .get_item()
        .table_name(table_name)
        .key("leaseKey", AttributeValue::S(lease_key.clone()))
        .send()
        .await?
        .item;

    match result {
        None => Ok(None),
        Some(item) => Lease::try_from(&item)
            .map(Some)
            .map_err(mk_lease_parse_error),
    }
}

fn mk_lease_parse_error(attribute: (String, Option<&AttributeValue>)) -> Fail {
    Fail::Kcl(format!(
        "Failed to parse lease at key: {}, value: {:?}",
        attribute.0, attribute.1
    ))
}
