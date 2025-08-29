# kcl-rust

This is a proof-of-concept Kinesis Client Library (KCL) for Rust which is used to be able to read records from an [AWS Kinesis stream](https://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html), on a distributed network, with at-least-once delivery of records, using best efforts to avoid duplication. This wraps the [AWS SDK](https://awslabs.github.io/aws-sdk-rust/) with a distributed consensus algorithm for managing shards that is compatible with [the Java Kinesis Client Library v2](https://github.com/awslabs/amazon-kinesis-client) (using dynamodb as a data store) and aggregated records written with the [Kinesis Producer Library](https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html).

Complete Rust crate documention is available, and examples of use are available in the `tests` folder including an integration test alongside the Java KCLv2.

## Distributed Algorithm

This KCL uses distributed consensus, whereby the consumers are collaborative and no node is (intentionally) malicious. The network is resilient against any subset of consumers failing. Calls to `ListShards` are only made by a random subset of nodes to limit API calls, in contrast with the Java KCL which has an elected leader that is a central point of failure.

The goal is for a fleet (of unknown size) of consumers to consume from distinct kinesis shards, having an even distribution of work, and with minimal disruption to the service. Every record in the stream is processed using an "at least once delivery" guarantee from the point of it entering the kinesis stream until it has been successfully checkpointed by the user.

To achive this, every consumer is assigned a `workerid` which uniquely identifies them on the network, we use UUIDs. A dynamodb table is created as the shared source of truth between all the consumers, called the "lease table". A "lease manager" subprocess is spawned for each consumer, periodically polling the lease table to obtain a full list of all the leases in existence, and polling the kinesis stream to obtain a full list of all the shards.

When the lease manager decides that the consumer should start consuming from one of the shards, it spawns a "lease worker" subprocess to obtain a lease and stream records until either the process ends or the end of the shard is reached (which happens when the stream is scaled in or out). The lease worker is responsible for writing heartbeats to the lease table every 10 seconds, signalling to the other consumers that the lease is active.

The user code receives kinesis records, along with their sequence numbers. It is the responsibility of the user code to write checkpoints into the lease table regularly (using conveniently provided APIs) that will allow consumers to pick up from an appropriate place if the lease is transferred.

The following pseudcode describes in more detail how each of these steps is accomplished, and even further detail is available by studying the rust source code.

### Lease

An entry in the lease table has the following fields

```
leaseKey                    TEXT
leaseOwner                  NULLABLE TEXT
leaseCounter                INTEGER
checkpoint                  TEXT
checkpointSubSequenceNumber INTEGER
```

where the `checkpoint` is one of the sentinel values `{ AT_TIMESTAMP, TRIM_HORIZON, LATEST, SHARD_END }` or a (non-padded) integer matching the regular expression `0|([1-9]\d{0,128})`.

Additional fields are also populated in order to remain backwards compatible with the Java KCLv2 but effectively duplicate information that is provided by the kinesis [`ListShards`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html) endpoint. This implementation chooses not to make use of these values, preferring a single source of truth at all times, and avoiding any problems related to staleness of data.

The `leaseKey` is the `ShardId` of the kinesis shard that it relates to. This is the primary key of the table.

The `leaseOwner` is populated if a consumer has acquired the lease and will continue to increment `leaseCounter` every 10 seconds.

The `checkpoint` (and related `checkpointSubSequenceNumber`) are written when user code checkpoints that all records have been read up to a specific point.

DynamoDB allows us to make atomic conditional updates, so conflicts (e.g. if two consumers attempt to claim the same lease) are resolved via a single source of truth. In the following, we'll document the exact conditions that are used for every update to a lease, and what happens if the condition is invalid (i.e. when a lease is contended).

### Lease Manager

The lease manager runs for every consumer and is responsible for:

- deleting leases from the database
- creating leases in the database
- spawning lease workers locally

The list of leases is obtained by a paginated [`Scan`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html) of the lease table.

The list of shards is obtained from the [`ListShards`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html) endpoint.

#### Deleting

Deleting a lease is simple: if there is no corresponding shard from kinesis, then the lease is deleted with an unconditional [`DeleteItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html) (using the `leaseKey` primary key), even if it hasn't been ended yet. This requires that the list of shards is queried after the list of leases, to avoid race conditions.

#### Creating

Creating a lease is a little more involved. Shards can be split (scaling out) or merged (scaling in), and we may see multiple steps in the ancestry when there are scaling operations in quick succession. Therefore, leases may only be created for orphans (i.e. there are no ancestor or descendent leases) or if all the parent leases have been ended. When creating a lease, the first consumer to create the lease wins and failures due to primary key constraint violations can be safely ignored.

The orphan strategy of the consumer (assumed to be the same across the fleet) dictates what the first lease looks like:

- Oldest (also called "trim horizon") will create a lease for orphaned shards and will start reading from the oldest available records. Consumers must be prepared to process records faster than they are being trimmed from the stream's retention period. The sentinel value `TRIM_HORIZON` is used in the `checkpoint` field in these cases.
- At a specific timestamp, will create leases for orphaned shards and every lease (even those created in the future) will only start reading records from the given timestamp; this means that leases may be ended immediately if their corresponding shard contains no information after the given date. Be careful about using timestamps in the future, as the AWS APIs do not document the behaviour. The sentinel value `AT_TIMESTAMP` is used in the `checkpoint` field in these cases, and the UNIX epoch (in millis) is entered into the `checkpointSubSequenceNumber` field.
- Latest will skip over orphans and create leases for the most recent descendents.

All the strategies should be equivalent to each other once the stream has caught up with the most recent records and should not be susceptible to race conditions, although if multiple strategies are deployed at the same time it is possible that consumers may consume more data than was requested.

A new lease is created using an unconditional (except for the primary key constraint) [`PutItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html).

The `tests` directory contains the file `create_new_leases.json` which lists various scenarios, and the expected action that every consumer should take. This behaviour is automatically asserted as part of the build.

#### Spawning

Calculation of when to spawn a lease worker is the heart of the distributed algorithm. If a lease manager sees that there are any leases that have no `leaseOwner`, or the `leaseCounter` has not been updated within the last 20 seconds, it will greedily spawn lease workers to try and acquire those leases. The consumer has two parameters that affect how greedy it can be:

- `leases_to_acquire` dictates how many leases we can try to obtain in every (20 second) cycle
- `max_leases` caps how many leases to hold at any moment

Note that it is possible that a consumer's own leases may not have been heartbeated, in which case the lease manager may choose to spawn a new lease worker to reacquire those leases. But in such cases, the lease manager checks to see if a lease worker is already in progress and will filter out locally processed leases. This ensures that progress will be made even if there is only one consumer in the fleet, and the subprocess (for whatever reason) exited before it was able to release the lease.

It is possible to deploy a fleet of consumers that collectively have a `max_leases` lower than the number of active shards, in which case the fleet will not be able to keep up and there could be data loss. Conversely, setting the `max_leases` too high, in combination with a large `leases_to_acquire`, can lead to the first consumers taking on too many leases and becoming unstable. It is safest to use a low `leases_to_acquire`, and then tune the `max_leases` based on your own consumer scaling policy. A sensible policy might be to autoscale based on the `bytes` or `worker_leases` metrics. Note that the Java KCLv2 does not have an equivalent of this parameter so it may require additional tuning in a multilang setting.

In addition, to rebalance lease assignments evenly, a consumer may attempt to take a lease from another consumer that has two or more leases than themselves. This allows the fleet to trend towards the average in a stable manner.

The `tests` directory contains the file `calculate_acquire.json` which lists various scenarios, and the expected action that every consumer should take. This behaviour is automatically asserted as part of the build.

### Lease Worker

The lease worker is spawned for a consumer when the lease manager decides that a lease can be acquired for a shard.

It is responsible for:

- acquiring the lease
- heartbeating the lease
- reading kinesis records and forwarding them to the user
- enabling the user code to checkpoint the lease
- releasing a lease at the user's request

#### Acquiring

A lease is acquired by setting `leaseOwner` to the consumer's `workerid`, and setting the `leaseCounter` to 1, using a conditional [`UpdateItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html):

```
SET leaseOwner = :new_owner, leaseCounter = :one
```

This is done with a conditional requirement that the `leaseOwner` either doesn't exist or is what we expected it to be (we use `NULL` in place of an actual workerid incase another implementation on the network chooses to null out entries instead of delete them)

```
attribute_not_exists(leaseOwner) OR leaseOwner = :last_owner
```

We also update a Java KCL debugging field

```
ADD ownerSwitchesSinceCheckpoint :one
```

(where `:one` is defined as a numerical value of "1", it is a shame that ddb does not support literal numeric values)

and use it as an opportunity to remove any optional Java KCL fields that are used in implementation-specific graceful handovers

```
REMOVE checkpointOwner, pendingCheckpoint, pendingCheckpointSubSequenceNumber, pendingCheckpointState, childShardIds, throughputKBps
```

If this operation succeeds, then we consider ourselves to be the new owner of the lease and continue to perform our other responsibilities.

As an optimisation, if there was a previous owner (i.e. we took the lease from somebody), we wait 10 seconds before reloading the lease entry so that we have the most recent `checkpoint`.

#### Heartbeats

Every 10 seconds we increment the `leaseCounter` with a conditional `UpdateItem`:

```
ADD leaseCounter :one
```

with the condition

```
leaseOwner = :owner AND checkpoint <> :ended
```

where we are the `:owner` and `:ended` is the sentinel value `SHARD_END` (recall that `<>` means "is not equal" in the ddb query language).

If the heartbeat fails, our lease worker informs the user that the lease has been lost and halts processing; the user will receive no more records from this lease unless it is chosen to be reacquired.

#### Reading

In polling mode, we must convert our table name into an ARN using the [`DescribeStreamSummary`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamSummary.html) and use the lease's `checkpoint` (and for the case of `AT_TIMESTAMP`, the `checkpointSubSequenceNumber`) in a call to [`GetShardIterator`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html) which gives us a token that can be used to traverse the shard with calls to [`GetRecords`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html).

The very first record we receive is filtered to exclude entries that have the exact same `checkpoint` as the one we started with, if their `sub_sequence_number` is lower than the `checkpointSubSequenceNumber`. For regular records, this has no impact except the extra overhead of receiving one extra record that is filtered, but it allows aggregated records to be checkpointed mid-record, as discussed later. Aggregated records are records that have been written using the Kinesis Producer Library.

The calls to `GetRecords` are repeated in a loop until the lease is lost or ended. If no records are received, we wait up to 2 seconds before trying again, otherwise we impose a self-throttled 200 milliseconds between each call so as to not break the AWS Kinesis limit of 5 requests per second per shard. This also implies that there can only be one polling consumer per stream; AWS's recommendation is to use Enhanced Fan Out (EFO) for multiple consumers.

#### Checkpointing

Users can choose when to checkpoint. Doing this too much may be very costly (to both performance and pricing) but failure to do so may result in too much data being duplicated when a lease is released or taken, and can result in failure to make progress in the stream if the end of a shard is reached.

When the user chooses to make a regular checkpoint, we perform a conditional `UpdateItem`

```
SET checkpoint = :sequence_number,
    ownerSwitchesSinceCheckpoint = :zero
```

with the (somewhat long-winded) conditions

```
checkpoint <> :SHARD_END
AND ((checkpoint IN (:LATEST, :TRIM_HORIZON, :AT_TIMESTAMP))
     OR (size(checkpoint) < size(:sequence_number) OR size(checkpoint) = size(:sequence_number) AND checkpoint < :sequence_number)
     OR (checkpoint = :sequence_number AND checkpointSubSequenceNumber > :sub_sequence_number))
```

which, in simpler terms means:

- the lease was not ended, and
  - the old checkpoint was a starting sentinel
  - or, the new checkpoint is more recent than the old one

but it is complicated by the fact that the `checkpoint` is a string, not a numeric, so we must simulate numeric comparison by doing lexicographical comparison and assuming the form of the big decimals. And also by the fact that the checkpoint may be for an aggregated record. We use strings here instead of numerics to remain compatible with the Java KCLv2, but we would prefer to use numeric types if we were in full control of the schema.

Note that we do not require that the checkpoint is made by a consumer who holds the lease. This is to allow consumers to checkpoint the shard even after it has been taken by another consumer, who will (in the worst case) read more records than they should. This ensure that there is no data loss while also making best efforts to avoid work duplication. The AWS Java KCL goes to further efforts for a graceful handover at this point, but we feel that the logic presented here is sufficient in most cases and any further efforts are not worth the additional complexity.

The user must explicit end the lease when they are informed that the shard has reached its end. It is the user's responsibility to ensure that they do this after processing all the records first. For this special form of checkpoint, we perform an unconditional `UpdateItem`

```
SET checkpoint = :SHARD_END, ownerSwitchesSinceCheckpoint = :zero
REMOVE leaseOwner
```

where we also remove ourselves as the owner (i.e. release the lease), and the lease worker halts processing.

As an optimisation, if a shard is ended, the lease worker prompts the lease manager to pre-emptively run instead of waiting up to 20 seconds. It will also try to obtain the children of the lease that recently ended, which is favourable to the user code. In the case of scaling in, two consumers may be contending for the child, and in the case of scaling out, the consumer would try to obtain both children lease. However, this is not guaranteed and it may be the case that by chance another independent consumer takes the lease for the children.

#### Releasing

At any moment, the user may request that the consumer (and, by implication, the) lease worker halts their work and releases all their leases. When this happens, we perform a conditional `UpdateItem`

```
SET leaseCounter = :zero
REMOVE leaseOwner
```

conditioned on us being the owner

```
leaseOwner = :owner
```

Failed releases are safely ignored as it means that somebody took the lease before we gave it up.


## Aggregated Records

Records that are written to a Kinesis stream by the Kinesis Producer Library are deaggregated automatically. The KPL uses [an adhoc format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md) which effectively batches multiple records that are expected to go to the same shard, into a single record, which is encoded using a protobuf format. Each record in an aggregated record shares the same sequence number but is given an incrementing sub-sequence number. End users of the KCL should not concern themselves about this implementation detail.

Producers to a kinesis stream need only need to use the KPL if they have many small records, and have provisioned their shards such that they would be breaking the AWS request limits (1,000 records per second per shard, up to 1MB) for the [`PutRecords`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html) batch call. In practice, this means that the KPL is only worth considering for records much smaller than 1kb and a simpler solution is to scale out the stream. However, some users may choose to underscale the stream and use the KPL to stay well within the record count limits.

This library internally implements the aggregation functionality, but does not provide a fully working KPL implementation; as that would require careful batching of records in such a way that they are guaranteed to go to the same shard in a reliable manner.

## Metrics

The cloudwatch metrics produced by this client are an improvement over those that are published by the Java AWS SDK, and are suitable for fleet monitoring.

Every consumer publishes the following fleet-wide metrics every 20 seconds:

- `total_leases`
- `total_shards`
- `unclaimed_leases`

along with these related to their own set of leases:

- `worker_leases`

Every lease within that consumer publishes the following metrics every 10 seconds:

- `millis_behind_latest`, provided by kinesis
- `bytes`, counting the payload bytes (not transport overhead)
- `records`, deaggregated (may be higher than kinesis records)

This set of metrics is enough to design alarms that can monitor the following production situations:

- the consumers are underprovisioned, or are encountering churning:
  - minimum of `millis_behind_latest` is higher than zero
  - minimum of `unclaimed_leases` is higher than zero
  - maximum of `bytes` (or `records`) is much higher than the average, indicating a "hot shard"
- there is potential data loss:
  - average `total_leases` is less than average `total_shards`
  - sum of `worker_leases` (or `bytes` or `records`) goes to zero (or is not present)

Note that it is normal for some of these alarms to trigger under normal circumstances for short periods of time (e.g. during multiple resharding events of the stream, or during catch up for the full history, or if the stream is empty), but if they continue to trigger then it may indicate an issue.

# Known Issues

## Enhanced Fan Out (EFO)

At the time of writing this code we were blocked from using EFO because `SubscribeToShard` was not implemented by [the Rust AWS SDK](https://docs.rs/aws-sdk-kinesis/latest/aws_sdk_kinesis/operation/index.html), tracked at https://github.com/awslabs/aws-sdk-rust/issues/213

That has now become available and we aim to add EFO support shortly.

The following endpoints are necessary to be able to implement EFO

- [`RegisterStreamConsumer`]( https://docs.aws.amazon.com/kinesis/latest/APIReference/API_RegisterStreamConsumer.html)
- [`SubscribeToShard`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_SubscribeToShard.html)

## Hot Shard Mitigation

We could populate and make use of a `throughputKBps` field in the lease table, similar to what what added to KCLv3, allowing workers to detect if they are currently subscribed to a "hot shard" (i.e. one with much more data than other shards, usually due to partition key collisions) and let them intentionally release their other shards so that they trend towards an average throughput of the fleet. While this is a nice idea, and compatible with other KCLv2 consumers, it is riddled with many operational uncertainties; can we trust the `throughputKBps` as a true measure of the shard throughput independent of other factors, and how do we know when the shard is no longer hot? Throughput often decreases as a worker has more leases, independently of the shards. Users experiencing these kinds of problems are perhaps safer to have a scaling policy based on the metrics provided above, although this may be added at some point.

## Kinesis Producer Library

The usecase for the KPL is quite niche, as it is only useful in situations where records are less than 1Kb, scaling out the shards is not an option, and consumers are hitting rate limits due to the number of records rather than their payload size. Therefore we have not provided a KPL implementation here, as the standard [`PutRecords`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html) is sufficient for most cases, although the rust implementation could be more efficient if there were a suitable workaround for https://github.com/awslabs/aws-sdk-rust/issues/1218

## License

Copyright 2024-2025 Sam Halliday, Rob Pickerill (The Walt Disney Company)

Licensed under the Tomorrow Open Source Technology License, Version 1.0 (the
"License"); you may not use this file except in compliance with the License. You
may obtain a copy of the License at

https://disneystreaming.github.io/TOST-1.0.txt

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
