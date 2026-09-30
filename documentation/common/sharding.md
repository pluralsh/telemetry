# Sharding

All databases route each record to a stable storage shard by hashing the
product's canonical key and looking the hash up in the routing epoch that was
effective at the record's timestamp. Storage shards decouple data placement from
the current writer replica count.

```text
client write
    │ (routing key, record time)
    ▼
routing epoch at record time ─► 128-bit hash range ─► stable storage shard
    │ assignment generation
    ├── local owner ─► SlateDB write
    └── remote owner ─► internal gRPC ─► SlateDB write

reader ─► opens every storage shard and merges results
```

SlateDB is single-writer/multi-reader, so exactly one writer owns a storage
shard at a time. Stale ownership responses refresh the assignment and are
retried up to `write.remote_retries`; fan-out is bounded by
`write.remote_concurrency`.

Routing uses the first 128 bits of BLAKE3 over each product's canonical key.
Meter and Line hash the namespace plus canonical labels; Track hashes the
namespace plus trace ID. Hash ranges are aligned to 4,096 routing slots (the
high 12 bits of the hash), which caps a deployment at 4,096 storage shards. The
slot is only a routing granularity: it is not stored in keys.

The default is one storage shard; Kubernetes sharded mode uses the writer
replica count, which defaults to one. There is no in-cluster shard replication:
failover transfers the Lease and reopens the same SlateDB data in shared object
storage.

## Routing epochs

A `ShardMap` holds an ordered list of routing epochs. Each epoch has an
`effective_from_ns` timestamp and a hash-range map over its shard count. A
record is routed by the last epoch whose `effective_from_ns` is at or before
the record's timestamp:

- Meter routes each sample by its timestamp, so a series that straddles a
  cutover is split into one write per epoch.
- Line routes each entry by its timestamp.
- Track routes a trace by its earliest span start time.

The record's identity still selects the shard within an epoch; time only
selects which epoch applies. Epochs only grow, so every shard referenced by an
older epoch still exists and remains readable.

Readers open every storage shard `[0, shard_count)` and merge results. Meter
deduplicates series by fingerprint across shards, Line merges streams with
equal labels, and Track merges the partial traces returned by each shard a
trace ID may have been routed to.

## Backends

- `standalone`: one process owns every shard.
- `static`: fixed owners declare contiguous half-open ranges. Ranges must
  exactly cover `[0, shards)` with no gaps or overlaps.
- `kubernetes`: StatefulSet pods provide stable identities, a ShardMap CR holds
  routing epochs and assignment generations, and Leases guard coordinator and
  shard ownership.

In Kubernetes mode, a leader balances contiguous ranges across current
StatefulSet members. The elected Rust coordinator writes the ShardMap with
`resourceVersion` compare-and-swap. Every writer and reader consumes the
ShardMap watch stream and applies only increasing generations. Writers acquire
each assigned shard Lease and release/handoff ownership during changes. Lease
watches accelerate handoff and coordinator failover.

## Online scale-up

Increasing writer replicas appends a routing epoch over the larger shard count.
The new epoch takes effect at the next alignment boundary that is at least the
lead time in the future (one hour alignment and two minutes lead by default), so
every writer observes the epoch before any record routes by it and each product
time partition is written under a single epoch.

New shards start as new, empty SlateDB databases. No data is copied, cloned, or
drained: records timestamped before the cutover keep routing by the previous
epoch, so their shard never changes. Late-arriving records are routed by their
own timestamp and land on the shard that already holds that time range.

Repeated scale requests before a cutover replace the pending epoch rather than
stacking epochs. The coordinator then rebalances ownership assignments across
the StatefulSet, which moves shard Leases but not data.

The product CR's writer replica count is user intent. The operator creates
additional StatefulSet pods before Rust extends the ShardMap, and reports the
number of routing epochs through product status. Scale-down is intentionally
blocked and reported through product status: shards referenced by any epoch
must stay writable until their data has expired.

## Rules and tuning

- Kubernetes uses the product CR writer replica count as the desired
  storage-shard count and gates the effective StatefulSet count against
  ShardMap state. `sharding.shards` only applies to the standalone and static
  backends.
- Retention eventually removes data routed by old epochs. Epochs themselves are
  retained so every shard stays readable.
- `io_concurrency_limit` sets a fixed per-pod storage I/O budget. Its default
  is 128, independent of how many storage shards a standalone process or
  reader opens. Kubernetes writers normally own one storage shard per pod.
- Set `renew_interval_seconds` comfortably below
  `lease_duration_seconds` (defaults 5 and 15).
- Every forwarding participant must share `auth.internal` when it is enabled.

Database records choose storage partition boundaries differently: Meter uses
time buckets, Line uses time segments, and Track uses time segments plus a
reserved trace-locator segment.

For provisional per-writer ingestion envelopes, operational headroom, and
product-shaped benchmark requirements, see
[Capacity planning and scaling](scaling.md).
