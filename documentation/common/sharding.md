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

query ─► any reader ─┬─ its own shard ─► in process
                     └─ other shards ─► their reader groups over internal gRPC
                     merge ─► Prometheus / Loki / Tempo response
```

SlateDB is single-writer/multi-reader, so exactly one writer owns a storage
shard at a time. Stale ownership responses refresh the assignment and are
retried up to `write.remote_retries`; fan-out is bounded by
`write.remote_concurrency`.

Routing uses the first 128 bits of BLAKE3 over each product's canonical key.
Metrics and Logs hash the namespace plus canonical labels; Traces hashes the
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

- Metrics routes each sample by its timestamp, so a series that straddles a
  cutover is split into one write per epoch.
- Logs routes each entry by its timestamp.
- Traces routes a trace by its earliest span start time.

The record's identity still selects the shard within an epoch; time only
selects which epoch applies. Epochs only grow, so every shard referenced by an
older epoch still exists and remains readable.

The reader that receives a query coordinates it over every storage shard
`[0, shard_count)` and merges the results. Metrics deduplicates series by
fingerprint across shards, Logs merges streams with equal labels, and Traces
merges the partial traces returned by each shard a trace ID may have been
routed to. How a reader reaches each shard depends on whether it belongs to a
reader group; see [Reader groups](#reader-groups).

All shards opened by one process share a single SlateDB block cache and meta
cache, so `storage.block_cache` and `storage.meta_cache` capacities are
per-process budgets. Each opened shard still holds its own SlateDB reader
state and WAL replay buffer.

## Reader groups

Without reader groups, every reader opens every storage shard. Its fixed
memory, manifest polling, and cache working set therefore grow with the
shard count, and adding readers only replicates that whole-dataset footprint.

With `sharding.read_groups`, readers are partitioned by storage shard: the
replicas of group `k` open only shard `k`. Any reader can still answer any
query. It reads its own shard in process and fetches every other shard from
that shard's group through the product's internal `ShardReader` gRPC service:

- Metrics resolves series on each shard, then streams their samples in the
  chunk encoding sample storage uses. PromQL is evaluated by the coordinating
  reader.
- Logs runs each LogQL expression per shard and streams the shard's partial
  output: matched lines, or the series and sample totals of a metric query.
  The coordinator merges them exactly as it merges local shards.
- Traces streams search results, partial traces, catalog values, and
  per-shard TraceQL hits.

The local shard is never read over gRPC, and a reader serving a shard-scoped
call never forwards it again, so a query costs at most one hop per remote
shard. Responses are converted to Prometheus, Loki, or Tempo JSON only by the
coordinating reader.

Each group's endpoint is a headless Service that resolves to every replica of
the group. A reader spreads calls across those replicas, fails over to
another replica when one is unavailable, and starts the same call on a second
replica when the first has not answered within `hedge_after_ms` (250 ms by
default; zero disables hedging). Follow-up calls of one query, such as
Metrics sample reads after series resolution, stay on the replica that
answered first. Calls are bounded by `request_timeout_seconds` and carry the
`auth.internal` credential.

A reader that belongs to a group reports ready once its own shard is open; it
does not wait for other groups. A query whose remote shard has no reachable
replica fails rather than returning partial results: Logs and Traces respond
`503`, and Metrics returns a Prometheus `internal` error. A group for a
shard that the ShardMap does not include yet opens nothing until a scale-up
adds that shard.

Per-shard limits apply per shard rather than per query when shards are read
remotely:

- Logs `max_pages` is checked by each shard against the pages that shard
  reads.
- Traces `max_candidate_traces` bounds each shard's TraceQL candidates.
  Each shard evaluates TraceQL itself and the coordinator merges hits by start
  time and trace ID before applying the limit. A trace can be split across
  shards only when its spans run across a routing-epoch cutover that moves
  its trace ID, because each part routes by its own earliest span. A hit
  whose spans lie within an hour of such a cutover is fetched and
  re-evaluated as a whole trace; other hits are used as each shard returned
  them, so queries over time ranges away from cutovers pay nothing extra.
  A structural query can still miss a split trace when no single shard's
  part matches it.

In Kubernetes the operator creates one reader StatefulSet per storage shard
and keeps the group count equal to the shard count; see
[Capacity planning and scaling](scaling.md#reader-groups-and-cost) for sizing
and cost.

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

Database records choose storage partition boundaries differently: Metrics uses
time buckets, Logs uses time segments, and Traces uses time segments plus a
reserved trace-locator segment.

For provisional per-writer ingestion envelopes, operational headroom, and
product-shaped benchmark requirements, see
[Capacity planning and scaling](scaling.md).
