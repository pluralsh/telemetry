# Sharding

All databases map each record to a deterministic virtual shard. Virtual shards
decouple stable data placement from the current writer replica count.

```text
client write
    │ hash / storage routing key
    ▼
virtual shard (0..N)
    │ assignment generation
    ├── local owner ─► SlateDB write
    └── remote owner ─► internal gRPC ─► SlateDB write

reader ─► opens/fans out across every virtual shard
```

SlateDB is single-writer/multi-reader, so exactly one writer owns a virtual
shard at a time. Stale ownership responses refresh the assignment and are
retried up to `write.remote_retries`; fan-out is bounded by
`write.remote_concurrency`.

Routing uses BLAKE3 modulo the virtual-shard count. Meter and Line hash the
namespace plus canonical labels; Track hashes the namespace plus trace ID.
There is no in-cluster shard replication: failover transfers the Lease and
reopens the same SlateDB data in shared object storage.

## Backends

- `standalone`: one process owns every shard.
- `static`: fixed owners declare contiguous half-open ranges. Ranges must
  exactly cover `[0, virtual_shards)` with no gaps or overlaps.
- `kubernetes`: StatefulSet pods provide stable identities, a ConfigMap holds
  assignment generations, and Leases guard coordinator and shard ownership.

In Kubernetes mode, a leader balances contiguous ranges across current
StatefulSet members. Writers watch assignments, acquire each assigned shard
Lease, and release/handoff ownership during changes. Lease watches accelerate
handoff and coordinator failover.

## Rules and tuning

- `virtual_shards` must be positive and identical for every process sharing a
  dataset. Changing it in place changes routing and is not a scaling operation.
- More virtual shards improve balancing granularity but open more databases and
  increase fan-out.
- `io_concurrency_multiplier` sets global shard I/O permits per open shard.
- Set `renew_interval_seconds` comfortably below
  `lease_duration_seconds` (defaults 5 and 15).
- Every forwarding participant must share `auth.internal` when it is enabled.

Database records choose routing boundaries differently: Meter uses time
buckets, Line uses time segments, and Track uses time segments plus a reserved
trace-locator segment.
