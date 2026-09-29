# Sharding

All databases map each record into an explicit, versioned 128-bit hash range
owned by a stable storage shard. Storage shards decouple data placement from
the current writer replica count.

```text
client write
    │ hash / storage routing key
    ▼
128-bit hash range ─► stable storage shard
    │ assignment generation
    ├── local owner ─► SlateDB write
    └── remote owner ─► internal gRPC ─► SlateDB write

reader ─► opens/fans out across every storage shard
```

SlateDB is single-writer/multi-reader, so exactly one writer owns a storage
shard at a time. Stale ownership responses refresh the assignment and are
retried up to `write.remote_retries`; fan-out is bounded by
`write.remote_concurrency`.

Routing uses the first 128 bits of BLAKE3 over each product's canonical key.
The high 12 bits select one of 4,096 fixed routing slots; explicit inclusive
hash ranges group contiguous slots into storage shards. Meter and Line hash the
namespace plus canonical labels; Track hashes the namespace plus trace ID. The
snapshot carries routing and writer-ownership generations together.
There is no in-cluster shard replication: failover transfers the Lease and
reopens the same SlateDB data in shared object storage.

## Backends

- `standalone`: one process owns every shard.
- `static`: fixed owners declare contiguous half-open ranges. Ranges must
  exactly cover `[0, virtual_shards)` with no gaps or overlaps.
- `kubernetes`: StatefulSet pods provide stable identities, a ShardMap CR holds
  assignment generations, and Leases guard coordinator and shard ownership.

In Kubernetes mode, a leader balances contiguous ranges across current
StatefulSet members. The elected Rust coordinator writes the ShardMap with
`resourceVersion` compare-and-swap. Every writer and reader consumes the
ShardMap watch stream and applies only increasing generations. Writers acquire
each assigned shard Lease and release/handoff ownership during changes. Lease
watches accelerate handoff and coordinator failover.

## Online scale-up

Increasing StatefulSet replicas starts one deterministic split at a time. The
`ShardMap.spec.migration` field durably records the desired final count, source
and target shards and owners, moved hash range, target routing map, and phase:

1. `preparing`: the source writer verifies that the target can be cloned
   without taking a data snapshot.
2. `prepared`: the coordinator marks the source assignment as draining.
3. `draining`: the source writer stops writes, flushes, closes, and releases
   its shard Lease.
4. `cloning`: after observing the release, the source writer creates a named
   checkpoint from the closed source and an idempotent projected clone.
5. `ready`: the clone has been verified and is safe to route.
6. `completing`: the coordinator atomically installs the new routing map and
   ownership assignment.
7. The coordinator removes the migration record. If more replicas were added,
   it plans the next single-shard split.

Routing and shard count remain unchanged through preparation, draining, and
cloning. Writes are retried during the short drain/clone window, so no
acknowledged writes can fall between a clone checkpoint and cutover.
The source's ingress read lock is held through coordinator acceptance. Taking
the exclusive drain lock therefore waits for every admitted write, rejects new
local writes, and orders coordinator flush and durable SlateDB flush before
close, Lease release, and checkpoint creation.
Consequently, failed preparation or cloning cannot expose a partial target. All
transitions increment the assignment generation and use the ShardMap
`resourceVersion` compare-and-swap. The source-side preparation hook is
idempotent so a restarted writer can safely retry it. A failed migration keeps
the old routing live and records its error for operator intervention.

The coordinator will not cut over until the old source Lease is released and
the projected clone is verified. Readers reconcile their open SlateDB handles
before publishing each watched routing generation.

The product CR's writer replica count is user intent. The operator creates
additional StatefulSet pods before Rust begins scale-up, but never reduces the
StatefulSet below the authoritative ShardMap count. Scale-down is intentionally
blocked and reported through product status until online shard merge is
implemented.

## Rules and tuning

- Kubernetes uses the product CR writer replica count as desired storage-shard
  count and gates the effective StatefulSet count against ShardMap state.
  `virtual_shards` remains a standalone/static backend setting.
- A replica-count increase is a requested storage-shard migration. Until each
  split completes, the persisted ShardMap remains authoritative.
- Storage keys encode the 12-bit routing slot inside each existing
  namespace/time segment, allowing projected clones to select a contiguous
  slot interval without increasing SlateDB segment count.
- `io_concurrency_limit` sets a fixed per-pod storage I/O budget. Its default
  is 128, independent of how many storage shards a standalone process or
  reader opens. Kubernetes writers normally own one storage shard per pod.
- Set `renew_interval_seconds` comfortably below
  `lease_duration_seconds` (defaults 5 and 15).
- Every forwarding participant must share `auth.internal` when it is enabled.

Database records choose routing boundaries differently: Meter uses time
buckets, Line uses time segments, and Track uses time segments plus a reserved
trace-locator segment.

For provisional per-writer ingestion envelopes, operational headroom, and
product-shaped benchmark requirements, see
[Capacity planning and scaling](scaling.md).
