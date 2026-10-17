# Metrics configuration

Run `plural-metrics-server --config <path>`. The default path is `config/metrics.yaml`.
The following is a complete representative configuration; copy it and adjust
the values for the deployment. See [sharding](../common/sharding.md) and
[authentication](../common/authentication.md) for operational details.

```yaml
# Server role: standalone, writer, or reader.
# Standalone serves reads and writes and requires backend: standalone.
mode: standalone

listeners:
  http: 0.0.0.0:8080 # Public Prometheus-compatible HTTP API.
  grpc: 0.0.0.0:9090 # Internal writer-to-writer gRPC API.

# Optional prefix for public APIs. It must start with "/" and not end with "/".
# Health, readiness, and metrics endpoints remain unprefixed.
path_prefix: /metrics

storage:
  path: metrics # SlateDB object-key prefix; namespace/shard suffixes are added.
  # Optional SlateDB TOML, JSON, or YAML settings file, layered over the
  # Metrics SlateDB defaults below.
  settings_path: /etc/metrics/SlateDb.toml

  # Types: Local, Aws, Azure, Gcp, or InMemory (case-sensitive).
  object_store:
    type: Local
    path: /var/lib/metrics
  # AWS example (credentials come from the standard AWS provider chain):
  # object_store:
  #   type: Aws
  #   region: us-east-1
  #   bucket: metrics
  #   endpoint: http://minio:9000 # Optional S3-compatible endpoint.
  #   allow_http: true
  #   virtual_hosted_style: false
  # Azure example:
  # object_store:
  #   type: Azure
  #   account: telemetry
  #   container: metrics
  #   endpoint: http://azurite:10000/telemetry # Optional.
  #   allow_http: true
  # GCP example:
  # object_store:
  #   type: Gcp
  #   bucket: metrics
  #   base_url: http://gcs-emulator:4443 # Optional.
  # In-memory example (development and tests only):
  # object_store:
  #   type: InMemory

  # Optional SST data-block cache. FoyerHybrid adds a disposable disk tier.
  block_cache:
    type: FoyerHybrid
    memory_capacity: 536870912 # Bytes; 512 MiB.
    disk_capacity: 10737418240 # Bytes; 10 GiB.
    disk_path: /var/cache/metrics
    write_policy: WriteOnInsertion # Or WriteOnEviction.
    flushers: 4
    buffer_pool_size: 16777216 # Optional; default is memory_capacity / 32.
    submit_queue_size_threshold: 1073741824 # Bytes; default 1 GiB.
  # In-memory cache alternative:
  # block_cache:
  #   type: FoyerMemory
  #   capacity: 536870912
  #   shards: 8 # Optional; Foyer derives this from CPUs when omitted.

  # Optional cache for indexes, filters, and statistics.
  meta_cache:
    type: FoyerMemory
    capacity: 134217728 # Bytes; 128 MiB.
    shards: 8 # Optional.

  # The disk readers cache blocks on; SST blocks are sized to throughput / iops
  # (a power of two, 4-64 KiB) so scans stream the disk rather than exhaust its
  # IOPS. Defaults to an AWS EBS gp3 volume's baseline, giving 64 KiB blocks.
  # Writers and readers of one database should describe the same disk.
  disk:
    throughput_bytes_per_second: 131072000 # 125 MiB/s.
    iops: 3000

retention_seconds: 5184000 # 60 days, the default. `null` keeps data forever.

# Per-shard query-cache capacity, counted in time-bucket entries (not bytes).
reader_cache_capacity: 268435456

# Optional SlateDB disk cache of what reader shards read from the object store,
# a directory per shard under path; off unless path is set. Put it on a volume
# that outlives restarts so a restarted reader skips the reads that filled it.
# reader_disk_cache:
#   path: /var/cache/metrics-reads
#   max_bytes_per_shard: 17179869184 # Optional; SlateDB's default, 16 GiB.

# Bytes of resolved selector postings (bucket + sorted matchers -> series ids)
# shared across queries, per namespace on each storage shard; 64 MiB.
# Entries are dropped once their bucket gains series.
matcher_cache_capacity_bytes: 67108864

# Bytes shared by cached forward-index entries and generation-aware resolved
# selectors on each storage shard; 64 MiB by default.
forward_index_cache_capacity_bytes: 67108864

# Range-query result cache. Steps are reused while the write generations of
# every hour bucket they read are unchanged on every shard; `@` queries,
# steps after now, and failed queries are never cached.
result_cache:
  enabled: true
  capacity_bytes: 134217728 # 128 MiB.

# Limits shared by every PromQL query this process evaluates. Queries past
# max_concurrent wait for a slot and fail with 503 after queue_timeout_seconds.
# Each query's engine memory (sample buffers, accumulators, intermediate
# results) is capped at max_memory_bytes, and all running queries together at
# memory_budget_bytes, so concurrent dashboard panels cannot add up past it.
# A query that finds the budget full fails, so keep max_memory_bytes at most
# half the budget and let max_concurrent queue bursts instead. Size the
# budget to what the container can spare after its caches; the operator
# sizes it from the memory limit, with half of that per query, at most 1 GiB.
query:
  max_concurrent: 32
  queue_timeout_seconds: 30
  max_memory_bytes: 1073741824 # 1 GiB.
  memory_budget_bytes: 2147483648 # 2 GiB.
  max_concurrent_shard_reads: 16 # Reads served to other reader groups at once.

# Writers roll each closed window_seconds window of hour buckets up into
# one bucket, then each closed day's windows into 24, 12 or 8 hour buckets,
# delay_seconds after each ends, checking every interval_seconds. No bucket
# covers more than max_bucket_bytes of stored data; hours too large to share
# a bucket stay as they are. See the storage format's Rollup section.
# The operator gives writers an 8Gi memory limit by default and does not
# roll out a writer whose limit is below 6Gi.
rollup:
  enabled: true
  max_bucket_bytes: 4294967296 # 4 GiB.
  delay_seconds: 600
  window_seconds: 14400 # 4 hours.
  merge_wait_seconds: 1800 # Longest a rollup waits for its merge before it is sealed anyway.
  interval_seconds: 300
  copy_buffer_bytes: 67108864 # Copied bytes queued before waiting for storage; the operator sizes it from the memory limit.

# Readers warm the SST filters and indexes of recent buckets into meta_cache,
# newest bucket first, in the background: they are ready at once, and queries
# during the pass find the cache partly warm. Warming stops once it holds
# metadata_budget_percent of meta_cache, counting what did not fit, and evicts
# what it warmed of SSTs compaction or rollups removed. Writers never warm.
# A section that sets only some keys keeps these defaults for the rest. See
# "Metadata warming" below.
cache_warmer:
  enabled: true # Startup pass; false leaves only continuous warming.
  block_readiness: false # true holds readiness until the pass ends or times out.
  timeout_seconds: 30 # Applies only with block_readiness.
  warm_range_seconds: 0 # 0 = all live data, bounded by metadata_budget_percent
  concurrency: 2 # SSTs in flight, out of sharding.io_concurrency_limit.
  include_payloads: false # true also warms hour buckets' sample blocks into block_cache.
  metadata_budget_percent: 75
  continuous: # Every interval, warms SSTs that appeared since the last pass.
    enabled: true
    interval_seconds: 10
    warm_range_seconds: 0
    payload_warm_range_seconds: 0 # 0 = 7200 unless warm_range_seconds is set
    include_payloads: true

write:
  # applied: memory only; written: mutable SlateDB state; durable: object store.
  durability: applied
  flush_interval_seconds: 10 # Durable flush and read-replica visibility interval; 0 disables it.
  # Per-storage-shard coordinator bounds. Memory can include the live delta,
  # up to two frozen deltas, and queued request payloads.
  buffer_queue_capacity: 10000
  buffer_flush_interval_milliseconds: 30000
  buffer_size_threshold_bytes: 67108864 # 64 MiB.
  remote_concurrency: 16 # Concurrent shard-forwarding batches.
  remote_retries: 2 # Retries after a stale-ownership response.

request:
  max_request_bytes: 33554432 # Maximum request body as received; 32 MiB.
  max_decoded_request_bytes: 134217728 # Maximum write body after gzip or snappy decoding; 128 MiB. At least max_request_bytes.

sharding:
  # Storage-shard count fixed when the dataset is created.
  shards: 1
  # Fixed per-pod storage I/O budget; independent of storage-shard count.
  io_concurrency_limit: 128
  backend: standalone

  # Static backend alternative. Ranges are half-open and must exactly cover
  # [0, shards); owner_id must match an owners entry.
  # backend: static
  # owner_id: metrics-0
  # owners:
  #   - id: metrics-0
  #     ordinal: 0
  #     endpoint: metrics-0:9090
  #     start_shard: 0
  #     end_shard: 4
  #   - id: metrics-1
  #     ordinal: 1
  #     endpoint: metrics-1:9090
  #     start_shard: 4
  #     end_shard: 8

  # Kubernetes backend alternative:
  # backend: kubernetes
  # database: metrics    # Leases are labeled telemetry.plural.sh/metrics=<database>
  # namespace: default
  # stateful_set: metrics
  # headless_service: metrics-headless
  # owner_port: 9090
  # shard_map: metrics-shard-map
  # coordinator_lease: metrics-shard-coordinator
  # shard_lease_prefix: metrics-shard
  # lease_duration_seconds: 15
  # renew_interval_seconds: 5 # Must be shorter than the lease duration.

  # Reader groups (reader mode only). The reader serves one storage shard and
  # reads every other shard from that shard's group over internal gRPC.
  # read_groups:
  #   shard: 0             # Defaults to the READER_SHARD environment variable.
  #   endpoint: metrics-reader-{shard}-headless.default.svc:9090
  #   hedge_after_ms: 250  # Second replica after this delay; 0 disables hedging.
  #   request_timeout_seconds: 120
  #   resolve_interval_seconds: 5 # Re-resolve each group's replica addresses.

auth:
  unauthenticated: false # Require credentials for namespace APIs.

  # Optional shared token for writer-to-writer requests; not an HTTP credential.
  internal: { source: env, name: METRICS_INTERNAL_TOKEN }
  # Other secret forms:
  # internal: { source: file, path: /var/run/secrets/metrics/internal-token }
  # internal: { source: literal, value: development-only }

  # Optional external Bearer JWT verification:
  jwt:
    jwks: { source: url, url: https://issuer.example/.well-known/jwks.json }
    # File alternative:
    # jwks: { source: file, path: /var/run/secrets/metrics/jwks.json }
    issuer: https://issuer.example/
    audience: metrics
    refresh_interval_seconds: 300
    request_timeout_seconds: 5

  # Basic credentials accepted for every namespace.
  global:
    read:
      - type: basic
        username: global-reader
        password: { source: env, name: METRICS_GLOBAL_READ_PASSWORD }
    write: []

# At least one unique namespace is required. Only listed namespaces are served.
namespaces:
  - name: default
    auth:
      read:
        - type: basic
          username: prometheus
          password: { source: file, path: /var/run/secrets/metrics/read-password }
      write:
        - type: basic
          username: remote-writer
          password: { source: env, name: METRICS_WRITE_PASSWORD }
```

The checked-in [`metrics.example.yaml`](../../config/metrics.example.yaml) is a
runnable local variant of this configuration.

## Metadata warming

Metrics and Traces readers (and standalone servers) warm SST metadata by
default. This applies to both products; Traces documents its keys in
[Traces configuration](../traces/configuration.md).

### Where metadata lives

SlateDB's cache splits by entry kind:

- Data blocks go to `storage.block_cache`.
- SST indexes, bloom filters and SST stats go to `storage.meta_cache`.

Without a `meta_cache`, metadata is not cached at all, and metadata-only
warming does nothing.

An in-memory cache (`FoyerMemory`) weighs each entry at its payload size plus
320 bytes. The 320 bytes cover the cache record, its key, its index slot and
the `Arc` around the decoded index or filter.

A fetched entry is clamped to its logical size, so a filter decoded from a
larger read buffer does not keep that buffer alive.

Foyer splits a cache's capacity evenly across its shards, and an entry larger
than its shard's share is evicted as soon as it is inserted. SlateDB's
default is one shard per CPU, so a small `meta_cache` on a large node could
not hold a single large index or filter. Unless `meta_cache.shards` is set,
`meta_cache` therefore uses at most one shard per 16 MiB of capacity (at
least one).

### The warm pass

A startup pass, and every continuous pass after it, works as follows:

1. It lists the live SSTs of the buckets (Traces: segments, plus the trace
   locator) inside `warm_range_seconds`. The default, `0`, lists all live
   data, so metadata warming covers retention and the budget decides how
   much of it is held. Payloads, when included, are warmed only for the
   buckets inside the payload range (`payload_warm_range_seconds`, or
   `warm_range_seconds` when set, or 7200 s): they share `block_cache` with
   queries, so a long payload range would churn it.
2. It orders them newest first. Metrics uses the bucket's end; Traces uses
   the segment's end, and the locator's SST creation time.
3. It warms each SST's index and filter, up to `concurrency` SSTs at a time,
   out of `sharding.io_concurrency_limit`.

A ledger records the bytes each warm added to `meta_cache`. These are
measured, not estimated: every metadata entry the warm fetched is attributed
to it, whether the warm loaded the entry or found it already cached.

Before an SST starts, its estimate is reserved against the budget, which is
`metadata_budget_percent` of `meta_cache`. The estimate is the index length
(×3 when compressed), plus the filter length, plus the per-entry overhead.

When the next SST does not fit:

- At startup, the pass stops. It reports that SST and every older one as
  skipped for budget, rather than churning the cache by evicting what it just
  warmed.
- On a continuous pass, a newer SST takes the room of the oldest SSTs the
  ledger holds. Their entries stay cached until the cache's own LRU evicts
  them. When even that is not enough, the pass stops as above.

On every pass, SSTs that are no longer live are forgotten and evicted from
`meta_cache`, so nothing the warmer did pins them. This covers SSTs removed by
compaction, by a rollup replacing a bucket, or by retention. The warmer holds
SST ids only, never cache handles.

Readiness does not wait for the pass unless `block_readiness: true`, which
also brings back `timeout_seconds`. Queries during the pass read whatever is
warm by then; because the newest buckets warm first, the dashboards' "last
hour" queries benefit first.

`continuous.include_payloads` (default `true`) still warms the sample blocks
of new hour buckets inside the payload range into `block_cache`. Those
blocks are bounded by `block_cache` and are not counted against the
metadata budget.

With the default range, `n` below is every live SST, so planning walks the
whole live set each pass. That is in-memory bookkeeping, about 150 bytes per
SST; SSTs the ledger already holds are skipped without I/O, and those that do
not fit the budget are counted without I/O. Startup I/O is bounded by what
fits the budget: about two reads per warmed SST.

### Memory accounting

In the table below:

- `C` is `meta_cache` capacity and `B = C × metadata_budget_percent / 100`.
- `D` is `block_cache`'s memory capacity.
- `N` is the number of live SSTs in all shards.
- `n` is the number of SSTs inside the warm range: all live SSTs by
  default.
- `k` is `concurrency`.

The default-sizing values are what the operator renders for a 4Gi reader
(`C` = 184 MiB, `D` = 676 MiB). The large-deployment values are for a 16Gi
reader (`C` = 737 MiB, `D` = 2.7 GiB) over 30 days of 1M series, or heavy
traces, with about 10k live SSTs.

| Component | Bound | 4Gi reader | 16Gi reader, 30d, 1M series |
|---|---|---|---|
| Warmed metadata (part of `meta_cache`) | ≤ `B` | ≤ 138 MiB | ≤ 553 MiB |
| `meta_cache` (warmed + query-fetched) | ≤ `C` | ≤ 184 MiB | ≤ 737 MiB |
| Ledger (SST id, bytes, recency per warmed SST) | ≈ 100 B × warmed SSTs ≤ 100 B × `n` | < 1 MiB | ≈ 1 MiB |
| Plan per pass (SST id, estimate, live-set entry) | ≈ 150 B × `N`, freed after the pass | < 1 MiB | ≈ 1.5 MiB |
| In-flight warm reads | `k` × (largest index × 4 + largest filter) | ≈ 10 MiB | ≈ 25 MiB |
| Data blocks from continuous payload warming | ≤ `D`, shared with queries | ≤ 676 MiB | ≤ 2.7 GiB |

Nothing in the table grows with retention or series count beyond the
per-SST bookkeeping. More metadata than `B` means more SSTs are skipped and
read cold, not more memory.

The bounds are in weighed cache bytes. In process memory, a warmed byte costs
about 2 bytes (see below), so plan for up to `2 × B` of footprint from warming,
and up to `2 × C` for a full `meta_cache` whoever filled it.

### Measured memory

The native profiles measure process footprint (`phys_footprint`, which also
counts compressed and swapped pages) next to the cache gauges. They live in
`crates/metrics/src/profile/cold/memory.rs` and
`crates/traces/src/db/profile.rs`, are `#[ignore]`d, and are run on a store
whose metadata exceeds the budget:

- Metrics: 27 SSTs, 4.5 MiB of metadata. `meta_cache` is 2 MiB (`B` =
  1.5 MiB) and `block_cache` 512 MiB.
- Traces: 3M traces, 109 SSTs, 20.7 MiB of metadata.

Results:

- The ledger always equals `foyer_memory_usage{name="slatedb_meta_cache"}`
  and the estimate is within 0.1%.
- Metrics, with a writer adding hours and rolling them up across 60
  continuous passes:
  - The ledger stayed at 1.34–1.50 MiB with about 18 SSTs skipped for budget.
  - Removed SSTs were evicted on most passes.
  - `meta_cache` never exceeded 2 MiB.
  - Footprint plateaued at 578 MiB once `block_cache` reached its 512 MiB cap,
    against 528 MiB predicted.
  - The writer's WAL replay added up to 200 MiB transiently.
- Traces: footprint grows linearly with the budget and stays flat across
  rollup cycles:

  | Budget | Warmed | Footprint over warming off |
  |---|---|---|
  | 6 MiB | 5.7 MiB | +9.5 MiB |
  | 12 MiB | 10.7 MiB | +16 MiB |
  | 48 MiB (all metadata) | 20.7 MiB | +31 MiB |

- Under concurrent queries the query working set (1–2.5 GiB in these debug
  builds) dominates, and warming's share is within run-to-run noise.

Each warmed byte costs 1.5–1.7 bytes of footprint. This gap is live memory,
not allocator retention: it does not change with `MIMALLOC_PURGE_DELAY=0`.
The weights count only the index and filter buffers, so the likely causes are
allocator size classes and copies held while a pass runs. The cause has not
been pinned down, which is why the sizing above uses 2×.

### Measured latency

The cold battery (`FUZZ_SCENARIO=cold`, scale 1, 3 repeats, MinIO +20 ms first
byte) compares three starts per query with `FUZZ_COLD_IMPL_STARTS=off,ready,warm`:
warming off, a query right after ready, and a query after the startup pass.
It ran in both start orders:

| Product | Start | Cold p50 | GETs per query (p50) |
|---|---|---|---|
| Metrics | off | 193–206 ms | 24–33 |
| Metrics | after warm | 164–176 ms | 20–27 |
| Traces | off | 506–716 ms | 89–106 |
| Traces | after warm | 360–380 ms | 73–87 |

A query right after ready makes slightly more GETs than with warming off,
because it fetches metadata the pass is still loading. It was as fast as or
faster than a query after warm, likely because it runs before the idle settle
and finds open connections. At this scale the startup pass finishes in under
0.2 s after ready. It costs 4–6 more startup GETs (20 KiB) for Metrics and
18 more (260–280 KiB) for Traces.

### Metrics

Each metric is labeled with `product`.

| Metric | Type | Meaning |
|---|---|---|
| `telemetry_cache_warmer_budget_bytes` | gauge | `B` |
| `telemetry_cache_warmer_warmed_bytes` | gauge | Bytes the ledger holds after the last pass |
| `telemetry_cache_warmer_warmed_ssts` | gauge | SSTs the ledger holds after the last pass |
| `telemetry_cache_warmer_planned_bytes` | gauge | Estimated metadata of every SST the last pass considered |
| `telemetry_cache_warmer_skipped_ssts{reason="budget"}` | gauge | What the last pass left cold because it did not fit |
| `telemetry_cache_warmer_skipped_bytes{reason="budget"}` | gauge | The skipped SSTs' estimated bytes |
| `telemetry_cache_warmer_displaced_ssts_total` | counter | Older SSTs forgotten to make room for newer ones |
| `telemetry_cache_warmer_evicted_ssts_total{status}` | counter | Removed SSTs evicted from `meta_cache` |
| `telemetry_cache_warmer_ssts_total{status}` | counter | SST warms by outcome |
| `telemetry_cache_warmer_runs_total{mode,status}` | counter | Passes by mode (`startup`, `continuous`) and outcome |
| `telemetry_cache_warmer_duration_seconds{mode}` | histogram | Duration of each pass |
| `foyer_memory_usage{name}` | gauge | Weighed usage of `slatedb_data_cache` and `slatedb_meta_cache` |
| `foyer_memory_entries{name}` | gauge | Their entry counts |
| `telemetry_slatedb_cache_capacity_bytes{cache}` | gauge | Their capacities (`cache` is the same name) |
| `telemetry_slatedb_cache_evictions_total{cache}` | counter | Their evictions |

### Compatibility

- Storage formats are unchanged.
- The server configs reject unknown keys, so the new keys
  (`block_readiness`, `metadata_budget_percent`) have defaults and older
  configs keep parsing.
- A `cache_warmer` section that sets only some keys keeps the reader
  defaults for the rest. For example, `cache_warmer: {concurrency: 4}` keeps
  warming on and readiness non-blocking.
- To get the earlier behaviour back:
  - `enabled: false` turns startup warming off. Continuous warming then
    records the live SSTs on its first pass and warms only newer ones, as
    before.
  - `block_readiness: true` holds readiness for the pass.
- The operator's `cacheWarmer` spec no longer defaults `enabled` to `false`.
  An unset value renders the product default (on for Metrics and Traces, off
  for Logs). Specs that set `enabled: false` explicitly keep warming off.
  Earlier CRDs defaulted it, so a resource that had a `cacheWarmer` section
  under them has `enabled: false` stored by the API server: remove the field
  to get the product default.

#### Warm ranges and rolling upgrades

- `warm_range_seconds` used to default to 7200 s for both passes. Its
  default is now `0`, all live data. An explicit value keeps its meaning:
  it bounds metadata and, unless `payload_warm_range_seconds` is set,
  payloads, so a config with `warm_range_seconds: 600` warms exactly what
  it did before.
- `payload_warm_range_seconds` is new. Servers that predate it reject it
  as an unknown key, so the operator renders it only when
  `payloadWarmRangeSeconds` is set. Set it once every reader runs a server
  that knows it.
- The operator renders `warm_range_seconds` only when `warmRangeSeconds`
  is set to something other than 7200. Earlier CRDs defaulted it to 7200
  and the API server stored that in existing resources, so 7200 is read as
  unset; choose 7199 or 7201 to pin a two-hour metadata range. Leaving the
  key out is safe for every server: older ones apply their own 7200
  default, newer ones warm all live data.
- So in any upgrade order, every server parses the config it is given:
  - Old operator, new servers: the operator still renders 7200, which new
    servers read as before.
  - New operator, old servers (for example pods not yet rolled): the key is
    left out and old servers default to 7200, as before.
  - New operator, new servers: metadata covers all live data, payloads the
    last 7200 s.

## SlateDB defaults

Metrics loads SlateDB settings from `storage.settings_path` or, without it,
from SlateDB's `SlateDb.{json,toml,yaml,yml}` files and `SLATEDB_` environment
variables, layered over these defaults instead of SlateDB's. Any key the user
sets wins; nested keys merge, so overriding one scheduler option keeps the
others. These add to the garbage-collection and trivial-move defaults every
database shares; see [storage compaction](../common/storage-compaction.md).

| Setting | Metrics | SlateDB |
|---|---|---|
| `l0_sst_size_bytes` | 16 MiB | 64 MiB |
| `compactor_options.scheduler_options.min_compaction_sources` | `"3"` | `"4"` |

Every scrape adds a merge operand per series key, and a query merges all
operands of each key it reads that compaction has not yet collapsed. SlateDB
only writes the memtable to L0 when it reaches `l0_sst_size_bytes`
(`max_wal_flushes_before_l0_flush` is too high to trigger at scrape rates), so
at 64 MiB recent data sits in many uncollapsed operands for tens of minutes.
On the `query_profile` bench (10k series at a 15 s interval, realtime mode,
memtable half full), PromQL range queries over the last hour ran a geometric
mean of 2.3x slower than over fully compacted data with SlateDB's
defaults, and 1.4x with Metrics' defaults (count over all series: 3.4x to
1.6x). The cost is an L0 flush about every 5 minutes instead of 21: about
1.5x the object-store puts and 3.5x the compaction bytes (16.7 vs
4.8 MiB/hour at this rate). Smaller L0 SSTs (8 or 4 MiB) gave no further
query gain for 5–8x the compaction bytes, and `min_compaction_sources = "2"`
roughly doubled compaction bytes for little gain. Raise `l0_sst_size_bytes`
to trade recent-data query latency for fewer writes.

Every SlateDB writer (Metrics, Logs and Traces alike) also flushes its
memtable to L0 every 10 seconds whenever it has
taken writes since the last flush, whatever its size. A reader replays each
manifest poll's new WAL files into a memtable of its own and drops those
memtables only once an L0 flush covers them, so a writer below
`l0_sst_size_bytes` would otherwise leave every reader get and scan probing
one memtable per poll since the last flush (about 150 after 30 minutes of
1 s polls). On a native replay of a 30-minute fuzz run with a split writer
and reader, the 10 s flush cut last-quarter cold `label_values` latency from
0.72 to 0.30 ms (p50) and instant queries from 1.13 to 0.90 ms. The cost is
up to 360 small L0 puts an hour per writer, folded by the compactor.
