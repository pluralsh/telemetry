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

retention_seconds: 5184000 # 60 days, the default. `null` keeps data forever.

# Per-shard query-cache capacity, counted in time-bucket entries (not bytes).
reader_cache_capacity: 268435456

# Bytes of resolved selector postings (bucket + sorted matchers -> series ids)
# shared across queries, per namespace on each storage shard; 64 MiB.
# Entries are dropped once their bucket gains series.
matcher_cache_capacity_bytes: 67108864

# Range-query result cache. Steps are reused while the write generations of
# every hour bucket they read are unchanged on every shard; `@` queries,
# steps after now, and failed queries are never cached.
result_cache:
  enabled: true
  capacity_bytes: 134217728 # 128 MiB.

write:
  # applied: memory only; written: mutable SlateDB state; durable: object store.
  durability: applied
  flush_interval_seconds: 10 # Durable flush and read-replica visibility interval; 0 disables it.
  # Per-storage-shard coordinator bounds. Memory can include the live delta,
  # up to two frozen deltas, and queued request payloads.
  buffer_queue_capacity: 10000
  buffer_flush_interval_milliseconds: 10000
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

## SlateDB defaults

Metrics loads SlateDB settings from `storage.settings_path` or, without it,
from SlateDB's `SlateDb.{json,toml,yaml,yml}` files and `SLATEDB_` environment
variables, layered over these defaults instead of SlateDB's. Any key the user
sets wins; nested keys merge, so overriding one scheduler option keeps the
others.

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
