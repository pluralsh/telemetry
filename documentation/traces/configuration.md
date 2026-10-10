# Traces configuration

Run `plural-traces-server --config <path>`. The following is a complete representative
configuration; copy it and adjust the values for the deployment. See
[sharding](../common/sharding.md) and
[authentication](../common/authentication.md) for operational details.

```yaml
# Server role: standalone, writer, or reader.
# Standalone serves reads and writes and requires backend: standalone.
mode: standalone

listeners:
  http: 0.0.0.0:3200 # Public Tempo-compatible HTTP API.
  grpc: 0.0.0.0:9092 # Internal writer-to-writer gRPC API.
  otlp_grpc: 0.0.0.0:4317 # OTLP ingestion.
  jaeger_grpc: 0.0.0.0:14250 # Jaeger collector ingestion.

storage:
  # Storage types are SlateDb and InMemory (case-sensitive).
  type: SlateDb
  path: traces # Object-key prefix; namespace/shard suffixes are added.
  settings_path: /etc/traces/SlateDb.toml # Optional SlateDB settings file.

  # Object-store types: Local, Aws, Azure, Gcp, or InMemory.
  object_store:
    type: Local
    path: /var/lib/traces
  # AWS example (credentials come from the standard AWS provider chain):
  # object_store:
  #   type: Aws
  #   region: us-east-1
  #   bucket: traces
  #   endpoint: http://minio:9000 # Optional S3-compatible endpoint.
  #   allow_http: true
  #   virtual_hosted_style: false
  # Azure example:
  # object_store:
  #   type: Azure
  #   account: telemetry
  #   container: traces
  #   endpoint: http://azurite:10000/telemetry # Optional.
  #   allow_http: true
  # GCP example:
  # object_store:
  #   type: Gcp
  #   bucket: traces
  #   base_url: http://gcs-emulator:4443 # Optional.
  # In-memory object-store example (development and tests only):
  # object_store:
  #   type: InMemory

  # Optional SST data-block cache. FoyerHybrid adds a disposable disk tier.
  block_cache:
    type: FoyerHybrid
    memory_capacity: 536870912 # Bytes; 512 MiB.
    disk_capacity: 10737418240 # Bytes; 10 GiB.
    disk_path: /var/cache/traces
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

# Entire storage alternative for ephemeral development and tests:
# storage:
#   type: InMemory

segment_duration_seconds: 3600 # Trace time partition width.
retention_seconds: 1209600 # Logical retention; 14 days, the default. `null` keeps data forever.

page:
  target_size_bytes: 1048576 # Preferred page trace data size, excluding the column sidecar; 1 MiB.
  max_size_bytes: 4194304 # Hard page trace data limit, excluding the column sidecar; 4 MiB.
  max_traces: 1024 # Maximum traces in one page.

write:
  # applied: memory only; written: mutable SlateDB state; durable: object store.
  durability: applied
  # Durable object-store flush and read-replica visibility interval; 0 disables it.
  flush_interval_seconds: 10
  # Per-storage-shard coordinator bounds. Memory can include the live delta,
  # up to two frozen deltas, and queued request payloads.
  buffer_queue_capacity: 10000
  buffer_flush_interval_milliseconds: 30000
  buffer_size_threshold_bytes: 67108864 # 64 MiB.
  remote_concurrency: 16 # Concurrent shard-forwarding batches.
  remote_retries: 2 # Retries after a stale-ownership response.

sharding:
  # Storage-shard count fixed when the dataset is created.
  shards: 1
  # Fixed per-pod storage I/O budget; independent of storage-shard count.
  io_concurrency_limit: 128
  backend: standalone

  # Static backend alternative. Ranges are half-open and must exactly cover
  # [0, shards); owner_id must match an owners entry.
  # backend: static
  # owner_id: traces-0
  # owners:
  #   - id: traces-0
  #     ordinal: 0
  #     endpoint: traces-0:9092
  #     start_shard: 0
  #     end_shard: 4
  #   - id: traces-1
  #     ordinal: 1
  #     endpoint: traces-1:9092
  #     start_shard: 4
  #     end_shard: 8

  # Kubernetes backend alternative (requires Kubernetes feature support):
  # backend: kubernetes
  # database: traces    # Leases are labeled telemetry.plural.sh/traces=<database>
  # namespace: default
  # stateful_set: traces
  # headless_service: traces-headless
  # owner_port: 9092
  # shard_map: traces-shard-map
  # coordinator_lease: traces-shard-coordinator
  # shard_lease_prefix: traces-shard
  # lease_duration_seconds: 15
  # renew_interval_seconds: 5 # Must be shorter than the lease duration.

request:
  max_request_bytes: 33554432 # Maximum request body as received; 32 MiB.
  max_decoded_request_bytes: 134217728 # Maximum write body after gzip, and maximum OTLP/Jaeger gRPC message; 128 MiB. At least max_request_bytes.
  request_concurrency: 64 # Concurrent ingestion requests.
  query_concurrency: 8 # Concurrent query work.
  max_candidates: 10000 # Candidate traces considered by a search.
  max_spans_per_trace: 100000 # Safety limit while assembling a trace.
  max_query_limit: 1000 # Maximum client-requested result count.

auth:
  unauthenticated: false # Require credentials for namespace APIs.
  internal: { source: env, name: TRACES_INTERNAL_TOKEN } # Writer-to-writer token.
  # Other secret forms:
  # internal: { source: file, path: /var/run/secrets/traces/internal-token }
  # internal: { source: literal, value: development-only }

  # Optional external Bearer JWT verification:
  jwt:
    jwks: { source: url, url: https://issuer.example/.well-known/jwks.json }
    # File alternative:
    # jwks: { source: file, path: /var/run/secrets/traces/jwks.json }
    issuer: https://issuer.example/
    audience: traces
    refresh_interval_seconds: 300
    request_timeout_seconds: 5

  # Basic credentials accepted for every namespace.
  global:
    read:
      - type: basic
        username: global-reader
        password: { source: env, name: TRACES_GLOBAL_READ_PASSWORD }
    write: []

# At least one unique namespace is required. Only listed namespaces are served.
namespaces:
  - name: default
    auth:
      read:
        - type: basic
          username: tempo-reader
          password: { source: file, path: /var/run/secrets/traces/read-password }
      write:
        - type: basic
          username: trace-writer
          password: { source: env, name: TRACES_WRITE_PASSWORD }
```

`page.target_size_bytes` must not exceed `page.max_size_bytes`. Durations and
resource limits must be positive. The checked-in
[`traces.example.yaml`](../../config/traces.example.yaml) is a runnable local
variant of this configuration.
