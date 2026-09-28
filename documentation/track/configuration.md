# Track configuration

Run `track-server --config <path>`. The following is a complete representative
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
  path: track # Object-key prefix; namespace/shard suffixes are added.
  settings_path: /etc/track/SlateDb.toml # Optional SlateDB settings file.

  # Object-store types: Local, Aws, Azure, Gcp, or InMemory.
  object_store:
    type: Local
    path: /var/lib/track
  # AWS example (credentials come from the standard AWS provider chain):
  # object_store:
  #   type: Aws
  #   region: us-east-1
  #   bucket: track
  #   endpoint: http://minio:9000 # Optional S3-compatible endpoint.
  #   allow_http: true
  #   virtual_hosted_style: false
  # Azure example:
  # object_store:
  #   type: Azure
  #   account: telemetry
  #   container: track
  #   endpoint: http://azurite:10000/telemetry # Optional.
  #   allow_http: true
  # GCP example:
  # object_store:
  #   type: Gcp
  #   bucket: track
  #   base_url: http://gcs-emulator:4443 # Optional.
  # In-memory object-store example (development and tests only):
  # object_store:
  #   type: InMemory

  # Optional SST data-block cache. FoyerHybrid adds a disposable disk tier.
  block_cache:
    type: FoyerHybrid
    memory_capacity: 536870912 # Bytes; 512 MiB.
    disk_capacity: 10737418240 # Bytes; 10 GiB.
    disk_path: /var/cache/track
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
retention_seconds: 2592000 # Optional logical retention; 30 days.

page:
  target_size_bytes: 1048576 # Preferred encoded page size; 1 MiB.
  max_size_bytes: 4194304 # Hard encoded page limit; 4 MiB.
  max_traces: 1024 # Maximum traces in one page.

# Successful periodic L0 flushes make accepted writes visible to readers.
visibility_interval_seconds: 1

write:
  # applied: memory only; written: mutable SlateDB state; durable: object store.
  durability: written
  remote_concurrency: 16 # Concurrent shard-forwarding batches.
  remote_retries: 2 # Retries after a stale-ownership response.

sharding:
  # Storage-shard count fixed when the dataset is created.
  virtual_shards: 8
  io_concurrency_multiplier: 8
  backend: standalone

  # Static backend alternative. Ranges are half-open and must exactly cover
  # [0, virtual_shards); owner_id must match an owners entry.
  # backend: static
  # owner_id: track-0
  # owners:
  #   - id: track-0
  #     ordinal: 0
  #     endpoint: track-0:9092
  #     start_shard: 0
  #     end_shard: 4
  #   - id: track-1
  #     ordinal: 1
  #     endpoint: track-1:9092
  #     start_shard: 4
  #     end_shard: 8

  # Kubernetes backend alternative (requires Kubernetes feature support):
  # backend: kubernetes
  # database: track    # Leases are labeled telemetry.plural.sh/track=<database>
  # namespace: default
  # stateful_set: track
  # headless_service: track-headless
  # owner_port: 9092
  # shard_map: track-shard-map
  # coordinator_lease: track-shard-coordinator
  # shard_lease_prefix: track-shard
  # lease_duration_seconds: 15
  # renew_interval_seconds: 5 # Must be shorter than the lease duration.

request:
  max_request_bytes: 10485760 # Maximum ingestion body; 10 MiB.
  request_concurrency: 64 # Concurrent ingestion requests.
  query_concurrency: 8 # Concurrent query work.
  max_candidates: 10000 # Candidate traces considered by a search.
  max_spans_per_trace: 100000 # Safety limit while assembling a trace.
  max_query_limit: 1000 # Maximum client-requested result count.

auth:
  unauthenticated: false # Require credentials for namespace APIs.
  internal: { source: env, name: TRACK_INTERNAL_TOKEN } # Writer-to-writer token.
  # Other secret forms:
  # internal: { source: file, path: /var/run/secrets/track/internal-token }
  # internal: { source: literal, value: development-only }

  # Optional external Bearer JWT verification:
  jwt:
    jwks: { source: url, url: https://issuer.example/.well-known/jwks.json }
    # File alternative:
    # jwks: { source: file, path: /var/run/secrets/track/jwks.json }
    issuer: https://issuer.example/
    audience: track
    refresh_interval_seconds: 300
    request_timeout_seconds: 5

  # Basic credentials accepted for every namespace.
  global:
    read:
      - type: basic
        username: global-reader
        password: { source: env, name: TRACK_GLOBAL_READ_PASSWORD }
    write: []

# At least one unique namespace is required. Only listed namespaces are served.
namespaces:
  - name: default
    auth:
      read:
        - type: basic
          username: tempo-reader
          password: { source: file, path: /var/run/secrets/track/read-password }
      write:
        - type: basic
          username: trace-writer
          password: { source: env, name: TRACK_WRITE_PASSWORD }
```

`page.target_size_bytes` must not exceed `page.max_size_bytes`. Durations and
resource limits must be positive. The checked-in
[`track.example.yaml`](../../config/track.example.yaml) is a runnable local
variant of this configuration.
