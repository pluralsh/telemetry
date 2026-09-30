# Meter configuration

Run `meter-server --config <path>`. The default path is `config/meter.yaml`.
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
path_prefix: /meter

storage:
  path: meter # SlateDB object-key prefix; namespace/shard suffixes are added.
  # Optional SlateDB TOML, JSON, or YAML settings file.
  settings_path: /etc/meter/SlateDb.toml

  # Types: Local, Aws, Azure, Gcp, or InMemory (case-sensitive).
  object_store:
    type: Local
    path: /var/lib/meter
  # AWS example (credentials come from the standard AWS provider chain):
  # object_store:
  #   type: Aws
  #   region: us-east-1
  #   bucket: meter
  #   endpoint: http://minio:9000 # Optional S3-compatible endpoint.
  #   allow_http: true
  #   virtual_hosted_style: false
  # Azure example:
  # object_store:
  #   type: Azure
  #   account: telemetry
  #   container: meter
  #   endpoint: http://azurite:10000/telemetry # Optional.
  #   allow_http: true
  # GCP example:
  # object_store:
  #   type: Gcp
  #   bucket: meter
  #   base_url: http://gcs-emulator:4443 # Optional.
  # In-memory example (development and tests only):
  # object_store:
  #   type: InMemory

  # Optional SST data-block cache. FoyerHybrid adds a disposable disk tier.
  block_cache:
    type: FoyerHybrid
    memory_capacity: 536870912 # Bytes; 512 MiB.
    disk_capacity: 10737418240 # Bytes; 10 GiB.
    disk_path: /var/cache/meter
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

retention_seconds: 2592000 # Optional retention; 30 days. Unset keeps data forever.

# Per-shard query-cache capacity, counted in time-bucket entries (not bytes).
reader_cache_capacity: 268435456

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

sharding:
  # Storage-shard count fixed when the dataset is created.
  shards: 1
  # Fixed per-pod storage I/O budget; independent of storage-shard count.
  io_concurrency_limit: 128
  backend: standalone

  # Static backend alternative. Ranges are half-open and must exactly cover
  # [0, shards); owner_id must match an owners entry.
  # backend: static
  # owner_id: meter-0
  # owners:
  #   - id: meter-0
  #     ordinal: 0
  #     endpoint: meter-0:9090
  #     start_shard: 0
  #     end_shard: 4
  #   - id: meter-1
  #     ordinal: 1
  #     endpoint: meter-1:9090
  #     start_shard: 4
  #     end_shard: 8

  # Kubernetes backend alternative:
  # backend: kubernetes
  # database: meter    # Leases are labeled telemetry.plural.sh/meter=<database>
  # namespace: default
  # stateful_set: meter
  # headless_service: meter-headless
  # owner_port: 9090
  # shard_map: meter-shard-map
  # coordinator_lease: meter-shard-coordinator
  # shard_lease_prefix: meter-shard
  # lease_duration_seconds: 15
  # renew_interval_seconds: 5 # Must be shorter than the lease duration.

auth:
  unauthenticated: false # Require credentials for namespace APIs.

  # Optional shared token for writer-to-writer requests; not an HTTP credential.
  internal: { source: env, name: METER_INTERNAL_TOKEN }
  # Other secret forms:
  # internal: { source: file, path: /var/run/secrets/meter/internal-token }
  # internal: { source: literal, value: development-only }

  # Optional external Bearer JWT verification:
  jwt:
    jwks: { source: url, url: https://issuer.example/.well-known/jwks.json }
    # File alternative:
    # jwks: { source: file, path: /var/run/secrets/meter/jwks.json }
    issuer: https://issuer.example/
    audience: meter
    refresh_interval_seconds: 300
    request_timeout_seconds: 5

  # Basic credentials accepted for every namespace.
  global:
    read:
      - type: basic
        username: global-reader
        password: { source: env, name: METER_GLOBAL_READ_PASSWORD }
    write: []

# At least one unique namespace is required. Only listed namespaces are served.
namespaces:
  - name: default
    auth:
      read:
        - type: basic
          username: prometheus
          password: { source: file, path: /var/run/secrets/meter/read-password }
      write:
        - type: basic
          username: remote-writer
          password: { source: env, name: METER_WRITE_PASSWORD }
```

The checked-in [`meter.example.yaml`](../../config/meter.example.yaml) is a
runnable local variant of this configuration.
