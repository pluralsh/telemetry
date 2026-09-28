# Line configuration

Run `line-server --config <path>`. The following is a complete representative
configuration; copy it and adjust the values for the deployment. See
[sharding](../common/sharding.md) and
[authentication](../common/authentication.md) for operational details.

```yaml
# Server role: standalone, writer, or reader.
# Standalone serves reads and writes and requires backend: standalone.
mode: standalone

listeners:
  http: 0.0.0.0:3100 # Public Loki-compatible HTTP API.
  grpc: 0.0.0.0:9091 # Internal writer-to-writer gRPC API.

storage:
  # Storage types are SlateDb and InMemory (case-sensitive).
  type: SlateDb
  path: line # Object-key prefix; namespace/shard suffixes are added.
  settings_path: /etc/line/SlateDb.toml # Optional SlateDB settings file.

  # Object-store types: Local, Aws, Azure, Gcp, or InMemory.
  object_store:
    type: Local
    path: /var/lib/line
  # AWS example (credentials come from the standard AWS provider chain):
  # object_store:
  #   type: Aws
  #   region: us-east-1
  #   bucket: line
  #   endpoint: http://minio:9000 # Optional S3-compatible endpoint.
  #   allow_http: true
  #   virtual_hosted_style: false
  # Azure example:
  # object_store:
  #   type: Azure
  #   account: telemetry
  #   container: line
  #   endpoint: http://azurite:10000/telemetry # Optional.
  #   allow_http: true
  # GCP example:
  # object_store:
  #   type: Gcp
  #   bucket: line
  #   base_url: http://gcs-emulator:4443 # Optional.
  # In-memory object-store example (development and tests only):
  # object_store:
  #   type: InMemory

  # Optional SST data-block cache. FoyerHybrid adds a disposable disk tier.
  block_cache:
    type: FoyerHybrid
    memory_capacity: 536870912 # Bytes; 512 MiB.
    disk_capacity: 10737418240 # Bytes; 10 GiB.
    disk_path: /var/cache/line
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

segment_duration_seconds: 3600 # Time partition width; keep stable for a dataset.
retention_seconds: 2592000 # Optional logical retention; 30 days.

page:
  target_size_bytes: 1048576 # Preferred compressed page size; 1 MiB.
  max_rows: 16384 # Maximum rows in a page.
  max_age_seconds: 1 # Flush age for a partially filled page.
  rows_per_block: 256 # Independently decoded block size.

# Successful periodic L0 flushes make accepted writes visible to readers.
visibility_interval_seconds: 1

write:
  # applied: memory only; written: mutable SlateDB state; durable: object store.
  durability: written
  remote_concurrency: 16 # Concurrent shard-forwarding batches.
  remote_retries: 2 # Retries after a stale-ownership response.

sharding:
  virtual_shards: 8 # Must match every process sharing this dataset.
  io_concurrency_multiplier: 8
  backend: standalone

  # Static backend alternative. Ranges are half-open and must exactly cover
  # [0, virtual_shards); owner_id must match an owners entry.
  # backend: static
  # owner_id: line-0
  # owners:
  #   - id: line-0
  #     ordinal: 0
  #     endpoint: line-0:9091
  #     start_shard: 0
  #     end_shard: 4
  #   - id: line-1
  #     ordinal: 1
  #     endpoint: line-1:9091
  #     start_shard: 4
  #     end_shard: 8

  # Kubernetes backend alternative:
  # backend: kubernetes
  # database: line    # Leases are labeled telemetry.plural.sh/line=<database>
  # namespace: default
  # stateful_set: line
  # headless_service: line-headless
  # owner_port: 9091
  # assignment_config_map: line-shard-assignments
  # coordinator_lease: line-shard-coordinator
  # shard_lease_prefix: line-shard
  # lease_duration_seconds: 15
  # renew_interval_seconds: 5 # Must be shorter than the lease duration.

request:
  max_request_bytes: 10485760 # Maximum ingestion body; 10 MiB.
  max_query_entries: 5000 # Maximum log entries returned.
  max_query_pages: 10000 # Maximum pages scanned by one query.
  max_structured_metadata_fields: 128 # Per-entry metadata field limit.
  query_concurrency: 16 # Concurrent query work.
  max_in_flight_query_bytes: 134217728 # Query memory budget; 128 MiB.

cache:
  query_entries: 256 # Serialized query responses; 0 disables the cache.

auth:
  unauthenticated: false # Require credentials for namespace APIs.
  internal: { source: env, name: LINE_INTERNAL_TOKEN } # Writer-to-writer token.
  # Other secret forms:
  # internal: { source: file, path: /var/run/secrets/line/internal-token }
  # internal: { source: literal, value: development-only }

  # Optional external Bearer JWT verification:
  jwt:
    jwks: { source: url, url: https://issuer.example/.well-known/jwks.json }
    # File alternative:
    # jwks: { source: file, path: /var/run/secrets/line/jwks.json }
    issuer: https://issuer.example/
    audience: line
    refresh_interval_seconds: 300
    request_timeout_seconds: 5

  # Basic credentials accepted for every namespace.
  global:
    read:
      - type: basic
        username: global-reader
        password: { source: env, name: LINE_GLOBAL_READ_PASSWORD }
    write: []

# At least one unique namespace is required. Only listed namespaces are served.
namespaces:
  - name: default
    auth:
      read:
        - type: basic
          username: loki-reader
          password: { source: file, path: /var/run/secrets/line/read-password }
      write:
        - type: basic
          username: log-writer
          password: { source: env, name: LINE_WRITE_PASSWORD }
```

The checked-in [`line.example.yaml`](../../config/line.example.yaml) is a
runnable local variant of this configuration.
