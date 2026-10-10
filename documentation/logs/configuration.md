# Logs configuration

Run `plural-logs-server --config <path>`. The following is a complete representative
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
  path: logs # Object-key prefix; namespace/shard suffixes are added.
  settings_path: /etc/logs/SlateDb.toml # Optional SlateDB settings file.

  # Object-store types: Local, Aws, Azure, Gcp, or InMemory.
  object_store:
    type: Local
    path: /var/lib/logs
  # AWS example (credentials come from the standard AWS provider chain):
  # object_store:
  #   type: Aws
  #   region: us-east-1
  #   bucket: logs
  #   endpoint: http://minio:9000 # Optional S3-compatible endpoint.
  #   allow_http: true
  #   virtual_hosted_style: false
  # Azure example:
  # object_store:
  #   type: Azure
  #   account: telemetry
  #   container: logs
  #   endpoint: http://azurite:10000/telemetry # Optional.
  #   allow_http: true
  # GCP example:
  # object_store:
  #   type: Gcp
  #   bucket: logs
  #   base_url: http://gcs-emulator:4443 # Optional.
  # In-memory object-store example (development and tests only):
  # object_store:
  #   type: InMemory

  # Optional SST data-block cache. FoyerHybrid adds a disposable disk tier.
  block_cache:
    type: FoyerHybrid
    memory_capacity: 536870912 # Bytes; 512 MiB.
    disk_capacity: 10737418240 # Bytes; 10 GiB.
    disk_path: /var/cache/logs
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
retention_seconds: 1209600 # Logical retention; 14 days, the default. `null` keeps data forever.

# Each write-buffer flush packs every written stream of a segment into
# multi-stream objects; these limits cut larger flushes into several objects.
page:
  target_size_bytes: 1048576 # Preferred object size; 1 MiB.
  max_rows: 16384 # Maximum rows in an object.
  rows_per_block: 256 # Rows per independently decoded single-stream block.

# Writer-side merging of each segment's small objects after write-buffer flushes.
compaction:
  enabled: true
  # Adjacent same-level objects merged into one; at least 2. Fewer merge when
  # the next would push the result past the page limits.
  fan_in: 8
  min_age_seconds: 30 # Age of a flushed object before its first merge.
  # After a segment ends, merge its remaining small objects regardless of fan_in.
  finalize_after_seconds: 300
  # Replaced objects stay readable this long for in-flight queries and
  # read replicas; keep it above the longest query and replica lag.
  delete_delay_seconds: 600
  max_merges_per_flush: 256

# Bytes of object blocks kept in memory across queries, with their bodies
# decompressed once a query needs them; one budget shared by every storage
# shard of the process; 256 MiB, the default. 0 disables it.
reader_cache_capacity: 268435456

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
  # owner_id: logs-0
  # owners:
  #   - id: logs-0
  #     ordinal: 0
  #     endpoint: logs-0:9091
  #     start_shard: 0
  #     end_shard: 4
  #   - id: logs-1
  #     ordinal: 1
  #     endpoint: logs-1:9091
  #     start_shard: 4
  #     end_shard: 8

  # Kubernetes backend alternative:
  # backend: kubernetes
  # database: logs    # Leases are labeled telemetry.plural.sh/logs=<database>
  # namespace: default
  # stateful_set: logs
  # headless_service: logs-headless
  # owner_port: 9091
  # shard_map: logs-shard-map
  # coordinator_lease: logs-shard-coordinator
  # shard_lease_prefix: logs-shard
  # lease_duration_seconds: 15
  # renew_interval_seconds: 5 # Must be shorter than the lease duration.

request:
  max_request_bytes: 33554432 # Maximum request body as received; 32 MiB.
  max_decoded_request_bytes: 134217728 # Maximum write body after gzip or snappy decoding; 128 MiB. At least max_request_bytes.
  max_query_entries: 5000 # Maximum log entries returned.
  max_query_pages: 10000 # Maximum read units (object block ranges) per query.
  max_structured_metadata_fields: 128 # Per-entry metadata field limit.
  query_concurrency: 16 # Concurrent query work.
  max_in_flight_query_bytes: 134217728 # Query memory budget; 128 MiB.

# Mapping for Elasticsearch _bulk documents; see the Logs APIs page.
# Per-request _msg_field, _time_field, and _stream_fields parameters override it.
elasticsearch:
  message_fields: [message, log, msg] # First present field becomes the log line.
  time_field: "@timestamp" # RFC3339 or epoch millis; missing uses receive time.
  stream_fields: [] # Document fields promoted to stream labels, e.g. kubernetes.namespace_name.

auth:
  unauthenticated: false # Require credentials for namespace APIs.
  internal: { source: env, name: LOGS_INTERNAL_TOKEN } # Writer-to-writer token.
  # Other secret forms:
  # internal: { source: file, path: /var/run/secrets/logs/internal-token }
  # internal: { source: literal, value: development-only }

  # Optional external Bearer JWT verification:
  jwt:
    jwks: { source: url, url: https://issuer.example/.well-known/jwks.json }
    # File alternative:
    # jwks: { source: file, path: /var/run/secrets/logs/jwks.json }
    issuer: https://issuer.example/
    audience: logs
    refresh_interval_seconds: 300
    request_timeout_seconds: 5

  # Basic credentials accepted for every namespace.
  global:
    read:
      - type: basic
        username: global-reader
        password: { source: env, name: LOGS_GLOBAL_READ_PASSWORD }
    write: []

# At least one unique namespace is required. Only listed namespaces are served.
namespaces:
  - name: default
    auth:
      read:
        - type: basic
          username: loki-reader
          password: { source: file, path: /var/run/secrets/logs/read-password }
      write:
        - type: basic
          username: log-writer
          password: { source: env, name: LOGS_WRITE_PASSWORD }
```

The checked-in [`logs.example.yaml`](../../config/logs.example.yaml) is a
runnable local variant of this configuration.
