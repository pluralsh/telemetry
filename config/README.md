# Meter server configuration

`meter-server --config <path>` reads YAML into the configuration below. The default path is
`config/meter.yaml`. Unknown fields are rejected in the server, listener, write, authentication,
JWT, and namespace sections. The checked-in [`meter.example.yaml`](meter.example.yaml) is a valid,
commented standalone configuration with alternatives for every tagged variant.

Defaults apply when a field or section is omitted. Fields described as required must be present
when their containing section or tagged variant is present.

## Server and listeners

- `mode`: server role. Valid values are `standalone` (default), `writer`, and `reader`.
  `standalone` serves reads and writes and must use `sharding.backend: standalone`. `writer`
  exposes external write routes plus the internal writer gRPC service; `reader` exposes only read
  routes. Split writer/reader deployments normally use `static` or `kubernetes` sharding.
- `listeners.http`: HTTP bind socket. Default `0.0.0.0:8080`.
- `listeners.grpc`: internal writer gRPC bind socket. Default `0.0.0.0:9090`.

All tenant APIs are under `/ns/{namespace}`. Read routes are `/api/v1/query`,
`/api/v1/query_range`, `/api/v1/series`, `/api/v1/labels`,
`/api/v1/label/{name}/values`, `/api/v1/metadata`, and `/federate`. Write routes are
`/api/v1/write` (Prometheus remote write) and `/v1/metrics` (OTLP/HTTP protobuf).
`/-/healthy`, `/-/ready`, and `/metrics` are not namespace-prefixed.

## `storage`

Meter always uses SlateDB. If the whole section is omitted, it defaults to `path: data`, an
`InMemory` object store, no settings file, and no caches. If `storage` is present, `path` and
`object_store` are required.

- `storage.path`: object-key prefix for Meter data. Each configured namespace is placed below a
  deterministic `namespace-<blake3>` child, and sharded deployments add a shard-specific suffix.
  Default `data`.
- `storage.settings_path`: optional path to a SlateDB TOML, JSON, or YAML settings file. If
  omitted, SlateDB loads its normal `SlateDb.toml`, `SlateDb.json`, or `SlateDb.yaml` files and
  `SLATEDB_` environment overrides.
- `storage.object_store`: required object-store variant:
  - `type: InMemory`: process-local, nonpersistent storage; no additional fields.
  - `type: Local`: local filesystem storage; requires `path`.
  - `type: Aws`: S3 storage; requires `region` and `bucket`. Credentials and optional endpoint
    settings are resolved by the underlying AWS object-store builder from its environment.
- `storage.block_cache`: optional SlateDB SST data-block cache.
- `storage.meta_cache`: optional SlateDB index/filter/stats cache. Either cache may use either
  cache variant; leaving one side absent disables caching for that block class.

Cache variants are:

- `type: FoyerMemory`
  - `capacity`: required capacity in bytes.
  - `shards`: optional shard count. When absent, Foyer derives it from available CPUs.
- `type: FoyerHybrid`
  - `memory_capacity`: required memory-tier capacity in bytes.
  - `disk_capacity`: required disk-tier capacity in bytes.
  - `disk_path`: required cache directory.
  - `write_policy`: `WriteOnInsertion` (default; send inserted entries to disk) or
    `WriteOnEviction` (send entries to disk when evicted from memory). These values are
    case-sensitive.
  - `flushers`: large-engine flush thread count. Default `4`.
  - `buffer_pool_size`: optional bytes reserved for the large-engine flush pipeline. The effective
    default is `memory_capacity / 32`; each flusher double-buffers, so actual allocation is about
    twice this value.
  - `submit_queue_size_threshold`: queued bytes before cache entries are dropped. Default
    `1073741824` (1 GiB).

`reader_cache_capacity` controls each shard reader's Moka cache of loaded time-bucket query
readers. The current implementation does not install a weight function, so the unit is entries,
not bytes. Default `268435456`.

## `write`

- `write.durability`: acknowledgement guarantee. Valid values are `applied`, `written`, and
  `durable`; default `written`.
  - `applied`: accepted into the in-memory delta, but not necessarily visible to snapshot-backed
    queries.
  - `written`: moved into SlateDB mutable state and visible to a fresh snapshot, but not yet
    guaranteed across restart.
  - `durable`: flushed to the configured object store before acknowledgement.
- `write.flush_interval_seconds`: seconds between durable flushes of active writers. Default `60`;
  `0` disables the periodic task. This bounds persistence and split-reader visibility for
  `applied` and `written` requests. Shutdown also flushes open writers.
- `write.remote_concurrency`: maximum concurrent shard batches forwarded to owners. Default `16`.
- `write.remote_retries`: retries after the initial internal gRPC attempt when ownership
  generation is stale. Default `2`.

## `sharding`

- `sharding.virtual_shards`: number of deterministic virtual shards. Default `64`; must be greater
  than zero. Keep it identical across all processes sharing a dataset.
- `sharding.backend`: `standalone` (default), `static`, or `kubernetes`.

`standalone` has no additional fields and assigns every shard to the process.

`static` requires:

- `owner_id`: this process's owner ID; it must match an `owners[].id`.
- `owners`: fixed owner list. Every entry requires `id`, numeric `ordinal`, internal gRPC
  `endpoint`, and the half-open range `[start_shard, end_shard)`. Ranges must be nonempty,
  contiguous, nonoverlapping, start at zero, and exactly cover `virtual_shards`.

`kubernetes` discovers writer membership from a StatefulSet, stores assignments in a ConfigMap,
and uses Leases for coordinator and shard ownership. The server must be built with the
`kubernetes` feature (enabled by default) and have namespace-scoped RBAC. Fields are:

- `namespace`: Kubernetes namespace. Default `default`.
- `stateful_set`: writer StatefulSet name. Default `meter`.
- `headless_service`: writer headless Service used for owner DNS. Default `meter-headless`.
- `owner_port`: internal gRPC port. Default `9090`.
- `assignment_config_map`: assignment ConfigMap name. Default `meter-shard-assignments`.
- `coordinator_lease`: coordinator Lease name. Default `meter-shard-coordinator`.
- `shard_lease_prefix`: prefix for per-shard Lease names. Default `meter-shard`.
- `lease_duration_seconds`: shard/coordinator lease duration in seconds. Default `15`.
- `renew_interval_seconds`: shard ownership renewal interval in seconds. Default `5`.
- `watch_poll_interval_seconds`: assignment-watch fallback poll interval in seconds. Default `2`.

The local Kubernetes owner ID is `POD_NAME` when set, otherwise
`<stateful_set>-${POD_ORDINAL:-0}`. The coordinator balances contiguous shard ranges as StatefulSet
membership changes. Readers open all virtual shards; writers open only currently owned shards.

## Secrets and authentication

A secret value supports exactly one source:

```yaml
{ source: literal, value: development-only }
{ source: env, name: METER_PASSWORD }
{ source: file, path: /var/run/secrets/meter/password }
```

`env` reads the named environment variable and `file` reads UTF-8 text from the path. Trailing
CR/LF characters are removed; an empty result is rejected. Secret values are redacted from debug
output. Prefer environment or read-only mounted files in production.

`auth.global.read` and `auth.global.write` are credential lists accepted for every namespace.
Each `namespaces[].auth.read` and `.write` list adds credentials for that namespace and permission.
The only configured credential type is HTTP Basic:

```yaml
- type: basic
  username: prometheus
  password: { source: file, path: /var/run/secrets/meter/password }
```

Global and namespace Basic credentials are alternatives, not cumulative requirements. If a
permission has no Basic credentials and JWT is disabled, that operation is anonymous. Once JWT is
enabled, a request must present either an applicable Basic credential or a valid Bearer JWT.

`auth.internal` is an optional secret used only for writer-to-writer gRPC. When set, callers send
and owners require `authorization: Bearer <token>`. Configure the same value on every writer and
reader that may forward writes. This token does not authorize external HTTP routes. When omitted,
the internal gRPC method has no token check.

## External JWT/JWKS

`auth.jwt` enables Bearer JWT verification for every namespace:

- `jwks`: required source:
  - `{ source: file, path: ... }`: read once during startup. The path must be nonempty; file
    changes are not reloaded.
  - `{ source: url, url: ... }`: fetched during startup and then refreshed lazily by requests after
    `refresh_interval_seconds`. A token with an unknown `kid` triggers an immediate refresh,
    rate-limited to the same interval. Failed refreshes keep the last valid key set.
- `issuer`: optional exact `iss` requirement. When set, the claim is required.
- `audience`: optional `aud` requirement. When absent, audience validation is disabled.
- `refresh_interval_seconds`: URL refresh and unknown-key retry interval. Default `300`; must be
  greater than zero.
- `request_timeout_seconds`: HTTP timeout for URL retrieval. Default `5`; must be greater than
  zero.

The JWKS must contain a nonempty `keys` array. Every key needs a unique nonempty `kid`, a supported
`alg`, and valid JWK key material. Supported algorithms are `HS256`, `HS384`, `HS512`, `ES256`,
`ES384`, `RS256`, `RS384`, `RS512`, `PS256`, `PS384`, `PS512`, and `EdDSA`. The JWT header must
contain a matching `kid` and `alg`.

JWT claims:

- `exp`: required expiration timestamp and validated.
- `nbf`: optional not-before timestamp and validated when present.
- `namespace`: required Rust regular expression matched against the requested configured namespace.
  Use anchors such as `^tenant-a$` for an exact match.
- `permission`: required exact value `read` or `write`.
- `iss` and `aud`: required and validated only when their corresponding configuration fields are
  set.

## `namespaces`

`namespaces` defaults to one unauthenticated namespace named `default` and must not be empty.
Entries require a unique `name`; names must be 1–255 bytes and cannot contain NUL or ASCII control
characters. Only listed namespaces are opened. Requests for any other name return `404`.

Each namespace has isolated storage and query state. Its APIs use the literal configured name in
`/ns/{namespace}`, while its storage prefix uses the deterministic namespace hash described above.
Authentication is evaluated separately for read and write access.
