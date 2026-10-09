# Plural Telemetry

Plural Telemetry is a Rust re-implementation of common observability products, in particular modeled after open source stores like Prometheus, Loki and Tempo for the main observability pillars of logs, metrics and traces.

All data stores are built with s3 as the backend storage, using the [slatedb](https://slatedb.io/) project as its ultimate WAL + LSM tree implementation

The project breakdown is:

1. Metrics - Prometheus compatible datastore with built-in OTLP ingest as well as remote write
2. Logs - Loki-compatible log store
3. Traces - Tempo-compatible trace store
4. PseudoFS - gRPC virtual filesystem for embedded language runtimes

Storage formats, indexing, APIs, configuration, sharding, and authentication
are documented in the [technical documentation](documentation/README.md).

We might add other interesting slatedb + rust projects in here as well, but they'll all be datastore focused as a core guiding principle.  Many of these are also inspired or utilize implementations from the [Opendata](https://www.opendata.dev/) project to bootstrap the implementation.

## Productionization

There are a few things we've explicitly added to enhance slatedb and make these datastores ready for real use:

1. Sharding - slatedb is single writer, multi-reader as a core design constraint.  Since this is observability focused, we want to be able to solve for multi-writer as a core need.  More documentation below.
2. Multi-tenancy - simple namespace path multi-tenancy allows you to share the same db across overlapping metrics datasets with minimal configuration overhead.
3. Authentication - common limitation of a lot of observability dbs, and pairs with multitenancy. Both basic auth and JWKS-based RSA signed JWT is supported.

## Deployment

Deployment is explicitly meant to be kubernetes based, since the sharding implementation leverages the k8s api.  We provide a full operator that configures each datastore in the two main deployment modes:

* standalone - single node reader + writer
* sharded - configurable scale out readers and writers

The operator manages annoyances like configuration setup, scaling, and pvc resizing - slatedb supports NVME-based caching that is seamlessly configurable via statefulsets.

See [Operator Docs](go/operator/docs/api.md) for full API documentation.

## Testing Strategy

In addition to robust unit tests, we implement an oracle based testing strategy against reference implementations.  Each of Metrics, Logs, and Traces are tested against their peer, prometheus, loki and mimir. Those test suites will grow in time but include basic query behavior, ingestion logic, and more.

### Differential fuzzing and performance

`tests/regression/harness/fuzz` generates random data and queries, writes the same data to each product and its peer, and compares every response. It records the latency of every request and samples the CPU and memory of every container. The numbers below come from one run per peer, all with seed `3887071361`. Every remaining mismatch traces to one of the differences listed below, one of which is a bug on our side.

#### Query latency

Latency of every answered query, in milliseconds, with the peer's figure first:

| product | peer | cases | p50 | p90 | p99 | p99.9 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Traces | Tempo 2.10.8, S3 backend | 5,836 | 20.9 / **3.9** | 61.3 / **22.2** | 159.8 / **66.6** | 611.3 / **310.7** |
| Logs | Loki 3.5.5, S3 backend | 6,035 | 9.2 / **6.1** | 70.7 / **44.8** | 497.6 / **216.4** | 1,584.2 / **872.5** |
| Metrics | Mimir 3.2.1, blocks via store-gateway | 1,658 | 4.6 / **1.8** | 16.8 / **4.5** | 81.4 / **22.1** | 448.4 / **83.0** |
| Metrics | Prometheus 3.14.0, local TSDB | 5,413 | 1.2 / **3.1** | 5.0 / **10.6** | 35.8 / **32.0** | 86.5 / **125.6** |

Median write latency, peer first: Traces 3.7 / 20.5 ms, Logs 4.3 / 2.6 ms, Metrics against Mimir 3.7 / 21.0 ms, Metrics against Prometheus 1.9 / 19.2 ms. Traces and Metrics writes use `durability: durable`, so they wait for the object-store upload before answering.

By query family:

- **Traces:** trace-by-id lookups take a median 1.9 ms against Tempo's 22.1 ms. TraceQL queries take 4.7–6.0 ms against Tempo's 17.6–25.5 ms.
- **Logs:** we are faster at every percentile. Per query, the median ratio of our latency to Loki's is 0.63x. About 1.8% of queries take more than twice as long as Loki's. Most of those are slower by under 100 ms, and all but two were fast again when rechecked. We read recent data through the object-store-backed block cache, while Loki serves it from ingester memory, so an occasional cache miss costs us milliseconds Loki does not pay. The two that stayed slow are a range aggregation that runs `logfmt`, a duration filter and `unwrap` over every line (about 7x Loki's latency), and a negated regex line filter followed by a substring filter (about 3.5x).
- **Metrics:** against Mimir reading object-storage blocks, the closest like-for-like comparison, we are 2.5–5x faster at every percentile. Prometheus answers these small queries from local memory and disk in about 1 ms, while every one of our reads goes through the object-store-backed reader, so its median is about 2.5x lower. Its tail is similar to ours.

#### CPU and memory

The harness reads each container's cumulative CPU time and working-set memory from the Docker Engine API every 2 seconds. CPU is total CPU-seconds over the run. Memory is the peak, summed across every container on that side at each sample. Our Traces and Metrics deployments are three processes (two writers and a reader); every peer is one process. MinIO stands in for cloud object storage and is excluded from both sides.

| product | peer | run | peer / ours CPU-s | peer / ours mean cores | peer / ours peak MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| Traces | Tempo | 15m | 373.6 / **79.5** | 0.42 / **0.09** | 585 / **169** |
| Logs | Loki | 15m | 314.5 / **209.6** | 0.36 / **0.24** | 1,477 / **564** |
| Metrics | Mimir | 15m | 568.1 / **31.0** | 0.63 / **0.03** | 621 / **76** |
| Metrics | Prometheus | 10m | 31.4 / **53.9** | 0.06 / **0.09** | 165 / **235** |

- **Traces:** about a fifth of Tempo's CPU and under a third of its peak memory.
- **Logs:** two-thirds of Loki's CPU and under 40% of its peak memory. Loki's memory also swings far more: its p95 is 991 MiB against our 555 MiB.
- **Metrics:** against Mimir, which runs every component (`-target=all`), we use about 1/18 of its CPU and an eighth of its peak memory.
- **Metrics against Prometheus:** Prometheus is a single process on local disk, so it uses less CPU and memory than our three processes. The reader accounts for most of our memory (198 MiB at peak), much of it its configured caches.

Peak memory includes the configured caches: 64 MiB block and 16 MiB meta caches per process, plus a 64 MiB reader cache per Metrics process. Each run writes its samples to `resources.jsonl` and summarizes them in `summary.md`. Set `FUZZ_RESOURCES=0` to turn sampling off.


## Sharding

Sharding is implemented on top of Kubernetes for coordination. Since all telemetry data is ultimately time index, we leverage temporal ordering to implement epoch based sharding, diagrammed below:

```text
                       record time ──────────────────────────────────────────►

                 epoch 0 (2 shards)          │ epoch 1 (4 shards)
                 effective_from = t0         │ effective_from = t1 (aligned hour)
   hash space    ┌────────────────────────┐  │  ┌────────────────────────┐
   (BLAKE3,      │ shard 0  [0x0.., 0x8..)│  │  │ shard 0  [0x0.., 0x4..)│
    128-bit)     │                        │  │  ├────────────────────────┤
                 │                        │  │  │ shard 1  [0x4.., 0x8..)│
                 ├────────────────────────┤  │  ├────────────────────────┤
                 │ shard 1  [0x8.., 0xf..]│  │  │ shard 2  [0x8.., 0xc..)│  new, empty
                 │                        │  │  ├────────────────────────┤  SlateDB dbs
                 │                        │  │  │ shard 3  [0xc.., 0xf..]│
                 └────────────────────────┘  │  └────────────────────────┘
                                             │
                 old data never moves;       │  late records with t < t1 still
                 still readable              │  route by epoch 0

  write path
  ──────────
  record ─► (namespace + labels | trace ID, timestamp)
         ─► epoch = last epoch with effective_from <= timestamp
         ─► shard = epoch.range_for(blake3_128(key))
         ─► owner of shard (Lease holder)
              ├── this writer ──────────────► SlateDB put
              └── another writer ─► gRPC ──► SlateDB put   (retry on stale owner)

  read path
  ─────────
  query ─► open shards [0, shard_count) ─► per-shard results ─► merge / dedupe

  scale-up (writer replicas 2 ─► 4)
  ─────────────────────────────────
  operator          adds StatefulSet pods writer-2, writer-3
  coordinator       (Lease-elected) appends epoch 1 to the ShardMap CR via
                    resourceVersion CAS, effective at the next aligned boundary
                    at least lead time ahead (default: 1h alignment, 2m lead)
  writers/readers   watch ShardMap, apply only increasing generations,
                    acquire Leases for their assigned shards
  cutover at t1     new records route by epoch 1; no data copied or drained
```

A single versioned assignment snapshot carries time-based routing epochs (each a 128-bit hash-range map) plus storage shard ownership. Scaling writers up publishes a new routing
epoch that cuts over at the next aligned boundary and routes new data to new,
empty storage shards; existing data stays where it was written and readers merge
across shards. Scale-down is not supported yet. See
[Sharding](documentation/common/sharding.md).

We utilize a few k8s api primitives to do this:

1. Statefulset durable naming - this allows us to ensure writers have consistent network identities across scaling decisions.
2. A ShardMap custom resource as the source of truth for routing epochs and shard range assignments.
3. Leases for ownership of physical shards.

K8s effectively provides an already CP datastore to manage that minimal configuration, and removes the additional need to provide a zookeeper or etcd store.  It's also a ubiquitous deployment pattern for hosted, third-party software, so effectively allows us to provide that guarantee with no net new dependencies.

## Installation

Install the telemetry-operator operator, including the `Metrics`, `Logs`, and
`NamespaceAuthentication` CRDs:

```sh
helm upgrade --install telemetry-operator oci://ghcr.io/pluralsh/charts/telemetry-operator \
  --version 0.1.18 \
  --namespace telemetry-system \
  --create-namespace
```

Create a `Metrics` instance. This example uses S3-compatible object storage, so
the referenced `metrics-s3` Secret must exist in the same namespace:

```yaml
apiVersion: telemetry.plural.sh/v1alpha1
kind: Metrics
metadata:
  name: metrics-sample
spec:
  mode: Sharded
  # version: 0.2.2 # you can float versions by leaving them unspecified
  config:
    # retention: 30d # w, d, h, m, s units; unset keeps data forever
    storage:
      path: metrics
      objectStore:
        type: Aws
        aws:
          region: us-east-1
          bucket: metrics
          accessKeyIDSecretRef:
            name: metrics-s3
            key: access-key-id
          secretAccessKeySecretRef:
            name: metrics-s3
            key: secret-access-key
    namespaces:
      - default
  ingress:
    enabled: true
    hostname: metrics.example.com
    ingressClass: nginx
    pathPrefix: /metrics
    tls:
      enabled: true
      secretName: metrics-sample-tls
  writer:
    replicas: 3
    dataVolume:
      persistentVolumeClaim:
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 10Gi
    cacheVolume:
      persistentVolumeClaim:
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 20Gi
  reader:
    replicas: 2
    dataVolume:
      persistentVolumeClaim:
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 10Gi
    cacheVolume:
      persistentVolumeClaim:
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 20Gi
```

Create a standalone `Logs` instance:

```yaml
apiVersion: telemetry.plural.sh/v1alpha1
kind: Logs
metadata:
  name: logs-sample
spec:
  mode: Standalone
  # version: 0.2.2
  config:
    # retention: 14d
    namespaces:
      - default
    storage:
      objectStore:
        type: Local
  ingress:
    enabled: false
  writer:
    replicas: 1
    dataVolume:
      persistentVolumeClaim:
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 10Gi
```

Authentication is configured per datastore namespace. The following resources
grant read access to the `default` namespace using passwords stored in
Kubernetes Secrets:

```yaml
apiVersion: telemetry.plural.sh/v1alpha1
kind: NamespaceAuthentication
metadata:
  name: prometheus-reader
spec:
  dataStoreRef:
    kind: Metrics
    name: metrics-sample
  namespace: default
  username: prometheus
  permission: read
  secretKeyRef:
    name: prometheus-basic-auth
    key: password
---
apiVersion: telemetry.plural.sh/v1alpha1
kind: NamespaceAuthentication
metadata:
  name: loki-reader
spec:
  dataStoreRef:
    kind: Logs
    name: logs-sample
  namespace: default
  username: loki
  permission: read
  secretKeyRef:
    name: loki-basic-auth
    key: password
```

## Known differences from the peers

The remaining non-matching cases come from these differences. The harness reports most of them as inconclusive; the rest are listed in each run's summary:

- **Tempo:** the tag-names API leaves out the span attribute `http.route` for backend blocks, even though searches on it match.
- **Loki:** `rate_counter` follows the units fix in grafana/loki#23684, which no Loki release up to and including v3.7.8 has.
- **Prometheus `topk`/`bottomk`:** when values tie, which series wins is unspecified, so tied results are compared as sorted values.
- **arm64 rounding:** the arm64 Go builds of the peers fuse multiply-adds, which moves `quantile` interpolation and `round(v, to_nearest)` by one unit in the last place. The amd64 builds match our results.
- **Metric names:** Metrics keeps `__name__` through some functions that Prometheus strips it from.
- **Mimir many-to-many errors:** Mimir skips some many-to-many matching errors that Prometheus and our implementation raise.
- **Mimir `absent_over_time` labels:** when a label has both an equality matcher and another matcher, Prometheus and our implementation drop it from the result. Mimir's query engine keeps it.
- **Mimir `bottomk` with NaN:** Mimir can select a NaN series where Prometheus's ordering, and ours, prefers a real value.
- **`exp` rounding:** for very large inputs, `exp` can differ from Prometheus in the last digit, which shows up in `count_values` labels.
- **Negative zero (our bug):** Metrics does not yet preserve the sign of a `-0` sample, so `1 / x` returns `+Inf` where Prometheus returns `-Inf`.

## Local Development

Install the pinned Go, Kubebuilder, Python, and Rust toolchains with
[mise](https://mise.jdx.dev/):

```sh
mise install
```

Run the basic Rust checks through mise:

```sh
mise exec -- cargo fmt --all --check
mise exec -- cargo check --workspace --all-targets --all-features --locked
mise exec -- cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
mise exec -- cargo test --workspace --all-features --locked
```

## Regression Tests

Install the regression harness dependencies after running the mise setup above:

```sh
mise exec -- python -m pip install -r tests/regression/requirements.txt
```

Run the fast, self-contained regression unit tests:

```sh
mise exec -- python -m pytest tests/regression/test_unit
```

Run the live compatibility suites, which require Docker and Docker Compose:

```sh
mise exec -- python -m pytest tests/regression/test_live
```

The extended live suite adds slower restart and retention coverage:

```sh
mise exec -- python -m pytest tests/regression/test_live --extended
```

The optional Kubernetes handoff regression requires Docker and
[kind](https://kind.sigs.k8s.io/):

```sh
mise exec -- tests/kind/test.sh
```

See [the regression harness documentation](tests/regression/README.md) for
suite coverage, ports, and troubleshooting details.

## Attribution

Parts of `common` are adapted from OpenData. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
