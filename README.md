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
  --version 0.1.0 \
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
  version: 0.1.0
  config:
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
  version: 0.1.0
  config:
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

## Testing Strategy

In addition to robust unit tests, we implement an oracle based testing strategy against reference implementations.  Each of Metrics, Logs, and Traces are tested against their peer, prometheus, loki and mimir. Those test suites will grow in time but include basic query behavior, ingestion logic, and more.

Performance and scalability tests are to be implemented in time.

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
