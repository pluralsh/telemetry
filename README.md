# Telemetry

Telemetry is a Rust workspace for observability products. Meter, the metrics product, is
implemented and deployable. Loom and Thread are currently minimal placeholders reserved for logs
and traces.

## Meter status and architecture

Meter is a namespace-isolated metrics service backed by SlateDB. Its current server combines:

- Prometheus remote-write ingestion and OTLP/HTTP protobuf metrics ingestion.
- Prometheus-style instant/range PromQL, series and label discovery, metadata, and federation
  HTTP APIs.
- Deterministic virtual sharding with standalone, fixed static, and Kubernetes-managed ownership.
- Separate scalable writer and global-reader roles over shared object storage. A reader opens all
  shard readers and plans a query globally once.
- Local filesystem, in-memory, and S3 object stores, with optional Foyer memory or hybrid
  memory/disk caches.
- Configurable write acknowledgement through applied, written, and object-store-durable stages,
  plus periodic durable flushing.
- Per-namespace and global HTTP Basic credentials, external JWT authorization, and an independent
  internal writer-to-writer gRPC token.

Workspace crates:

- `common`: byte encoding, SlateDB factories/configuration, and write coordination.
- `proto`: internal tonic/prost writer API.
- `sharding`: virtual-shard planning, assignment, Kubernetes coordination, leasing, and ownership.
- `meter`: namespace-aware TSDB, ingestion conversion, and sharded query engine.
- `meter-server`: Axum/tonic server and `meter-server` binary.
- `regression`: black-box Prometheus comparison and deployment regression runner.
- `loom` / `thread`: future logs/traces placeholders.

Meter targets the API surface listed below; it does not claim complete Prometheus server or PromQL
compatibility.

## Namespaces and routes

Only namespaces declared in configuration are opened, with separate storage and query state.
Read routes are rooted at `/read/ns/{namespace}` and write routes at
`/write/ns/{namespace}`:

- Writes: `/write/ns/{namespace}/api/v1/write` and
  `/write/ns/{namespace}/v1/metrics`.
- Queries: `/read/ns/{namespace}/api/v1/query` and
  `/read/ns/{namespace}/api/v1/query_range`.
- Discovery/metadata: below `/read/ns/{namespace}` at `/api/v1/series`, `/api/v1/labels`,
  `/api/v1/label/{name}/values`, `/api/v1/metadata`, and `/federate`.

An optional `path_prefix`, such as `/meter`, can scope both public route trees. `/-/healthy`,
`/-/ready`, and `/metrics` remain unprefixed. Writer mode installs only write routes; reader mode
installs only read routes; standalone installs both.

## Run and configure

The complete field-by-field YAML reference, defaults, units, variants, mode constraints, and
authentication behavior is in [config/README.md](config/README.md). Start from the fully commented
[config/meter.example.yaml](config/meter.example.yaml):

```sh
cp config/meter.example.yaml config/meter.yaml
mise exec -- cargo run --package meter-server -- --config config/meter.yaml
```

Use environment variables or mounted files for production secrets. The
`crates/meter-server/Dockerfile` runtime runs `meter-server`; mount configuration at
`/app/config/meter.yaml`.

Deployment roles:

- `standalone`: one process owns all virtual shards and serves reads and writes. Multiple
  standalone processes must not concurrently open the same dataset.
- `writer`: serves ingestion and internal gRPC, opening only locally owned shards.
- `reader`: serves query/read APIs and opens every shard read-only without fencing writers.

Static sharding uses an identical fixed owner map on every process. Kubernetes sharding discovers
writer StatefulSet members, publishes assignments through a ConfigMap, and protects coordinator
and shard ownership with Leases. Split deployments require object storage shared by all writers
and readers.

## Helm

Install the Kubebuilder operator, including its `Meter` and `NamespaceAuthentication` CRDs:

```sh
helm upgrade --install telemetry-operator oci://ghcr.io/pluralsh/charts/telemetry-operator \
  --version 0.1.0 \
  --namespace telemetry-system \
  --create-namespace
```

Then apply a `Meter` and, optionally, namespace-scoped Basic authentication:

```sh
kubectl apply -f go/operator/config/samples/telemetry_v1alpha1_meter.yaml
kubectl create secret generic prometheus-basic-auth --from-literal=password=change-me
kubectl apply -f go/operator/config/samples/telemetry_v1alpha1_namespaceauthentication.yaml
```

See the [operator chart documentation](chart/telemetry-operator/README.md) for image and RBAC
settings. The direct Meter chart supports one-pod `standalone` and Kubernetes-coordinated
`sharded` topologies:

```sh
helm install meter oci://ghcr.io/pluralsh/charts/meter \
  --version 0.1.0 --namespace telemetry --create-namespace
helm install meter oci://ghcr.io/pluralsh/charts/meter \
  --version 0.1.0 --namespace telemetry --create-namespace \
  --set mode=sharded
```

For sharded use, configure a shared object store and the internal token. The chart generates
role-specific Meter YAML, Services, StatefulSets, and namespace-scoped RBAC. See
[chart/meter/README.md](chart/meter/README.md) for values, storage caveats, and secure secret/JWKS
mounting.

## Authentication

HTTP Basic credentials can be global or namespace-specific and separately scoped to reads and
writes. Bearer credentials are JWTs verified against a configured JWKS file or URL; tokens require
expiration, namespace-regex, and `read`/`write` permission claims. Optional issuer and audience
checks are supported. URL key sets refresh periodically and on unknown key IDs; file key sets are
loaded at startup. See the [configuration reference](config/README.md#external-jwtjwks) for exact
claim, algorithm, and refresh semantics.

## Development and verification

Rust and Go 1.27 are managed by [mise](https://mise.jdx.dev/) using `mise.toml`.

```sh
mise install
mise exec -- cargo fmt --all --check
mise exec -- cargo check --workspace --all-targets --all-features --locked
mise exec -- cargo test --workspace --all-features --locked
mise exec -- cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

The operator lives in `go/operator` and follows the standard Kubebuilder workflow. Generate
DeepCopy code and CRDs, format and vet Go, and run focused unit plus envtest controller tests with:

```sh
mise exec -- sh -c 'cd go/operator && make generate manifests crd-docs fmt vet test'
```

`make test` downloads matching envtest control-plane binaries and excludes the generated Kind e2e
package. After changing API markers, copy the generated CRDs from
`go/operator/config/crd/bases/` to `chart/telemetry-operator/crds/`, keeping them byte-identical.
The generated CRD API reference is written to
[`go/operator/docs/api.md`](go/operator/docs/api.md).
The sample [Meter](go/operator/config/samples/telemetry_v1alpha1_meter.yaml) and
[NamespaceAuthentication](go/operator/config/samples/telemetry_v1alpha1_namespaceauthentication.yaml)
resources are useful starting points.

CI runs formatting, workspace checks, Clippy, all-feature unit/integration tests, and the Docker
regression suite, plus operator generation, manifests, formatting, vet, envtest, and Helm chart
checks. `tests/regression/test.sh` compares deterministic results with pinned Prometheus across
tested instant/range selectors, regex matching, aggregation, rate, offsets, binary joins,
discovery, metadata, boundary/empty behavior, OTLP conversion, namespace isolation, Basic/JWT
authorization, forwarded-write idempotency, read-only route rejection, and reader freshness.
See [tests/regression/README.md](tests/regression/README.md).

`tests/kind/test.sh` is an optional, manually dispatched kind scenario covering lease-backed
assignment, authenticated forwarding, writer scaling `1 -> 3 -> 1`, and reads after shard handoff.
It is intentionally outside normal Cargo and pull-request CI. Current black-box coverage is a
focused compatibility subset, not an exhaustive Prometheus conformance suite or long-duration
performance/chaos test.

## Attribution

Parts of `common` are adapted from OpenData. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
