# Technical documentation

Plural Telemetry provides object-store-backed databases and related services:

- [Metrics](metrics/): Prometheus-compatible metrics.
- [Logs](logs/): Loki-compatible logs.
- [Traces](traces/): Tempo-compatible traces.
- [PseudoFS](pseudofs/): a gRPC virtual filesystem for embedded runtimes.

Each database section describes its storage format, index strategy, public APIs,
and server configuration. Cross-cutting behavior lives under [common](common/):

- [Sharding](common/sharding.md)
- [Capacity planning and scaling](common/scaling.md)
- [Authentication](common/authentication.md)

These pages describe the implementation and operationally important limits.
For Kubernetes CRD fields, also see the generated
[operator API reference](../go/operator/docs/api.md). Complete runnable server
examples live in [`config/`](../config/).

## Generated OpenAPI

Each server owns an OpenAPI 3.1 description generated with Utoipa. Regenerate
the checked-in JSON documents after changing an HTTP route:

- [Metrics](openapi/metrics.json)
- [Logs](openapi/logs.json)
- [Traces](openapi/traces.json)

```sh
cargo run -p api-docs
```

CI-style drift check:

```sh
cargo run -p api-docs -- --check
```

OpenAPI covers HTTP APIs. Internal writer, OTLP, and Jaeger gRPC services remain
described in the product API pages and their protobuf definitions.
