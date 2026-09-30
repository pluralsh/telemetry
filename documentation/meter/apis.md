# Meter APIs

Machine-readable HTTP reference: [OpenAPI 3.1 JSON](../openapi/meter.json).

All data routes are namespace-scoped. If `path_prefix` is configured, prepend
it to the routes below.

## Read APIs

| Method | Route | Purpose |
| --- | --- | --- |
| GET, POST | `/read/ns/{namespace}/api/v1/query` | Instant PromQL query |
| GET, POST | `/read/ns/{namespace}/api/v1/query_range` | Range PromQL query |
| GET, POST | `/read/ns/{namespace}/api/v1/series` | Find matching series |
| GET | `/read/ns/{namespace}/api/v1/labels` | List label names |
| GET | `/read/ns/{namespace}/api/v1/label/{name}/values` | List label values |
| GET | `/read/ns/{namespace}/api/v1/metadata` | Metric metadata |
| GET | `/read/ns/{namespace}/federate` | Prometheus federation |

Prometheus query parameters and response envelopes are used. See
[Prometheus compatibility](prometheus-compatibility.md) for supported PromQL,
parameters, and the remaining gaps.

## Write APIs

| Method | Route | Encoding |
| --- | --- | --- |
| POST | `/write/ns/{namespace}/api/v1/write` | Prometheus remote-write protobuf/snappy |
| POST | `/write/ns/{namespace}/v1/metrics` | OTLP metrics, protobuf or JSON (optionally gzip) |

## Operational and internal APIs

`GET /-/healthy`, `GET /-/ready`, and `GET /metrics` are unprefixed and not
namespace-scoped. Writers also expose an internal gRPC service on
`listeners.grpc`; it is for shard forwarding, not client ingestion.

Reader mode serves reads only, writer mode serves writes and internal gRPC,
and standalone mode serves both.
