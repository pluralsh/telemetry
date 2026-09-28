# Track APIs

Machine-readable HTTP reference: [OpenAPI 3.1 JSON](../openapi/track.json). The OpenAPI
document covers HTTP; OTLP, Jaeger, and internal gRPC use protobuf service
definitions.

## Ingestion

| Protocol | Address or route | Namespace |
| --- | --- | --- |
| OTLP/HTTP | `POST /write/ns/{namespace}/v1/traces` | URL path |
| OTLP/gRPC TraceService | `listeners.otlp_grpc` (default `4317`) | `x-scope-orgid` metadata |
| Zipkin v2 JSON | `POST /write/ns/{namespace}/api/v2/spans` | URL path |
| Jaeger collector gRPC | `listeners.jaeger_grpc` (default `14250`) | `x-scope-orgid` metadata |

OTLP/HTTP accepts protobuf and JSON according to `Content-Type`. gRPC services
accept Basic or Bearer authorization metadata under the same namespace policy
as HTTP writes.

## Tempo-compatible reads

- `GET /read/ns/{namespace}/api/traces/{trace_id}`
- `GET /read/ns/{namespace}/api/v2/traces/{trace_id}`
- `GET /read/ns/{namespace}/api/search` for TraceQL search
- v1 and v2 tag-name and tag-value discovery routes
- `GET /read/ns/{namespace}/api/echo`

Trace lookup returns JSON by default; request a supported protobuf media type
with `Accept` for protobuf. TraceQL metrics query range is routed but returns
`501 Not Implemented`.

`GET /-/healthy` and `GET /-/ready` are operational endpoints. The internal
gRPC listener (default `9092`) is for shard forwarding.

The current operator-generated Service exposes HTTP and internal gRPC only.
Expose pod ports `4317` and `14250` separately before sending OTLP or Jaeger
traffic through an operator-managed deployment.

Reader mode disables ingestion, writer mode disables reads, and standalone
mode enables both.
