# Logs APIs

Machine-readable HTTP reference: [OpenAPI 3.1 JSON](../openapi/logs.json).

Logs exposes a focused Loki-compatible HTTP surface.

| Method | Route | Purpose |
| --- | --- | --- |
| GET, POST | `/read/ns/{namespace}/loki/api/v1/query` | Instant LogQL query |
| GET, POST | `/read/ns/{namespace}/loki/api/v1/query_range` | Range LogQL query |
| GET | `/read/ns/{namespace}/loki/api/v1/labels` | Stream-label names |
| GET | `/read/ns/{namespace}/loki/api/v1/label/{name}/values` | Stream-label values |
| GET, POST | `/read/ns/{namespace}/loki/api/v1/series` | Matching stream label sets |
| POST | `/write/ns/{namespace}/loki/api/v1/push` | Loki push ingestion |
| POST | `/write/ns/{namespace}/otlp/v1/logs` | OTLP log ingestion |

Query endpoints accept Loki-style `query`, time/range, `limit`, and `direction`
parameters and return Loki response envelopes. The implementation supports log
stream selection and the implemented LogQL pipeline; it is not a blanket
promise that every Loki endpoint or LogQL feature exists.

Metadata endpoints accept Loki-style `start`, `end`, and `since` parameters.
`series` requires at least one `match[]` stream selector; POST requests use
`application/x-www-form-urlencoded`. Results include stream labels only:
per-entry structured metadata is intentionally excluded. Label names and
values are read from the durable per-segment discovery catalog, while series
are reconstructed from label postings and forward-label records.

Loki push accepts the supported protobuf/snappy and JSON forms. OTLP uses the
OTLP log request encoding selected by `Content-Type`; compressed requests honor
supported `Content-Encoding` values.

`GET /-/healthy` and `GET /-/ready` are operational endpoints. The internal
gRPC listener (default `9091`) carries shard-forwarded writes only.

Authentication is evaluated separately for read and write routes. Reader mode
does not ingest, writer mode does not query, and standalone mode enables both.
