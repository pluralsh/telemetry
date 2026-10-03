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
| POST | `/write/ns/{namespace}/elasticsearch/_bulk` | Elasticsearch bulk ingestion |
| POST | `/write/ns/{namespace}/elasticsearch/{index}/_bulk` | Elasticsearch bulk ingestion with a default index |
| GET | `/write/ns/{namespace}/elasticsearch/` | Elasticsearch version handshake |
| GET | `/write/ns/{namespace}/elasticsearch/_cluster/health` | Elasticsearch health handshake |

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

## Elasticsearch bulk

Shippers that speak the Elasticsearch `_bulk` protocol (Fluent Bit, Fluentd,
Vector, Logstash, Filebeat) can point at `/write/ns/{namespace}/elasticsearch`
as their Elasticsearch host. Bodies are NDJSON action/document pairs and may be
gzip-compressed. `index` and `create` actions are ingested; `update` and
`delete` fail per item because logs are append-only. The response follows the
bulk API shape, with `errors: true` and a per-item `400` for documents that
cannot be mapped, so shippers retry or drop only those items. Malformed action
lines fail the whole request with `400`.

Each document becomes one log entry:

- **Timestamp:** the configured time field (default `@timestamp`), as RFC3339
  or epoch milliseconds. Documents without one use the receive time.
- **Line:** the first configured message field present (default `message`,
  `log`, `msg`). Non-string values are JSON-encoded. When no message field is
  present, the whole document is stored as the JSON line.
- **Stream labels:** `index`, with any Logstash date suffix removed
  (`logstash-2024.01.31` becomes `logstash`), plus any configured stream fields.
- **Structured metadata:** all remaining fields, flattened with `_` separators
  (`kubernetes.pod_name` becomes `kubernetes_pod_name`). Arrays are JSON-encoded
  and nulls are dropped.

The query parameters `_msg_field`, `_time_field`, and `_stream_fields`
(comma-separated) override the configured mapping for one request. Promote
only low-cardinality fields such as namespace or app to stream labels.

Fluent Bit:

```ini
[OUTPUT]
    Name               es
    Match              *
    Host               logs.example.com
    Port               8080
    Path               /write/ns/default/elasticsearch
    Logstash_Format    On
    Suppress_Type_Name On
    Compress           gzip
```

Vector:

```yaml
sinks:
  logs:
    type: elasticsearch
    inputs: [kubernetes]
    endpoints: [http://logs.example.com:8080/write/ns/default/elasticsearch]
    api_version: v8
    compression: gzip
    query:
      _stream_fields: kubernetes.pod_namespace
```

Logstash:

```ruby
output {
  elasticsearch {
    hosts => ["http://logs.example.com:8080/write/ns/default/elasticsearch"]
    index => "logstash-%{+YYYY.MM.dd}"
    ilm_enabled => false
    manage_template => false
  }
}
```

`GET /-/healthy` and `GET /-/ready` are operational endpoints. The internal
gRPC listener (default `9091`) carries shard-forwarded writes only.

Authentication is evaluated separately for read and write routes. Reader mode
does not ingest, writer mode does not query, and standalone mode enables both.
