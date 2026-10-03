#![allow(dead_code)]

use utoipa::{OpenApi, ToSchema};

#[derive(ToSchema)]
struct LokiEnvelope {
    status: String,
    #[schema(value_type = Object)]
    data: serde_json::Value,
}

#[derive(ToSchema)]
struct ErrorEnvelope {
    error: String,
}

#[derive(ToSchema)]
#[schema(value_type = String, format = Binary)]
struct BinaryBody(String);

#[derive(ToSchema)]
struct JsonBody {}

#[derive(ToSchema)]
struct LokiPush {
    streams: Vec<LokiStream>,
}

#[derive(ToSchema)]
struct LokiStream {
    /// Stream labels.
    stream: std::collections::BTreeMap<String, String>,
    /// Loki `[timestamp_ns, line, optional_metadata]` tuples.
    #[schema(value_type = Vec<Vec<Object>>)]
    values: Vec<Vec<serde_json::Value>>,
}

#[utoipa::path(
    get,
    path = "/-/healthy",
    tag = "operations",
    responses((status = 200, description = "Process is healthy"))
)]
fn healthy() {}

#[utoipa::path(
    get,
    path = "/-/ready",
    tag = "operations",
    responses(
        (status = 200, description = "Server is ready"),
        (status = 503, description = "Assigned shards are not ready")
    )
)]
fn ready() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/loki/api/v1/query",
    tag = "query",
    params(
        ("namespace" = String, Path, description = "Configured tenant namespace"),
        ("query" = String, Query, description = "LogQL expression"),
        ("time" = Option<String>, Query),
        ("limit" = Option<usize>, Query),
        ("direction" = Option<String>, Query, description = "forward or backward")
    ),
    responses(
        (status = 200, description = "Loki query response", body = LokiEnvelope),
        (status = 400, description = "Invalid query", body = ErrorEnvelope),
        (status = 401, description = "Authentication required"),
        (status = 404, description = "Unknown namespace")
    )
)]
fn query_get() {}

#[utoipa::path(
    post,
    path = "/read/ns/{namespace}/loki/api/v1/query",
    tag = "query",
    params(("namespace" = String, Path)),
    request_body(content = String, content_type = "application/x-www-form-urlencoded"),
    responses((status = 200, description = "Loki query response", body = LokiEnvelope))
)]
fn query_post() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/loki/api/v1/query_range",
    tag = "query",
    params(
        ("namespace" = String, Path),
        ("query" = String, Query, description = "LogQL expression"),
        ("start" = Option<String>, Query),
        ("end" = Option<String>, Query),
        ("since" = Option<String>, Query),
        ("step" = Option<String>, Query),
        ("limit" = Option<usize>, Query),
        ("direction" = Option<String>, Query)
    ),
    responses(
        (status = 200, description = "Loki range-query response", body = LokiEnvelope),
        (status = 400, description = "Invalid query", body = ErrorEnvelope)
    )
)]
fn query_range_get() {}

#[utoipa::path(
    post,
    path = "/read/ns/{namespace}/loki/api/v1/query_range",
    tag = "query",
    params(("namespace" = String, Path)),
    request_body(content = String, content_type = "application/x-www-form-urlencoded"),
    responses((status = 200, description = "Loki range-query response", body = LokiEnvelope))
)]
fn query_range_post() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/loki/api/v1/labels",
    tag = "metadata",
    params(
        ("namespace" = String, Path),
        ("start" = Option<String>, Query, description = "Range start as nanoseconds, seconds, or RFC3339"),
        ("end" = Option<String>, Query, description = "Range end as nanoseconds, seconds, or RFC3339"),
        ("since" = Option<String>, Query, description = "Default lookback when start is omitted")
    ),
    responses(
        (status = 200, description = "Sorted stream-label names", body = LokiEnvelope),
        (status = 400, description = "Invalid time range", body = ErrorEnvelope),
        (status = 401, description = "Authentication required")
    )
)]
fn label_names() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/loki/api/v1/label/{name}/values",
    tag = "metadata",
    params(
        ("namespace" = String, Path),
        ("name" = String, Path, description = "Stream-label name"),
        ("start" = Option<String>, Query),
        ("end" = Option<String>, Query),
        ("since" = Option<String>, Query)
    ),
    responses(
        (status = 200, description = "Sorted values for the stream label", body = LokiEnvelope),
        (status = 400, description = "Invalid time range", body = ErrorEnvelope),
        (status = 401, description = "Authentication required")
    )
)]
fn label_values() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/loki/api/v1/series",
    tag = "metadata",
    params(
        ("namespace" = String, Path),
        ("match[]" = Vec<String>, Query, description = "One or more Loki stream selectors"),
        ("start" = Option<String>, Query),
        ("end" = Option<String>, Query),
        ("since" = Option<String>, Query)
    ),
    responses(
        (status = 200, description = "Matching stream label sets", body = LokiEnvelope),
        (status = 400, description = "Invalid selector or time range", body = ErrorEnvelope),
        (status = 401, description = "Authentication required")
    )
)]
fn series_get() {}

#[utoipa::path(
    post,
    path = "/read/ns/{namespace}/loki/api/v1/series",
    tag = "metadata",
    params(("namespace" = String, Path)),
    request_body(content = String, content_type = "application/x-www-form-urlencoded"),
    responses(
        (status = 200, description = "Matching stream label sets", body = LokiEnvelope),
        (status = 400, description = "Invalid selector or time range", body = ErrorEnvelope),
        (status = 401, description = "Authentication required")
    )
)]
fn series_post() {}

#[utoipa::path(
    post,
    path = "/write/ns/{namespace}/loki/api/v1/push",
    tag = "ingest",
    params(("namespace" = String, Path)),
    request_body(
        content(
            (LokiPush = "application/json"),
            (BinaryBody = "application/x-protobuf")
        ),
        description = "Loki push JSON or Snappy-compressed protobuf."
    ),
    responses(
        (status = 204, description = "Logs accepted"),
        (status = 400, description = "Invalid push request", body = ErrorEnvelope),
        (status = 413, description = "Request exceeds configured limit")
    )
)]
fn loki_push() {}

#[utoipa::path(
    post,
    path = "/write/ns/{namespace}/otlp/v1/logs",
    tag = "ingest",
    params(("namespace" = String, Path)),
    request_body(
        content(
            (JsonBody = "application/json"),
            (BinaryBody = "application/x-protobuf")
        ),
        description = "OTLP ExportLogsServiceRequest."
    ),
    responses(
        (status = 200, description = "OTLP logs accepted"),
        (status = 400, description = "Invalid OTLP request", body = ErrorEnvelope),
        (status = 413, description = "Request exceeds configured limit")
    )
)]
fn otlp_logs() {}

#[derive(ToSchema)]
struct BulkResponse {
    took: u64,
    /// True when at least one item failed.
    errors: bool,
    /// One `{action: {_index, _id, status, ...}}` object per bulk action, in request order.
    #[schema(value_type = Vec<Object>)]
    items: Vec<serde_json::Value>,
}

#[utoipa::path(
    post,
    path = "/write/ns/{namespace}/elasticsearch/_bulk",
    tag = "ingest",
    params(
        ("namespace" = String, Path),
        ("_msg_field" = Option<String>, Query, description = "Comma-separated message fields; overrides configuration"),
        ("_time_field" = Option<String>, Query, description = "Timestamp field; overrides configuration"),
        ("_stream_fields" = Option<String>, Query, description = "Comma-separated document fields promoted to stream labels")
    ),
    request_body(
        content(
            (String = "application/x-ndjson")
        ),
        description = "Elasticsearch bulk NDJSON. `index` and `create` actions are ingested; `update` and `delete` fail per item."
    ),
    responses(
        (status = 200, description = "Per-item bulk results", body = BulkResponse),
        (status = 400, description = "Malformed bulk body", body = ErrorEnvelope),
        (status = 413, description = "Request exceeds configured limit")
    )
)]
fn elasticsearch_bulk() {}

#[utoipa::path(
    post,
    path = "/write/ns/{namespace}/elasticsearch/{index}/_bulk",
    tag = "ingest",
    params(
        ("namespace" = String, Path),
        ("index" = String, Path, description = "Default index for actions without `_index`"),
        ("_msg_field" = Option<String>, Query),
        ("_time_field" = Option<String>, Query),
        ("_stream_fields" = Option<String>, Query)
    ),
    request_body(content(
        (String = "application/x-ndjson")
    )),
    responses(
        (status = 200, description = "Per-item bulk results", body = BulkResponse),
        (status = 400, description = "Malformed bulk body", body = ErrorEnvelope),
        (status = 413, description = "Request exceeds configured limit")
    )
)]
fn elasticsearch_index_bulk() {}

#[utoipa::path(
    get,
    path = "/write/ns/{namespace}/elasticsearch",
    tag = "ingest",
    params(("namespace" = String, Path)),
    responses((status = 200, description = "Elasticsearch version handshake", body = Object))
)]
fn elasticsearch_info() {}

#[utoipa::path(
    get,
    path = "/write/ns/{namespace}/elasticsearch/_cluster/health",
    tag = "ingest",
    params(("namespace" = String, Path)),
    responses((status = 200, description = "Always green", body = Object))
)]
fn elasticsearch_cluster_health() {}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Logs HTTP API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Loki-compatible LogQL query and log ingestion API."
    ),
    paths(
        healthy,
        ready,
        query_get,
        query_post,
        query_range_get,
        query_range_post,
        label_names,
        label_values,
        series_get,
        series_post,
        loki_push,
        otlp_logs,
        elasticsearch_bulk,
        elasticsearch_index_bulk,
        elasticsearch_info,
        elasticsearch_cluster_health
    ),
    components(schemas(
        LokiEnvelope,
        ErrorEnvelope,
        BinaryBody,
        JsonBody,
        LokiPush,
        LokiStream,
        BulkResponse
    )),
    tags(
        (name = "query", description = "LogQL query"),
        (name = "metadata", description = "Stream-label discovery"),
        (name = "ingest", description = "Loki, OTLP, and Elasticsearch bulk ingestion"),
        (name = "operations", description = "Health and readiness")
    )
)]
struct ApiDoc;

pub fn document() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}
