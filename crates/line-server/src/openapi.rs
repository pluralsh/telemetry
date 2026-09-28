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

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Line HTTP API",
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
        loki_push,
        otlp_logs
    ),
    components(schemas(
        LokiEnvelope,
        ErrorEnvelope,
        BinaryBody,
        JsonBody,
        LokiPush,
        LokiStream
    )),
    tags(
        (name = "query", description = "LogQL query"),
        (name = "ingest", description = "Loki and OTLP ingestion"),
        (name = "operations", description = "Health and readiness")
    )
)]
struct ApiDoc;

pub fn document() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}
