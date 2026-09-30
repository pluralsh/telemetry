#![allow(dead_code)]

use utoipa::{OpenApi, ToSchema};

#[derive(ToSchema)]
struct ApiEnvelope {
    /// Prometheus response status (`success` or `error`).
    status: String,
    #[schema(value_type = Object)]
    data: serde_json::Value,
}

#[derive(ToSchema)]
struct ErrorEnvelope {
    status: String,
    error_type: Option<String>,
    error: String,
}

#[derive(ToSchema)]
#[schema(value_type = String, format = Binary)]
struct BinaryBody(String);

#[derive(ToSchema)]
struct JsonBody {}

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
    path = "/metrics",
    tag = "operations",
    responses((status = 200, description = "Server metrics", body = String, content_type = "text/plain"))
)]
fn metrics() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/v1/query",
    tag = "query",
    params(
        ("namespace" = String, Path, description = "Configured tenant namespace"),
        ("query" = String, Query, description = "PromQL expression"),
        ("time" = Option<String>, Query, description = "Evaluation timestamp: Unix seconds or RFC 3339")
    ),
    responses(
        (status = 200, description = "Prometheus instant-query response", body = ApiEnvelope),
        (status = 400, description = "Invalid query", body = ErrorEnvelope),
        (status = 422, description = "Query failed during evaluation", body = ErrorEnvelope),
        (status = 401, description = "Authentication required"),
        (status = 404, description = "Unknown namespace")
    )
)]
fn query_get() {}

#[utoipa::path(
    post,
    path = "/read/ns/{namespace}/api/v1/query",
    tag = "query",
    params(("namespace" = String, Path, description = "Configured tenant namespace")),
    request_body(content = String, content_type = "application/x-www-form-urlencoded"),
    responses(
        (status = 200, description = "Prometheus instant-query response", body = ApiEnvelope),
        (status = 400, description = "Invalid query", body = ErrorEnvelope),
        (status = 422, description = "Query failed during evaluation", body = ErrorEnvelope)
    )
)]
fn query_post() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/v1/query_range",
    tag = "query",
    params(
        ("namespace" = String, Path),
        ("query" = String, Query, description = "PromQL expression"),
        ("start" = String, Query, description = "Unix seconds or RFC 3339"),
        ("end" = String, Query, description = "Unix seconds or RFC 3339"),
        ("step" = String, Query, description = "Seconds or a Prometheus duration such as `15s`")
    ),
    responses(
        (status = 200, description = "Prometheus range-query response", body = ApiEnvelope),
        (status = 400, description = "Invalid query", body = ErrorEnvelope),
        (status = 422, description = "Query failed during evaluation", body = ErrorEnvelope)
    )
)]
fn query_range_get() {}

#[utoipa::path(
    post,
    path = "/read/ns/{namespace}/api/v1/query_range",
    tag = "query",
    params(("namespace" = String, Path)),
    request_body(content = String, content_type = "application/x-www-form-urlencoded"),
    responses(
        (status = 200, description = "Prometheus range-query response", body = ApiEnvelope),
        (status = 400, description = "Invalid query", body = ErrorEnvelope),
        (status = 422, description = "Query failed during evaluation", body = ErrorEnvelope)
    )
)]
fn query_range_post() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/v1/series",
    tag = "metadata",
    params(
        ("namespace" = String, Path),
        ("match[]" = Vec<String>, Query, description = "Series selectors"),
        ("start" = Option<String>, Query),
        ("end" = Option<String>, Query)
    ),
    responses((status = 200, description = "Matching series", body = ApiEnvelope))
)]
fn series_get() {}

#[utoipa::path(
    post,
    path = "/read/ns/{namespace}/api/v1/series",
    tag = "metadata",
    params(("namespace" = String, Path)),
    request_body(content = String, content_type = "application/x-www-form-urlencoded"),
    responses((status = 200, description = "Matching series", body = ApiEnvelope))
)]
fn series_post() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/v1/labels",
    tag = "metadata",
    params(
        ("namespace" = String, Path),
        ("match[]" = Option<Vec<String>>, Query),
        ("start" = Option<String>, Query),
        ("end" = Option<String>, Query)
    ),
    responses((status = 200, description = "Label names", body = ApiEnvelope))
)]
fn labels() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/v1/label/{name}/values",
    tag = "metadata",
    params(
        ("namespace" = String, Path),
        ("name" = String, Path, description = "Label name"),
        ("match[]" = Option<Vec<String>>, Query),
        ("start" = Option<String>, Query),
        ("end" = Option<String>, Query)
    ),
    responses((status = 200, description = "Label values", body = ApiEnvelope))
)]
fn label_values() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/v1/metadata",
    tag = "metadata",
    params(
        ("namespace" = String, Path),
        ("metric" = Option<String>, Query),
        ("limit" = Option<usize>, Query)
    ),
    responses((status = 200, description = "Metric metadata", body = ApiEnvelope))
)]
fn metadata() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/federate",
    tag = "query",
    params(
        ("namespace" = String, Path),
        ("match[]" = Vec<String>, Query, description = "Series selectors")
    ),
    responses((status = 200, description = "Prometheus text exposition", body = String, content_type = "text/plain"))
)]
fn federate() {}

#[utoipa::path(
    post,
    path = "/write/ns/{namespace}/api/v1/write",
    tag = "ingest",
    params(("namespace" = String, Path)),
    request_body(content = BinaryBody, content_type = "application/x-protobuf", description = "Snappy-compressed Prometheus remote-write request: `prometheus.WriteRequest` (1.0, default) or `io.prometheus.write.v2.Request` when the content type carries `proto=io.prometheus.write.v2.Request`. Both carry native histograms."),
    responses(
        (status = 204, description = "Write accepted; 2.0 requests also receive X-Prometheus-Remote-Write-{Samples,Histograms,Exemplars}-Written headers"),
        (status = 400, description = "Invalid remote-write request", body = ErrorEnvelope),
        (status = 415, description = "Unsupported remote-write protobuf message", body = ErrorEnvelope)
    )
)]
fn remote_write() {}

#[utoipa::path(
    post,
    path = "/write/ns/{namespace}/v1/metrics",
    tag = "ingest",
    params(("namespace" = String, Path)),
    request_body(
        content(
            (BinaryBody = "application/x-protobuf"),
            (JsonBody = "application/json")
        ),
        description = "OTLP ExportMetricsServiceRequest as protobuf or OTLP/JSON, optionally with `Content-Encoding: gzip`."
    ),
    responses(
        (status = 200, description = "OTLP metrics accepted"),
        (status = 400, description = "Invalid OTLP request", body = ErrorEnvelope)
    )
)]
fn otlp_metrics() {}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Meter HTTP API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Prometheus-compatible query and ingestion API. Configure path_prefix to prepend a common prefix to data routes."
    ),
    paths(
        healthy,
        ready,
        metrics,
        query_get,
        query_post,
        query_range_get,
        query_range_post,
        series_get,
        series_post,
        labels,
        label_values,
        metadata,
        federate,
        remote_write,
        otlp_metrics
    ),
    components(schemas(ApiEnvelope, ErrorEnvelope, BinaryBody, JsonBody)),
    tags(
        (name = "query", description = "PromQL and federation"),
        (name = "metadata", description = "Series and label discovery"),
        (name = "ingest", description = "Prometheus and OTLP ingestion"),
        (name = "operations", description = "Health and server telemetry")
    )
)]
struct ApiDoc;

pub fn document() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}
