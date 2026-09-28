#![allow(dead_code)]

use std::collections::BTreeMap;

use utoipa::{OpenApi, ToSchema};

#[derive(ToSchema)]
struct ErrorEnvelope {
    error: String,
}

#[derive(ToSchema)]
struct TraceResponse {
    #[schema(value_type = Object)]
    batches: serde_json::Value,
}

#[derive(ToSchema)]
struct SearchResponse {
    #[schema(value_type = Vec<Object>)]
    traces: Vec<serde_json::Value>,
}

#[derive(ToSchema)]
#[schema(value_type = String, format = Binary)]
struct BinaryBody(String);

#[derive(ToSchema)]
struct JsonBody {}

#[derive(ToSchema)]
#[schema(rename_all = "camelCase")]
struct ZipkinSpan {
    trace_id: String,
    id: String,
    parent_id: Option<String>,
    name: Option<String>,
    timestamp: Option<u64>,
    duration: Option<u64>,
    local_endpoint: Option<ZipkinEndpoint>,
    remote_endpoint: Option<ZipkinEndpoint>,
    tags: Option<BTreeMap<String, String>>,
    annotations: Option<Vec<ZipkinAnnotation>>,
}

#[derive(ToSchema)]
#[schema(rename_all = "camelCase")]
struct ZipkinEndpoint {
    service_name: Option<String>,
    ipv4: Option<String>,
    ipv6: Option<String>,
    port: Option<u16>,
}

#[derive(ToSchema)]
struct ZipkinAnnotation {
    timestamp: u64,
    value: String,
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
    post,
    path = "/write/ns/{namespace}/v1/traces",
    tag = "ingest",
    params(("namespace" = String, Path, description = "Configured tenant namespace")),
    request_body(
        content(
            (JsonBody = "application/json"),
            (BinaryBody = "application/x-protobuf")
        ),
        description = "OTLP ExportTraceServiceRequest."
    ),
    responses(
        (status = 200, description = "OTLP traces accepted"),
        (status = 400, description = "Invalid OTLP request", body = ErrorEnvelope),
        (status = 413, description = "Request exceeds configured limit")
    )
)]
fn otlp_traces() {}

#[utoipa::path(
    post,
    path = "/write/ns/{namespace}/api/v2/spans",
    tag = "ingest",
    params(("namespace" = String, Path)),
    request_body(content = Vec<ZipkinSpan>, content_type = "application/json"),
    responses(
        (status = 202, description = "Zipkin spans accepted"),
        (status = 400, description = "Invalid Zipkin request", body = ErrorEnvelope)
    )
)]
fn zipkin_spans() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/traces/{trace_id}",
    tag = "traces",
    params(
        ("namespace" = String, Path),
        ("trace_id" = String, Path, description = "32-character hexadecimal trace ID")
    ),
    responses(
        (status = 200, description = "Tempo v1 trace response", content(
            (TraceResponse = "application/json"),
            (BinaryBody = "application/x-protobuf")
        )),
        (status = 404, description = "Trace not found")
    )
)]
fn trace_v1() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/v2/traces/{trace_id}",
    tag = "traces",
    params(
        ("namespace" = String, Path),
        ("trace_id" = String, Path, description = "32-character hexadecimal trace ID")
    ),
    responses(
        (status = 200, description = "Tempo v2 trace response", content(
            (TraceResponse = "application/json"),
            (BinaryBody = "application/x-protobuf")
        )),
        (status = 404, description = "Trace not found")
    )
)]
fn trace_v2() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/search",
    tag = "search",
    params(
        ("namespace" = String, Path),
        ("q" = Option<String>, Query, description = "TraceQL expression"),
        ("tags" = Option<String>, Query, description = "Legacy key=value selector"),
        ("start" = Option<u64>, Query, description = "Unix seconds"),
        ("end" = Option<u64>, Query, description = "Unix seconds"),
        ("limit" = Option<usize>, Query)
    ),
    responses(
        (status = 200, description = "Matching trace summaries", body = SearchResponse),
        (status = 400, description = "Invalid TraceQL query", body = ErrorEnvelope)
    )
)]
fn search() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/search/tags",
    tag = "search",
    params(("namespace" = String, Path)),
    responses((status = 200, description = "Tempo tag names"))
)]
fn tag_names_legacy() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/v1/tags",
    tag = "search",
    params(("namespace" = String, Path)),
    responses((status = 200, description = "Tempo v1 tag names"))
)]
fn tag_names_v1() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/v2/search/tags",
    tag = "search",
    params(
        ("namespace" = String, Path),
        ("scope" = Option<String>, Query),
        ("q" = Option<String>, Query),
        ("start" = Option<u64>, Query),
        ("end" = Option<u64>, Query)
    ),
    responses((status = 200, description = "Tempo v2 tag names"))
)]
fn tag_names_v2() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/search/tag/{name}/values",
    tag = "search",
    params(
        ("namespace" = String, Path),
        ("name" = String, Path),
        ("q" = Option<String>, Query),
        ("start" = Option<u64>, Query),
        ("end" = Option<u64>, Query)
    ),
    responses((status = 200, description = "Tempo tag values"))
)]
fn tag_values_legacy() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/v1/tag/{name}/values",
    tag = "search",
    params(
        ("namespace" = String, Path),
        ("name" = String, Path)
    ),
    responses((status = 200, description = "Tempo v1 tag values"))
)]
fn tag_values_v1() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/v2/search/tag/{name}/values",
    tag = "search",
    params(
        ("namespace" = String, Path),
        ("name" = String, Path),
        ("scope" = Option<String>, Query),
        ("q" = Option<String>, Query),
        ("start" = Option<u64>, Query),
        ("end" = Option<u64>, Query)
    ),
    responses((status = 200, description = "Tempo v2 tag values"))
)]
fn tag_values_v2() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/echo",
    tag = "operations",
    params(("namespace" = String, Path)),
    responses((status = 200, description = "Tempo-compatible echo"))
)]
fn echo() {}

#[utoipa::path(
    get,
    path = "/read/ns/{namespace}/api/metrics/query_range",
    tag = "search",
    params(("namespace" = String, Path)),
    responses((status = 501, description = "TraceQL metrics are not implemented"))
)]
fn metrics_query_range() {}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Track HTTP API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Tempo-compatible trace query and OTLP/Zipkin HTTP ingestion API. OTLP and Jaeger gRPC services are documented separately."
    ),
    paths(
        healthy,
        ready,
        otlp_traces,
        zipkin_spans,
        trace_v1,
        trace_v2,
        search,
        tag_names_legacy,
        tag_names_v1,
        tag_names_v2,
        tag_values_legacy,
        tag_values_v1,
        tag_values_v2,
        echo,
        metrics_query_range
    ),
    components(schemas(
        ErrorEnvelope,
        TraceResponse,
        SearchResponse,
        BinaryBody,
        JsonBody,
        ZipkinSpan,
        ZipkinEndpoint,
        ZipkinAnnotation
    )),
    tags(
        (name = "traces", description = "Trace lookup"),
        (name = "search", description = "TraceQL and tag discovery"),
        (name = "ingest", description = "OTLP and Zipkin ingestion"),
        (name = "operations", description = "Health, readiness, and compatibility")
    )
)]
struct ApiDoc;

pub fn document() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}
