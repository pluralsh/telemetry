use server_common::http::{check_content_encoding, content_type};
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use opentelemetry_proto::tonic::{
    collector::trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
    common::v1::{AnyValue, KeyValue, any_value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, TracesData, span},
};
use plural_traces::{AttributeScope, Namespace, QueryOptions, TraceId, trace_batches};
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};
use server_common::auth::{Permission, authorize};
use tower_http::compression::CompressionLayer;

use crate::{
    AppState,
    config::{NamespaceConfig, ServerMode},
};

pub fn router(state: AppState) -> Router {
    // Ingest wraps the limits so it counts bytes as received, not decoded.
    let writes = server_common::http::limit_request_bodies(
        Router::new()
            .route("/write/ns/{namespace}/v1/traces", post(otlp_http))
            .route("/write/ns/{namespace}/api/v2/spans", post(zipkin)),
        state.config.request.max_request_bytes,
        state.config.request.max_decoded_request_bytes,
    )
    .route_layer(state.ingest.http_layer());
    let reads = Router::new()
        .route(
            "/read/ns/{namespace}/api/traces/{trace_id}",
            get(trace_by_id),
        )
        .route(
            "/read/ns/{namespace}/api/v2/traces/{trace_id}",
            get(trace_by_id),
        )
        .route("/read/ns/{namespace}/api/search", get(search))
        .route("/read/ns/{namespace}/api/search/tags", get(tag_names))
        .route("/read/ns/{namespace}/api/v1/tags", get(tag_names))
        .route("/read/ns/{namespace}/api/v2/search/tags", get(tag_names_v2))
        .route(
            "/read/ns/{namespace}/api/search/tag/{name}/values",
            get(tag_values),
        )
        .route(
            "/read/ns/{namespace}/api/v1/tag/{name}/values",
            get(tag_values),
        )
        .route(
            "/read/ns/{namespace}/api/v2/search/tag/{name}/values",
            get(tag_values_v2),
        )
        .route("/read/ns/{namespace}/api/echo", get(echo))
        .route(
            "/read/ns/{namespace}/api/metrics/query_range",
            get(metrics_unsupported),
        )
        .layer(CompressionLayer::new());
    let public = Router::new().merge(writes).merge(reads);
    let app = Router::new()
        .route("/-/healthy", get(|| async { StatusCode::OK }))
        .route("/metrics", get(server_common::runtime::scrape_metrics))
        .route("/-/ready", get(readiness));
    let app = if state.config.path_prefix.is_empty() {
        app.merge(public)
    } else {
        app.nest(&state.config.path_prefix, public)
    };
    app.with_state(state)
}

async fn readiness(State(state): State<AppState>) -> StatusCode {
    if state.is_ready().await {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn otlp_http(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let json_request = content_type(&headers) == Some("application/json");
    match otlp_http_result(&state, namespace, headers, body).await {
        Ok(response) => response,
        Err(error) => error.into_otlp_response(json_request),
    }
}

async fn otlp_http_result(
    state: &AppState,
    namespace: String,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    require_write_mode(state)?;
    authorize_namespace(state, &namespace, &headers, Permission::Write).await?;
    check_content_encoding(&headers, false)?;
    let _permit = state
        .request_limit
        .acquire()
        .await
        .map_err(|_| ApiError::unavailable("server is shutting down"))?;
    let is_json = content_type(&headers) == Some("application/json");
    let request = if is_json {
        serde_json::from_slice::<ExportTraceServiceRequest>(&body).map_err(ApiError::bad_request)?
    } else if matches!(
        content_type(&headers),
        Some(
            "application/x-protobuf"
                | "application/protobuf"
                | "application/octet-stream"
                | "application/vnd.google.protobuf"
        )
    ) {
        ExportTraceServiceRequest::decode(body).map_err(ApiError::bad_request)?
    } else {
        return Err(ApiError::unsupported_media("unsupported content type"));
    };
    let batches = trace_batches(request).map_err(ApiError::bad_request)?;
    write_batches(state, namespace, batches).await?;
    if is_json {
        Ok(Json(json!({})).into_response())
    } else {
        let response = ExportTraceServiceResponse {
            partial_success: None,
        };
        Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/x-protobuf")],
            response.encode_to_vec(),
        )
            .into_response())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ZipkinSpan {
    trace_id: String,
    id: String,
    #[serde(default)]
    parent_id: Option<String>,
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: Option<String>,
    timestamp: u64,
    #[serde(default)]
    duration: u64,
    #[serde(default)]
    local_endpoint: Option<ZipkinEndpoint>,
    #[serde(default)]
    tags: BTreeMap<String, String>,
    #[serde(default)]
    annotations: Vec<ZipkinAnnotation>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ZipkinEndpoint {
    #[serde(default)]
    service_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ZipkinAnnotation {
    timestamp: u64,
    value: String,
}

async fn zipkin(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    require_write_mode(&state)?;
    authorize_namespace(&state, &namespace, &headers, Permission::Write).await?;
    check_content_encoding(&headers, false)?;
    if content_type(&headers) != Some("application/json") {
        return Err(ApiError::unsupported_media("unsupported content type"));
    }
    let spans: Vec<ZipkinSpan> = serde_json::from_slice(&body).map_err(ApiError::bad_request)?;
    if spans.is_empty() {
        return Err(ApiError::bad_request(
            "Zipkin request must contain at least one span",
        ));
    }
    let mut resources = Vec::new();
    for value in spans {
        let trace_id = parse_zipkin_trace_id(&value.trace_id)?;
        let span_id = parse_hex_exact(&value.id, 8, "Zipkin span ID")?;
        let parent_span_id = value
            .parent_id
            .as_deref()
            .map(|value| parse_hex_exact(value, 8, "Zipkin parent span ID"))
            .transpose()?
            .unwrap_or_default();
        let service_name = value
            .local_endpoint
            .and_then(|endpoint| endpoint.service_name);
        let status = value.tags.get("error").map(|message| Status {
            message: message.clone(),
            code: 2,
        });
        let attributes = value
            .tags
            .into_iter()
            .map(|(key, value)| string_attribute(key, value))
            .collect();
        let events = value
            .annotations
            .into_iter()
            .map(|annotation| span::Event {
                time_unix_nano: annotation.timestamp.saturating_mul(1_000),
                name: annotation.value,
                attributes: Vec::new(),
                dropped_attributes_count: 0,
            })
            .collect();
        let resource = service_name.map(|service| Resource {
            attributes: vec![string_attribute("service.name", service)],
            dropped_attributes_count: 0,
        });
        resources.push(ResourceSpans {
            resource,
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: trace_id.as_bytes().to_vec(),
                    span_id,
                    parent_span_id,
                    name: value.name,
                    kind: zipkin_kind(value.kind.as_deref()),
                    start_time_unix_nano: value.timestamp.saturating_mul(1_000),
                    end_time_unix_nano: value
                        .timestamp
                        .saturating_add(value.duration)
                        .saturating_mul(1_000),
                    attributes,
                    events,
                    status,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
    }
    let batches = plural_traces::trace_batches_from_resource_spans(resources)
        .map_err(ApiError::bad_request)?;
    write_batches(&state, namespace, batches).await?;
    Ok(StatusCode::ACCEPTED)
}

async fn trace_by_id(
    State(state): State<AppState>,
    Path((namespace, trace_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    require_read_mode(&state)?;
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    let _permit = state
        .query_limit
        .acquire()
        .await
        .map_err(|_| ApiError::unavailable("server is shutting down"))?;
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    let trace_id = TraceId::from_str(&trace_id).map_err(ApiError::bad_request)?;
    let assignment = state.router.assignment().read().await.clone();
    let trace = state
        .db
        .get_trace(&assignment, &namespace, trace_id)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("trace not found"))?;
    let data = TracesData {
        resource_spans: trace.resource_spans,
    };
    if accepts_protobuf(&headers) {
        Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/x-protobuf")],
            data.encode_to_vec(),
        )
            .into_response())
    } else {
        Ok(Json(data).into_response())
    }
}

#[derive(Deserialize)]
struct SearchParams {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    tags: Option<String>,
    #[serde(default)]
    start: Option<u64>,
    #[serde(default)]
    end: Option<u64>,
    #[serde(default)]
    limit: Option<usize>,
}

async fn search(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<SearchParams>,
) -> Result<Json<Value>, ApiError> {
    require_read_mode(&state)?;
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    let _permit = state
        .query_limit
        .acquire()
        .await
        .map_err(|_| ApiError::unavailable("server is shutting down"))?;
    let limit = params.limit.unwrap_or(20);
    if limit == 0 || limit > state.config.request.max_query_limit {
        return Err(ApiError::bad_request("limit is outside configured range"));
    }
    let query = params
        .q
        .or_else(|| params.tags.map(|tags| legacy_tags_query(&tags)))
        .unwrap_or_else(|| "{}".into());
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    let results = state
        .db
        .search_traceql(
            &namespace,
            params.start.unwrap_or(0).saturating_mul(1_000_000_000),
            params
                .end
                .unwrap_or(u64::MAX / 1_000_000_000)
                .saturating_mul(1_000_000_000),
            &query,
            QueryOptions {
                limit,
                max_candidate_traces: state.config.request.max_candidates,
                max_spans_per_trace: state.config.request.max_spans_per_trace,
                max_concurrency: state.config.request.query_concurrency,
            },
        )
        .await
        .map_err(ApiError::bad_request)?;
    let traces = results
        .into_iter()
        .map(|result| {
            json!({
                "traceID": result.trace_id.to_string(),
                "rootServiceName": result.root_service_name.unwrap_or_default(),
                "rootTraceName": result.root_span_name.unwrap_or_default(),
                "startTimeUnixNano": result.start_ns.to_string(),
                "durationMs": result.end_ns.saturating_sub(result.start_ns) as f64 / 1_000_000.0,
                "spanSet": {"matched": result.matched_spans},
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(
        json!({"traces": traces, "metrics": {"inspectedTraces": traces.len()}}),
    ))
}

async fn tag_names(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<TagParams>,
) -> Result<Json<Value>, ApiError> {
    let namespace = catalog_namespace(&state, &namespace, &headers).await?;
    let names = if params.scope.as_deref() == Some("intrinsic") {
        intrinsic_tag_names()
    } else {
        state
            .db
            .catalog_names(
                &namespace,
                params.start_ns(),
                params.end_ns(),
                attribute_scope(params.scope.as_deref()),
            )
            .await
            .map_err(ApiError::internal)?
            .into_iter()
            .collect()
    };
    Ok(Json(json!({"tagNames": names})))
}

async fn tag_names_v2(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<TagParams>,
) -> Result<Json<Value>, ApiError> {
    let namespace = catalog_namespace(&state, &namespace, &headers).await?;
    let mut scopes = Vec::new();
    if params
        .scope
        .as_deref()
        .is_none_or(|scope| scope == "resource")
    {
        let resource = state
            .db
            .catalog_names(
                &namespace,
                params.start_ns(),
                params.end_ns(),
                Some(AttributeScope::Resource),
            )
            .await
            .map_err(ApiError::internal)?;
        scopes.push(json!({"name": "resource", "tags": resource}));
    }
    if params.scope.as_deref().is_none_or(|scope| scope == "span") {
        let span = state
            .db
            .catalog_names(
                &namespace,
                params.start_ns(),
                params.end_ns(),
                Some(AttributeScope::Span),
            )
            .await
            .map_err(ApiError::internal)?;
        scopes.push(json!({"name": "span", "tags": span}));
    }
    if params.scope.as_deref() == Some("intrinsic") {
        scopes.push(json!({"name": "intrinsic", "tags": intrinsic_tag_names()}));
    }
    Ok(Json(json!({"scopes": scopes})))
}

async fn tag_values(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    Query(params): Query<TagParams>,
) -> Result<Json<Value>, ApiError> {
    let namespace = catalog_namespace(&state, &namespace, &headers).await?;
    let (scope, name) = scoped_name(&name, params.scope.as_deref());
    let values = state
        .db
        .catalog_values(&namespace, params.start_ns(), params.end_ns(), scope, name)
        .await
        .map_err(ApiError::internal)?
        .into_iter()
        .map(|value| discovery_value_parts(value).1)
        .collect::<BTreeSet<_>>();
    Ok(Json(json!({"tagValues": values})))
}

async fn tag_values_v2(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    Query(params): Query<TagParams>,
) -> Result<Json<Value>, ApiError> {
    let namespace = catalog_namespace(&state, &namespace, &headers).await?;
    let (scope, name) = scoped_name(&name, params.scope.as_deref());
    let values = state
        .db
        .catalog_values(&namespace, params.start_ns(), params.end_ns(), scope, name)
        .await
        .map_err(ApiError::internal)?
        .into_iter()
        .map(discovery_value_parts)
        .map(|(kind, value)| json!({"type": kind, "value": value}))
        .collect::<Vec<_>>();
    Ok(Json(json!({"tagValues": values})))
}

#[derive(Default, Deserialize)]
struct TagParams {
    scope: Option<String>,
    #[serde(default)]
    start: Option<u64>,
    #[serde(default)]
    end: Option<u64>,
}

impl TagParams {
    fn start_ns(&self) -> u64 {
        self.start.unwrap_or(0).saturating_mul(1_000_000_000)
    }

    fn end_ns(&self) -> u64 {
        self.end
            .unwrap_or(u64::MAX / 1_000_000_000)
            .saturating_mul(1_000_000_000)
    }
}

async fn catalog_namespace(
    state: &AppState,
    namespace: &str,
    headers: &HeaderMap,
) -> Result<Namespace, ApiError> {
    require_read_mode(state)?;
    authorize_namespace(state, namespace, headers, Permission::Read).await?;
    Namespace::new(namespace).map_err(ApiError::bad_request)
}

fn attribute_scope(scope: Option<&str>) -> Option<AttributeScope> {
    match scope {
        Some("resource") => Some(AttributeScope::Resource),
        Some("span") => Some(AttributeScope::Span),
        _ => None,
    }
}

fn scoped_name<'a>(
    name: &'a str,
    requested_scope: Option<&str>,
) -> (Option<AttributeScope>, &'a str) {
    if let Some(name) = name.strip_prefix("resource.") {
        (Some(AttributeScope::Resource), name)
    } else if let Some(name) = name.strip_prefix("span.") {
        (Some(AttributeScope::Span), name)
    } else {
        (attribute_scope(requested_scope), name)
    }
}

fn discovery_value_parts(value: common::discovery::DiscoveryValue) -> (&'static str, String) {
    match value {
        common::discovery::DiscoveryValue::String(value) => ("string", value),
        common::discovery::DiscoveryValue::Bool(value) => ("bool", value.to_string()),
        common::discovery::DiscoveryValue::Int(value) => ("int", value.to_string()),
        common::discovery::DiscoveryValue::Double(value) => ("float", value.to_string()),
    }
}

fn intrinsic_tag_names() -> BTreeSet<String> {
    [
        "duration",
        "name",
        "status",
        "statusMessage",
        "kind",
        "rootServiceName",
        "rootName",
        "traceDuration",
        "event:name",
        "event:timeSinceStart",
        "link:spanID",
        "link:traceID",
        "instrumentation:name",
        "instrumentation:version",
        "trace:id",
        "span:id",
        "span:parentID",
        "span:status",
        "span:statusMessage",
        "span:duration",
        "span:name",
        "span:kind",
        "trace:rootName",
        "trace:rootService",
        "trace:duration",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

async fn echo(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    require_read_mode(&state)?;
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    Ok(Json(json!({"status":"ok"})))
}

async fn metrics_unsupported() -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        "TraceQL metrics execution is not implemented",
    )
        .into_response()
}

async fn write_batches(
    state: &AppState,
    namespace: String,
    batches: Vec<plural_traces::TraceBatch>,
) -> Result<(), ApiError> {
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    state
        .route_write(&namespace, batches, ulid::Ulid::new().to_string())
        .await
}

async fn authorize_namespace(
    state: &AppState,
    namespace: &str,
    headers: &HeaderMap,
    permission: Permission,
) -> Result<NamespaceConfig, ApiError> {
    let config = state
        .namespaces
        .get(namespace)
        .cloned()
        .ok_or_else(|| ApiError::not_found("unknown namespace"))?;
    if authorize(
        headers,
        state.config.auth.unauthenticated,
        &state.config.auth.global,
        &config.auth,
        state.jwt.as_ref(),
        namespace,
        permission,
    )
    .await
    {
        Ok(config)
    } else {
        Err(ApiError::unauthorized())
    }
}

fn require_write_mode(state: &AppState) -> Result<(), ApiError> {
    if state.config.mode == ServerMode::Reader {
        Err(ApiError::not_found("write routes are disabled"))
    } else {
        Ok(())
    }
}

fn require_read_mode(state: &AppState) -> Result<(), ApiError> {
    if state.config.mode == ServerMode::Writer {
        Err(ApiError::not_found("read routes are disabled"))
    } else {
        Ok(())
    }
}

fn accepts_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("protobuf") || value.contains("octet-stream"))
}

fn parse_zipkin_trace_id(value: &str) -> Result<TraceId, ApiError> {
    let normalized = if value.len() == 16 {
        format!("{value:0>32}")
    } else {
        value.to_owned()
    };
    TraceId::from_str(&normalized).map_err(ApiError::bad_request)
}

fn parse_hex_exact(value: &str, bytes: usize, name: &str) -> Result<Vec<u8>, ApiError> {
    if value.len() != bytes * 2 {
        return Err(ApiError::bad_request(format!(
            "{name} must contain exactly {} hex characters",
            bytes * 2
        )));
    }
    (0..bytes)
        .map(|index| {
            u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).map_err(ApiError::bad_request)
        })
        .collect()
}

fn zipkin_kind(value: Option<&str>) -> i32 {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "SERVER" => span::SpanKind::Server as i32,
        "CLIENT" => span::SpanKind::Client as i32,
        "PRODUCER" => span::SpanKind::Producer as i32,
        "CONSUMER" => span::SpanKind::Consumer as i32,
        _ => span::SpanKind::Internal as i32,
    }
}

fn string_attribute(key: impl Into<String>, value: impl Into<String>) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
    }
}

fn legacy_tags_query(tags: &str) -> String {
    let expressions = tags
        .split_whitespace()
        .filter_map(|tag| tag.split_once('='))
        .map(|(name, value)| {
            format!(
                r#"resource."{}" = {}"#,
                name.replace('"', r#"\""#),
                serde_json::to_string(value).unwrap()
            )
        })
        .collect::<Vec<_>>();
    if expressions.is_empty() {
        "{}".into()
    } else {
        format!("{{ {} }}", expressions.join(" && "))
    }
}

pub(crate) use server_common::ApiError;

pub(crate) fn traces_error(error: plural_traces::Error) -> ApiError {
    match error {
        plural_traces::Error::Invalid(_) => ApiError::bad_request(error),
        plural_traces::Error::Backpressure => ApiError::too_many_requests(error),
        plural_traces::Error::Unavailable(_)
        | plural_traces::Error::Storage(_)
        | plural_traces::Error::Shard(_) => ApiError::unavailable(error),
        plural_traces::Error::Corrupt(_)
        | plural_traces::Error::Json(_)
        | plural_traces::Error::Protobuf(_)
        | plural_traces::Error::Compression(_)
        | plural_traces::Error::TraceQl(_) => ApiError::internal(error),
    }
}

/// The OTLP collector service is built on tonic 0.12, a separate crate
/// version from the server's own gRPC stack.
pub(crate) fn otlp_grpc_status(error: ApiError) -> tonic_otlp::Status {
    let status = error.into_grpc_status();
    tonic_otlp::Status::new(
        tonic_otlp::Code::from(status.code() as i32),
        status.message().to_owned(),
    )
}

#[cfg(test)]
mod protocol_tests {
    use axum::body::to_bytes;
    use server_common::http::GoogleRpcStatus;

    use super::*;

    #[tokio::test]
    async fn zipkin_backpressure_is_retryable() {
        let response = traces_error(plural_traces::Error::Backpressure).into_response();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    }

    #[tokio::test]
    async fn otlp_http_errors_use_google_rpc_status_mappings() {
        let protobuf = traces_error(plural_traces::Error::Backpressure).into_otlp_response(false);
        assert_eq!(protobuf.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(protobuf.headers()[header::RETRY_AFTER], "1");
        assert_eq!(
            protobuf.headers()[header::CONTENT_TYPE],
            "application/x-protobuf"
        );
        let body = to_bytes(protobuf.into_body(), usize::MAX).await.unwrap();
        let status = GoogleRpcStatus::decode(body).unwrap();
        assert_eq!(status.code, 8);
        assert_eq!(status.message, "write buffer is full");

        let json = ApiError::unavailable("flusher stopped").into_otlp_response(true);
        assert_eq!(json.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json.headers()[header::CONTENT_TYPE], "application/json");
        let body = to_bytes(json.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"code": 14, "message": "flusher stopped"})
        );
    }

    #[test]
    fn grpc_retryable_errors_map_to_unavailable() {
        assert_eq!(
            otlp_grpc_status(traces_error(plural_traces::Error::Backpressure)).code(),
            tonic_otlp::Code::Unavailable
        );
        assert_eq!(
            otlp_grpc_status(ApiError::unavailable("flusher stopped")).code(),
            tonic_otlp::Code::Unavailable
        );
        assert_eq!(
            traces_error(plural_traces::Error::Backpressure)
                .into_grpc_status()
                .code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            ApiError::unavailable("flusher stopped")
                .into_grpc_status()
                .code(),
            tonic::Code::Unavailable
        );
    }
}
