use std::{
    collections::BTreeMap,
    ops::RangeInclusive,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, RawQuery, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use common::display::prometheus_float;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use plural_metrics::{
    Namespace, OtelConfig, OtelConverter, ShardedMetrics, remote_write::Protocol,
};
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};
use server_common::auth::{Permission, authorize};
use server_common::http::{check_content_encoding, check_snappy_size, content_type};
use tower_http::compression::CompressionLayer;

use crate::{config::ServerMode, state::AppState};

pub fn router(state: AppState) -> Router {
    let request = &state.config.request;
    let limits = |router| {
        server_common::http::limit_request_bodies(
            router,
            request.max_request_bytes,
            request.max_decoded_request_bytes,
        )
    };
    let mut app = Router::new()
        .route("/-/healthy", get(|| async { StatusCode::OK }))
        .route(
            "/-/ready",
            get(|State(state): State<AppState>| async move {
                if state.is_ready().await {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        )
        .route("/metrics", get(server_common::runtime::scrape_metrics));

    if state.config.mode != ServerMode::Writer {
        let read_routes = Router::new()
            .route("/api/v1/query", get(query).post(query_form))
            .route(
                "/api/v1/query_range",
                get(query_range).post(query_range_form),
            )
            .route("/api/v1/series", get(series).post(series_form))
            .route("/api/v1/labels", get(labels))
            .route("/api/v1/label/{name}/values", get(label_values))
            .route("/api/v1/metadata", get(metadata))
            .route("/federate", get(federate))
            .layer(CompressionLayer::new());
        app = app.nest(
            &format!("{}/read/ns/{{namespace}}", state.config.path_prefix),
            limits(read_routes),
        );
    }
    if state.config.mode != ServerMode::Reader {
        // Ingest wraps the limits so it counts bytes as received, not decoded.
        let write_routes = limits(
            Router::new()
                .route("/api/v1/write", post(remote_write))
                .route("/v1/metrics", post(otlp_http)),
        )
        .route_layer(state.ingest.http_layer());
        app = app.nest(
            &format!("{}/write/ns/{{namespace}}", state.config.path_prefix),
            write_routes,
        );
    }
    app.with_state(state)
}

async fn remote_write(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Write).await?;
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    let protocol = Protocol::from_content_type(content_type).ok_or_else(|| {
        ApiError::unsupported_media(format!(
            "unsupported remote write content type {:?}",
            content_type.unwrap_or_default()
        ))
    })?;
    check_content_encoding(&headers, true)?;
    check_snappy_size(&body, state.config.request.max_decoded_request_bytes)?;
    let batch = plural_metrics::remote_write::parse_remote_write(&body, protocol)
        .map_err(ApiError::bad_request)?;
    let (samples, histograms) = (batch.samples, batch.histograms);
    let request_id = request_id(&headers, &body);
    state
        .route_write(
            &namespace,
            batch.series,
            state.config.write.durability,
            request_id,
        )
        .await?;
    Ok(match protocol {
        Protocol::V1 => StatusCode::NO_CONTENT.into_response(),
        Protocol::V2 => (
            StatusCode::NO_CONTENT,
            [
                (
                    "x-prometheus-remote-write-samples-written",
                    samples.to_string(),
                ),
                (
                    "x-prometheus-remote-write-histograms-written",
                    histograms.to_string(),
                ),
                (
                    "x-prometheus-remote-write-exemplars-written",
                    "0".to_string(),
                ),
            ],
        )
            .into_response(),
    })
}

async fn otlp_http(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let json = content_type(&headers) == Some("application/json");
    match otlp_http_result(&state, namespace, headers, body, json).await {
        Ok(response) => response,
        Err(error) => error.into_otlp_response(json),
    }
}

async fn otlp_http_result(
    state: &AppState,
    namespace: String,
    headers: HeaderMap,
    body: Bytes,
    json: bool,
) -> Result<Response, ApiError> {
    authorize_namespace(state, &namespace, &headers, Permission::Write).await?;
    check_content_encoding(&headers, false)?;
    let id = request_id(&headers, &body);
    let request = if json {
        plural_metrics::otel::decode_metrics_json(&body).map_err(ApiError::bad_request)?
    } else if matches!(
        content_type(&headers),
        None | Some(
            "application/x-protobuf"
                | "application/protobuf"
                | "application/octet-stream"
                | "application/vnd.google.protobuf"
        )
    ) {
        ExportMetricsServiceRequest::decode(body).map_err(ApiError::bad_request)?
    } else {
        return Err(ApiError::unsupported_media("unsupported OTLP content type"));
    };
    let series = OtelConverter::new(OtelConfig::default())
        .convert(&request)
        .map_err(ApiError::bad_request)?;
    state
        .route_write(&namespace, series, state.config.write.durability, id)
        .await?;
    if json {
        return Ok(Json(json!({})).into_response());
    }
    let mut encoded = Vec::new();
    ExportMetricsServiceResponse {
        partial_success: None,
    }
    .encode(&mut encoded)
    .map_err(ApiError::internal)?;
    Ok((
        StatusCode::OK,
        [("content-type", "application/x-protobuf")],
        encoded,
    )
        .into_response())
}

#[derive(Deserialize)]
struct InstantQuery {
    query: String,
    time: Option<String>,
    trace: Option<String>,
}

/// `trace=true`, `trace=1`, or a bare `trace` asks for the per-query trace.
fn trace_requested(flag: Option<&str>) -> bool {
    flag.is_some_and(|flag| {
        let flag = flag.trim();
        flag.is_empty() || flag == "1" || flag.eq_ignore_ascii_case("true")
    })
}

async fn query(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<InstantQuery>,
) -> Result<Response, ApiError> {
    execute_query(state, namespace, headers, params).await
}

async fn query_form(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let params = serde_html_form::from_bytes(&body).map_err(ApiError::bad_request)?;
    execute_query(state, namespace, headers, params).await
}

async fn execute_query(
    state: AppState,
    namespace: String,
    headers: HeaderMap,
    params: InstantQuery,
) -> Result<Response, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    let at = params.time.as_deref().map(parse_time).transpose()?;
    let reader = reader(&state, &namespace).await?;
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    let expression = params.query;
    let trace = trace_requested(params.trace.as_deref());
    let (value, trace) = tokio::spawn(async move {
        reader
            .query_with_trace(&namespace, &expression, at, trace)
            .await
    })
    .await
    .map_err(ApiError::internal)?
    .map_err(query_error)?;
    let mut response = plural_metrics::query_value_to_response(Ok(value));
    response.trace = trace;
    prom_json(response, "instant")
}

#[derive(Deserialize)]
struct RangeQuery {
    query: String,
    start: String,
    end: String,
    step: String,
    trace: Option<String>,
}

async fn query_range(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<RangeQuery>,
) -> Result<Response, ApiError> {
    execute_query_range(state, namespace, headers, params).await
}

async fn query_range_form(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let params = serde_html_form::from_bytes(&body).map_err(ApiError::bad_request)?;
    execute_query_range(state, namespace, headers, params).await
}

async fn execute_query_range(
    state: AppState,
    namespace: String,
    headers: HeaderMap,
    params: RangeQuery,
) -> Result<Response, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    let start = parse_time(&params.start)?;
    let end = parse_time(&params.end)?;
    let step = plural_metrics::parse_duration(&params.step).map_err(ApiError::bad_request)?;
    if step.is_zero() || end < start {
        return Err(ApiError::bad_request("invalid range or step"));
    }
    let reader = reader(&state, &namespace).await?;
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    let expression = params.query;
    let range = RangeInclusive::new(start, end);
    let trace = trace_requested(params.trace.as_deref());
    let (values, trace) = tokio::spawn(async move {
        reader
            .query_range_with_trace(&namespace, &expression, range, step, trace)
            .await
    })
    .await
    .map_err(ApiError::internal)?
    .map_err(query_error)?;
    let mut response = plural_metrics::range_result_to_response(Ok(values));
    response.trace = trace;
    prom_json(response, "range")
}

#[derive(Default, Deserialize)]
struct MatchQuery {
    #[serde(rename = "match[]", default)]
    matches: Vec<String>,
    start: Option<String>,
    end: Option<String>,
}

async fn series(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Result<Json<Value>, ApiError> {
    let params = parse_match_query(raw.as_deref())?;
    execute_series(state, namespace, headers, params).await
}

async fn series_form(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let params = serde_html_form::from_bytes(&body).map_err(ApiError::bad_request)?;
    execute_series(state, namespace, headers, params).await
}

async fn execute_series(
    state: AppState,
    namespace: String,
    headers: HeaderMap,
    params: MatchQuery,
) -> Result<Json<Value>, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    let refs = params
        .matches
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let range = time_range(params.start, params.end)?;
    let metrics_namespace = Namespace::new(&namespace).map_err(ApiError::bad_request)?;
    let data = reader(&state, &namespace)
        .await?
        .series(&metrics_namespace, &refs, range)
        .await
        .map_err(query_error)?;
    Ok(Json(json!({"status":"success","data":data})))
}

async fn labels(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Result<Json<Value>, ApiError> {
    let params = parse_match_query(raw.as_deref())?;
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    let refs = params
        .matches
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let range = time_range(params.start, params.end)?;
    let metrics_namespace = Namespace::new(&namespace).map_err(ApiError::bad_request)?;
    let data = reader(&state, &namespace)
        .await?
        .labels(
            &metrics_namespace,
            (!refs.is_empty()).then_some(refs.as_slice()),
            range,
        )
        .await
        .map_err(query_error)?;
    Ok(Json(json!({"status":"success","data":data})))
}

async fn label_values(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Result<Json<Value>, ApiError> {
    let params = parse_match_query(raw.as_deref())?;
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    let refs = params
        .matches
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let range = time_range(params.start, params.end)?;
    let metrics_namespace = Namespace::new(&namespace).map_err(ApiError::bad_request)?;
    let data = reader(&state, &namespace)
        .await?
        .label_values(
            &metrics_namespace,
            &name,
            (!refs.is_empty()).then_some(refs.as_slice()),
            range,
        )
        .await
        .map_err(query_error)?;
    Ok(Json(json!({"status":"success","data":data})))
}

#[derive(Default, Deserialize)]
struct MetadataQuery {
    metric: Option<String>,
    limit: Option<usize>,
    limit_per_metric: Option<usize>,
}

async fn metadata(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<MetadataQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    let metrics_namespace = Namespace::new(&namespace).map_err(ApiError::bad_request)?;
    let entries = reader(&state, &namespace)
        .await?
        .metadata(&metrics_namespace, params.metric.as_deref())
        .await
        .map_err(query_error)?;
    let mut data: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for entry in entries {
        let values = data.entry(entry.metric_name).or_default();
        if params
            .limit_per_metric
            .is_none_or(|limit| values.len() < limit)
        {
            values.push(json!({
                "type": entry.metric_type.as_ref().map(|kind| kind.as_str()).unwrap_or(""),
                "help": entry.description.unwrap_or_default(),
                "unit": entry.unit.unwrap_or_default(),
            }));
        }
    }
    if let Some(limit) = params.limit {
        data = data.into_iter().take(limit).collect();
    }
    Ok(Json(json!({"status":"success","data":data})))
}

async fn federate(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Result<Response, ApiError> {
    let params = parse_match_query(raw.as_deref())?;
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    let reader = reader(&state, &namespace).await?;
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    let mut output = String::new();
    for matcher in &params.matches {
        let reader = Arc::clone(&reader);
        let namespace = namespace.clone();
        let matcher = matcher.clone();
        let value = tokio::spawn(async move { reader.query(&namespace, &matcher, None).await })
            .await
            .map_err(ApiError::internal)?
            .map_err(query_error)?;
        for sample in value.into_matrix() {
            let metric = format_prometheus_labels(&sample.labels);
            for (timestamp, value) in sample.samples {
                output.push_str(&format!(
                    "{metric} {} {timestamp}\n",
                    prometheus_float(value)
                ));
            }
        }
    }
    Ok((
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        output,
    )
        .into_response())
}

fn parse_match_query(raw: Option<&str>) -> Result<MatchQuery, ApiError> {
    serde_html_form::from_str(raw.unwrap_or_default()).map_err(ApiError::bad_request)
}

fn format_prometheus_labels(labels: &plural_metrics::Labels) -> String {
    let metric = labels.metric_name();
    let attributes = labels
        .iter()
        .filter(|label| label.name != "__name__")
        .map(|label| {
            format!(
                "{}=\"{}\"",
                label.name,
                label
                    .value
                    .replace('\\', "\\\\")
                    .replace('\n', "\\n")
                    .replace('"', "\\\"")
            )
        })
        .collect::<Vec<_>>();
    if attributes.is_empty() {
        metric.to_owned()
    } else {
        format!("{metric}{{{}}}", attributes.join(","))
    }
}

async fn reader(state: &AppState, namespace: &str) -> Result<Arc<ShardedMetrics>, ApiError> {
    if state.namespace(namespace).is_none() {
        return Err(ApiError::not_found("namespace is not readable"));
    }
    state
        .readers
        .as_ref()
        .cloned()
        .ok_or_else(|| ApiError::not_found("namespace is not readable"))
}

async fn authorize_namespace(
    state: &AppState,
    namespace: &str,
    headers: &HeaderMap,
    permission: Permission,
) -> Result<(), ApiError> {
    let config = state
        .namespace(namespace)
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
        Ok(())
    } else {
        Err(ApiError::unauthorized())
    }
}

/// Parses a Prometheus API timestamp: Unix seconds (fractional allowed) or
/// RFC 3339. Times before the epoch clamp to it, so clients sending
/// Prometheus' minimum-time sentinel still get the full range.
fn parse_time(value: &str) -> Result<SystemTime, ApiError> {
    if let Ok(seconds) = value.parse::<f64>() {
        return plural_metrics::unix_seconds(seconds.max(0.0))
            .ok_or_else(|| ApiError::bad_request(format!("invalid timestamp {value:?}")));
    }
    let time = plural_metrics::parse_timestamp(value).map_err(ApiError::bad_request)?;
    Ok(time.max(UNIX_EPOCH))
}

fn time_range(
    start: Option<String>,
    end: Option<String>,
) -> Result<RangeInclusive<SystemTime>, ApiError> {
    Ok(RangeInclusive::new(
        start
            .as_deref()
            .map(parse_time)
            .transpose()?
            .unwrap_or(UNIX_EPOCH),
        end.as_deref()
            .map(parse_time)
            .transpose()?
            .unwrap_or_else(SystemTime::now),
    ))
}

/// Query results go through Metrics' Prometheus wire encoders so native
/// histograms (`histogram` / `histograms`) and float spellings match upstream.
/// Serialized straight to bytes: an intermediate `serde_json::Value` costs a
/// map node and a `String` per point on large range results.
fn prom_json(
    response: impl serde::Serialize,
    query_type: &'static str,
) -> Result<Response, ApiError> {
    let started = Instant::now();
    let body = serde_json::to_vec(&response).map_err(ApiError::internal)?;
    metrics::histogram!(
        "telemetry_metrics_response_serialization_seconds",
        "query_type" => query_type
    )
    .record(started.elapsed().as_secs_f64());
    metrics::counter!(
        "telemetry_metrics_response_bytes_total",
        "query_type" => query_type
    )
    .increment(body.len() as u64);
    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response())
}

fn request_id(headers: &HeaderMap, body: &[u8]) -> String {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| blake3::hash(body).to_hex().to_string())
}

pub(crate) use server_common::ApiError;

pub(crate) fn metrics_error(error: plural_metrics::Error) -> ApiError {
    match error {
        plural_metrics::Error::InvalidInput(_) | plural_metrics::Error::Encoding(_) => {
            ApiError::bad_request(error)
        }
        plural_metrics::Error::Backpressure => ApiError::too_many_requests(error),
        plural_metrics::Error::Storage(_) | plural_metrics::Error::Shard(_) => {
            ApiError::unavailable(error)
        }
        plural_metrics::Error::Internal(_) => ApiError::internal(error),
    }
}

/// Maps query failures onto the status codes and `errorType`s Prometheus uses.
fn query_error(error: plural_metrics::QueryError) -> ApiError {
    match error {
        plural_metrics::QueryError::InvalidQuery(_) => ApiError::bad_request(error),
        plural_metrics::QueryError::Execution(_) => {
            ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, error).with_error_type("execution")
        }
        plural_metrics::QueryError::Timeout => {
            ApiError::new(StatusCode::SERVICE_UNAVAILABLE, error).with_error_type("timeout")
        }
        plural_metrics::QueryError::Storage(_) => ApiError::internal(error),
    }
}

#[cfg(test)]
mod protocol_tests {
    use axum::body::to_bytes;
    use server_common::http::GoogleRpcStatus;

    use super::*;

    #[test]
    fn remote_write_backpressure_is_retryable() {
        let response = metrics_error(plural_metrics::Error::Backpressure).into_response();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
    }

    #[test]
    fn remote_write_storage_failure_is_service_unavailable() {
        let response =
            metrics_error(plural_metrics::Error::Storage("flusher stopped".into())).into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .is_none()
        );
    }

    #[tokio::test]
    async fn otlp_http_errors_use_google_rpc_status() {
        let response = metrics_error(plural_metrics::Error::Backpressure).into_otlp_response(false);

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers()[axum::http::header::CONTENT_TYPE],
            "application/x-protobuf"
        );
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let status = GoogleRpcStatus::decode(body).unwrap();
        assert_eq!(status.code, 8);
        assert_eq!(status.message, "Backpressure: write queue is full");
    }
}
