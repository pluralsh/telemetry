use std::{
    collections::BTreeMap,
    io::Read,
    ops::RangeInclusive,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
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
use meter::{Namespace, OtelConfig, OtelConverter, ShardedMeter, remote_write::Protocol};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};
use server_common::auth::{Permission, authorize};
use server_common::http::content_type;

use crate::{config::ServerMode, state::AppState};

pub fn router(state: AppState) -> Router {
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
            .route("/federate", get(federate));
        app = app.nest(
            &format!("{}/read/ns/{{namespace}}", state.config.path_prefix),
            read_routes,
        );
    }
    if state.config.mode != ServerMode::Reader {
        let write_routes = Router::new()
            .route("/api/v1/write", post(remote_write))
            .route("/v1/metrics", post(otlp_http));
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
    let batch =
        meter::remote_write::parse_remote_write(&body, protocol).map_err(ApiError::bad_request)?;
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
    let id = request_id(&headers, &body);
    let body = decode_content_encoding(&headers, body)?;
    let request = if json {
        meter::otel::decode_metrics_json(&body).map_err(ApiError::bad_request)?
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

/// Upper bound on a decompressed OTLP body, guarding against gzip bombs.
const MAX_DECODED_OTLP_BYTES: u64 = 64 * 1024 * 1024;

fn decode_content_encoding(headers: &HeaderMap, body: Bytes) -> Result<Bytes, ApiError> {
    let encoding = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .unwrap_or("");
    if encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") {
        return Ok(body);
    }
    if !encoding.eq_ignore_ascii_case("gzip") {
        return Err(ApiError::unsupported_media(format!(
            "unsupported content encoding {encoding:?}"
        )));
    }
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(body.as_ref())
        .take(MAX_DECODED_OTLP_BYTES + 1)
        .read_to_end(&mut decoded)
        .map_err(ApiError::bad_request)?;
    if decoded.len() as u64 > MAX_DECODED_OTLP_BYTES {
        return Err(ApiError::too_large());
    }
    Ok(Bytes::from(decoded))
}

#[derive(Deserialize)]
struct InstantQuery {
    query: String,
    time: Option<String>,
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
    let value = tokio::spawn(async move { reader.query(&namespace, &expression, at).await })
        .await
        .map_err(ApiError::internal)?
        .map_err(query_error)?;
    prom_json(meter::query_value_to_response(Ok(value)))
}

#[derive(Deserialize)]
struct RangeQuery {
    query: String,
    start: String,
    end: String,
    step: String,
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
    let step = meter::parse_duration(&params.step).map_err(ApiError::bad_request)?;
    if step.is_zero() || end < start {
        return Err(ApiError::bad_request("invalid range or step"));
    }
    let reader = reader(&state, &namespace).await?;
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    let expression = params.query;
    let range = RangeInclusive::new(start, end);
    let values = tokio::spawn(async move {
        reader
            .query_range(&namespace, &expression, range, step)
            .await
    })
    .await
    .map_err(ApiError::internal)?
    .map_err(query_error)?;
    prom_json(meter::range_result_to_response(Ok(values)))
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
    let meter_namespace = Namespace::new(&namespace).map_err(ApiError::bad_request)?;
    let data = reader(&state, &namespace)
        .await?
        .series(&meter_namespace, &refs, range)
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
    let meter_namespace = Namespace::new(&namespace).map_err(ApiError::bad_request)?;
    let data = reader(&state, &namespace)
        .await?
        .labels(
            &meter_namespace,
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
    let meter_namespace = Namespace::new(&namespace).map_err(ApiError::bad_request)?;
    let data = reader(&state, &namespace)
        .await?
        .label_values(
            &meter_namespace,
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
    let meter_namespace = Namespace::new(&namespace).map_err(ApiError::bad_request)?;
    let entries = reader(&state, &namespace)
        .await?
        .metadata(&meter_namespace, params.metric.as_deref())
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

fn format_prometheus_labels(labels: &meter::Labels) -> String {
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

async fn reader(state: &AppState, namespace: &str) -> Result<Arc<ShardedMeter>, ApiError> {
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
        let since_epoch = Duration::try_from_secs_f64(seconds.max(0.0))
            .map_err(|_| ApiError::bad_request(format!("invalid timestamp {value:?}")))?;
        return UNIX_EPOCH
            .checked_add(since_epoch)
            .ok_or_else(|| ApiError::bad_request(format!("invalid timestamp {value:?}")));
    }
    let time = meter::parse_timestamp(value).map_err(ApiError::bad_request)?;
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

/// Query results go through Meter's Prometheus wire encoders so native
/// histograms (`histogram` / `histograms`) and float spellings match upstream.
/// Serialized straight to bytes: an intermediate `serde_json::Value` costs a
/// map node and a `String` per point on large range results.
fn prom_json(response: impl serde::Serialize) -> Result<Response, ApiError> {
    let body = serde_json::to_vec(&response).map_err(ApiError::internal)?;
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

pub(crate) fn meter_error(error: meter::Error) -> ApiError {
    match error {
        meter::Error::InvalidInput(_) | meter::Error::Encoding(_) => ApiError::bad_request(error),
        meter::Error::Backpressure => ApiError::too_many_requests(error),
        meter::Error::Storage(_) | meter::Error::Shard(_) => ApiError::unavailable(error),
        meter::Error::Internal(_) => ApiError::internal(error),
    }
}

/// Maps query failures onto the status codes and `errorType`s Prometheus uses.
fn query_error(error: meter::QueryError) -> ApiError {
    match error {
        meter::QueryError::InvalidQuery(_) => ApiError::bad_request(error),
        meter::QueryError::Execution(_) => {
            ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, error).with_error_type("execution")
        }
        meter::QueryError::Timeout => {
            ApiError::new(StatusCode::SERVICE_UNAVAILABLE, error).with_error_type("timeout")
        }
        meter::QueryError::Storage(_) => ApiError::internal(error),
    }
}

#[cfg(test)]
mod protocol_tests {
    use axum::body::to_bytes;
    use server_common::http::GoogleRpcStatus;

    use super::*;

    #[test]
    fn remote_write_backpressure_is_retryable() {
        let response = meter_error(meter::Error::Backpressure).into_response();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
    }

    #[test]
    fn remote_write_storage_failure_is_service_unavailable() {
        let response = meter_error(meter::Error::Storage("flusher stopped".into())).into_response();

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
        let response = meter_error(meter::Error::Backpressure).into_otlp_response(false);

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
