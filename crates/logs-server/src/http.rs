use server_common::http::{check_size, content_type};
use std::{collections::BTreeMap, io::Read};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, RawQuery, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use common::display::{hex, prometheus_float, sanitize_label_name};
use flate2::read::GzDecoder;
use opentelemetry_proto::tonic::{
    collector::logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
    common::v1::{AnyValue, KeyValue, any_value},
};
use plural_logs::{
    Direction, Field, Fields, Label, Labels, LogBatch, LogEntry, Namespace, QueryOptions,
    QueryRequest, QueryResult,
};
use prost::Message;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use server_common::auth::{Permission, authorize};

use crate::{AppState, config::NamespaceConfig};

const ENCODING_FLAGS: &str = "x-loki-response-encoding-flags";
const CATEGORIZE_LABELS: &str = "categorize-labels";

pub fn router(state: AppState) -> Router {
    let public = Router::new()
        .route(
            "/read/ns/{namespace}/loki/api/v1/query",
            get(query_get).post(query_post),
        )
        .route(
            "/read/ns/{namespace}/loki/api/v1/query_range",
            get(query_range_get).post(query_range_post),
        )
        .route("/read/ns/{namespace}/loki/api/v1/labels", get(label_names))
        .route(
            "/read/ns/{namespace}/loki/api/v1/label/{name}/values",
            get(label_values),
        )
        .route(
            "/read/ns/{namespace}/loki/api/v1/series",
            get(series_get).post(series_post),
        )
        .merge(
            Router::new()
                .route("/write/ns/{namespace}/loki/api/v1/push", post(loki_push))
                .route("/write/ns/{namespace}/otlp/v1/logs", post(otlp_logs))
                .route_layer(state.ingest.http_layer()),
        );
    let app = Router::new()
        .route("/-/healthy", get(|| async { StatusCode::OK }))
        .route("/metrics", get(server_common::runtime::scrape_metrics))
        .route(
            "/-/ready",
            get(|State(state): State<AppState>| async move {
                if state.is_ready().await {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        );
    let app = if state.config.path_prefix.is_empty() {
        app.merge(public)
    } else {
        app.nest(&state.config.path_prefix, public)
    };
    app.with_state(state)
}

#[derive(Debug, Deserialize)]
struct QueryParams {
    query: String,
    time: Option<String>,
    limit: Option<usize>,
    direction: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RangeParams {
    query: String,
    start: Option<String>,
    end: Option<String>,
    since: Option<String>,
    step: Option<String>,
    limit: Option<usize>,
    direction: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DiscoveryParams {
    start: Option<String>,
    end: Option<String>,
    since: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SeriesParams {
    #[serde(rename = "match[]", default)]
    selectors: Vec<String>,
    start: Option<String>,
    end: Option<String>,
    since: Option<String>,
}

async fn query_get(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Json<Value>, ApiError> {
    execute_query(state, namespace, headers, params).await
}

async fn query_post(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    check_size(body.len(), state.config.request.max_request_bytes)?;
    let params = serde_html_form::from_bytes(&body).map_err(ApiError::bad_request)?;
    execute_query(state, namespace, headers, params).await
}

async fn execute_query(
    state: AppState,
    namespace: String,
    headers: HeaderMap,
    params: QueryParams,
) -> Result<Json<Value>, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    validate_query(&params.query)?;
    let timestamp = params
        .time
        .as_deref()
        .map(parse_timestamp)
        .transpose()?
        .unwrap_or_else(common::time::now_ns);
    let options = query_options(&state, params.limit, params.direction.as_deref())?;
    let request = if is_log_query(&params.query) {
        QueryRequest::instant_logs(&params.query, timestamp)
    } else {
        QueryRequest::instant(&params.query, timestamp)
    };
    query_response(state, namespace, headers, request, options).await
}

async fn query_range_get(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<RangeParams>,
) -> Result<Json<Value>, ApiError> {
    execute_query_range(state, namespace, headers, params).await
}

async fn query_range_post(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    check_size(body.len(), state.config.request.max_request_bytes)?;
    let params = serde_html_form::from_bytes(&body).map_err(ApiError::bad_request)?;
    execute_query_range(state, namespace, headers, params).await
}

async fn execute_query_range(
    state: AppState,
    namespace: String,
    headers: HeaderMap,
    params: RangeParams,
) -> Result<Json<Value>, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    validate_query(&params.query)?;
    let now = common::time::now_ns();
    let end = params
        .end
        .as_deref()
        .map(parse_timestamp)
        .transpose()?
        .unwrap_or(now);
    let since = params
        .since
        .as_deref()
        .map(parse_duration_ns)
        .transpose()?
        .unwrap_or(3_600_000_000_000);
    let default_end = end.min(now);
    let start = params
        .start
        .as_deref()
        .map(parse_timestamp)
        .transpose()?
        .unwrap_or_else(|| default_end.saturating_sub(since));
    if end < start {
        return Err(ApiError::bad_request(
            "end timestamp must not be before start",
        ));
    }
    let step = params
        .step
        .as_deref()
        .map(parse_seconds_or_duration)
        .transpose()?
        .unwrap_or_else(|| {
            ((end - start) / 1_000_000_000 / 250)
                .max(1)
                .saturating_mul(1_000_000_000)
        });
    if step <= 0 || (end - start) / step > 11_000 {
        return Err(ApiError::bad_request(
            "step must be positive and produce at most 11000 points",
        ));
    }
    let options = query_options(&state, params.limit, params.direction.as_deref())?;
    let request = QueryRequest::range(&params.query, start, end, step);
    query_response(state, namespace, headers, request, options).await
}

async fn label_names(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<DiscoveryParams>,
) -> Result<Json<Value>, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    let (start, end) = discovery_range(params.start, params.end, params.since)?;
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    let names = state
        .db
        .label_names(&namespace, start, end)
        .await
        .map_err(logs_error)?;
    Ok(Json(json!({"status":"success","data":names})))
}

async fn label_values(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    Query(params): Query<DiscoveryParams>,
) -> Result<Json<Value>, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    let (start, end) = discovery_range(params.start, params.end, params.since)?;
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    let values = state
        .db
        .label_values(&namespace, &name, start, end)
        .await
        .map_err(logs_error)?;
    Ok(Json(json!({"status":"success","data":values})))
}

async fn series_get(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Result<Json<Value>, ApiError> {
    let query = query.unwrap_or_default();
    let params = serde_html_form::from_bytes(query.as_bytes()).map_err(ApiError::bad_request)?;
    execute_series(state, namespace, headers, params).await
}

async fn series_post(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    check_size(body.len(), state.config.request.max_request_bytes)?;
    let params = serde_html_form::from_bytes(&body).map_err(ApiError::bad_request)?;
    execute_series(state, namespace, headers, params).await
}

async fn execute_series(
    state: AppState,
    namespace: String,
    headers: HeaderMap,
    params: SeriesParams,
) -> Result<Json<Value>, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    let (start, end) = discovery_range(params.start, params.end, params.since)?;
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    let series = state
        .db
        .series(&namespace, &params.selectors, start, end)
        .await
        .map_err(logs_error)?;
    let data = series
        .iter()
        .map(label_map)
        .collect::<Vec<BTreeMap<String, String>>>();
    Ok(Json(json!({"status":"success","data":data})))
}

fn discovery_range(
    start: Option<String>,
    end: Option<String>,
    since: Option<String>,
) -> Result<(i64, i64), ApiError> {
    let now = common::time::now_ns();
    let end = end
        .as_deref()
        .map(parse_timestamp)
        .transpose()?
        .unwrap_or(now);
    let since = since
        .as_deref()
        .map(parse_duration_ns)
        .transpose()?
        .unwrap_or(3_600_000_000_000);
    let start = start
        .as_deref()
        .map(parse_timestamp)
        .transpose()?
        .unwrap_or_else(|| end.min(now).saturating_sub(since));
    if end < start {
        return Err(ApiError::bad_request(
            "end timestamp must not be before start",
        ));
    }
    Ok((start, end))
}

async fn query_response(
    state: AppState,
    namespace: String,
    headers: HeaderMap,
    request: QueryRequest,
    options: QueryOptions,
) -> Result<Json<Value>, ApiError> {
    let categorized = encoding_flag(&headers, CATEGORIZE_LABELS);
    let key = format!(
        "{namespace}\0{}\0{}\0{}\0{:?}\0{:?}\0{}\0{}\0{categorized}",
        request.query,
        request.start_ns,
        request.end_ns,
        request.step_ns,
        options.direction,
        options.limit,
        options.max_pages
    );
    if let Some(value) = state.cached_query(&key).await {
        return Ok(Json(value));
    }
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    let result = state
        .db
        .query(&namespace, &request, options)
        .await
        .map_err(ApiError::bad_request)?;
    let value = loki_response(result, categorized);
    state.cache_query(key, value.clone()).await;
    Ok(Json(value))
}

fn loki_response(result: QueryResult, categorized: bool) -> Value {
    let (kind, result) = match result {
        QueryResult::Streams(streams) => (
            "streams",
            Value::Array(
                streams
                    .into_iter()
                    .flat_map(|stream| stream_values(stream, categorized))
                    .collect(),
            ),
        ),
        QueryResult::Vector(samples) => (
            "vector",
            Value::Array(
                samples
                    .into_iter()
                    .map(|sample| {
                        json!({
                            "metric": label_map(&sample.labels),
                            "value": [seconds(sample.sample.timestamp_ns), prometheus_float(sample.sample.value)]
                        })
                    })
                    .collect(),
            ),
        ),
        QueryResult::Matrix(series) => (
            "matrix",
            Value::Array(
                series
                    .into_iter()
                    .map(|series| {
                        json!({
                            "metric": label_map(&series.labels),
                            "values": series.samples.into_iter().map(|sample| {
                                json!([seconds(sample.timestamp_ns), prometheus_float(sample.value)])
                            }).collect::<Vec<_>>()
                        })
                    })
                    .collect(),
            ),
        ),
        QueryResult::Scalar(sample) => (
            "scalar",
            json!([seconds(sample.timestamp_ns), prometheus_float(sample.value)]),
        ),
    };
    let mut data = json!({"resultType": kind, "result": result});
    if categorized && kind == "streams" {
        data["encodingFlags"] = json!([CATEGORIZE_LABELS]);
    }
    json!({"status":"success","data":data})
}

fn stream_values(stream: plural_logs::LogStream, categorized: bool) -> Vec<Value> {
    if categorized {
        let values = stream
            .entries
            .into_iter()
            .map(|entry| {
                let metadata = entry
                    .structured_metadata
                    .iter()
                    .map(|field| (field.name.clone(), json!(field.value)))
                    .collect::<Map<_, _>>();
                json!([
                    entry.timestamp_ns.to_string(),
                    entry.line,
                    {"structuredMetadata": metadata}
                ])
            })
            .collect::<Vec<_>>();
        return vec![json!({"stream":label_map(&stream.labels),"values":values})];
    }
    let mut groups: BTreeMap<Vec<(String, String)>, Vec<Value>> = BTreeMap::new();
    for entry in stream.entries {
        let mut labels = stream
            .labels
            .iter()
            .map(|label| (label.name.clone(), label.value.clone()))
            .collect::<BTreeMap<_, _>>();
        labels.extend(
            entry
                .structured_metadata
                .iter()
                .map(|field| (field.name.clone(), field.value.clone())),
        );
        groups
            .entry(labels.into_iter().collect())
            .or_default()
            .push(json!([entry.timestamp_ns.to_string(), entry.line]));
    }
    groups
        .into_iter()
        .map(|(labels, values)| {
            json!({"stream":labels.into_iter().collect::<BTreeMap<_,_>>(),"values":values})
        })
        .collect()
}

async fn write_batches(
    state: &AppState,
    namespace: String,
    batches: Vec<LogBatch>,
) -> Result<(), ApiError> {
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    state
        .route_write(&namespace, batches, ulid::Ulid::new().to_string())
        .await?;
    state.invalidate_queries().await;
    state.mark_dirty();
    Ok(())
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

fn query_options(
    state: &AppState,
    limit: Option<usize>,
    direction: Option<&str>,
) -> Result<QueryOptions, ApiError> {
    let limit = limit.unwrap_or(100);
    if limit == 0 || limit > state.config.request.max_query_entries {
        return Err(ApiError::bad_request(
            "limit is outside the configured range",
        ));
    }
    let direction = match direction
        .unwrap_or("backward")
        .to_ascii_lowercase()
        .as_str()
    {
        "forward" => Direction::Forward,
        "backward" => Direction::Backward,
        _ => {
            return Err(ApiError::bad_request(
                "direction must be forward or backward",
            ));
        }
    };
    Ok(QueryOptions {
        limit,
        direction,
        max_pages: state.config.request.max_query_pages,
        max_concurrency: state.config.request.query_concurrency,
        max_in_flight_bytes: state.config.request.max_in_flight_query_bytes,
    })
}

fn parse_timestamp(value: &str) -> Result<i64, ApiError> {
    if value.contains('.')
        && let Ok(seconds) = value.parse::<f64>()
    {
        if !seconds.is_finite() {
            return Err(ApiError::bad_request("timestamp must be finite"));
        }
        let whole = seconds.trunc();
        let fractional = ((seconds.fract() * 1_000.0).round() / 1_000.0) * 1_000_000_000.0;
        return Ok((whole * 1_000_000_000.0 + fractional) as i64);
    }
    if let Ok(parsed) = value.parse::<i64>() {
        return Ok(if value.len() <= 10 {
            parsed.saturating_mul(1_000_000_000)
        } else {
            parsed
        });
    }
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|value| value.timestamp_nanos_opt().unwrap_or(i64::MAX))
        .map_err(ApiError::bad_request)
}

fn parse_seconds_or_duration(value: &str) -> Result<i64, ApiError> {
    value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .map(|value| (value * 1_000_000_000.0) as i64)
        .map_or_else(|| parse_duration_ns(value), Ok)
}

fn parse_duration_ns(value: &str) -> Result<i64, ApiError> {
    let duration = common::time::parse_duration_ns(value).map_err(ApiError::bad_request)?;
    if duration <= 0 {
        return Err(ApiError::bad_request("duration is outside the valid range"));
    }
    Ok(duration)
}

fn parse_label_set(value: &str) -> Result<Labels, ApiError> {
    let value = value.trim();
    let inner = value
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
        .ok_or_else(|| ApiError::bad_request("labels must be enclosed in braces"))?;
    let mut labels = Vec::new();
    let mut rest = inner.trim();
    while !rest.is_empty() {
        let (name, after_name) = rest
            .split_once('=')
            .ok_or_else(|| ApiError::bad_request("invalid label set"))?;
        let name = name.trim();
        let after_name = after_name.trim_start();
        if !after_name.starts_with('"') {
            return Err(ApiError::bad_request("label values must be quoted"));
        }
        let mut escaped = false;
        let mut end = None;
        for (index, character) in after_name[1..].char_indices() {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                end = Some(index + 1);
                break;
            }
        }
        let end = end.ok_or_else(|| ApiError::bad_request("unterminated label value"))?;
        let decoded: String =
            serde_json::from_str(&after_name[..=end]).map_err(ApiError::bad_request)?;
        labels.push(Label::new(name, decoded));
        rest = after_name[end + 1..].trim_start();
        if let Some(next) = rest.strip_prefix(',') {
            rest = next.trim_start();
        } else if !rest.is_empty() {
            return Err(ApiError::bad_request("expected comma between labels"));
        }
    }
    Labels::new(labels).map_err(ApiError::bad_request)
}

fn json_fields(value: &Value, limit: usize) -> Result<Fields, ApiError> {
    let object = value
        .as_object()
        .ok_or_else(|| ApiError::bad_request("structured metadata must be an object"))?;
    if object.len() > limit {
        return Err(ApiError::bad_request("too many structured metadata fields"));
    }
    Fields::new(
        object
            .iter()
            .map(|(name, value)| {
                value
                    .as_str()
                    .map(|value| Field::new(name, value))
                    .ok_or_else(|| {
                        ApiError::bad_request("structured metadata values must be strings")
                    })
            })
            .collect::<Result<Vec<_>, _>>()?,
    )
    .map_err(ApiError::bad_request)
}

fn decode_content(
    headers: &HeaderMap,
    body: &[u8],
    allow_snappy: bool,
) -> Result<Vec<u8>, ApiError> {
    let encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("identity")
        .trim();
    if encoding.eq_ignore_ascii_case("identity") || encoding.is_empty() {
        return Ok(body.to_vec());
    }
    if encoding.eq_ignore_ascii_case("snappy") && allow_snappy {
        return Ok(body.to_vec());
    }
    if encoding.eq_ignore_ascii_case("gzip") {
        let mut decoded = Vec::new();
        GzDecoder::new(body)
            .read_to_end(&mut decoded)
            .map_err(ApiError::bad_request)?;
        return Ok(decoded);
    }
    Err(ApiError::unsupported_media(
        "unsupported content type or encoding",
    ))
}

fn encoding_flag(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get(ENCODING_FLAGS)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(',').any(|flag| flag.trim() == expected))
}

fn validate_query(query: &str) -> Result<(), ApiError> {
    if query.trim().is_empty() {
        Err(ApiError::bad_request("query cannot be empty"))
    } else {
        Ok(())
    }
}

fn is_log_query(query: &str) -> bool {
    query.trim_start().starts_with('{')
        && ![
            "count_over_time",
            "rate(",
            "bytes_over_time",
            "bytes_rate",
            "sum_over_time",
            "avg_over_time",
            "min_over_time",
            "max_over_time",
            "quantile_over_time",
            "first_over_time",
            "last_over_time",
            "absent_over_time",
        ]
        .iter()
        .any(|function| query.contains(function))
}

fn label_map(labels: &Labels) -> BTreeMap<String, String> {
    labels
        .iter()
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect()
}

fn seconds(timestamp_ns: i64) -> f64 {
    (timestamp_ns / 1_000_000) as f64 / 1_000.0
}

pub(crate) use server_common::ApiError;

mod push;

use push::*;

pub(crate) fn logs_error(error: plural_logs::Error) -> ApiError {
    match error {
        plural_logs::Error::Invalid(_) => ApiError::bad_request(error),
        plural_logs::Error::Backpressure => ApiError::too_many_requests(error),
        plural_logs::Error::Unavailable(_)
        | plural_logs::Error::Storage(_)
        | plural_logs::Error::Shard(_) => ApiError::unavailable(error),
        plural_logs::Error::Corrupt(_)
        | plural_logs::Error::Json(_)
        | plural_logs::Error::Compression(_)
        | plural_logs::Error::Query(_)
        | plural_logs::Error::Regex(_) => ApiError::internal(error),
    }
}

#[cfg(test)]
mod parameter_tests {
    use axum::body::to_bytes;
    use server_common::http::GoogleRpcStatus;

    use super::*;

    #[test]
    fn timestamp_parsing_matches_loki_units_and_precision() {
        assert_eq!(parse_timestamp("10").unwrap(), 10_000_000_000);
        assert_eq!(parse_timestamp("00000000001").unwrap(), 1);
        assert_eq!(parse_timestamp("10.123456").unwrap(), 10_123_000_000);
        assert_eq!(
            parse_timestamp("1970-01-01T00:00:01.000000042Z").unwrap(),
            1_000_000_042
        );
    }

    #[test]
    fn duration_parsing_accepts_seconds_and_prometheus_units() {
        assert_eq!(parse_seconds_or_duration("1.5").unwrap(), 1_500_000_000);
        assert_eq!(parse_seconds_or_duration("1m30s").unwrap(), 90_000_000_000);
        assert!(parse_seconds_or_duration("0").unwrap() == 0);
        assert!(parse_seconds_or_duration("invalid").is_err());
    }

    #[tokio::test]
    async fn loki_backpressure_is_retryable() {
        let response = logs_error(plural_logs::Error::Backpressure).into_response();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    }

    #[tokio::test]
    async fn otlp_errors_use_google_rpc_status_mappings() {
        let protobuf = logs_error(plural_logs::Error::Backpressure).into_otlp_response(false);
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
}
