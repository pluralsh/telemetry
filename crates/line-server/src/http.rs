use std::{
    collections::BTreeMap,
    io::Read,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use flate2::read::GzDecoder;
use line::{
    Direction, Field, Fields, Label, Labels, LogBatch, LogEntry, Namespace, QueryOptions,
    QueryRequest, QueryResult,
};
use meter_server::auth::{Permission, authorize};
use opentelemetry_proto::tonic::{
    collector::logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
    common::v1::{AnyValue, KeyValue, any_value},
};
use prost::Message;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::{AppState, config::NamespaceConfig};

const ENCODING_FLAGS: &str = "x-loki-response-encoding-flags";
const CATEGORIZE_LABELS: &str = "categorize-labels";

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/-/healthy", get(|| async { StatusCode::OK }))
        .route(
            "/-/ready",
            get(|State(state): State<AppState>| async move {
                if state.is_ready() {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        )
        .route(
            "/read/ns/{namespace}/loki/api/v1/query",
            get(query_get).post(query_post),
        )
        .route(
            "/read/ns/{namespace}/loki/api/v1/query_range",
            get(query_range_get).post(query_range_post),
        )
        .route("/write/ns/{namespace}/loki/api/v1/push", post(loki_push))
        .route("/write/ns/{namespace}/otlp/v1/logs", post(otlp_logs))
        .with_state(state)
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
    check_size(&state, body.len())?;
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
        .unwrap_or_else(now_ns);
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
    check_size(&state, body.len())?;
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
    let now = now_ns();
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
                            "value": [seconds(sample.sample.timestamp_ns), number(sample.sample.value)]
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
                                json!([seconds(sample.timestamp_ns), number(sample.value)])
                            }).collect::<Vec<_>>()
                        })
                    })
                    .collect(),
            ),
        ),
        QueryResult::Scalar(sample) => (
            "scalar",
            json!([seconds(sample.timestamp_ns), number(sample.value)]),
        ),
    };
    let mut data = json!({"resultType": kind, "result": result});
    if categorized && kind == "streams" {
        data["encodingFlags"] = json!([CATEGORIZE_LABELS]);
    }
    json!({"status":"success","data":data})
}

fn stream_values(stream: line::LogStream, categorized: bool) -> Vec<Value> {
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

async fn loki_push(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Write).await?;
    check_size(&state, body.len())?;
    let content_type = content_type(&headers).unwrap_or("application/x-protobuf");
    let protobuf = matches!(
        content_type,
        "application/x-protobuf" | "application/vnd.google.protobuf"
    );
    let body = decode_content(&headers, &body, protobuf)?;
    check_size(&state, body.len())?;
    let batches = if content_type == "application/json" {
        parse_json_push(&body, state.config.request.max_structured_metadata_fields)?
    } else if protobuf {
        parse_protobuf_push(&body, state.config.request.max_structured_metadata_fields)?
    } else {
        return Err(ApiError::unsupported_media());
    };
    write_batches(&state, namespace, batches).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct JsonPush {
    streams: Vec<JsonStream>,
}

#[derive(Deserialize)]
struct JsonStream {
    stream: BTreeMap<String, String>,
    values: Vec<Vec<Value>>,
}

fn parse_json_push(body: &[u8], metadata_limit: usize) -> Result<Vec<LogBatch>, ApiError> {
    let request: JsonPush = serde_json::from_slice(body).map_err(ApiError::bad_request)?;
    request
        .streams
        .into_iter()
        .map(|stream| {
            let labels = Labels::new(
                stream
                    .stream
                    .into_iter()
                    .map(|(name, value)| Label::new(name, value))
                    .collect(),
            )
            .map_err(ApiError::bad_request)?;
            let entries = stream
                .values
                .into_iter()
                .map(|value| json_entry(value, metadata_limit))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogBatch::new(labels, entries))
        })
        .collect()
}

fn json_entry(value: Vec<Value>, metadata_limit: usize) -> Result<LogEntry, ApiError> {
    if !(2..=3).contains(&value.len()) {
        return Err(ApiError::bad_request(
            "Loki values must contain timestamp, line, and optional metadata",
        ));
    }
    let timestamp = value[0]
        .as_str()
        .ok_or_else(|| ApiError::bad_request("Loki timestamp must be a string"))?
        .parse::<i64>()
        .map_err(ApiError::bad_request)?;
    let line = value[1]
        .as_str()
        .ok_or_else(|| ApiError::bad_request("Loki line must be a string"))?
        .to_owned();
    let fields = value
        .get(2)
        .map(|value| json_fields(value, metadata_limit))
        .transpose()?
        .unwrap_or_default();
    Ok(LogEntry::with_structured_metadata(timestamp, line, fields))
}

#[derive(Clone, PartialEq, Message)]
struct PushRequest {
    #[prost(message, repeated, tag = "1")]
    streams: Vec<StreamAdapter>,
}

#[derive(Clone, PartialEq, Message)]
struct StreamAdapter {
    #[prost(string, tag = "1")]
    labels: String,
    #[prost(message, repeated, tag = "2")]
    entries: Vec<EntryAdapter>,
}

#[derive(Clone, PartialEq, Message)]
struct EntryAdapter {
    #[prost(message, optional, tag = "1")]
    timestamp: Option<ProtoTimestamp>,
    #[prost(string, tag = "2")]
    line: String,
    #[prost(message, repeated, tag = "3")]
    structured_metadata: Vec<LabelPair>,
}

#[derive(Clone, Copy, PartialEq, Message)]
struct ProtoTimestamp {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(int32, tag = "2")]
    nanos: i32,
}

#[derive(Clone, PartialEq, Message)]
struct LabelPair {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

fn parse_protobuf_push(body: &[u8], metadata_limit: usize) -> Result<Vec<LogBatch>, ApiError> {
    let decoded = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(ApiError::bad_request)?;
    let request = PushRequest::decode(decoded.as_slice()).map_err(ApiError::bad_request)?;
    request
        .streams
        .into_iter()
        .map(|stream| {
            let labels = parse_label_set(&stream.labels)?;
            let entries = stream
                .entries
                .into_iter()
                .map(|entry| {
                    if entry.structured_metadata.len() > metadata_limit {
                        return Err(ApiError::bad_request("too many structured metadata fields"));
                    }
                    let timestamp = entry
                        .timestamp
                        .ok_or_else(|| ApiError::bad_request("entry timestamp is required"))?;
                    if !(0..1_000_000_000).contains(&timestamp.nanos) {
                        return Err(ApiError::bad_request("invalid timestamp nanos"));
                    }
                    let fields = Fields::new(
                        entry
                            .structured_metadata
                            .into_iter()
                            .map(|field| Field::new(field.name, field.value))
                            .collect(),
                    )
                    .map_err(ApiError::bad_request)?;
                    Ok(LogEntry::with_structured_metadata(
                        timestamp
                            .seconds
                            .saturating_mul(1_000_000_000)
                            .saturating_add(i64::from(timestamp.nanos)),
                        entry.line,
                        fields,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogBatch::new(labels, entries))
        })
        .collect()
}

async fn otlp_logs(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Write).await?;
    check_size(&state, body.len())?;
    let body = decode_content(&headers, &body, false)?;
    check_size(&state, body.len())?;
    let json_request = content_type(&headers) == Some("application/json");
    let request: ExportLogsServiceRequest = if json_request {
        serde_json::from_slice(&body).map_err(ApiError::bad_request)?
    } else if matches!(
        content_type(&headers),
        Some("application/x-protobuf" | "application/protobuf" | "application/octet-stream")
    ) {
        ExportLogsServiceRequest::decode(body.as_slice()).map_err(ApiError::bad_request)?
    } else {
        return Err(ApiError::unsupported_media());
    };
    let batches = otlp_batches(request, state.config.request.max_structured_metadata_fields)?;
    write_batches(&state, namespace, batches).await?;
    let response = ExportLogsServiceResponse {
        partial_success: None,
    };
    if json_request {
        // OTLP/JSON uses the protobuf JSON mapping, where an absent
        // partial_success field is omitted rather than serialized as null.
        Ok((StatusCode::OK, Json(json!({}))).into_response())
    } else {
        let mut encoded = Vec::new();
        response.encode(&mut encoded).map_err(ApiError::internal)?;
        Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/x-protobuf")],
            encoded,
        )
            .into_response())
    }
}

fn otlp_batches(
    request: ExportLogsServiceRequest,
    metadata_limit: usize,
) -> Result<Vec<LogBatch>, ApiError> {
    let mut batches = Vec::new();
    for resource_logs in request.resource_logs {
        let resource = resource_logs
            .resource
            .map(|resource| resource.attributes)
            .unwrap_or_default();
        let labels = Labels::new(
            resource
                .iter()
                .map(|attribute| {
                    Label::new(
                        sanitize_label(&attribute.key),
                        attribute.value.as_ref().map(any_value).unwrap_or_default(),
                    )
                })
                .collect(),
        )
        .map_err(ApiError::bad_request)?;
        for scope_logs in resource_logs.scope_logs {
            let mut scope_fields = Vec::new();
            if let Some(scope) = scope_logs.scope {
                if !scope.name.is_empty() {
                    scope_fields.push(Field::new("scope_name", scope.name));
                }
                if !scope.version.is_empty() {
                    scope_fields.push(Field::new("scope_version", scope.version));
                }
                scope_fields.extend(key_values(scope.attributes));
            }
            let mut entries = Vec::new();
            for record in scope_logs.log_records {
                let mut fields = scope_fields.clone();
                fields.extend(key_values(record.attributes));
                if !record.severity_text.is_empty() {
                    fields.push(Field::new("severity_text", record.severity_text));
                }
                if !record.trace_id.is_empty() {
                    fields.push(Field::new("trace_id", hex(&record.trace_id)));
                }
                if !record.span_id.is_empty() {
                    fields.push(Field::new("span_id", hex(&record.span_id)));
                }
                if fields.len() > metadata_limit {
                    return Err(ApiError::bad_request("too many structured metadata fields"));
                }
                let fields = Fields::new(fields).map_err(ApiError::bad_request)?;
                let timestamp = if record.time_unix_nano != 0 {
                    record.time_unix_nano
                } else {
                    record.observed_time_unix_nano
                };
                let timestamp = i64::try_from(timestamp)
                    .map_err(|_| ApiError::bad_request("OTLP timestamp exceeds i64"))?;
                let line = record.body.as_ref().map(any_value).unwrap_or_default();
                entries.push(LogEntry::with_structured_metadata(timestamp, line, fields));
            }
            if !entries.is_empty() {
                batches.push(LogBatch::new(labels.clone(), entries));
            }
        }
    }
    Ok(batches)
}

fn key_values(values: Vec<KeyValue>) -> Vec<Field> {
    values
        .into_iter()
        .map(|value| {
            Field::new(
                sanitize_label(&value.key),
                value.value.as_ref().map(any_value).unwrap_or_default(),
            )
        })
        .collect()
}

fn any_value(value: &AnyValue) -> String {
    match value.value.as_ref() {
        Some(any_value::Value::StringValue(value)) => value.clone(),
        Some(any_value::Value::BoolValue(value)) => value.to_string(),
        Some(any_value::Value::IntValue(value)) => value.to_string(),
        Some(any_value::Value::DoubleValue(value)) => number(*value),
        Some(any_value::Value::BytesValue(value)) => hex(value),
        Some(any_value::Value::ArrayValue(value)) => {
            serde_json::to_string(&value.values.iter().map(any_value).collect::<Vec<String>>())
                .unwrap_or_default()
        }
        Some(any_value::Value::KvlistValue(value)) => serde_json::to_string(
            &value
                .values
                .iter()
                .map(|item| {
                    (
                        item.key.clone(),
                        item.value.as_ref().map(any_value).unwrap_or_default(),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        )
        .unwrap_or_default(),
        None => String::new(),
    }
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
    let mut total = 0f64;
    let mut number = String::new();
    let mut cursor = 0;
    let bytes = value.as_bytes();
    while cursor < bytes.len() {
        while cursor < bytes.len() && (bytes[cursor].is_ascii_digit() || bytes[cursor] == b'.') {
            number.push(char::from(bytes[cursor]));
            cursor += 1;
        }
        if number.is_empty() {
            return Err(ApiError::bad_request("invalid duration"));
        }
        let amount = number.parse::<f64>().map_err(ApiError::bad_request)?;
        number.clear();
        let (unit, width) = if value[cursor..].starts_with("ms") {
            (1_000_000f64, 2)
        } else if value[cursor..].starts_with("us") || value[cursor..].starts_with("µs") {
            (
                1_000f64,
                value[cursor..].chars().next().map_or(2, char::len_utf8) + 1,
            )
        } else {
            let unit = match bytes.get(cursor).copied() {
                Some(b'n') if bytes.get(cursor + 1) == Some(&b's') => {
                    cursor += 1;
                    1f64
                }
                Some(b's') => 1_000_000_000f64,
                Some(b'm') => 60_000_000_000f64,
                Some(b'h') => 3_600_000_000_000f64,
                Some(b'd') => 86_400_000_000_000f64,
                Some(b'w') => 604_800_000_000_000f64,
                _ => return Err(ApiError::bad_request("invalid duration unit")),
            };
            (unit, 1)
        };
        cursor += width;
        total += amount * unit;
    }
    if !total.is_finite() || total <= 0.0 || total > i64::MAX as f64 {
        return Err(ApiError::bad_request("duration is outside the valid range"));
    }
    Ok(total as i64)
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
    Err(ApiError::unsupported_media())
}

fn content_type(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn check_size(state: &AppState, size: usize) -> Result<(), ApiError> {
    if size > state.config.request.max_request_bytes {
        Err(ApiError::too_large())
    } else {
        Ok(())
    }
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

fn number(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "+Inf".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_owned()
    } else {
        value.to_string()
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn sanitize_label(value: &str) -> String {
    let mut value = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if value
        .as_bytes()
        .first()
        .is_some_and(|byte| byte.is_ascii_digit())
    {
        value.insert(0, '_');
    }
    value
}

fn hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: error.to_string(),
        }
    }

    pub(crate) fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    }

    fn not_found(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: error.to_string(),
        }
    }

    pub(crate) fn unavailable(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: error.to_string(),
        }
    }

    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: "authentication required".to_owned(),
        }
    }

    fn unsupported_media() -> Self {
        Self {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            message: "unsupported content type or encoding".to_owned(),
        }
    }

    fn too_large() -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: "request body exceeds configured limit".to_owned(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "status":"error",
                "errorType": if self.status.is_server_error() {"server"} else {"bad_data"},
                "error":self.message
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod parameter_tests {
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
}
