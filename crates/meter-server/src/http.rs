use std::{
    collections::BTreeMap,
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
use meter::{Namespace, OtelConfig, OtelConverter, QueryValue, ShardedMeter};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};
use server_common::auth::{Permission, authorize};

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
) -> Result<StatusCode, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Write).await?;
    let series = meter::remote_write::parse_remote_write(&body).map_err(ApiError::bad_request)?;
    let request_id = request_id(&headers, &body);
    state
        .route_write(
            &namespace,
            series,
            state.config.write.durability,
            request_id,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn otlp_http(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match otlp_http_result(&state, namespace, headers, body).await {
        Ok(response) => response,
        Err(error) => error.into_otlp_response(false),
    }
}

async fn otlp_http_result(
    state: &AppState,
    namespace: String,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    authorize_namespace(state, &namespace, &headers, Permission::Write).await?;
    let id = request_id(&headers, &body);
    let request = ExportMetricsServiceRequest::decode(body).map_err(ApiError::bad_request)?;
    let series = OtelConverter::new(OtelConfig::default())
        .convert(&request)
        .map_err(ApiError::bad_request)?;
    state
        .route_write(&namespace, series, state.config.write.durability, id)
        .await?;
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
    time: Option<f64>,
}

async fn query(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<InstantQuery>,
) -> Result<Json<Value>, ApiError> {
    execute_query(state, namespace, headers, params).await
}

async fn query_form(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let params = serde_html_form::from_bytes(&body).map_err(ApiError::bad_request)?;
    execute_query(state, namespace, headers, params).await
}

async fn execute_query(
    state: AppState,
    namespace: String,
    headers: HeaderMap,
    params: InstantQuery,
) -> Result<Json<Value>, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    let reader = reader(&state, &namespace).await?;
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    let at = params.time.map(system_time);
    let expression = params.query;
    let value = tokio::spawn(async move { reader.query(&namespace, &expression, at).await })
        .await
        .map_err(ApiError::internal)?
        .map_err(ApiError::bad_request)?;
    Ok(Json(prom_query(value)))
}

#[derive(Deserialize)]
struct RangeQuery {
    query: String,
    start: f64,
    end: f64,
    step: f64,
}

async fn query_range(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<RangeQuery>,
) -> Result<Json<Value>, ApiError> {
    execute_query_range(state, namespace, headers, params).await
}

async fn query_range_form(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let params = serde_html_form::from_bytes(&body).map_err(ApiError::bad_request)?;
    execute_query_range(state, namespace, headers, params).await
}

async fn execute_query_range(
    state: AppState,
    namespace: String,
    headers: HeaderMap,
    params: RangeQuery,
) -> Result<Json<Value>, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Read).await?;
    if params.step <= 0.0 || params.end < params.start {
        return Err(ApiError::bad_request("invalid range or step"));
    }
    let reader = reader(&state, &namespace).await?;
    let namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
    let expression = params.query;
    let range = RangeInclusive::new(system_time(params.start), system_time(params.end));
    let step = Duration::from_secs_f64(params.step);
    let values = tokio::spawn(async move {
        reader
            .query_range(&namespace, &expression, range, step)
            .await
    })
    .await
    .map_err(ApiError::internal)?
    .map_err(ApiError::bad_request)?;
    Ok(Json(
        json!({"status":"success","data":{"resultType":"matrix","result":
            values.into_iter().map(|sample| json!({
                "metric": sample.labels,
                "values": sample.samples.into_iter().map(|(time, value)| json!([time as f64 / 1000.0, value.to_string()])).collect::<Vec<_>>()
            })).collect::<Vec<_>>()
        }}),
    ))
}

#[derive(Default, Deserialize)]
struct MatchQuery {
    #[serde(rename = "match[]", default)]
    matches: Vec<String>,
    start: Option<f64>,
    end: Option<f64>,
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
    let range = time_range(params.start, params.end);
    let meter_namespace = Namespace::new(&namespace).map_err(ApiError::bad_request)?;
    let data = reader(&state, &namespace)
        .await?
        .series(&meter_namespace, &refs, range)
        .await
        .map_err(ApiError::bad_request)?;
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
    let meter_namespace = Namespace::new(&namespace).map_err(ApiError::bad_request)?;
    let data = reader(&state, &namespace)
        .await?
        .labels(
            &meter_namespace,
            (!refs.is_empty()).then_some(refs.as_slice()),
            time_range(params.start, params.end),
        )
        .await
        .map_err(ApiError::bad_request)?;
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
    let meter_namespace = Namespace::new(&namespace).map_err(ApiError::bad_request)?;
    let data = reader(&state, &namespace)
        .await?
        .label_values(
            &meter_namespace,
            &name,
            (!refs.is_empty()).then_some(refs.as_slice()),
            time_range(params.start, params.end),
        )
        .await
        .map_err(ApiError::bad_request)?;
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
        .map_err(ApiError::bad_request)?;
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
            .map_err(ApiError::bad_request)?;
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

fn system_time(seconds: f64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs_f64(seconds.max(0.0))
}

fn time_range(start: Option<f64>, end: Option<f64>) -> RangeInclusive<SystemTime> {
    RangeInclusive::new(
        start.map(system_time).unwrap_or(UNIX_EPOCH),
        end.map(system_time).unwrap_or_else(SystemTime::now),
    )
}

fn prom_query(value: QueryValue) -> Value {
    match value {
        QueryValue::Scalar {
            timestamp_ms,
            value,
        } => {
            json!({"status":"success","data":{"resultType":"scalar","result":[timestamp_ms as f64 / 1000.0,value.to_string()]}})
        }
        QueryValue::Vector(samples) => {
            json!({"status":"success","data":{"resultType":"vector","result":
                samples.into_iter().map(|sample| json!({"metric":sample.labels,"value":[sample.timestamp_ms as f64 / 1000.0,sample.value.to_string()]})).collect::<Vec<_>>()
            }})
        }
        QueryValue::Matrix(samples) => {
            json!({"status":"success","data":{"resultType":"matrix","result":
                samples.into_iter().map(|sample| json!({"metric":sample.labels,"values":sample.samples.into_iter().map(|(time,value)| json!([time as f64 / 1000.0,value.to_string()])).collect::<Vec<_>>()})).collect::<Vec<_>>()
            }})
        }
    }
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
