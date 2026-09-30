//! Log ingestion: Loki JSON/protobuf push and OTLP logs.

use super::*;

pub(super) async fn loki_push(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Write).await?;
    check_size(body.len(), state.config.request.max_request_bytes)?;
    let content_type = content_type(&headers).unwrap_or("application/x-protobuf");
    let protobuf = matches!(
        content_type,
        "application/x-protobuf" | "application/vnd.google.protobuf"
    );
    let body = decode_content(&headers, &body, protobuf)?;
    check_size(body.len(), state.config.request.max_request_bytes)?;
    let batches = if content_type == "application/json" {
        parse_json_push(&body, state.config.request.max_structured_metadata_fields)?
    } else if protobuf {
        parse_protobuf_push(&body, state.config.request.max_structured_metadata_fields)?
    } else {
        return Err(ApiError::unsupported_media(
            "unsupported content type or encoding",
        ));
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

pub(super) async fn otlp_logs(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let json_request = content_type(&headers) == Some("application/json");
    match otlp_logs_result(&state, namespace, headers, body).await {
        Ok(response) => response,
        Err(error) => error.into_otlp_response(json_request),
    }
}

async fn otlp_logs_result(
    state: &AppState,
    namespace: String,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    authorize_namespace(state, &namespace, &headers, Permission::Write).await?;
    check_size(body.len(), state.config.request.max_request_bytes)?;
    let body = decode_content(&headers, &body, false)?;
    check_size(body.len(), state.config.request.max_request_bytes)?;
    let json_request = content_type(&headers) == Some("application/json");
    let request: ExportLogsServiceRequest = if json_request {
        serde_json::from_slice(&body).map_err(ApiError::bad_request)?
    } else if matches!(
        content_type(&headers),
        Some("application/x-protobuf" | "application/protobuf" | "application/octet-stream")
    ) {
        ExportLogsServiceRequest::decode(body.as_slice()).map_err(ApiError::bad_request)?
    } else {
        return Err(ApiError::unsupported_media(
            "unsupported content type or encoding",
        ));
    };
    let batches = otlp_batches(request, state.config.request.max_structured_metadata_fields)?;
    write_batches(state, namespace, batches).await?;
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
                        sanitize_label_name(&attribute.key),
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
                sanitize_label_name(&value.key),
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
        Some(any_value::Value::DoubleValue(value)) => prometheus_float(*value),
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
