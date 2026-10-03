//! Elasticsearch `_bulk` ingestion for Logstash-format shippers such as
//! Fluent Bit, Fluentd, Vector, and Logstash.

use std::time::Instant;

use super::*;
use crate::config::ElasticsearchConfig;

/// Reported to clients that pick a protocol from the server version.
const COMPATIBLE_VERSION: &str = "8.11.0";
const PRODUCT_HEADER: &str = "x-elastic-product";

#[derive(Debug, Default, Deserialize)]
pub(super) struct BulkParams {
    #[serde(rename = "_msg_field")]
    msg_field: Option<String>,
    #[serde(rename = "_time_field")]
    time_field: Option<String>,
    #[serde(rename = "_stream_fields")]
    stream_fields: Option<String>,
}

pub(super) async fn bulk(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    Query(params): Query<BulkParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    bulk_result(&state, namespace, None, params, &headers, &body).await
}

pub(super) async fn index_bulk(
    State(state): State<AppState>,
    Path((namespace, index)): Path<(String, String)>,
    Query(params): Query<BulkParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    bulk_result(&state, namespace, Some(index), params, &headers, &body).await
}

/// `GET /`, which clients use to detect the server version and product.
pub(super) async fn info(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Write).await?;
    Ok(elastic_response(json!({
        "name": "plural-logs",
        "cluster_name": "plural-logs",
        "version": {
            "number": COMPATIBLE_VERSION,
            "build_flavor": "default",
            "minimum_wire_compatibility_version": "7.17.0",
            "minimum_index_compatibility_version": "7.0.0"
        },
        "tagline": "You Know, for Search"
    })))
}

/// `GET /_cluster/health`, used as a liveness check by Vector and others.
pub(super) async fn cluster_health(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorize_namespace(&state, &namespace, &headers, Permission::Write).await?;
    Ok(elastic_response(
        json!({"cluster_name": "plural-logs", "status": "green", "timed_out": false}),
    ))
}

fn elastic_response(body: Value) -> Response {
    ([(PRODUCT_HEADER, "Elasticsearch")], Json(body)).into_response()
}

async fn bulk_result(
    state: &AppState,
    namespace: String,
    default_index: Option<String>,
    params: BulkParams,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response, ApiError> {
    authorize_namespace(state, &namespace, headers, Permission::Write).await?;
    check_content_encoding(headers, false)?;
    let started = Instant::now();
    let mapping = Mapping::new(
        &state.config.elasticsearch,
        params,
        state.config.request.max_structured_metadata_fields,
    );
    let body = std::str::from_utf8(body).map_err(ApiError::bad_request)?;
    let received_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX);
    let mut lines = body.lines().filter(|line| !line.trim().is_empty());
    let mut streams = BTreeMap::<Labels, Vec<LogEntry>>::new();
    let mut items = Vec::new();
    let mut errors = false;
    while let Some(line) = lines.next() {
        let (action, meta) = parse_action(line)?;
        let index = meta.index.or_else(|| default_index.clone());
        let outcome = match action {
            Action::Index | Action::Create => {
                let document = lines
                    .next()
                    .ok_or_else(|| ApiError::bad_request("bulk action is missing its document"))?;
                mapping
                    .entry(index.as_deref().unwrap_or_default(), document, received_ns)
                    .map(|(labels, entry)| streams.entry(labels).or_default().push(entry))
            }
            Action::Update => {
                lines.next();
                Err("update is not supported; logs are append-only".to_owned())
            }
            Action::Delete => Err("delete is not supported; logs are append-only".to_owned()),
        };
        errors |= outcome.is_err();
        items.push(bulk_item(action, index, meta.id, outcome));
    }
    if !streams.is_empty() {
        let batches = streams
            .into_iter()
            .map(|(labels, entries)| LogBatch::new(labels, entries))
            .collect();
        write_batches(state, namespace, batches).await?;
    }
    Ok(elastic_response(json!({
        "took": started.elapsed().as_millis() as u64,
        "errors": errors,
        "items": items,
    })))
}

#[derive(Clone, Copy)]
enum Action {
    Index,
    Create,
    Update,
    Delete,
}

impl Action {
    fn name(self) -> &'static str {
        match self {
            Self::Index => "index",
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

#[derive(Default, Deserialize)]
struct ActionMeta {
    #[serde(rename = "_index")]
    index: Option<String>,
    #[serde(rename = "_id")]
    id: Option<String>,
}

/// Malformed action lines fail the whole request, as in Elasticsearch,
/// since the document pairing after them can no longer be trusted.
fn parse_action(line: &str) -> Result<(Action, ActionMeta), ApiError> {
    let malformed = || ApiError::bad_request(format!("malformed bulk action line: {line}"));
    let value: BTreeMap<String, Value> = serde_json::from_str(line).map_err(|_| malformed())?;
    let mut entries = value.into_iter();
    let (Some((name, meta)), None) = (entries.next(), entries.next()) else {
        return Err(malformed());
    };
    let action = match name.as_str() {
        "index" => Action::Index,
        "create" => Action::Create,
        "update" => Action::Update,
        "delete" => Action::Delete,
        _ => return Err(malformed()),
    };
    let meta = serde_json::from_value(meta).map_err(|_| malformed())?;
    Ok((action, meta))
}

fn bulk_item(
    action: Action,
    index: Option<String>,
    id: Option<String>,
    outcome: Result<(), String>,
) -> Value {
    let id = id.unwrap_or_else(|| ulid::Ulid::new().to_string());
    let result = match outcome {
        Ok(()) => json!({
            "_index": index,
            "_id": id,
            "_version": 1,
            "result": "created",
            "_shards": {"total": 1, "successful": 1, "failed": 0},
            "status": 201,
        }),
        Err(reason) => json!({
            "_index": index,
            "_id": id,
            "status": 400,
            "error": {"type": "document_parsing_exception", "reason": reason},
        }),
    };
    json!({ action.name(): result })
}

struct Mapping {
    message_fields: Vec<String>,
    time_field: String,
    stream_fields: Vec<String>,
    metadata_limit: usize,
}

impl Mapping {
    fn new(config: &ElasticsearchConfig, params: BulkParams, metadata_limit: usize) -> Self {
        Self {
            message_fields: params
                .msg_field
                .map(|value| field_list(&value))
                .unwrap_or_else(|| config.message_fields.clone()),
            time_field: params
                .time_field
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| config.time_field.clone()),
            stream_fields: params
                .stream_fields
                .map(|value| field_list(&value))
                .unwrap_or_else(|| config.stream_fields.clone()),
            metadata_limit,
        }
    }

    /// Maps one document; errors are reported per bulk item.
    fn entry(
        &self,
        index: &str,
        document: &str,
        received_ns: i64,
    ) -> Result<(Labels, LogEntry), String> {
        let Value::Object(mut document) =
            serde_json::from_str(document).map_err(|error| error.to_string())?
        else {
            return Err("document must be a JSON object".to_owned());
        };
        let timestamp = match take_path(&mut document, &self.time_field) {
            None | Some(Value::Null) => received_ns,
            Some(value) => parse_time(&value)?,
        };

        let mut labels = BTreeMap::new();
        let index = index_base(index);
        if !index.is_empty() {
            labels.insert("index".to_owned(), index.to_owned());
        }
        for field in &self.stream_fields {
            if let Some(value) = take_path(&mut document, field).as_ref().and_then(scalar) {
                labels.insert(sanitize_label_name(field), value);
            }
        }
        let labels = Labels::new(
            labels
                .into_iter()
                .map(|(name, value)| Label::new(name, value))
                .collect(),
        )
        .map_err(|error| error.to_string())?;

        let message = self
            .message_fields
            .iter()
            .find_map(|field| take_path(&mut document, field));
        let Some(message) = message else {
            let line = Value::Object(document).to_string();
            return Ok((labels, LogEntry::new(timestamp, line)));
        };
        let line = match message {
            Value::String(line) => line,
            other => other.to_string(),
        };
        let mut fields = BTreeMap::new();
        flatten("", Value::Object(document), &mut fields);
        if fields.len() > self.metadata_limit {
            return Err(format!(
                "document has {} fields; the structured metadata limit is {}",
                fields.len(),
                self.metadata_limit
            ));
        }
        let fields = Fields::new(
            fields
                .into_iter()
                .map(|(name, value)| Field::new(name, value))
                .collect(),
        )
        .map_err(|error| error.to_string())?;
        Ok((
            labels,
            LogEntry::with_structured_metadata(timestamp, line, fields),
        ))
    }
}

fn field_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|field| !field.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

/// Removes `path` from the document, matching a literal dotted key before
/// walking nested objects, since shippers emit both shapes.
fn take_path(document: &mut serde_json::Map<String, Value>, path: &str) -> Option<Value> {
    if let Some(value) = document.remove(path) {
        return Some(value);
    }
    let (head, rest) = path.split_once('.')?;
    match document.get_mut(head)? {
        Value::Object(child) => take_path(child, rest),
        _ => None,
    }
}

fn scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

/// Flattens nested objects into sanitized `a_b_c` names. Arrays are kept as
/// JSON, nulls and empty objects are dropped, and the first of two names that
/// sanitize alike wins.
fn flatten(prefix: &str, value: Value, fields: &mut BTreeMap<String, String>) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let name = if prefix.is_empty() {
                    key
                } else {
                    format!("{prefix}.{key}")
                };
                flatten(&name, value, fields);
            }
        }
        Value::Null => {}
        value => {
            let value = match value {
                Value::String(value) => value,
                other => other.to_string(),
            };
            fields.entry(sanitize_label_name(prefix)).or_insert(value);
        }
    }
}

/// Elasticsearch's default date format: ISO 8601 or epoch milliseconds.
fn parse_time(value: &Value) -> Result<i64, String> {
    let invalid = || format!("unsupported timestamp {value}");
    let from_millis = |millis: f64| {
        let whole = millis.trunc();
        if !whole.is_finite() || whole.abs() >= (i64::MAX / 1_000_000) as f64 {
            return Err(invalid());
        }
        Ok((whole as i64) * 1_000_000 + (millis.fract() * 1_000_000.0).round() as i64)
    };
    let from_text = |text: &str| match text.parse::<i64>() {
        Ok(millis) => millis.checked_mul(1_000_000).ok_or_else(invalid),
        Err(_) => text
            .parse::<f64>()
            .map_err(|_| invalid())
            .and_then(from_millis),
    };
    match value {
        Value::Number(number) => from_text(&number.to_string()),
        Value::String(text) => {
            if let Ok(time) = chrono::DateTime::parse_from_rfc3339(text) {
                return time.timestamp_nanos_opt().ok_or_else(invalid);
            }
            if let Ok(time) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f") {
                return time.and_utc().timestamp_nanos_opt().ok_or_else(invalid);
            }
            from_text(text)
        }
        _ => Err(invalid()),
    }
}

/// Strips a trailing date such as Logstash's `-2024.01.31` so daily indices
/// share one stream label.
fn index_base(index: &str) -> &str {
    let Some(split) = index.len().checked_sub(10) else {
        return index;
    };
    let Some(date) = index.get(split..) else {
        return index;
    };
    let bytes = date.as_bytes();
    let is_date = bytes
        .iter()
        .enumerate()
        .all(|(position, byte)| match position {
            4 | 7 => matches!(byte, b'.' | b'-'),
            _ => byte.is_ascii_digit(),
        })
        && bytes[4] == bytes[7];
    if !is_date {
        return index;
    }
    let base = &index[..split];
    base.strip_suffix(['-', '_', '.']).unwrap_or(base)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapping() -> Mapping {
        Mapping::new(&ElasticsearchConfig::default(), BulkParams::default(), 8)
    }

    #[test]
    fn index_base_strips_logstash_dates() {
        assert_eq!(index_base("logstash-2024.01.31"), "logstash");
        assert_eq!(index_base("app_2024-01-31"), "app");
        assert_eq!(index_base("2024.01.31"), "");
        assert_eq!(index_base("logs-2024.01-31"), "logs-2024.01-31");
        assert_eq!(index_base("short"), "short");
        assert_eq!(index_base("ünïcødé-2024.01.31"), "ünïcødé");
    }

    #[test]
    fn timestamps_accept_iso_and_epoch_millis() {
        let expected = 1_706_659_200_123_000_000;
        assert_eq!(
            parse_time(&json!("2024-01-31T00:00:00.123Z")).unwrap(),
            expected
        );
        assert_eq!(
            parse_time(&json!("2024-01-31T00:00:00.123")).unwrap(),
            expected
        );
        assert_eq!(parse_time(&json!(1_706_659_200_123u64)).unwrap(), expected);
        assert_eq!(parse_time(&json!("1706659200123")).unwrap(), expected);
        assert_eq!(
            parse_time(&json!(1_706_659_200_123.5)).unwrap(),
            expected + 500_000
        );
        assert_eq!(
            parse_time(&json!(1_706_659_200_123.5)).unwrap(),
            expected + 500_000
        );
        assert!(parse_time(&json!("yesterday")).is_err());
        assert!(parse_time(&json!(true)).is_err());
    }

    #[test]
    fn documents_map_message_labels_and_flattened_metadata() {
        let mapping = Mapping::new(
            &ElasticsearchConfig::default(),
            BulkParams {
                stream_fields: Some("kubernetes.namespace_name, host".to_owned()),
                ..BulkParams::default()
            },
            8,
        );
        let (labels, entry) = mapping
            .entry(
                "logstash-2024.01.31",
                r#"{"@timestamp":"2024-01-31T00:00:00Z","log":"hello","host":"node-1",
                    "kubernetes":{"namespace_name":"prod","pod_name":"api-0","labels":{"app.kubernetes.io/name":"api"}},
                    "level":"info","count":3,"tags":["a","b"],"missing":null}"#,
                0,
            )
            .unwrap();
        assert_eq!(
            labels,
            Labels::new(vec![
                Label::new("host", "node-1"),
                Label::new("index", "logstash"),
                Label::new("kubernetes_namespace_name", "prod"),
            ])
            .unwrap()
        );
        assert_eq!(entry.timestamp_ns, 1_706_659_200_000_000_000);
        assert_eq!(entry.line, "hello");
        let fields = entry
            .structured_metadata
            .iter()
            .map(|field| (field.name.as_str(), field.value.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            fields,
            vec![
                ("count", "3"),
                ("kubernetes_labels_app_kubernetes_io_name", "api"),
                ("kubernetes_pod_name", "api-0"),
                ("level", "info"),
                ("tags", r#"["a","b"]"#),
            ]
        );
    }

    #[test]
    fn documents_without_a_message_are_stored_as_json() {
        let (labels, entry) = mapping()
            .entry("", r#"{"user":"ada","status":200}"#, 42)
            .unwrap();
        assert!(labels.is_empty());
        assert_eq!(entry.timestamp_ns, 42);
        assert_eq!(entry.line, r#"{"status":200,"user":"ada"}"#);
        assert!(entry.structured_metadata.is_empty());
    }

    #[test]
    fn invalid_documents_fail_per_item() {
        let mapping = mapping();
        assert!(mapping.entry("", "[1]", 0).is_err());
        assert!(mapping.entry("", "{not json", 0).is_err());
        assert!(
            mapping
                .entry("", r#"{"message":"x","@timestamp":"later"}"#, 0)
                .is_err()
        );
        let wide = (0..9)
            .map(|index| format!(r#""f{index}":1"#))
            .collect::<Vec<_>>()
            .join(",");
        let error = mapping
            .entry("", &format!(r#"{{"message":"x",{wide}}}"#), 0)
            .unwrap_err();
        assert!(error.contains("limit is 8"), "{error}");
    }

    #[test]
    fn action_lines_are_validated() {
        assert!(matches!(
            parse_action(r#"{"create":{"_index":"a","_id":"1"}}"#),
            Ok((
                Action::Create,
                ActionMeta {
                    index: Some(_),
                    id: Some(_)
                }
            ))
        ));
        assert!(matches!(
            parse_action(r#"{"index":{"_type":"_doc"}}"#),
            Ok((Action::Index, _))
        ));
        assert!(parse_action(r#"{"upsert":{}}"#).is_err());
        assert!(parse_action(r#"{"index":{},"create":{}}"#).is_err());
        assert!(parse_action(r#"{"message":"a document, not an action"}"#).is_err());
    }
}
