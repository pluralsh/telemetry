use std::{io::Write, time::Duration};

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use common::storage::config::StorageConfig;
use flate2::{Compression, write::GzEncoder};
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::{
    collector::logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
    common::v1::{AnyValue, KeyValue, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use prost::Message;
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{
    AppState,
    config::{Access, Config, Credential, NamespaceConfig, Secret},
    router,
};

async fn state(namespaces: Vec<NamespaceConfig>) -> AppState {
    state_with_flush_interval(namespaces, 3600).await
}

async fn state_with_flush_interval(
    namespaces: Vec<NamespaceConfig>,
    flush_interval_seconds: u64,
) -> AppState {
    let mut config = Config {
        storage: StorageConfig::InMemory,
        namespaces,
        ..Config::default()
    };
    config.write.flush_interval_seconds = flush_interval_seconds;
    config.auth.unauthenticated = true;
    config.cache_warmer.enabled = false;
    AppState::open(config).await.unwrap()
}

async fn authenticated_state(namespaces: Vec<NamespaceConfig>) -> AppState {
    let mut config = Config {
        storage: StorageConfig::InMemory,
        namespaces,
        ..Config::default()
    };
    config.write.flush_interval_seconds = 3600;
    config.cache_warmer.enabled = false;
    AppState::open(config).await.unwrap()
}

#[tokio::test]
async fn path_prefix_scopes_public_apis_but_not_health() {
    let mut config = Config {
        path_prefix: "/logs".to_owned(),
        storage: StorageConfig::InMemory,
        namespaces: vec![namespace("tenant")],
        ..Config::default()
    };
    config.auth.unauthenticated = true;
    let state = AppState::open(config).await.unwrap();
    let app = router(state.clone());

    for (path, expected) in [
        ("/-/healthy", StatusCode::OK),
        ("/metrics", StatusCode::OK),
        (
            "/logs/read/ns/tenant/loki/api/v1/query?query=%7Bapp%3D%22test%22%7D",
            StatusCode::OK,
        ),
        (
            "/read/ns/tenant/loki/api/v1/query?query=%7Bapp%3D%22test%22%7D",
            StatusCode::NOT_FOUND,
        ),
        ("/logs/-/healthy", StatusCode::NOT_FOUND),
        ("/logs/metrics", StatusCode::NOT_FOUND),
    ] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{path}");
    }
    state.shutdown().await.unwrap();
}

#[derive(Default)]
struct UsageRecorder(std::sync::Mutex<Vec<(String, u64)>>);

impl server_common::ingest::IngestMiddleware for UsageRecorder {
    fn record(&self, request: &server_common::ingest::IngestRequest<'_>) {
        self.0
            .lock()
            .unwrap()
            .push((request.namespace.to_owned(), request.bytes));
    }
}

#[tokio::test]
async fn ingest_layer_records_accepted_pushes_by_wire_bytes() {
    let mut config = Config {
        path_prefix: "/logs".to_owned(),
        storage: StorageConfig::InMemory,
        namespaces: vec![namespace("tenant")],
        ..Config::default()
    };
    config.auth.unauthenticated = true;
    let mut state = AppState::open(config).await.unwrap();
    let recorder = std::sync::Arc::new(UsageRecorder::default());
    state.ingest = server_common::ingest::IngestPipeline::new(
        server_common::ingest::Signal::Logs,
        vec![recorder.clone()],
    );
    let accepted =
        json!({"streams":[{"stream":{"app":"api"},"values":[["1","hello"]]}]}).to_string();
    let compressed = gzip(accepted.as_bytes());

    for (body, expected) in [
        (compressed.clone(), StatusCode::NO_CONTENT),
        (gzip(b"not json"), StatusCode::BAD_REQUEST),
    ] {
        let response = router(state.clone())
            .oneshot(
                Request::post("/logs/write/ns/tenant/loki/api/v1/push")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::CONTENT_ENCODING, "gzip")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }

    assert_eq!(
        *recorder.0.lock().unwrap(),
        vec![("tenant".to_owned(), compressed.len() as u64)]
    );
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn pushes_are_bounded_by_wire_and_decoded_limits() {
    let mut config = Config {
        storage: StorageConfig::InMemory,
        namespaces: vec![namespace("tenant")],
        ..Config::default()
    };
    config.auth.unauthenticated = true;
    config.cache_warmer.enabled = false;
    config.write.flush_interval_seconds = 3600;
    config.request.max_request_bytes = 4 << 20;
    config.request.max_decoded_request_bytes = 8 << 20;
    let state = AppState::open(config).await.unwrap();
    // Each body is padded to roughly `bytes` with 1 KiB lines.
    let json_push = |bytes: usize| {
        let line = "x".repeat(1024);
        let values = (0..bytes / 1040)
            .map(|index| json!([(index + 1).to_string(), line]))
            .collect::<Vec<_>>();
        json!({"streams":[{"stream":{"app":"api"},"values":values}]})
            .to_string()
            .into_bytes()
    };
    let send = |body: Vec<u8>, content_type: &'static str, encoding: Option<&'static str>| {
        let state = state.clone();
        async move {
            let mut request = Request::post("/write/ns/tenant/loki/api/v1/push")
                .header(header::CONTENT_TYPE, content_type)
                .header(header::CONTENT_LENGTH, body.len());
            if let Some(encoding) = encoding {
                request = request.header(header::CONTENT_ENCODING, encoding);
            }
            router(state)
                .oneshot(request.body(Body::from(body)).unwrap())
                .await
                .unwrap()
                .status()
        }
    };

    // Above axum's 2 MiB default extractor limit, below the configured cap.
    assert_eq!(
        send(json_push(3 << 20), "application/json", None).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        send(json_push(5 << 20), "application/json", None).await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    // Small on the wire but inflating past the decoded cap.
    let inflated = gzip(&json_push(9 << 20));
    assert!(inflated.len() < 1 << 20);
    assert_eq!(
        send(inflated, "application/json", Some("gzip")).await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(
        send(gzip(&json_push(6 << 20)), "application/json", Some("gzip")).await,
        StatusCode::NO_CONTENT
    );
    // A snappy header declaring ~4 GiB is refused before allocation.
    assert_eq!(
        send(
            vec![0xff, 0xff, 0xff, 0xff, 0x0f],
            "application/x-protobuf",
            None
        )
        .await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    state.shutdown().await.unwrap();
}

async fn bulk(state: &AppState, path: &str, body: &str) -> (StatusCode, Value) {
    let body = gzip(body.as_bytes());
    let response = router(state.clone())
        .oneshot(
            Request::post(path)
                .header(header::CONTENT_TYPE, "application/x-ndjson")
                .header(header::CONTENT_ENCODING, "gzip")
                .header(header::CONTENT_LENGTH, body.len())
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    state.db.flush().await.unwrap();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

#[tokio::test]
async fn elasticsearch_bulk_ingests_logstash_documents() {
    let state = state(vec![namespace("tenant")]).await;
    // Fluent Bit with Logstash_Format: daily index, kubernetes metadata, and
    // `log` as the message field.
    let request = [
        r#"{"create":{"_index":"logstash-2024.01.31"}}"#,
        r#"{"@timestamp":"1970-01-01T00:00:01.000Z","log":"started","stream":"stdout","kubernetes":{"namespace_name":"prod","pod_name":"api-0"}}"#,
        r#"{"index":{"_index":"logstash-2024.02.01","_id":"given"}}"#,
        r#"{"@timestamp":1500,"log":"ready","kubernetes":{"namespace_name":"prod","pod_name":"api-1"}}"#,
        r#"{"index":{}}"#,
        r#"{"@timestamp":"not a time","log":"dropped"}"#,
        r#"{"delete":{"_index":"logstash-2024.01.31","_id":"1"}}"#,
        "",
    ]
    .join("\n");
    let (status, body) = bulk(
        &state,
        "/write/ns/tenant/elasticsearch/_bulk?_stream_fields=kubernetes.namespace_name",
        &request,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["errors"], true);
    let statuses = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| {
            let (action, result) = item.as_object().unwrap().iter().next().unwrap();
            (action.as_str(), result["status"].as_u64().unwrap())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        statuses,
        vec![
            ("create", 201),
            ("index", 201),
            ("index", 400),
            ("delete", 400)
        ]
    );
    assert_eq!(body["items"][1]["index"]["_id"], "given");

    let categorized = |path: &'static str| {
        let state = state.clone();
        async move {
            let response = router(state)
                .oneshot(
                    Request::get(path)
                        .header("x-loki-response-encoding-flags", "categorize-labels")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            response_json(response).await
        }
    };
    let body = categorized(
        "/read/ns/tenant/loki/api/v1/query_range?query=%7Bindex%3D%22logstash%22%7D&start=0&end=3&direction=forward",
    )
    .await;
    assert_eq!(body["data"]["result"].as_array().unwrap().len(), 1);
    assert_eq!(
        body["data"]["result"][0]["stream"],
        json!({"index": "logstash", "kubernetes_namespace_name": "prod"})
    );
    assert_eq!(
        body["data"]["result"][0]["values"],
        json!([
            ["1000000000", "started", {"structuredMetadata": {"kubernetes_pod_name": "api-0", "stream": "stdout"}}],
            ["1500000000", "ready", {"structuredMetadata": {"kubernetes_pod_name": "api-1"}}]
        ])
    );

    // The index in the path applies when actions omit `_index`.
    let (status, body) = bulk(
        &state,
        "/write/ns/tenant/elasticsearch/audit/_bulk?_msg_field=event",
        "{\"index\":{}}\n{\"@timestamp\":\"1970-01-01T00:00:02Z\",\"event\":\"login\",\"user\":\"ada\"}",
    )
    .await;
    assert_eq!((status, &body["errors"]), (StatusCode::OK, &json!(false)));
    let body = categorized(
        "/read/ns/tenant/loki/api/v1/query_range?query=%7Bindex%3D%22audit%22%7D&start=0&end=3",
    )
    .await;
    assert_eq!(
        body["data"]["result"][0]["values"],
        json!([["2000000000", "login", {"structuredMetadata": {"user": "ada"}}]])
    );
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn elasticsearch_rejects_malformed_bulk_and_answers_handshakes() {
    let state = state(vec![namespace("tenant")]).await;
    let (status, _) = bulk(
        &state,
        "/write/ns/tenant/elasticsearch/_bulk",
        "{\"message\":\"a document where an action belongs\"}\n",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = bulk(
        &state,
        "/write/ns/tenant/elasticsearch/_bulk",
        "{\"index\":{}}\n",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    for path in [
        "/write/ns/tenant/elasticsearch",
        "/write/ns/tenant/elasticsearch/",
        "/write/ns/tenant/elasticsearch/_cluster/health",
    ] {
        let response = query(&state, path).await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(response.headers()["x-elastic-product"], "Elasticsearch");
    }
    let info = response_json(query(&state, "/write/ns/tenant/elasticsearch").await).await;
    assert!(
        info["version"]["number"]
            .as_str()
            .unwrap()
            .starts_with("8.")
    );
    state.shutdown().await.unwrap();

    let protected = authenticated_state(vec![namespace("tenant")]).await;
    let (status, _) = bulk(
        &protected,
        "/write/ns/tenant/elasticsearch/_bulk",
        "{\"index\":{}}\n{\"message\":\"x\"}\n",
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        query(&protected, "/write/ns/tenant/elasticsearch")
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    protected.shutdown().await.unwrap();
}

fn namespace(name: &str) -> NamespaceConfig {
    NamespaceConfig {
        name: name.to_owned(),
        auth: Access::default(),
        usage_reporting_endpoint: None,
    }
}

async fn response_json(response: axum::response::Response) -> Value {
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

async fn query(state: &AppState, path: &str) -> axum::response::Response {
    router(state.clone())
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn push(state: &AppState, namespace: &str, body: Value) -> StatusCode {
    let response = router(state.clone())
        .oneshot(
            Request::post(format!("/write/ns/{namespace}/loki/api/v1/push"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    state.db.flush().await.unwrap();
    status
}

fn gzip(body: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(body).unwrap();
    encoder.finish().unwrap()
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

fn snappy_push(line: &str, timestamp_ns: i64) -> Vec<u8> {
    let request = PushRequest {
        streams: vec![StreamAdapter {
            labels: r#"{app="proto"}"#.to_owned(),
            entries: vec![EntryAdapter {
                timestamp: Some(ProtoTimestamp {
                    seconds: timestamp_ns / 1_000_000_000,
                    nanos: (timestamp_ns % 1_000_000_000) as i32,
                }),
                line: line.to_owned(),
                structured_metadata: vec![LabelPair {
                    name: "source".to_owned(),
                    value: "snappy".to_owned(),
                }],
            }],
        }],
    };
    snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .unwrap()
}

fn otlp_request(line: &str, timestamp_ns: u64) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_owned(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("checkout".to_owned())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: timestamp_ns,
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(line.to_owned())),
                    }),
                    severity_text: "INFO".to_owned(),
                    attributes: vec![KeyValue {
                        key: "http.method".to_owned(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("GET".to_owned())),
                        }),
                    }],
                    ..LogRecord::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
async fn json_push_and_queries_match_loki_shapes() {
    let state = state(vec![namespace("tenant")]).await;
    assert_eq!(
        push(
            &state,
            "tenant",
            json!({"streams":[{
                "stream":{"app":"api"},
                "values":[
                    ["1000000000","one",{"trace_id":"a"}],
                    ["2000000000","two",{"trace_id":"b"}]
                ]
            }]})
        )
        .await,
        StatusCode::NO_CONTENT
    );

    let response = router(state.clone())
        .oneshot(
            Request::get(
                "/read/ns/tenant/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=0&end=3&direction=forward",
            )
            .header("x-loki-response-encoding-flags", "categorize-labels")
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["data"]["resultType"], "streams");
    assert_eq!(body["data"]["encodingFlags"], json!(["categorize-labels"]));
    assert_eq!(body["data"]["result"][0]["stream"], json!({"app":"api"}));
    assert_eq!(
        body["data"]["result"][0]["values"][0],
        json!(["1000000000", "one", {"structuredMetadata":{"trace_id":"a"}}])
    );

    let response = router(state.clone())
        .oneshot(
            Request::post("/read/ns/tenant/loki/api/v1/query")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=count_over_time%28%7Bapp%3D%22api%22%7D%5B5s%5D%29&time=3",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = response_json(response).await;
    assert_eq!(body["data"]["resultType"], "vector");
    let mut series = body["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|sample| (sample["metric"].clone(), sample["value"][1].clone()))
        .collect::<Vec<_>>();
    series.sort_by_key(|(metric, _)| metric.to_string());
    assert_eq!(
        series,
        vec![
            (json!({"app":"api","trace_id":"a"}), json!("1")),
            (json!({"app":"api","trace_id":"b"}), json!("1")),
        ]
    );
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn metadata_endpoints_return_stream_labels_only() {
    let state = state(vec![namespace("tenant")]).await;
    assert_eq!(
        push(
            &state,
            "tenant",
            json!({"streams":[
                {
                    "stream":{"app":"api","env":"prod"},
                    "values":[["1000000000","one",{"trace_id":"a"}]]
                },
                {
                    "stream":{"app":"worker","env":"staging"},
                    "values":[["2000000000","two",{"trace_id":"b"}]]
                }
            ]})
        )
        .await,
        StatusCode::NO_CONTENT
    );

    let labels = query(&state, "/read/ns/tenant/loki/api/v1/labels?start=0&end=3").await;
    assert_eq!(labels.status(), StatusCode::OK);
    assert_eq!(
        response_json(labels).await,
        json!({"status":"success","data":["app","env"]})
    );

    let values = query(
        &state,
        "/read/ns/tenant/loki/api/v1/label/app/values?start=0&end=3",
    )
    .await;
    assert_eq!(
        response_json(values).await,
        json!({"status":"success","data":["api","worker"]})
    );

    let series = query(
        &state,
        "/read/ns/tenant/loki/api/v1/series?match%5B%5D=%7Bapp%3D~%22api%7Cworker%22%2Cenv%21%3D%22staging%22%7D&start=0&end=3",
    )
    .await;
    assert_eq!(series.status(), StatusCode::OK);
    assert_eq!(
        response_json(series).await,
        json!({"status":"success","data":[{"app":"api","env":"prod"}]})
    );

    let post = router(state.clone())
        .oneshot(
            Request::post("/read/ns/tenant/loki/api/v1/series")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(
                    "match%5B%5D=%7Bapp%3D%22worker%22%7D&start=0&end=3",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response_json(post).await,
        json!({"status":"success","data":[{"app":"worker","env":"staging"}]})
    );
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_stream_shape_folds_metadata_into_labels() {
    let state = state(vec![namespace("tenant")]).await;
    push(
        &state,
        "tenant",
        json!({"streams":[{"stream":{"app":"api"},"values":[
            ["1000000000","one",{"trace_id":"a"}],
            ["2000000000","two",{"trace_id":"b"}]
        ]}]}),
    )
    .await;
    let response = router(state.clone())
        .oneshot(
            Request::get(
                "/read/ns/tenant/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=0&end=3",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    let body = response_json(response).await;
    assert_eq!(body["data"]["result"].as_array().unwrap().len(), 2);
    assert_eq!(
        body["data"]["result"][0]["stream"],
        json!({"app":"api","trace_id":"a"})
    );
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn snappy_protobuf_push_defaults_content_type_and_honors_encoding() {
    let state = state(vec![namespace("tenant")]).await;
    for (timestamp, encoding) in [(1_000_000_000, None), (2_000_000_000, Some("snappy"))] {
        let mut request = Request::post("/write/ns/tenant/loki/api/v1/push");
        if let Some(encoding) = encoding {
            request = request.header(header::CONTENT_ENCODING, encoding);
        }
        let response = router(state.clone())
            .oneshot(
                request
                    .body(Body::from(snappy_push("protobuf", timestamp)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    state.db.flush().await.unwrap();

    let body = response_json(
        query(
            &state,
            "/read/ns/tenant/loki/api/v1/query_range?query=%7Bapp%3D%22proto%22%7D&start=0&end=3&direction=forward",
        )
        .await,
    )
    .await;
    assert_eq!(body["data"]["resultType"], "streams");
    assert_eq!(body["data"]["result"].as_array().unwrap().len(), 1);
    assert_eq!(
        body["data"]["result"][0]["stream"],
        json!({"app":"proto","source":"snappy"})
    );
    assert_eq!(
        body["data"]["result"][0]["values"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn otlp_protobuf_json_and_gzip_are_accepted_with_protocol_responses() {
    let state = state(vec![namespace("tenant")]).await;

    let protobuf = otlp_request("protobuf", 1_000_000_000).encode_to_vec();
    let response = router(state.clone())
        .oneshot(
            Request::post("/write/ns/tenant/otlp/v1/logs")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(protobuf))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/x-protobuf"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        ExportLogsServiceResponse::decode(body)
            .unwrap()
            .partial_success,
        None
    );

    let json_body = serde_json::to_vec(&otlp_request("json", 2_000_000_000)).unwrap();
    let response = router(state.clone())
        .oneshot(
            Request::post("/write/ns/tenant/otlp/v1/logs")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(response_json(response).await, json!({}));

    let protobuf = otlp_request("gzip", 3_000_000_000).encode_to_vec();
    let response = router(state.clone())
        .oneshot(
            Request::post("/write/ns/tenant/otlp/v1/logs")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .header(header::CONTENT_ENCODING, "gzip")
                .body(Body::from(gzip(&protobuf)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    state.db.flush().await.unwrap();
    let body = response_json(
        query(
            &state,
            "/read/ns/tenant/loki/api/v1/query_range?query=%7Bservice_name%3D%22checkout%22%7D&start=0&end=4&direction=forward",
        )
        .await,
    )
    .await;
    assert_eq!(body["data"]["resultType"], "streams");
    assert_eq!(body["data"]["result"].as_array().unwrap().len(), 1);
    assert_eq!(
        body["data"]["result"][0]["stream"],
        json!({
            "http_method":"GET",
            "service_name":"checkout",
            "severity_text":"INFO"
        })
    );
    assert_eq!(
        body["data"]["result"][0]["values"],
        json!([
            ["1000000000", "protobuf"],
            ["2000000000", "json"],
            ["3000000000", "gzip"]
        ])
    );
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn post_forms_and_all_loki_result_shapes_are_supported() {
    let state = state(vec![namespace("tenant")]).await;
    assert_eq!(
        push(
            &state,
            "tenant",
            json!({"streams":[{"stream":{"app":"api"},"values":[
                ["1000000000","one"],
                ["2000000000","two"]
            ]}]})
        )
        .await,
        StatusCode::NO_CONTENT
    );

    let cases = [
        (
            "/read/ns/tenant/loki/api/v1/query",
            "query=1&time=3",
            "scalar",
        ),
        (
            "/read/ns/tenant/loki/api/v1/query",
            "query=count_over_time%28%7Bapp%3D%22api%22%7D%5B5s%5D%29&time=3",
            "vector",
        ),
        (
            "/read/ns/tenant/loki/api/v1/query_range",
            "query=count_over_time%28%7Bapp%3D%22api%22%7D%5B5s%5D%29&start=1&end=3&step=1",
            "matrix",
        ),
        (
            "/read/ns/tenant/loki/api/v1/query_range",
            "query=%7Bapp%3D%22api%22%7D&start=0&end=3&direction=forward",
            "streams",
        ),
    ];
    for (path, form, expected) in cases {
        let response = router(state.clone())
            .oneshot(
                Request::post(path)
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from(form))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}: {form}");
        let body = response_json(response).await;
        assert_eq!(body["data"]["resultType"], expected, "{path}: {form}");
    }
    state.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn gzip_loki_json_and_visibility_flush_make_writes_queryable() {
    let state = state_with_flush_interval(vec![namespace("tenant")], 1).await;
    let body = json!({"streams":[{"stream":{"app":"visible"},"values":[
        ["1000000000","after flush"]
    ]}]})
    .to_string();
    let response = router(state.clone())
        .oneshot(
            Request::post("/write/ns/tenant/loki/api/v1/push")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CONTENT_ENCODING, "gzip")
                .body(Body::from(gzip(body.as_bytes())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(state.has_pending_visibility());

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..100 {
        if !state.has_pending_visibility() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        !state.has_pending_visibility(),
        "visibility task did not flush the accepted write"
    );

    let body = response_json(
        query(
            &state,
            "/read/ns/tenant/loki/api/v1/query_range?query=%7Bapp%3D%22visible%22%7D&start=0&end=2",
        )
        .await,
    )
    .await;
    assert!(!body["data"]["result"].as_array().unwrap().is_empty());
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_and_unsupported_encodings_are_rejected() {
    let state = state(vec![namespace("tenant")]).await;
    let cases = [
        (
            Request::post("/write/ns/tenant/loki/api/v1/push")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CONTENT_ENCODING, "gzip")
                .body(Body::from("not gzip"))
                .unwrap(),
            StatusCode::BAD_REQUEST,
        ),
        (
            Request::post("/write/ns/tenant/loki/api/v1/push")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CONTENT_ENCODING, "br")
                .body(Body::from("{}"))
                .unwrap(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        (
            Request::post("/write/ns/tenant/loki/api/v1/push")
                .body(Body::from("not snappy"))
                .unwrap(),
            StatusCode::BAD_REQUEST,
        ),
        (
            Request::post("/write/ns/tenant/otlp/v1/logs")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(vec![0xff, 0xff]))
                .unwrap(),
            StatusCode::BAD_REQUEST,
        ),
        (
            Request::post("/write/ns/tenant/otlp/v1/logs")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CONTENT_ENCODING, "snappy")
                .body(Body::from("{}"))
                .unwrap(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
    ];
    for (request, expected) in cases {
        let response = router(state.clone()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), expected);
    }
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_requests_and_unknown_namespaces_are_rejected() {
    let state = state(vec![namespace("tenant")]).await;
    for request in [
        Request::post("/write/ns/tenant/loki/api/v1/push")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{"))
            .unwrap(),
        Request::get(
            "/read/ns/tenant/loki/api/v1/query_range?query=%7Bapp%3D%22x%22%7D&start=3&end=1",
        )
        .body(Body::empty())
        .unwrap(),
    ] {
        assert_eq!(
            router(state.clone())
                .oneshot(request)
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    let response = router(state.clone())
        .oneshot(
            Request::get("/read/ns/missing/loki/api/v1/query?query=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn authorization_is_scoped_only_by_path_namespace() {
    let protected = NamespaceConfig {
        name: "protected".to_owned(),
        auth: Access {
            read: vec![Credential::Basic {
                username: "reader".to_owned(),
                password: Secret::Literal {
                    value: "secret".to_owned(),
                },
            }],
            write: vec![],
        },
        usage_reporting_endpoint: None,
    };
    let state = authenticated_state(vec![protected, namespace("open")]).await;
    let unauthorized = router(state.clone())
        .oneshot(
            Request::get("/read/ns/protected/loki/api/v1/query?query=1")
                .header("x-scope-orgid", "open")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    let authorized = router(state.clone())
        .oneshot(
            Request::get("/read/ns/protected/loki/api/v1/query?query=1")
                .header(
                    header::AUTHORIZATION,
                    format!("Basic {}", STANDARD.encode("reader:secret")),
                )
                .header("x-scope-orgid", "open")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authorized.status(), StatusCode::OK);
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn namespace_http_auth_is_secure_by_default_and_explicitly_bypassable() {
    let protected_state = authenticated_state(vec![namespace("tenant")]).await;
    let denied = router(protected_state.clone())
        .oneshot(
            Request::get("/read/ns/tenant/loki/api/v1/query?query=1")
                .header("x-scope-orgid", "tenant")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    protected_state.shutdown().await.unwrap();

    let open_state = state(vec![namespace("tenant")]).await;
    let allowed = router(open_state.clone())
        .oneshot(
            Request::get("/read/ns/tenant/loki/api/v1/query?query=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::OK);
    open_state.shutdown().await.unwrap();
}

#[tokio::test]
async fn scope_org_id_does_not_override_path_storage_namespace() {
    let state = state(vec![namespace("path-tenant"), namespace("header-tenant")]).await;
    let response = router(state.clone())
        .oneshot(
            Request::post("/write/ns/path-tenant/loki/api/v1/push")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-scope-orgid", "header-tenant")
                .body(Body::from(
                    json!({"streams":[{"stream":{"app":"scoped"},"values":[
                        ["1000000000","path wins"]
                    ]}]})
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    state.db.flush().await.unwrap();

    for (namespace, expected_entries) in [("path-tenant", 1), ("header-tenant", 0)] {
        let body = response_json(
            query(
                &state,
                &format!(
                    "/read/ns/{namespace}/loki/api/v1/query_range?query=%7Bapp%3D%22scoped%22%7D&start=0&end=2"
                ),
            )
            .await,
        )
        .await;
        assert_eq!(
            body["data"]["result"].as_array().unwrap().len(),
            expected_entries
        );
    }
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn health_and_readiness_are_available() {
    let state = state(vec![namespace("default")]).await;
    for path in ["/-/healthy", "/-/ready"] {
        let response = router(state.clone())
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    state.shutdown().await.unwrap();
}
