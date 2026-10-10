use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use common::storage::config::StorageConfig;
use flate2::read::GzDecoder;
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message;
use std::io::Read;
use tower::ServiceExt;

use crate::{
    AppState,
    config::{Config, NamespaceConfig, RequestConfig, ServerMode},
    jaeger::{
        Batch as JaegerBatch, KeyValue as JaegerKeyValue, Log as JaegerLog, PostSpansRequest,
        Process as JaegerProcess, Span as JaegerSpan, ValueType,
        collector_service_server::CollectorService,
    },
    router,
};

async fn state(mode: ServerMode, unauthenticated: bool) -> AppState {
    let mut config = Config {
        mode,
        storage: StorageConfig::InMemory,
        namespaces: vec![NamespaceConfig {
            name: "tenant".into(),
            auth: Default::default(),
            usage_reporting_endpoint: None,
        }],
        ..Config::default()
    };
    config.auth.unauthenticated = unauthenticated;
    config.cache_warmer.enabled = false;
    AppState::open(config).await.unwrap()
}

#[tokio::test]
async fn path_prefix_scopes_public_apis_but_not_health() {
    let mut config = Config {
        path_prefix: "/traces".into(),
        mode: ServerMode::Standalone,
        storage: StorageConfig::InMemory,
        namespaces: vec![NamespaceConfig {
            name: "tenant".into(),
            auth: Default::default(),
            usage_reporting_endpoint: None,
        }],
        ..Config::default()
    };
    config.auth.unauthenticated = true;
    let state = AppState::open(config).await.unwrap();
    let app = router(state.clone());

    for (path, expected) in [
        ("/-/healthy", StatusCode::OK),
        ("/metrics", StatusCode::OK),
        ("/traces/read/ns/tenant/api/echo", StatusCode::OK),
        ("/read/ns/tenant/api/echo", StatusCode::NOT_FOUND),
        ("/traces/-/healthy", StatusCode::NOT_FOUND),
        ("/traces/metrics", StatusCode::NOT_FOUND),
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

#[tokio::test]
async fn query_responses_support_gzip_compression() {
    let state = state(ServerMode::Standalone, true).await;
    let response = router(state.clone())
        .oneshot(
            Request::get("/read/ns/tenant/api/search")
                .header(header::ACCEPT_ENCODING, "gzip")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_ENCODING], "gzip");

    let compressed = response.into_body().collect().await.unwrap().to_bytes();
    let mut decoder = GzDecoder::new(compressed.as_ref());
    let mut decoded = String::new();
    decoder.read_to_string(&mut decoded).unwrap();
    let response: serde_json::Value = serde_json::from_str(&decoded).unwrap();
    assert!(response["traces"].is_array());
    state.shutdown().await.unwrap();
}

fn otlp_request() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_attribute("service.name", "checkout")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    name: "root".into(),
                    start_time_unix_nano: 1,
                    end_time_unix_nano: 2,
                    attributes: vec![string_attribute("http.method", "GET")],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// An OTLP request of roughly `bytes` encoded, as 1 KiB spans across traces.
fn sized_otlp_request(bytes: usize) -> ExportTraceServiceRequest {
    let padding = "x".repeat(1024);
    let spans = (0..bytes / 1100)
        .map(|index| {
            let mut trace_id = vec![0; 16];
            trace_id[..8].copy_from_slice(&(index as u64 + 1).to_be_bytes());
            Span {
                trace_id,
                span_id: vec![2; 8],
                name: "padded".into(),
                start_time_unix_nano: 1,
                end_time_unix_nano: 2,
                attributes: vec![string_attribute("padding", &padding)],
                ..Default::default()
            }
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[tokio::test]
async fn otlp_grpc_accepts_messages_up_to_the_decoded_limit() {
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;

    let mut config = Config {
        storage: StorageConfig::InMemory,
        request: RequestConfig {
            max_request_bytes: 4 << 20,
            max_decoded_request_bytes: 8 << 20,
            ..RequestConfig::default()
        },
        namespaces: vec![NamespaceConfig {
            name: "tenant".into(),
            auth: Default::default(),
            usage_reporting_endpoint: None,
        }],
        ..Config::default()
    };
    config.auth.unauthenticated = true;
    config.cache_warmer.enabled = false;
    let state = AppState::open(config).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(
        tonic_otlp::transport::Server::builder()
            .add_service(crate::otlp_grpc_service(state.clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let mut client = TraceServiceClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let export = |bytes: usize| {
        let mut request = tonic_otlp::Request::new(sized_otlp_request(bytes));
        request
            .metadata_mut()
            .insert("x-scope-orgid", "tenant".parse().unwrap());
        request
    };

    // Above tonic's 4 MiB default, below the configured decoded cap.
    let accepted = export(6 << 20);
    assert!(accepted.get_ref().encoded_len() > 4 << 20);
    client.export(accepted).await.unwrap();
    let status = client.export(export(9 << 20)).await.unwrap_err();
    assert_eq!(status.code(), tonic_otlp::Code::OutOfRange, "{status:?}");

    server.abort();
    state.shutdown().await.unwrap();
}

fn string_attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
    }
}

#[tokio::test]
async fn protobuf_ingest_then_tempo_search_and_trace_lookup() {
    let state = state(ServerMode::Standalone, true).await;
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(
            Request::post("/write/ns/tenant/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(otlp_request().encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    state.db.flush().await.unwrap();
    let response = app
        .clone()
        .oneshot(
            Request::get("/read/ns/tenant/api/search?q=%7B%7D&start=0&end=1&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["traces"].as_array().unwrap().len(), 1);

    let response = app
        .oneshot(
            Request::get("/read/ns/tenant/api/v2/traces/01010101010101010101010101010101")
                .header(header::ACCEPT, "application/x-protobuf")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/x-protobuf"
    );
    state.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn periodic_durable_flushes_accepted_writes() {
    let state = state(ServerMode::Standalone, true).await;
    let response = router(state.clone())
        .oneshot(
            Request::post("/write/ns/tenant/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(otlp_request().encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(state.has_pending_visibility());

    tokio::task::yield_now().await;
    tokio::time::advance(server_common::config::WriteConfig::default().flush_interval()).await;
    for _ in 0..100 {
        if !state.has_pending_visibility() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        !state.has_pending_visibility(),
        "durable flush task did not flush the accepted write"
    );
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn zipkin_ingest_translates_and_is_queryable() {
    let state = state(ServerMode::Standalone, true).await;
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(
            Request::post("/write/ns/tenant/api/v2/spans")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"[{"traceId":"0000000000000001","id":"0000000000000002","name":"zipkin","kind":"SERVER","timestamp":1,"duration":2,"localEndpoint":{"serviceName":"api"},"tags":{"error":"failed","http.method":"GET"},"annotations":[{"timestamp":2,"value":"event"}]}]"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    state.db.flush().await.unwrap();
    let response = app
        .oneshot(
            Request::get("/read/ns/tenant/api/traces/00000000000000000000000000000001")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn canonical_tag_endpoints_finish_and_return_tempo_shapes() {
    let state = state(ServerMode::Standalone, true).await;
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(
            Request::post("/write/ns/tenant/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(otlp_request().encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    state.db.flush().await.unwrap();
    for (path, expected) in [
        (
            "/read/ns/tenant/api/search/tags",
            serde_json::json!({"tagNames": ["http.method", "service.name"]}),
        ),
        (
            "/read/ns/tenant/api/search/tag/http.method/values",
            serde_json::json!({"tagValues": ["GET"]}),
        ),
        (
            "/read/ns/tenant/api/v2/search/tags",
            serde_json::json!({"scopes": [
                {"name": "resource", "tags": ["service.name"]},
                {"name": "span", "tags": ["http.method"]}
            ]}),
        ),
        (
            "/read/ns/tenant/api/v2/search/tag/http.method/values",
            serde_json::json!({"tagValues": [{"type": "string", "value": "GET"}]}),
        ),
        (
            "/read/ns/tenant/api/v2/search/tag/span.http.method/values",
            serde_json::json!({"tagValues": [{"type": "string", "value": "GET"}]}),
        ),
        (
            "/read/ns/tenant/api/v2/search/tag/resource.service.name/values",
            serde_json::json!({"tagValues": [{"type": "string", "value": "checkout"}]}),
        ),
    ] {
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            app.clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap()),
        )
        .await
        .expect("tag endpoint must not scan theoretical time buckets")
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(value, expected);
    }

    let response = app
        .clone()
        .oneshot(
            Request::get("/read/ns/tenant/api/v2/search/tags?scope=intrinsic")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let value: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["scopes"][0]["name"], "intrinsic");
    assert!(
        value["scopes"][0]["tags"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("trace:id"))
    );
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn jaeger_collector_converts_process_span_and_log_data() {
    let state = state(ServerMode::Standalone, true).await;
    let mut request = tonic::Request::new(PostSpansRequest {
        batch: Some(JaegerBatch {
            process: Some(JaegerProcess {
                service_name: "payments".into(),
                tags: vec![jaeger_string_tag("deployment.environment", "test")],
            }),
            spans: vec![JaegerSpan {
                trace_id: vec![3; 16],
                span_id: vec![4; 8],
                operation_name: "charge".into(),
                start_time: Some(prost_types::Timestamp {
                    seconds: 1,
                    nanos: 2,
                }),
                duration: Some(prost_types::Duration {
                    seconds: 0,
                    nanos: 10,
                }),
                tags: vec![jaeger_string_tag("http.method", "POST")],
                logs: vec![JaegerLog {
                    timestamp: Some(prost_types::Timestamp {
                        seconds: 1,
                        nanos: 5,
                    }),
                    fields: vec![jaeger_string_tag("event", "authorized")],
                }],
                ..Default::default()
            }],
        }),
    });
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant".parse().unwrap());
    CollectorService::post_spans(&state, request).await.unwrap();

    state.db.flush().await.unwrap();
    let routing = state.router.assignment().read().await.clone();
    let trace = state
        .db
        .get_trace(
            &routing,
            &plural_traces::Namespace::new("tenant").unwrap(),
            plural_traces::TraceId::new([3; 16]).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let resource = trace.resource_spans[0].resource.as_ref().unwrap();
    assert!(resource.attributes.iter().any(|value| {
        value.key == "service.name"
            && value.value.as_ref().and_then(|value| value.value.as_ref())
                == Some(&any_value::Value::StringValue("payments".into()))
    }));
    let span = &trace.resource_spans[0].scope_spans[0].spans[0];
    assert_eq!(span.name, "charge");
    assert_eq!(span.events[0].name, "authorized");
    assert!(span.events[0].attributes.is_empty());
    state.shutdown().await.unwrap();
}

fn jaeger_string_tag(key: &str, value: &str) -> JaegerKeyValue {
    JaegerKeyValue {
        key: key.into(),
        v_type: ValueType::String as i32,
        v_str: value.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn enforces_auth_modes_and_request_size() {
    let protected = state(ServerMode::Standalone, false).await;
    let response = router(protected.clone())
        .oneshot(
            Request::get("/read/ns/tenant/api/echo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    protected.shutdown().await.unwrap();

    let reader = state(ServerMode::Reader, true).await;
    let response = router(reader.clone())
        .oneshot(
            Request::post("/write/ns/tenant/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(otlp_request().encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    reader.shutdown().await.unwrap();

    let mut config = Config {
        storage: StorageConfig::InMemory,
        request: RequestConfig {
            max_request_bytes: 1,
            ..RequestConfig::default()
        },
        namespaces: vec![NamespaceConfig {
            name: "tenant".into(),
            auth: Default::default(),
            usage_reporting_endpoint: None,
        }],
        ..Config::default()
    };
    config.auth.unauthenticated = true;
    let limited = AppState::open(config).await.unwrap();
    let body = otlp_request().encode_to_vec();
    let response = router(limited.clone())
        .oneshot(
            Request::post("/write/ns/tenant/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .header(header::CONTENT_LENGTH, body.len())
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    limited.shutdown().await.unwrap();
}
