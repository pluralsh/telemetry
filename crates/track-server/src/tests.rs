use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use common::storage::config::StorageConfig;
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message;
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
        }],
        ..Config::default()
    };
    config.auth.unauthenticated = unauthenticated;
    AppState::open(config).await.unwrap()
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

    let trace = state
        .db
        .get_trace(
            &track::Namespace::new("tenant").unwrap(),
            track::TraceId::new([3; 16]).unwrap(),
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
        }],
        ..Config::default()
    };
    config.auth.unauthenticated = true;
    let limited = AppState::open(config).await.unwrap();
    let response = router(limited.clone())
        .oneshot(
            Request::post("/write/ns/tenant/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(otlp_request().encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    limited.shutdown().await.unwrap();
}
