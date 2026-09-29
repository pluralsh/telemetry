use std::{
    collections::HashSet,
    fs,
    sync::{Arc, Mutex, atomic::AtomicBool},
};

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{
        Request as HttpRequest, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE},
    },
};
use base64::Engine;
use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig};
use http_body_util::BodyExt;
use proto::meter::internal::v1::internal_writer_server::InternalWriter;
use sharding::{
    AssignmentGeneration, AssignmentState, BoxError, DEFAULT_VIRTUAL_SHARDS, FakeAssignmentStore,
    FakeLeaseBackend, OwnershipManager, OwnershipManagerConfig, ShardLifecycle,
};
use tokio::sync::{RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request};
use tower::ServiceExt;

use super::*;

fn test_config(mode: ServerMode) -> Config {
    let mut config = Config {
        mode,
        storage: SlateDbStorageConfig {
            path: "meter-tests".to_owned(),
            object_store: ObjectStoreConfig::InMemory,
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        },
        namespaces: vec![
            NamespaceConfig {
                name: "alpha".to_owned(),
                auth: Default::default(),
            },
            NamespaceConfig {
                name: "beta".to_owned(),
                auth: Default::default(),
            },
        ],
        ..Config::default()
    };
    config.auth.unauthenticated = true;
    config.cache_warmer.enabled = false;
    config
}

fn state(mode: ServerMode) -> AppState {
    let config = test_config(mode);
    let (_, assignment) = assignment_for(&config).unwrap();
    AppState {
        remote_limit: Arc::new(Semaphore::new(4)),
        config: Arc::new(config),
        jwt: None,
        writers: None,
        readers: None,
        assignment: Arc::new(RwLock::new(assignment)),
        local_owner: "standalone".to_owned(),
        completed_requests: Arc::new(Mutex::new(HashSet::new())),
        draining_shards: Arc::new(RwLock::new(HashSet::new())),
        cancellation: CancellationToken::new(),
        background_tasks: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        flush_runs: Arc::new(AtomicU64::new(0)),
        cache_warmed: Arc::new(AtomicBool::new(true)),
    }
}

#[tokio::test]
async fn routes_are_namespaced_and_health_is_not() {
    let app = router(state(ServerMode::Standalone));
    assert_eq!(
        app.clone()
            .oneshot(HttpRequest::get("/-/healthy").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone()
            .oneshot(
                HttpRequest::get("/read/ns/alpha/api/v1/query?query=up")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.oneshot(
            HttpRequest::get("/api/v1/query?query=up")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap()
        .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn path_prefix_scopes_public_apis_but_not_health() {
    let mut state = state(ServerMode::Standalone);
    let mut config = (*state.config).clone();
    config.path_prefix = "/meter".to_owned();
    state.config = Arc::new(config);
    let app = router(state);

    let prefixed = app
        .clone()
        .oneshot(
            HttpRequest::post("/meter/write/ns/alpha/api/v1/write")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(prefixed.status(), StatusCode::NOT_FOUND);

    let unprefixed = app
        .clone()
        .oneshot(
            HttpRequest::post("/write/ns/alpha/api/v1/write")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unprefixed.status(), StatusCode::NOT_FOUND);

    let health = app
        .clone()
        .oneshot(HttpRequest::get("/-/healthy").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let metrics = app
        .oneshot(HttpRequest::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(metrics.status(), StatusCode::OK);
}

#[tokio::test]
async fn unknown_namespace_cannot_cross_tenant_boundary() {
    let response = router(state(ServerMode::Standalone))
        .oneshot(
            HttpRequest::get("/read/ns/gamma/api/v1/query?query=up")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn reader_mode_omits_write_routes() {
    let response = router(state(ServerMode::Reader))
        .oneshot(
            HttpRequest::post("/write/ns/alpha/api/v1/write")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn writer_mode_omits_query_routes() {
    let response = router(state(ServerMode::Writer))
        .oneshot(
            HttpRequest::get("/read/ns/alpha/api/v1/query?query=up")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn readiness_tracks_mode_resources_and_supported_routes() {
    for mode in [ServerMode::Reader, ServerMode::Writer] {
        let app = router(state(mode));
        let readiness = app
            .clone()
            .oneshot(HttpRequest::get("/-/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(readiness.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    let standalone = live_standalone_state().await;
    let readiness = router(standalone.clone())
        .oneshot(HttpRequest::get("/-/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(readiness.status(), StatusCode::OK);
    standalone.shutdown().await.unwrap();
}

async fn live_standalone_state() -> AppState {
    let mut config = test_config(ServerMode::Standalone);
    config.sharding.virtual_shards = 1;
    config.write.flush_interval_seconds = 60;
    AppState::open(config).await.unwrap()
}

#[tokio::test]
async fn query_range_and_series_accept_form_posts() {
    let state = live_standalone_state().await;
    let app = router(state.clone());
    for (path, form) in [
        ("/read/ns/alpha/api/v1/query", "query=1"),
        (
            "/read/ns/alpha/api/v1/query_range",
            "query=1&start=1&end=2&step=1",
        ),
        (
            "/read/ns/alpha/api/v1/series",
            "match%5B%5D=up&match%5B%5D=process_start_time_seconds",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                HttpRequest::post(path)
                    .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from(form))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn form_posts_preserve_read_auth() {
    let mut config = test_config(ServerMode::Standalone);
    config.sharding.virtual_shards = 1;
    config.auth.unauthenticated = false;
    config.namespaces[0].auth.read = vec![crate::config::Credential::Basic {
        username: "reader".to_owned(),
        password: crate::config::Secret::Literal {
            value: "read-password".to_owned(),
        },
    }];
    let state = AppState::open(config).await.unwrap();
    let response = router(state.clone())
        .oneshot(
            HttpRequest::post("/read/ns/alpha/api/v1/query")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("query=1"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = router(state.clone())
        .oneshot(
            HttpRequest::post("/read/ns/alpha/api/v1/query")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(
                    AUTHORIZATION,
                    format!(
                        "Basic {}",
                        base64::prelude::BASE64_STANDARD.encode("reader:read-password")
                    ),
                )
                .body(Body::from("query=1"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn namespace_http_auth_is_secure_by_default_and_explicitly_bypassable() {
    for (unauthenticated, expected) in [(false, StatusCode::UNAUTHORIZED), (true, StatusCode::OK)] {
        let mut config = test_config(ServerMode::Standalone);
        config.sharding.virtual_shards = 1;
        config.auth.unauthenticated = unauthenticated;
        let state = AppState::open(config).await.unwrap();
        let response = router(state.clone())
            .oneshot(
                HttpRequest::get("/read/ns/alpha/api/v1/query?query=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        state.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn malformed_bearer_returns_generic_unauthorized_response() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("jwks.json");
    fs::write(
        &path,
        r#"{"keys":[{"kty":"oct","kid":"test","alg":"HS256","k":"bWV0ZXItdGVzdC1vbmx5LXNpZ25pbmcta2V5"}]}"#,
    )
    .unwrap();
    let mut config = test_config(ServerMode::Standalone);
    config.sharding.virtual_shards = 1;
    config.auth.unauthenticated = false;
    config.auth.jwt = Some(crate::config::JwtConfig {
        jwks: crate::config::JwksSource::File {
            path: path.display().to_string(),
        },
        ..crate::config::JwtConfig::default()
    });
    let state = AppState::open(config).await.unwrap();
    let response = router(state.clone())
        .oneshot(
            HttpRequest::get("/read/ns/alpha/api/v1/query?query=1")
                .header(AUTHORIZATION, "Bearer malformed")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"],
        "authentication required"
    );
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn metadata_filters_and_federate_renders_complete_labels() {
    let state = live_standalone_state().await;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let mut item = Series::new(
        "federated_metric",
        vec![
            Label::new("instance", "node\"one"),
            Label::new("region", "us\\east"),
        ],
        vec![Sample::new(timestamp, f64::INFINITY)],
    );
    item.metric_type = Some(meter::MetricType::Gauge);
    item.unit = Some("widgets".to_owned());
    item.description = Some("Federated test metric".to_owned());
    state
        .route_write(
            "alpha",
            vec![item],
            Durability::Written,
            "metadata-federate".to_owned(),
        )
        .await
        .unwrap();

    let app = router(state.clone());
    let metadata = app
        .clone()
        .oneshot(
            HttpRequest::get(
                "/read/ns/alpha/api/v1/metadata?metric=federated_metric&limit=1&limit_per_metric=1",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(metadata.status(), StatusCode::OK);
    let metadata_body = metadata.into_body().collect().await.unwrap().to_bytes();
    let metadata_json: serde_json::Value = serde_json::from_slice(&metadata_body).unwrap();
    assert_eq!(
        metadata_json["data"]["federated_metric"][0]["help"],
        "Federated test metric"
    );
    assert_eq!(
        metadata_json["data"]["federated_metric"][0]["type"],
        "gauge"
    );
    let labels = app
        .clone()
        .oneshot(
            HttpRequest::get("/read/ns/alpha/api/v1/labels")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(labels.status(), StatusCode::OK);
    let labels_body = labels.into_body().collect().await.unwrap().to_bytes();
    let labels_json: serde_json::Value = serde_json::from_slice(&labels_body).unwrap();
    assert_eq!(
        labels_json["data"],
        serde_json::json!(["__name__", "instance", "region"])
    );
    let values = app
        .clone()
        .oneshot(
            HttpRequest::get("/read/ns/alpha/api/v1/label/region/values")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(values.status(), StatusCode::OK);
    let values_body = values.into_body().collect().await.unwrap().to_bytes();
    let values_json: serde_json::Value = serde_json::from_slice(&values_body).unwrap();
    assert_eq!(values_json["data"], serde_json::json!(["us\\east"]));
    let isolated = app
        .clone()
        .oneshot(
            HttpRequest::get("/read/ns/beta/api/v1/metadata?metric=federated_metric")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let isolated_body = isolated.into_body().collect().await.unwrap().to_bytes();
    let isolated_json: serde_json::Value = serde_json::from_slice(&isolated_body).unwrap();
    assert_eq!(isolated_json["data"], serde_json::json!({}));

    let federate = app
        .oneshot(
            HttpRequest::get("/read/ns/alpha/federate?match%5B%5D=federated_metric")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(federate.status(), StatusCode::OK);
    assert_eq!(
        federate.headers()["content-type"],
        "text/plain; version=0.0.4"
    );
    let body = String::from_utf8(
        federate
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains("federated_metric{"));
    assert!(body.contains("instance=\"node\\\"one\""));
    assert!(body.contains("region=\"us\\\\east\""));
    assert!(body.contains(" +Inf "));
    state.shutdown().await.unwrap();
}

#[tokio::test]
async fn periodic_flush_makes_metadata_visible_to_db_reader() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = test_config(ServerMode::Writer);
    config.sharding.virtual_shards = 1;
    config.write.flush_interval_seconds = 1;
    config.storage = SlateDbStorageConfig {
        path: "meter".to_owned(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: directory.path().display().to_string(),
        }),
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    };
    let state = AppState::open(config.clone()).await.unwrap();
    let mut item = Series::new(
        "durably_visible",
        vec![Label::new("instance", "writer")],
        vec![Sample::new(1_700_000_000_000, 1.0)],
    );
    item.metric_type = Some(meter::MetricType::Gauge);
    item.unit = Some("items".to_owned());
    item.description = Some("Durably visible metric".to_owned());
    state
        .route_write(
            "alpha",
            vec![item],
            Durability::Applied,
            "periodic-flush".to_owned(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert!(state.flush_runs.load(Ordering::Relaxed) > 0);

    let namespace = Namespace::new("alpha").unwrap();
    let mut storage = meter_config(&config).storage;
    storage.path = ShardingOptions::new(1, 4)
        .unwrap()
        .shard_path(&storage.path, ShardId::new(0))
        .unwrap();
    let reader = meter::TimeSeriesDbReader::open(
        storage,
        slatedb::config::DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(100),
            skip_wal_replay: false,
            ..slatedb::config::DbReaderOptions::default()
        },
        32,
    )
    .await
    .unwrap();
    let query = reader
        .query(
            &namespace,
            "durably_visible",
            Some(UNIX_EPOCH + Duration::from_millis(1_700_000_000_000)),
        )
        .await
        .unwrap();
    assert!(!query.into_matrix().is_empty());
    let metadata = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let metadata = reader
                .metadata(&namespace, Some("durably_visible"))
                .await
                .unwrap();
            if !metadata.is_empty() {
                return metadata;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].unit.as_deref(), Some("items"));
    assert_eq!(
        metadata[0].description.as_deref(),
        Some("Durably visible metric")
    );
    reader.close().await.unwrap();
    state.shutdown().await.unwrap();
}

#[test]
fn static_assignment_groups_local_and_remote_owners() {
    let mut config = test_config(ServerMode::Writer);
    config.sharding.virtual_shards = 4;
    config.sharding.kind = ShardingBackend::Static {
        owner_id: "meter-0".to_owned(),
        owners: vec![
            StaticOwner {
                id: "meter-0".to_owned(),
                ordinal: 0,
                endpoint: "meter-0:9090".to_owned(),
                start_shard: 0,
                end_shard: 2,
            },
            StaticOwner {
                id: "meter-1".to_owned(),
                ordinal: 1,
                endpoint: "meter-1:9090".to_owned(),
                start_shard: 2,
                end_shard: 4,
            },
        ],
    };
    config.validate().unwrap();
    let (local, assignment) = assignment_for(&config).unwrap();
    assert_eq!(local, "meter-0");
    assert_eq!(assignment.owner_of(ShardId::new(0)).unwrap().id, "meter-0");
    assert_eq!(assignment.owner_of(ShardId::new(3)).unwrap().id, "meter-1");
}

#[cfg(feature = "kubernetes")]
#[test]
fn coordinator_uses_replica_count_as_desired_shard_count() {
    let one = vec![Owner::new("meter-0", 0)];
    let two = vec![Owner::new("meter-0", 0), Owner::new("meter-1", 1)];
    let initial = balanced_contiguous(AssignmentGeneration::new(1), 1, &one, None).unwrap();
    assert!(!membership_changed(Some(&initial), &one, 1));
    assert!(membership_changed(Some(&initial), &two, 2));

    let scaled = balanced_contiguous(AssignmentGeneration::new(2), 2, &two, None).unwrap();
    assert!(!membership_changed(Some(&scaled), &two, 2));
    assert!(membership_changed(Some(&scaled), &one, 1));
}

fn proto_request(generation: u64, shard: u64) -> WriteBatchRequest {
    WriteBatchRequest {
        namespace: Some(ProtoNamespace {
            name: "alpha".to_owned(),
        }),
        assignment_generation: generation,
        shard_id: shard,
        series: vec![],
        metadata: vec![],
        durability: ProtoDurability::Applied as i32,
        request_id: "request-1".to_owned(),
    }
}

#[tokio::test]
async fn grpc_rejects_stale_generation_and_non_owner() {
    let state = state(ServerMode::Writer);
    let stale = InternalWriter::write(&state, Request::new(proto_request(0, 0)))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::FailedPrecondition);
    assert!(stale.message().contains("stale_ownership"));

    let remote = Owner::new("remote", 1);
    let map = ShardMap::new(
        AssignmentGeneration::new(2),
        64,
        vec![Assignment::new(
            remote,
            ShardRange::within(0, 64, 64).unwrap(),
            AssignmentState::Active,
        )],
    )
    .unwrap();
    state.assignment.write().await.clone_from(&map);
    let non_owner = InternalWriter::write(&state, Request::new(proto_request(2, 0)))
        .await
        .unwrap_err();
    assert_eq!(non_owner.code(), Code::FailedPrecondition);
    assert!(non_owner.message().contains("non_owner"));
}

#[tokio::test]
async fn grpc_retries_are_idempotent() {
    let state = AppState::open(test_config(ServerMode::Writer))
        .await
        .unwrap();
    let namespace = Namespace::new("alpha").unwrap();
    let options = ShardingOptions::new(DEFAULT_VIRTUAL_SHARDS, 4).unwrap();
    let routing = state.assignment.read().await.routing.clone();
    let mut item = Series::new(
        "idempotent_total",
        vec![Label::new("instance", "a")],
        vec![Sample::new(1_700_000_000_000, 1.0)],
    );
    let shard = options.route(&routing, &namespace, &item.labels);
    let request = WriteBatchRequest {
        namespace: Some(ProtoNamespace {
            name: "alpha".to_owned(),
        }),
        assignment_generation: 1,
        shard_id: u64::from(shard.get()),
        series: vec![ProtoSeries {
            labels: item
                .labels
                .drain(..)
                .map(|label| ProtoLabel {
                    name: label.name,
                    value: label.value,
                })
                .collect(),
            samples: vec![ProtoSample {
                timestamp_ms: 1_700_000_000_000,
                value: 1.0,
            }],
        }],
        metadata: vec![],
        durability: ProtoDurability::Applied as i32,
        request_id: "same-request".to_owned(),
    };
    let first = InternalWriter::write(&state, Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner();
    let second = InternalWriter::write(&state, Request::new(request))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.accepted_series, second.accepted_series);
    assert_eq!(first.accepted_samples, second.accepted_samples);
    let beta_request = WriteBatchRequest {
        namespace: Some(ProtoNamespace {
            name: "beta".to_owned(),
        }),
        assignment_generation: 1,
        shard_id: 0,
        series: vec![],
        metadata: vec![],
        durability: ProtoDurability::Applied as i32,
        request_id: "same-request".to_owned(),
    };
    InternalWriter::write(&state, Request::new(beta_request))
        .await
        .unwrap();
    {
        let completed = state.completed_requests.lock().unwrap();
        assert!(completed.contains(&("alpha".to_owned(), "same-request".to_owned())));
        assert!(completed.contains(&("beta".to_owned(), "same-request".to_owned())));
    }
    state.shutdown().await.unwrap();
}

#[derive(Default)]
struct RecordingLifecycle(Mutex<Vec<&'static str>>);

#[async_trait]
impl ShardLifecycle for RecordingLifecycle {
    async fn open(
        &self,
        _shard: ShardId,
        _generation: AssignmentGeneration,
    ) -> Result<(), BoxError> {
        self.0.lock().unwrap().push("open");
        Ok(())
    }

    async fn drain(&self, _shard: ShardId) -> Result<(), BoxError> {
        self.0.lock().unwrap().push("drain");
        Ok(())
    }

    async fn flush(&self, _shard: ShardId) -> Result<(), BoxError> {
        self.0.lock().unwrap().push("flush");
        Ok(())
    }

    async fn close(&self, _shard: ShardId) -> Result<(), BoxError> {
        self.0.lock().unwrap().push("close");
        Ok(())
    }
}

#[tokio::test]
async fn graceful_drain_flushes_before_close_and_release() {
    let mut config = test_config(ServerMode::Standalone);
    config.sharding.virtual_shards = 1;
    let (_, map) = assignment_for(&config).unwrap();
    let lifecycle = Arc::new(RecordingLifecycle::default());
    let manager = OwnershipManager::new(
        "standalone",
        OwnershipManagerConfig::default(),
        Arc::new(FakeAssignmentStore::new(Some(map))),
        Arc::new(FakeLeaseBackend::default()),
        lifecycle.clone(),
    );
    let cancel = CancellationToken::new();
    cancel.cancel();
    manager.run(cancel).await.unwrap();
    assert_eq!(
        *lifecycle.0.lock().unwrap(),
        ["open", "drain", "flush", "close"]
    );
}
