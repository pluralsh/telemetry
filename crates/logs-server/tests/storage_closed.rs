//! A writer whose SlateDB handle is closed for good must fail its probes so
//! Kubernetes restarts it. Its own binary: the probes read the process-wide
//! storage health.

use std::{path::Path, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use common::storage::config::{
    LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig,
};
use plural_logs_server::{
    AppState,
    config::{Config, NamespaceConfig},
    router,
};
use tower::ServiceExt;

async fn status(app: &Router, path: &str) -> StatusCode {
    app.clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

/// Every SlateDB database under `root`, as a path relative to it.
fn databases(root: &Path, dir: &Path, found: &mut Vec<String>) {
    if dir.join("manifest").is_dir() {
        let relative = dir.strip_prefix(root).unwrap();
        found.push(relative.to_str().unwrap().to_owned());
        return;
    }
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            databases(root, &path, found);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_writer_fails_liveness_and_readiness() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "logs".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.path().display().to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
            disk: Default::default(),
        }),
        namespaces: vec![NamespaceConfig {
            name: "tenant".to_owned(),
            auth: Default::default(),
            usage_reporting_endpoint: None,
        }],
        ..Config::default()
    };
    config.auth.unauthenticated = true;
    config.cache_warmer.enabled = false;
    config.write.flush_interval_seconds = 3600;
    let state = AppState::open(config).await.unwrap();
    let app = router(state.clone());
    assert_eq!(status(&app, "/-/healthy").await, StatusCode::OK);
    assert_eq!(status(&app, "/-/ready").await, StatusCode::OK);

    // A second writer on each database fences the server's, which SlateDB
    // then closes for good.
    let mut paths = Vec::new();
    databases(dir.path(), dir.path(), &mut paths);
    assert!(!paths.is_empty(), "no SlateDB databases under {dir:?}");
    let mut successors = Vec::new();
    for path in &paths {
        let storage = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: path.clone(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.path().display().to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
            disk: Default::default(),
        });
        let successor = plural_logs::LogDb::open(plural_logs::Config {
            storage,
            ..plural_logs::Config::default()
        })
        .await
        .unwrap_or_else(|error| panic!("open a successor on {path}: {error}"));
        successors.push(successor);
    }
    let push = Request::post("/write/ns/tenant/loki/api/v1/push")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"streams":[{"stream":{"app":"api"},"values":[["1","after fencing"]]}]}"#,
        ))
        .unwrap();
    let _ = app.clone().oneshot(push).await.unwrap();

    tokio::time::timeout(Duration::from_secs(30), async {
        while status(&app, "/-/healthy").await == StatusCode::OK {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("liveness kept passing after the writer was fenced");
    assert_eq!(
        status(&app, "/-/healthy").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        status(&app, "/-/ready").await,
        StatusCode::SERVICE_UNAVAILABLE
    );

    for successor in successors {
        successor.close().await.unwrap();
    }
    let _ = state.shutdown().await;
}
