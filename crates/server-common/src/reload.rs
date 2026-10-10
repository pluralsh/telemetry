//! Live configuration reload. The operator mounts each server's config from a
//! Secret volume, which the kubelet refreshes in place, so servers poll the
//! file and apply each new revision without restarting.
//!
//! Only [`LiveConfig`] (namespaces, global auth, and cache warming) changes in
//! a running process. Any other change is reported by [`restart_required`]
//! and takes effect on the next restart.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use common::CacheWarmerConfig;
use serde::Serialize;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::auth::Access;

/// How often servers check their config file for a new revision.
pub const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// The settings a running server applies on reload.
pub struct LiveConfig<N> {
    pub namespaces: HashMap<String, N>,
    pub unauthenticated: bool,
    pub global: Access,
    pub cache_warmer: CacheWarmerConfig,
}

impl<N> LiveConfig<N> {
    /// Whether moving to `next` changes what the cache warmer does.
    pub fn warms_differently(&self, next: &Self) -> bool {
        self.cache_warmer != next.cache_warmer
            || self.namespaces.len() != next.namespaces.len()
            || !self
                .namespaces
                .keys()
                .all(|name| next.namespaces.contains_key(name))
    }
}

/// A value replaced whole on reload; readers take a cheap snapshot.
pub struct Live<T>(RwLock<Arc<T>>);

impl<T> Live<T> {
    pub fn new(value: T) -> Self {
        Self(RwLock::new(Arc::new(value)))
    }

    pub fn load(&self) -> Arc<T> {
        Arc::clone(&self.0.read().unwrap_or_else(PoisonError::into_inner))
    }

    pub fn store(&self, value: T) {
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(value);
    }
}

/// Top-level config sections that differ between `running` and `next` in
/// ways only a restart applies. Namespaces count only through their usage
/// reporting endpoints, which the ingest pipeline captures at startup.
pub fn restart_required<C: Serialize>(running: &C, next: &C) -> Vec<String> {
    let (Ok(Value::Object(running)), Ok(Value::Object(next))) = (
        serde_json::to_value(running).map(static_view),
        serde_json::to_value(next).map(static_view),
    ) else {
        return vec!["config".to_owned()];
    };
    let mut sections: Vec<String> = running
        .keys()
        .chain(next.keys())
        .filter(|key| running.get(*key) != next.get(*key))
        .cloned()
        .collect();
    sections.sort();
    sections.dedup();
    sections
}

fn static_view(mut config: Value) -> Value {
    let Some(sections) = config.as_object_mut() else {
        return config;
    };
    sections.remove("cache_warmer");
    if let Some(auth) = sections.get_mut("auth").and_then(Value::as_object_mut) {
        auth.remove("global");
        auth.remove("unauthenticated");
    }
    if let Some(namespaces) = sections.get_mut("namespaces") {
        let endpoints: BTreeMap<String, Value> = namespaces
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|namespace| {
                let name = namespace.get("name")?.as_str()?;
                let endpoint = namespace.get("usage_reporting_endpoint")?;
                (!endpoint.is_null()).then(|| (name.to_owned(), endpoint.clone()))
            })
            .collect();
        *namespaces = serde_json::to_value(endpoints).unwrap_or_default();
    }
    config
}

/// Polls `path` every `interval` and passes each new revision to `apply`,
/// which returns the sections it could not apply without a restart or an
/// error when the revision is invalid and the running config stays.
pub fn spawn_config_watcher<A, Fut>(
    product: &'static str,
    path: PathBuf,
    interval: Duration,
    cancellation: CancellationToken,
    apply: A,
) -> JoinHandle<()>
where
    A: Fn(String) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Vec<String>, String>> + Send,
{
    tokio::spawn(async move {
        let mut seen = tokio::fs::read(&path).await.ok();
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let raw = match tokio::fs::read(&path).await {
                Ok(raw) => raw,
                Err(error) => {
                    tracing::warn!(product, path = %path.display(), %error, "cannot read config");
                    continue;
                }
            };
            if seen.as_ref() == Some(&raw) {
                continue;
            }
            seen = Some(raw.clone());
            let result = match String::from_utf8(raw) {
                Ok(raw) => apply(raw).await,
                Err(error) => Err(error.to_string()),
            };
            let outcome = match result {
                Ok(restart) => {
                    metrics::gauge!("telemetry_config_restart_required", "product" => product)
                        .set(if restart.is_empty() { 0.0 } else { 1.0 });
                    if restart.is_empty() {
                        tracing::info!(product, "applied config reload");
                    } else {
                        tracing::warn!(
                            product,
                            sections = ?restart,
                            "applied config reload; changes to these sections take effect after a restart"
                        );
                    }
                    "applied"
                }
                Err(error) => {
                    tracing::error!(product, %error, "rejected config reload; keeping the running config");
                    "invalid"
                }
            };
            metrics::counter!(
                "telemetry_config_reloads_total",
                "product" => product,
                "outcome" => outcome
            )
            .increment(1);
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;

    #[test]
    fn live_sections_never_require_a_restart() {
        let running = json!({
            "mode": "reader",
            "cache_warmer": {"enabled": false},
            "auth": {"unauthenticated": false, "global": {"read": []}, "jwt": null},
            "namespaces": [{"name": "a", "auth": {"read": []}, "usage_reporting_endpoint": null}],
        });
        let next = json!({
            "mode": "reader",
            "cache_warmer": {"enabled": true},
            "auth": {"unauthenticated": true, "global": {"read": [{"type": "basic"}]}, "jwt": null},
            "namespaces": [
                {"name": "a", "auth": {"read": [{"type": "basic"}]}, "usage_reporting_endpoint": null},
                {"name": "b", "auth": {"read": []}, "usage_reporting_endpoint": null},
            ],
        });
        assert!(restart_required(&running, &next).is_empty());
    }

    #[test]
    fn static_sections_and_usage_endpoints_require_a_restart() {
        let running = json!({
            "mode": "reader",
            "storage": {"path": "a"},
            "auth": {"jwt": null},
            "namespaces": [{"name": "a", "usage_reporting_endpoint": null}],
        });
        let next = json!({
            "mode": "reader",
            "storage": {"path": "b"},
            "auth": {"jwt": {"issuer": "x"}},
            "namespaces": [{"name": "a", "usage_reporting_endpoint": "http://usage"}],
        });
        assert_eq!(
            restart_required(&running, &next),
            ["auth", "namespaces", "storage"]
        );
    }

    #[tokio::test]
    async fn watcher_applies_each_new_revision_once() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "one").unwrap();
        let applied = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&applied);
        let cancellation = CancellationToken::new();
        let task = spawn_config_watcher(
            "test",
            file.path().to_owned(),
            Duration::from_millis(10),
            cancellation.clone(),
            move |raw| {
                recorded.lock().unwrap().push(raw);
                async { Ok(Vec::new()) }
            },
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
        std::fs::write(file.path(), "two").unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancellation.cancel();
        task.await.unwrap();

        assert_eq!(*applied.lock().unwrap(), ["two"]);
    }
}
