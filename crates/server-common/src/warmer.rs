//! Startup and continuous block-cache warming shared by the product servers.

use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::storage::slate::warm::WarmLedger;
use common::{CacheWarmerConfig, SstWarmTracker, WarmWindow};
use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// One warming pass over a product's recent data.
pub struct WarmPass {
    pub window: WarmWindow,
    pub concurrency: usize,
    pub cancel: CancellationToken,
    /// Present for continuous warming, and for the startup pass before it.
    pub tracker: Option<Arc<SstWarmTracker>>,
    /// What every pass of this warmer has warmed, for budgeted warming.
    pub ledger: Arc<WarmLedger>,
    /// See [`CacheWarmerConfig::metadata_budget_percent`].
    pub metadata_budget_percent: u8,
}

/// Whether a server in this role and configuration runs any warming at all.
pub fn warming_enabled(config: &CacheWarmerConfig, serves_reads: bool) -> bool {
    serves_reads && (config.enabled || config.continuous.enabled)
}

/// Whether readiness waits for the startup pass of a server in this role.
pub fn readiness_waits(config: &CacheWarmerConfig, serves_reads: bool) -> bool {
    serves_reads && config.enabled && config.block_readiness
}

/// The warmer of metrics and traces readers: a metadata-only startup pass
/// in the background, without delaying readiness.
pub fn reader_warmer_defaults() -> CacheWarmerConfig {
    CacheWarmerConfig {
        enabled: true,
        block_readiness: false,
        ..CacheWarmerConfig::default()
    }
}

/// Deserializes a `cache_warmer` section over [`reader_warmer_defaults`],
/// so a section naming only some keys keeps the reader defaults for the
/// rest.
pub fn deserialize_reader_warmer<'de, D>(deserializer: D) -> Result<CacheWarmerConfig, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let section = serde_json::Value::deserialize(deserializer)?;
    let mut merged =
        serde_json::to_value(reader_warmer_defaults()).map_err(serde::de::Error::custom)?;
    merge(&mut merged, section);
    serde_json::from_value(merged).map_err(serde::de::Error::custom)
}

fn merge(base: &mut serde_json::Value, over: serde_json::Value) {
    match (base, over) {
        (serde_json::Value::Object(base), serde_json::Value::Object(over)) => {
            for (key, value) in over {
                match base.get_mut(&key) {
                    Some(slot) => merge(slot, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (base, over) => *base = over,
    }
}

/// Spawns the startup pass (when enabled) and then, when continuous warming
/// is enabled, a pass every `continuous.interval_seconds` that warms only SSTs
/// that appeared since the previous pass. `cache_warmed` is set once the
/// startup pass ends, whatever its outcome; a startup pass that does not
/// block readiness has no timeout.
pub fn spawn_cache_warmer<W, Fut, E>(
    product: &'static str,
    config: &CacheWarmerConfig,
    cancellation: CancellationToken,
    cache_warmed: Arc<AtomicBool>,
    warm: W,
) -> JoinHandle<()>
where
    W: Fn(WarmPass) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send,
    E: Display,
{
    let config = config.clone();
    let tracker = config
        .continuous
        .enabled
        .then(|| Arc::new(SstWarmTracker::new(config.enabled)));
    let ledger = Arc::new(WarmLedger::new(config.enabled));
    tokio::spawn(async move {
        if config.enabled {
            let timeout = config
                .block_readiness
                .then(|| Duration::from_secs(config.timeout_seconds));
            metrics::gauge!("telemetry_cache_warmer_active", "product" => product).set(1.0);
            let pass = WarmPass {
                window: config.window(),
                concurrency: config.concurrency,
                cancel: cancellation.clone(),
                tracker: tracker.clone(),
                ledger: Arc::clone(&ledger),
                metadata_budget_percent: config.metadata_budget_percent,
            };
            let status =
                run_pass(product, "startup", timeout, tracker.as_deref(), warm(pass)).await;
            metrics::gauge!("telemetry_cache_warmer_active", "product" => product).set(0.0);
            if status != "success" {
                tracing::warn!(product, status, "startup cache warming did not complete");
            }
        }
        cache_warmed.store(true, Ordering::Release);

        let Some(tracker) = tracker else { return };
        let continuous = &config.continuous;
        let mut ticker = tokio::time::interval(Duration::from_secs(continuous.interval_seconds));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let pass = WarmPass {
                window: continuous.window(),
                concurrency: config.concurrency,
                cancel: cancellation.clone(),
                tracker: Some(Arc::clone(&tracker)),
                ledger: Arc::clone(&ledger),
                metadata_budget_percent: config.metadata_budget_percent,
            };
            run_pass(product, "continuous", None, Some(&tracker), warm(pass)).await;
        }
    })
}

async fn run_pass<Fut, E>(
    product: &'static str,
    mode: &'static str,
    timeout: Option<Duration>,
    tracker: Option<&SstWarmTracker>,
    pass: Fut,
) -> &'static str
where
    Fut: Future<Output = Result<(), E>>,
    E: Display,
{
    let started = Instant::now();
    let result = match timeout {
        Some(timeout) => tokio::time::timeout(timeout, pass).await.ok(),
        None => Some(pass.await),
    };
    let status = match result {
        Some(Ok(())) => "success",
        Some(Err(error)) => {
            tracing::warn!(product, mode, %error, "cache warming failed");
            "error"
        }
        None => "timeout",
    };
    if let Some(tracker) = tracker {
        if status == "success" {
            tracker.finish_pass();
        } else {
            tracker.abandon_pass();
        }
    }
    metrics::counter!(
        "telemetry_cache_warmer_runs_total",
        "product" => product,
        "mode" => mode,
        "status" => status
    )
    .increment(1);
    metrics::histogram!(
        "telemetry_cache_warmer_duration_seconds",
        "product" => product,
        "mode" => mode,
        "status" => status
    )
    .record(started.elapsed().as_secs_f64());
    status
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn continuous_passes_follow_the_startup_pass() {
        let mut config = CacheWarmerConfig {
            enabled: true,
            ..CacheWarmerConfig::default()
        };
        config.continuous.enabled = true;
        config.continuous.interval_seconds = 15;
        config.continuous.warm_range_seconds = 600;
        let passes = Arc::new(Mutex::new(Vec::new()));
        let cache_warmed = Arc::new(AtomicBool::new(false));
        let cancellation = CancellationToken::new();
        let recorded = Arc::clone(&passes);
        let task = spawn_cache_warmer(
            "test",
            &config,
            cancellation.clone(),
            Arc::clone(&cache_warmed),
            move |pass: WarmPass| {
                recorded
                    .lock()
                    .unwrap()
                    .push((pass.window, pass.tracker.is_some()));
                async { Ok::<(), String>(()) }
            },
        );

        tokio::time::sleep(Duration::from_secs(31)).await;
        assert!(cache_warmed.load(Ordering::Acquire));
        cancellation.cancel();
        task.await.unwrap();

        let passes = passes.lock().unwrap();
        let startup = WarmWindow {
            metadata: None,
            payloads: None,
        };
        assert_eq!(passes[0], (startup, true));
        assert!(
            passes.len() >= 3,
            "expected continuous passes, got {passes:?}"
        );
        let ten_minutes = Some(Duration::from_secs(600));
        let continuous = WarmWindow {
            metadata: ten_minutes,
            payloads: ten_minutes,
        };
        assert!(passes[1..].iter().all(|pass| *pass == (continuous, true)));
    }

    #[tokio::test(start_paused = true)]
    async fn startup_only_warming_runs_once() {
        let mut config = CacheWarmerConfig {
            enabled: true,
            ..CacheWarmerConfig::default()
        };
        config.continuous.enabled = false;
        let passes = Arc::new(Mutex::new(0));
        let cache_warmed = Arc::new(AtomicBool::new(false));
        let recorded = Arc::clone(&passes);
        spawn_cache_warmer(
            "test",
            &config,
            CancellationToken::new(),
            Arc::clone(&cache_warmed),
            move |pass: WarmPass| {
                assert!(pass.tracker.is_none());
                *recorded.lock().unwrap() += 1;
                async { Ok::<(), String>(()) }
            },
        )
        .await
        .unwrap();

        assert!(cache_warmed.load(Ordering::Acquire));
        assert_eq!(*passes.lock().unwrap(), 1);
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ReaderConfig {
        #[serde(
            default = "reader_warmer_defaults",
            deserialize_with = "deserialize_reader_warmer"
        )]
        cache_warmer: CacheWarmerConfig,
    }

    #[test]
    fn reader_sections_merge_over_reader_defaults() {
        let absent: ReaderConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(absent.cache_warmer, reader_warmer_defaults());
        assert!(absent.cache_warmer.enabled && !absent.cache_warmer.block_readiness);

        let partial: ReaderConfig = serde_json::from_str(
            r#"{"cache_warmer": {"concurrency": 4, "continuous": {"interval_seconds": 30}}}"#,
        )
        .unwrap();
        let mut expected = reader_warmer_defaults();
        expected.concurrency = 4;
        expected.continuous.interval_seconds = 30;
        assert_eq!(partial.cache_warmer, expected);

        let disabled: ReaderConfig =
            serde_json::from_str(r#"{"cache_warmer": {"enabled": false}}"#).unwrap();
        assert!(!disabled.cache_warmer.enabled);

        let unknown = serde_json::from_str::<ReaderConfig>(r#"{"cache_warmer": {"bogus": 1}}"#);
        assert!(unknown.is_err());
        let nested = serde_json::from_str::<ReaderConfig>(
            r#"{"cache_warmer": {"continuous": {"bogus": 1}}}"#,
        );
        assert!(nested.is_err());
    }

    #[test]
    fn readiness_waits_only_when_asked() {
        assert!(!readiness_waits(&reader_warmer_defaults(), true));
        let blocking = CacheWarmerConfig {
            enabled: true,
            ..CacheWarmerConfig::default()
        };
        assert!(readiness_waits(&blocking, true));
        assert!(!readiness_waits(&blocking, false));
        assert!(!readiness_waits(&CacheWarmerConfig::default(), true));
    }

    #[tokio::test(start_paused = true)]
    async fn a_background_startup_pass_has_no_timeout() {
        let mut config = reader_warmer_defaults();
        config.continuous.enabled = false;
        config.timeout_seconds = 1;
        let cache_warmed = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        let done = Arc::clone(&finished);
        let percent = config.metadata_budget_percent;
        spawn_cache_warmer(
            "test",
            &config,
            CancellationToken::new(),
            Arc::clone(&cache_warmed),
            move |pass: WarmPass| {
                assert_eq!(pass.metadata_budget_percent, percent);
                let done = Arc::clone(&done);
                async move {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    done.store(true, Ordering::Release);
                    Ok::<(), String>(())
                }
            },
        )
        .await
        .unwrap();
        assert!(finished.load(Ordering::Acquire));
        assert!(cache_warmed.load(Ordering::Acquire));
    }
}
