//! Startup and continuous block-cache warming shared by the product servers.

use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::{CacheWarmerConfig, SstWarmTracker};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// One warming pass over a product's recent data.
pub struct WarmPass {
    pub range: Duration,
    pub include_payloads: bool,
    pub concurrency: usize,
    pub cancel: CancellationToken,
    /// Present for continuous warming, and for the startup pass before it.
    pub tracker: Option<Arc<SstWarmTracker>>,
}

/// Whether a server in this role and configuration runs any warming at all.
pub fn warming_enabled(config: &CacheWarmerConfig, serves_reads: bool) -> bool {
    serves_reads && (config.enabled || config.continuous.enabled)
}

/// Spawns the startup pass (when enabled) and then, when continuous warming
/// is enabled, a pass every `continuous.interval_seconds` that warms only SSTs
/// that appeared since the previous pass. `cache_warmed` is set once the
/// startup pass ends, whatever its outcome.
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
    tokio::spawn(async move {
        if config.enabled {
            let timeout = Duration::from_secs(config.timeout_seconds);
            metrics::gauge!("telemetry_cache_warmer_active", "product" => product).set(1.0);
            let pass = WarmPass {
                range: Duration::from_secs(config.warm_range_seconds),
                include_payloads: config.include_payloads,
                concurrency: config.concurrency,
                cancel: cancellation.clone(),
                tracker: tracker.clone(),
            };
            let status = run_pass(
                product,
                "startup",
                Some(timeout),
                tracker.as_deref(),
                warm(pass),
            )
            .await;
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
                range: Duration::from_secs(continuous.warm_range_seconds),
                include_payloads: continuous.include_payloads,
                concurrency: config.concurrency,
                cancel: cancellation.clone(),
                tracker: Some(Arc::clone(&tracker)),
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
                recorded.lock().unwrap().push((
                    pass.range,
                    pass.include_payloads,
                    pass.tracker.is_some(),
                ));
                async { Ok::<(), String>(()) }
            },
        );

        tokio::time::sleep(Duration::from_secs(31)).await;
        assert!(cache_warmed.load(Ordering::Acquire));
        cancellation.cancel();
        task.await.unwrap();

        let passes = passes.lock().unwrap();
        assert_eq!(passes[0], (Duration::from_secs(7_200), false, true));
        assert!(
            passes.len() >= 3,
            "expected continuous passes, got {passes:?}"
        );
        assert!(
            passes[1..]
                .iter()
                .all(|pass| *pass == (Duration::from_secs(600), true, true))
        );
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
}
