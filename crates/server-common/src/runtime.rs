//! Process bootstrap shared by the product server binaries.

use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Selects `ring` when several rustls crypto providers are compiled in.
pub fn install_rustls_crypto_provider() -> anyhow::Result<()> {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return Ok(());
    }
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls ring crypto provider"))
}

/// Installs the process-wide metrics recorder used by all telemetry products.
///
/// This must run before opening application state so startup and cache-warmer
/// metrics are captured.
pub fn install_metrics_recorder() -> anyhow::Result<()> {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|error| anyhow::anyhow!("failed to install Prometheus recorder: {error}"))?;
    PROMETHEUS_HANDLE
        .set(handle)
        .map_err(|_| anyhow::anyhow!("Prometheus recorder is already installed"))
}

/// Renders all process metrics using the Prometheus text exposition format.
pub async fn scrape_metrics() -> String {
    PROMETHEUS_HANDLE
        .get()
        .map(PrometheusHandle::render)
        .unwrap_or_default()
}

/// Resolves on Ctrl-C or, on Unix, SIGTERM. If a handler cannot be
/// installed, that signal is ignored rather than triggering shutdown.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install Ctrl-C handler");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installs_a_crypto_provider_when_multiple_are_compiled() {
        install_rustls_crypto_provider().unwrap();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    #[tokio::test]
    async fn installed_recorder_renders_metrics() {
        install_metrics_recorder().unwrap();
        metrics::counter!("telemetry_runtime_metrics_test_total").increment(1);

        let body = scrape_metrics().await;
        assert!(body.contains("telemetry_runtime_metrics_test_total 1"));
    }
}
