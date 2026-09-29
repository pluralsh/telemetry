use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Installs the process-wide metrics recorder used by all telemetry products.
///
/// This must run before opening application state so startup and cache-warmer
/// metrics are captured.
pub fn install_recorder() -> anyhow::Result<()> {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|error| anyhow::anyhow!("failed to install Prometheus recorder: {error}"))?;
    PROMETHEUS_HANDLE
        .set(handle)
        .map_err(|_| anyhow::anyhow!("Prometheus recorder is already installed"))
}

/// Renders all process metrics using the Prometheus text exposition format.
pub async fn scrape() -> String {
    PROMETHEUS_HANDLE
        .get()
        .map(PrometheusHandle::render)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn installed_recorder_renders_metrics() {
        super::install_recorder().unwrap();
        metrics::counter!("telemetry_runtime_metrics_test_total").increment(1);

        let body = super::scrape().await;
        assert!(body.contains("telemetry_runtime_metrics_test_total 1"));
    }
}
