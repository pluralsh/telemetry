//! Per-namespace usage reporting to Plural Console.
//!
//! Accepted ingest bytes are buffered per namespace and periodically flushed
//! to that namespace's endpoint with Console's `PluralServer.MeterMetrics`
//! RPC, the same contract the Console observability proxy uses. A failed
//! flush is added back to the buffer and retried on the next tick.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use proto::plrl::{MeterMetricsRequest, plural_server_client::PluralServerClient};
use serde::{Deserialize, Serialize};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

use crate::ingest::{IngestMiddleware, IngestRequest, Signal};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct UsageReportingConfig {
    pub flush_interval_seconds: u64,
    pub timeout_seconds: u64,
}

impl Default for UsageReportingConfig {
    fn default() -> Self {
        Self {
            flush_interval_seconds: 30,
            timeout_seconds: 10,
        }
    }
}

impl UsageReportingConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.flush_interval_seconds == 0 || self.timeout_seconds == 0 {
            return Err(
                "usage_reporting flush interval and timeout must be greater than zero".to_owned(),
            );
        }
        Ok(())
    }
}

/// Validates a namespace's `usage_reporting_endpoint`, which is either
/// `host:port` or `http://host:port`.
pub fn validate_endpoint(endpoint: &str) -> Result<(), String> {
    let uri = endpoint_uri(endpoint)?;
    Endpoint::from_shared(uri)
        .map(drop)
        .map_err(|error| format!("invalid usage reporting endpoint {endpoint:?}: {error}"))
}

fn endpoint_uri(endpoint: &str) -> Result<String, String> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err("usage reporting endpoint must not be empty".to_owned());
    }
    if endpoint.starts_with("http://") {
        return Ok(endpoint.to_owned());
    }
    if endpoint.contains("://") {
        return Err(format!(
            "usage reporting endpoint {endpoint:?} must be host:port or http://host:port"
        ));
    }
    Ok(format!("http://{endpoint}"))
}

struct NamespaceUsage {
    name: String,
    client: PluralServerClient<Channel>,
    bytes: AtomicU64,
}

impl NamespaceUsage {
    async fn flush(&self, signal: Signal) {
        let bytes = self.bytes.swap(0, Ordering::AcqRel);
        if bytes == 0 {
            return;
        }
        let request = MeterMetricsRequest {
            bytes: i64::try_from(bytes).unwrap_or(i64::MAX),
        };
        let error = match self.client.clone().meter_metrics(request).await {
            Ok(response) if response.get_ref().success => {
                metrics::counter!(
                    "telemetry_usage_reported_bytes_total",
                    "product" => signal.as_str(),
                    "namespace" => self.name.clone()
                )
                .increment(bytes);
                return;
            }
            Ok(_) => "unsuccessful response".to_owned(),
            Err(status) => status.to_string(),
        };
        self.bytes.fetch_add(bytes, Ordering::AcqRel);
        metrics::counter!(
            "telemetry_usage_report_failures_total",
            "product" => signal.as_str(),
            "namespace" => self.name.clone()
        )
        .increment(1);
        tracing::warn!(
            namespace = %self.name,
            bytes,
            %error,
            "usage report failed; retrying next flush"
        );
    }
}

pub struct UsageReporter {
    signal: Signal,
    interval: Duration,
    namespaces: HashMap<String, Arc<NamespaceUsage>>,
}

impl UsageReporter {
    /// Returns `None` when no namespace has a usage reporting endpoint.
    /// Must be called within a Tokio runtime.
    pub fn new<'a>(
        signal: Signal,
        namespaces: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
        config: &UsageReportingConfig,
    ) -> anyhow::Result<Option<Self>> {
        let timeout = Duration::from_secs(config.timeout_seconds);
        let mut channels: HashMap<String, Channel> = HashMap::new();
        let mut usage = HashMap::new();
        for (name, endpoint) in namespaces {
            let Some(endpoint) = endpoint else {
                continue;
            };
            let uri = endpoint_uri(endpoint).map_err(anyhow::Error::msg)?;
            let channel = match channels.get(&uri) {
                Some(channel) => channel.clone(),
                None => {
                    let channel = Endpoint::from_shared(uri.clone())?
                        .connect_timeout(timeout)
                        .timeout(timeout)
                        .connect_lazy();
                    channels.insert(uri, channel.clone());
                    channel
                }
            };
            usage.insert(
                name.to_owned(),
                Arc::new(NamespaceUsage {
                    name: name.to_owned(),
                    client: PluralServerClient::new(channel),
                    bytes: AtomicU64::new(0),
                }),
            );
        }
        if usage.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            signal,
            interval: Duration::from_secs(config.flush_interval_seconds),
            namespaces: usage,
        }))
    }

    pub fn add_bytes(&self, namespace: &str, bytes: u64) {
        if bytes == 0 {
            return;
        }
        if let Some(usage) = self.namespaces.get(namespace) {
            usage.bytes.fetch_add(bytes, Ordering::AcqRel);
        }
    }

    /// Flushes every namespace concurrently so one slow endpoint cannot
    /// delay the others.
    pub async fn flush(&self) {
        let mut flushes = JoinSet::new();
        for usage in self.namespaces.values() {
            let usage = Arc::clone(usage);
            let signal = self.signal;
            flushes.spawn(async move { usage.flush(signal).await });
        }
        while flushes.join_next().await.is_some() {}
    }

    /// Flushes on every interval, and once more when `cancellation` fires.
    pub fn spawn(self: Arc<Self>, cancellation: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => {
                        self.flush().await;
                        return;
                    }
                    _ = ticker.tick() => self.flush().await,
                }
            }
        })
    }

    #[cfg(test)]
    fn pending(&self, namespace: &str) -> u64 {
        self.namespaces[namespace].bytes.load(Ordering::Acquire)
    }
}

impl IngestMiddleware for UsageReporter {
    fn record(&self, request: &IngestRequest<'_>) {
        self.add_bytes(request.namespace, request.bytes);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use proto::plrl::{
        AiConfig, AiConfigRequest, MeterMetricsResponse, ObservabilityConfig,
        ObservabilityConfigRequest, ProxyAuthenticationRequest, ProxyAuthenticationResponse,
        VerifyClusterRequest, VerifyClusterResponse,
        plural_server_server::{PluralServer, PluralServerServer},
    };
    use tokio::net::TcpListener;
    use tonic::{Request, Response, Status};

    use super::*;

    #[derive(Clone, Default)]
    struct Console {
        metered: Arc<Mutex<Vec<i64>>>,
    }

    #[tonic::async_trait]
    impl PluralServer for Console {
        async fn meter_metrics(
            &self,
            request: Request<MeterMetricsRequest>,
        ) -> Result<Response<MeterMetricsResponse>, Status> {
            self.metered.lock().unwrap().push(request.get_ref().bytes);
            Ok(Response::new(MeterMetricsResponse { success: true }))
        }

        async fn get_ai_config(
            &self,
            _: Request<AiConfigRequest>,
        ) -> Result<Response<AiConfig>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn get_observability_config(
            &self,
            _: Request<ObservabilityConfigRequest>,
        ) -> Result<Response<ObservabilityConfig>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn proxy_authentication(
            &self,
            _: Request<ProxyAuthenticationRequest>,
        ) -> Result<Response<ProxyAuthenticationResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn verify_cluster(
            &self,
            _: Request<VerifyClusterRequest>,
        ) -> Result<Response<VerifyClusterResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
    }

    async fn serve(console: Console) -> (String, CancellationToken) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let cancel = CancellationToken::new();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(PluralServerServer::new(console))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    cancel.clone().cancelled_owned(),
                ),
        );
        (address, cancel)
    }

    fn config() -> UsageReportingConfig {
        UsageReportingConfig {
            flush_interval_seconds: 3600,
            timeout_seconds: 1,
        }
    }

    #[test]
    fn endpoints_must_be_plaintext_host_port() {
        validate_endpoint("console:50051").unwrap();
        validate_endpoint("http://console.plrl-console:50051").unwrap();
        assert!(validate_endpoint("").is_err());
        assert!(validate_endpoint("https://console:50051").is_err());
    }

    #[tokio::test]
    async fn buffers_per_namespace_and_flushes_to_each_endpoint() {
        let first = Console::default();
        let second = Console::default();
        let (first_address, first_cancel) = serve(first.clone()).await;
        let (second_address, second_cancel) = serve(second.clone()).await;
        let reporter = UsageReporter::new(
            Signal::Logs,
            [
                ("a", Some(first_address.as_str())),
                ("b", Some(second_address.as_str())),
                ("unmetered", None),
            ],
            &config(),
        )
        .unwrap()
        .unwrap();

        reporter.add_bytes("a", 10);
        reporter.add_bytes("a", 5);
        reporter.add_bytes("b", 7);
        reporter.add_bytes("unmetered", 100);
        reporter.flush().await;
        reporter.flush().await;

        assert_eq!(*first.metered.lock().unwrap(), vec![15]);
        assert_eq!(*second.metered.lock().unwrap(), vec![7]);
        first_cancel.cancel();
        second_cancel.cancel();
    }

    #[tokio::test]
    async fn failed_flushes_are_retried() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        drop(listener);
        let reporter =
            UsageReporter::new(Signal::Traces, [("a", Some(address.as_str()))], &config())
                .unwrap()
                .unwrap();

        reporter.add_bytes("a", 9);
        reporter.flush().await;
        assert_eq!(reporter.pending("a"), 9);
    }

    #[tokio::test]
    async fn cancellation_flushes_remaining_usage() {
        let console = Console::default();
        let (address, cancel) = serve(console.clone()).await;
        let reporter = Arc::new(
            UsageReporter::new(Signal::Metrics, [("a", Some(address.as_str()))], &config())
                .unwrap()
                .unwrap(),
        );
        let shutdown = CancellationToken::new();
        let task = Arc::clone(&reporter).spawn(shutdown.clone());

        reporter.record(&IngestRequest {
            signal: Signal::Metrics,
            namespace: "a",
            bytes: 3,
        });
        shutdown.cancel();
        task.await.unwrap();

        assert_eq!(*console.metered.lock().unwrap(), vec![3]);
        cancel.cancel();
    }
}
