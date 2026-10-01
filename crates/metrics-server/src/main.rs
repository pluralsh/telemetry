use std::path::PathBuf;

use clap::Parser;
use plural_metrics_server::{AppState, config::Config, grpc_service, router};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

#[derive(Debug, Parser)]
#[command(name = "plural-metrics-server")]
struct Args {
    #[arg(short, long, default_value = "config/metrics.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    server_common::runtime::install_rustls_crypto_provider()?;
    tracing_subscriber::fmt::init();
    server_common::runtime::install_metrics_recorder()?;
    metrics::gauge!("telemetry_server_up", "product" => "metrics").set(1.0);
    let args = Args::parse();
    let config = Config::from_path(args.config)?;
    let http_addr = config.listeners.http;
    let grpc_addr = config.listeners.grpc;
    let state = AppState::open(config).await?;
    let cancellation = CancellationToken::new();
    let http_cancel = cancellation.clone();
    let grpc_cancel = cancellation.clone();
    let http = axum::serve(TcpListener::bind(http_addr).await?, router(state.clone()))
        .with_graceful_shutdown(http_cancel.cancelled_owned());
    let grpc = Server::builder()
        .add_service(grpc_service(state.clone()))
        .serve_with_shutdown(grpc_addr, grpc_cancel.cancelled_owned());

    tokio::select! {
        result = http => result?,
        result = grpc => result?,
        _ = server_common::runtime::shutdown_signal() => {}
    }
    cancellation.cancel();
    state.shutdown().await
}
