use std::path::PathBuf;

use clap::Parser;
use plural_logs_server::{AppState, config::Config, grpc_service, router};
use tokio::net::TcpListener;
use tonic::transport::Server;

/// Query paths allocate and free per row; the system allocator made this
/// most of their CPU time.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(name = "plural-logs-server")]
struct Args {
    #[arg(short, long, default_value = "config/logs.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    server_common::runtime::install_rustls_crypto_provider()?;
    tracing_subscriber::fmt::init();
    server_common::runtime::install_metrics_recorder()?;
    metrics::gauge!("telemetry_server_up", "product" => "logs").set(1.0);
    let path = Args::parse().config;
    let config = Config::from_path(&path)?;
    let address = config.listeners.http;
    let grpc_address = config.listeners.grpc;
    let state = AppState::open(config).await?;
    state.watch_config(path).await;
    let cancellation = tokio_util::sync::CancellationToken::new();
    let server = axum::serve(TcpListener::bind(address).await?, router(state.clone()))
        .with_graceful_shutdown(cancellation.clone().cancelled_owned());
    let grpc = Server::builder()
        .add_service(grpc_service(state.clone()))
        .serve_with_shutdown(grpc_address, cancellation.clone().cancelled_owned());
    tokio::select! {
        result = server => result?,
        result = grpc => result?,
        () = server_common::runtime::shutdown_signal() => {}
    }
    cancellation.cancel();
    state.shutdown().await
}
