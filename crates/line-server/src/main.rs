use std::path::PathBuf;

use clap::Parser;
use line_server::{AppState, config::Config, grpc_service, router};
use tokio::net::TcpListener;
use tonic::transport::Server;

#[derive(Debug, Parser)]
#[command(name = "line-server")]
struct Args {
    #[arg(short, long, default_value = "config/line.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    meter_server::install_rustls_crypto_provider()?;
    tracing_subscriber::fmt::init();
    meter_server::runtime_metrics::install_recorder()?;
    metrics::gauge!("telemetry_server_up", "product" => "line").set(1.0);
    let config = Config::from_path(Args::parse().config)?;
    let address = config.listeners.http;
    let grpc_address = config.listeners.grpc;
    let state = AppState::open(config).await?;
    let cancellation = tokio_util::sync::CancellationToken::new();
    let server = axum::serve(TcpListener::bind(address).await?, router(state.clone()))
        .with_graceful_shutdown(cancellation.clone().cancelled_owned());
    let grpc = Server::builder()
        .add_service(grpc_service(state.clone()))
        .serve_with_shutdown(grpc_address, cancellation.clone().cancelled_owned());
    tokio::select! {
        result = server => result?,
        result = grpc => result?,
        () = shutdown_signal() => {}
    }
    cancellation.cancel();
    state.shutdown().await
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
