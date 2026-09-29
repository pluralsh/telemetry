use std::path::PathBuf;

use clap::Parser;
use meter_server::{AppState, config::Config, grpc_service, router};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

#[derive(Debug, Parser)]
#[command(name = "meter-server")]
struct Args {
    #[arg(short, long, default_value = "config/meter.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    meter_server::install_rustls_crypto_provider()?;
    tracing_subscriber::fmt::init();
    meter_server::runtime_metrics::install_recorder()?;
    metrics::gauge!("telemetry_server_up", "product" => "meter").set(1.0);
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
        _ = shutdown_signal() => {}
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
