use std::path::PathBuf;

use clap::Parser;
use tokio::net::TcpListener;
use tonic::transport::Server;
use track_server::{
    AppState, config::Config, grpc_service, jaeger_grpc_service, otlp_grpc_service, router,
};

#[derive(Debug, Parser)]
#[command(name = "track-server")]
struct Args {
    #[arg(short, long, default_value = "config/track.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    meter_server::install_rustls_crypto_provider()?;
    tracing_subscriber::fmt::init();
    let config = Config::from_path(Args::parse().config)?;
    let http_address = config.listeners.http;
    let grpc_address = config.listeners.grpc;
    let otlp_address = config.listeners.otlp_grpc;
    let jaeger_address = config.listeners.jaeger_grpc;
    let state = AppState::open(config).await?;
    let cancellation = tokio_util::sync::CancellationToken::new();
    let http = axum::serve(
        TcpListener::bind(http_address).await?,
        router(state.clone()),
    )
    .with_graceful_shutdown(cancellation.clone().cancelled_owned());
    let internal = Server::builder()
        .add_service(grpc_service(state.clone()))
        .serve_with_shutdown(grpc_address, cancellation.clone().cancelled_owned());
    let otlp = tonic_otlp::transport::Server::builder()
        .add_service(otlp_grpc_service(state.clone()))
        .serve_with_shutdown(otlp_address, cancellation.clone().cancelled_owned());
    let jaeger = Server::builder()
        .add_service(jaeger_grpc_service(state.clone()))
        .serve_with_shutdown(jaeger_address, cancellation.clone().cancelled_owned());
    tokio::select! {
        result = http => result?,
        result = internal => result?,
        result = otlp => result?,
        result = jaeger => result?,
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
