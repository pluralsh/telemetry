use std::path::PathBuf;

use clap::Parser;
use plural_traces_server::{
    AppState, config::Config, grpc_service, jaeger_grpc_service, jaeger_ingest_layer,
    otlp_grpc_service, otlp_ingest_layer, router,
};
use tokio::net::TcpListener;
use tonic::transport::Server;

/// Query paths allocate and free per row; the system allocator made this
/// most of their CPU time.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(name = "plural-traces-server")]
struct Args {
    #[arg(short, long, default_value = "config/traces.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    server_common::runtime::install_rustls_crypto_provider()?;
    tracing_subscriber::fmt::init();
    server_common::runtime::install_metrics_recorder()?;
    metrics::gauge!("telemetry_server_up", "product" => "traces").set(1.0);
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
        .layer(otlp_ingest_layer(&state))
        .add_service(otlp_grpc_service(state.clone()))
        .serve_with_shutdown(otlp_address, cancellation.clone().cancelled_owned());
    let jaeger = Server::builder()
        .layer(jaeger_ingest_layer(&state))
        .add_service(jaeger_grpc_service(state.clone()))
        .serve_with_shutdown(jaeger_address, cancellation.clone().cancelled_owned());
    tokio::select! {
        result = http => result?,
        result = internal => result?,
        result = otlp => result?,
        result = jaeger => result?,
        () = server_common::runtime::shutdown_signal() => {}
    }
    cancellation.cancel();
    state.shutdown().await
}
