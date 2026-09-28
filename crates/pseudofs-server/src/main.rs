// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::path::PathBuf;

use clap::Parser;
use pseudofs_server::config::Config;
use tonic::transport::Server;

#[derive(Debug, Parser)]
#[command(name = "pseudofs-server")]
struct Args {
    #[arg(short, long, default_value = "config/pseudofs.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let config = Config::from_path(Args::parse().config)?;
    let (fs, service) = pseudofs_server::open(&config).await?;
    let api = service.into_server(
        config.max_decoding_message_bytes,
        config.max_encoding_message_bytes,
    );
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<proto::pseudofs::v1::pseudo_fs_server::PseudoFsServer<
            pseudofs_server::Service,
        >>()
        .await;
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()?;
    let cancellation = tokio_util::sync::CancellationToken::new();
    let shutdown = cancellation.clone();
    let address = config.listener;
    let server = Server::builder()
        .add_service(health_service)
        .add_service(reflection)
        .add_service(api)
        .serve_with_shutdown(address, cancellation.cancelled_owned());
    tokio::select! {
        result = server => result?,
        () = shutdown_signal() => {}
    }
    shutdown.cancel();
    fs.flush().await?;
    fs.close().await?;
    Ok(())
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
