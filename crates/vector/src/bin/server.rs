//! OpenData Vector HTTP Server binary entry point.

use std::sync::Arc;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[cfg(feature = "kubernetes")]
use tokio_util::sync::CancellationToken;
use vector::cluster::{ClusterConfig, Role};
use vector::server::{
    AccessConfig, ClusterServer, Guard, MetricsState, NoPeers, Peers, VectorReaderServer,
    VectorServer, VectorServerConfig, load_cluster_config, load_reader_config, load_vector_config,
};
use vector::{VectorCluster, VectorDb, VectorDbReader};

/// CLI arguments for the vector server.
#[derive(Debug, Parser)]
#[command(name = "plural-vector")]
#[command(about = "OpenData Vector HTTP Server")]
struct CliArgs {
    /// HTTP server port.
    #[arg(long, default_value = "8080")]
    port: u16,

    #[command(subcommand)]
    command: Command,
}

/// Server mode subcommands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Run the multi-namespace, sharded vector cluster server.
    Cluster {
        /// Path to the cluster configuration file (YAML).
        #[arg(long)]
        config: String,
        /// Overrides the configured role: standalone, writer or reader.
        #[arg(long, env = "VECTOR_ROLE")]
        role: Option<String>,
        /// This replica's name: a writer's owner name in the topology.
        #[arg(long, env = "POD_NAME")]
        identity: Option<String>,
        /// Readers: the shard to serve (default: every shard).
        #[arg(long, env = "READER_SHARD")]
        reader_shard: Option<u32>,
    },
    /// Run the read-write single-collection vector server.
    Vector {
        /// Path to the vector database configuration file (YAML).
        #[arg(long)]
        config: String,
    },
    /// Run the read-only vector reader server.
    Reader {
        /// Path to the reader configuration file (YAML).
        #[arg(long)]
        config: String,
    },
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Install the metrics-rs Prometheus recorder before any subsystem
    // (including slatedb) emits metrics. The resulting handle is rendered by
    // the `/metrics` route.
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let metrics_state = MetricsState {
        handle: recorder.handle(),
    };
    metrics::set_global_recorder(recorder).expect("global metrics recorder already installed");

    // Parse CLI arguments
    let args = CliArgs::parse();
    let server_config = VectorServerConfig {
        port: args.port,
        ..VectorServerConfig::default()
    };

    match args.command {
        Command::Cluster {
            config,
            role,
            identity,
            reader_shard,
        } => {
            let mut cluster_config = load_cluster_config(&config);
            if let Some(role) = role {
                cluster_config.role = serde_yaml::from_str(&role)
                    .unwrap_or_else(|e| panic!("invalid role '{role}': {e}"));
            }
            if reader_shard.is_some() {
                cluster_config.reader_shard = reader_shard;
            }
            tracing::info!("Opening vector cluster with config: {:?}", cluster_config);
            let raw = std::fs::read_to_string(&config)
                .unwrap_or_else(|e| panic!("Failed to read config file '{config}': {e}"));
            let access = AccessConfig::from_yaml(&raw).unwrap_or_else(|e| panic!("{e}"));
            let server_config = VectorServerConfig::from_cluster_yaml(args.port, &raw)
                .unwrap_or_else(|e| panic!("{e}"));
            let guard = Arc::new(
                Guard::new(&access)
                    .await
                    .expect("Failed to configure authentication"),
            );
            let running: serde_json::Value = serde_yaml::from_str(&raw)
                .unwrap_or_else(|e| panic!("Failed to parse config file '{config}': {e}"));
            let (cluster, peers) = open_cluster(cluster_config, identity).await;
            cluster.declare_schemas(access.schemas());
            cluster.spawn_schema_applier();
            guard.watch(config.into(), running, Arc::clone(&cluster));
            ClusterServer::new(cluster, server_config, metrics_state)
                .with_peers(peers)
                .with_guard(guard)
                .run()
                .await;
        }
        Command::Vector { config } => {
            let vector_config = load_vector_config(&config);
            tracing::info!("Opening vector database with config: {:?}", vector_config);
            let metadata_fields = vector_config.metadata_fields.clone();

            let db = Arc::new(
                VectorDb::open(vector_config)
                    .await
                    .expect("Failed to open vector database"),
            );

            let server = VectorServer::new(db, server_config, metadata_fields, metrics_state);
            server.run().await;
        }
        Command::Reader { config } => {
            let reader_config = load_reader_config(&config);
            tracing::info!(
                "Opening vector database reader with config: {:?}",
                reader_config
            );
            let metadata_fields = reader_config.metadata_fields.clone();

            let reader = VectorDbReader::open(reader_config)
                .await
                .expect("Failed to open vector database reader");

            let server = VectorReaderServer::new(
                Arc::new(reader),
                server_config,
                metadata_fields,
                metrics_state,
            );
            server.run().await;
        }
    }
}

/// Opens the cluster in its configured role, with how to reach its peers.
async fn open_cluster(
    config: ClusterConfig,
    identity: Option<String>,
) -> (Arc<VectorCluster>, Arc<dyn Peers>) {
    if config.role == Role::Standalone {
        let cluster = VectorCluster::open(config)
            .await
            .expect("Failed to open vector cluster");
        return (Arc::new(cluster), Arc::new(NoPeers));
    }
    open_replica(config, identity).await
}

#[cfg(feature = "kubernetes")]
async fn open_replica(
    config: ClusterConfig,
    identity: Option<String>,
) -> (Arc<VectorCluster>, Arc<dyn Peers>) {
    use vector::cluster::kubernetes;
    let identity = identity.unwrap_or_else(|| match config.role {
        Role::Reader => format!("reader-{}", std::process::id()),
        _ => panic!("writer replicas need --identity or POD_NAME"),
    });
    let settings = kubernetes::settings(&config).expect("Invalid Kubernetes settings");
    let runtime = kubernetes::runtime(&config, identity, CancellationToken::new())
        .await
        .expect("Failed to connect to Kubernetes");
    let cluster = Arc::new(
        VectorCluster::open_with(config, runtime)
            .await
            .expect("Failed to open vector cluster replica"),
    );
    cluster.spawn_reconciler();
    (cluster, Arc::new(settings))
}

#[cfg(not(feature = "kubernetes"))]
async fn open_replica(
    config: ClusterConfig,
    _identity: Option<String>,
) -> (Arc<VectorCluster>, Arc<dyn Peers>) {
    panic!(
        "the {:?} role runs on Kubernetes; build with the 'kubernetes' feature",
        config.role
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_server_config_from_cli_args() {
        // given
        let args = CliArgs {
            port: 9090,
            command: Command::Vector {
                config: "unused.yaml".to_string(),
            },
        };

        // when
        let server_config = VectorServerConfig {
            port: args.port,
            ..VectorServerConfig::default()
        };

        // then
        assert_eq!(server_config.port, 9090);
    }
}
