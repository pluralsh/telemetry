//! HTTP server for OpenData Vector.
//!
//! This module provides an HTTP API for interacting with the vector database,
//! exposing upsert, search, and get operations via REST endpoints.

mod auth;
mod cluster;
mod config;
mod error;
mod handlers;
mod metrics;
mod middleware;
pub mod openapi;
mod proto;
mod request;
mod response;
mod vector;
mod vector_reader;

pub use auth::{AccessConfig, Guard, INTERNAL_TOKEN_HEADER, NamespaceAccess};
pub use cluster::{
    ClusterServer, FORWARDED_HEADER, NoPeers, Peers, StaticPeers, cluster_router,
    cluster_router_guarded, cluster_router_prefixed, cluster_router_with,
};
pub use config::{VectorServerConfig, load_cluster_config, load_reader_config, load_vector_config};
pub use metrics::MetricsState;
pub use request::{JsonComparisonFilter, JsonFilter, JsonInFilter};
pub use vector::VectorServer;
pub use vector_reader::VectorReaderServer;
