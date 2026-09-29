use std::{collections::HashSet, fs, net::SocketAddr, path::Path, time::Duration};

use common::storage::config::StorageConfig;
pub use meter_server::config::{Access, AuthConfig};
use serde::{Deserialize, Serialize};

pub use sharding::server::{ServerMode, StaticOwner};

#[derive(Debug, Clone, Default)]
pub struct TrackProduct;

impl sharding::server::Product for TrackProduct {
    const NAME: &'static str = "track";
    const OWNER_PORT: u16 = 9092;
}

pub type ShardingConfig = sharding::server::ShardingConfig<TrackProduct>;
pub type ShardingBackend = sharding::server::ShardingBackend<TrackProduct>;
pub type KubernetesShardingConfig = sharding::server::KubernetesShardingConfig<TrackProduct>;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    #[default]
    Applied,
    Written,
    Durable,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WriteConfig {
    pub durability: Durability,
    /// Interval between durable storage flushes. Zero disables periodic flushes.
    pub flush_interval_seconds: u64,
    pub buffer_queue_capacity: usize,
    pub buffer_flush_interval_milliseconds: u64,
    pub buffer_size_threshold_bytes: usize,
    pub remote_concurrency: usize,
    pub remote_retries: usize,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            durability: Durability::Applied,
            flush_interval_seconds: 10,
            buffer_queue_capacity: 10_000,
            buffer_flush_interval_milliseconds: 10_000,
            buffer_size_threshold_bytes: 64 * 1024 * 1024,
            remote_concurrency: 16,
            remote_retries: 2,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceConfig {
    pub name: String,
    #[serde(default)]
    pub auth: Access,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ListenerConfig {
    pub http: SocketAddr,
    pub grpc: SocketAddr,
    pub otlp_grpc: SocketAddr,
    pub jaeger_grpc: SocketAddr,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            http: "0.0.0.0:3200".parse().unwrap(),
            grpc: "0.0.0.0:9092".parse().unwrap(),
            otlp_grpc: "0.0.0.0:4317".parse().unwrap(),
            jaeger_grpc: "0.0.0.0:14250".parse().unwrap(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PageConfig {
    pub target_size_bytes: usize,
    pub max_size_bytes: usize,
    pub max_traces: usize,
}

impl Default for PageConfig {
    fn default() -> Self {
        let value = track::PageConfig::default();
        Self {
            target_size_bytes: value.target_size_bytes,
            max_size_bytes: value.max_size_bytes,
            max_traces: value.max_traces,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RequestConfig {
    pub max_request_bytes: usize,
    pub request_concurrency: usize,
    pub query_concurrency: usize,
    pub max_candidates: usize,
    pub max_spans_per_trace: usize,
    pub max_query_limit: usize,
}

impl Default for RequestConfig {
    fn default() -> Self {
        Self {
            max_request_bytes: 10 * 1024 * 1024,
            request_concurrency: 64,
            query_concurrency: 8,
            max_candidates: 10_000,
            max_spans_per_trace: 100_000,
            max_query_limit: 1_000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub mode: ServerMode,
    pub listeners: ListenerConfig,
    pub storage: StorageConfig,
    pub segment_duration_seconds: u64,
    pub retention_seconds: Option<u64>,
    pub page: PageConfig,
    pub write: WriteConfig,
    pub sharding: ShardingConfig,
    pub request: RequestConfig,
    pub auth: AuthConfig,
    pub namespaces: Vec<NamespaceConfig>,
}

impl Default for Config {
    fn default() -> Self {
        let core = track::Config::default();
        Self {
            mode: ServerMode::Standalone,
            listeners: ListenerConfig::default(),
            storage: core.storage,
            segment_duration_seconds: core.segment_duration.as_secs(),
            retention_seconds: core.retention.map(|value| value.as_secs()),
            page: PageConfig::default(),
            write: WriteConfig::default(),
            sharding: ShardingConfig::default(),
            request: RequestConfig::default(),
            auth: AuthConfig::default(),
            namespaces: vec![NamespaceConfig {
                name: "default".into(),
                auth: Access::default(),
            }],
        }
    }
}

impl Config {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let value = fs::read_to_string(path).map_err(ConfigError::Io)?;
        let config: Self = serde_yaml::from_str(&value).map_err(ConfigError::Yaml)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.segment_duration_seconds == 0
            || self.page.target_size_bytes == 0
            || self.page.max_size_bytes == 0
            || self.page.max_traces == 0
            || self.write.buffer_queue_capacity == 0
            || self.write.buffer_flush_interval_milliseconds == 0
            || self.write.buffer_size_threshold_bytes == 0
            || self.write.remote_concurrency == 0
            || self.request.max_request_bytes == 0
            || self.request.request_concurrency == 0
            || self.request.query_concurrency == 0
            || self.request.max_candidates == 0
            || self.request.max_spans_per_trace == 0
            || self.request.max_query_limit == 0
        {
            return Err(ConfigError::Validation(
                "durations and resource limits must be greater than zero".into(),
            ));
        }
        if self.page.target_size_bytes > self.page.max_size_bytes {
            return Err(ConfigError::Validation(
                "page target_size_bytes cannot exceed max_size_bytes".into(),
            ));
        }
        if self.namespaces.is_empty() {
            return Err(ConfigError::Validation(
                "at least one namespace is required".into(),
            ));
        }
        let mut names = HashSet::new();
        for namespace in &self.namespaces {
            track::Namespace::new(&namespace.name)
                .map_err(|error| ConfigError::Validation(error.to_string()))?;
            if !names.insert(&namespace.name) {
                return Err(ConfigError::Validation("duplicate namespace".into()));
            }
        }
        self.sharding
            .validate(self.mode)
            .map_err(ConfigError::Validation)?;
        Ok(())
    }

    pub(crate) fn track_config(&self) -> track::Config {
        track::Config {
            storage: self.storage.clone(),
            segment_duration: Duration::from_secs(self.segment_duration_seconds),
            retention: self.retention_seconds.map(Duration::from_secs),
            page: track::PageConfig {
                target_size_bytes: self.page.target_size_bytes,
                max_size_bytes: self.page.max_size_bytes,
                max_traces: self.page.max_traces,
            },
            write_buffer: common::coordinator::WriteCoordinatorConfig {
                queue_capacity: self.write.buffer_queue_capacity,
                flush_interval: Duration::from_millis(
                    self.write.buffer_flush_interval_milliseconds,
                ),
                flush_size_threshold: self.write.buffer_size_threshold_bytes,
            },
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read configuration: {0}")]
    Io(#[source] std::io::Error),
    #[error("invalid YAML configuration: {0}")]
    Yaml(#[source] serde_yaml::Error),
    #[error("invalid configuration: {0}")]
    Validation(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_checked_in_example_are_valid() {
        Config::default().validate().unwrap();
        let example: Config =
            serde_yaml::from_str(include_str!("../../../config/track.example.yaml")).unwrap();
        example.validate().unwrap();
        assert_eq!(example.listeners.http.port(), 3200);
        assert_eq!(example.listeners.grpc.port(), 9092);
        assert_eq!(example.listeners.otlp_grpc.port(), 4317);
        assert_eq!(example.listeners.jaeger_grpc.port(), 14250);
    }

    #[test]
    fn rejects_zero_limits_and_incomplete_static_assignments() {
        let mut config = Config::default();
        config.request.max_candidates = 0;
        assert!(config.validate().is_err());
        config.request.max_candidates = 1;
        config.mode = ServerMode::Writer;
        config.sharding.kind = ShardingBackend::Static {
            owner_id: "a".into(),
            owners: vec![StaticOwner {
                id: "a".into(),
                ordinal: 0,
                endpoint: "127.0.0.1:9092".into(),
                start_shard: 0,
                end_shard: 1,
            }],
        };
        assert!(config.validate().is_err());
    }
}
