use std::{collections::HashSet, fs, net::SocketAddr, path::Path, time::Duration};

use common::{CacheWarmerConfig, storage::config::StorageConfig};
use serde::{Deserialize, Serialize};
pub use server_common::auth::{Access, AuthConfig};
pub use server_common::config::{Durability, WriteConfig};
pub use server_common::usage::UsageReportingConfig;

pub use sharding::server::{ServerMode, StaticOwner};

#[derive(Debug, Clone, Default)]
pub struct TracesProduct;

impl sharding::server::Product for TracesProduct {
    const NAME: &'static str = "traces";
    const OWNER_PORT: u16 = 9092;
}

pub type ShardingConfig = sharding::server::ShardingConfig<TracesProduct>;
pub type ShardingBackend = sharding::server::ShardingBackend<TracesProduct>;
pub type KubernetesShardingConfig = sharding::server::KubernetesShardingConfig<TracesProduct>;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceConfig {
    pub name: String,
    #[serde(default)]
    pub auth: Access,
    /// Console gRPC endpoint that receives this namespace's ingest usage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_reporting_endpoint: Option<String>,
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
        let value = plural_traces::PageConfig::default();
        Self {
            target_size_bytes: value.target_size_bytes,
            max_size_bytes: value.max_size_bytes,
            max_traces: value.max_traces,
        }
    }
}

/// Per-shard byte budgets of the cross-query read caches; zero disables one.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReadCacheConfig {
    pub metadata_bytes: u64,
    pub postings_bytes: u64,
    pub pages_bytes: u64,
    pub locators_bytes: u64,
}

impl Default for ReadCacheConfig {
    fn default() -> Self {
        let value = plural_traces::ReadCacheConfig::default();
        Self {
            metadata_bytes: value.metadata_bytes,
            postings_bytes: value.postings_bytes,
            pages_bytes: value.pages_bytes,
            locators_bytes: value.locators_bytes,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RequestConfig {
    /// Cap on a request body as received, before content decoding.
    pub max_request_bytes: usize,
    /// Cap on a write body after gzip decoding, and on a decoded gRPC message.
    pub max_decoded_request_bytes: usize,
    pub request_concurrency: usize,
    pub query_concurrency: usize,
    pub max_candidates: usize,
    pub max_spans_per_trace: usize,
    pub max_query_limit: usize,
}

impl Default for RequestConfig {
    fn default() -> Self {
        Self {
            max_request_bytes: server_common::http::DEFAULT_MAX_REQUEST_BYTES,
            max_decoded_request_bytes: server_common::http::DEFAULT_MAX_DECODED_REQUEST_BYTES,
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
    /// Optional prefix for public read and write HTTP APIs, such as `/traces`.
    pub path_prefix: String,
    pub storage: StorageConfig,
    pub segment_duration_seconds: u64,
    /// How long data is kept, counted from ingestion; 14 days by default,
    /// `null` keeps it forever.
    pub retention_seconds: Option<u64>,
    pub page: PageConfig,
    pub read_cache: ReadCacheConfig,
    pub write: WriteConfig,
    pub sharding: ShardingConfig,
    pub request: RequestConfig,
    pub cache_warmer: CacheWarmerConfig,
    pub auth: AuthConfig,
    pub usage_reporting: UsageReportingConfig,
    pub namespaces: Vec<NamespaceConfig>,
}

impl Default for Config {
    fn default() -> Self {
        let core = plural_traces::Config::default();
        Self {
            mode: ServerMode::Standalone,
            listeners: ListenerConfig::default(),
            path_prefix: String::new(),
            storage: core.storage,
            segment_duration_seconds: core.segment_duration.as_secs(),
            retention_seconds: core.retention.map(|value| value.as_secs()),
            page: PageConfig::default(),
            read_cache: ReadCacheConfig::default(),
            write: WriteConfig::default(),
            sharding: ShardingConfig::default(),
            request: RequestConfig::default(),
            cache_warmer: CacheWarmerConfig::default(),
            auth: AuthConfig::default(),
            usage_reporting: UsageReportingConfig::default(),
            namespaces: vec![NamespaceConfig {
                name: "default".into(),
                auth: Access::default(),
                usage_reporting_endpoint: None,
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
        if !self.path_prefix.is_empty()
            && (!self.path_prefix.starts_with('/')
                || self.path_prefix.len() == 1
                || self.path_prefix.ends_with('/'))
        {
            return Err(ConfigError::Validation(
                "path_prefix must be empty or start with '/' and must not end with '/'".into(),
            ));
        }
        if self.segment_duration_seconds == 0
            || self.page.target_size_bytes == 0
            || self.page.max_size_bytes == 0
            || self.page.max_traces == 0
            || self.write.has_zero_limit()
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
        server_common::http::validate_request_limits(
            self.request.max_request_bytes,
            self.request.max_decoded_request_bytes,
        )
        .map_err(ConfigError::Validation)?;
        if self.page.target_size_bytes > self.page.max_size_bytes {
            return Err(ConfigError::Validation(
                "page target_size_bytes cannot exceed max_size_bytes".into(),
            ));
        }
        if self.cache_warmer.enabled && self.cache_warmer.warm_range_seconds == 0 {
            return Err(ConfigError::Validation(
                "cache_warmer.warm_range_seconds must be greater than zero when enabled".into(),
            ));
        }
        if self.cache_warmer.enabled && self.cache_warmer.timeout_seconds == 0 {
            return Err(ConfigError::Validation(
                "cache_warmer.timeout_seconds must be greater than zero when enabled".into(),
            ));
        }
        if self.cache_warmer.enabled && self.cache_warmer.concurrency == 0 {
            return Err(ConfigError::Validation(
                "cache_warmer.concurrency must be greater than zero when enabled".into(),
            ));
        }
        if self.namespaces.is_empty() {
            return Err(ConfigError::Validation(
                "at least one namespace is required".into(),
            ));
        }
        let mut names = HashSet::new();
        for namespace in &self.namespaces {
            plural_traces::Namespace::new(&namespace.name)
                .map_err(|error| ConfigError::Validation(error.to_string()))?;
            if !names.insert(&namespace.name) {
                return Err(ConfigError::Validation("duplicate namespace".into()));
            }
            if let Some(endpoint) = &namespace.usage_reporting_endpoint {
                server_common::usage::validate_endpoint(endpoint)
                    .map_err(ConfigError::Validation)?;
            }
        }
        self.usage_reporting
            .validate()
            .map_err(ConfigError::Validation)?;
        self.sharding
            .validate(self.mode)
            .map_err(ConfigError::Validation)?;
        Ok(())
    }

    pub(crate) fn traces_config(&self) -> plural_traces::Config {
        plural_traces::Config {
            storage: self.storage.clone(),
            segment_duration: Duration::from_secs(self.segment_duration_seconds),
            retention: self.retention_seconds.map(Duration::from_secs),
            page: plural_traces::PageConfig {
                target_size_bytes: self.page.target_size_bytes,
                max_size_bytes: self.page.max_size_bytes,
                max_traces: self.page.max_traces,
            },
            write_buffer: self.write.write_buffer(),
            read_cache: plural_traces::ReadCacheConfig {
                metadata_bytes: self.read_cache.metadata_bytes,
                postings_bytes: self.read_cache.postings_bytes,
                pages_bytes: self.read_cache.pages_bytes,
                locators_bytes: self.read_cache.locators_bytes,
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
            serde_yaml::from_str(include_str!("../../../config/traces.example.yaml")).unwrap();
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
        config.sharding.shards = 2;
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

    #[test]
    fn path_prefix_must_be_canonical() {
        for invalid in ["traces", "/", "/traces/"] {
            let config = Config {
                path_prefix: invalid.into(),
                ..Config::default()
            };
            assert!(config.validate().is_err());
        }
        let config = Config {
            path_prefix: "/traces".into(),
            ..Config::default()
        };
        config.validate().unwrap();
    }
}
