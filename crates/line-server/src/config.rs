use std::{collections::HashSet, fs, net::SocketAddr, path::Path, time::Duration};

use common::storage::config::StorageConfig;
pub use meter_server::config::{Access, AuthConfig, Credential, JwksSource, JwtConfig, Secret};
use serde::{Deserialize, Serialize};

pub use sharding::server::{ServerMode, StaticOwner};

#[derive(Debug, Clone, Default)]
pub struct LineProduct;

impl sharding::server::Product for LineProduct {
    const NAME: &'static str = "line";
    const OWNER_PORT: u16 = 9091;
}

pub type ShardingConfig = sharding::server::ShardingConfig<LineProduct>;
pub type ShardingBackend = sharding::server::ShardingBackend<LineProduct>;
pub type KubernetesShardingConfig = sharding::server::KubernetesShardingConfig<LineProduct>;

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    #[default]
    Applied,
    Written,
    Durable,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceConfig {
    pub name: String,
    #[serde(default)]
    pub auth: Access,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ListenerConfig {
    pub http: SocketAddr,
    pub grpc: SocketAddr,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            http: "0.0.0.0:3100".parse().expect("valid default address"),
            grpc: "0.0.0.0:9091".parse().expect("valid default address"),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PageConfig {
    pub target_size_bytes: usize,
    pub max_rows: usize,
    pub max_age_seconds: u64,
    pub rows_per_block: usize,
}

impl Default for PageConfig {
    fn default() -> Self {
        let page = line::PageConfig::default();
        Self {
            target_size_bytes: page.target_size_bytes,
            max_rows: page.max_rows,
            max_age_seconds: page.max_age.as_secs(),
            rows_per_block: page.rows_per_block,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RequestConfig {
    pub max_request_bytes: usize,
    pub max_query_entries: usize,
    pub max_query_pages: usize,
    pub max_structured_metadata_fields: usize,
    pub query_concurrency: usize,
    pub max_in_flight_query_bytes: usize,
}

impl Default for RequestConfig {
    fn default() -> Self {
        Self {
            max_request_bytes: 10 * 1024 * 1024,
            max_query_entries: 5_000,
            max_query_pages: 10_000,
            max_structured_metadata_fields: 128,
            query_concurrency: 16,
            max_in_flight_query_bytes: 128 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// Maximum number of serialized query responses retained in memory.
    /// Zero disables the response cache.
    pub query_entries: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self { query_entries: 256 }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub mode: ServerMode,
    pub listeners: ListenerConfig,
    /// Optional prefix for public read and write HTTP APIs, such as `/line`.
    pub path_prefix: String,
    pub storage: StorageConfig,
    pub segment_duration_seconds: u64,
    pub retention_seconds: Option<u64>,
    pub page: PageConfig,
    pub write: WriteConfig,
    pub sharding: ShardingConfig,
    pub request: RequestConfig,
    pub cache: CacheConfig,
    pub auth: AuthConfig,
    pub namespaces: Vec<NamespaceConfig>,
}

impl Default for Config {
    fn default() -> Self {
        let core = line::Config::default();
        Self {
            mode: ServerMode::Standalone,
            listeners: ListenerConfig::default(),
            path_prefix: String::new(),
            storage: core.storage,
            segment_duration_seconds: core.segment_duration.as_secs(),
            retention_seconds: core.retention.map(|value| value.as_secs()),
            page: PageConfig::default(),
            write: WriteConfig::default(),
            sharding: ShardingConfig::default(),
            request: RequestConfig::default(),
            cache: CacheConfig::default(),
            auth: AuthConfig::default(),
            namespaces: vec![NamespaceConfig {
                name: "default".to_owned(),
                auth: Access::default(),
            }],
        }
    }
}

impl Config {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let raw = fs::read_to_string(path).map_err(ConfigError::Io)?;
        let config: Self = serde_yaml::from_str(&raw).map_err(ConfigError::Yaml)?;
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
                "path_prefix must be empty or start with '/' and must not end with '/'".to_owned(),
            ));
        }
        if self.segment_duration_seconds == 0
            || self.page.target_size_bytes == 0
            || self.page.max_rows == 0
            || self.page.max_age_seconds == 0
            || self.page.rows_per_block == 0
            || self.request.max_request_bytes == 0
            || self.request.max_query_entries == 0
            || self.request.max_query_pages == 0
            || self.request.max_structured_metadata_fields == 0
            || self.request.query_concurrency == 0
            || self.request.max_in_flight_query_bytes == 0
            || self.write.buffer_queue_capacity == 0
            || self.write.buffer_flush_interval_milliseconds == 0
            || self.write.buffer_size_threshold_bytes == 0
            || self.write.remote_concurrency == 0
        {
            return Err(ConfigError::Validation(
                "durations and resource limits must be greater than zero".to_owned(),
            ));
        }
        if self.namespaces.is_empty() {
            return Err(ConfigError::Validation(
                "at least one namespace is required".to_owned(),
            ));
        }
        self.sharding
            .validate(self.mode)
            .map_err(ConfigError::Validation)?;
        let mut names = HashSet::new();
        for namespace in &self.namespaces {
            line::Namespace::new(&namespace.name).map_err(|error| {
                ConfigError::Validation(format!("invalid namespace {}: {error}", namespace.name))
            })?;
            if !names.insert(&namespace.name) {
                return Err(ConfigError::Validation(format!(
                    "duplicate namespace {}",
                    namespace.name
                )));
            }
        }
        if let Some(jwt) = &self.auth.jwt
            && (jwt.refresh_interval_seconds == 0 || jwt.request_timeout_seconds == 0)
        {
            return Err(ConfigError::Validation(
                "JWT refresh and request timeout must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn line_config(&self) -> line::Config {
        line::Config {
            storage: self.storage.clone(),
            segment_duration: Duration::from_secs(self.segment_duration_seconds),
            retention: self.retention_seconds.map(Duration::from_secs),
            page: line::PageConfig {
                target_size_bytes: self.page.target_size_bytes,
                max_rows: self.page.max_rows,
                max_age: Duration::from_secs(self.page.max_age_seconds),
                rows_per_block: self.page.rows_per_block,
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
        let default = Config::default();
        assert!(!default.auth.unauthenticated);
        default.validate().unwrap();
        let example: Config =
            serde_yaml::from_str(include_str!("../../../config/line.example.yaml")).unwrap();
        example.validate().unwrap();
    }

    #[test]
    fn rejects_duplicate_or_invalid_namespaces_and_zero_limits() {
        let mut config = Config {
            namespaces: vec![
                NamespaceConfig {
                    name: "duplicate".to_owned(),
                    auth: Access::default(),
                },
                NamespaceConfig {
                    name: "duplicate".to_owned(),
                    auth: Access::default(),
                },
            ],
            ..Config::default()
        };
        assert!(config.validate().is_err());
        config.namespaces = vec![NamespaceConfig {
            name: "valid".to_owned(),
            auth: Access::default(),
        }];
        config.request.max_request_bytes = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn path_prefix_must_be_canonical() {
        for invalid in ["line", "/", "/line/"] {
            let config = Config {
                path_prefix: invalid.to_owned(),
                ..Config::default()
            };
            assert!(config.validate().is_err());
        }
        let config = Config {
            path_prefix: "/line".to_owned(),
            ..Config::default()
        };
        config.validate().unwrap();
    }
}
