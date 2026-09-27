use std::{collections::HashSet, fs, net::SocketAddr, path::Path, time::Duration};

use common::storage::config::StorageConfig;
pub use meter_server::config::{Access, AuthConfig, Credential, JwksSource, JwtConfig, Secret};
use serde::{Deserialize, Serialize};
use sharding::DEFAULT_VIRTUAL_SHARDS;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerMode {
    Writer,
    Reader,
    #[default]
    Standalone,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    Applied,
    #[default]
    Written,
    Durable,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WriteConfig {
    pub durability: Durability,
    pub remote_concurrency: usize,
    pub remote_retries: usize,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            durability: Durability::Written,
            remote_concurrency: 16,
            remote_retries: 2,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum ShardingBackend {
    #[default]
    Standalone,
    Static {
        owner_id: String,
        owners: Vec<StaticOwner>,
    },
    Kubernetes(KubernetesShardingConfig),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StaticOwner {
    pub id: String,
    pub ordinal: u32,
    pub endpoint: String,
    pub start_shard: u32,
    pub end_shard: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct KubernetesShardingConfig {
    pub namespace: String,
    pub stateful_set: String,
    pub headless_service: String,
    pub owner_port: u16,
    pub assignment_config_map: String,
    pub coordinator_lease: String,
    pub shard_lease_prefix: String,
    pub lease_duration_seconds: u64,
    pub renew_interval_seconds: u64,
    pub watch_poll_interval_seconds: u64,
}

impl Default for KubernetesShardingConfig {
    fn default() -> Self {
        Self {
            namespace: "default".into(),
            stateful_set: "line".into(),
            headless_service: "line-headless".into(),
            owner_port: 9091,
            assignment_config_map: "line-shard-assignments".into(),
            coordinator_lease: "line-shard-coordinator".into(),
            shard_lease_prefix: "line-shard".into(),
            lease_duration_seconds: 15,
            renew_interval_seconds: 5,
            watch_poll_interval_seconds: 2,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ShardingConfig {
    pub virtual_shards: u32,
    #[serde(flatten)]
    pub kind: ShardingBackend,
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            virtual_shards: DEFAULT_VIRTUAL_SHARDS,
            kind: ShardingBackend::Standalone,
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
            query_concurrency: 8,
            max_in_flight_query_bytes: 64 * 1024 * 1024,
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
    pub storage: StorageConfig,
    pub segment_duration_seconds: u64,
    pub retention_seconds: Option<u64>,
    pub page: PageConfig,
    /// Interval between L0 flushes. A successful flush makes accepted writes
    /// visible to readers.
    pub visibility_interval_seconds: u64,
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
            storage: core.storage,
            segment_duration_seconds: core.segment_duration.as_secs(),
            retention_seconds: core.retention.map(|value| value.as_secs()),
            page: PageConfig::default(),
            visibility_interval_seconds: 1,
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
            || self.write.remote_concurrency == 0
            || self.sharding.virtual_shards == 0
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
        if self.mode == ServerMode::Standalone
            && !matches!(self.sharding.kind, ShardingBackend::Standalone)
        {
            return Err(ConfigError::Validation(
                "standalone mode requires the standalone sharding backend".into(),
            ));
        }
        if let ShardingBackend::Static { owner_id, owners } = &self.sharding.kind {
            if !owners.iter().any(|owner| &owner.id == owner_id) {
                return Err(ConfigError::Validation(
                    "static owner_id has no owner entry".into(),
                ));
            }
            let mut ranges = owners
                .iter()
                .map(|owner| (owner.start_shard, owner.end_shard))
                .collect::<Vec<_>>();
            ranges.sort_unstable();
            let mut expected = 0;
            for (start, end) in ranges {
                if start != expected || end <= start || end > self.sharding.virtual_shards {
                    return Err(ConfigError::Validation(
                        "static owners must exactly cover all virtual shards".into(),
                    ));
                }
                expected = end;
            }
            if expected != self.sharding.virtual_shards {
                return Err(ConfigError::Validation(
                    "static owners must exactly cover all virtual shards".into(),
                ));
            }
        }
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
}
