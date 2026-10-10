use std::{collections::HashSet, fs, net::SocketAddr, path::Path};

use common::{CacheWarmerConfig, storage::config::SlateDbStorageConfig};
use serde::{Deserialize, Serialize};

pub use server_common::auth::{Access, AuthConfig, Credential, JwksSource, JwtConfig, Secret};
pub use server_common::config::{Durability, WriteConfig};
pub use server_common::usage::UsageReportingConfig;
pub use sharding::server::{ServerMode, StaticOwner};

#[derive(Debug, Clone, Default)]
pub struct MetricsProduct;

impl sharding::server::Product for MetricsProduct {
    const NAME: &'static str = "metrics";
    const OWNER_PORT: u16 = 9090;
}

pub type ShardingConfig = sharding::server::ShardingConfig<MetricsProduct>;
pub type ShardingBackend = sharding::server::ShardingBackend<MetricsProduct>;
pub type KubernetesShardingConfig = sharding::server::KubernetesShardingConfig<MetricsProduct>;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceConfig {
    pub name: String,
    #[serde(default)]
    pub auth: Access,
    /// Console gRPC endpoint that receives this namespace's ingest usage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_reporting_endpoint: Option<String>,
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
            http: "0.0.0.0:8080".parse().unwrap(),
            grpc: "0.0.0.0:9090".parse().unwrap(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RequestConfig {
    /// Cap on a request body as received, before content decoding.
    pub max_request_bytes: usize,
    /// Cap on a write body after gzip or snappy decoding.
    pub max_decoded_request_bytes: usize,
}

impl Default for RequestConfig {
    fn default() -> Self {
        Self {
            max_request_bytes: server_common::http::DEFAULT_MAX_REQUEST_BYTES,
            max_decoded_request_bytes: server_common::http::DEFAULT_MAX_DECODED_REQUEST_BYTES,
        }
    }
}

/// Reuse of range-query results whose dependent buckets are unchanged.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResultCacheConfig {
    pub enabled: bool,
    pub capacity_bytes: u64,
}

impl Default for ResultCacheConfig {
    fn default() -> Self {
        let defaults = plural_metrics::QueryCacheConfig::default();
        Self {
            enabled: defaults.result_cache_enabled,
            capacity_bytes: defaults.result_capacity_bytes,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub mode: ServerMode,
    pub listeners: ListenerConfig,
    /// Optional prefix for public read and write HTTP APIs, such as `/metrics`.
    pub path_prefix: String,
    pub storage: SlateDbStorageConfig,
    /// Samples older than this may be dropped by compaction; 60 days by
    /// default, `null` keeps data forever.
    pub retention_seconds: Option<u64>,
    pub reader_cache_capacity: u64,
    /// Byte budget of each namespace's selector-matcher cache, per shard.
    pub matcher_cache_capacity_bytes: u64,
    /// Byte budget for forward-index entries and resolved selectors, per shard.
    pub forward_index_cache_capacity_bytes: u64,
    pub result_cache: ResultCacheConfig,
    pub cache_warmer: CacheWarmerConfig,
    pub write: WriteConfig,
    pub request: RequestConfig,
    pub sharding: ShardingConfig,
    pub auth: AuthConfig,
    pub usage_reporting: UsageReportingConfig,
    pub namespaces: Vec<NamespaceConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: ServerMode::Standalone,
            listeners: ListenerConfig::default(),
            path_prefix: String::new(),
            storage: SlateDbStorageConfig::default(),
            retention_seconds: Some(plural_metrics::DEFAULT_RETENTION.as_secs()),
            reader_cache_capacity: 256 * 1024 * 1024,
            matcher_cache_capacity_bytes: plural_metrics::QueryCacheConfig::default()
                .matcher_capacity_bytes,
            forward_index_cache_capacity_bytes: plural_metrics::QueryCacheConfig::default()
                .forward_index_capacity_bytes,
            result_cache: ResultCacheConfig::default(),
            cache_warmer: CacheWarmerConfig::default(),
            write: WriteConfig::default(),
            request: RequestConfig::default(),
            sharding: ShardingConfig::default(),
            auth: AuthConfig::default(),
            usage_reporting: UsageReportingConfig::default(),
            namespaces: vec![NamespaceConfig {
                name: "default".to_owned(),
                auth: Access::default(),
                usage_reporting_endpoint: None,
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
        if self.write.has_zero_limit() {
            return Err(ConfigError::Validation(
                "write buffer and concurrency limits must be greater than zero".to_owned(),
            ));
        }
        server_common::http::validate_request_limits(
            self.request.max_request_bytes,
            self.request.max_decoded_request_bytes,
        )
        .map_err(ConfigError::Validation)?;
        if !self.path_prefix.is_empty()
            && (!self.path_prefix.starts_with('/')
                || self.path_prefix.len() == 1
                || self.path_prefix.ends_with('/'))
        {
            return Err(ConfigError::Validation(
                "path_prefix must be empty or start with '/' and must not end with '/'".to_owned(),
            ));
        }
        self.cache_warmer
            .validate()
            .map_err(ConfigError::Validation)?;
        if self.namespaces.is_empty() {
            return Err(ConfigError::Validation(
                "at least one namespace is required".to_owned(),
            ));
        }
        if let Some(jwt) = &self.auth.jwt {
            if jwt.refresh_interval_seconds == 0 {
                return Err(ConfigError::Validation(
                    "auth.jwt.refresh_interval_seconds must be greater than zero".to_owned(),
                ));
            }
            if jwt.request_timeout_seconds == 0 {
                return Err(ConfigError::Validation(
                    "auth.jwt.request_timeout_seconds must be greater than zero".to_owned(),
                ));
            }
            match &jwt.jwks {
                JwksSource::File { path } if path.is_empty() => {
                    return Err(ConfigError::Validation(
                        "auth.jwt.jwks.path cannot be empty".to_owned(),
                    ));
                }
                JwksSource::Url { url }
                    if !(url.starts_with("https://") || url.starts_with("http://")) =>
                {
                    return Err(ConfigError::Validation(
                        "auth.jwt.jwks.url must use http or https".to_owned(),
                    ));
                }
                JwksSource::Url { url } if url.parse::<reqwest::Url>().is_err() => {
                    return Err(ConfigError::Validation(
                        "auth.jwt.jwks.url must be a valid URL".to_owned(),
                    ));
                }
                _ => {}
            }
        }
        let mut names = HashSet::new();
        for namespace in &self.namespaces {
            plural_metrics::Namespace::new(&namespace.name).map_err(|error| {
                ConfigError::Validation(format!("invalid namespace {}: {error}", namespace.name))
            })?;
            if !names.insert(&namespace.name) {
                return Err(ConfigError::Validation(format!(
                    "duplicate namespace {}",
                    namespace.name
                )));
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
    fn defaults_and_validation() {
        let config: Config = serde_yaml::from_str("{}").unwrap();
        assert_eq!(config.sharding.shards, 1);
        assert_eq!(config.sharding.io_concurrency_limit, 128);
        assert!(!config.auth.unauthenticated);
        config.validate().unwrap();
        let invalid: Config =
            serde_yaml::from_str("sharding:\n  shards: 0\n  backend: standalone\n").unwrap();
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn path_prefix_must_be_canonical() {
        for prefix in ["metrics", "/", "/metrics/"] {
            let config = Config {
                path_prefix: prefix.to_owned(),
                ..Config::default()
            };
            assert!(config.validate().is_err(), "{prefix} should be rejected");
        }
        let config = Config {
            path_prefix: "/metrics".to_owned(),
            ..Config::default()
        };
        config.validate().unwrap();
    }

    #[test]
    fn checked_in_example_is_valid() {
        let config: Config =
            serde_yaml::from_str(include_str!("../../../config/metrics.example.yaml")).unwrap();
        config.validate().unwrap();
    }

    #[test]
    fn opaque_bearer_credentials_are_rejected() {
        let yaml = r#"
auth:
  global:
    read:
      - type: bearer
        token: { source: literal, value: stale-token }
"#;
        assert!(serde_yaml::from_str::<Config>(yaml).is_err());
    }

    #[test]
    fn unauthenticated_must_be_an_explicit_boolean() {
        let omitted: Config = serde_yaml::from_str("auth: {}").unwrap();
        assert!(!omitted.auth.unauthenticated);
        let disabled: Config = serde_yaml::from_str("auth:\n  unauthenticated: false\n").unwrap();
        assert!(!disabled.auth.unauthenticated);
        let enabled: Config = serde_yaml::from_str("auth:\n  unauthenticated: true\n").unwrap();
        assert!(enabled.auth.unauthenticated);
    }
}
