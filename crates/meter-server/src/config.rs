use std::{collections::HashSet, env, fmt, fs, net::SocketAddr, path::Path};

use common::storage::config::SlateDbStorageConfig;
use serde::{Deserialize, Serialize};
use sharding::{DEFAULT_IO_CONCURRENCY_MULTIPLIER, DEFAULT_VIRTUAL_SHARDS};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerMode {
    Writer,
    Reader,
    Standalone,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum Secret {
    Literal { value: String },
    Env { name: String },
    File { path: String },
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Secret([REDACTED])")
    }
}

impl Secret {
    pub fn expose(&self) -> Result<String, ConfigError> {
        let value = match self {
            Self::Literal { value } => value.clone(),
            Self::Env { name } => env::var(name).map_err(|_| {
                ConfigError::Secret(format!("environment variable {name} is unset"))
            })?,
            Self::File { path } => fs::read_to_string(path)
                .map_err(|error| ConfigError::Secret(format!("cannot read {path}: {error}")))?,
        };
        let value = value.trim_end_matches(['\r', '\n']).to_owned();
        if value.is_empty() {
            return Err(ConfigError::Secret("secret cannot be empty".to_owned()));
        }
        Ok(value)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Credential {
    Basic { username: String, password: Secret },
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Access {
    pub read: Vec<Credential>,
    pub write: Vec<Credential>,
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
            http: "0.0.0.0:8080".parse().unwrap(),
            grpc: "0.0.0.0:9090".parse().unwrap(),
        }
    }
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
    pub flush_interval_seconds: u64,
    pub remote_concurrency: usize,
    pub remote_retries: usize,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            durability: Durability::Written,
            flush_interval_seconds: 60,
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
}

impl Default for KubernetesShardingConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_owned(),
            stateful_set: "meter".to_owned(),
            headless_service: "meter-headless".to_owned(),
            owner_port: 9090,
            assignment_config_map: "meter-shard-assignments".to_owned(),
            coordinator_lease: "meter-shard-coordinator".to_owned(),
            shard_lease_prefix: "meter-shard".to_owned(),
            lease_duration_seconds: 15,
            renew_interval_seconds: 5,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ShardingConfig {
    pub virtual_shards: u32,
    pub io_concurrency_multiplier: u32,
    #[serde(flatten)]
    pub kind: ShardingBackend,
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            virtual_shards: DEFAULT_VIRTUAL_SHARDS,
            io_concurrency_multiplier: DEFAULT_IO_CONCURRENCY_MULTIPLIER,
            kind: ShardingBackend::Standalone,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    pub unauthenticated: bool,
    pub jwt: Option<JwtConfig>,
    pub global: Access,
    pub internal: Option<Secret>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct JwtConfig {
    pub jwks: JwksSource,
    pub issuer: Option<String>,
    pub audience: Option<String>,
    pub refresh_interval_seconds: u64,
    pub request_timeout_seconds: u64,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            jwks: JwksSource::File {
                path: String::new(),
            },
            issuer: None,
            audience: None,
            refresh_interval_seconds: 300,
            request_timeout_seconds: 5,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum JwksSource {
    File { path: String },
    Url { url: String },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub mode: ServerMode,
    pub listeners: ListenerConfig,
    /// Optional prefix for public read and write HTTP APIs, such as `/meter`.
    pub path_prefix: String,
    pub storage: SlateDbStorageConfig,
    pub reader_cache_capacity: u64,
    pub write: WriteConfig,
    pub sharding: ShardingConfig,
    pub auth: AuthConfig,
    pub namespaces: Vec<NamespaceConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: ServerMode::Standalone,
            listeners: ListenerConfig::default(),
            path_prefix: String::new(),
            storage: SlateDbStorageConfig::default(),
            reader_cache_capacity: 256 * 1024 * 1024,
            write: WriteConfig::default(),
            sharding: ShardingConfig::default(),
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
        if self.sharding.virtual_shards == 0 {
            return Err(ConfigError::Validation(
                "sharding.virtual_shards must be greater than zero".to_owned(),
            ));
        }
        if self.sharding.io_concurrency_multiplier == 0 {
            return Err(ConfigError::Validation(
                "sharding.io_concurrency_multiplier must be greater than zero".to_owned(),
            ));
        }
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
            meter::Namespace::new(&namespace.name).map_err(|error| {
                ConfigError::Validation(format!("invalid namespace {}: {error}", namespace.name))
            })?;
            if !names.insert(&namespace.name) {
                return Err(ConfigError::Validation(format!(
                    "duplicate namespace {}",
                    namespace.name
                )));
            }
        }
        if self.mode == ServerMode::Standalone
            && !matches!(self.sharding.kind, ShardingBackend::Standalone)
        {
            return Err(ConfigError::Validation(
                "standalone mode requires the standalone sharding backend".to_owned(),
            ));
        }
        if let ShardingBackend::Static { owners, owner_id } = &self.sharding.kind {
            if !owners.iter().any(|owner| &owner.id == owner_id) {
                return Err(ConfigError::Validation(format!(
                    "static owner_id {owner_id} has no owner entry"
                )));
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
                        "static owners must exactly cover all virtual shards".to_owned(),
                    ));
                }
                expected = end;
            }
            if expected != self.sharding.virtual_shards {
                return Err(ConfigError::Validation(
                    "static owners must exactly cover all virtual shards".to_owned(),
                ));
            }
        }
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
    #[error("cannot resolve secret: {0}")]
    Secret(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_validation() {
        let config: Config = serde_yaml::from_str("{}").unwrap();
        assert_eq!(config.sharding.virtual_shards, 8);
        assert_eq!(config.sharding.io_concurrency_multiplier, 4);
        assert!(!config.auth.unauthenticated);
        config.validate().unwrap();
        let invalid: Config =
            serde_yaml::from_str("sharding:\n  virtual_shards: 0\n  backend: standalone\n")
                .unwrap();
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn path_prefix_must_be_canonical() {
        for prefix in ["meter", "/", "/meter/"] {
            let config = Config {
                path_prefix: prefix.to_owned(),
                ..Config::default()
            };
            assert!(config.validate().is_err(), "{prefix} should be rejected");
        }
        let config = Config {
            path_prefix: "/meter".to_owned(),
            ..Config::default()
        };
        config.validate().unwrap();
    }

    #[test]
    fn secret_sources_and_redaction() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        fs::write(&path, "from-file\n").unwrap();
        let secret = Secret::File {
            path: path.display().to_string(),
        };
        assert_eq!(secret.expose().unwrap(), "from-file");
        assert!(!format!("{secret:?}").contains("from-file"));
        let literal = Secret::Literal {
            value: "literal".to_owned(),
        };
        assert_eq!(literal.expose().unwrap(), "literal");
        assert!(!format!("{literal:?}").contains("literal"));
    }

    #[test]
    fn checked_in_example_is_valid() {
        let config: Config =
            serde_yaml::from_str(include_str!("../../../config/meter.example.yaml")).unwrap();
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
