//! Configuration for the vector HTTP server.

use figment::Figment;
use figment::providers::{Format, Serialized, Yaml};

use crate::Config;
use crate::cluster::ClusterConfig;
use crate::model::ReaderConfig;

/// Load a vector database configuration from a YAML file.
///
/// Fields present in the file override the defaults from [`Config::default()`].
/// Fields omitted from the file retain their default values.
pub fn load_vector_config(path: &str) -> Config {
    Figment::from(Serialized::defaults(Config::default()))
        .merge(Yaml::file(path))
        .extract()
        .unwrap_or_else(|e| panic!("Failed to load config file '{}': {}", path, e))
}

/// Load a cluster configuration from a YAML file, over
/// [`ClusterConfig::default()`].
pub fn load_cluster_config(path: &str) -> ClusterConfig {
    Figment::from(Serialized::defaults(ClusterConfig::default()))
        .merge(Yaml::file(path))
        .extract()
        .unwrap_or_else(|e| panic!("Failed to load config file '{}': {}", path, e))
}

/// Load a reader configuration from a YAML file.
pub fn load_reader_config(path: &str) -> ReaderConfig {
    Figment::from(Yaml::file(path))
        .extract()
        .unwrap_or_else(|e| panic!("Failed to load config file '{}': {}", path, e))
}

/// Configuration for the vector HTTP server.
#[derive(Debug, Clone)]
pub struct VectorServerConfig {
    /// HTTP server port.
    pub port: u16,
    /// Prepended to the cluster's `/api/v1` routes, e.g. `/vector`, so it can
    /// share a hostname with other services. Health and metrics routes stay
    /// at the root.
    pub path_prefix: String,
}

impl Default for VectorServerConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            path_prefix: String::new(),
        }
    }
}

impl VectorServerConfig {
    /// The server settings of a cluster configuration file: its top-level
    /// `path_prefix`.
    pub fn from_cluster_yaml(port: u16, raw: &str) -> Result<Self, String> {
        #[derive(serde::Deserialize)]
        struct Http {
            #[serde(default)]
            path_prefix: String,
        }
        let http: Http = serde_yaml::from_str(raw).map_err(|e| format!("invalid config: {e}"))?;
        let config = Self {
            port,
            path_prefix: http.path_prefix,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        let prefix = &self.path_prefix;
        if !prefix.is_empty()
            && (!prefix.starts_with('/') || prefix.len() == 1 || prefix.ends_with('/'))
        {
            return Err(
                "path_prefix must be empty or start with '/' and must not end with '/'".to_owned(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::StorageConfig;

    #[test]
    fn should_load_vector_config_from_yaml() {
        // given
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        std::fs::write(
            &config_path,
            r#"
storage:
  type: InMemory
dimensions: 384
distance_metric: L2
flush_interval: 60
split_threshold_vectors: 2000
merge_threshold_vectors: 500
split_search_neighbourhood: 16
max_pending_and_running_rebalance_tasks: 16
rebalance_backpressure_resume_threshold: 8
max_rebalance_tasks: 8
chunk_target: 4096
metadata_fields: []
"#,
        )
        .unwrap();

        // when
        let config = load_vector_config(config_path.to_str().unwrap());

        // then
        assert!(matches!(config.storage, StorageConfig::InMemory));
        assert_eq!(config.dimensions, 384);
    }

    #[test]
    fn should_load_vector_config_with_defaults() {
        // given
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        std::fs::write(
            &config_path,
            r#"
storage:
  type: InMemory
dimensions: 2
distance_metric: L2
metadata_fields:
  - name: label
    field_type: String
    indexed: true
"#,
        )
        .unwrap();

        // when
        let config = load_vector_config(config_path.to_str().unwrap());

        // then
        assert!(matches!(config.storage, StorageConfig::InMemory));
        assert_eq!(config.dimensions, 2);
        assert_eq!(config.flush_interval, std::time::Duration::from_secs(60));
        assert_eq!(config.split_threshold_vectors, 150);
        assert_eq!(config.merge_threshold_vectors, 50);
    }

    #[test]
    fn should_load_cluster_config_with_defaults() {
        // given
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("cluster.yaml");
        std::fs::write(
            &config_path,
            r#"
storage:
  type: InMemory
index:
  split_threshold_vectors: 500
"#,
        )
        .unwrap();

        // when
        let config = load_cluster_config(config_path.to_str().unwrap());

        // then
        assert!(matches!(config.storage, StorageConfig::InMemory));
        assert_eq!(config.index.split_threshold_vectors, 500);
        assert_eq!(config.index.merge_threshold_vectors, 50);
        assert_eq!(config.flush_interval, std::time::Duration::from_secs(1));
    }

    #[test]
    fn should_load_replica_cluster_config() {
        // given
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("reader.yaml");
        std::fs::write(
            &config_path,
            r#"
storage:
  type: InMemory
role: reader
reader_shard: 3
kubernetes:
  name: search
  namespace: vectors
"#,
        )
        .unwrap();

        // when
        let config = load_cluster_config(config_path.to_str().unwrap());

        // then
        assert_eq!(config.role, crate::cluster::Role::Reader);
        assert_eq!(config.reader_shard, Some(3));
        let kubernetes = config.kubernetes.unwrap();
        assert_eq!(kubernetes.port, 8080);
        assert_eq!(
            kubernetes.writer_address("search-writer-1"),
            "search-writer-1.search-writer-headless.vectors.svc:8080"
        );
        assert_eq!(
            kubernetes.reader_address(3),
            "search-reader-3-headless.vectors.svc:8080"
        );
    }

    #[test]
    fn should_load_embedding_config() {
        // given
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("cluster.yaml");
        std::fs::write(
            &config_path,
            r#"
storage:
  type: InMemory
embedding:
  provider: openai
  model: text-embedding-3-small
  api_key:
    source: file
    path: /etc/vector/embedding/api-key
  timeout: 5
  cache:
    capacity: 1048576
"#,
        )
        .unwrap();

        // when
        let config = load_cluster_config(config_path.to_str().unwrap());

        // then
        let embedding = config.embedding.unwrap();
        assert_eq!(
            embedding.provider,
            crate::cluster::EmbeddingProvider::OpenAi
        );
        assert_eq!(embedding.timeout, std::time::Duration::from_secs(5));
        assert_eq!(embedding.cache.capacity, 1 << 20);
        assert_eq!(embedding.batch_size, 96);
    }

    #[test]
    fn should_read_a_canonical_path_prefix_from_the_cluster_config() {
        // given
        let raw = "storage:\n  type: InMemory\npath_prefix: /vector\n";

        // when
        let config = VectorServerConfig::from_cluster_yaml(9000, raw).unwrap();

        // then
        assert_eq!(config.port, 9000);
        assert_eq!(config.path_prefix, "/vector");
        let unset = VectorServerConfig::from_cluster_yaml(8080, "storage:\n  type: InMemory\n");
        assert_eq!(unset.unwrap().path_prefix, "");
        for invalid in ["vector", "/", "/vector/"] {
            let raw = format!("path_prefix: {invalid}\n");
            assert!(
                VectorServerConfig::from_cluster_yaml(8080, &raw).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn should_load_reader_config_from_yaml() {
        // given
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("reader.yaml");
        std::fs::write(
            &config_path,
            r#"
storage:
  type: InMemory
dimensions: 384
distance_metric: L2
metadata_fields: []
"#,
        )
        .unwrap();

        // when
        let config = load_reader_config(config_path.to_str().unwrap());

        // then
        assert!(matches!(config.storage, StorageConfig::InMemory));
        assert_eq!(config.dimensions, 384);
    }
}
