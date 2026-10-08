//! Configuration options for OpenData TimeSeries operations.
//!
//! This module defines the configuration and options structs that control
//! the behavior of the time series database, including storage setup and
//! write operation parameters.

use std::time::Duration;

use common::{
    coordinator::WriteCoordinatorConfig,
    storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig},
};

/// Configuration for opening a [`TimeSeriesDb`](crate::timeseries::TimeSeriesDb) database.
///
/// This struct holds all the settings needed to initialize a time series
/// instance, including storage backend configuration and operational parameters.
///
/// # Example
///
/// ```no_run
/// use plural_metrics::{Config, Namespace};
/// use common::storage::config::SlateDbStorageConfig;
/// use std::time::Duration;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = Config {
///     storage: SlateDbStorageConfig::default(),
///     flush_interval: Duration::from_secs(30),
///     retention: Some(Duration::from_secs(86400 * 7)), // 7 days
///     ..Default::default()
/// };
/// let ts = plural_metrics::TimeSeriesDb::open(config).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Config {
    /// Storage backend configuration.
    ///
    /// Determines where and how time series data is persisted. See
    /// [`SlateDbStorageConfig`] for object-store and tuning options.
    pub storage: SlateDbStorageConfig,

    /// How often to flush data to durable storage.
    ///
    /// Data is buffered in memory and periodically flushed to the storage backend.
    /// Lower values provide better durability at the cost of write performance.
    pub flush_interval: Duration,

    /// Maximum age of data to retain.
    ///
    /// Data older than this duration may be automatically deleted during
    /// compaction. Set to `None` to retain data indefinitely.
    pub retention: Option<Duration>,

    /// Bounds and flush triggers for each bucket/routing-slot write delta.
    pub write_buffer: WriteCoordinatorConfig,

    /// Cross-query caches on the read path.
    pub query_cache: QueryCacheConfig,
}

/// Default [`Config::retention`]: 60 days.
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(60 * 24 * 60 * 60);

/// Capacities of the caches shared across queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryCacheConfig {
    /// Byte budget of each namespace's selector-matcher cache, which maps a
    /// bucket and a normalized matcher set to the matching series IDs.
    pub matcher_capacity_bytes: u64,
    /// Byte budget of each namespace's decoded-series cache, which keeps a
    /// bucket's decoded samples per series until the bucket is next flushed.
    pub series_capacity_bytes: u64,
    /// Whether [`ShardedMetrics`](crate::ShardedMetrics) reuses earlier
    /// range-query results whose dependent buckets are unchanged.
    pub result_cache_enabled: bool,
    /// Byte budget of the range-query result cache.
    pub result_capacity_bytes: u64,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self {
            matcher_capacity_bytes: 64 * 1024 * 1024,
            series_capacity_bytes: 128 * 1024 * 1024,
            result_cache_enabled: true,
            result_capacity_bytes: 128 * 1024 * 1024,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // Local `.data` directory by default (matches the historical
            // `StorageConfig::default()`; SlateDbStorageConfig::default() would
            // use an in-memory object store instead).
            storage: SlateDbStorageConfig {
                path: "data".to_string(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: ".data".to_string(),
                }),
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            },
            flush_interval: Duration::from_secs(60),
            retention: Some(DEFAULT_RETENTION),
            write_buffer: WriteCoordinatorConfig::default(),
            query_cache: QueryCacheConfig::default(),
        }
    }
}
