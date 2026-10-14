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

    /// Concurrency and memory limits shared by every query the process runs.
    pub query_limits: crate::QueryLimitsConfig,

    /// Background rollup of closed hour buckets into wider buckets.
    pub rollup: RollupConfig,
}

/// Background rollup of closed hour buckets into wider buckets.
///
/// Once a day is `delay` past its end, its hours are re-written into the
/// fewest aligned buckets of 24, 12, 8, 6, 4, 3 or 2 hours whose stored size
/// stays within `max_bucket_bytes`, so a query over the day opens a few
/// segments instead of 24. Hours too large to share a bucket stay as they
/// are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RollupConfig {
    pub enabled: bool,
    /// Largest stored size, summed over the covered hours, of a rollup
    /// bucket.
    pub max_bucket_bytes: u64,
    /// How long after a day ends before its hours are rolled up, leaving
    /// room for late samples.
    pub delay: Duration,
    /// How often to look for days to roll up.
    pub interval: Duration,
    /// Copied bytes a rollup may hand the writer before waiting for them to
    /// reach storage. Larger values copy faster at the cost of memory.
    pub copy_buffer_bytes: u64,
}

impl Default for RollupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_bucket_bytes: 4 << 30,
            delay: Duration::from_secs(60 * 60),
            interval: Duration::from_secs(5 * 60),
            copy_buffer_bytes: common::storage::rollup::DEFAULT_COPY_BUFFER_BYTES,
        }
    }
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
    /// Byte budget for forward-index entries and fully resolved selectors.
    pub forward_index_capacity_bytes: u64,
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
            forward_index_capacity_bytes: crate::minitsdb::DEFAULT_FORWARD_CACHE_CAPACITY_BYTES,
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
                disk: Default::default(),
            },
            flush_interval: Duration::from_secs(60),
            retention: Some(DEFAULT_RETENTION),
            write_buffer: WriteCoordinatorConfig::default(),
            query_cache: QueryCacheConfig::default(),
            query_limits: crate::QueryLimitsConfig::default(),
            rollup: RollupConfig::default(),
        }
    }
}
