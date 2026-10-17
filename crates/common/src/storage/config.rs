//! Storage configuration types.
//!
//! This module provides configuration structures for different storage backends,
//! allowing services to configure storage type (InMemory or SlateDB) via config files
//! or environment variables.

use serde::{Deserialize, Serialize};

/// Default cadence for durable writer flushes and continuous reader warming.
pub const DEFAULT_FLUSH_INTERVAL_SECONDS: u64 = 10;

/// Startup policy for preloading recent SlateDB blocks into configured caches.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct CacheWarmerConfig {
    /// Whether startup warming is enabled.
    pub enabled: bool,
    /// Recent wall-clock window whose metadata is warmed; 0 warms all live
    /// data, which retention bounds, and the metadata budget bounds what
    /// is held. Payloads use it too unless `payload_warm_range_seconds`
    /// is set; see [`Self::window`].
    pub warm_range_seconds: u64,
    /// Recent wall-clock window whose payloads are warmed when
    /// `include_payloads` is set; 0 derives it from `warm_range_seconds`.
    pub payload_warm_range_seconds: u64,
    /// Maximum time startup warming may delay readiness, when it does.
    pub timeout_seconds: u64,
    /// Maximum number of SSTs warmed concurrently.
    pub concurrency: usize,
    /// Whether to warm payload/sample blocks in addition to indexes and metadata.
    pub include_payloads: bool,
    /// Whether readiness waits (up to `timeout_seconds`) for the startup
    /// pass. When false the server is ready at once and warms in the
    /// background, newest data first.
    pub block_readiness: bool,
    /// Share of the metadata cache's memory the warmer may fill, so queries
    /// keep the rest. Metrics and traces stop warming metadata there and
    /// count what did not fit; logs ignores it.
    pub metadata_budget_percent: u8,
    /// Periodic warming of SSTs that appear after startup.
    pub continuous: ContinuousCacheWarmerConfig,
}

/// [`CacheWarmerConfig::metadata_budget_percent`] unless configured.
pub const DEFAULT_METADATA_BUDGET_PERCENT: u8 = 75;

impl Default for CacheWarmerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            warm_range_seconds: 0,
            payload_warm_range_seconds: 0,
            timeout_seconds: 30,
            concurrency: 2,
            include_payloads: false,
            block_readiness: true,
            metadata_budget_percent: DEFAULT_METADATA_BUDGET_PERCENT,
            continuous: ContinuousCacheWarmerConfig::default(),
        }
    }
}

impl CacheWarmerConfig {
    /// Rejects zero ranges, timeouts, intervals and concurrency for enabled warmers.
    pub fn validate(&self) -> Result<(), String> {
        if self.metadata_budget_percent > 100 {
            return Err("cache_warmer.metadata_budget_percent must be at most 100".to_owned());
        }
        if self.enabled && self.timeout_seconds == 0 {
            return Err(
                "cache_warmer.timeout_seconds must be greater than zero when enabled".to_owned(),
            );
        }
        if (self.enabled || self.continuous.enabled) && self.concurrency == 0 {
            return Err(
                "cache_warmer.concurrency must be greater than zero when enabled".to_owned(),
            );
        }
        if self.continuous.enabled && self.continuous.interval_seconds == 0 {
            return Err(
                "cache_warmer.continuous.interval_seconds must be greater than zero when enabled"
                    .to_owned(),
            );
        }
        Ok(())
    }

    /// What the startup pass warms.
    pub fn window(&self) -> WarmWindow {
        WarmWindow::new(
            self.warm_range_seconds,
            self.payload_warm_range_seconds,
            self.include_payloads,
        )
    }
}

/// Payload range when neither range is configured. Payloads churn the data
/// cache, so unlike metadata they stay limited to the most recent data.
pub const DEFAULT_PAYLOAD_WARM_RANGE_SECONDS: u64 = 7_200;

/// What one warming pass plans, as ages back from now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WarmWindow {
    /// Data whose metadata is warmed; `None` is all live data.
    pub metadata: Option<std::time::Duration>,
    /// Data whose payloads are warmed too; `None` warms no payloads.
    pub payloads: Option<std::time::Duration>,
}

impl WarmWindow {
    /// The window of a warmer configured with these keys, where 0 means
    /// unset: a set `warm_range_seconds` bounds payloads too, as it did
    /// before payloads had their own range.
    pub fn new(warm_range_seconds: u64, payload_warm_range_seconds: u64, payloads: bool) -> Self {
        let seconds = |seconds: u64| (seconds > 0).then(|| std::time::Duration::from_secs(seconds));
        let payload_seconds = [payload_warm_range_seconds, warm_range_seconds]
            .into_iter()
            .find(|seconds| *seconds > 0)
            .unwrap_or(DEFAULT_PAYLOAD_WARM_RANGE_SECONDS);
        Self {
            metadata: seconds(warm_range_seconds),
            payloads: payloads.then(|| std::time::Duration::from_secs(payload_seconds)),
        }
    }

    /// This window for a reader without a metadata cache, where warming
    /// metadata holds nothing: only what payload warming covers.
    pub fn payloads_only(self) -> Self {
        Self {
            metadata: self.payloads,
            ..self
        }
    }

    /// The oldest timestamp, in the unit of `now`, whose metadata is
    /// warmed; `units_per_second` converts the window.
    pub fn metadata_since(&self, now: i64, units_per_second: i64) -> i64 {
        since(now, self.metadata, units_per_second)
    }

    /// [`Self::metadata_since`] for payloads, or `None` without payloads.
    pub fn payloads_since(&self, now: i64, units_per_second: i64) -> Option<i64> {
        self.payloads
            .map(|range| since(now, Some(range), units_per_second))
    }
}

fn since(now: i64, range: Option<std::time::Duration>, units_per_second: i64) -> i64 {
    let Some(range) = range else {
        return i64::MIN;
    };
    let millis = i128::try_from(range.as_millis()).unwrap_or(i128::MAX);
    let units = i128::from(units_per_second).saturating_mul(millis) / 1_000;
    i64::try_from(i128::from(now).saturating_sub(units)).unwrap_or(i64::MIN)
}

/// Readers poll their in-memory manifest every `interval_seconds` and warm
/// only SSTs they have not seen before, so each pass costs roughly the bytes
/// flushed or compacted since the previous one.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ContinuousCacheWarmerConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    /// As [`CacheWarmerConfig::warm_range_seconds`], for new SSTs.
    pub warm_range_seconds: u64,
    /// As [`CacheWarmerConfig::payload_warm_range_seconds`], for new SSTs.
    pub payload_warm_range_seconds: u64,
    pub include_payloads: bool,
}

impl Default for ContinuousCacheWarmerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_seconds: DEFAULT_FLUSH_INTERVAL_SECONDS,
            warm_range_seconds: 0,
            payload_warm_range_seconds: 0,
            include_payloads: true,
        }
    }
}

impl ContinuousCacheWarmerConfig {
    /// What each continuous pass warms.
    pub fn window(&self) -> WarmWindow {
        WarmWindow::new(
            self.warm_range_seconds,
            self.payload_warm_range_seconds,
            self.include_payloads,
        )
    }
}

/// Top-level storage configuration.
///
/// Defaults to `SlateDb` with a local `/tmp/opendata-storage` directory.
// The `SlateDb` variant is large (it carries object-store and two cache
// configs), but `StorageConfig` is constructed rarely at startup and never
// moved in a hot path, so the size asymmetry with `InMemory` doesn't matter.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum StorageConfig {
    InMemory,
    SlateDb(SlateDbStorageConfig),
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: ".data".to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
            disk: Default::default(),
        })
    }
}

/// SlateDB-specific configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SlateDbStorageConfig {
    /// Path prefix for SlateDB data in the object store.
    pub path: String,

    /// Object store provider configuration.
    pub object_store: ObjectStoreConfig,

    /// Optional path to SlateDB settings file (TOML/YAML/JSON).
    ///
    /// If not provided, uses SlateDB's `Settings::load()` which checks for
    /// `SlateDb.toml`, `SlateDb.json`, `SlateDb.yaml` in the working directory
    /// and merges any `SLATEDB_` prefixed environment variables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings_path: Option<String>,

    /// Optional cache for SST *data* block lookups.
    ///
    /// When configured, reduces object store reads by caching hot data blocks
    /// in memory and/or on local disk. Maps to the `block_cache` side of
    /// SlateDB's [`slatedb::db_cache::SplitCache`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_cache: Option<BlockCacheConfig>,

    /// Optional cache for SST *metadata* — indexes, filters, and stats blocks.
    ///
    /// Maps to the `meta_cache` side of SlateDB's
    /// [`slatedb::db_cache::SplitCache`]. Metadata blocks are small but
    /// consulted on every read, so a dedicated cache keeps them resident
    /// instead of competing with large data blocks for capacity. An in-memory
    /// [`BlockCacheConfig::FoyerMemory`] is usually the right choice: size it
    /// to comfortably hold the live index + filter set and it effectively
    /// never evicts.
    ///
    /// When either `block_cache` or `meta_cache` is set, the two are combined
    /// into a `SplitCache` that routes data blocks to `block_cache` and
    /// index/filter/stats blocks to `meta_cache`. A side left unset simply
    /// caches nothing for that class of block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta_cache: Option<BlockCacheConfig>,

    /// The disk readers serve cached blocks from, which sizes the SST blocks
    /// writers and compactors produce: see [`DiskPerformance::sst_block_bytes`].
    /// Writers and readers of one database should describe the same disk.
    #[serde(default)]
    pub disk: DiskPerformance,
}

/// What a disk sustains, as its provider advertises it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct DiskPerformance {
    pub throughput_bytes_per_second: u64,
    pub iops: u64,
}

/// AWS EBS gp3's baseline: 125 MiB/s and 3,000 IOPS.
impl Default for DiskPerformance {
    fn default() -> Self {
        Self {
            throughput_bytes_per_second: 125 * 1024 * 1024,
            iops: 3_000,
        }
    }
}

impl DiskPerformance {
    pub const MIN_SST_BLOCK_BYTES: usize = 4 * 1024;
    pub const MAX_SST_BLOCK_BYTES: usize = 64 * 1024;

    /// The smallest power-of-two block at which a scan exhausts the disk's
    /// throughput before its IOPS: below `throughput / iops` reads are IOPS
    /// bound, so a scan runs slower than the disk can stream. Larger blocks
    /// scan no faster and cost selective reads more, so the smallest such
    /// block is chosen, within what SlateDB supports.
    pub fn sst_block_bytes(&self) -> usize {
        let knee = self.throughput_bytes_per_second / self.iops.max(1);
        usize::try_from(knee)
            .unwrap_or(usize::MAX)
            .checked_next_power_of_two()
            .unwrap_or(Self::MAX_SST_BLOCK_BYTES)
            .clamp(Self::MIN_SST_BLOCK_BYTES, Self::MAX_SST_BLOCK_BYTES)
    }
}

/// Cache configuration for SlateDB. Used for both the data-block cache
/// (`block_cache`) and the metadata cache (`meta_cache`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum BlockCacheConfig {
    /// Two-tier cache using foyer: in-memory + on-disk (ideally NVMe).
    FoyerHybrid(FoyerHybridCacheConfig),
    /// Single-tier in-memory cache using foyer. Lowest-latency option, with
    /// no durable tier — contents are rebuilt on restart (cheap when paired
    /// with cache warming). A good fit for the metadata cache.
    FoyerMemory(FoyerMemoryCacheConfig),
}

/// Configuration for foyer's in-memory (single-tier) cache.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FoyerMemoryCacheConfig {
    /// In-memory cache capacity in bytes.
    pub capacity: u64,
    /// Number of shards. When absent, foyer derives a default from the
    /// available CPU count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shards: Option<usize>,
}

/// Write policy for foyer's hybrid cache.
///
/// Controls when entries are written to the disk tier.
#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FoyerWritePolicy {
    /// Write to disk when an entry is inserted into the memory cache.
    /// Ensures every cached block is also persisted to the disk tier.
    #[default]
    WriteOnInsertion,
    /// Write to disk only when an entry is evicted from the memory cache.
    /// This is foyer's default policy.
    WriteOnEviction,
}

/// Configuration for foyer's hybrid (memory + disk) block cache.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FoyerHybridCacheConfig {
    /// In-memory cache capacity in bytes.
    pub memory_capacity: u64,
    /// On-disk cache capacity in bytes.
    pub disk_capacity: u64,
    /// Path for the on-disk cache directory.
    pub disk_path: String,
    /// Write policy for the hybrid cache. Default: `WriteOnInsertion`.
    #[serde(default)]
    pub write_policy: FoyerWritePolicy,
    /// Number of flush threads for the large engine. Default: 4.
    #[serde(default = "default_flushers")]
    pub flushers: usize,
    /// Buffer pool size in bytes for the large engine flush pipeline.
    /// Each flusher double-buffers, so actual allocation is ~2x this value.
    /// Default: `memory_capacity / 32` (computed at build time when absent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_pool_size: Option<u64>,
    /// Submit queue size threshold in bytes. Entries are dropped when
    /// the queue exceeds this limit. Queued entries are held in memory
    /// outside `memory_capacity`, so this adds to the process footprint.
    /// Default: 64 MiB.
    #[serde(default = "default_submit_queue_size_threshold")]
    pub submit_queue_size_threshold: u64,
}

fn default_flushers() -> usize {
    4
}

fn default_submit_queue_size_threshold() -> u64 {
    64 * 1024 * 1024
}

impl FoyerHybridCacheConfig {
    /// Returns the effective buffer pool size: explicit value if set,
    /// otherwise `memory_capacity / 32`.
    pub fn effective_buffer_pool_size(&self) -> u64 {
        self.buffer_pool_size.unwrap_or(self.memory_capacity / 32)
    }
}

impl Default for SlateDbStorageConfig {
    fn default() -> Self {
        Self {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::default(),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
            disk: DiskPerformance::default(),
        }
    }
}

impl StorageConfig {
    /// Returns a new config with the path modified by appending a suffix.
    ///
    /// For SlateDB storage, appends the suffix to the path (e.g., "data" -> "data/0").
    /// For InMemory storage, returns a clone unchanged.
    pub fn with_path_suffix(&self, suffix: &str) -> Self {
        match self {
            StorageConfig::InMemory => StorageConfig::InMemory,
            StorageConfig::SlateDb(config) => StorageConfig::SlateDb(SlateDbStorageConfig {
                path: format!("{}/{}", config.path, suffix),
                object_store: config.object_store.clone(),
                settings_path: config.settings_path.clone(),
                block_cache: config.block_cache.clone(),
                meta_cache: config.meta_cache.clone(),
                disk: config.disk,
            }),
        }
    }
}

/// Object store provider configuration for SlateDB.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ObjectStoreConfig {
    /// In-memory object store (useful for testing and development).
    #[default]
    InMemory,

    /// AWS S3 object store.
    Aws(AwsObjectStoreConfig),

    /// Azure Blob Storage object store.
    Azure(AzureObjectStoreConfig),

    /// Google Cloud Storage object store.
    Gcp(GcpObjectStoreConfig),

    /// Local filesystem object store.
    Local(LocalObjectStoreConfig),
}

/// AWS S3 object store configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AwsObjectStoreConfig {
    /// AWS region (e.g., "us-west-2").
    pub region: String,

    /// S3 bucket name.
    pub bucket: String,

    /// Optional endpoint for S3-compatible object stores.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Permit unencrypted HTTP connections to a custom endpoint.
    #[serde(default)]
    pub allow_http: bool,

    /// Use virtual-hosted-style requests instead of path-style requests.
    #[serde(default)]
    pub virtual_hosted_style: bool,
}

/// Azure Blob Storage object store configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AzureObjectStoreConfig {
    /// Azure storage account name.
    pub account: String,

    /// Azure Blob Storage container name.
    pub container: String,

    /// Optional custom Blob Storage endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Permit unencrypted HTTP connections to a custom endpoint.
    #[serde(default)]
    pub allow_http: bool,
}

/// Google Cloud Storage object store configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GcpObjectStoreConfig {
    /// Google Cloud Storage bucket name.
    pub bucket: String,

    /// Optional custom Google Cloud Storage API base URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
}

/// Local filesystem object store configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalObjectStoreConfig {
    /// Path to the local directory for storage.
    pub path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sst_blocks_are_the_smallest_that_exhaust_disk_throughput() {
        let disk = |throughput_bytes_per_second, iops| DiskPerformance {
            throughput_bytes_per_second,
            iops,
        };
        assert_eq!(DiskPerformance::default().sst_block_bytes(), 64 * 1024);
        assert_eq!(disk(2_000_000_000, 400_000).sst_block_bytes(), 8 * 1024);
        assert_eq!(
            disk(1_000 * 1024 * 1024, 16_000).sst_block_bytes(),
            64 * 1024
        );
        assert_eq!(disk(250 * 1024 * 1024, 16_000).sst_block_bytes(), 16 * 1024);
        assert_eq!(disk(1, 1_000_000).sst_block_bytes(), 4 * 1024);
        assert_eq!(disk(u64::MAX, 0).sst_block_bytes(), 64 * 1024);

        let parsed: SlateDbStorageConfig = serde_yaml::from_str(
            "path: data\nobject_store:\n  type: InMemory\ndisk:\n  iops: 16000\n",
        )
        .unwrap();
        assert_eq!(
            parsed.disk,
            disk(
                DiskPerformance::default().throughput_bytes_per_second,
                16_000
            )
        );
    }

    #[test]
    fn cache_warmer_continuously_warms_by_default() {
        assert_eq!(
            CacheWarmerConfig::default(),
            CacheWarmerConfig {
                enabled: false,
                warm_range_seconds: 0,
                payload_warm_range_seconds: 0,
                timeout_seconds: 30,
                concurrency: 2,
                include_payloads: false,
                block_readiness: true,
                metadata_budget_percent: 75,
                continuous: ContinuousCacheWarmerConfig {
                    enabled: true,
                    interval_seconds: DEFAULT_FLUSH_INTERVAL_SECONDS,
                    warm_range_seconds: 0,
                    payload_warm_range_seconds: 0,
                    include_payloads: true,
                },
            }
        );
    }

    #[test]
    fn metadata_covers_all_live_data_and_payloads_stay_recent_by_default() {
        let hours = |hours: u64| Some(std::time::Duration::from_secs(hours * 3_600));
        let config = CacheWarmerConfig::default();
        assert_eq!(
            config.window(),
            WarmWindow {
                metadata: None,
                payloads: None
            }
        );
        assert_eq!(
            config.continuous.window(),
            WarmWindow {
                metadata: None,
                payloads: hours(2)
            }
        );
        assert_eq!(
            WarmWindow::new(600, 0, true),
            WarmWindow {
                metadata: Some(std::time::Duration::from_secs(600)),
                payloads: Some(std::time::Duration::from_secs(600))
            }
        );
        assert_eq!(
            WarmWindow::new(86_400, 0, true).payloads,
            hours(24),
            "an explicit range keeps bounding payloads"
        );
        assert_eq!(
            WarmWindow::new(0, 1_800, true),
            WarmWindow {
                metadata: None,
                payloads: Some(std::time::Duration::from_secs(1_800))
            }
        );
        assert_eq!(
            WarmWindow::new(0, 0, true).payloads_only().metadata,
            hours(2)
        );
    }

    #[test]
    fn warm_windows_convert_to_timestamps() {
        let window = WarmWindow::new(0, 3_600, true);
        assert_eq!(window.metadata_since(10_000, 1), i64::MIN);
        assert_eq!(window.payloads_since(10_000, 1), Some(6_400));
        assert_eq!(
            window.payloads_since(10_000_000_000_000, 1_000_000_000),
            Some(10_000_000_000_000 - 3_600_000_000_000)
        );
        assert_eq!(WarmWindow::new(1, 0, false).payloads_since(5, 1), None);
        assert_eq!(
            WarmWindow::new(u64::MAX, 0, false).metadata_since(0, 1_000_000_000),
            i64::MIN
        );
    }

    #[test]
    fn old_cache_warmer_sections_parse_unchanged() {
        let old: CacheWarmerConfig = serde_yaml::from_str(
            "enabled: true\nwarm_range_seconds: 7200\ninclude_payloads: false\ncontinuous:\n  warm_range_seconds: 7200\n  include_payloads: true\n",
        )
        .unwrap();
        assert!(old.validate().is_ok());
        assert_eq!(
            old.window().metadata,
            Some(std::time::Duration::from_secs(7_200))
        );
        assert_eq!(
            old.continuous.window().payloads,
            Some(std::time::Duration::from_secs(7_200))
        );
        let zero: CacheWarmerConfig =
            serde_yaml::from_str("enabled: true\nwarm_range_seconds: 0\n").unwrap();
        assert!(zero.validate().is_ok());
    }

    #[test]
    fn continuous_cache_warmer_requires_positive_interval_and_concurrency() {
        let mut config = CacheWarmerConfig::default();
        config.continuous.enabled = true;
        assert!(config.validate().is_ok());

        config.continuous.interval_seconds = 0;
        assert!(config.validate().unwrap_err().contains("interval_seconds"));

        config.continuous.interval_seconds = DEFAULT_FLUSH_INTERVAL_SECONDS;
        config.concurrency = 0;
        assert!(config.validate().unwrap_err().contains("concurrency"));
    }

    #[test]
    fn should_default_to_slatedb_with_local_data_dir() {
        // given/when
        let config = StorageConfig::default();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert_eq!(slate_config.path, "data");
                assert_eq!(
                    slate_config.object_store,
                    ObjectStoreConfig::Local(LocalObjectStoreConfig {
                        path: ".data".to_string()
                    })
                );
            }
            _ => panic!("Expected SlateDb config as default"),
        }
    }

    #[test]
    fn should_deserialize_in_memory_config() {
        // given
        let yaml = r#"type: InMemory"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        assert_eq!(config, StorageConfig::InMemory);
    }

    #[test]
    fn should_deserialize_slatedb_config_with_local_object_store() {
        // given
        let yaml = r#"
type: SlateDb
path: my-data
object_store:
  type: Local
  path: /tmp/slatedb
"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert_eq!(slate_config.path, "my-data");
                assert_eq!(
                    slate_config.object_store,
                    ObjectStoreConfig::Local(LocalObjectStoreConfig {
                        path: "/tmp/slatedb".to_string()
                    })
                );
                assert!(slate_config.settings_path.is_none());
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_deserialize_slatedb_config_with_aws_object_store() {
        // given
        let yaml = r#"
type: SlateDb
path: my-data
object_store:
  type: Aws
  region: us-west-2
  bucket: my-bucket
settings_path: slatedb.toml
"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert_eq!(slate_config.path, "my-data");
                assert_eq!(
                    slate_config.object_store,
                    ObjectStoreConfig::Aws(AwsObjectStoreConfig {
                        region: "us-west-2".to_string(),
                        bucket: "my-bucket".to_string(),
                        endpoint: None,
                        allow_http: false,
                        virtual_hosted_style: false,
                    })
                );
                assert_eq!(slate_config.settings_path, Some("slatedb.toml".to_string()));
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_deserialize_cloud_object_store_options() {
        let azure: ObjectStoreConfig = serde_yaml::from_str(
            r#"
type: Azure
account: telemetry
container: metrics
endpoint: http://azurite:10000/telemetry
allow_http: true
"#,
        )
        .unwrap();
        assert_eq!(
            azure,
            ObjectStoreConfig::Azure(AzureObjectStoreConfig {
                account: "telemetry".to_string(),
                container: "metrics".to_string(),
                endpoint: Some("http://azurite:10000/telemetry".to_string()),
                allow_http: true,
            })
        );

        let gcp: ObjectStoreConfig = serde_yaml::from_str(
            r#"
type: Gcp
bucket: metrics
base_url: http://gcs-emulator:4443
"#,
        )
        .unwrap();
        assert_eq!(
            gcp,
            ObjectStoreConfig::Gcp(GcpObjectStoreConfig {
                bucket: "metrics".to_string(),
                base_url: Some("http://gcs-emulator:4443".to_string()),
            })
        );
    }

    #[test]
    fn should_deserialize_slatedb_config_with_in_memory_object_store() {
        // given
        let yaml = r#"
type: SlateDb
path: test-data
object_store:
  type: InMemory
"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert_eq!(slate_config.path, "test-data");
                assert_eq!(slate_config.object_store, ObjectStoreConfig::InMemory);
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_serialize_slatedb_config() {
        // given
        let config = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "my-data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: "/tmp/slatedb".to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
            disk: Default::default(),
        });

        // when
        let yaml = serde_yaml::to_string(&config).unwrap();

        // then
        assert!(yaml.contains("type: SlateDb"));
        assert!(yaml.contains("path: my-data"));
        assert!(yaml.contains("type: Local"));
        // settings_path and caches should be omitted when None
        assert!(!yaml.contains("settings_path"));
        assert!(!yaml.contains("block_cache"));
        assert!(!yaml.contains("meta_cache"));
    }

    #[test]
    fn should_deserialize_block_cache_config() {
        let yaml = r#"
type: SlateDb
path: data
object_store:
  type: InMemory
block_cache:
  type: FoyerHybrid
  memory_capacity: 8589934592
  disk_capacity: 150323855360
  disk_path: /mnt/nvme/block-cache
"#;
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            StorageConfig::SlateDb(slate_config) => {
                let cache = slate_config.block_cache.expect("block_cache should be set");
                match cache {
                    BlockCacheConfig::FoyerHybrid(foyer) => {
                        assert_eq!(foyer.memory_capacity, 8589934592);
                        assert_eq!(foyer.disk_capacity, 150323855360);
                        assert_eq!(foyer.disk_path, "/mnt/nvme/block-cache");
                        // new fields should get defaults
                        assert_eq!(foyer.write_policy, FoyerWritePolicy::WriteOnInsertion);
                        assert_eq!(foyer.flushers, 4);
                        assert!(foyer.buffer_pool_size.is_none());
                        assert_eq!(foyer.submit_queue_size_threshold, 64 * 1024 * 1024);
                        // effective buffer pool = memory_capacity / 32
                        assert_eq!(foyer.effective_buffer_pool_size(), 8589934592 / 32);
                    }
                    other => panic!("expected FoyerHybrid, got {other:?}"),
                }
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_deserialize_block_cache_with_explicit_engine_options() {
        // given
        let yaml = r#"
type: SlateDb
path: data
object_store:
  type: InMemory
block_cache:
  type: FoyerHybrid
  memory_capacity: 4294967296
  disk_capacity: 10737418240
  disk_path: /mnt/nvme/cache
  write_policy: WriteOnEviction
  flushers: 2
  buffer_pool_size: 134217728
  submit_queue_size_threshold: 536870912
"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                let cache = slate_config.block_cache.expect("block_cache should be set");
                match cache {
                    BlockCacheConfig::FoyerHybrid(foyer) => {
                        assert_eq!(foyer.write_policy, FoyerWritePolicy::WriteOnEviction);
                        assert_eq!(foyer.flushers, 2);
                        assert_eq!(foyer.buffer_pool_size, Some(134217728));
                        assert_eq!(foyer.submit_queue_size_threshold, 536870912);
                        // explicit value overrides derivation
                        assert_eq!(foyer.effective_buffer_pool_size(), 134217728);
                    }
                    other => panic!("expected FoyerHybrid, got {other:?}"),
                }
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_derive_buffer_pool_size_from_memory_capacity() {
        // given
        let config = FoyerHybridCacheConfig {
            memory_capacity: 8 * 1024 * 1024 * 1024, // 8 GiB
            disk_capacity: 100 * 1024 * 1024 * 1024,
            disk_path: "/tmp/cache".to_string(),
            write_policy: FoyerWritePolicy::default(),
            flushers: 4,
            buffer_pool_size: None,
            submit_queue_size_threshold: 1024 * 1024 * 1024,
        };

        // when/then
        assert_eq!(
            config.effective_buffer_pool_size(),
            256 * 1024 * 1024 // 256 MiB = 8 GiB / 32
        );
    }

    #[test]
    fn should_default_block_cache_to_none() {
        let yaml = r#"
type: SlateDb
path: data
object_store:
  type: InMemory
"#;
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert!(slate_config.block_cache.is_none());
                assert!(slate_config.meta_cache.is_none());
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_deserialize_split_cache_with_hybrid_data_and_memory_meta() {
        // given: a data block_cache on disk-backed hybrid, plus a small
        // in-memory meta_cache for index/filter/stats blocks.
        let yaml = r#"
type: SlateDb
path: data
object_store:
  type: InMemory
block_cache:
  type: FoyerHybrid
  memory_capacity: 536870912
  disk_capacity: 10737418240
  disk_path: /mnt/nvme/data-cache
meta_cache:
  type: FoyerMemory
  capacity: 134217728
"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        let StorageConfig::SlateDb(slate_config) = config else {
            panic!("Expected SlateDb config");
        };
        match slate_config.block_cache.expect("block_cache should be set") {
            BlockCacheConfig::FoyerHybrid(foyer) => {
                assert_eq!(foyer.memory_capacity, 536870912);
                assert_eq!(foyer.disk_path, "/mnt/nvme/data-cache");
            }
            other => panic!("expected FoyerHybrid data cache, got {other:?}"),
        }
        match slate_config.meta_cache.expect("meta_cache should be set") {
            BlockCacheConfig::FoyerMemory(mem) => {
                assert_eq!(mem.capacity, 134217728);
                assert!(mem.shards.is_none());
            }
            other => panic!("expected FoyerMemory meta cache, got {other:?}"),
        }
    }

    #[test]
    fn should_roundtrip_split_cache_config() {
        // given
        let config = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::InMemory,
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
                capacity: 64 * 1024 * 1024,
                shards: Some(8),
            })),
            meta_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
                capacity: 16 * 1024 * 1024,
                shards: None,
            })),
            disk: Default::default(),
        });

        // when
        let yaml = serde_yaml::to_string(&config).unwrap();
        let parsed: StorageConfig = serde_yaml::from_str(&yaml).unwrap();

        // then
        assert_eq!(config, parsed);
    }
}
