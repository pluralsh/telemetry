// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::time::Duration;

use common::coordinator::WriteCoordinatorConfig;
use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, StorageConfig};

/// Single-node Logs storage and immutable-page tuning.
#[derive(Clone, Debug)]
pub struct Config {
    pub storage: StorageConfig,
    pub segment_duration: Duration,
    /// Period of the discovery rollup, which answers label and series
    /// requests for whole periods without walking their segments. A whole
    /// multiple of `segment_duration`; `None` disables it.
    pub discovery_rollup: Option<Duration>,
    /// How long data is kept, counted from ingestion; `None` keeps it
    /// forever.
    pub retention: Option<Duration>,
    pub page: PageConfig,
    pub compaction: CompactionConfig,
    pub write_buffer: WriteCoordinatorConfig,
    /// Byte budget of the process's cross-query cache of object blocks,
    /// which keeps each cached block's bodies once a query decompresses
    /// them. Zero disables it.
    pub block_cache_capacity_bytes: u64,
}

/// Default [`Config::block_cache_capacity_bytes`]: 256 MiB.
pub const DEFAULT_BLOCK_CACHE_CAPACITY_BYTES: u64 = 256 * 1024 * 1024;

/// Default [`Config::retention`]: 14 days.
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(14 * 24 * 60 * 60);

/// Every write-buffer flush cuts at least one page per written stream; these
/// limits split larger flushes further.
#[derive(Clone, Debug)]
pub struct PageConfig {
    pub target_size_bytes: usize,
    pub max_rows: usize,
    pub rows_per_block: usize,
}

/// Background merging of each stream's small pages, run by the writer after
/// write-buffer flushes.
#[derive(Clone, Debug)]
pub struct CompactionConfig {
    pub enabled: bool,
    /// Consecutive same-level pages merged into one.
    pub fan_in: usize,
    /// Minimum age of a written page before its first merge.
    pub min_age: Duration,
    /// Delay after a segment ends before its remaining small pages are merged
    /// regardless of `fan_in`.
    pub finalize_after: Duration,
    /// How long replaced payloads stay readable for in-flight queries and
    /// lagging read replicas.
    pub delete_delay: Duration,
    /// Upper bound on merges performed after one flush.
    pub max_merges_per_flush: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::SlateDb(common::storage::config::SlateDbStorageConfig {
                path: "logs".to_owned(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: ".data".to_owned(),
                }),
                settings_path: None,
                block_cache: None,
                meta_cache: None,
                disk: Default::default(),
            }),
            segment_duration: Duration::from_secs(60 * 60),
            discovery_rollup: Some(Duration::from_secs(24 * 60 * 60)),
            retention: Some(DEFAULT_RETENTION),
            page: PageConfig::default(),
            compaction: CompactionConfig::default(),
            write_buffer: WriteCoordinatorConfig::default(),
            block_cache_capacity_bytes: DEFAULT_BLOCK_CACHE_CAPACITY_BYTES,
        }
    }
}

impl Default for PageConfig {
    fn default() -> Self {
        Self {
            target_size_bytes: 1024 * 1024,
            max_rows: 16_384,
            rows_per_block: 256,
        }
    }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            fan_in: 8,
            min_age: Duration::from_secs(30),
            finalize_after: Duration::from_secs(5 * 60),
            delete_delay: Duration::from_secs(10 * 60),
            max_merges_per_flush: 256,
        }
    }
}

impl Config {
    pub(crate) fn validate(&self) -> crate::Result<()> {
        if self.segment_duration.is_zero() {
            return Err(crate::Error::Invalid(
                "segment_duration must be positive".to_owned(),
            ));
        }
        if let Some(rollup) = self.discovery_rollup
            && (rollup.is_zero() || rollup.as_nanos() % self.segment_duration.as_nanos() != 0)
        {
            return Err(crate::Error::Invalid(
                "discovery_rollup must be a whole multiple of segment_duration".to_owned(),
            ));
        }
        if self.page.target_size_bytes == 0
            || self.page.max_rows == 0
            || self.page.rows_per_block == 0
            || self.write_buffer.queue_capacity == 0
            || self.write_buffer.flush_interval.is_zero()
            || self.write_buffer.flush_size_threshold == 0
        {
            return Err(crate::Error::Invalid(
                "page size, row, and block limits must be positive".to_owned(),
            ));
        }
        if self.compaction.enabled
            && (self.compaction.fan_in < 2 || self.compaction.max_merges_per_flush == 0)
        {
            return Err(crate::Error::Invalid(
                "compaction fan_in must be at least 2 and max_merges_per_flush positive".to_owned(),
            ));
        }
        Ok(())
    }
}
