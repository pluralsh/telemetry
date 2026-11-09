// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

use std::time::Duration;

use common::coordinator::WriteCoordinatorConfig;
use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, StorageConfig};

#[derive(Clone, Debug)]
pub struct Config {
    pub storage: StorageConfig,
    pub segment_duration: Duration,
    /// How long data is kept, counted from ingestion; `None` keeps it
    /// forever.
    pub retention: Option<Duration>,
    pub page: PageConfig,
    pub write_buffer: WriteCoordinatorConfig,
    pub read_cache: crate::ReadCacheConfig,
    /// Estimated bytes SlateDB's compactions of every shard of the process
    /// hold at once; see [`common::storage::compaction::CompactionMemory`].
    pub compaction_memory_bytes: u64,
    /// Encoded OTLP bytes of writes every shard of the process has applied
    /// but not yet written to storage; see
    /// [`common::write_budget::WriteBudget`]. Ingest past it is rejected
    /// with backpressure, and the write buffer freezes at a third of it at
    /// most. Writes hold [`RESIDENT_BYTES_PER_ENCODED_BYTE`] times this
    /// while they are decoded, buffered and encoded into storage writes.
    pub write_budget_bytes: u64,
}

/// Peak resident bytes a delta and its flush hold per encoded OTLP byte:
/// decoded spans hold about 2.4 times their encoding, and encoding them into
/// pages, locators and postings peaks at 5.6 times.
pub const RESIDENT_BYTES_PER_ENCODED_BYTE: u64 = 6;

/// Default [`Config::write_budget_bytes`]: 48 MiB, so deltas of 16 MiB fit
/// three times and the write path holds about 290 MiB.
pub const DEFAULT_WRITE_BUDGET_BYTES: u64 = 48 * 1024 * 1024;

/// Default [`Config::retention`]: 14 days.
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(14 * 24 * 60 * 60);

#[derive(Clone, Debug)]
pub struct PageConfig {
    /// Soft target used to cut a page before adding another trace, measured
    /// over trace data without the page's column sidecar.
    pub target_size_bytes: usize,
    /// Hard bound on a page's trace data, without its column sidecar, which
    /// is always written in addition. A single trace exceeding it is rejected.
    pub max_size_bytes: usize,
    pub max_traces: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::SlateDb(common::storage::config::SlateDbStorageConfig {
                path: "traces".to_owned(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: ".data".to_owned(),
                }),
                settings_path: None,
                l0_flush_parallelism: None,
                block_cache: None,
                meta_cache: None,
                disk: Default::default(),
            }),
            segment_duration: Duration::from_secs(60 * 60),
            retention: Some(DEFAULT_RETENTION),
            page: PageConfig::default(),
            write_buffer: WriteCoordinatorConfig::default(),
            read_cache: crate::ReadCacheConfig::default(),
            compaction_memory_bytes: common::storage::compaction::DEFAULT_COMPACTION_MEMORY_BYTES,
            write_budget_bytes: DEFAULT_WRITE_BUDGET_BYTES,
        }
    }
}

impl Default for PageConfig {
    fn default() -> Self {
        Self {
            target_size_bytes: 1024 * 1024,
            max_size_bytes: 4 * 1024 * 1024,
            max_traces: 1024,
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
        if self.page.target_size_bytes == 0
            || self.page.max_size_bytes == 0
            || self.page.max_traces == 0
            || self.write_buffer.queue_capacity == 0
            || self.write_buffer.flush_interval.is_zero()
            || self.write_buffer.flush_size_threshold == 0
            || self.write_budget_bytes == 0
        {
            return Err(crate::Error::Invalid(
                "page size and trace limits must be positive".to_owned(),
            ));
        }
        if self.page.target_size_bytes > self.page.max_size_bytes {
            return Err(crate::Error::Invalid(
                "page target_size_bytes cannot exceed max_size_bytes".to_owned(),
            ));
        }
        Ok(())
    }
}
