//! Configuration sections shared by the product servers.

use std::time::Duration;

use common::coordinator::WriteCoordinatorConfig;
use serde::{Deserialize, Serialize};

/// How far a write must progress before the server acknowledges it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
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
            flush_interval_seconds: common::DEFAULT_FLUSH_INTERVAL_SECONDS,
            buffer_queue_capacity: 10_000,
            buffer_flush_interval_milliseconds: 30_000,
            buffer_size_threshold_bytes: 64 * 1024 * 1024,
            remote_concurrency: 16,
            remote_retries: 2,
        }
    }
}

impl WriteConfig {
    /// Whether any buffer or forwarding limit is zero, which no server accepts.
    pub fn has_zero_limit(&self) -> bool {
        self.buffer_queue_capacity == 0
            || self.buffer_flush_interval_milliseconds == 0
            || self.buffer_size_threshold_bytes == 0
            || self.remote_concurrency == 0
    }

    pub fn flush_interval(&self) -> Duration {
        Duration::from_secs(self.flush_interval_seconds)
    }

    pub fn write_buffer(&self) -> WriteCoordinatorConfig {
        WriteCoordinatorConfig {
            queue_capacity: self.buffer_queue_capacity,
            flush_interval: Duration::from_millis(self.buffer_flush_interval_milliseconds),
            flush_size_threshold: self.buffer_size_threshold_bytes,
        }
    }
}

/// Background rollup of closed time segments into wider ones.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SegmentRollupConfig {
    pub enabled: bool,
    /// Largest rollup segment, by the stored size of what it covers.
    pub max_segment_bytes: u64,
    /// How long after a day ends before its segments are rolled up.
    pub delay_seconds: u64,
    /// How often to look for days to roll up.
    pub interval_seconds: u64,
    /// Copied bytes a rollup may queue before waiting for storage.
    pub copy_buffer_bytes: u64,
}

impl Default for SegmentRollupConfig {
    fn default() -> Self {
        let defaults = common::storage::rollup::RollupConfig::default();
        Self {
            enabled: defaults.enabled,
            max_segment_bytes: defaults.max_segment_bytes,
            delay_seconds: defaults.delay.as_secs(),
            interval_seconds: defaults.interval.as_secs(),
            copy_buffer_bytes: defaults.copy_buffer_bytes,
        }
    }
}

impl SegmentRollupConfig {
    pub fn rollup(&self) -> common::storage::rollup::RollupConfig {
        common::storage::rollup::RollupConfig {
            enabled: self.enabled,
            max_segment_bytes: self.max_segment_bytes,
            delay: Duration::from_secs(self.delay_seconds),
            interval: Duration::from_secs(self.interval_seconds.max(1)),
            copy_buffer_bytes: self.copy_buffer_bytes.max(1),
        }
    }
}
