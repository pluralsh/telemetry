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
            flush_interval_seconds: 30,
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
