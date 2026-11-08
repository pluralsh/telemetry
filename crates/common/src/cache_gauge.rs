// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

//! What a process's in-memory caches hold, as `telemetry_cache_used_bytes`
//! by cache name.
//!
//! Each shard's database can hold its own instance of a cache, so an
//! instance adds the change in its size to the process-wide gauge and takes
//! back what it reported when dropped: the gauge is the sum over instances.

use std::sync::atomic::{AtomicU64, Ordering};

use metrics::Gauge;

pub struct CacheGauge {
    gauge: Gauge,
    reported: AtomicU64,
}

impl CacheGauge {
    pub fn new(cache: &'static str) -> Self {
        Self {
            gauge: metrics::gauge!("telemetry_cache_used_bytes", "cache" => cache),
            reported: AtomicU64::new(0),
        }
    }

    /// Reports that this instance holds `bytes`.
    pub fn set(&self, bytes: u64) {
        let previous = self.reported.swap(bytes, Ordering::Relaxed);
        if bytes >= previous {
            self.gauge.increment((bytes - previous) as f64);
        } else {
            self.gauge.decrement((previous - bytes) as f64);
        }
    }
}

impl Drop for CacheGauge {
    fn drop(&mut self) {
        self.gauge
            .decrement(self.reported.load(Ordering::Relaxed) as f64);
    }
}

impl std::fmt::Debug for CacheGauge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheGauge")
            .field("reported", &self.reported.load(Ordering::Relaxed))
            .finish()
    }
}
