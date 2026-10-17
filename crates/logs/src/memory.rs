// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Process-wide memory budgets.
//!
//! Every database a process opens (one per shard) draws on one
//! [`MemoryBudget`], as it shares one block cache, so the memory reads and
//! writes hold is bounded per process rather than per query, shard or
//! write:
//!
//! - **decode**: the estimated decoded bytes of the read units every query,
//!   sample read and rollup copy holds from fetching a unit until its rows
//!   are handed on. A rollup copy keeps a unit's share until its rows are
//!   applied to the write buffer, where the write budget takes them over.
//! - **write**: the estimated bytes of writes applied to the write buffer
//!   but not yet written to storage, which covers the live delta, the
//!   flushes queued behind it and the one being written. Every delta is a
//!   subset of those writes, so this also bounds the largest flush, which
//!   the write buffer's size threshold alone does not: while flushes are
//!   queued the live delta keeps absorbing writes. A flush materializes
//!   its objects, full-text index and storage batch at once, on the order
//!   of 35 times its estimated bytes for production-shaped logs (a 64 MiB
//!   delta of 450k rows became 4.2M storage operations and 2.3 GiB), so
//!   the write path holds roughly `(35 + 2) * write_bytes` at most.
//!
//! A unit or write larger than a whole budget waits for all of it rather
//! than forever, so one budget holds at most
//! `max(capacity, largest single acquisition)`.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::error::{Error, Result};

/// Default [`MemoryConfig::decode_bytes`]: 256 MiB.
pub const DEFAULT_DECODE_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// Default [`MemoryConfig::write_bytes`]: 16 MiB. A flush holds about 23
/// times its bytes while it builds its storage writes, so about 370 MiB.
pub const DEFAULT_WRITE_BUDGET_BYTES: u64 = 16 * 1024 * 1024;

/// Logs' SlateDB `max_unflushed_bytes`, 128 MiB rather than SlateDB's
/// 1 GiB: the estimated encoded size of the memtables and WAL buffer past
/// which SlateDB holds new writes until L0 flushes catch up. Memtables hold
/// about 2.5 times their estimate, and readers replay the same unflushed
/// WAL, so this bounds both. It must exceed `l0_sst_size_bytes`, which
/// stays at SlateDB's 64 MiB: smaller L0 SSTs stall writes on L0 count.
pub const DEFAULT_MAX_UNFLUSHED_BYTES: usize = 128 * 1024 * 1024;

/// SlateDB's settings with logs' memtable bound, the base a settings file
/// or `SLATEDB_` environment variables override.
pub(crate) fn slatedb_settings() -> slatedb::config::Settings {
    slatedb::config::Settings {
        max_unflushed_bytes: DEFAULT_MAX_UNFLUSHED_BYTES,
        ..common::storage::settings::base_settings()
    }
}

/// Byte budgets of a process's [`MemoryBudget`].
#[derive(Clone, Debug)]
pub struct MemoryConfig {
    /// Estimated decoded bytes the read units of every query, sample read
    /// and rollup copy in the process hold at once.
    pub decode_bytes: u64,
    /// Estimated bytes of writes the process has applied but not yet
    /// written to storage, and so the most one flush holds. Ingest past it
    /// fails with [`Error::Backpressure`]; rollup copies wait for room. A
    /// write that finds it full flushes the buffer, so it may be below the
    /// write buffer's `flush_size_threshold`.
    pub write_bytes: u64,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            decode_bytes: DEFAULT_DECODE_BUDGET_BYTES,
            write_bytes: DEFAULT_WRITE_BUDGET_BYTES,
        }
    }
}

/// The process-wide budgets of [`MemoryConfig`], cloned into every database
/// the process opens.
#[derive(Clone, Debug)]
pub(crate) struct MemoryBudget {
    decode: Arc<Budget>,
    write: Arc<Budget>,
    /// The half of `write` rollup copies may hold at most, so they never
    /// keep ingest out of the other half.
    copy: Arc<Budget>,
}

/// Who a write's bytes are held for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Writer {
    /// Fails at once when the budget is full, so clients retry later.
    Ingest,
    /// Waits for room in its half of the budget.
    Copy,
}

impl MemoryBudget {
    pub(crate) fn new(config: &MemoryConfig) -> Self {
        Self {
            decode: Arc::new(Budget::new("decode", config.decode_bytes)),
            write: Arc::new(Budget::new("write", config.write_bytes)),
            copy: Arc::new(Budget::new("copy", (config.write_bytes / 2).max(1))),
        }
    }

    /// Holds `bytes` of decoding until the permit drops.
    pub(crate) async fn decode(&self, bytes: u64) -> Result<BudgetPermit> {
        self.decode.acquire(bytes).await
    }

    /// Holds `bytes` of applied writes until the permit drops. Ingest fails
    /// with [`Error::Backpressure`] at once when they do not fit; a copy
    /// waits up to `wait` for its half of the budget and then the budget.
    pub(crate) async fn write(
        &self,
        writer: Writer,
        bytes: u64,
        wait: Duration,
    ) -> Result<WritePermit> {
        match writer {
            Writer::Ingest => Ok(WritePermit {
                _copy: None,
                _write: self.write.try_acquire(bytes)?,
            }),
            Writer::Copy => tokio::time::timeout(wait, async {
                let bytes = bytes.min(u64::from(self.copy.capacity));
                let copy = self.copy.acquire(bytes).await?;
                Ok(WritePermit {
                    _copy: Some(copy),
                    _write: self.write.acquire(bytes).await?,
                })
            })
            .await
            .map_err(|_| Error::Backpressure)?,
        }
    }

    /// Whether a write of `bytes` by `writer` would find room now.
    pub(crate) fn write_fits(&self, writer: Writer, bytes: u64) -> bool {
        let fits = |budget: &Budget| budget.available() >= bytes.min(u64::from(budget.capacity));
        match writer {
            Writer::Ingest => fits(&self.write),
            Writer::Copy => {
                fits(&self.copy)
                    && self.write.available() >= bytes.min(u64::from(self.copy.capacity))
            }
        }
    }

    /// Bytes of the write budget no write holds.
    #[cfg(test)]
    pub(crate) fn write_available(&self) -> u64 {
        self.write.available()
    }

    pub(crate) fn write_capacity(&self) -> u64 {
        u64::from(self.write.capacity)
    }

    /// The most one ingest write holds, a quarter of the write budget; larger
    /// requests are applied in chunks of it.
    pub(crate) fn write_chunk_bytes(&self) -> u64 {
        (self.write_capacity() / 4).max(1)
    }

    /// Bytes of the decode budget no read holds.
    #[cfg(test)]
    pub(crate) fn decode_available(&self) -> u64 {
        self.decode.available()
    }
}

#[derive(Debug)]
struct Budget {
    name: &'static str,
    semaphore: Arc<Semaphore>,
    capacity: u32,
}

impl Budget {
    fn new(name: &'static str, capacity: u64) -> Self {
        let capacity = u32::try_from(capacity).unwrap_or(u32::MAX).max(1);
        ::metrics::gauge!("logs_memory_budget_capacity_bytes", "budget" => name)
            .set(f64::from(capacity));
        Self {
            name,
            semaphore: Arc::new(Semaphore::new(capacity as usize)),
            capacity,
        }
    }

    /// `bytes` clamped to what one acquisition may take.
    fn weight(&self, bytes: u64) -> u32 {
        u32::try_from(bytes)
            .unwrap_or(u32::MAX)
            .clamp(1, self.capacity)
    }

    async fn acquire(self: &Arc<Self>, bytes: u64) -> Result<BudgetPermit> {
        let weight = self.weight(bytes);
        let permit = match Arc::clone(&self.semaphore).try_acquire_many_owned(weight) {
            Ok(permit) => permit,
            Err(_) => {
                ::metrics::counter!("logs_memory_budget_waits_total", "budget" => self.name)
                    .increment(1);
                Arc::clone(&self.semaphore)
                    .acquire_many_owned(weight)
                    .await
                    .map_err(|_| Error::Unavailable(format!("{} budget closed", self.name)))?
            }
        };
        Ok(self.held(permit))
    }

    fn try_acquire(self: &Arc<Self>, bytes: u64) -> Result<BudgetPermit> {
        match Arc::clone(&self.semaphore).try_acquire_many_owned(self.weight(bytes)) {
            Ok(permit) => Ok(self.held(permit)),
            Err(TryAcquireError::NoPermits) => {
                ::metrics::counter!("logs_memory_budget_rejections_total", "budget" => self.name)
                    .increment(1);
                Err(Error::Backpressure)
            }
            Err(TryAcquireError::Closed) => {
                Err(Error::Unavailable(format!("{} budget closed", self.name)))
            }
        }
    }

    fn held(self: &Arc<Self>, permit: OwnedSemaphorePermit) -> BudgetPermit {
        self.report();
        BudgetPermit {
            permit: Some(permit),
            budget: Arc::clone(self),
        }
    }

    fn available(&self) -> u64 {
        self.semaphore.available_permits() as u64
    }

    fn report(&self) {
        ::metrics::gauge!("logs_memory_budget_used_bytes", "budget" => self.name)
            .set((u64::from(self.capacity).saturating_sub(self.available())) as f64);
    }
}

/// Bytes of a write held in the write budget, and in the copy half of it
/// for rollup copies, until this drops.
#[derive(Debug)]
pub(crate) struct WritePermit {
    _copy: Option<BudgetPermit>,
    _write: BudgetPermit,
}

/// Bytes of a [`MemoryBudget`] held until this drops.
#[derive(Debug)]
pub(crate) struct BudgetPermit {
    permit: Option<OwnedSemaphorePermit>,
    budget: Arc<Budget>,
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.budget.report();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(decode_bytes: u64, write_bytes: u64) -> MemoryBudget {
        MemoryBudget::new(&MemoryConfig {
            decode_bytes,
            write_bytes,
        })
    }

    #[tokio::test]
    async fn clones_share_one_budget() {
        let first = budget(100, 100);
        let second = first.clone();
        let held = first.decode(60).await.unwrap();
        assert_eq!(second.decode_available(), 40);
        let waiting = tokio::spawn({
            let second = second.clone();
            async move { second.decode(60).await.map(drop) }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished(), "the shared budget has no room");
        drop(held);
        waiting.await.unwrap().unwrap();
        assert_eq!(first.decode_available(), 100);
    }

    #[tokio::test]
    async fn an_acquisition_larger_than_the_budget_takes_all_of_it() {
        let budget = budget(100, 100);
        let held = budget.decode(1_000).await.unwrap();
        assert_eq!(budget.decode_available(), 0);
        drop(held);
        assert_eq!(budget.decode_available(), 100);
    }

    const WAIT: Duration = Duration::from_millis(10);

    #[test]
    fn slatedb_settings_cap_unflushed_bytes_above_the_l0_sst_size() {
        let settings = slatedb_settings();
        assert_eq!(settings.max_unflushed_bytes, DEFAULT_MAX_UNFLUSHED_BYTES);
        assert!(settings.max_unflushed_bytes > settings.l0_sst_size_bytes);
    }

    #[tokio::test]
    async fn a_full_write_budget_rejects_ingest_and_times_copies_out() {
        let budget = budget(100, 100);
        let held = budget.write(Writer::Ingest, 80, WAIT).await.unwrap();
        assert!(matches!(
            budget.write(Writer::Ingest, 30, WAIT).await,
            Err(Error::Backpressure)
        ));
        assert!(matches!(
            budget.write(Writer::Copy, 30, WAIT).await,
            Err(Error::Backpressure)
        ));
        budget.write(Writer::Ingest, 20, WAIT).await.unwrap();
        drop(held);
        assert_eq!(budget.write_available(), 100);
    }

    #[tokio::test]
    async fn copies_hold_at_most_half_the_write_budget() {
        let budget = budget(100, 100);
        let copies = [
            budget.write(Writer::Copy, 30, WAIT).await.unwrap(),
            budget.write(Writer::Copy, 20, WAIT).await.unwrap(),
        ];
        assert!(matches!(
            budget.write(Writer::Copy, 1, WAIT).await,
            Err(Error::Backpressure)
        ));
        // Ingest still has the half copies cannot take.
        let ingest = budget.write(Writer::Ingest, 50, WAIT).await.unwrap();
        assert_eq!(budget.write_available(), 0);
        drop(copies);
        drop(ingest);
        assert_eq!(budget.write_available(), 100);
        // A copy larger than its half takes all of the half.
        let large = budget.write(Writer::Copy, 1_000, WAIT).await.unwrap();
        assert_eq!(budget.write_available(), 50);
        drop(large);
    }
}
