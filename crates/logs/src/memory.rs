// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

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
//!   Rollup copies hold an allowance of their own beside it, half its
//!   size, so a copy never takes room ingest needs.
//! - **merge**: [`MERGE_BYTES_PER_INPUT_BYTE`] times the stored bytes of
//!   the objects each compactor merge reads: their blocks, the merged
//!   object's and the copy SlateDB's memtable takes of its write.
//! - **compaction**: the estimate of what SlateDB's compactions of every
//!   shard hold; see [`CompactionMemory`].
//!
//! A unit, write or merge larger than a whole budget waits for all of it
//! rather than forever, so one budget holds at most
//! `max(capacity, largest single acquisition)`.

use std::sync::Arc;
use std::time::Duration;

pub use common::storage::compaction::{CompactionMemory, DEFAULT_COMPACTION_MEMORY_BYTES};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::error::{Error, Result};

/// How long ingest waits for room in the write budget before it is turned
/// away, as long as clients are told to wait before retrying. Concurrent
/// requests arrive faster than one flush frees the budget, so failing at
/// once would reject bursts that fit a moment later.
pub(crate) const INGEST_WAIT: Duration = Duration::from_secs(1);

/// Default [`MemoryConfig::decode_bytes`]: 256 MiB.
pub const DEFAULT_DECODE_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// Default [`MemoryConfig::write_bytes`]: 24 MiB, so the default 8 MiB
/// buffer threshold fits three times. A flush holds about 23 times its
/// bytes while it builds its storage writes, so about 185 MiB.
pub const DEFAULT_WRITE_BUDGET_BYTES: u64 = 24 * 1024 * 1024;

/// Resident bytes a compactor merge holds per stored byte it reads: the
/// inputs' blocks, the merged object's, and SlateDB's memtable copy of its
/// write, which holds about 1.5 times what it is given beyond that.
pub const MERGE_BYTES_PER_INPUT_BYTE: u64 = 4;

/// Default [`MemoryConfig::merge_bytes`]: one merge of the default 32 MiB
/// `max_object_bytes` of inputs.
pub const DEFAULT_MERGE_BUDGET_BYTES: u64 = MERGE_BYTES_PER_INPUT_BYTE * 32 * 1024 * 1024;

/// SlateDB's settings with the writers' memtable bound, sized for the L0
/// uploads `storage` asks for, the base a settings file or `SLATEDB_`
/// environment variables override.
pub(crate) fn slatedb_settings(
    storage: &common::storage::config::StorageConfig,
) -> slatedb::config::Settings {
    common::storage::settings::writer_settings_for(storage)
}

/// Byte budgets of a process's [`MemoryBudget`].
#[derive(Clone, Debug)]
pub struct MemoryConfig {
    /// Estimated decoded bytes the read units of every query, sample read
    /// and rollup copy in the process hold at once.
    pub decode_bytes: u64,
    /// Estimated bytes of writes the process has applied but not yet
    /// written to storage, and so the most one flush holds. Ingest past it
    /// fails with [`Error::Backpressure`]; rollup copies wait for room. The
    /// write buffer freezes at a third of it at most, whatever its
    /// `flush_size_threshold`, so ingest has room while one flush is encoded
    /// and the one before it written.
    pub write_bytes: u64,
    /// Estimated bytes the compactor merges of every shard hold at once.
    pub merge_bytes: u64,
    /// Estimated bytes SlateDB's compactions of every shard hold at once.
    pub compaction_bytes: u64,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            decode_bytes: DEFAULT_DECODE_BUDGET_BYTES,
            write_bytes: DEFAULT_WRITE_BUDGET_BYTES,
            merge_bytes: DEFAULT_MERGE_BUDGET_BYTES,
            compaction_bytes: DEFAULT_COMPACTION_MEMORY_BYTES,
        }
    }
}

/// The process-wide budgets of [`MemoryConfig`], cloned into every database
/// the process opens.
#[derive(Clone, Debug)]
pub(crate) struct MemoryBudget {
    decode: Arc<Budget>,
    write: Arc<Budget>,
    /// What rollup copies hold, half the size of `write` and beside it: with
    /// copies drawing on `write`, a rollup on a busy writer turned ingest
    /// away.
    copy: Arc<Budget>,
    merge: Arc<Budget>,
    compaction: CompactionMemory,
}

/// Who a write's bytes are held for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Writer {
    /// Waits up to [`INGEST_WAIT`] for room, then fails so clients retry
    /// later.
    Ingest,
    /// Waits for room in the copies' allowance.
    Copy,
}

impl MemoryBudget {
    pub(crate) fn new(config: &MemoryConfig) -> Self {
        Self {
            decode: Arc::new(Budget::new("decode", config.decode_bytes)),
            write: Arc::new(Budget::new("write", config.write_bytes)),
            copy: Arc::new(Budget::new("copy", (config.write_bytes / 2).max(1))),
            merge: Arc::new(Budget::new("merge", config.merge_bytes)),
            compaction: CompactionMemory::new(config.compaction_bytes),
        }
    }

    /// Holds `bytes` of decoding until the permit drops.
    pub(crate) async fn decode(&self, bytes: u64) -> Result<BudgetPermit> {
        self.decode.acquire(bytes).await
    }

    /// Holds a compactor merge of `input_bytes` stored bytes until the
    /// permit drops.
    pub(crate) async fn merge(&self, input_bytes: u64) -> Result<BudgetPermit> {
        self.merge
            .acquire(input_bytes.saturating_mul(MERGE_BYTES_PER_INPUT_BYTE))
            .await
    }

    /// The budget SlateDB's compactions of every shard share.
    pub(crate) fn compaction(&self) -> CompactionMemory {
        self.compaction.clone()
    }

    /// Holds `bytes` of applied writes until the permit drops. Ingest waits
    /// up to `wait` for the write budget, a copy for the copies' allowance;
    /// either fails with [`Error::Backpressure`] after.
    pub(crate) async fn write(
        &self,
        writer: Writer,
        bytes: u64,
        wait: Duration,
    ) -> Result<WritePermit> {
        match writer {
            Writer::Ingest => Ok(WritePermit {
                _held: self.write.acquire_within(bytes, wait).await?,
            }),
            Writer::Copy => Ok(WritePermit {
                _held: tokio::time::timeout(wait, self.copy.acquire(bytes))
                    .await
                    .map_err(|_| Error::Backpressure)??,
            }),
        }
    }

    /// Whether a write of `bytes` by `writer` would find room now.
    pub(crate) fn write_fits(&self, writer: Writer, bytes: u64) -> bool {
        let fits = |budget: &Budget| budget.available() >= bytes.min(u64::from(budget.capacity));
        match writer {
            Writer::Ingest => fits(&self.write),
            Writer::Copy => fits(&self.copy),
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

    /// What rollup copies may hold at once.
    pub(crate) fn copy_capacity(&self) -> u64 {
        u64::from(self.copy.capacity)
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

    /// Waits up to `wait` for `bytes`, then fails with backpressure.
    async fn acquire_within(self: &Arc<Self>, bytes: u64, wait: Duration) -> Result<BudgetPermit> {
        let weight = self.weight(bytes);
        match Arc::clone(&self.semaphore).try_acquire_many_owned(weight) {
            Ok(permit) => return Ok(self.held(permit)),
            Err(TryAcquireError::Closed) => {
                return Err(Error::Unavailable(format!("{} budget closed", self.name)));
            }
            Err(TryAcquireError::NoPermits) => {}
        }
        ::metrics::counter!("logs_memory_budget_waits_total", "budget" => self.name).increment(1);
        match tokio::time::timeout(wait, Arc::clone(&self.semaphore).acquire_many_owned(weight))
            .await
        {
            Ok(Ok(permit)) => Ok(self.held(permit)),
            Ok(Err(_)) => Err(Error::Unavailable(format!("{} budget closed", self.name))),
            Err(_) => {
                ::metrics::counter!("logs_memory_budget_rejections_total", "budget" => self.name)
                    .increment(1);
                Err(Error::Backpressure)
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

/// Bytes of a write held in the write budget, or in the copies' allowance
/// for rollup copies, until this drops.
#[derive(Debug)]
pub(crate) struct WritePermit {
    _held: BudgetPermit,
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
            ..MemoryConfig::default()
        })
    }

    #[tokio::test]
    async fn merges_wait_for_their_share_of_the_merge_budget() {
        let budget = MemoryBudget::new(&MemoryConfig {
            merge_bytes: 100,
            ..MemoryConfig::default()
        });
        let held = budget.merge(20).await.unwrap();
        let waiting = tokio::spawn({
            let budget = budget.clone();
            async move { budget.merge(10).await.map(drop) }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished(), "80 of 100 bytes are held");
        drop(held);
        waiting.await.unwrap().unwrap();
        // A merge larger than the budget takes all of it.
        drop(budget.merge(1_000).await.unwrap());
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
        let settings = slatedb_settings(&common::storage::config::StorageConfig::InMemory);
        assert_eq!(
            settings.max_unflushed_bytes,
            common::storage::settings::DEFAULT_MAX_UNFLUSHED_BYTES
        );
        assert!(settings.max_unflushed_bytes > settings.l0_sst_size_bytes);
    }

    #[tokio::test]
    async fn a_full_write_budget_rejects_ingest_and_leaves_copies_their_room() {
        let budget = budget(100, 100);
        let held = budget.write(Writer::Ingest, 80, WAIT).await.unwrap();
        assert!(matches!(
            budget.write(Writer::Ingest, 30, WAIT).await,
            Err(Error::Backpressure)
        ));
        let copy = budget.write(Writer::Copy, 30, WAIT).await.unwrap();
        budget.write(Writer::Ingest, 20, WAIT).await.unwrap();
        drop((held, copy));
        assert_eq!(budget.write_available(), 100);
    }

    #[tokio::test]
    async fn copies_hold_half_the_write_budget_beside_it() {
        let budget = budget(100, 100);
        assert_eq!(budget.copy_capacity(), 50);
        let copies = [
            budget.write(Writer::Copy, 30, WAIT).await.unwrap(),
            budget.write(Writer::Copy, 20, WAIT).await.unwrap(),
        ];
        assert!(!budget.write_fits(Writer::Copy, 1));
        assert!(matches!(
            budget.write(Writer::Copy, 1, WAIT).await,
            Err(Error::Backpressure)
        ));
        // Ingest still has the whole write budget.
        let ingest = budget.write(Writer::Ingest, 100, WAIT).await.unwrap();
        assert_eq!(budget.write_available(), 0);
        drop((copies, ingest));
        assert_eq!(budget.write_available(), 100);
        // A copy larger than its allowance takes all of it.
        let large = budget.write(Writer::Copy, 1_000, WAIT).await.unwrap();
        assert!(!budget.write_fits(Writer::Copy, 1));
        assert_eq!(budget.write_available(), 100);
        drop(large);
    }
}
