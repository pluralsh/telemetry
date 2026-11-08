// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

//! A process-wide bound on the estimated bytes of writes applied to a write
//! buffer but not yet written to storage.
//!
//! A write holds its share from before it is applied until its delta is
//! written, so the live delta, the one being encoded and the one being
//! committed all count against it, however far flushes fall behind. Ingest
//! waits up to [`INGEST_WAIT`] for its share and then fails, so clients back
//! off rather than the process growing until it is killed. Rollup copies
//! wait for room in an allowance of their own, half the budget beside it,
//! so a copy never takes room ingest needs: with copies drawing on the
//! budget too, a rollup on a busy writer turned ingest away.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// How long ingest waits for room before it is turned away, as long as
/// clients are told to wait before retrying. Concurrent requests arrive
/// faster than one flush frees the budget, so failing at once would reject
/// bursts that fit a moment later.
pub const INGEST_WAIT: Duration = Duration::from_secs(1);

/// A process's write budget; clones share it.
#[derive(Clone, Debug)]
pub struct WriteBudget {
    name: &'static str,
    write: Arc<Semaphore>,
    copy: Arc<Semaphore>,
    capacity: u32,
}

/// A write's share of a [`WriteBudget`], held until this drops.
#[derive(Debug)]
pub struct WritePermit {
    _copy: Option<OwnedSemaphorePermit>,
    write: Option<OwnedSemaphorePermit>,
    budget: WriteBudget,
}

impl WritePermit {
    /// Gives back all but `bytes` of this share, for a write that turned
    /// out to hold less than it reserved.
    pub fn shrink(&mut self, bytes: u64) {
        let keep = self.budget.weight(bytes) as usize;
        for permit in [self.write.as_mut(), self._copy.as_mut()]
            .into_iter()
            .flatten()
        {
            if let Some(extra) = permit.num_permits().checked_sub(keep).filter(|&n| n > 0) {
                drop(permit.split(extra));
            }
        }
        self.budget.report();
    }
}

impl Drop for WritePermit {
    fn drop(&mut self) {
        drop(self.write.take());
        self.budget.report();
    }
}

impl WriteBudget {
    /// A budget of `capacity` bytes, reported under `budget="{name}"`.
    pub fn new(name: &'static str, capacity: u64) -> Self {
        let capacity = u32::try_from(capacity).unwrap_or(u32::MAX).max(1);
        ::metrics::gauge!("write_budget_capacity_bytes", "budget" => name).set(f64::from(capacity));
        Self {
            name,
            write: Arc::new(Semaphore::new(capacity as usize)),
            copy: Arc::new(Semaphore::new((capacity / 2).max(1) as usize)),
            capacity,
        }
    }

    pub fn capacity(&self) -> u64 {
        u64::from(self.capacity)
    }

    /// Whether `bytes` of ingest, or of a copy, would find room now.
    pub fn fits(&self, copy: bool, bytes: u64) -> bool {
        let (semaphore, weight) = if copy {
            (&self.copy, self.weight(bytes).min(self.copy_capacity()))
        } else {
            (&self.write, self.weight(bytes))
        };
        semaphore.available_permits() as u64 >= u64::from(weight)
    }

    /// Holds `bytes` of ingest, waiting up to `wait` for room, or `None`
    /// when it runs out.
    pub async fn ingest(&self, bytes: u64, wait: Duration) -> Option<WritePermit> {
        let weight = self.weight(bytes);
        match Arc::clone(&self.write).try_acquire_many_owned(weight) {
            Ok(write) => return Some(self.held(None, Some(write))),
            Err(TryAcquireError::Closed) => return None,
            Err(TryAcquireError::NoPermits) => {}
        }
        ::metrics::counter!("write_budget_waits_total", "budget" => self.name).increment(1);
        match tokio::time::timeout(wait, Arc::clone(&self.write).acquire_many_owned(weight)).await {
            Ok(Ok(write)) => Some(self.held(None, Some(write))),
            Ok(Err(_)) => None,
            Err(_) => {
                ::metrics::counter!("write_budget_rejections_total", "budget" => self.name)
                    .increment(1);
                None
            }
        }
    }

    /// Holds `bytes` of a copy, waiting up to `wait` for room in the copies'
    /// allowance, or `None` when it runs out.
    pub async fn copy(&self, bytes: u64, wait: Duration) -> Option<WritePermit> {
        let weight = self.weight(bytes).min(self.copy_capacity());
        let copy = tokio::time::timeout(wait, Arc::clone(&self.copy).acquire_many_owned(weight))
            .await
            .ok()?
            .ok()?;
        Some(self.held(Some(copy), None))
    }

    /// Bytes of the budget no ingest holds.
    pub fn available(&self) -> u64 {
        self.write.available_permits() as u64
    }

    fn copy_capacity(&self) -> u32 {
        (self.capacity / 2).max(1)
    }

    /// `bytes` clamped to what one write may take, so a write larger than
    /// the whole budget waits for all of it rather than forever.
    fn weight(&self, bytes: u64) -> u32 {
        u32::try_from(bytes)
            .unwrap_or(u32::MAX)
            .clamp(1, self.capacity)
    }

    fn held(
        &self,
        copy: Option<OwnedSemaphorePermit>,
        write: Option<OwnedSemaphorePermit>,
    ) -> WritePermit {
        let permit = WritePermit {
            _copy: copy,
            write,
            budget: self.clone(),
        };
        self.report();
        permit
    }

    fn report(&self) {
        ::metrics::gauge!("write_budget_used_bytes", "budget" => self.name)
            .set(u64::from(self.capacity).saturating_sub(self.available()) as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WAIT: Duration = Duration::from_millis(20);

    #[tokio::test]
    async fn ingest_waits_for_room_then_fails() {
        let budget = WriteBudget::new("test", 100);
        let held = budget.ingest(80, WAIT).await.unwrap();
        assert!(!budget.fits(false, 30));
        assert!(budget.ingest(30, WAIT).await.is_none());
        let waiting = tokio::spawn({
            let budget = budget.clone();
            async move { budget.ingest(30, Duration::from_secs(5)).await.is_some() }
        });
        tokio::time::sleep(WAIT).await;
        drop(held);
        assert!(waiting.await.unwrap());
        assert_eq!(budget.available(), 100);
    }

    #[tokio::test]
    async fn copies_hold_half_beside_ingest_and_time_out() {
        let budget = WriteBudget::new("test", 100);
        let copy = budget.copy(1_000, WAIT).await.unwrap();
        assert_eq!(budget.available(), 100);
        assert!(!budget.fits(true, 1));
        assert!(budget.copy(1, WAIT).await.is_none());
        let ingest = budget.ingest(100, WAIT).await.unwrap();
        drop((copy, ingest));
        assert!(budget.fits(true, 50));
        assert_eq!(budget.available(), 100);
    }

    #[tokio::test]
    async fn shrinking_a_permit_gives_back_the_rest() {
        let budget = WriteBudget::new("test", 100);
        let mut ingest = budget.ingest(40, WAIT).await.unwrap();
        let mut copy = budget.copy(40, WAIT).await.unwrap();
        ingest.shrink(10);
        copy.shrink(5);
        copy.shrink(20);
        assert_eq!(budget.available(), 90);
        assert!(budget.copy(45, WAIT).await.is_some());
        drop((ingest, copy));
        assert_eq!(budget.available(), 100);
    }

    #[tokio::test]
    async fn a_write_larger_than_the_budget_takes_all_of_it() {
        let budget = WriteBudget::new("test", 100);
        let held = budget.ingest(1_000, WAIT).await.unwrap();
        assert_eq!(budget.available(), 0);
        drop(held);
        assert_eq!(budget.available(), 100);
    }
}
