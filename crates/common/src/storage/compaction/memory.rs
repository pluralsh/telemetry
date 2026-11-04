//! Process-wide bound on the memory SlateDB compactions hold.
//!
//! A compaction worker reads each source through its own iterator, which
//! keeps `max_fetch_tasks` reads of `bytes_to_fetch` (rounded up to whole
//! blocks) in flight ahead of its cursor, and writes each output SST through
//! an object-store multipart writer that buffers a 10 MiB part and keeps up
//! to 8 more in flight. Every subcompaction holds both, so a compaction
//! holds about
//!
//! ```text
//! subcompactions * (sources * max_fetch_tasks * bytes_to_fetch + 90 MiB)
//! ```
//!
//! however small its inputs are, and SlateDB bounds neither the sources of
//! a compaction nor how many run at once by bytes.
//!
//! [`CompactionMemory`] does: [`CompactionMemory::bound`] shrinks a
//! compactor's options until one compaction of its most sources fits the
//! budget, and every database sharing it admits a proposal only while the
//! estimates of the compactions they all run, the proposal's included, fit.
//! A compaction larger than the whole budget runs only when no other does,
//! so the process holds at most `max(budget, largest compaction)`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use slatedb::compactor::CompactionSpec;
use slatedb::config::{CompactionWorkerOptions, CompactorOptions};

use crate::storage::config::DiskPerformance;

const MIB: u64 = 1024 * 1024;

/// Default [`CompactionMemory`] budget: one compaction of eight sources with
/// SlateDB's default prefetch and subcompactions takes about 620 MiB.
pub const DEFAULT_COMPACTION_MEMORY_BYTES: u64 = 640 * MIB;

/// What one subcompaction's output writer holds at most: object_store's
/// `BufWriter`, which SlateDB builds with its defaults, buffers a 10 MiB
/// part and uploads up to 8 at once.
const OUTPUT_BYTES: u64 = 9 * 10 * MIB;

/// The largest SST block writers produce. A fetch is rounded up to whole
/// blocks, so each may read this much more than `bytes_to_fetch`.
const MAX_BLOCK_BYTES: u64 = DiskPerformance::MAX_SST_BLOCK_BYTES as u64;

/// A byte budget shared by the compactions of every database that clones
/// it, normally one per process.
#[derive(Clone, Debug)]
pub struct CompactionMemory(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    budget: u64,
    /// Estimated bytes each scheduler's admitted compactions hold, by
    /// scheduler.
    held: Mutex<HashMap<u64, u64>>,
    next_scheduler: AtomicU64,
}

impl Default for CompactionMemory {
    fn default() -> Self {
        Self::new(DEFAULT_COMPACTION_MEMORY_BYTES)
    }
}

impl CompactionMemory {
    pub fn new(budget_bytes: u64) -> Self {
        let budget = budget_bytes.max(1);
        ::metrics::gauge!("storage_compaction_memory_budget_bytes").set(budget as f64);
        Self(Arc::new(Inner {
            budget,
            held: Mutex::default(),
            next_scheduler: AtomicU64::new(0),
        }))
    }

    pub fn budget(&self) -> u64 {
        self.0.budget
    }

    /// Estimated bytes the compactions of every database sharing this budget
    /// hold.
    pub fn held(&self) -> u64 {
        self.lock().values().sum()
    }

    /// Shrinks `options` until a compaction of `max_sources` sources fits the
    /// budget: prefetch first, down to one block per read and one read per
    /// source, then subcompactions, then how many compactions run at once.
    /// Options already below what the budget allows are kept. Without an
    /// embedded worker, compactions run in another process and nothing
    /// changes.
    pub fn bound(&self, options: &mut CompactorOptions, max_sources: usize) {
        let Some(worker) = options.worker.as_mut() else {
            return;
        };
        let sources = max_sources.max(1) as u64;
        let budget = self.budget();
        let subcompaction =
            |worker: &CompactionWorkerOptions| sources * Cost::per_source(worker) + OUTPUT_BYTES;
        while subcompaction(worker) > budget {
            if worker.bytes_to_fetch as u64 > MAX_BLOCK_BYTES {
                worker.bytes_to_fetch = (worker.bytes_to_fetch / 2).max(MAX_BLOCK_BYTES as usize);
            } else if worker.max_fetch_tasks > 1 {
                worker.max_fetch_tasks -= 1;
            } else {
                break;
            }
        }
        let subcompaction = subcompaction(worker);
        let subcompactions = usize::try_from(budget / subcompaction)
            .unwrap_or(usize::MAX)
            .max(1);
        worker.max_subcompactions = worker.max_subcompactions.clamp(1, subcompactions);
        let compaction = subcompaction * worker.max_subcompactions as u64;
        let concurrent = usize::try_from(budget / compaction)
            .unwrap_or(usize::MAX)
            .max(1);
        worker.max_concurrent_compactions = worker.max_concurrent_compactions.clamp(1, concurrent);
        options.max_concurrent_compactions =
            options.max_concurrent_compactions.clamp(1, concurrent);

        let option = |name: &str| {
            options
                .scheduler_options
                .get(name)
                .and_then(|value| value.parse::<usize>().ok())
        };
        let max_sources = max_sources.max(2);
        let max = option("max_compaction_sources").map_or(max_sources, |max| max.min(max_sources));
        let min = option("min_compaction_sources").map(|min| min.min(max));
        options
            .scheduler_options
            .insert("max_compaction_sources".to_owned(), max.to_string());
        if let Some(min) = min {
            options
                .scheduler_options
                .insert("min_compaction_sources".to_owned(), min.to_string());
        }
    }

    /// A share of the budget for one scheduler, released when it drops.
    pub(super) fn holder(&self) -> Holder {
        Holder {
            memory: self.clone(),
            id: self.0.next_scheduler.fetch_add(1, Ordering::Relaxed),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, u64>> {
        self.0
            .held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn report(&self, held: &HashMap<u64, u64>) {
        ::metrics::gauge!("storage_compaction_memory_held_bytes")
            .set(held.values().sum::<u64>() as f64);
    }
}

/// One scheduler's share of a [`CompactionMemory`].
#[derive(Debug)]
pub(super) struct Holder {
    memory: CompactionMemory,
    id: u64,
}

impl Holder {
    pub(super) fn budget(&self) -> u64 {
        self.memory.budget()
    }

    /// What the other schedulers sharing the budget hold.
    pub(super) fn others(&self) -> u64 {
        self.memory
            .lock()
            .iter()
            .filter(|(id, _)| **id != self.id)
            .map(|(_, bytes)| *bytes)
            .sum()
    }

    pub(super) fn set(&self, bytes: u64) {
        let mut held = self.memory.lock();
        held.insert(self.id, bytes);
        self.memory.report(&held);
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        let mut held = self.memory.lock();
        held.remove(&self.id);
        self.memory.report(&held);
    }
}

/// The estimated bytes compactions hold under one compactor's options.
#[derive(Clone, Copy, Debug)]
pub(super) struct Cost {
    per_source: u64,
    subcompactions: u64,
}

impl Cost {
    /// `None` when compactions run in another process.
    pub(super) fn new(options: &CompactorOptions) -> Option<Self> {
        let worker = options.worker.as_ref()?;
        Some(Self {
            per_source: Self::per_source(worker),
            subcompactions: worker.max_subcompactions.max(1) as u64,
        })
    }

    fn per_source(worker: &CompactionWorkerOptions) -> u64 {
        let fetch = (worker.bytes_to_fetch as u64)
            .max(1)
            .div_ceil(MAX_BLOCK_BYTES)
            * MAX_BLOCK_BYTES;
        worker.max_fetch_tasks.max(1) as u64 * fetch
    }

    /// What `spec` holds while it runs, split into `subcompactions` when the
    /// worker has planned them; drains read nothing.
    pub(super) fn of(&self, spec: &CompactionSpec, subcompactions: Option<usize>) -> u64 {
        if spec.is_drain() {
            return 0;
        }
        let subcompactions = subcompactions
            .filter(|planned| *planned > 0)
            .map_or(self.subcompactions, |planned| planned as u64);
        let sources = spec.sources().len() as u64;
        subcompactions * (sources * self.per_source + OUTPUT_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use slatedb::compactor::SourceId;

    use super::*;

    fn options() -> CompactorOptions {
        CompactorOptions::default()
    }

    #[test]
    fn a_large_budget_keeps_slatedb_defaults_and_caps_sources() {
        let mut bounded = options();
        CompactionMemory::new(64 * 1024 * MIB).bound(&mut bounded, 8);
        let (worker, defaults) = (bounded.worker.clone().unwrap(), options().worker.unwrap());
        assert_eq!(worker.max_subcompactions, defaults.max_subcompactions);
        assert_eq!(
            worker.max_concurrent_compactions,
            defaults.max_concurrent_compactions
        );
        assert_eq!(worker.bytes_to_fetch, defaults.bytes_to_fetch);
        assert_eq!(bounded.max_concurrent_compactions, 4);
        assert_eq!(bounded.scheduler_options["max_compaction_sources"], "8");
    }

    #[test]
    fn the_default_budget_fits_one_compaction_of_eight_sources() {
        let mut bounded = options();
        let memory = CompactionMemory::default();
        memory.bound(&mut bounded, 8);
        let cost = Cost::new(&bounded).unwrap();
        let sources = (0..8).map(SourceId::SortedRun).collect::<Vec<_>>();
        let spec = CompactionSpec::new(sources, 0);
        assert!(cost.of(&spec, None) <= memory.budget());
        assert_eq!(bounded.max_concurrent_compactions, 1);
        assert_eq!(bounded.worker.unwrap().max_subcompactions, 4);
    }

    #[test]
    fn small_budgets_shrink_prefetch_then_subcompactions() {
        let mut bounded = options();
        CompactionMemory::new(128 * MIB).bound(&mut bounded, 8);
        let worker = bounded.worker.clone().unwrap();
        assert_eq!(worker.max_subcompactions, 1);
        assert_eq!(worker.max_concurrent_compactions, 1);
        assert!(8 * Cost::per_source(&worker) + OUTPUT_BYTES <= 128 * MIB);
        // Below one subcompaction's floor, prefetch stops at one block.
        let mut tiny = options();
        CompactionMemory::new(MIB).bound(&mut tiny, 8);
        let worker = tiny.worker.unwrap();
        assert_eq!(
            (worker.max_fetch_tasks, worker.bytes_to_fetch as u64),
            (1, MAX_BLOCK_BYTES)
        );
    }

    #[test]
    fn keeps_lower_settings_and_a_lower_minimum_of_sources() {
        let mut bounded = options();
        bounded.max_concurrent_compactions = 1;
        bounded
            .scheduler_options
            .insert("min_compaction_sources".to_owned(), "12".to_owned());
        CompactionMemory::new(64 * 1024 * MIB).bound(&mut bounded, 8);
        assert_eq!(bounded.max_concurrent_compactions, 1);
        assert_eq!(bounded.scheduler_options["min_compaction_sources"], "8");
    }

    #[test]
    fn holders_share_the_budget_until_they_drop() {
        let memory = CompactionMemory::new(100);
        let (first, second) = (memory.holder(), memory.holder());
        first.set(30);
        second.set(50);
        assert_eq!(
            (first.others(), second.others(), memory.held()),
            (50, 30, 80)
        );
        drop(second);
        assert_eq!((first.others(), memory.held()), (0, 30));
    }

    #[test]
    fn drains_hold_nothing_and_planned_subcompactions_replace_the_maximum() {
        let cost = Cost::new(&options()).unwrap();
        let sources = vec![SourceId::SortedRun(1), SourceId::SortedRun(2)];
        let merge = CompactionSpec::new(sources.clone(), 1);
        assert_eq!(cost.of(&merge, Some(1)) * 4, cost.of(&merge, None));
        let drain = CompactionSpec::drain_segment(bytes::Bytes::from_static(b"s"), sources);
        assert_eq!(cost.of(&drain, None), 0);
    }
}
