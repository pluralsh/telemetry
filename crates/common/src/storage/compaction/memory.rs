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
//! however small its inputs are. SlateDB splits a compaction into ranges of
//! at least its largest input SST, up to `max_subcompactions` of them, and
//! every range reads every source; it bounds neither
//! the sources of a compaction nor how many run at once by bytes.
//!
//! [`CompactionMemory`] does: [`CompactionMemory::bound`] shrinks a
//! compactor's options until one compaction of its most sources fits the
//! budget, and every database sharing it admits a proposal only while the
//! estimates of the compactions they all run, the proposal's included, fit.
//! A compaction larger than the whole budget runs only when no other does,
//! so the process holds at most `max(budget, largest compaction)`.
//!
//! Writes stall once a segment taking them has too many L0 SSTs, and only
//! compactions of those L0s (relief) clear them. Every other compaction (a
//! merge) takes at most [`CompactionMemory::merge_budget`], half the budget.
//! Subcompactions are capped at what that half fits, so relief always has
//! room for one compaction as wide as any, however much merges hold: with a
//! quarter, a merge of a segment's runs held back the next compaction of its
//! L0s for many seconds at a time.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use slatedb::compactor::CompactionSpec;
use slatedb::config::{CompactionWorkerOptions, CompactorOptions};

use crate::storage::config::DiskPerformance;

const MIB: u64 = 1024 * 1024;

/// Default [`CompactionMemory`] budget: four compactions of eight sources
/// with SlateDB's default prefetch, of one subcompaction each, take about
/// 620 MiB, and its merge share fits one of two subcompactions.
pub const DEFAULT_COMPACTION_MEMORY_BYTES: u64 = 640 * MIB;

/// The share of the budget, as a divisor, that only relief may use.
const RELIEF_DIVISOR: u64 = 2;

/// What one subcompaction's output writer holds at most: object_store's
/// `BufWriter`, which SlateDB builds with its defaults, buffers a 10 MiB
/// part and uploads up to 8 at once. A subcompaction writing less, about its
/// share of the input, holds at most that.
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
    held: Mutex<HashMap<u64, Held>>,
    next_scheduler: AtomicU64,
}

/// Estimated bytes admitted compactions hold: all of them, and merges alone.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Held {
    pub(super) total: u64,
    pub(super) merges: u64,
}

impl Held {
    pub(super) fn add(self, bytes: u64, relief: bool) -> Self {
        Self {
            total: self.total.saturating_add(bytes),
            merges: if relief {
                self.merges
            } else {
                self.merges.saturating_add(bytes)
            },
        }
    }
}

impl std::iter::Sum for Held {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |sum, held| Self {
            total: sum.total.saturating_add(held.total),
            merges: sum.merges.saturating_add(held.merges),
        })
    }
}

impl Default for CompactionMemory {
    fn default() -> Self {
        Self::new(DEFAULT_COMPACTION_MEMORY_BYTES)
    }
}

impl CompactionMemory {
    pub fn new(budget_bytes: u64) -> Self {
        Self(Arc::new(Inner {
            budget: budget_bytes.max(1),
            held: Mutex::default(),
            next_scheduler: AtomicU64::new(0),
        }))
    }

    pub fn budget(&self) -> u64 {
        self.0.budget
    }

    /// What merges may hold together: the budget less the share kept for
    /// relief.
    pub fn merge_budget(&self) -> u64 {
        self.budget() - self.budget() / RELIEF_DIVISOR
    }

    /// Estimated bytes the compactions of every database sharing this budget
    /// hold.
    pub fn held(&self) -> u64 {
        self.lock().values().copied().sum::<Held>().total
    }

    /// [`Self::bound_for`] the cores this process may run on.
    pub fn bound(&self, options: &mut CompactorOptions, max_sources: usize) {
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        self.bound_for(options, max_sources, cores);
    }

    /// Shrinks `options` until a compaction of `max_sources` sources fits the
    /// merge budget: prefetch first, down to one block per read and one read
    /// per source, then subcompactions, which also stay within `cores`, since
    /// more than run at once only add memory. How many compactions run at
    /// once is capped at what the budget fits of compactions of one
    /// subcompaction, which compactions of one input SST's worth of input
    /// are; admission holds larger ones back. Options already below what the
    /// budget allows are kept. Without an embedded worker, compactions run
    /// in another process and nothing changes.
    pub(super) fn bound_for(
        &self,
        options: &mut CompactorOptions,
        max_sources: usize,
        cores: usize,
    ) {
        let Some(worker) = options.worker.as_mut() else {
            return;
        };
        let sources = max_sources.max(1) as u64;
        let (budget, merges) = (self.budget(), self.merge_budget());
        let subcompaction =
            |worker: &CompactionWorkerOptions| sources * Cost::per_source(worker) + OUTPUT_BYTES;
        while subcompaction(worker) > merges {
            if worker.bytes_to_fetch as u64 > MAX_BLOCK_BYTES {
                worker.bytes_to_fetch = (worker.bytes_to_fetch / 2).max(MAX_BLOCK_BYTES as usize);
            } else if worker.max_fetch_tasks > 1 {
                worker.max_fetch_tasks -= 1;
            } else {
                break;
            }
        }
        let subcompaction = subcompaction(worker);
        let subcompactions = usize::try_from(merges / subcompaction)
            .unwrap_or(usize::MAX)
            .min(cores)
            .max(1);
        worker.max_subcompactions = worker.max_subcompactions.clamp(1, subcompactions);
        let concurrent = usize::try_from(budget / subcompaction)
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
    /// Reports the budget, since policies build a default before taking the
    /// process's shared one.
    pub(super) fn holder(&self) -> Holder {
        ::metrics::gauge!("storage_compaction_memory_budget_bytes").set(self.0.budget as f64);
        Holder {
            memory: self.clone(),
            id: self.0.next_scheduler.fetch_add(1, Ordering::Relaxed),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Held>> {
        self.0
            .held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn report(&self, held: &HashMap<u64, Held>) {
        let held: Held = held.values().copied().sum();
        ::metrics::gauge!("storage_compaction_memory_held_bytes", "kind" => "relief")
            .set(held.total.saturating_sub(held.merges) as f64);
        ::metrics::gauge!("storage_compaction_memory_held_bytes", "kind" => "merge")
            .set(held.merges as f64);
    }
}

/// One scheduler's share of a [`CompactionMemory`].
#[derive(Debug)]
pub(super) struct Holder {
    memory: CompactionMemory,
    id: u64,
}

impl Holder {
    pub(super) fn memory(&self) -> &CompactionMemory {
        &self.memory
    }

    /// What the other schedulers sharing the budget hold.
    pub(super) fn others(&self) -> Held {
        self.memory
            .lock()
            .iter()
            .filter(|(id, _)| **id != self.id)
            .map(|(_, held)| *held)
            .sum()
    }

    pub(super) fn set(&self, held: Held) {
        let mut all = self.memory.lock();
        all.insert(self.id, held);
        self.memory.report(&all);
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        let mut held = self.memory.lock();
        held.remove(&self.id);
        self.memory.report(&held);
    }
}

/// The estimated sizes of a compaction's input SSTs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Inputs {
    pub(super) total: u64,
    pub(super) largest: u64,
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

    /// What `spec` holds while it runs, split into the `planned`
    /// subcompactions once the worker has planned them, before that into as
    /// many as SlateDB plans for `inputs`, a range per largest input SST of
    /// input, and into the most when its inputs aren't known; drains read
    /// nothing. Each holds its prefetch and its output, up to its share of
    /// the inputs.
    pub(super) fn of(
        &self,
        spec: &CompactionSpec,
        planned: Option<usize>,
        inputs: Option<Inputs>,
    ) -> u64 {
        if spec.is_drain() {
            return 0;
        }
        let subcompactions = match (planned.filter(|planned| *planned > 0), inputs) {
            (Some(planned), _) => planned as u64,
            (None, Some(inputs)) => inputs
                .total
                .div_ceil(inputs.largest.max(1))
                .clamp(1, self.subcompactions),
            (None, None) => self.subcompactions,
        };
        let output = inputs.map_or(OUTPUT_BYTES, |inputs| {
            inputs.total.div_ceil(subcompactions).min(OUTPUT_BYTES)
        });
        let sources = spec.sources().len() as u64;
        subcompactions * (sources * self.per_source + output)
    }
}

#[cfg(test)]
mod tests {
    use slatedb::compactor::SourceId;

    use super::*;

    fn options() -> CompactorOptions {
        CompactorOptions::default()
    }

    const CORES: usize = 16;

    #[test]
    fn a_large_budget_keeps_slatedb_defaults_and_caps_sources() {
        let mut bounded = options();
        CompactionMemory::new(64 * 1024 * MIB).bound_for(&mut bounded, 8, CORES);
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
    fn the_default_budget_fits_a_merge_of_eight_sources_beside_relief() {
        let mut bounded = options();
        let memory = CompactionMemory::default();
        memory.bound_for(&mut bounded, 8, CORES);
        let cost = Cost::new(&bounded).unwrap();
        let sources = (0..8).map(SourceId::SortedRun).collect::<Vec<_>>();
        let spec = CompactionSpec::new(sources, 0);
        // The widest merge fits the merge share, and the widest relief what
        // merges leave.
        assert!(cost.of(&spec, None, None) <= memory.merge_budget());
        assert!(memory.budget() - memory.merge_budget() >= cost.of(&spec, None, None));
        assert_eq!(bounded.worker.unwrap().max_subcompactions, 2);
        // Compactions of one subcompaction each, four of them fit.
        assert!(4 * cost.of(&spec, Some(1), None) <= memory.budget());
        assert_eq!(bounded.max_concurrent_compactions, 4);
    }

    #[test]
    fn subcompactions_stay_within_the_cores() {
        let (mut one, mut two) = (options(), options());
        let memory = CompactionMemory::new(64 * 1024 * MIB);
        memory.bound_for(&mut one, 8, 1);
        memory.bound_for(&mut two, 8, 2);
        assert_eq!(one.worker.unwrap().max_subcompactions, 1);
        assert_eq!(two.worker.as_ref().unwrap().max_subcompactions, 2);
        // Compactions still run beside each other on few cores.
        assert_eq!(two.max_concurrent_compactions, 4);
    }

    #[test]
    fn small_budgets_shrink_prefetch_then_subcompactions() {
        let mut bounded = options();
        CompactionMemory::new(128 * MIB).bound_for(&mut bounded, 8, CORES);
        let worker = bounded.worker.clone().unwrap();
        assert_eq!(worker.max_subcompactions, 1);
        assert_eq!(worker.max_concurrent_compactions, 1);
        assert!(8 * Cost::per_source(&worker) + OUTPUT_BYTES <= 96 * MIB);
        // Below one subcompaction's floor, prefetch stops at one block.
        let mut tiny = options();
        CompactionMemory::new(MIB).bound_for(&mut tiny, 8, CORES);
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
        CompactionMemory::new(64 * 1024 * MIB).bound_for(&mut bounded, 8, CORES);
        assert_eq!(bounded.max_concurrent_compactions, 1);
        assert_eq!(bounded.scheduler_options["min_compaction_sources"], "8");
    }

    #[test]
    fn holders_share_the_budget_until_they_drop() {
        let memory = CompactionMemory::new(100);
        let (first, second) = (memory.holder(), memory.holder());
        let held = |total, merges| Held { total, merges };
        first.set(held(30, 10));
        second.set(held(50, 50));
        assert_eq!(
            (first.others(), second.others(), memory.held()),
            (held(50, 50), held(30, 10), 80)
        );
        drop(second);
        assert_eq!((first.others(), memory.held()), (held(0, 0), 30));
        assert_eq!(memory.merge_budget(), 50);
    }

    #[test]
    fn drains_hold_nothing_and_planned_subcompactions_replace_the_maximum() {
        let cost = Cost::new(&options()).unwrap();
        let sources = vec![SourceId::SortedRun(1), SourceId::SortedRun(2)];
        let merge = CompactionSpec::new(sources.clone(), 1);
        assert_eq!(
            cost.of(&merge, Some(1), None) * 4,
            cost.of(&merge, None, None)
        );
        let drain = CompactionSpec::drain_segment(bytes::Bytes::from_static(b"s"), sources);
        assert_eq!(cost.of(&drain, None, None), 0);
    }

    #[test]
    fn unplanned_compactions_take_a_subcompaction_per_largest_input_sst_of_input() {
        let cost = Cost::new(&options()).unwrap();
        let merge = CompactionSpec::new(vec![SourceId::SortedRun(1), SourceId::SortedRun(2)], 1);
        let one = cost.of(&merge, Some(1), None);
        let prefetch = one - OUTPUT_BYTES;
        let inputs = |total, largest| Some(Inputs { total, largest });
        assert_eq!(cost.of(&merge, None, Some(Inputs::default())), prefetch);
        assert_eq!(
            cost.of(&merge, None, inputs(64 * MIB, 64 * MIB)),
            prefetch + 64 * MIB
        );
        // Four L0 SSTs of 64 MiB split four ways however small they are
        // beside `max_sst_size`, each writing about one SST's worth.
        assert_eq!(
            cost.of(&merge, None, inputs(256 * MIB, 64 * MIB)),
            4 * (prefetch + 64 * MIB)
        );
        assert_eq!(
            cost.of(&merge, None, inputs(100 * 64 * MIB, 64 * MIB)),
            4 * one
        );
        // Planned subcompactions win over the input estimate.
        assert_eq!(
            cost.of(&merge, Some(3), inputs(3 * 64 * MIB, 64 * MIB)),
            3 * (prefetch + 64 * MIB)
        );
    }
}
