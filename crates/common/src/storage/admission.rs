//! Turns ingest away before SlateDB stalls it on L0.
//!
//! SlateDB holds a memtable's flush once a segment it touches has
//! `l0_max_ssts` L0 SSTs, and every write then waits until compaction
//! drains them: clients see a stall and then a wave of timeouts, and the
//! backlog grows while they retry. [`Admission`] follows the deepest
//! segment's L0 and turns away a growing share of ingest as it nears the
//! limit, so clients back off while flushes still go through. Writes it
//! already took still reach L0 after it stops taking more: up to
//! `max_unflushed_bytes` of memtables and the uploads in flight, so it stops
//! that many SSTs short of the limit. Rollup copies are not asked: they
//! already wait for room in their half of the write budget.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use slatedb::DbStatus;
use slatedb::config::Settings;
use tokio::sync::watch;

/// Ingest a writer turned away to keep L0 short of the stall limit.
pub const ADMISSION_REJECTIONS: &str = "storage_admission_rejections_total";
/// L0 SSTs in a writer's deepest segment.
pub const SEGMENT_L0_DEPTH: &str = "storage_segment_l0_depth";

/// The L0 a writer sheds ingest against.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct L0Limit {
    /// SlateDB's `l0_max_ssts`; below two nothing is shed.
    pub max_ssts: usize,
    /// L0 SSTs that writes already taken can still add: unflushed
    /// memtables and uploads in flight.
    pub backlog: usize,
}

impl L0Limit {
    pub fn of(settings: &Settings) -> Self {
        Self {
            max_ssts: settings.l0_max_ssts,
            backlog: settings
                .max_unflushed_bytes
                .div_ceil(settings.l0_sst_size_bytes.max(1))
                + settings.l0_flush_parallelism,
        }
    }
}

/// A writer's ingest admission; clones share it.
#[derive(Clone, Debug)]
pub struct Admission {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    depth: AtomicUsize,
    max_ssts: AtomicUsize,
    backlog: AtomicUsize,
    turns: AtomicU64,
}

impl Default for Admission {
    fn default() -> Self {
        Self::unlimited()
    }
}

impl Admission {
    /// Admits every write, for storage that never stalls on L0.
    pub fn unlimited() -> Self {
        Self {
            inner: Arc::new(Inner {
                depth: AtomicUsize::new(0),
                max_ssts: AtomicUsize::new(0),
                backlog: AtomicUsize::new(0),
                turns: AtomicU64::new(0),
            }),
        }
    }

    /// Sheds ingest against `limit` for the database behind `status`,
    /// following it until the database is dropped.
    pub fn follow(mut status: watch::Receiver<DbStatus>, limit: L0Limit) -> Self {
        let admission = Self::unlimited();
        admission.limit_to(limit);
        admission.observe(&status.borrow_and_update());
        tokio::spawn({
            let admission = admission.clone();
            async move {
                while status.changed().await.is_ok() {
                    admission.observe(&status.borrow_and_update());
                }
            }
        });
        admission
    }

    pub fn limit_to(&self, limit: L0Limit) {
        self.inner.max_ssts.store(limit.max_ssts, Ordering::Relaxed);
        self.inner.backlog.store(limit.backlog, Ordering::Relaxed);
    }

    /// Follows `status`'s manifest: the deepest segment's L0 count.
    pub fn observe(&self, status: &DbStatus) {
        let manifest = &status.current_manifest;
        let depth = manifest
            .segments()
            .iter()
            .map(|segment| segment.l0().len())
            .chain(std::iter::once(manifest.l0().len()))
            .max()
            .unwrap_or(0);
        self.inner.depth.store(depth, Ordering::Relaxed);
        metrics::gauge!(SEGMENT_L0_DEPTH).set(depth as f64);
    }

    /// Whether to take on an ingest write now. Below five eighths of the
    /// limit every write is admitted; from there each further L0 SST turns
    /// away an equal further share, all of it once the backlog would fill
    /// L0 to the limit, or one SST short of it at least.
    pub fn admits(&self) -> bool {
        let limit = self.inner.max_ssts.load(Ordering::Relaxed);
        let backlog = self.inner.backlog.load(Ordering::Relaxed);
        let depth = self.inner.depth.load(Ordering::Relaxed);
        // A limit below two stalls on any L0, which shedding cannot avoid.
        if limit < 2 {
            return true;
        }
        let last = limit.saturating_sub(backlog.max(1)).max(1);
        let from = (limit * 5).div_ceil(8).min(last);
        if depth < from {
            return true;
        }
        let span = (last - from + 1) as u64;
        let shed = (depth.min(last) - from + 1) as u64;
        let admitted = self.inner.turns.fetch_add(1, Ordering::Relaxed) % span >= shed;
        if !admitted {
            metrics::counter!(ADMISSION_REJECTIONS).increment(1);
        }
        admitted
    }

    #[cfg(test)]
    fn set_depth(&self, depth: usize) {
        self.inner.depth.store(depth, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admitted(admission: &Admission, depth: usize) -> usize {
        admission.set_depth(depth);
        (0..300).filter(|_| admission.admits()).count()
    }

    #[test]
    fn sheds_a_growing_share_as_l0_nears_the_limit() {
        let admission = Admission::unlimited();
        assert_eq!(admitted(&admission, 100), 300);
        admission.limit_to(L0Limit {
            max_ssts: 16,
            backlog: 3,
        });
        assert_eq!(admitted(&admission, 9), 300);
        assert_eq!(admitted(&admission, 10), 225);
        assert_eq!(admitted(&admission, 11), 150);
        assert_eq!(admitted(&admission, 12), 75);
        assert_eq!(admitted(&admission, 13), 0);
        assert_eq!(admitted(&admission, 20), 0);
    }

    #[test]
    fn stops_where_the_backlog_would_fill_l0() {
        let admission = Admission::unlimited();
        admission.limit_to(L0Limit {
            max_ssts: 8,
            backlog: 3,
        });
        assert_eq!(admitted(&admission, 4), 300);
        assert_eq!(admitted(&admission, 5), 0);
    }

    #[test]
    fn backlog_is_the_unflushed_memtables_and_uploads_in_flight() {
        let settings = Settings {
            max_unflushed_bytes: 128 << 20,
            l0_sst_size_bytes: 64 << 20,
            l0_flush_parallelism: 1,
            l0_max_ssts: 8,
            ..Settings::default()
        };
        assert_eq!(
            L0Limit::of(&settings),
            L0Limit {
                max_ssts: 8,
                backlog: 3
            }
        );
    }

    #[test]
    fn a_tiny_limit_turns_everything_away_one_short_of_it() {
        let admission = Admission::unlimited();
        admission.limit_to(L0Limit {
            max_ssts: 2,
            backlog: 0,
        });
        assert_eq!(admitted(&admission, 0), 300);
        assert_eq!(admitted(&admission, 1), 0);
    }
}
