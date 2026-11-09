//! Turns ingest away before SlateDB stalls it on L0 or holds it on
//! unflushed memtables.
//!
//! SlateDB holds a memtable's flush once a segment it touches has
//! `l0_max_ssts` L0 SSTs, and every write then waits until compaction
//! drains them: clients see a stall and then a wave of timeouts, and the
//! backlog grows while they retry. [`Admission`] follows the deepest
//! segment's L0 and turns away a growing share of ingest as it nears the
//! limit, so clients back off while flushes still go through. Writes it
//! already took still reach L0 after it stops taking more: up to
//! `max_unflushed_bytes` of memtables and the uploads in flight, so it stops
//! that many SSTs short of the limit.
//!
//! SlateDB also holds every write once its unflushed WAL and memtables reach
//! `max_unflushed_bytes`, which happens when L0 uploads fall behind ingest,
//! such as on a writer out of CPU. It holds for seconds, so ingest waiting
//! on it times out in bursts. [`Admission`] sheds a growing share of ingest
//! from five eighths of `max_unflushed_bytes` too, whichever of the two is
//! deeper, so those writers turn some ingest away instead. Rollup copies are
//! never turned away, but before each copy buffer they wait while any ingest
//! is shed ([`Admission::yield_to_ingest`]), so a rollup slows down rather
//! than take L0 and memtable room from ingest.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use slatedb::DbStatus;
use slatedb::config::Settings;
use tokio::sync::watch;

/// Ingest a writer turned away to keep L0 short of the stall limit.
pub const ADMISSION_REJECTIONS: &str = "storage_admission_rejections_total";
/// Milliseconds rollup copies waited for ingest to stop being shed.
pub const COPY_YIELD_MILLISECONDS: &str = "storage_admission_copy_yield_milliseconds_total";
/// Longest a rollup copy waits per copy buffer for ingest to stop being
/// shed. A writer shedding all along still copies a buffer this often, so
/// a 4 GiB rollup through 64 MiB buffers waits at most about 11 minutes.
pub const COPY_YIELD_WAIT: Duration = Duration::from_secs(10);
const YIELD_POLL: Duration = Duration::from_millis(50);
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
    /// SlateDB's `max_unflushed_bytes`; zero sheds nothing on it.
    pub max_unflushed_bytes: usize,
}

impl L0Limit {
    pub fn of(settings: &Settings) -> Self {
        Self {
            max_ssts: settings.l0_max_ssts,
            backlog: settings
                .max_unflushed_bytes
                .div_ceil(settings.l0_sst_size_bytes.max(1))
                + settings.l0_flush_parallelism,
            max_unflushed_bytes: settings.max_unflushed_bytes,
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
    max_unflushed: AtomicUsize,
    /// SlateDB's unflushed bytes, once the database reports them.
    unflushed: OnceLock<Arc<AtomicU64>>,
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
                max_unflushed: AtomicUsize::new(0),
                unflushed: OnceLock::new(),
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
        self.inner
            .max_unflushed
            .store(limit.max_unflushed_bytes, Ordering::Relaxed);
    }

    /// Sheds against `unflushed` as well, which the database's metrics
    /// recorder keeps; see
    /// [`MetricsRsRecorder::tracking_unflushed`](super::metrics_recorder::MetricsRsRecorder::tracking_unflushed).
    pub fn track_unflushed(&self, unflushed: Arc<AtomicU64>) {
        let _ = self.inner.unflushed.set(unflushed);
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

    /// Whether to take on an ingest write now: the deeper of the L0 and the
    /// unflushed shares decides. Below five eighths of the L0 limit every
    /// write is admitted; from there each further L0 SST turns away an equal
    /// further share, all of it once the backlog would fill L0 to the limit,
    /// or one SST short of it at least. Likewise each further eighth of
    /// `max_unflushed_bytes` past five turns away a further quarter.
    pub fn admits(&self) -> bool {
        let (shed, span) = match (self.l0_share(), self.unflushed_share()) {
            (Some(l0), Some(unflushed)) => {
                if l0.0 * unflushed.1 >= unflushed.0 * l0.1 {
                    l0
                } else {
                    unflushed
                }
            }
            (Some(share), None) | (None, Some(share)) => share,
            (None, None) => return true,
        };
        let admitted = self.inner.turns.fetch_add(1, Ordering::Relaxed) % span >= shed;
        if !admitted {
            metrics::counter!(ADMISSION_REJECTIONS).increment(1);
        }
        admitted
    }

    /// Whether any ingest is being turned away now.
    pub fn sheds(&self) -> bool {
        self.l0_share().is_some() || self.unflushed_share().is_some()
    }

    /// Waits while ingest is being shed, for at most `wait`, so a rollup
    /// copy leaves L0 and the memtables to ingest; past `wait` it goes
    /// ahead so rollups still finish on a writer that sheds all along.
    pub async fn yield_to_ingest(&self, wait: Duration) {
        if !self.sheds() {
            return;
        }
        let started = tokio::time::Instant::now();
        while self.sheds() && started.elapsed() < wait {
            tokio::time::sleep(YIELD_POLL.min(wait)).await;
        }
        let waited = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        metrics::counter!(COPY_YIELD_MILLISECONDS).increment(waited);
    }

    /// `(shed, of)` ingest writes to turn away for L0 depth, if any.
    fn l0_share(&self) -> Option<(u64, u64)> {
        let limit = self.inner.max_ssts.load(Ordering::Relaxed);
        let backlog = self.inner.backlog.load(Ordering::Relaxed);
        let depth = self.inner.depth.load(Ordering::Relaxed);
        // A limit below two stalls on any L0, which shedding cannot avoid.
        if limit < 2 {
            return None;
        }
        let last = limit.saturating_sub(backlog.max(1)).max(1);
        let from = (limit * 5).div_ceil(8).min(last);
        (depth >= from).then(|| {
            (
                (depth.min(last) - from + 1) as u64,
                (last - from + 1) as u64,
            )
        })
    }

    /// `(shed, of)` ingest writes to turn away for unflushed bytes, if any.
    fn unflushed_share(&self) -> Option<(u64, u64)> {
        let max = self.inner.max_unflushed.load(Ordering::Relaxed) as u64;
        let unflushed = self.inner.unflushed.get()?.load(Ordering::Relaxed);
        if max == 0 {
            return None;
        }
        let eighths = unflushed.saturating_mul(8) / max;
        (eighths >= 5).then(|| ((eighths - 4).min(4), 4))
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
            max_unflushed_bytes: 0,
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
            max_unflushed_bytes: 0,
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
                backlog: 3,
                max_unflushed_bytes: 128 << 20,
            }
        );
    }

    #[test]
    fn sheds_a_further_quarter_per_eighth_of_unflushed_bytes_past_five() {
        let admission = Admission::unlimited();
        admission.limit_to(L0Limit {
            max_ssts: 0,
            backlog: 0,
            max_unflushed_bytes: 800,
        });
        let unflushed = Arc::new(AtomicU64::new(0));
        admission.track_unflushed(Arc::clone(&unflushed));
        for (bytes, expected) in [
            (499, 300),
            (500, 225),
            (600, 150),
            (700, 75),
            (800, 0),
            (900, 0),
        ] {
            unflushed.store(bytes, Ordering::Relaxed);
            assert_eq!(admitted(&admission, 0), expected, "{bytes} unflushed");
        }
    }

    #[test]
    fn the_deeper_of_l0_and_unflushed_bytes_sheds() {
        let admission = Admission::unlimited();
        admission.limit_to(L0Limit {
            max_ssts: 16,
            backlog: 3,
            max_unflushed_bytes: 800,
        });
        let unflushed = Arc::new(AtomicU64::new(600));
        admission.track_unflushed(Arc::clone(&unflushed));
        assert_eq!(admitted(&admission, 10), 150);
        assert_eq!(admitted(&admission, 12), 75);
        unflushed.store(0, Ordering::Relaxed);
        assert_eq!(admitted(&admission, 0), 300);
    }

    #[tokio::test(start_paused = true)]
    async fn copies_wait_while_ingest_is_shed_then_go_ahead() {
        let admission = Admission::unlimited();
        admission.limit_to(L0Limit {
            max_ssts: 16,
            backlog: 3,
            max_unflushed_bytes: 0,
        });
        admission.set_depth(9);
        assert!(!admission.sheds());
        let started = tokio::time::Instant::now();
        admission.yield_to_ingest(Duration::from_secs(10)).await;
        assert_eq!(started.elapsed(), Duration::ZERO);

        admission.set_depth(10);
        assert!(admission.sheds());
        admission.yield_to_ingest(Duration::from_secs(10)).await;
        assert!(started.elapsed() >= Duration::from_secs(10));

        let started = tokio::time::Instant::now();
        let waiting = tokio::spawn({
            let admission = admission.clone();
            async move { admission.yield_to_ingest(Duration::from_secs(10)).await }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        admission.set_depth(4);
        waiting.await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn a_tiny_limit_turns_everything_away_one_short_of_it() {
        let admission = Admission::unlimited();
        admission.limit_to(L0Limit {
            max_ssts: 2,
            backlog: 0,
            max_unflushed_bytes: 0,
        });
        assert_eq!(admitted(&admission, 0), 300);
        assert_eq!(admitted(&admission, 1), 0);
    }
}
