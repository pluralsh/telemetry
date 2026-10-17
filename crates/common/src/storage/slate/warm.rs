//! Block-cache warming over SlateDB SSTs, shared by every product.
//!
//! [`warm_ssts`] warms a list of SSTs as given. [`warm_planned`] warms a
//! [`WarmPlan`] newest first into a byte budget of the metadata cache,
//! keeping a [`WarmLedger`] of what it warmed so it stops at the budget
//! instead of evicting its own work, and evicts what compaction removed.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use slatedb::manifest::{SsTableId, SsTableInfo};
use slatedb::{CacheTarget, DbCacheManagerOps};
use tokio_util::sync::CancellationToken;

use crate::storage::factory::SharedDbCache;
use crate::storage::metered_cache::{ENTRY_OVERHEAD_BYTES, attributing};

/// SSTs already handed to the block cache, so periodic passes warm only files
/// that appeared since the previous pass.
///
/// One tracker may span every shard of a process: compacted SST ids are
/// ULIDs. A pass that finishes drops every id it did not see, which covers
/// both SSTs compaction removed and SSTs whose bucket left the warm window.
#[derive(Debug, Default)]
pub struct SstWarmTracker {
    state: Mutex<TrackerState>,
    armed: AtomicBool,
}

#[derive(Debug, Default)]
struct TrackerState {
    seen: HashSet<SsTableId>,
    pass: HashSet<SsTableId>,
}

impl SstWarmTracker {
    /// An unarmed tracker records the live SSTs of its first pass without
    /// warming them, so enabling continuous warming never starts with a bulk
    /// warm of the whole window; an armed one warms everything it has not seen.
    pub fn new(armed: bool) -> Self {
        Self {
            state: Mutex::default(),
            armed: AtomicBool::new(armed),
        }
    }

    /// Ends a completed pass: forgets SSTs it did not see and arms the tracker.
    pub fn finish_pass(&self) {
        let mut state = self.lock();
        let pass = std::mem::take(&mut state.pass);
        state.seen.retain(|id| pass.contains(id));
        self.armed.store(true, Ordering::Release);
    }

    /// Ends a failed or cancelled pass without forgetting SSTs it never reached.
    pub fn abandon_pass(&self) {
        self.lock().pass.clear();
    }

    /// Records `id` as live in this pass; true when it should be warmed now.
    fn claim(&self, id: SsTableId) -> bool {
        let mut state = self.lock();
        state.pass.insert(id);
        state.seen.insert(id) && self.armed.load(Ordering::Acquire)
    }

    fn forget(&self, id: SsTableId) {
        self.lock().seen.remove(&id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TrackerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Warms `work` with up to `concurrency` SSTs in flight, skipping SSTs that
/// `tracker` has already warmed. SSTs are claimed only when their warm
/// starts, so a cancelled pass leaves the rest for the next one, and a failed
/// warm is forgotten so the next pass retries it.
pub async fn warm_ssts<D>(
    db: &D,
    product: &'static str,
    work: Vec<(SsTableId, Arc<[CacheTarget]>)>,
    include_payloads: bool,
    concurrency: usize,
    cancel: &CancellationToken,
    tracker: Option<&SstWarmTracker>,
) -> Result<(), slatedb::Error>
where
    D: DbCacheManagerOps + Sync + ?Sized,
{
    futures::stream::iter(work)
        .filter(|(id, _)| std::future::ready(tracker.is_none_or(|tracker| tracker.claim(*id))))
        .map(|(id, targets)| async move {
            let result = db.warm_sst(id, &targets).await;
            if result.is_err()
                && let Some(tracker) = tracker
            {
                tracker.forget(id);
            }
            metrics::counter!(
                "telemetry_cache_warmer_ssts_total",
                "product" => product,
                "status" => if result.is_ok() { "success" } else { "error" },
                "payloads" => if include_payloads { "included" } else { "excluded" }
            )
            .increment(1);
            result
        })
        .buffer_unordered(concurrency.max(1))
        .take_until(cancel.cancelled())
        .try_collect::<Vec<()>>()
        .await?;
    Ok(())
}

/// A database a [`WarmPlan`] warms, as a [`WarmLedger`] keys it.
pub type WarmDb = Arc<dyn DbCacheManagerOps + Send + Sync>;

/// Identifies a database in a process while it is open.
pub fn db_key<T: ?Sized>(db: &Arc<T>) -> usize {
    Arc::as_ptr(db).cast::<()>() as usize
}

/// What an SST's filters and index weigh in a metadata cache, from their
/// encoded lengths. A compressed index decodes larger; the factor is a
/// reservation only, replaced by the measured weight once warmed.
pub fn metadata_estimate(info: &SsTableInfo) -> u64 {
    let overhead = ENTRY_OVERHEAD_BYTES as u64;
    let index = if info.compression_codec.is_some() {
        info.index_len.saturating_mul(3)
    } else {
        info.index_len
    };
    let filters = if info.filter_len > 0 {
        info.filter_len + overhead
    } else {
        0
    };
    index + overhead + filters
}

/// [`metadata_estimate`] of an SST's index alone.
pub fn index_estimate(info: &SsTableInfo) -> u64 {
    let filtered = SsTableInfo {
        filter_len: 0,
        ..info.clone()
    };
    metadata_estimate(&filtered)
}

struct PlannedSst {
    db: usize,
    id: SsTableId,
    targets: Arc<[CacheTarget]>,
    recency: i64,
    estimate: u64,
}

/// The SSTs one pass may warm, and every SST each database's manifest
/// still holds, so the pass can tell removed SSTs from ones that merely
/// left the warm window.
#[derive(Default)]
pub struct WarmPlan {
    dbs: HashMap<usize, (WarmDb, HashSet<SsTableId>)>,
    ssts: Vec<PlannedSst>,
}

impl WarmPlan {
    /// Adds `db` with the ids of every SST its manifest holds. Returns its
    /// key for [`Self::push`].
    pub fn add_database(&mut self, db: WarmDb, live: impl IntoIterator<Item = SsTableId>) -> usize {
        let key = db_key(&db);
        self.dbs
            .entry(key)
            .or_insert_with(|| (db, HashSet::new()))
            .1
            .extend(live);
        key
    }

    /// Plans warming `targets` of SST `id` of database `db`. `recency` (any
    /// unit, larger is newer, comparable across passes) orders the pass.
    pub fn push(
        &mut self,
        db: usize,
        id: SsTableId,
        targets: Arc<[CacheTarget]>,
        recency: i64,
        estimate: u64,
    ) {
        self.ssts.push(PlannedSst {
            db,
            id,
            targets,
            recency,
            estimate,
        });
    }

    pub fn len(&self) -> usize {
        self.ssts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ssts.is_empty()
    }
}

type LedgerKey = (usize, SsTableId);

#[derive(Clone, Copy, Debug)]
struct Warmed {
    bytes: u64,
    recency: i64,
}

/// The SSTs a budgeted warmer has warmed and the metadata bytes they hold.
///
/// It holds only SST ids and byte counts, never cache entries, so it pins
/// nothing: what it forgets stays evictable. A pass forgets SSTs their
/// database no longer holds (and evicts them), SSTs of databases it no
/// longer plans, and the oldest SSTs when a newer one needs their room.
#[derive(Debug, Default)]
pub struct WarmLedger {
    state: Mutex<LedgerState>,
    armed: AtomicBool,
}

#[derive(Debug, Default)]
struct LedgerState {
    warmed: HashMap<LedgerKey, Warmed>,
    /// SSTs an unarmed pass recorded without warming.
    ignored: HashSet<LedgerKey>,
    bytes: u64,
    reserved: u64,
}

impl LedgerState {
    fn forget(&mut self, key: &LedgerKey) -> bool {
        match self.warmed.remove(key) {
            Some(warmed) => {
                self.bytes -= warmed.bytes;
                true
            }
            None => false,
        }
    }
}

enum Reservation {
    Fits,
    /// Fits after forgetting this many older SSTs.
    Displaced(usize),
    Full,
}

impl WarmLedger {
    /// An unarmed ledger records its first pass's SSTs without warming
    /// them, as [`SstWarmTracker::new`] does.
    pub fn new(armed: bool) -> Self {
        Self {
            state: Mutex::default(),
            armed: AtomicBool::new(armed),
        }
    }

    /// Metadata bytes held by the SSTs it warmed.
    pub fn bytes(&self) -> u64 {
        self.lock().bytes
    }

    /// SSTs it warmed and still counts.
    pub fn ssts(&self) -> usize {
        self.lock().warmed.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LedgerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Forgets what `plan` shows is gone. Returns the SSTs to evict: warmed
    /// ones their database's manifest no longer holds.
    fn reap(&self, plan: &WarmPlan) -> Vec<LedgerKey> {
        let in_pass: HashSet<LedgerKey> = plan.ssts.iter().map(|sst| (sst.db, sst.id)).collect();
        let mut state = self.lock();
        let live = |key: &LedgerKey| plan.dbs.get(&key.0).map(|(_, live)| live.contains(&key.1));
        let removed: Vec<LedgerKey> = state
            .warmed
            .keys()
            .filter(|key| live(key) != Some(true))
            .copied()
            .collect();
        for key in &removed {
            state.forget(key);
        }
        state.ignored.retain(|key| in_pass.contains(key));
        removed
            .into_iter()
            .filter(|key| live(key) == Some(false))
            .collect()
    }

    fn known(&self, key: &LedgerKey) -> bool {
        let state = self.lock();
        state.warmed.contains_key(key) || state.ignored.contains(key)
    }

    fn ignore(&self, keys: impl IntoIterator<Item = LedgerKey>) {
        self.lock().ignored.extend(keys);
    }

    fn reserve(&self, sst: &PlannedSst, budget: u64) -> Reservation {
        let mut state = self.lock();
        let fits = |state: &LedgerState| state.bytes + state.reserved + sst.estimate <= budget;
        if fits(&state) {
            state.reserved += sst.estimate;
            return Reservation::Fits;
        }
        let mut older: Vec<(i64, LedgerKey)> = state
            .warmed
            .iter()
            .filter(|(_, warmed)| warmed.recency < sst.recency)
            .map(|(key, warmed)| (warmed.recency, *key))
            .collect();
        older.sort_unstable_by_key(|(recency, _)| *recency);
        let releasable: u64 = older.iter().map(|(_, key)| state.warmed[key].bytes).sum();
        if state.bytes - releasable + state.reserved + sst.estimate > budget {
            return Reservation::Full;
        }
        let mut displaced = 0;
        for (_, key) in older {
            if fits(&state) {
                break;
            }
            state.forget(&key);
            displaced += 1;
        }
        state.reserved += sst.estimate;
        Reservation::Displaced(displaced)
    }

    /// Releases the reservations of SSTs a cancelled pass dropped.
    fn release(&self, reserved: u64) {
        self.lock().reserved -= reserved;
    }

    fn complete(&self, sst: &PlannedSst, warmed_bytes: Option<u64>) {
        let mut state = self.lock();
        state.reserved -= sst.estimate;
        if let Some(bytes) = warmed_bytes {
            let key = (sst.db, sst.id);
            state.forget(&key);
            state.bytes += bytes;
            state.warmed.insert(
                key,
                Warmed {
                    bytes,
                    recency: sst.recency,
                },
            );
        }
    }
}

/// What one [`warm_planned`] pass did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WarmReport {
    /// SSTs the plan held, warmed or not, and their estimated metadata.
    pub planned_ssts: usize,
    pub planned_bytes: u64,
    pub warmed_ssts: usize,
    pub warmed_bytes: u64,
    pub failed_ssts: usize,
    /// SSTs left unwarmed because the budget was full, newest first from
    /// the first that did not fit.
    pub skipped_ssts: usize,
    pub skipped_bytes: u64,
    /// Older warmed SSTs forgotten to make room for newer ones.
    pub displaced_ssts: usize,
    /// Warmed SSTs compaction removed, evicted from the cache.
    pub evicted_ssts: usize,
}

/// How a budgeted pass charges what it warms.
#[derive(Clone, Copy, Debug)]
pub struct WarmBudget {
    /// Metadata cache bytes the warmer may hold.
    pub bytes: u64,
    /// Whether the metadata cache measures warmed bytes (a metered
    /// in-memory cache); otherwise each SST is charged its estimate.
    pub measured: bool,
}

impl WarmBudget {
    /// No bound, for caches without a metadata cache: SlateDB then caches
    /// no filters or indexes, so warming holds none.
    pub const UNBOUNDED: Self = Self {
        bytes: u64::MAX,
        measured: false,
    };

    /// `percent` of `cache`'s in-memory metadata capacity, or `None` when it
    /// has no metadata cache.
    pub fn of(cache: &SharedDbCache, percent: u8) -> Option<Self> {
        let meta = cache.meta()?;
        Some(Self {
            bytes: meta.capacity_bytes / 100 * u64::from(percent.min(100)),
            measured: meta.usage.is_some(),
        })
    }
}

/// Warms `plan` newest first with up to `concurrency` SSTs in flight,
/// skipping SSTs `ledger` already holds, until the next SST would push the
/// ledger past `budget`: that SST and every older one are skipped and
/// counted, unless forgetting older warmed SSTs makes room. Warmed SSTs
/// their database no longer holds are evicted first.
///
/// Transient memory is bounded by `concurrency` SSTs' filters and indexes
/// in flight. A cancelled pass keeps what it warmed.
#[allow(clippy::too_many_arguments)]
pub async fn warm_planned(
    product: &'static str,
    mut plan: WarmPlan,
    budget: WarmBudget,
    include_payloads: bool,
    concurrency: usize,
    cancel: &CancellationToken,
    ledger: &WarmLedger,
) -> WarmReport {
    let started = Instant::now();
    let concurrency = concurrency.max(1);
    let mut report = WarmReport {
        planned_ssts: plan.ssts.len(),
        planned_bytes: plan.ssts.iter().map(|sst| sst.estimate).sum(),
        ..WarmReport::default()
    };
    let removed = ledger.reap(&plan);
    report.evicted_ssts = evict(product, &plan, removed, concurrency, cancel).await;

    plan.ssts.sort_by_key(|sst| std::cmp::Reverse(sst.recency));
    let ssts: Vec<PlannedSst> = std::mem::take(&mut plan.ssts)
        .into_iter()
        .filter(|sst| !ledger.known(&(sst.db, sst.id)))
        .collect();
    if !ledger.armed.swap(true, Ordering::AcqRel) {
        ledger.ignore(ssts.iter().map(|sst| (sst.db, sst.id)));
        record(product, &report, ledger, budget, started);
        return report;
    }

    let payloads = if include_payloads {
        "included"
    } else {
        "excluded"
    };
    let mut ssts = ssts.into_iter();
    let mut in_flight = FuturesUnordered::new();
    let mut reserved = 0;
    loop {
        while in_flight.len() < concurrency && !cancel.is_cancelled() {
            let Some(sst) = ssts.next() else { break };
            match ledger.reserve(&sst, budget.bytes) {
                Reservation::Fits => {}
                Reservation::Displaced(displaced) => report.displaced_ssts += displaced,
                Reservation::Full => {
                    report.skipped_ssts += 1;
                    report.skipped_bytes += sst.estimate;
                    for sst in ssts.by_ref() {
                        report.skipped_ssts += 1;
                        report.skipped_bytes += sst.estimate;
                    }
                    break;
                }
            }
            reserved += sst.estimate;
            let db = Arc::clone(&plan.dbs[&sst.db].0);
            in_flight.push(async move {
                let bytes = Arc::new(AtomicU64::new(0));
                let result =
                    attributing(Arc::clone(&bytes), db.warm_sst(sst.id, &sst.targets)).await;
                let measured = if budget.measured {
                    bytes.load(Ordering::Relaxed)
                } else {
                    sst.estimate
                };
                (sst, result.map(|()| measured))
            });
        }
        if in_flight.is_empty() {
            break;
        }
        let (sst, result) = tokio::select! {
            () = cancel.cancelled() => break,
            Some(done) = in_flight.next() => done,
        };
        reserved -= sst.estimate;
        let status = match &result {
            Ok(bytes) => {
                report.warmed_ssts += 1;
                report.warmed_bytes += bytes;
                "success"
            }
            Err(error) => {
                tracing::debug!(product, sst = ?sst.id, %error, "warming an SST failed");
                report.failed_ssts += 1;
                "error"
            }
        };
        ledger.complete(&sst, result.ok());
        metrics::counter!(
            "telemetry_cache_warmer_ssts_total",
            "product" => product,
            "status" => status,
            "payloads" => payloads
        )
        .increment(1);
    }
    drop(in_flight);
    ledger.release(reserved);
    record(product, &report, ledger, budget, started);
    report
}

async fn evict(
    product: &'static str,
    plan: &WarmPlan,
    removed: Vec<LedgerKey>,
    concurrency: usize,
    cancel: &CancellationToken,
) -> usize {
    futures::stream::iter(removed)
        .map(|(db, id)| {
            let db = Arc::clone(&plan.dbs[&db].0);
            async move {
                let result = db.evict_cached_sst(id).await;
                metrics::counter!(
                    "telemetry_cache_warmer_evicted_ssts_total",
                    "product" => product,
                    "status" => if result.is_ok() { "success" } else { "error" }
                )
                .increment(1);
                result.is_ok()
            }
        })
        .buffer_unordered(concurrency)
        .take_until(cancel.cancelled())
        .filter(|evicted| std::future::ready(*evicted))
        .count()
        .await
}

fn record(
    product: &'static str,
    report: &WarmReport,
    ledger: &WarmLedger,
    budget: WarmBudget,
    started: Instant,
) {
    metrics::gauge!("telemetry_cache_warmer_budget_bytes", "product" => product)
        .set(budget.bytes as f64);
    metrics::gauge!("telemetry_cache_warmer_warmed_bytes", "product" => product)
        .set(ledger.bytes() as f64);
    metrics::gauge!("telemetry_cache_warmer_warmed_ssts", "product" => product)
        .set(ledger.ssts() as f64);
    metrics::gauge!("telemetry_cache_warmer_planned_bytes", "product" => product)
        .set(report.planned_bytes as f64);
    metrics::gauge!("telemetry_cache_warmer_skipped_ssts", "product" => product, "reason" => "budget")
        .set(report.skipped_ssts as f64);
    metrics::gauge!("telemetry_cache_warmer_skipped_bytes", "product" => product, "reason" => "budget")
        .set(report.skipped_bytes as f64);
    metrics::counter!("telemetry_cache_warmer_displaced_ssts_total", "product" => product)
        .increment(report.displaced_ssts as u64);
    let changed =
        report.warmed_ssts + report.failed_ssts + report.displaced_ssts + report.evicted_ssts;
    if changed == 0 {
        return;
    }
    tracing::info!(
        product,
        planned_ssts = report.planned_ssts,
        planned_bytes = report.planned_bytes,
        warmed_ssts = report.warmed_ssts,
        warmed_bytes = report.warmed_bytes,
        skipped_ssts = report.skipped_ssts,
        skipped_bytes = report.skipped_bytes,
        evicted_ssts = report.evicted_ssts,
        failed_ssts = report.failed_ssts,
        ledger_bytes = ledger.bytes(),
        budget_bytes = budget.bytes,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "metadata warm pass finished"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u64) -> SsTableId {
        SsTableId::Wal(n)
    }

    #[test]
    fn unarmed_tracker_records_first_pass_without_warming() {
        let tracker = SstWarmTracker::new(false);
        assert!(!tracker.claim(id(1)));
        tracker.finish_pass();

        assert!(!tracker.claim(id(1)));
        assert!(tracker.claim(id(2)));
    }

    #[test]
    fn finished_pass_forgets_ssts_it_did_not_see() {
        let tracker = SstWarmTracker::new(true);
        assert!(tracker.claim(id(1)));
        assert!(tracker.claim(id(2)));
        tracker.finish_pass();

        assert!(!tracker.claim(id(2)));
        tracker.finish_pass();

        assert!(tracker.claim(id(1)));
    }

    #[test]
    fn abandoned_pass_keeps_unvisited_ssts() {
        let tracker = SstWarmTracker::new(true);
        assert!(tracker.claim(id(1)));
        assert!(tracker.claim(id(2)));
        tracker.finish_pass();

        assert!(!tracker.claim(id(1)));
        tracker.abandon_pass();

        assert!(!tracker.claim(id(1)));
        assert!(!tracker.claim(id(2)));
    }

    #[test]
    fn forgotten_sst_is_warmed_again() {
        let tracker = SstWarmTracker::new(true);
        assert!(tracker.claim(id(1)));
        tracker.forget(id(1));
        assert!(tracker.claim(id(1)));
    }

    #[derive(Default)]
    struct FakeDb {
        warmed: Mutex<Vec<SsTableId>>,
        evicted: Mutex<Vec<SsTableId>>,
        failing: Mutex<HashSet<SsTableId>>,
    }

    #[async_trait::async_trait]
    impl DbCacheManagerOps for FakeDb {
        async fn warm_sst(&self, id: SsTableId, _: &[CacheTarget]) -> Result<(), slatedb::Error> {
            if self.failing.lock().unwrap().contains(&id) {
                return Err(slatedb::Error::internal("injected".to_owned()));
            }
            self.warmed.lock().unwrap().push(id);
            Ok(())
        }

        async fn evict_cached_sst(&self, id: SsTableId) -> Result<(), slatedb::Error> {
            self.evicted.lock().unwrap().push(id);
            Ok(())
        }
    }

    const ESTIMATE: u64 = 100;

    /// SSTs `ids` of `db`, newer as the id grows, each estimated at
    /// [`ESTIMATE`] bytes; `live` is the rest of its manifest.
    fn plan(db: &Arc<FakeDb>, ids: impl IntoIterator<Item = u64> + Clone) -> WarmPlan {
        let mut plan = WarmPlan::default();
        let key = plan.add_database(Arc::clone(db) as WarmDb, ids.clone().into_iter().map(id));
        let targets: Arc<[CacheTarget]> = vec![CacheTarget::Filters, CacheTarget::Index].into();
        for n in ids {
            plan.push(key, id(n), Arc::clone(&targets), n as i64, ESTIMATE);
        }
        plan
    }

    fn budget(bytes: u64) -> WarmBudget {
        WarmBudget {
            bytes,
            measured: false,
        }
    }

    async fn pass(
        db: &Arc<FakeDb>,
        ids: std::ops::Range<u64>,
        bytes: u64,
        ledger: &WarmLedger,
    ) -> WarmReport {
        warm_planned(
            "test",
            plan(db, ids),
            budget(bytes),
            false,
            2,
            &CancellationToken::new(),
            ledger,
        )
        .await
    }

    fn warmed(db: &FakeDb) -> Vec<u64> {
        let mut ids: Vec<u64> = db
            .warmed
            .lock()
            .unwrap()
            .iter()
            .map(SsTableId::unwrap_wal_id)
            .collect();
        ids.sort_unstable();
        ids
    }

    #[tokio::test]
    async fn stops_at_the_budget_newest_first_and_counts_the_rest() {
        let db = Arc::new(FakeDb::default());
        let ledger = WarmLedger::new(true);

        let report = pass(&db, 0..10, 450, &ledger).await;

        assert_eq!(warmed(&db), vec![6, 7, 8, 9]);
        assert_eq!(report.warmed_ssts, 4);
        assert_eq!((report.skipped_ssts, report.skipped_bytes), (6, 600));
        assert_eq!(ledger.bytes(), 400);

        let again = pass(&db, 0..10, 450, &ledger).await;
        assert_eq!(again.warmed_ssts, 0);
        assert_eq!(again.skipped_ssts, 6);
        assert_eq!(
            db.warmed.lock().unwrap().len(),
            4,
            "a full budget warms nothing"
        );
    }

    #[tokio::test]
    async fn newer_ssts_take_the_room_of_the_oldest() {
        let db = Arc::new(FakeDb::default());
        let ledger = WarmLedger::new(true);
        pass(&db, 0..10, 450, &ledger).await;

        let report = pass(&db, 0..12, 450, &ledger).await;

        assert_eq!(warmed(&db), vec![6, 7, 8, 9, 10, 11]);
        assert_eq!(report.displaced_ssts, 2);
        assert_eq!(report.skipped_ssts, 6);
        assert_eq!(ledger.bytes(), 400);
        assert_eq!(ledger.ssts(), 4);
        assert!(
            db.evicted.lock().unwrap().is_empty(),
            "displacing evicts nothing"
        );
    }

    #[tokio::test]
    async fn ssts_compaction_removed_are_forgotten_and_evicted() {
        let db = Arc::new(FakeDb::default());
        let ledger = WarmLedger::new(true);
        pass(&db, 0..4, 1_000, &ledger).await;
        assert_eq!(ledger.ssts(), 4);

        let report = pass(&db, 2..6, 1_000, &ledger).await;

        let mut evicted: Vec<u64> = db
            .evicted
            .lock()
            .unwrap()
            .iter()
            .map(SsTableId::unwrap_wal_id)
            .collect();
        evicted.sort_unstable();
        assert_eq!(evicted, vec![0, 1]);
        assert_eq!(report.evicted_ssts, 2);
        assert_eq!(report.warmed_ssts, 2);
        assert_eq!(ledger.ssts(), 4);
        assert_eq!(ledger.bytes(), 400);
    }

    #[tokio::test]
    async fn closed_databases_are_forgotten_without_eviction() {
        let db = Arc::new(FakeDb::default());
        let ledger = WarmLedger::new(true);
        pass(&db, 0..4, 1_000, &ledger).await;

        let other = Arc::new(FakeDb::default());
        pass(&other, 0..2, 1_000, &ledger).await;

        assert_eq!(ledger.ssts(), 2);
        assert!(db.evicted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_unarmed_ledger_records_its_first_pass() {
        let db = Arc::new(FakeDb::default());
        let ledger = WarmLedger::new(false);

        pass(&db, 0..4, 1_000, &ledger).await;
        assert!(warmed(&db).is_empty());

        pass(&db, 0..6, 1_000, &ledger).await;
        assert_eq!(warmed(&db), vec![4, 5]);
    }

    #[tokio::test]
    async fn failed_warms_release_their_reservation_and_retry() {
        let db = Arc::new(FakeDb::default());
        db.failing.lock().unwrap().insert(id(3));
        let ledger = WarmLedger::new(true);

        let report = pass(&db, 0..4, 300, &ledger).await;
        assert_eq!(report.failed_ssts, 1);
        assert_eq!(warmed(&db), vec![0, 1, 2]);
        assert_eq!(ledger.bytes(), 300);

        db.failing.lock().unwrap().clear();
        let retry = pass(&db, 0..4, 300, &ledger).await;
        assert_eq!(retry.displaced_ssts, 1);
        assert_eq!(warmed(&db), vec![0, 1, 2, 3]);
        assert_eq!(ledger.bytes(), 300);
    }

    #[tokio::test]
    async fn a_cancelled_pass_holds_no_reservation() {
        let db = Arc::new(FakeDb::default());
        let ledger = WarmLedger::new(true);
        let cancel = CancellationToken::new();
        cancel.cancel();

        let report = warm_planned(
            "test",
            plan(&db, 0..4),
            budget(250),
            false,
            2,
            &cancel,
            &ledger,
        )
        .await;
        assert_eq!(report.warmed_ssts, 0);

        let report = pass(&db, 0..4, 250, &ledger).await;
        assert_eq!(report.warmed_ssts, 2);
        assert_eq!(ledger.lock().reserved, 0);
    }

    /// A reader over `ssts` flushed L0 SSTs of 2,000 keys each, sharing a
    /// metered metadata cache of `meta_capacity` bytes.
    async fn reader_over_flushed_ssts(
        ssts: usize,
        meta_capacity: u64,
    ) -> (tempfile::TempDir, Arc<slatedb::DbReader>, SharedDbCache) {
        use crate::storage::config::{
            BlockCacheConfig, FoyerMemoryCacheConfig, LocalObjectStoreConfig, ObjectStoreConfig,
            SlateDbStorageConfig,
        };
        use slatedb::config::{FlushOptions, FlushType, Settings};

        let dir = tempfile::tempdir().unwrap();
        let config = SlateDbStorageConfig {
            path: "db".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.path().to_str().unwrap().to_owned(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
                capacity: 4 << 20,
                shards: Some(1),
            })),
            meta_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
                capacity: meta_capacity,
                shards: None,
            })),
            disk: Default::default(),
        };
        let store = crate::storage::factory::create_object_store(&config.object_store).unwrap();
        let writer = slatedb::Db::builder(config.path.clone(), Arc::clone(&store))
            .with_settings(Settings {
                compactor_options: None,
                l0_max_ssts: ssts + 1,
                ..Settings::default()
            })
            .build()
            .await
            .unwrap();
        for sst in 0..ssts {
            let mut batch = slatedb::WriteBatch::new();
            for key in 0..2_000 {
                batch.put(format!("{sst:04}-{key:06}").as_bytes(), b"value");
            }
            writer.write(batch).await.unwrap();
            writer
                .flush_with_options(FlushOptions {
                    flush_type: FlushType::MemTable,
                })
                .await
                .unwrap();
        }
        writer.close().await.unwrap();

        let cache = SharedDbCache::from_slatedb_config(&config).await.unwrap();
        let reader = slatedb::DbReader::builder(config.path.clone(), store)
            .with_db_cache(cache.cache().unwrap())
            .build()
            .await
            .unwrap();
        (dir, Arc::new(reader), cache)
    }

    fn plan_l0(reader: &Arc<slatedb::DbReader>, keep: impl Fn(usize) -> bool) -> WarmPlan {
        let manifest = reader.status().current_manifest;
        let views: Vec<_> = manifest.l0().iter().rev().enumerate().collect();
        let mut plan = WarmPlan::default();
        let key = plan.add_database(
            Arc::clone(reader) as WarmDb,
            views
                .iter()
                .filter(|(n, _)| keep(*n))
                .map(|(_, view)| view.sst.id),
        );
        let targets: Arc<[CacheTarget]> = vec![CacheTarget::Filters, CacheTarget::Index].into();
        for (n, view) in views.into_iter().filter(|(n, _)| keep(*n)) {
            let estimate = metadata_estimate(&view.sst.info);
            plan.push(key, view.sst.id, Arc::clone(&targets), n as i64, estimate);
        }
        plan
    }

    #[tokio::test]
    async fn real_metadata_warming_stays_in_budget_and_releases_removed_ssts() {
        let (_dir, reader, cache) = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            reader_over_flushed_ssts(12, 64 << 10),
        )
        .await
        .expect("writing the SSTs stalled");
        let usage = cache.meta().unwrap().usage.clone().unwrap();
        let budget = WarmBudget::of(&cache, 50).unwrap();
        assert!(budget.measured);
        let ledger = WarmLedger::new(true);
        let cancel = CancellationToken::new();

        let report = warm_planned(
            "test",
            plan_l0(&reader, |_| true),
            budget,
            false,
            4,
            &cancel,
            &ledger,
        )
        .await;

        assert!(
            report.warmed_ssts > 0 && report.skipped_ssts > 0,
            "{report:?}"
        );
        assert_eq!(report.warmed_ssts + report.skipped_ssts, 12);
        assert!(ledger.bytes() <= budget.bytes);
        assert_eq!(ledger.bytes(), report.warmed_bytes);
        assert_eq!(
            usage.usage_bytes(),
            ledger.bytes(),
            "the warmer filled the cache alone"
        );
        assert_eq!(
            usage.evictions(),
            0,
            "warming stops before the cache evicts"
        );

        let warmed = report.warmed_ssts;
        let report = warm_planned(
            "test",
            plan_l0(&reader, |_| false),
            budget,
            false,
            4,
            &cancel,
            &ledger,
        )
        .await;

        assert_eq!(report.evicted_ssts, warmed);
        assert_eq!(ledger.bytes(), 0);
        assert_eq!(
            usage.usage_bytes(),
            0,
            "removed SSTs' metadata left the cache"
        );
    }

    #[test]
    fn estimates_cover_filters_index_and_entry_overhead() {
        let info = SsTableInfo {
            index_len: 1_000,
            filter_len: 4_000,
            ..SsTableInfo::default()
        };
        let overhead = ENTRY_OVERHEAD_BYTES as u64;
        assert_eq!(metadata_estimate(&info), 5_000 + 2 * overhead);
        let unfiltered = SsTableInfo {
            filter_len: 0,
            ..info
        };
        assert_eq!(metadata_estimate(&unfiltered), 1_000 + overhead);
    }
}
