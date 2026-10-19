//! Drives a real SlateDB writer and compactor through Logs-shaped rollups:
//! hourly segments, 4-hour window rollups, a day rollup made from them, the
//! day-wide segment discovery records live in, and the current hour, with
//! [`SegmentCompaction`]'s rollup hooks, merge holds and trivial moves. Every
//! seal is followed by supersede drains of what it covers.
//!
//! SlateDB 0.15 panics its compactor task when one validation pass finishes
//! two or more compactions without a worker (drains, trivial moves, rejected
//! proposals) and the `.compactions` write that follows conflicts with a
//! worker's: the stored copy brings the older ones back as `Submitted`, and the
//! next 10 s stats tick fails to find their sources. Production-like polls of
//! a few seconds leave them in place across that tick; [`Racing`] supplies
//! the conflicts that S3 latency and busy workers make in production.
//!
//! Each schedule takes 20 s to a few minutes, so the loop is ignored by
//! default:
//!
//! ```sh
//! ROLLUP_SCHEDULES=6 ROLLUP_SEED=100 ROLLUP_LOG=/tmp/rollup.log \
//!     cargo test -p common --test compactor_rollup -- --ignored --nocapture
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use common::namespace::Namespace;
use common::serde::scope::{KeyScope, ScopedSegmentExtractor, Segment};
use common::storage::compaction::{MergeHolds, SegmentCompaction, merged_since};
use common::storage::rollup::{self, DAY_NS, Marker, marker_key};
use common::storage::settings::base_settings;
use futures::StreamExt;
use futures::stream::BoxStream;
use slatedb::config::{
    CompactionWorkerOptions, CompactorOptions, FlushOptions, FlushType, PutOptions, Settings,
    WriteOptions,
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::path::Path;
use slatedb::object_store::throttle::{ThrottleConfig, ThrottledStore};
use slatedb::object_store::{
    self, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions as StorePutOptions, PutPayload,
    PutResult,
};
use slatedb::{CompactorBuilder, Db, DbBuilder};

const SCOPE: KeyScope = KeyScope::new(0x03, 0x06);
const HOUR: i64 = DAY_NS / 24;
const WINDOW: i64 = 4 * HOUR;
const PATH: &str = "compactor-rollup";

fn span(prefix: &[u8]) -> Option<(i64, i64)> {
    let (_, segment, _) = SCOPE.parse(prefix).ok()?;
    let segment = rollup::data_segment(segment);
    let end_ns = segment.end_ns()?;
    Some((
        segment.start_ns.div_euclid(1_000_000),
        end_ns.div_euclid(1_000_000) + i64::from(end_ns.rem_euclid(1_000_000) != 0),
    ))
}

/// Panics anywhere in the process, as `location: message`.
fn panics() -> &'static Mutex<Vec<String>> {
    static PANICS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    PANICS.get_or_init(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if let Some(panics) = PANICS.get() {
                panics
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(info.to_string());
            }
            previous(info);
        }));
        Mutex::new(Vec::new())
    })
}

#[derive(Debug)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// SplitMix64's finalizer: a bijection, so scattered keys stay unique.
fn scatter(serial: u64) -> u64 {
    let mut z = serial.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap()
}

/// Share of `.compactions` writes [`Racing`] makes conflict.
fn race_percent() -> u64 {
    std::env::var("ROLLUP_RACE_PERCENT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30)
}

fn slow_percent() -> u64 {
    std::env::var("ROLLUP_SLOW_PERCENT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(80)
}

/// Before some new versions of `.compactions`, publishes the previous version
/// under the new name, as a compaction worker's claim, heartbeat or result
/// landing first would, so the conditional write conflicts and the writer
/// reloads and merges the stored copy.
#[derive(Debug)]
struct Racing {
    inner: Arc<dyn ObjectStore>,
    percent: u64,
    rng: Mutex<Rng>,
    races: AtomicU64,
}

impl std::fmt::Display for Racing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Racing({})", self.inner)
    }
}

impl Racing {
    async fn race(&self, location: &Path) -> object_store::Result<()> {
        let Some((directory, _)) = location.as_ref().rsplit_once('/') else {
            return Ok(());
        };
        let directory = Path::from(directory);
        let mut previous: Option<Path> = None;
        let mut listing = self.inner.list(Some(&directory));
        while let Some(meta) = listing.next().await {
            let meta = meta?;
            if meta.location.as_ref().ends_with(".compactions")
                && previous
                    .as_ref()
                    .is_none_or(|previous| meta.location.as_ref() > previous.as_ref())
            {
                previous = Some(meta.location);
            }
        }
        let Some(previous) = previous.filter(|previous| previous != location) else {
            return Ok(());
        };
        let bytes = self.inner.get(&previous).await?.bytes().await?;
        let created = self
            .inner
            .put_opts(
                location,
                bytes.into(),
                StorePutOptions::from(PutMode::Create),
            )
            .await;
        if created.is_ok() {
            self.races.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectStore for Racing {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: StorePutOptions,
    ) -> object_store::Result<PutResult> {
        let race = location.as_ref().ends_with(".compactions")
            && !matches!(opts.mode, PutMode::Overwrite)
            && self
                .rng
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .chance(self.percent);
        if race {
            self.race(location).await?;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

struct Harness {
    poll_interval: Duration,
    races: Arc<Racing>,
    db: Arc<Db>,
    namespace: Namespace,
    holds: MergeHolds,
    serial: AtomicU64,
    stop: AtomicBool,
}

impl Harness {
    fn prefix(&self, segment: Segment) -> Bytes {
        let mut bytes = BytesMut::new();
        SCOPE.write(&mut bytes, &self.namespace, segment);
        bytes.freeze()
    }

    fn closed(&self) -> Option<String> {
        if let Some(panic) = panics()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .first()
        {
            return Some(panic.clone());
        }
        self.db
            .status()
            .close_reason
            .map(|reason| format!("closed without a recorded panic: {reason:?}"))
    }

    async fn put(&self, key: Bytes) -> bool {
        self.db
            .put_with_options(
                key,
                vec![0x5a; 192],
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                    ..WriteOptions::default()
                },
            )
            .await
            .is_ok()
    }

    /// Writes `rows` records to `segment`, as Logs does, also writing a
    /// discovery record to its day-wide period segment. Ingested records
    /// scatter over the segment's keys, so successive flushes overlap; a
    /// rollup copies in key order, so its flushes don't.
    async fn write(&self, segment: Segment, rows: u64, in_order: bool) -> bool {
        let period = self.prefix(Segment::containing(segment.start_ns, DAY_NS));
        let prefix = self.prefix(segment);
        for _ in 0..rows {
            let serial = self.serial.fetch_add(1, Ordering::Relaxed);
            let mut key = BytesMut::from(prefix.as_ref());
            key.put_u64(if in_order { serial } else { scatter(serial) });
            if !self.put(key.freeze()).await {
                return false;
            }
        }
        let mut key = BytesMut::from(period.as_ref());
        key.put_u8(0xd1);
        key.put_slice(&prefix);
        key.put_u64(scatter(self.serial.fetch_add(1, Ordering::Relaxed)));
        self.put(key.freeze()).await
    }

    async fn flush(&self) -> bool {
        self.db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .is_ok()
    }

    async fn marker(&self, marker: Marker, segment: Segment) -> bool {
        self.put(marker_key(SCOPE, &self.namespace, marker, segment))
            .await
            && self.flush().await
    }

    /// One Logs rollup into `wide` of `sources`: the pending marker, a copy
    /// in `chunks` flushes holding merges of `wide` and its sources, a merge
    /// request, then the seal.
    async fn roll_up(&self, rng: &mut Rng, wide: Segment, sources: &[Segment]) -> bool {
        let prefix = self.prefix(wide);
        let hold = self.holds.copying(prefix.clone());
        let _source_holds: Vec<_> = sources
            .iter()
            .map(|source| self.holds.copying(self.prefix(*source)))
            .collect();
        if !self.marker(Marker::Pending, wide).await {
            return false;
        }
        for _ in 0..sources.len() {
            for _ in 0..1 + rng.below(3) {
                if !self.write(wide, 20 + rng.below(60), true).await || !self.flush().await {
                    return false;
                }
            }
        }
        let copied_ms = u64::try_from(now_ns() / 1_000_000).unwrap();
        hold.merge();
        let started = std::time::Instant::now();
        while !merged_since(&self.db.manifest(), &prefix, copied_ms)
            && started.elapsed() < Duration::from_secs(3)
        {
            if self.closed().is_some() {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let sealed = self.marker(Marker::Sealed, wide).await;
        drop(hold);
        sealed
    }
}

async fn open(seed: u64, rng: &mut Rng) -> Harness {
    // Object-store latency like S3's widens the window between the
    // coordinator loading `.compactions` and writing it back, in which the
    // worker's claims and heartbeats conflict with it.
    let latency = Duration::from_millis(rng.below(40));
    let throttled: Arc<dyn ObjectStore> = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig {
            wait_get_per_call: latency / 2,
            wait_put_per_call: latency,
            wait_list_per_call: latency / 2,
            wait_delete_per_call: latency / 2,
            // Slow reads keep merges running across worker heartbeats.
            wait_get_per_byte: Duration::from_nanos(rng.below(3_000)),
            ..ThrottleConfig::default()
        },
    ));
    let races = Arc::new(Racing {
        inner: throttled,
        percent: race_percent(),
        rng: Mutex::new(Rng(rng.next() | 1)),
        races: AtomicU64::new(0),
    });
    let object_store: Arc<dyn ObjectStore> = races.clone();
    let holds = MergeHolds::default();
    let policy = SegmentCompaction::new(span)
        .with_grace(Duration::ZERO)
        .with_max_open_sources(2 + usize::try_from(rng.below(6)).unwrap())
        .with_holds(holds.clone())
        .with_superseded(rollup::superseded(SCOPE, HOUR))
        .with_unsealed(rollup::unsealed(SCOPE, HOUR));
    // Production polls every 5 s, which leaves a resurrected spec in place
    // across a 10 s stats tick; fast polls clear it within milliseconds.
    let slow = rng.chance(slow_percent());
    let poll_interval = if slow {
        Duration::from_millis(4_000 + rng.below(5_000))
    } else {
        Duration::from_millis(5 + rng.below(20))
    };
    let mut compactor = CompactorOptions {
        poll_interval,
        max_concurrent_compactions: 2 + usize::try_from(rng.below(4)).unwrap(),
        enable_trivial_move: true,
        worker: Some(CompactionWorkerOptions {
            compactions_poll_interval: Duration::from_millis(5 + rng.below(30)),
            heartbeat_interval: Duration::from_millis(20 + rng.below(200)),
            max_concurrent_compactions: 1 + usize::try_from(rng.below(4)).unwrap(),
            max_sst_size: 4 << 10,
            ..CompactionWorkerOptions::default()
        }),
        ..CompactorOptions::default()
    };
    compactor.scheduler_options.insert(
        "min_compaction_sources".to_owned(),
        (2 + rng.below(3)).to_string(),
    );
    let settings = Settings {
        // One L0 compaction per segment per poll keeps up with far fewer,
        // larger L0s when polls are seconds apart.
        l0_sst_size_bytes: if slow { 64 << 10 } else { 8 << 10 },
        compactor_options: None,
        ..base_settings()
    };
    let db = DbBuilder::new(PATH.to_owned(), object_store.clone())
        .with_settings(settings)
        .with_segment_extractor(ScopedSegmentExtractor::new("repro/v1", SCOPE).shared())
        .with_compactor_builder(
            CompactorBuilder::new(PATH.to_owned(), object_store)
                .with_options(compactor)
                .with_scheduler_supplier(policy.supplier(&format!("{PATH}-{seed}"))),
        )
        .build()
        .await
        .unwrap();
    Harness {
        poll_interval,
        races,
        db: Arc::new(db),
        namespace: Namespace::new("default").unwrap(),
        holds,
        serial: AtomicU64::new(0),
        stop: AtomicBool::new(false),
    }
}

/// Runs one schedule; returns how the database closed, if it did.
async fn schedule(seed: u64) -> Option<String> {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let harness = Arc::new(open(seed, &mut rng).await);
    let run =
        tokio::time::timeout(Duration::from_secs(240), drive(harness.clone(), &mut rng)).await;
    harness.stop.store(true, Ordering::Relaxed);
    let closed = harness.closed();
    if run.is_err() && closed.is_none() {
        let manifest = harness.db.manifest();
        for segment in manifest.segments() {
            eprintln!(
                "stalled seed {seed}: {:02x?} l0={} runs={:?}",
                segment.prefix().as_ref(),
                segment.l0().len(),
                segment
                    .compacted()
                    .iter()
                    .map(|run| run.id)
                    .collect::<Vec<_>>()
            );
        }
        panic!("schedule {seed} stalled");
    }
    eprintln!(
        "schedule {seed}: {} conflicting .compactions writes",
        harness.races.races.load(Ordering::Relaxed)
    );
    let _ = harness.db.close().await;
    closed
}

async fn drive(harness: Arc<Harness>, rng: &mut Rng) {
    let rng = &mut *rng;
    let now = now_ns();
    let today = Segment::containing(now, DAY_NS);
    let yesterday = Segment::new(today.start_ns - DAY_NS, DAY_NS);
    let current = Segment::containing(now, HOUR);
    // A compactor polling every few seconds keeps up with far fewer L0s.
    let slow = harness.poll_interval >= Duration::from_secs(1);

    // Ingest into the current hour, and so into today's period segment.
    let ingest = {
        let harness = harness.clone();
        let (pause, flush_every) = if slow {
            (50 + rng.below(100), 60)
        } else {
            (1 + rng.below(10), 3)
        };
        tokio::spawn(async move {
            let mut round = 0u64;
            while !harness.stop.load(Ordering::Relaxed) {
                if !harness.write(current, 8, false).await {
                    return;
                }
                round += 1;
                if round.is_multiple_of(flush_every) && !harness.flush().await {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(pause)).await;
            }
        })
    };

    let windows = if slow {
        2 + rng.below(3)
    } else {
        2 + rng.below(5)
    };
    let mut sealed = Vec::new();
    'windows: for index in 0..windows {
        let window = Segment::new(
            yesterday.start_ns + i64::try_from(index).unwrap() * WINDOW,
            WINDOW,
        );
        let hours: Vec<Segment> = (0..4)
            .map(|hour| Segment::new(window.start_ns + hour * HOUR, HOUR))
            .collect();
        for _ in 0..if slow {
            1 + rng.below(2)
        } else {
            2 + rng.below(4)
        } {
            for hour in &hours {
                if !harness.write(*hour, 10 + rng.below(80), false).await {
                    break 'windows;
                }
            }
            if !harness.flush().await {
                break 'windows;
            }
        }
        tokio::time::sleep(Duration::from_millis(rng.below(200))).await;
        if !harness.roll_up(rng, window, &hours).await {
            break;
        }
        sealed.push(window);
        let poll_ms = u64::try_from(harness.poll_interval.as_millis()).unwrap();
        tokio::time::sleep(Duration::from_millis(rng.below(2 * poll_ms))).await;
        // Rows for the window arriving after its seal go to the rollup.
        for _ in 0..rng.below(4) {
            if !harness.write(window, 5 + rng.below(20), false).await || !harness.flush().await {
                break 'windows;
            }
            tokio::time::sleep(Duration::from_millis(rng.below(50))).await;
        }
    }
    if harness.closed().is_none() && sealed.len() >= 2 && rng.chance(70) {
        harness.roll_up(rng, yesterday, &sealed).await;
        for _ in 0..rng.below(4) {
            if !harness.write(yesterday, 5 + rng.below(20), false).await || !harness.flush().await {
                break;
            }
        }
    }
    // Let the supersede drains and late merges play out, past a stats tick.
    let linger = if slow { 480 } else { 40 };
    for _ in 0..linger {
        if harness.closed().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    harness.stop.store(true, Ordering::Relaxed);
    let _ = ingest.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "fuzz loop over minutes-long schedules; run with --ignored"]
async fn rollups_with_supersede_drains_never_close_the_database() {
    panics();
    if let Ok(path) = std::env::var("ROLLUP_LOG") {
        let file = std::fs::File::create(path).unwrap();
        let _ = tracing_subscriber::fmt()
            .with_writer(Mutex::new(file))
            .with_ansi(false)
            .with_max_level(if std::env::var("ROLLUP_DEBUG").is_ok() {
                tracing::Level::DEBUG
            } else {
                tracing::Level::INFO
            })
            .try_init();
    }
    let schedules: u64 = std::env::var("ROLLUP_SCHEDULES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(6);
    let first: u64 = std::env::var("ROLLUP_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    for seed in first..first + schedules {
        let started = std::time::Instant::now();
        let closed = schedule(seed).await;
        eprintln!("schedule {seed} done in {:?}", started.elapsed());
        if let Some(closed) = closed {
            panic!("schedule {seed} closed the database: {closed}");
        }
    }
}
