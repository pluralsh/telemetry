//! Process memory of a reader warming metadata over the cold store.
//!
//! `profile_warm_memory` opens a reader over `PROFILE_STORE_DIR` (ingested
//! as [`super::profile_cold_queries`] does when missing) and records its
//! resident set size, the metadata cache's usage and the warmer's ledger:
//! after opening, after the startup pass, after each of `PROFILE_CYCLES`
//! continuous passes `PROFILE_CYCLE_SECONDS` apart, and while
//! `PROFILE_QUERY_TASKS` tasks run the cold queries for
//! `PROFILE_QUERY_SECONDS`. `PROFILE_WARM_PERCENT` and
//! `PROFILE_WARM_CONCURRENCY` are the warmer's budget and concurrency;
//! `PROFILE_META_CACHE_BYTES` sizes the metadata cache. Run
//! `profile_warm_memory_writer` alongside in another process to keep
//! writing, compacting and rolling up `PROFILE_WRITER_HOURS` more hours,
//! one every `PROFILE_WRITER_PACE_SECONDS`, so continuous passes see SSTs
//! removed. `PROFILE_OUT` receives one JSON line per phase.

use std::sync::atomic::AtomicU64;

use common::WarmWindow;
use common::storage::config::DEFAULT_PAYLOAD_WARM_RANGE_SECONDS;
use common::storage::slate::warm::{WarmLedger, WarmReport};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

use super::*;

const MIB: f64 = (1u64 << 20) as f64;

fn mib(bytes: u64) -> f64 {
    bytes as f64 / MIB
}

fn rss(system: &mut System, pid: Pid) -> u64 {
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing().with_memory(),
    );
    system.process(pid).map_or(0, |process| process.memory())
}

/// Samples this process's RSS every 50 ms, keeping the peak since the
/// last [`Self::take_peak`].
struct Sampler {
    peak: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Sampler {
    fn start() -> Self {
        let peak = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let thread = std::thread::spawn({
            let peak = Arc::clone(&peak);
            let stop = Arc::clone(&stop);
            move || {
                let pid = sysinfo::get_current_pid().unwrap();
                let mut system = System::new();
                while !stop.load(Ordering::Relaxed) {
                    peak.fetch_max(rss(&mut system, pid), Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        });
        Self {
            peak,
            stop,
            thread: Some(thread),
        }
    }

    fn take_peak(&self) -> u64 {
        self.peak.swap(0, Ordering::Relaxed)
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[derive(serde::Serialize)]
struct Phase {
    phase: String,
    unix_ms: i64,
    at_s: f64,
    rss_mib: f64,
    peak_rss_mib: f64,
    meta_usage_mib: f64,
    meta_capacity_mib: f64,
    meta_evictions: u64,
    data_usage_mib: f64,
    ledger_mib: f64,
    ledger_ssts: usize,
    planned_ssts: usize,
    planned_mib: f64,
    warmed_ssts: usize,
    warmed_mib: f64,
    skipped_ssts: usize,
    skipped_mib: f64,
    evicted_ssts: usize,
    displaced_ssts: usize,
    gets: usize,
    get_mib: f64,
    elapsed_ms: f64,
    queries: usize,
}

struct Recorder {
    started: Instant,
    sampler: Sampler,
    system: System,
    pid: Pid,
    out: Option<std::fs::File>,
    queries: Arc<std::sync::atomic::AtomicUsize>,
}

impl Recorder {
    fn record(
        &mut self,
        phase: &str,
        db: &ShardedMetrics,
        ledger: &WarmLedger,
        report: WarmReport,
        gets: &[Get],
        elapsed: Duration,
    ) {
        let cache = db.db_cache();
        let meta = cache.meta().and_then(|meta| meta.usage.as_ref());
        let rss = rss(&mut self.system, self.pid);
        let phase = Phase {
            phase: phase.to_owned(),
            unix_ms: common::time::now_ms(),
            at_s: self.started.elapsed().as_secs_f64(),
            rss_mib: mib(rss),
            peak_rss_mib: mib(self.sampler.take_peak().max(rss)),
            meta_usage_mib: mib(meta.map_or(0, |meta| meta.usage_bytes())),
            meta_capacity_mib: mib(meta.map_or(0, |meta| meta.capacity_bytes())),
            meta_evictions: meta.map_or(0, |meta| meta.evictions()),
            data_usage_mib: mib(cache.data_usage().map_or(0, |data| data.usage_bytes())),
            ledger_mib: mib(ledger.bytes()),
            ledger_ssts: ledger.ssts(),
            planned_ssts: report.planned_ssts,
            planned_mib: mib(report.planned_bytes),
            warmed_ssts: report.warmed_ssts,
            warmed_mib: mib(report.warmed_bytes),
            skipped_ssts: report.skipped_ssts,
            skipped_mib: mib(report.skipped_bytes),
            evicted_ssts: report.evicted_ssts,
            displaced_ssts: report.displaced_ssts,
            gets: gets.len(),
            get_mib: mib(gets.iter().map(|get| get.bytes).sum()),
            elapsed_ms: ms(elapsed),
            queries: self.queries.load(Ordering::Relaxed),
        };
        let line = serde_json::to_string(&phase).unwrap();
        eprintln!("{line}");
        if let Some(out) = &mut self.out {
            writeln!(out, "{line}").unwrap();
        }
    }
}

fn store_dir() -> PathBuf {
    PathBuf::from(
        std::env::var("PROFILE_STORE_DIR").unwrap_or("/tmp/warm-mt/metrics-store".to_owned()),
    )
}

async fn ingested(dir: &Path) -> Meta {
    let meta_path = dir.join("meta.json");
    if !meta_path.exists() {
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let namespaces = env("PROFILE_NAMESPACES").unwrap_or(40);
        let layout = std::env::var("PROFILE_LAYOUT").unwrap_or("default".to_string());
        ingest(dir, default_end_ms(), namespaces, layout).await;
    }
    serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_warm_memory() {
    let dir = store_dir();
    let meta = ingested(&dir).await;
    let latency = Duration::from_millis(env("PROFILE_LATENCY_MS").unwrap_or(20));
    let percent: u8 = env("PROFILE_WARM_PERCENT").unwrap_or(75);
    let concurrency: usize = env("PROFILE_WARM_CONCURRENCY").unwrap_or(2);
    let cycles: usize = env("PROFILE_CYCLES").unwrap_or(18);
    let cycle = Duration::from_secs(env("PROFILE_CYCLE_SECONDS").unwrap_or(10));
    let query_tasks: usize = env("PROFILE_QUERY_TASKS").unwrap_or(8);
    let query_time = Duration::from_secs(env("PROFILE_QUERY_SECONDS").unwrap_or(60));
    let payloads = std::env::var("PROFILE_CONTINUOUS_PAYLOADS").as_deref() != Ok("off");
    // Everything from the store's first hour (and what a writer adds) to
    // now, or the last PROFILE_WARM_RANGE_SECONDS, where 0 is all live data.
    let metadata = match env::<u64>("PROFILE_WARM_RANGE_SECONDS") {
        Some(0) => None,
        Some(seconds) => Some(Duration::from_secs(seconds)),
        None => Some(Duration::from_millis(
            (common::time::now_ms() - (meta.end_ms - 26 * HOUR_MS)) as u64,
        )),
    };
    let startup = WarmWindow {
        metadata,
        payloads: None,
    };
    let continuous = WarmWindow {
        metadata,
        payloads: payloads.then(|| Duration::from_secs(DEFAULT_PAYLOAD_WARM_RANGE_SECONDS)),
    };
    let namespaces = [namespace()];
    let cancel = tokio_util::sync::CancellationToken::new();

    let mut recorder = Recorder {
        started: Instant::now(),
        sampler: Sampler::start(),
        system: System::new(),
        pid: sysinfo::get_current_pid().unwrap(),
        out: std::env::var("PROFILE_OUT").ok().map(|path| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap()
        }),
        queries: Arc::default(),
    };
    let ledger = WarmLedger::new(true);
    let db = cold_reader(&dir, latency, false).await;
    recorder.record(
        "opened",
        &db,
        &ledger,
        WarmReport::default(),
        &[],
        Duration::ZERO,
    );

    let started = Instant::now();
    start_recording();
    let report = db
        .warm_recent(&namespaces, startup, concurrency, &cancel, &ledger, percent)
        .await
        .unwrap();
    let gets = stop_recording();
    recorder.record("startup", &db, &ledger, report, &gets, started.elapsed());

    for n in 0..cycles {
        tokio::time::sleep(cycle).await;
        let started = Instant::now();
        let report = db
            .warm_recent(
                &namespaces,
                continuous,
                concurrency,
                &cancel,
                &ledger,
                percent,
            )
            .await
            .unwrap();
        recorder.record(
            &format!("cycle-{n}"),
            &db,
            &ledger,
            report,
            &[],
            started.elapsed(),
        );
    }

    let db = Arc::new(db);
    let ran = Arc::clone(&recorder.queries);
    let end_ms = meta.end_ms;
    let deadline = Instant::now() + query_time;
    let tasks: Vec<_> = (0..query_tasks)
        .map(|task| {
            let db = Arc::clone(&db);
            let ran = Arc::clone(&ran);
            tokio::spawn(async move {
                let mut n = task;
                while Instant::now() < deadline {
                    let (_, text) = QUERIES[n % QUERIES.len()];
                    db.query_range(&namespace(), text, day_range(end_ms), STEP)
                        .await
                        .unwrap();
                    ran.fetch_add(1, Ordering::Relaxed);
                    n += 1;
                }
            })
        })
        .collect();
    let mut n = 0;
    while Instant::now() < deadline {
        tokio::time::sleep(cycle.min(deadline.saturating_duration_since(Instant::now()))).await;
        let started = Instant::now();
        let report = db
            .warm_recent(
                &namespaces,
                continuous,
                concurrency,
                &cancel,
                &ledger,
                percent,
            )
            .await
            .unwrap();
        recorder.record(
            &format!("queries-{n}"),
            &db,
            &ledger,
            report,
            &[],
            started.elapsed(),
        );
        n += 1;
    }
    for task in tasks {
        task.await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    recorder.record(
        "after-queries",
        &db,
        &ledger,
        WarmReport::default(),
        &[],
        Duration::ZERO,
    );
}

/// Keeps writing the cold store's series past what is already there,
/// flushing every simulated minute and rolling up every simulated hour, at
/// one simulated hour per `PROFILE_WRITER_PACE_SECONDS`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_warm_memory_writer() {
    let dir = store_dir();
    let meta = ingested(&dir).await;
    let hours: i64 = env("PROFILE_WRITER_HOURS").unwrap_or(8);
    let pace = Duration::from_secs(env("PROFILE_WRITER_PACE_SECONDS").unwrap_or(15));
    let written_path = dir.join("written.json");
    let from_ms: i64 = std::fs::read(&written_path)
        .ok()
        .map_or(meta.end_ms, |bytes| serde_json::from_slice(&bytes).unwrap());

    let _layout = crate::read_tuning::override_write_layout(write_layout(&meta.layout)).await;
    let shard_path = ShardingOptions::shard_path(BASE_PATH, ShardId::new(0));
    let config = Config {
        storage: storage_config(&dir, shard_path),
        rollup: crate::RollupConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let rollup = crate::RollupConfig::default();
    let writer = TimeSeriesDb::open(config).await.unwrap();
    let namespace = namespace();
    let mut generator = Generator {
        namespaces: meta.namespaces,
        pods: HashMap::new(),
    };
    let mut t = from_ms;
    let mut hour_started = Instant::now();
    while t < from_ms + hours * HOUR_MS {
        let round = generator.round(t);
        let mut round = round.into_iter().peekable();
        while round.peek().is_some() {
            let batch: Vec<Series> = round.by_ref().take(WRITE_BATCH).collect();
            writer
                .write_with_visibility(&namespace, batch, Visibility::Applied)
                .await
                .unwrap();
        }
        writer.flush().await.unwrap();
        if ((t - from_ms) / ROUND_MS + 1) % 15 == 0 {
            writer.storage().flush_memtable().await.unwrap();
        }
        t += ROUND_MS;
        if t % HOUR_MS == 0 {
            let sealed = writer.rollup_now(&rollup, t).await.unwrap();
            eprintln!(
                "wrote to {:.1}h past the store, sealed {sealed}, layout {:?}",
                (t - meta.end_ms) as f64 / HOUR_MS as f64,
                writer.storage().layout()
            );
            tokio::time::sleep(pace.saturating_sub(hour_started.elapsed())).await;
            hour_started = Instant::now();
        }
    }
    writer.close().await.unwrap();
    std::fs::write(&written_path, serde_json::to_vec(&t).unwrap()).unwrap();
}
