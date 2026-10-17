//! Manual memory profiles of the write path, run under an external resident
//! size sampler that lines its samples up with the phases printed here.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::storage::config::{
    LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig,
};

use super::write::FLUSHED_OPS;
use super::*;
use crate::config::{CompactionConfig, PageConfig};
use crate::memory::MemoryConfig;
use crate::{LogBatch, LogEntry};
use tokio_util::sync::CancellationToken;

const SECOND: i64 = 1_000_000_000;
const WORDS: [&str; 6] = [
    "GET /api/cart",
    "POST /login",
    "timeout",
    "ok",
    "retry",
    "error",
];

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} is not valid"))
    })
}

/// Requests of `rows` production-shaped lines over `streams` Zipf-weighted
/// pod streams in the hour before `end_ns`.
fn requests(rows: usize, per_request: usize, streams: usize, end_ns: i64) -> Vec<Vec<LogBatch>> {
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    let labels: Vec<Labels> = (0..streams)
        .map(|rank| {
            Labels::new(vec![
                Label::new("cluster", format!("cluster-{}", rank % 7)),
                Label::new("namespace", format!("ns-{}", rank % 40)),
                Label::new("pod", format!("pod-{rank}")),
                Label::new("container", format!("c-{}", rank % 3)),
                Label::new("job", format!("ns-{}/pod-{rank}", rank % 40)),
            ])
            .unwrap()
        })
        .collect();
    let weights: Vec<f64> = (0..streams)
        .map(|rank| 1.0 / ((rank + 1) as f64).powf(1.1))
        .collect();
    let total: f64 = weights.iter().sum();
    let mut cumulative = Vec::with_capacity(streams);
    let mut sum = 0.0;
    for weight in weights {
        sum += weight / total;
        cumulative.push(sum);
    }
    let mut out = Vec::new();
    let mut written = 0;
    while written < rows {
        let count = per_request.min(rows - written);
        let mut by_stream: BTreeMap<usize, Vec<LogEntry>> = BTreeMap::new();
        for _ in 0..count {
            let pick = rng.below(1 << 30) as f64 / f64::from(1 << 30);
            let stream = cumulative.partition_point(|edge| *edge < pick);
            let ts = end_ns - 3600 * SECOND + rng.below((3600 * SECOND) as u64) as i64;
            let word = WORDS[rng.below(WORDS.len() as u64) as usize];
            by_stream
                .entry(stream.min(streams - 1))
                .or_default()
                .push(LogEntry::new(
                    ts,
                    format!(
                        "ts={ts} level=info caller=handler.go:{} msg=\"{word}\" trace_id={:016x} span_id={:08x} duration_ms={} status={}",
                        rng.below(900),
                        rng.next(),
                        rng.next() as u32,
                        rng.below(5000),
                        200 + rng.below(4) * 100,
                    ),
                ));
        }
        out.push(
            by_stream
                .into_iter()
                .map(|(stream, mut entries)| {
                    entries.sort_by_key(|entry| entry.timestamp_ns);
                    LogBatch::new(labels[stream].clone(), entries)
                })
                .collect(),
        );
        written += count;
    }
    out
}

fn profile_config(
    store: &std::path::Path,
    write_buffer: common::coordinator::WriteCoordinatorConfig,
    memory: MemoryConfig,
) -> Config {
    Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "profile".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: store.display().to_string(),
            }),
            settings_path: std::env::var("PROFILE_SLATEDB_SETTINGS").ok(),
            block_cache: None,
            meta_cache: None,
            disk: Default::default(),
        }),
        segment_duration: Duration::from_secs(3600),
        discovery_rollup: Some(Duration::from_secs(24 * 3600)),
        stream_locality_labels: Vec::new(),
        stream_id_block_size: 0,
        retention: None,
        page: PageConfig {
            target_size_bytes: 1 << 20,
            max_rows: 8192,
            rows_per_block: 256,
        },
        compaction: CompactionConfig {
            enabled: false,
            ..CompactionConfig::default()
        },
        write_buffer,
        block_cache_capacity_bytes: 0,
        result_cache_capacity_bytes: 0,
        memory,
    }
}

/// Keeps the latest value of every counter and gauge by name and labels, so
/// the profiles can print SlateDB's own statistics.
#[derive(Default)]
struct StatsRecorder(std::sync::Mutex<HashMap<String, Arc<std::sync::atomic::AtomicU64>>>);

impl StatsRecorder {
    fn handle(&self, key: &metrics::Key) -> Arc<std::sync::atomic::AtomicU64> {
        let mut name = key.name().to_owned();
        for label in key.labels() {
            name.push_str(&format!(",{}={}", label.key(), label.value()));
        }
        self.0.lock().unwrap().entry(name).or_default().clone()
    }

    /// The counter or gauge `name`, which may name labels, summed over the
    /// labels it does not name.
    fn value(&self, name: &str, gauge: bool) -> f64 {
        let prefix = format!("{name},");
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| *key == name || key.starts_with(&prefix))
            .map(|(_, value)| {
                let bits = value.load(Ordering::Relaxed);
                if gauge {
                    f64::from_bits(bits)
                } else {
                    bits as f64
                }
            })
            .sum()
    }
}

impl metrics::Recorder for StatsRecorder {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::from_arc(self.handle(key))
    }

    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::from_arc(self.handle(key))
    }

    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// SlateDB's memtable, L0, SST and compaction statistics.
fn slatedb_stats(stats: &StatsRecorder) -> String {
    let mib = |bytes: f64| bytes / f64::from(1 << 20);
    format!(
        "mem_mib={:.0} l0={} ssts={} l0_flush_mib={:.0} compacted_mib={:.0} compactor_ssts={} \
         memtable_write_mib={:.0} slate_backpressure={} l0_stalls={} (per key {})",
        mib(stats.value("slatedb.db.total_mem_size_bytes", true)),
        stats.value("slatedb.db.l0_sst_count", true),
        stats.value("slatedb.db.sst_count", true),
        mib(stats.value("slatedb.db.l0_flush_bytes", false)),
        mib(stats.value("slatedb.compactor.bytes_compacted", false)),
        stats.value("slatedb.compactor.ssts_written", false),
        mib(stats.value("slatedb.db.memtable_write_bytes", false)),
        stats.value("slatedb.db.backpressure_count", false),
        stats.value("slatedb.db.l0_stall_count", false),
        stats.value("slatedb.db.l0_stall_count,type=num_ssts_per_key", false),
    )
}

/// Ingests `PROFILE_ROUND_ROWS` rows into each of `PROFILE_ROUNDS` past
/// days and rolls each day up in turn, while `PROFILE_BURST_RPS` rows a
/// second arrive in `PROFILE_BURST_ROWS`-row requests, as ingest does, for
/// the rollup's duration. Prints each round's ingest rate, rollup time,
/// burst rejections and SlateDB statistics for the sampler.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_rollup_rounds() {
    static STATS: std::sync::OnceLock<&'static StatsRecorder> = std::sync::OnceLock::new();
    let stats = *STATS.get_or_init(|| Box::leak(Box::default()));
    let _ = metrics::set_global_recorder(stats);
    let started = Instant::now();
    let phase = |name: String, detail: String| {
        println!(
            "phase {name} at {:.1}s {detail}",
            started.elapsed().as_secs_f64()
        );
    };
    let store = std::path::PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    let rounds: i64 = env("PROFILE_ROUNDS", 6);
    let round_rows: usize = env("PROFILE_ROUND_ROWS", 600_000);
    let burst_rps: u64 = env("PROFILE_BURST_RPS", 5_000);
    let burst_rows: usize = env("PROFILE_BURST_ROWS", 1_000);
    let mut write_buffer = common::coordinator::WriteCoordinatorConfig::default();
    write_buffer.flush_size_threshold = env(
        "PROFILE_THRESHOLD_MB",
        write_buffer.flush_size_threshold >> 20,
    ) << 20;
    let mut memory = MemoryConfig::default();
    memory.write_bytes = env("PROFILE_WRITE_MB", memory.write_bytes >> 20) << 20;
    let db = Arc::new(
        LogDb::open(profile_config(&store, write_buffer, memory))
            .await
            .unwrap(),
    );
    let namespace = Namespace::new("default").unwrap();
    let day = 24 * 3600 * SECOND;
    let today = common::time::now_ns() / day * day;
    phase("open".to_owned(), slatedb_stats(stats));

    for round in 1..=rounds {
        // Four hours of one past day, oldest day first.
        let day_start = today - (rounds + 1 - round) * day;
        let mut day_requests = Vec::new();
        for hour in [3, 9, 15, 21] {
            day_requests.extend(requests(
                round_rows / 4,
                env("PROFILE_REQUEST_ROWS", 2_000),
                env("PROFILE_STREAMS", 4_660),
                day_start + hour * 3600 * SECOND,
            ));
        }
        let ingest = Instant::now();
        let mut rejected = 0usize;
        for request in day_requests {
            loop {
                match db
                    .write_with_durability(&namespace, request.clone(), Durability::Written)
                    .await
                {
                    Ok(_) => break,
                    Err(Error::Backpressure) => {
                        rejected += 1;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("{error}"),
                }
            }
        }
        phase(
            format!("ingested{round}"),
            format!(
                "rows_per_second={:.0} rejected={rejected} {}",
                round_rows as f64 / ingest.elapsed().as_secs_f64(),
                slatedb_stats(stats)
            ),
        );

        let stop = CancellationToken::new();
        let burst = tokio::spawn({
            let (db, namespace, stop) = (db.clone(), namespace.clone(), stop.clone());
            async move {
                let (mut accepted, mut rejected, mut slowest) = (0usize, 0usize, Duration::ZERO);
                let period = Duration::from_secs_f64(burst_rows as f64 / burst_rps.max(1) as f64);
                let mut tick = tokio::time::interval(period);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut seed = 0;
                while !stop.is_cancelled() {
                    tick.tick().await;
                    seed += 1;
                    let request = requests(
                        burst_rows,
                        burst_rows,
                        env("PROFILE_STREAMS", 4_660),
                        common::time::now_ns() + seed,
                    )
                    .remove(0);
                    let sent = Instant::now();
                    match db
                        .write_with_durability(&namespace, request, Durability::Written)
                        .await
                    {
                        Ok(report) => accepted += report.rows,
                        Err(Error::Backpressure) => rejected += 1,
                        Err(error) => panic!("{error}"),
                    }
                    slowest = slowest.max(sent.elapsed());
                }
                (accepted, rejected, slowest)
            }
        });
        let rollup = Instant::now();
        let sealed = db
            .rollup_at(
                &common::storage::rollup::RollupConfig {
                    delay: Duration::ZERO,
                    merge_wait: Duration::from_secs(env("PROFILE_MERGE_WAIT_SECONDS", 60)),
                    ..Default::default()
                },
                common::time::now_ns(),
            )
            .await
            .unwrap();
        let rollup_seconds = rollup.elapsed().as_secs_f64();
        stop.cancel();
        let (accepted, burst_rejected, slowest) = burst.await.unwrap();
        phase(
            format!("rolled{round}"),
            format!(
                "sealed={sealed} rollup_seconds={rollup_seconds:.1} burst_rows_per_second={:.0} \
                 burst_rejected_requests={burst_rejected} burst_slowest_ms={} {}",
                accepted as f64 / rollup_seconds,
                slowest.as_millis(),
                slatedb_stats(stats)
            ),
        );
        tokio::time::sleep(Duration::from_secs(10)).await;
        phase(format!("settled{round}"), slatedb_stats(stats));
    }
    Arc::into_inner(db).unwrap().close().await.unwrap();
    phase("closed".to_owned(), String::new());
}

/// Opens a reader over `PROFILE_STORE` that replays the WAL, as reader
/// servers do, and keeps it open for `PROFILE_SECONDS` while a
/// `profile_write_memory` run in another process writes the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_reader_replay() {
    let started = Instant::now();
    let store = std::path::PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    let config = profile_config(
        &store,
        common::coordinator::WriteCoordinatorConfig::default(),
        MemoryConfig::default(),
    );
    let options = DbReaderOptions {
        skip_wal_replay: false,
        manifest_poll_interval: Duration::from_secs(env("PROFILE_POLL_SECONDS", 10)),
        ..DbReaderOptions::default()
    };
    let db = loop {
        match LogDb::open_reader(config.clone(), options.clone()).await {
            Ok(db) => break db,
            Err(_) if started.elapsed() < Duration::from_secs(60) => {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(error) => panic!("{error}"),
        }
    };
    println!("phase open at {:.1}s", started.elapsed().as_secs_f64());
    tokio::time::sleep(Duration::from_secs(env("PROFILE_SECONDS", 120))).await;
    drop(db);
    println!("phase closed at {:.1}s", started.elapsed().as_secs_f64());
}

/// Writes `PROFILE_ROWS` rows in `PROFILE_REQUEST_ROWS`-row requests, then
/// flushes, printing each phase's time for the sampler.
///
/// With `PROFILE_ONE_DELTA` every row lands in one write-buffer delta that
/// one flush writes, which is what a flush costs per input byte. Otherwise
/// the write buffer and budgets keep their defaults, or `PROFILE_WRITE_MB`
/// and `PROFILE_THRESHOLD_MB`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_write_memory() {
    let started = Instant::now();
    let phase = |name: &str, detail: String| {
        println!(
            "phase {name} at {:.1}s {detail}",
            started.elapsed().as_secs_f64()
        );
    };
    let store = std::path::PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    let rows: usize = env("PROFILE_ROWS", 450_000);
    let one_delta = std::env::var("PROFILE_ONE_DELTA").is_ok();
    let mut write_buffer = common::coordinator::WriteCoordinatorConfig::default();
    let mut memory = MemoryConfig::default();
    if one_delta {
        write_buffer.flush_interval = Duration::from_secs(24 * 3600);
        write_buffer.flush_size_threshold = usize::MAX;
        memory.write_bytes = u64::from(u32::MAX);
    } else {
        write_buffer.flush_size_threshold = env(
            "PROFILE_THRESHOLD_MB",
            write_buffer.flush_size_threshold >> 20,
        ) << 20;
        memory.write_bytes = env("PROFILE_WRITE_MB", memory.write_bytes >> 20) << 20;
    }
    let config = profile_config(&store, write_buffer, memory);
    let requests = requests(
        rows,
        env("PROFILE_REQUEST_ROWS", 2_000),
        env("PROFILE_STREAMS", 4_660),
        common::time::now_ns(),
    );
    let line_bytes: usize = requests
        .iter()
        .flatten()
        .flat_map(|batch| &batch.entries)
        .map(|entry| entry.line.len())
        .sum();
    let namespace = Namespace::new("default").unwrap();
    let db = LogDb::open(config).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    phase("open", format!("rows={rows} line_bytes={line_bytes}"));
    let mut rejected = 0usize;
    let mut held = 0;
    for request in requests {
        loop {
            match db
                .write_with_durability(&namespace, request.clone(), Durability::Applied)
                .await
            {
                Ok(_) => break,
                Err(Error::Backpressure) => {
                    rejected += 1;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("{error}"),
            }
        }
        held = held.max(db.memory.write_capacity() - db.memory.write_available());
    }
    let applied = started.elapsed();
    phase(
        "applied",
        format!(
            "held_bytes={held} rejected={rejected} ops={}",
            FLUSHED_OPS.load(Ordering::Relaxed)
        ),
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    phase("flush", String::new());
    db.flush().await.unwrap();
    phase(
        "flushed",
        format!(
            "ops={} rows_per_second={:.0}",
            FLUSHED_OPS.load(Ordering::Relaxed),
            rows as f64
                / started
                    .elapsed()
                    .saturating_sub(Duration::from_secs(5))
                    .as_secs_f64()
        ),
    );
    let _ = applied;
    tokio::time::sleep(Duration::from_secs(3)).await;
    db.writer().unwrap().flush_memtable().await.unwrap();
    phase("memtable", String::new());
    tokio::time::sleep(Duration::from_secs(3)).await;
    db.close().await.unwrap();
    phase("closed", String::new());
}
