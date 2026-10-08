//! End-to-end PromQL range queries over a realistic 10k-series, 2-hour
//! corpus, looped for a fixed wall time so an external sampler (macOS
//! `sample <pid>`, `perf`, ...) can attribute CPU time across the read
//! path. Not a criterion bench.
//!
//! ```text
//! cargo bench -p plural-metrics --features bench-internals --bench query_profile -- \
//!     [--state compacted|streamed|l0|realtime|realtime-compacted] [--cache block|none] \
//!     [--baseline] [--query NAME|all] [--seconds N] [--trace N] [--scrapes N] \
//!     [--l0-bytes BYTES] [--memtable M] [--batch P] [--min-sources N]
//! ```
//!
//! States: `compacted` writes each series' bucket as one value (one entry
//! per key, as after full compaction); `streamed` writes one scrape at a
//! time with Metrics' default compactor settings; `l0` streams with
//! compaction disabled, leaving every key's merge operands in L0.
//!
//! `realtime` models a long-running writer: one merge operand per series
//! every `--batch` scrapes, SlateDB freezing the memtable to L0 whenever
//! its estimate reaches `--l0-bytes` (`l0_sst_size_bytes`; Metrics' default,
//! 16 MiB, is ~20 scrapes of this corpus, SlateDB's 64 MiB ~76), a wait for
//! the compactor after every L0 flush (compactor, worker and manifest polls
//! shortened to 100 ms), and loading stopped once at least `--scrapes` are
//! in and exactly `--memtable` scrapes sit unflushed in the memtable.
//! `--l0-bytes` and `--min-sources` default to Metrics' SlateDB tuning
//! (`bench_support::default_slatedb_tuning`). `realtime-compacted` loads
//! one value per key per bucket into SSTs under the same settings, as
//! the baseline. Both print the L0 / sorted-run fan-in and the write cost
//! per simulated hour from SlateDB's metrics. `--baseline` also loads a
//! `realtime-compacted` database with the same scrape count in-process,
//! alternates query iterations between the two so background load hits
//! both alike, and prints each query's mean and p50 `RATIO` against it.
//!
//! Each query prints `BEGIN <names> pid=<pid>` before its loop so a driver
//! can attach a sampler, and reports the read-path merge operands per
//! query (SlateDB counts `E + ceil(E / 100)` for a key with `E` entries).
//! `--trace N` additionally runs `N` traced iterations per query and
//! prints the collector's phase and storage timings.

use std::ops::RangeInclusive;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::storage::config::{
    BlockCacheConfig, FoyerMemoryCacheConfig, ObjectStoreConfig, SlateDbStorageConfig,
};
use plural_metrics::bench_support::{default_slatedb_tuning, query_range_traced};
use plural_metrics::{
    Config, Label, MetricType, Namespace, Sample, Series, Temporality, TimeSeriesDb, Visibility,
    range_result_to_response,
};

const HOUR_MS: i64 = 60 * 60 * 1000;
const T0_MS: i64 = 1_699_999_200_000;
const SCRAPE_MS: i64 = 15_000;
const SCRAPES_PER_HOUR: i64 = HOUR_MS / SCRAPE_MS;
const COMBOS: usize = 200;
const COUNTERS: usize = 25;
const GAUGES: usize = 17;
const RATIOS: usize = 8;
/// About one L0 flush per `max_wal_flushes_before_l0_flush` (4096 x 100 ms).
const SCRAPES_PER_L0_FLUSH: i64 = 28;

const QUERIES: [(&str, &str); 5] = [
    ("raw", "app_gauge_03"),
    ("rate", "sum by (job) (rate(app_counter_01_total[5m]))"),
    (
        "avg_over_time",
        r#"avg_over_time({__name__=~"app_gauge_0[0-4]"}[5m])"#,
    ),
    ("count_all", r#"count({__name__=~"app_.*"})"#),
    (
        "sum_rate_counters",
        r#"sum(rate({__name__=~"app_counter_.*"}[5m]))"#,
    ),
];

/// xorshift64*, so every run loads the same corpus.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[derive(Clone, Copy)]
enum Shape {
    IntCounter { max_step: u64 },
    DecimalCounter { scale: f64 },
    Bytes,
    Quarters,
    Smooth { period: f64 },
    Ratio,
}

/// One series' generator: identity, scrape phase and value state.
struct SeriesGen {
    name: String,
    labels: Vec<Label>,
    counter: bool,
    shape: Shape,
    phase_ms: i64,
    value: f64,
    rng: Rng,
}

impl SeriesGen {
    fn timestamp(&mut self, scrape: i64) -> i64 {
        T0_MS + scrape * SCRAPE_MS + self.phase_ms + self.rng.below(6) as i64
    }

    fn next_value(&mut self, scrape: i64) -> f64 {
        let rng = &mut self.rng;
        self.value = match self.shape {
            Shape::IntCounter { .. } | Shape::DecimalCounter { .. } if rng.below(2_000) == 0 => 0.0,
            Shape::IntCounter { max_step } => self.value + rng.below(max_step + 1) as f64,
            Shape::DecimalCounter { scale } => {
                self.value + (rng.below(1_000) as f64 * scale * 1000.0).round() / 1000.0
            }
            Shape::Bytes => {
                let delta = rng.below(64) as f64 - 31.0;
                (self.value + delta * 4096.0).max(0.0)
            }
            Shape::Quarters => {
                if rng.below(8) == 0 {
                    rng.below(400) as f64 * 0.25
                } else {
                    self.value
                }
            }
            Shape::Smooth { period } => {
                let t = scrape as f64 / period;
                50.0 + 40.0 * (t * std::f64::consts::TAU).sin() + rng.unit()
            }
            Shape::Ratio => rng.unit(),
        };
        self.value
    }
}

fn corpus() -> Vec<SeriesGen> {
    let mut rng = Rng(0x5eed_cafe);
    let mut out = Vec::with_capacity((COUNTERS + GAUGES + RATIOS) * COMBOS);
    let metrics = (0..COUNTERS)
        .map(|i| (format!("app_counter_{i:02}_total"), true))
        .chain((0..GAUGES).map(|i| (format!("app_gauge_{i:02}"), false)))
        .chain((0..RATIOS).map(|i| (format!("app_ratio_{i:02}"), false)));
    for (metric_idx, (name, counter)) in metrics.enumerate() {
        for combo in 0..COMBOS {
            let labels = vec![
                Label::new("job", format!("job-{}", combo % 5)),
                Label::new(
                    "instance",
                    format!("10.0.{}.{}:9100", combo % 5, combo / 5 % 40),
                ),
                Label::new("pod", format!("pod-{combo:03}-{}", combo % 7)),
            ];
            let shape = if counter {
                if combo % 2 == 0 {
                    Shape::IntCounter {
                        max_step: [3, 100, 5_000][combo % 3],
                    }
                } else {
                    Shape::DecimalCounter {
                        scale: [0.001, 0.25][combo / 2 % 2],
                    }
                }
            } else if name.starts_with("app_ratio") {
                Shape::Ratio
            } else {
                match (metric_idx + combo) % 3 {
                    0 => Shape::Bytes,
                    1 => Shape::Quarters,
                    _ => Shape::Smooth {
                        period: 40.0 + (combo % 11) as f64 * 10.0,
                    },
                }
            };
            let value = match shape {
                Shape::Bytes => (1 + rng.below(4_000)) as f64 * 1_048_576.0,
                Shape::Quarters => rng.below(400) as f64 * 0.25,
                _ => 0.0,
            };
            out.push(SeriesGen {
                name: name.clone(),
                labels,
                counter,
                shape,
                phase_ms: rng.below(SCRAPE_MS as u64 - 10) as i64,
                value,
                rng: Rng(rng.next() | 1),
            });
        }
    }
    out
}

fn series(generator: &SeriesGen, samples: Vec<Sample>) -> Series {
    let mut series = Series::new(&generator.name, generator.labels.clone(), samples);
    if generator.counter {
        series.metric_type = Some(MetricType::Sum {
            monotonic: true,
            temporality: Temporality::Cumulative,
        });
    }
    series
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Compacted,
    Streamed,
    L0,
    Realtime,
    RealtimeCompacted,
}

impl State {
    /// Freezes the memtable at `l0_sst_size_bytes` (the only L0 trigger a
    /// production writer reaches: WAL ids advance only on non-empty WAL
    /// flushes, so `max_wal_flushes_before_l0_flush` = 4096, its minimum,
    /// is hours away at one write per 10 s) and waits for the compactor
    /// after every L0 flush.
    fn paced(self) -> bool {
        matches!(self, Self::Realtime | Self::RealtimeCompacted)
    }
}

#[derive(Clone)]
struct Args {
    state: State,
    /// Also load a `realtime-compacted` copy into a second database and
    /// run every query against both, for an in-process ratio.
    baseline: bool,
    block_cache: bool,
    query: Option<String>,
    seconds: u64,
    trace: usize,
    /// For `realtime`, the minimum: loading continues until the memtable
    /// holds exactly `memtable` scrapes, and this becomes the actual count.
    scrapes: i64,
    l0_bytes: Option<u64>,
    memtable: i64,
    batch: i64,
    min_sources: Option<usize>,
}

impl Args {
    fn end_ms(&self) -> i64 {
        T0_MS + self.scrapes * SCRAPE_MS
    }

    /// Metrics' default `min_compaction_sources` unless overridden.
    fn min_sources(&self) -> usize {
        self.min_sources.unwrap_or(default_slatedb_tuning().1)
    }
}

fn parse_args() -> Args {
    let mut args = Args {
        state: State::Compacted,
        baseline: false,
        block_cache: true,
        query: None,
        seconds: 15,
        trace: 0,
        scrapes: 2 * SCRAPES_PER_HOUR,
        l0_bytes: None,
        memtable: 0,
        batch: 1,
        min_sources: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut value = || it.next().unwrap_or_else(|| panic!("{arg} needs a value"));
        match arg.as_str() {
            "--state" => {
                args.state = match value().as_str() {
                    "compacted" => State::Compacted,
                    "streamed" => State::Streamed,
                    "l0" => State::L0,
                    "realtime" => State::Realtime,
                    "realtime-compacted" => State::RealtimeCompacted,
                    other => panic!("unknown state {other}"),
                }
            }
            "--baseline" => args.baseline = true,
            "--cache" => args.block_cache = value() == "block",
            "--query" => args.query = Some(value()).filter(|q| q != "all"),
            "--seconds" => args.seconds = value().parse().expect("seconds"),
            "--trace" => args.trace = value().parse().expect("trace iterations"),
            "--scrapes" => args.scrapes = value().parse().expect("scrapes"),
            "--l0-bytes" => args.l0_bytes = Some(value().parse().expect("l0-bytes")),
            "--memtable" => args.memtable = value().parse().expect("memtable"),
            "--batch" => args.batch = value().parse().expect("batch"),
            "--min-sources" => args.min_sources = Some(value().parse().expect("min-sources")),
            "--unpin-after" => {
                value();
                eprintln!("--unpin-after is ignored: idle buckets no longer hold a snapshot");
            }
            "--bench" => {}
            other => panic!("unknown argument {other}"),
        }
    }
    assert!(
        args.batch > 0 && args.memtable % args.batch == 0,
        "memtable must be a multiple of batch"
    );
    if args.state == State::RealtimeCompacted {
        // Small enough that each bucket's single write freezes the memtable.
        args.l0_bytes.get_or_insert(1 << 20);
    }
    args
}

fn settings_file(args: &Args) -> String {
    let path = std::env::temp_dir().join(format!(
        "query-profile-slatedb-{}-{}.json",
        std::process::id(),
        args.state as u8
    ));
    let extra = match args.state {
        State::L0 => r#","l0_max_ssts":4096,"l0_max_ssts_per_key":4096,
            "compactor_options":{"scheduler_options":{"min_compaction_sources":"100000"}}"#
            .to_string(),
        // The short poll intervals only compress the compactor's reaction
        // time; production polls every 5 s against L0 flushes minutes apart.
        _ if args.state.paced() => {
            let scheduler = args
                .min_sources
                .map(|n| format!(r#""min_compaction_sources":"{n}""#))
                .unwrap_or_default();
            let l0_bytes = args
                .l0_bytes
                .map(|n| format!(r#","l0_sst_size_bytes":{n}"#))
                .unwrap_or_default();
            format!(
                r#","manifest_poll_interval":"100ms"{l0_bytes},
                "compactor_options":{{"poll_interval":"100ms","commit_compacted_interval":"100ms",
                    "scheduler_options":{{{scheduler}}},
                    "worker":{{"compactions_poll_interval":"100ms"}}}}"#
            )
        }
        _ => String::new(),
    };
    std::fs::write(&path, format!(r#"{{"compression_codec":"Zstd"{extra}}}"#))
        .expect("write settings");
    path.to_string_lossy().into_owned()
}

/// A `metrics` recorder keeping every counter and gauge SlateDB registers
/// (it reports through `MetricsRsRecorder`), so the bench can read L0 /
/// sorted-run counts, compactor activity, write bytes and merge operands.
mod stats {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, LazyLock, Mutex};

    use metrics::{
        Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };

    pub const L0_SSTS: &str = "slatedb.db.l0_sst_count";
    pub const SEGMENT_MAX_L0_SSTS: &str = "slatedb.db.segment_max_l0_sst_count";
    pub const SORTED_RUNS: &str = "slatedb.db.sorted_run_count";
    pub const SSTS: &str = "slatedb.db.sst_count";
    pub const L0_FLUSHES: &str = "slatedb.memtable_flush.l0_flush_count";
    pub const WAL_FLUSH_BYTES: &str = "slatedb.wal.wal_flush_bytes";
    pub const OBJECT_REQUESTS: &str = "slatedb.object_store.request_count";
    pub const MEMTABLE_FREEZES: &str = "slatedb.memtable_flush.memtable_freeze_count";
    pub const L0_FLUSH_BYTES: &str = "slatedb.db.l0_flush_bytes";
    pub const MEMTABLE_WRITE_BYTES: &str = "slatedb.db.memtable_write_bytes";
    pub const RUNNING_COMPACTIONS: &str = "slatedb.compactor.running_compactions";
    pub const COMPACTED_BYTES: &str = "slatedb.compactor.bytes_compacted";
    pub const COMPACTED_SSTS: &str = "slatedb.compactor.ssts_written";
    pub const MERGE_OPERANDS: &str = "slatedb.merge_operator_operands";

    /// Name (with `{k=v,..}` labels) to `(is_gauge, value)`; gauges hold
    /// `f64` bits.
    type Registry = BTreeMap<String, (bool, Arc<AtomicU64>)>;

    static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(Default::default);

    struct Capture;

    impl Recorder for Capture {
        fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
            Counter::from_arc(cell(key, false))
        }

        fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
            Gauge::from_arc(cell(key, true))
        }

        fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    fn cell(key: &Key, gauge: bool) -> Arc<AtomicU64> {
        let labels: Vec<String> = key
            .labels()
            .map(|label| format!("{}={}", label.key(), label.value()))
            .collect();
        let name = if labels.is_empty() {
            key.name().to_string()
        } else {
            format!("{}{{{}}}", key.name(), labels.join(","))
        };
        let mut registry = REGISTRY.lock().expect("registry");
        registry
            .entry(name)
            .or_insert_with(|| (gauge, Arc::default()))
            .1
            .clone()
    }

    /// Must run before the database opens: handles registered earlier stay
    /// no-ops.
    pub fn install() {
        metrics::set_global_recorder(Capture).expect("install recorder");
    }

    fn value(gauge: bool, cell: &AtomicU64) -> f64 {
        let raw = cell.load(Ordering::Relaxed);
        if gauge {
            f64::from_bits(raw)
        } else {
            raw as f64
        }
    }

    /// `name` summed over its label sets whose labels contain `label`.
    pub fn get(name: &str, label: &str) -> f64 {
        let registry = REGISTRY.lock().expect("registry");
        registry
            .iter()
            .filter(|(key, _)| key.split('{').next() == Some(name) && key.contains(label))
            .map(|(_, (gauge, cell))| value(*gauge, cell))
            .sum()
    }

    pub fn dump(prefix: &str) {
        let registry = REGISTRY.lock().expect("registry");
        for (key, (gauge, cell)) in registry.iter().filter(|(k, _)| k.starts_with(prefix)) {
            println!("  stat {key} = {}", value(*gauge, cell));
        }
    }
}

/// Polls every 250 ms until the compactor has caught up: idle, every
/// segment's L0 below `min_sources` (so the size-tiered scheduler has
/// nothing left to start), and the manifest view unchanged across two
/// polls. Gives up after 60 s with a warning.
async fn settle(min_sources: usize) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let (mut last, mut stable) = (None, 0);
    while stable < 2 {
        if Instant::now() > deadline {
            println!("warning: compactor did not settle; continuing");
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
        let snapshot = [
            stats::get(stats::SEGMENT_MAX_L0_SSTS, ""),
            stats::get(stats::SORTED_RUNS, ""),
            stats::get(stats::COMPACTED_SSTS, ""),
        ];
        let caught_up =
            stats::get(stats::RUNNING_COMPACTIONS, "") == 0.0 && snapshot[0] < min_sources as f64;
        stable = if caught_up && last == Some(snapshot) {
            stable + 1
        } else {
            0
        };
        last = Some(snapshot);
    }
}

async fn open(args: &Args) -> TimeSeriesDb {
    let cache = |capacity| {
        BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
            capacity,
            shards: None,
        })
    };
    let storage = SlateDbStorageConfig {
        path: "query-profile".to_string(),
        object_store: ObjectStoreConfig::InMemory,
        settings_path: Some(settings_file(args)),
        block_cache: args.block_cache.then(|| cache(512 << 20)),
        meta_cache: args.block_cache.then(|| cache(128 << 20)),
    };
    TimeSeriesDb::open(Config {
        storage,
        ..Default::default()
    })
    .await
    .expect("open")
}

async fn load(db: &TimeSeriesDb, namespace: &Namespace, args: &mut Args) -> u64 {
    if args.state == State::Realtime {
        args.scrapes = load_realtime(db, namespace, args).await;
        return (args.scrapes as usize * corpus().len()) as u64;
    }
    let mut generators = corpus();
    let mut samples = 0;
    if matches!(args.state, State::Compacted | State::RealtimeCompacted) {
        let mut start = 0;
        while start < args.scrapes {
            let end = (start + SCRAPES_PER_HOUR).min(args.scrapes);
            let batch: Vec<Series> = generators
                .iter_mut()
                .map(|generator| {
                    let points = (start..end)
                        .map(|scrape| {
                            Sample::new(generator.timestamp(scrape), generator.next_value(scrape))
                        })
                        .collect();
                    series(generator, points)
                })
                .collect();
            samples += batch.iter().map(|s| s.samples.len() as u64).sum::<u64>();
            db.write_with_visibility(namespace, batch, Visibility::Durable)
                .await
                .expect("write");
            start = end;
        }
    } else {
        for scrape in 0..args.scrapes {
            let batch: Vec<Series> = generators
                .iter_mut()
                .map(|generator| {
                    let point =
                        Sample::new(generator.timestamp(scrape), generator.next_value(scrape));
                    series(generator, vec![point])
                })
                .collect();
            samples += batch.len() as u64;
            let visibility = if (scrape + 1) % SCRAPES_PER_L0_FLUSH == 0 {
                Visibility::Durable
            } else {
                Visibility::Written
            };
            db.write_with_visibility(namespace, batch, visibility)
                .await
                .expect("write");
        }
    }
    db.flush().await.expect("flush");
    if args.state.paced() {
        settle(args.min_sources()).await;
    }
    samples
}

/// Production pacing: one merge operand per series every `batch` scrapes
/// (`Visibility::Written`, no WAL-durability wait), SlateDB freezing the
/// memtable at `l0_sst_size_bytes`, and after every freeze a wait for the
/// L0 upload and the compactor. Stops once at least `scrapes` are loaded
/// and exactly `memtable` scrapes sit unflushed in the memtable; returns
/// the scrape count.
async fn load_realtime(db: &TimeSeriesDb, namespace: &Namespace, args: &Args) -> i64 {
    let mut generators = corpus();
    let mut pending: Vec<Vec<Sample>> = vec![Vec::new(); generators.len()];
    let (mut freezes, mut last_freeze) = (Vec::new(), 0);
    let mut scrape = 0;
    loop {
        for (generator, points) in generators.iter_mut().zip(&mut pending) {
            points.push(Sample::new(
                generator.timestamp(scrape),
                generator.next_value(scrape),
            ));
        }
        scrape += 1;
        if scrape % args.batch != 0 {
            continue;
        }
        let batch: Vec<Series> = generators
            .iter()
            .zip(&mut pending)
            .map(|(generator, points)| series(generator, std::mem::take(points)))
            .collect();
        db.write_with_visibility(namespace, batch, Visibility::Written)
            .await
            .expect("write");
        let frozen = stats::get(stats::MEMTABLE_FREEZES, "");
        if frozen > freezes.len() as f64 {
            freezes.push(scrape);
            last_freeze = scrape;
            while stats::get(stats::L0_FLUSHES, "") < frozen {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            settle(args.min_sources()).await;
        }
        if scrape >= args.scrapes && scrape - last_freeze == args.memtable {
            break;
        }
        assert!(
            scrape < args.scrapes + 4 * SCRAPES_PER_HOUR,
            "memtable never held {} scrapes; freezes at {freezes:?}",
            args.memtable
        );
    }
    let gaps: Vec<i64> = freezes.windows(2).map(|w| w[1] - w[0]).collect();
    println!("l0 freezes after scrapes {freezes:?} (gaps {gaps:?})");
    scrape
}

fn query_range(end_ms: i64) -> RangeInclusive<SystemTime> {
    let ms = |ms: i64| UNIX_EPOCH + Duration::from_millis(ms as u64);
    ms(end_ms - HOUR_MS)..=ms(end_ms)
}

fn print_storage_state(args: &Args) {
    let memtable = if args.state == State::Realtime {
        args.memtable
    } else {
        0
    };
    println!(
        "fan-in: l0_ssts={} segment_max_l0_ssts={} sorted_runs={} ssts={} \
         memtable_scrapes={memtable} memtable_operands_per_key={}",
        stats::get(stats::L0_SSTS, ""),
        stats::get(stats::SEGMENT_MAX_L0_SSTS, ""),
        stats::get(stats::SORTED_RUNS, ""),
        stats::get(stats::SSTS, ""),
        memtable / args.batch,
    );
    println!(
        "tuning: l0_sst_size_bytes={} min_compaction_sources={}",
        args.l0_bytes
            .map_or(default_slatedb_tuning().0 as u64, |bytes| bytes),
        args.min_sources(),
    );
    let hours = args.scrapes as f64 / SCRAPES_PER_HOUR as f64;
    let per_hour = |name| stats::get(name, "") / hours;
    let mib = |name| per_hour(name) / (1 << 20) as f64;
    let puts = |component: &str| {
        stats::get(
            stats::OBJECT_REQUESTS,
            &format!("component={component},op=put"),
        ) / hours
    };
    println!(
        "write cost per hour: l0_flushes={:.1} l0_flush_mib={:.2} compacted_ssts={:.1} \
         compacted_mib={:.2} wal_mib={:.2} memtable_write_mib={:.2} \
         writer_puts={:.1} compactor_puts={:.1}",
        per_hour(stats::L0_FLUSHES),
        mib(stats::L0_FLUSH_BYTES),
        per_hour(stats::COMPACTED_SSTS),
        mib(stats::COMPACTED_BYTES),
        mib(stats::WAL_FLUSH_BYTES),
        mib(stats::MEMTABLE_WRITE_BYTES),
        puts("db"),
        puts("compactor"),
    );
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e3
}

#[derive(Default)]
struct QueryRuns {
    query_times: Vec<Duration>,
    serialize_times: Vec<Duration>,
    read_operands: f64,
    shape: (usize, usize, usize),
}

/// Loops `query` for `secs` over every database in `dbs`, one iteration
/// each in turn so background load hits them alike, and prints each one's
/// timings. Returns each database's `(mean, p50)` query milliseconds.
async fn run_query(
    dbs: &[(&TimeSeriesDb, String)],
    namespace: &Namespace,
    query: &str,
    secs: u64,
    end_ms: i64,
) -> Vec<(f64, f64)> {
    let step = Duration::from_secs(15);
    let read_operands = || stats::get(stats::MERGE_OPERANDS, "path=read");
    let names: Vec<&str> = dbs.iter().map(|(_, name)| name.as_str()).collect();
    println!("BEGIN {} pid={}", names.join(","), std::process::id());
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut runs: Vec<QueryRuns> = dbs.iter().map(|_| QueryRuns::default()).collect();
    while Instant::now() < deadline {
        for ((db, _), run) in dbs.iter().zip(&mut runs) {
            let operands_before = read_operands();
            let start = Instant::now();
            let result = db
                .query_range(namespace, query, query_range(end_ms), step)
                .await
                .expect("query");
            let queried = Instant::now();
            run.read_operands += read_operands() - operands_before;
            let points = result.iter().map(|s| s.samples.len()).sum::<usize>();
            let series_count = result.len();
            let body = serde_json::to_vec(&range_result_to_response(Ok(result))).expect("json");
            run.shape = (series_count, points, body.len());
            run.query_times.push(queried - start);
            run.serialize_times.push(queried.elapsed());
        }
    }
    println!("END {}", names.join(","));
    let mean = |times: &[Duration]| ms(times.iter().sum::<Duration>()) / times.len() as f64;
    let mut out = Vec::with_capacity(runs.len());
    for (name, mut run) in names.iter().zip(runs) {
        run.query_times.sort();
        let iterations = run.query_times.len();
        let (series_count, points, bytes) = run.shape;
        let (query_mean, query_p50) = (mean(&run.query_times), ms(run.query_times[iterations / 2]));
        println!(
            "{name}: iterations={iterations} query_mean_ms={query_mean:.2} \
             query_p50_ms={query_p50:.2} serialize_mean_ms={:.2} series={series_count} \
             points={points} json_bytes={bytes} read_merge_operands_per_query={:.0}",
            mean(&run.serialize_times),
            run.read_operands / iterations as f64,
        );
        out.push((query_mean, query_p50));
    }
    out
}

async fn trace_query(
    db: &TimeSeriesDb,
    namespace: &Namespace,
    (name, query): (&str, &str),
    n: usize,
    end_ms: i64,
) {
    let step = Duration::from_secs(15);
    let mut totals = std::collections::BTreeMap::<String, (f64, u64)>::new();
    let mut wall = Duration::ZERO;
    for _ in 0..n {
        let start = Instant::now();
        let (_, trace) = query_range_traced(db, namespace, query, query_range(end_ms), step).await;
        wall += start.elapsed();
        let mut add = |key: String, value: &serde_json::Value, calls: u64| {
            let entry = totals.entry(key).or_default();
            entry.0 += value.as_f64().unwrap_or(0.0);
            entry.1 += calls;
        };
        for phase in trace["phases"].as_array().into_iter().flatten() {
            add(
                format!("phase.{}", phase["name"].as_str().unwrap_or("?")),
                &phase["elapsedMs"],
                1,
            );
        }
        for io in trace["io"].as_array().into_iter().flatten() {
            let kind = io["kind"].as_str().unwrap_or("?");
            let calls = io["callCount"].as_u64().unwrap_or(0);
            add(format!("io.{kind}.cumulative"), &io["cumulativeMs"], calls);
            add(format!("io.{kind}.wall"), &io["wallMs"], 0);
            add(format!("io.{kind}.bytes"), &io["bytes"], 0);
        }
        for op in trace["operators"].as_array().into_iter().flatten() {
            let op_name = op["opName"].as_str().unwrap_or("?");
            add(
                format!("op.{}.{op_name}", op["nodeId"]),
                &op["totalMs"],
                op["callCount"].as_u64().unwrap_or(0),
            );
        }
    }
    println!(
        "TRACE {name}: iterations={n} wall_mean_ms={:.2}",
        ms(wall) / n as f64
    );
    for (key, (value, calls)) in totals {
        println!(
            "  {key:<48} {:>12.3} calls/iter={}",
            value / n as f64,
            calls / n as u64
        );
    }
}

fn main() {
    let mut args = parse_args();
    stats::install();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let db = open(&args).await;
        let namespace = Namespace::default();
        let start = Instant::now();
        let samples = load(&db, &namespace, &mut args).await;
        if args.state == State::Streamed {
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
        println!(
            "loaded series={} samples={samples} in {:.1}s",
            (COUNTERS + GAUGES + RATIOS) * COMBOS,
            start.elapsed().as_secs_f64()
        );
        print_storage_state(&args);
        if std::env::var_os("QUERY_PROFILE_DUMP_STATS").is_some() {
            stats::dump("slatedb.");
        }
        let end_ms = args.end_ms();
        let baseline = if args.baseline {
            // Same `min_sources`: both databases report into the same
            // SlateDB gauges, which `settle` compares against it.
            let mut baseline_args = Args {
                state: State::RealtimeCompacted,
                l0_bytes: Some(1 << 20),
                min_sources: Some(args.min_sources()),
                ..args.clone()
            };
            let baseline = open(&baseline_args).await;
            load(&baseline, &namespace, &mut baseline_args).await;
            Some(baseline)
        } else {
            None
        };
        let selected = QUERIES
            .iter()
            .filter(|(name, _)| args.query.as_deref().is_none_or(|q| q == *name));
        for db in std::iter::once(&db).chain(&baseline) {
            for (_, query) in selected.clone() {
                db.query_range(
                    &namespace,
                    query,
                    query_range(end_ms),
                    Duration::from_secs(15),
                )
                .await
                .expect("warm-up query");
            }
        }
        for &(name, query) in selected {
            if args.trace > 0 {
                trace_query(&db, &namespace, (name, query), args.trace, end_ms).await;
            }
            let mut dbs = vec![(&db, name.to_string())];
            dbs.extend(baseline.iter().map(|b| (b, format!("{name}@compacted"))));
            let times = run_query(&dbs, &namespace, query, args.seconds, end_ms).await;
            if let [(mean, p50), (base_mean, base_p50)] = times[..] {
                println!(
                    "RATIO {name}: {mean:.2} / {base_mean:.2} = {:.2}x \
                     p50 {p50:.2} / {base_p50:.2} = {:.2}x",
                    mean / base_mean,
                    p50 / base_p50,
                );
            }
        }
    });
}
