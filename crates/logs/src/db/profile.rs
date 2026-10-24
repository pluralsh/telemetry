//! Temporary phase profiler for the log query read path.

use std::time::{Duration, Instant};

use common::WarmWindow;
use common::storage::config::{
    BlockCacheConfig, FoyerMemoryCacheConfig, LocalObjectStoreConfig, ObjectStoreConfig,
    SlateDbStorageConfig, StorageConfig,
};
use common::storage::slate::warm::{
    WarmBudget, WarmLedger, WarmPlan, WarmReport, WarmShares, warm_planned,
};
use slatedb::config::DbReaderOptions;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::config::{CompactionConfig, PageConfig};
use crate::{Direction, LogBatch, LogEntry, QueryOptions, QueryRequest};

// The server's allocator, so profiles weigh allocation as it does.
// `profile-system-allocator` measures live memory without its retention.
#[cfg(not(feature = "profile-system-allocator"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const RUN: &str = "profile-run";
const SERVICES: [&str; 6] = [
    "frontend",
    "cart",
    "checkout",
    "payments",
    "inventory",
    "auth",
];
const WORDS: [&str; 6] = [
    "GET /api/cart",
    "POST /login",
    "timeout",
    "ok",
    "retry",
    "error",
];
const SECOND: i64 = 1_000_000_000;

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

fn config(path: &std::path::Path, production: bool) -> Config {
    let cache = |capacity| {
        Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
            capacity,
            shards: None,
        }))
    };
    Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "profile".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: path.display().to_string(),
            }),
            settings_path: None,
            block_cache: cache(64 << 20),
            meta_cache: cache(16 << 20),
            disk: Default::default(),
        }),
        segment_duration: Duration::from_secs(if production { 3600 } else { 60 }),
        discovery_rollup: Some(Duration::from_secs(24 * 3600)),
        stream_locality_labels: Vec::new(),
        stream_id_block_size: 0,
        retention: None,
        page: if production {
            PageConfig::default()
        } else {
            PageConfig {
                target_size_bytes: 16384,
                max_rows: 128,
                ..PageConfig::default()
            }
        },
        compaction: CompactionConfig {
            enabled: false,
            ..CompactionConfig::default()
        },
        write_buffer: Default::default(),
        block_cache_capacity_bytes: crate::DEFAULT_BLOCK_CACHE_CAPACITY_BYTES,
        result_cache_capacity_bytes: 0,
        memory: Default::default(),
    }
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

async fn profile(production: bool) {
    let rounds: usize = std::env::var("PROFILE_ROUNDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(50);
    let dir = tempfile::tempdir().unwrap();
    let namespace = &Namespace::new("profile").unwrap();
    let now = common::time::now_ns();
    let window = 1200 * SECOND;
    let writer = LogDb::open(config(dir.path(), production)).await.unwrap();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for round in 0..rounds {
        let batches = (0..50)
            .map(|stream| {
                let labels = Labels::new(vec![
                    Label::new("job", "fuzz"),
                    Label::new("fuzz_run", RUN),
                    Label::new("service", SERVICES[stream % SERVICES.len()]),
                    Label::new("pod", format!("pod-{round}-{stream}")),
                ])
                .unwrap();
                let mut entries = (0..12)
                    .map(|_| {
                        let ts = now - window + (rng.below(window as u64) as i64);
                        let word = WORDS[rng.below(WORDS.len() as u64) as usize];
                        LogEntry::new(ts, format!("{word} request id={}", rng.next()))
                    })
                    .collect::<Vec<_>>();
                entries.sort_by_key(|entry| entry.timestamp_ns);
                LogBatch::new(labels, entries)
            })
            .collect();
        writer
            .write_with_durability(namespace, batches, Durability::Durable)
            .await
            .unwrap();
    }
    writer.close().await.unwrap();
    let db = LogDb::open_reader(config(dir.path(), production), DbReaderOptions::default())
        .await
        .unwrap();
    let (start, end) = (now - window - 60 * SECOND, now);
    let selector = format!(r#"{{job="fuzz", fuzz_run="{RUN}"}}"#);
    let queries = [
        (selector.clone(), None),
        (format!(r#"{selector} |= "timeout""#), None),
        (
            format!("sum by (service) (count_over_time({selector}[1m]))"),
            Some(15 * SECOND),
        ),
    ];
    println!(
        "== {} config ({rounds} rounds) ==",
        if production {
            "production"
        } else {
            "regression"
        }
    );
    for (query, step_ns) in &queries {
        let request = QueryRequest {
            query: query.clone(),
            start_ns: start,
            end_ns: end,
            step_ns: *step_ns,
        };
        let options = QueryOptions {
            limit: 5000,
            direction: Direction::Forward,
            max_pages: 1_000_000,
            ..QueryOptions::default()
        };
        db.query(namespace, &request, options.clone())
            .await
            .unwrap();
        let started = Instant::now();
        db.query(namespace, &request, options).await.unwrap();
        let total = started.elapsed();
        println!("query {:>7.2}ms | {query}", ms(total));
    }

    let filter = StreamFilter::exact(vec![Label::new("job", "fuzz"), Label::new("fuzz_run", RUN)]);
    let now_unix_ms = unix_time_ms().unwrap();
    let segments = db.segment_ids((start, end), false).unwrap();
    let (mut ids_t, mut labels_t, mut meta_t) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let (mut streams, mut pages, mut live_segments) = (0, 0, 0);
    for &segment in &segments {
        let started = Instant::now();
        let ids = db
            .stream_ids(namespace, segment, &filter.exact)
            .await
            .unwrap();
        ids_t += started.elapsed();
        if !ids.is_empty() {
            live_segments += 1;
        }
        streams += ids.len();
        let started = Instant::now();
        db.stream_labels(namespace, segment, ids).await.unwrap();
        labels_t += started.elapsed();
        let started = Instant::now();
        let found = db
            .segment_streams(namespace, segment, &filter, (start, end), now_unix_ms)
            .await
            .unwrap();
        meta_t += started.elapsed();
        pages += found.iter().map(|stream| stream.runs.len()).sum::<usize>();
    }
    let (mut forward_t, mut forward_n, mut metadata_t, mut metadata_n) =
        (Duration::ZERO, 0, Duration::ZERO, 0);
    for &segment in &segments {
        let started = Instant::now();
        let mut iterator = db
            .storage
            .scan_prefix_iter(
                crate::codec::forward_prefix(namespace, segment),
                common::BytesRange::unbounded(),
                None,
            )
            .await
            .unwrap();
        while let Some(record) = iterator.next().await.unwrap() {
            crate::codec::decode_labels(&record.value).unwrap();
            forward_n += 1;
        }
        forward_t += started.elapsed();
        let started = Instant::now();
        let mut iterator = db
            .storage
            .scan_prefix_iter(
                crate::codec::segment_run_prefix(namespace, segment),
                common::BytesRange::unbounded(),
                None,
            )
            .await
            .unwrap();
        while let Some(record) = iterator.next().await.unwrap() {
            crate::codec::decode_run(&record.value).unwrap();
            metadata_n += 1;
        }
        metadata_t += started.elapsed();
    }
    println!(
        "  segment-wide scans: forward labels {forward_n} records {:.2}ms  run records {metadata_n} records {:.2}ms",
        ms(forward_t),
        ms(metadata_t)
    );
    let started = Instant::now();
    let targets = db
        .scan_targets(namespace, start, end, &filter)
        .await
        .unwrap();
    let scan_t = started.elapsed();
    let started = Instant::now();
    let rows = db
        .read_bounded(namespace, targets, &PageBudget::new(usize::MAX))
        .await
        .unwrap();
    let read_t = started.elapsed();
    let started = Instant::now();
    let mut sequential_rows = 0;
    db.read_segments(
        namespace,
        (start, end),
        &filter,
        &PageBudget::new(usize::MAX),
        false,
        |chunk| {
            sequential_rows += chunk.len();
            Ok(ControlFlow::Continue(()))
        },
    )
    .await
    .unwrap();
    let segments_t = started.elapsed();
    println!(
        "segments {} (live {live_segments}) stream-segments {streams} pages {pages} rows {}",
        segments.len(),
        rows.len()
    );
    println!(
        "  serial per segment: stream_ids {:.2}ms  stream_labels {:.2}ms  segment_streams(ids+labels+metadata) {:.2}ms",
        ms(ids_t),
        ms(labels_t),
        ms(meta_t)
    );
    println!(
        "  scan_targets (concurrent segments) {:.2}ms  read_bounded {:.2}ms  read_segments (sequential, early-stop path) {:.2}ms rows {sequential_rows}",
        ms(scan_t),
        ms(read_t),
        ms(segments_t)
    );
    db.close().await.unwrap();
}

#[derive(serde::Deserialize)]
struct ReplayStream {
    stream: BTreeMap<String, String>,
    values: Vec<(String, String)>,
}

#[derive(serde::Deserialize)]
struct ReplayQuery {
    id: String,
    query: String,
    start: i64,
    end: i64,
    step: Option<i64>,
    oracle_ms: f64,
    impl_ms: f64,
    limit: Option<usize>,
    direction: Option<String>,
}

/// Replays a fuzz run's data rounds (`PROFILE_REPLAY/rounds.json`) and times
/// its slow queries (`PROFILE_REPLAY/queries.json`) by phase.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_fuzz_replay() {
    let replay = std::path::PathBuf::from(std::env::var("PROFILE_REPLAY").unwrap());
    let rounds: Vec<Vec<ReplayStream>> =
        serde_json::from_slice(&std::fs::read(replay.join("rounds.json")).unwrap()).unwrap();
    let queries: Vec<ReplayQuery> =
        serde_json::from_slice(&std::fs::read(replay.join("queries.json")).unwrap()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let namespace = Namespace::new("regression").unwrap();
    // Reads an existing server's store (`PROFILE_DATA`, `PROFILE_DATA_PATH`)
    // instead of ingesting the rounds.
    let existing = std::env::var("PROFILE_DATA").ok();
    let mut config = config(
        existing.as_deref().map_or(dir.path(), std::path::Path::new),
        true,
    );
    if let (Some(_), StorageConfig::SlateDb(storage)) = (&existing, &mut config.storage) {
        storage.path = std::env::var("PROFILE_DATA_PATH").unwrap();
    }
    config.compaction.enabled = std::env::var("PROFILE_COMPACT").is_ok();
    config.compaction.min_age = Duration::ZERO;
    if existing.is_none() {
        let settle = std::env::var("PROFILE_COMPACT").map_or(Duration::ZERO, |secs| {
            Duration::from_secs(secs.parse().unwrap())
        });
        ingest(&config, &namespace, rounds, settle).await;
    }
    let db = LogDb::open_reader(config, DbReaderOptions::default())
        .await
        .unwrap();
    replay_queries(&db, &namespace, &queries).await;
    db.close().await.unwrap();
}

/// Writes `rounds`, then gives compaction `settle` to catch up when enabled.
async fn ingest(
    config: &Config,
    namespace: &Namespace,
    rounds: Vec<Vec<ReplayStream>>,
    settle: Duration,
) {
    let writer = LogDb::open(config.clone()).await.unwrap();
    for round in rounds {
        let batches = round
            .into_iter()
            .map(|stream| {
                let labels = Labels::new(
                    stream
                        .stream
                        .into_iter()
                        .map(|(name, value)| Label::new(name, value))
                        .collect(),
                )
                .unwrap();
                let mut entries = stream
                    .values
                    .into_iter()
                    .map(|(ts, line)| LogEntry::new(ts.parse().unwrap(), line))
                    .collect::<Vec<_>>();
                entries.sort_by_key(|entry| entry.timestamp_ns);
                LogBatch::new(labels, entries)
            })
            .collect();
        writer
            .write_with_durability(namespace, batches, Durability::Durable)
            .await
            .unwrap();
    }
    if config.compaction.enabled {
        tokio::time::sleep(settle).await;
    }
    writer.close().await.unwrap();
}

async fn replay_queries(db: &LogDb, namespace: &Namespace, queries: &[ReplayQuery]) {
    for query in queries {
        let mut options = QueryOptions::default();
        if let Some(limit) = query.limit {
            options.limit = limit;
        }
        if query.direction.as_deref() == Some("backward") {
            options.direction = Direction::Backward;
        }
        let request = QueryRequest {
            query: query.query.clone(),
            start_ns: query.start,
            end_ns: query.end,
            step_ns: query.step,
        }
        .frontend_step_aligned();
        db.query(namespace, &request, options.clone())
            .await
            .unwrap();
        // Loops a query long enough for a sampling profiler to attach.
        let repeat = std::env::var("PROFILE_REPEAT").map_or(0, |value| value.parse().unwrap());
        for _ in 0..repeat {
            db.query(namespace, &request, options.clone())
                .await
                .unwrap();
        }
        let runs = std::env::var("PROFILE_RUNS").map_or(1, |value| value.parse().unwrap());
        let mut times = Vec::new();
        for _ in 0..runs {
            let started = Instant::now();
            db.query(namespace, &request, options.clone())
                .await
                .unwrap();
            times.push(started.elapsed());
        }
        times.sort();
        let total = times[times.len() / 2];
        if runs > 1 {
            println!(
                "{} median {:.1}ms min {:.1}ms",
                query.id,
                ms(total),
                ms(times[0])
            );
        }

        let parsed = crate::logql::parse(&request.query).unwrap();
        let plan = crate::query::ScanPlan::new(&request, &parsed, &options).unwrap();
        let started = Instant::now();
        let targets = db
            .scan_targets(namespace, plan.scan_start, request.end_ns, &plan.streams)
            .await
            .unwrap();
        let scan_t = started.elapsed();
        let estimate = targets.estimate(plan.is_lineless());
        let streams = targets
            .segments
            .iter()
            .map(|(_, streams)| streams.len())
            .sum::<usize>();
        let started = Instant::now();
        let rows = db
            .read_bounded(namespace, targets, &PageBudget::new(usize::MAX))
            .await
            .unwrap();
        let read_t = started.elapsed();
        println!(
            "{} total {:>7.1}ms (fuzz impl {:.0}ms, loki {:.0}ms) | scan_targets {:.1}ms read {:.1}ms rest {:.1}ms | segments {} streams {streams} pages {} read-units {} ({} compressed bytes) rows {} steps {}",
            query.id,
            ms(total),
            query.impl_ms,
            query.oracle_ms,
            ms(scan_t),
            ms(read_t),
            ms(total.saturating_sub(scan_t + read_t)),
            plan_segments(db, plan.scan_start, request.end_ns),
            estimate.pages,
            estimate.read_units,
            estimate.compressed_bytes,
            rows.len(),
            request
                .step_ns
                .map_or(1, |step| (request.end_ns - request.start_ns) / step + 1),
        );
    }
}

/// One matched fuzz case, as flattened from `cases.jsonl` with the server's
/// parameter defaults applied.
#[derive(serde::Deserialize)]
struct FuzzCase {
    id: String,
    family: String,
    kind: String,
    impl_ms: f64,
    query: Option<String>,
    start: Option<i64>,
    end: Option<i64>,
    step: Option<i64>,
    time: Option<i64>,
    limit: Option<usize>,
    direction: Option<String>,
    name: Option<String>,
    selectors: Option<Vec<String>>,
}

fn classify_key(key: &[u8]) -> &'static str {
    match crate::codec::key_record_type(key) {
        None => "segment",
        Some(1) => "next_stream_id",
        Some(2) => "stream_dictionary",
        Some(3) => "forward_labels",
        Some(4) => "label_postings",
        Some(5) => "run",
        Some(6) => "object_block",
        Some(7) => "next_object_id",
        Some(8) => "search_field_stats",
        Some(9) => "search_term_stats",
        Some(10) => "search_term_directory",
        Some(11) => "search_posting_block",
        Some(12) => "object_tombstone",
        Some(13) => "rollup",
        Some(14) => "object_directory",
        Some(15) => "summary",
        Some(_) => "catalog",
    }
}

async fn run_case(db: &LogDb, namespace: &Namespace, case: &FuzzCase) -> Result<usize> {
    let span = || (case.start.unwrap(), case.end.unwrap());
    Ok(match case.kind.as_str() {
        "labels" => {
            let (start, end) = span();
            db.label_names(namespace, start, end).await?.len()
        }
        "label_values" => {
            let (start, end) = span();
            db.label_values(namespace, case.name.as_deref().unwrap(), start, end)
                .await?
                .len()
        }
        "series" => {
            let (start, end) = span();
            db.series(namespace, case.selectors.as_deref().unwrap(), start, end)
                .await?
                .len()
        }
        kind => {
            let query = case.query.clone().unwrap();
            let request = match kind {
                "range" => {
                    let (start, end) = span();
                    QueryRequest::range(query, start, end, case.step.unwrap())
                        .frontend_step_aligned()
                }
                "instant_logs" => QueryRequest::instant_logs(query, case.time.unwrap()),
                "instant" => QueryRequest::instant(query, case.time.unwrap()),
                other => panic!("unknown case kind {other}"),
            };
            let options = QueryOptions {
                limit: case.limit.unwrap_or(100),
                direction: if case.direction.as_deref() == Some("forward") {
                    Direction::Forward
                } else {
                    Direction::Backward
                },
                ..QueryOptions::default()
            };
            match db.query(namespace, &request, options).await? {
                crate::QueryResult::Streams(streams) => streams.len(),
                crate::QueryResult::Vector(samples) => samples.len(),
                crate::QueryResult::Matrix(series) => series.len(),
                crate::QueryResult::Scalar(_) => 1,
            }
        }
    })
}

/// Replays every matched case of a fuzz run (`PROFILE_CASES/{rounds,cases}.json`,
/// written by `python -m harness.fuzz.replay logs`) against a reader, writing per-query latency
/// and storage reads by record type to `PROFILE_OUT` as JSON lines. The store
/// at `PROFILE_STORE` is ingested once and reused across runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_fuzz_cases() {
    use std::io::Write as _;

    let input = std::path::PathBuf::from(std::env::var("PROFILE_CASES").unwrap());
    let store = std::path::PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    let reps: usize = std::env::var("PROFILE_REPS").map_or(3, |value| value.parse().unwrap());
    let namespace = Namespace::new("regression").unwrap();
    let mut config = Config {
        storage: config(&store, true).storage,
        retention: None,
        ..Config::default()
    };
    config.compaction.min_age = Duration::ZERO;
    if let Ok(bytes) = std::env::var("PROFILE_BLOCK_CACHE") {
        config.block_cache_capacity_bytes = bytes.parse().unwrap();
    }
    let marker = store.join("ingested");
    if !marker.exists() {
        std::fs::create_dir_all(&store).unwrap();
        let rounds: Vec<Vec<ReplayStream>> =
            serde_json::from_slice(&std::fs::read(input.join("rounds.json")).unwrap()).unwrap();
        ingest(&config, &namespace, rounds, Duration::from_secs(30)).await;
        std::fs::write(&marker, b"").unwrap();
    }
    let cases: Vec<FuzzCase> =
        serde_json::from_slice(&std::fs::read(input.join("cases.json")).unwrap()).unwrap();

    let mut db = LogDb::open_reader(config, DbReaderOptions::default())
        .await
        .unwrap();
    let counting = Arc::new(common::storage::counting::CountingStorage::new(
        db.storage.clone(),
        classify_key,
    ));
    db.storage = counting.clone();
    let mut out = std::io::BufWriter::new(
        std::fs::File::create(std::env::var("PROFILE_OUT").unwrap()).unwrap(),
    );
    for case in &cases {
        let mut best: Option<(Duration, serde_json::Value)> = None;
        let mut outcome = Ok(0);
        for _ in 0..reps {
            counting.take();
            let started = Instant::now();
            outcome = run_case(&db, &namespace, case).await;
            let elapsed = started.elapsed();
            let io = counting
                .take()
                .into_iter()
                .map(|((op, class), stats)| {
                    (
                        format!("{op}/{class}"),
                        serde_json::json!({
                            "calls": stats.calls,
                            "records": stats.records,
                            "bytes": stats.bytes,
                            "ms": ms(stats.elapsed),
                        }),
                    )
                })
                .collect::<serde_json::Map<_, _>>();
            if best.as_ref().is_none_or(|(fastest, _)| elapsed < *fastest) {
                best = Some((elapsed, serde_json::Value::Object(io)));
            }
        }
        let (elapsed, io) = best.unwrap();
        let (results, error) = match outcome {
            Ok(results) => (results, None),
            Err(error) => (0, Some(error.to_string())),
        };
        let row = serde_json::json!({
            "id": case.id,
            "family": case.family,
            "query": case.query,
            "ms": ms(elapsed),
            "impl_ms": case.impl_ms,
            "results": results,
            "error": error,
            "io": io,
        });
        writeln!(out, "{row}").unwrap();
    }
    out.flush().unwrap();
    db.close().await.unwrap();
}

/// Replays a fuzz run as it ran: ingests `PROFILE_CASES/rounds.json` round
/// by round into a writer configured as the production standalone server
/// (1h segments, 512 MiB + 128 MiB SlateDB caches, page compaction) at a
/// fresh `PROFILE_STORE`, and after each round runs that round's cases once
/// per `PROFILE_ARMS` tuning, rotating the order case by case so the arms
/// share data, caches and host noise. Arms are `;`-separated lists of
/// `unfiltered=0|1`, `cached=0|1`, `lookahead=N`, `units=N`, `read=N` and
/// `list=N`, empty for the default. Writes one JSON line per case and arm,
/// and each round's SST layout, to `PROFILE_OUT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual profiling"]
async fn profile_fuzz_progressive() {
    use std::io::Write as _;

    let input = std::path::PathBuf::from(std::env::var("PROFILE_CASES").unwrap());
    let store = std::path::PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    assert!(!store.exists(), "PROFILE_STORE must not exist yet");
    let arms: Vec<(String, ReadTuning)> = std::env::var("PROFILE_ARMS")
        .unwrap_or_default()
        .split(';')
        .map(|spec| {
            let mut tuning = ReadTuning::default();
            for setting in spec.split(',').filter(|setting| !setting.is_empty()) {
                let (name, value) = setting.split_once('=').unwrap();
                let value: usize = value.parse().unwrap();
                match name {
                    "unfiltered" => tuning.unfiltered_reads = value != 0,
                    "cached" => tuning.cached_filters = value != 0,
                    "lookahead" => tuning.segment_lookahead = value,
                    "units" => tuning.pipeline_units = value,
                    "read" => tuning.read_concurrency = value,
                    "list" => tuning.list_concurrency = value,
                    other => panic!("unknown arm setting {other}"),
                }
            }
            let name = if spec.is_empty() { "default" } else { spec };
            (name.to_owned(), tuning)
        })
        .collect();
    let namespace = Namespace::new("regression").unwrap();
    let mut config = production_config(&store);
    config.page = PageConfig {
        target_size_bytes: 1 << 20,
        max_rows: 8192,
        rows_per_block: 256,
    };
    config.compaction = CompactionConfig::default();
    config.write_buffer.flush_interval = Duration::from_secs(10);
    config.stream_locality_labels = Config::default().stream_locality_labels;
    config.stream_id_block_size = Config::default().stream_id_block_size;
    if let Ok(ids) = std::env::var("PROFILE_STREAM_ID_BLOCK") {
        config.stream_id_block_size = ids.parse().unwrap();
    }
    if let (Ok(shards), StorageConfig::SlateDb(storage)) =
        (std::env::var("PROFILE_META_SHARDS"), &mut config.storage)
        && let Some(BlockCacheConfig::FoyerMemory(meta)) = &mut storage.meta_cache
    {
        meta.shards = Some(shards.parse().unwrap());
    }
    let rounds: Vec<Vec<ReplayStream>> =
        serde_json::from_slice(&std::fs::read(input.join("rounds.json")).unwrap()).unwrap();
    let cases: Vec<FuzzCase> =
        serde_json::from_slice(&std::fs::read(input.join("cases.json")).unwrap()).unwrap();
    let round_of = |case: &FuzzCase| -> usize { case.id[1..5].parse().unwrap() };
    let max_rounds: usize =
        std::env::var("PROFILE_ROUNDS").map_or(usize::MAX, |value| value.parse().unwrap());
    // Memtable flushes and compaction run on timers, so rounds are paced
    // as the fuzz ran them (`PROFILE_ROUND_SECS`, 17) for its SST layout.
    let pace = Duration::from_secs_f64(
        std::env::var("PROFILE_ROUND_SECS").map_or(17.0, |value| value.parse().unwrap()),
    );
    // Every object-store read waits `PROFILE_LATENCY_MS` (2), as the fuzz
    // stack's MinIO does, and is counted.
    let StorageConfig::SlateDb(slate) = &config.storage else {
        unreachable!("profiles run on SlateDB");
    };
    let gets = Arc::new(AtomicUsize::new(0));
    let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(LatencyStore {
        inner: common::storage::factory::create_object_store(&slate.object_store).unwrap(),
        latency: Duration::from_secs_f64(
            std::env::var("PROFILE_LATENCY_MS").map_or(2.0, |value| value.parse().unwrap())
                / 1000.0,
        ),
        gets: Arc::clone(&gets),
    });
    let cache = SharedDbCache::from_config(&config.storage).await.unwrap();
    let blocks = BlockCache::new(config.block_cache_capacity_bytes);
    let memory = MemoryBudget::new(&config.memory);
    let mut db = LogDb::open_over_store(config, &cache, &blocks, &memory, Some(store))
        .await
        .unwrap();
    // Queries read through a tally of their storage calls by operation.
    let counting = Arc::new(common::storage::counting::CountingStorage::new(
        db.storage.clone(),
        classify_key,
    ));
    db.storage = counting.clone();
    // The server's continuous warm pass, run between cases every 10s
    // unless `PROFILE_WARM=0`, with the first arm's tuning.
    let warm = std::env::var("PROFILE_WARM").map_or(true, |value| value != "0");
    let ledger = WarmLedger::new(true);
    let cancel = CancellationToken::new();
    let mut warmed_at = Instant::now();
    let mut out = std::io::BufWriter::new(
        std::fs::File::create(std::env::var("PROFILE_OUT").unwrap()).unwrap(),
    );
    let started = Instant::now();
    let mut turn = 0usize;
    for (round, streams) in rounds.into_iter().enumerate().take(max_rounds) {
        tokio::time::sleep_until((started + pace * u32::try_from(round).unwrap()).into()).await;
        let batches = streams
            .into_iter()
            .map(|stream| {
                let labels = Labels::new(
                    stream
                        .stream
                        .into_iter()
                        .map(|(name, value)| Label::new(name, value))
                        .collect(),
                )
                .unwrap();
                let mut entries = stream
                    .values
                    .into_iter()
                    .map(|(ts, line)| LogEntry::new(ts.parse().unwrap(), line))
                    .collect::<Vec<_>>();
                entries.sort_by_key(|entry| entry.timestamp_ns);
                LogBatch::new(labels, entries)
            })
            .collect();
        db.write_with_durability(&namespace, batches, Durability::Written)
            .await
            .unwrap();
        for case in cases.iter().filter(|case| round_of(case) == round) {
            if warm && warmed_at.elapsed() >= Duration::from_secs(10) {
                db.tuning = arms[0].1;
                let mut plan = WarmPlan::default();
                db.plan_warm(
                    std::slice::from_ref(&namespace),
                    common::storage::config::ContinuousCacheWarmerConfig::default().window(),
                    &mut plan,
                );
                warm_planned(
                    "logs",
                    plan,
                    WarmBudget::of(&cache, WarmShares::default()).unwrap_or(WarmBudget::UNBOUNDED),
                    true,
                    2,
                    &cancel,
                    &ledger,
                )
                .await;
                warmed_at = Instant::now();
            }
            turn += 1;
            for index in 0..arms.len() {
                let (name, tuning) = &arms[(index + turn) % arms.len()];
                db.tuning = *tuning;
                let gets_before = gets.load(Ordering::Relaxed);
                counting.take();
                let case_started = Instant::now();
                let outcome = run_case(&db, &namespace, case).await;
                let elapsed = case_started.elapsed();
                let mut ops = BTreeMap::<&str, (u64, u64)>::new();
                for ((op, _), stats) in counting.take() {
                    let entry = ops.entry(op).or_default();
                    entry.0 += stats.calls;
                    entry.1 += stats.records;
                }
                let row = serde_json::json!({
                    "id": case.id,
                    "round": round,
                    "family": case.family,
                    "arm": name,
                    "ms": ms(elapsed),
                    "ops": ops,
                    "gets": gets.load(Ordering::Relaxed) - gets_before,
                    "impl_ms": case.impl_ms,
                    "results": outcome.as_ref().ok(),
                    "error": outcome.as_ref().err().map(ToString::to_string),
                });
                writeln!(out, "{row}").unwrap();
            }
        }
        writeln!(
            out,
            "{}",
            serde_json::json!({ "round": round, "layout": slate_layout(&db) })
        )
        .unwrap();
        if round % 10 == 9 {
            out.flush().unwrap();
            println!(
                "round {} at {:.0}s, layout (l0 ssts, sorted runs, run ssts) per segment: {:?}",
                round + 1,
                started.elapsed().as_secs_f64(),
                slate_layout(&db)
            );
        }
    }
    out.flush().unwrap();
    db.close().await.unwrap();
}

/// An object store whose reads wait `latency` before starting, counted in
/// `gets`.
#[derive(Debug)]
struct LatencyStore {
    inner: Arc<dyn slatedb::object_store::ObjectStore>,
    latency: Duration,
    gets: Arc<AtomicUsize>,
}

impl std::fmt::Display for LatencyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LatencyStore({})", self.inner)
    }
}

#[async_trait]
impl slatedb::object_store::ObjectStore for LatencyStore {
    async fn put_opts(
        &self,
        location: &slatedb::object_store::path::Path,
        payload: slatedb::object_store::PutPayload,
        opts: slatedb::object_store::PutOptions,
    ) -> slatedb::object_store::Result<slatedb::object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &slatedb::object_store::path::Path,
        opts: slatedb::object_store::PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn slatedb::object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &slatedb::object_store::path::Path,
        options: slatedb::object_store::GetOptions,
    ) -> slatedb::object_store::Result<slatedb::object_store::GetResult> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(self.latency).await;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            slatedb::object_store::Result<slatedb::object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<
        'static,
        slatedb::object_store::Result<slatedb::object_store::path::Path>,
    > {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&slatedb::object_store::path::Path>,
    ) -> futures::stream::BoxStream<
        'static,
        slatedb::object_store::Result<slatedb::object_store::ObjectMeta>,
    > {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&slatedb::object_store::path::Path>,
    ) -> slatedb::object_store::Result<slatedb::object_store::ListResult> {
        tokio::time::sleep(self.latency).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &slatedb::object_store::path::Path,
        to: &slatedb::object_store::path::Path,
        options: slatedb::object_store::CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// L0 SSTs, sorted runs and their SSTs of each SlateDB segment with any.
fn slate_layout(db: &LogDb) -> Vec<(usize, usize, usize)> {
    let Some(slate) = db.storage.slate_read() else {
        return Vec::new();
    };
    slate
        .manifest()
        .segments()
        .iter()
        .filter(|segment| !segment.l0().is_empty() || !segment.compacted().is_empty())
        .map(|segment| {
            (
                segment.l0().len(),
                segment.compacted().len(),
                segment
                    .compacted()
                    .iter()
                    .map(|run| run.sst_views.len())
                    .sum(),
            )
        })
        .collect()
}

/// Runs one replay case (`PROFILE_CASE` id from `PROFILE_CASES/cases.json`)
/// several times against the replay store and reports how results differ.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_repeat_case() {
    let input = std::path::PathBuf::from(std::env::var("PROFILE_CASES").unwrap());
    let store = std::path::PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    let id = std::env::var("PROFILE_CASE").unwrap();
    let cases: Vec<FuzzCase> =
        serde_json::from_slice(&std::fs::read(input.join("cases.json")).unwrap()).unwrap();
    let case = cases.into_iter().find(|case| case.id == id).unwrap();
    let mut config = Config {
        storage: config(&store, true).storage,
        retention: None,
        ..Config::default()
    };
    config.compaction.min_age = Duration::ZERO;
    let db = LogDb::open_reader(config, DbReaderOptions::default())
        .await
        .unwrap();
    let namespace = Namespace::new("regression").unwrap();
    let (start, end) = (case.start.unwrap(), case.end.unwrap());
    let request = QueryRequest::range(case.query.clone().unwrap(), start, end, case.step.unwrap())
        .frontend_step_aligned();
    let options = QueryOptions {
        limit: case.limit.unwrap_or(100),
        direction: Direction::Backward,
        ..QueryOptions::default()
    };
    let mut first: Option<Vec<crate::LogStream>> = None;
    for run in 0..8 {
        let crate::QueryResult::Streams(streams) = db
            .query(&namespace, &request, options.clone())
            .await
            .unwrap()
        else {
            panic!("expected streams");
        };
        let entries = streams
            .iter()
            .map(|stream| stream.entries.len())
            .sum::<usize>();
        let timestamps = streams
            .iter()
            .flat_map(|stream| stream.entries.iter().map(|entry| entry.timestamp_ns));
        let (min, max) = timestamps.fold((i64::MAX, i64::MIN), |(lo, hi), ts| {
            (lo.min(ts), hi.max(ts))
        });
        println!(
            "run {run}: streams {} entries {entries} oldest {min} newest {max}",
            streams.len()
        );
        match &first {
            None => first = Some(streams),
            Some(first) => {
                let labels = |streams: &[crate::LogStream]| {
                    streams
                        .iter()
                        .map(|stream| format!("{:?}", stream.labels))
                        .collect::<std::collections::BTreeSet<_>>()
                };
                let (a, b) = (labels(first), labels(&streams));
                let entries = |streams: &[crate::LogStream]| {
                    streams
                        .iter()
                        .flat_map(|stream| {
                            stream.entries.iter().map(move |entry| {
                                (
                                    entry.timestamp_ns,
                                    format!("{:?}", stream.labels),
                                    entry.line.clone(),
                                )
                            })
                        })
                        .collect::<std::collections::BTreeSet<_>>()
                };
                let (ea, eb) = (entries(first), entries(&streams));
                let missing = ea.difference(&eb).map(|entry| entry.0).collect::<Vec<_>>();
                let extra = eb.difference(&ea).map(|entry| entry.0).collect::<Vec<_>>();
                println!(
                    "  vs run 0: streams -{} +{}; entries -{} (ts {:?}..{:?}) +{} (ts {:?}..{:?})",
                    a.difference(&b).count(),
                    b.difference(&a).count(),
                    missing.len(),
                    missing.first(),
                    missing.last(),
                    extra.len(),
                    extra.first(),
                    extra.last(),
                );
            }
        }
    }
    db.close().await.unwrap();
}

/// Shape of the replay store's runs: count, rows and blocks per level.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_store_shape() {
    let store = std::path::PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    let config = Config {
        storage: config(&store, true).storage,
        retention: None,
        ..Config::default()
    };
    let db = LogDb::open_reader(config, DbReaderOptions::default())
        .await
        .unwrap();
    let mut records = db
        .storage
        .scan_prefix_iter(Bytes::new(), BytesRange::unbounded(), None)
        .await
        .unwrap();
    let mut levels = std::collections::BTreeMap::<u8, Vec<(u32, u32, i64)>>::new();
    let mut classes = std::collections::BTreeMap::<&str, (usize, usize)>::new();
    let mut totals = (0u64, 0u64, 0u64);
    while let Some(record) = records.next().await.unwrap() {
        let class = classify_key(&record.key);
        let entry = classes.entry(class).or_default();
        entry.0 += 1;
        entry.1 += record.value.len();
        if class == "run" {
            let run = crate::codec::decode_run(&record.value).unwrap();
            totals.0 += u64::from(run.rows);
            totals.1 += run.line_bytes;
            totals.2 += u64::from(run.bytes);
            levels.entry(run.level).or_default().push((
                run.rows,
                run.blocks,
                run.max_timestamp_ns - run.min_timestamp_ns,
            ));
        }
    }
    for (class, (count, bytes)) in classes {
        println!("{class:24} records {count:9} bytes {bytes:12}");
    }
    println!(
        "rows {} line bytes {} encoded bytes {}",
        totals.0, totals.1, totals.2
    );
    for (level, mut runs) in levels {
        let median = |runs: &mut Vec<(u32, u32, i64)>, key: fn(&(u32, u32, i64)) -> i64| {
            runs.sort_by_key(key);
            key(&runs[runs.len() / 2])
        };
        let count = runs.len();
        let rows = median(&mut runs, |run| run.0.into());
        let blocks = median(&mut runs, |run| run.1.into());
        let span = median(&mut runs, |run| run.2) / 1_000_000_000;
        println!("level {level}: runs {count} median rows {rows} blocks {blocks} span {span}s");
    }
    db.close().await.unwrap();
}

/// Clusters of [`profile_cluster_day`]: name, streams and lines per day, shaped
/// like the demo deployment's.
const CLUSTERS: [(&str, usize, u64); 7] = [
    ("mgmt", 900, 5_600_000),
    ("demo-prod", 960, 4_600_000),
    ("demo-dev01", 700, 2_800_000),
    ("demo-dev", 700, 2_500_000),
    ("gke-fleet-prod", 600, 1_900_000),
    ("gke-fleet-dev", 600, 1_850_000),
    ("k3s-jetson", 200, 300_000),
];
const FLUSH_SECONDS: i64 = 10;

/// The operator's production logs config, without retention.
fn production_config(store: &std::path::Path) -> Config {
    let mut config = Config {
        storage: config(store, true).storage,
        retention: None,
        page: PageConfig {
            target_size_bytes: 1 << 20,
            max_rows: 8192,
            rows_per_block: 256,
        },
        ..Config::default()
    };
    if let Ok(bytes) = std::env::var("PROFILE_OBJECT_BYTES") {
        config.compaction.max_object_bytes = bytes.parse().unwrap();
    }
    if let Ok(rows) = std::env::var("PROFILE_OBJECT_ROWS") {
        config.compaction.max_object_rows = rows.parse().unwrap();
    }
    if let Ok(ids) = std::env::var("PROFILE_STREAM_ID_BLOCK") {
        config.stream_id_block_size = ids.parse().unwrap();
    }
    // `PROFILE_DECODE_MB` and `PROFILE_WRITE_MB` override the memory budgets.
    if let Ok(mb) = std::env::var("PROFILE_DECODE_MB") {
        config.memory.decode_bytes = mb.parse::<u64>().unwrap() << 20;
    }
    if let Ok(mb) = std::env::var("PROFILE_WRITE_MB") {
        config.memory.write_bytes = mb.parse::<u64>().unwrap() << 20;
    }
    config.compaction.min_age = Duration::ZERO;
    config.write_buffer.flush_interval = Duration::from_secs(3600);
    // The operator's reader caches: 512 MiB of SST blocks, 128 MiB of
    // indexes and filters.
    let cache = |name: &str, default: u64| {
        Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
            capacity: std::env::var(name).map_or(default, |value| value.parse().unwrap()),
            shards: None,
        }))
    };
    if let StorageConfig::SlateDb(storage) = &mut config.storage {
        storage.settings_path = std::env::var("PROFILE_SLATEDB_SETTINGS").ok();
        storage.block_cache = cache("PROFILE_SST_CACHE", 512 << 20);
        storage.meta_cache = cache("PROFILE_META_CACHE", 128 << 20);
    }
    config
}

/// Writes `hours` of every cluster's streams, one flush per
/// [`FLUSH_SECONDS`] of data as the production writer flushes, ending now.
async fn ingest_clusters(config: &Config, namespace: &Namespace, hours: i64) {
    struct Synth {
        labels: Labels,
        per_flush: f64,
        owed: f64,
    }
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    let mut streams = Vec::new();
    for (cluster, count, daily) in CLUSTERS {
        let weights = (0..count)
            .map(|rank| 1.0 / ((rank + 1) as f64).powf(1.1))
            .collect::<Vec<_>>();
        let total: f64 = weights.iter().sum();
        for (rank, weight) in weights.into_iter().enumerate() {
            let labels = Labels::new(vec![
                Label::new("cluster", cluster),
                Label::new("namespace", format!("ns-{}", rank % 40)),
                Label::new("pod", format!("{cluster}-pod-{rank}")),
                Label::new("container", format!("c-{}", rank % 3)),
                Label::new("job", format!("ns-{}/{cluster}-pod-{rank}", rank % 40)),
            ])
            .unwrap();
            streams.push(Synth {
                labels,
                per_flush: daily as f64 * weight / total * FLUSH_SECONDS as f64 / 86_400.0,
                owed: rng.below(1000) as f64 / 1000.0,
            });
        }
    }
    let writer = LogDb::open(config.clone()).await.unwrap();
    let end = profile_end(FLUSH_SECONDS);
    let flushes = hours * 3600 / FLUSH_SECONDS;
    let started = Instant::now();
    let mut rows = 0usize;
    for flush in 0..flushes {
        let at = end - (flushes - flush) * FLUSH_SECONDS * SECOND;
        let mut batches = Vec::new();
        for stream in &mut streams {
            stream.owed += stream.per_flush;
            let count = stream.owed as usize;
            if count == 0 {
                continue;
            }
            stream.owed -= count as f64;
            let mut timestamps = (0..count)
                .map(|_| at + rng.below((FLUSH_SECONDS * SECOND) as u64) as i64)
                .collect::<Vec<_>>();
            timestamps.sort_unstable();
            let entries = timestamps
                .into_iter()
                .map(|ts| {
                    let word = WORDS[rng.below(WORDS.len() as u64) as usize];
                    LogEntry::new(
                        ts,
                        format!(
                            "ts={ts} level=info caller=handler.go:{} msg=\"{word}\" trace_id={:016x} span_id={:08x} duration_ms={} status={}",
                            rng.below(900),
                            rng.next(),
                            rng.next() as u32,
                            rng.below(5000),
                            200 + rng.below(4) * 100,
                        ),
                    )
                })
                .collect::<Vec<_>>();
            rows += entries.len();
            batches.push(LogBatch::new(stream.labels.clone(), entries));
        }
        writer
            .write_with_durability(namespace, batches, Durability::Written)
            .await
            .unwrap();
        if flush % 360 == 359 {
            println!(
                "ingested hour {} rows {rows} in {:.0}s",
                (flush + 1) / 360,
                started.elapsed().as_secs_f64()
            );
        }
    }
    writer.flush().await.unwrap();
    // Lets the last segments' finalizing merges run.
    for _ in 0..64 {
        writer
            .write_with_durability(namespace, Vec::new(), Durability::Written)
            .await
            .unwrap();
    }
    writer.close().await.unwrap();
}

/// An object store that sleeps before every read, as S3's first-byte latency
/// does, and counts them.
static GET_KEYS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
/// Every [`SlowStore`] read's start, end and key, for wave analysis.
static GET_SPANS: std::sync::Mutex<Vec<(Instant, Instant, String)>> =
    std::sync::Mutex::new(Vec::new());

/// Distinct object-store reads since the last call, and the most repeated.
fn take_get_keys() -> String {
    let keys = std::mem::take(&mut *GET_KEYS.lock().unwrap());
    if std::env::var("PROFILE_GET_KEYS").is_ok() {
        eprintln!("gets: {keys:#?}");
    }
    let mut counts = HashMap::<String, usize>::new();
    for key in keys {
        *counts.entry(key).or_default() += 1;
    }
    let mut repeated = counts.iter().filter(|(_, n)| **n > 1).collect::<Vec<_>>();
    repeated.sort_by_key(|(_, n)| std::cmp::Reverse(**n));
    format!(
        "distinct {} repeated {} top {:?}",
        counts.len(),
        repeated.len(),
        repeated
            .iter()
            .take(3)
            .map(|(k, n)| (k.as_str(), **n))
            .collect::<Vec<_>>()
    )
}

#[derive(Debug)]
struct SlowStore {
    inner: Arc<dyn slatedb::object_store::ObjectStore>,
    latency: Duration,
    gets: Arc<AtomicUsize>,
}

impl std::fmt::Display for SlowStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowStore({})", self.inner)
    }
}

#[async_trait]
impl slatedb::object_store::ObjectStore for SlowStore {
    async fn put_opts(
        &self,
        location: &slatedb::object_store::path::Path,
        payload: slatedb::object_store::PutPayload,
        opts: slatedb::object_store::PutOptions,
    ) -> slatedb::object_store::Result<slatedb::object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &slatedb::object_store::path::Path,
        opts: slatedb::object_store::PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn slatedb::object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &slatedb::object_store::path::Path,
        options: slatedb::object_store::GetOptions,
    ) -> slatedb::object_store::Result<slatedb::object_store::GetResult> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        let key = format!("{location} {:?}", options.range);
        GET_KEYS.lock().unwrap().push(key.clone());
        let started = Instant::now();
        tokio::time::sleep(self.latency).await;
        let result = self.inner.get_opts(location, options).await;
        GET_SPANS
            .lock()
            .unwrap()
            .push((started, Instant::now(), key));
        result
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            slatedb::object_store::Result<slatedb::object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<
        'static,
        slatedb::object_store::Result<slatedb::object_store::path::Path>,
    > {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&slatedb::object_store::path::Path>,
    ) -> futures::stream::BoxStream<
        'static,
        slatedb::object_store::Result<slatedb::object_store::ObjectMeta>,
    > {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&slatedb::object_store::path::Path>,
    ) -> slatedb::object_store::Result<slatedb::object_store::ListResult> {
        let started = Instant::now();
        tokio::time::sleep(self.latency).await;
        let result = self.inner.list_with_delimiter(prefix).await;
        GET_SPANS
            .lock()
            .unwrap()
            .push((started, Instant::now(), format!("LIST {prefix:?}")));
        result
    }

    async fn copy_opts(
        &self,
        from: &slatedb::object_store::path::Path,
        to: &slatedb::object_store::path::Path,
        options: slatedb::object_store::CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A reader with empty caches whose object store sleeps `latency` per
/// request, as a cold read against S3 does.
async fn cold_reader(config: &Config, latency: Duration, gets: &Arc<AtomicUsize>) -> LogDb {
    cold_reader_cached(config, latency, gets).await.0
}

/// [`cold_reader`] with the SlateDB cache it reads through.
async fn cold_reader_cached(
    config: &Config,
    latency: Duration,
    gets: &Arc<AtomicUsize>,
) -> (LogDb, SharedDbCache) {
    let StorageConfig::SlateDb(slate) = &config.storage else {
        unreachable!("profiles run on SlateDB");
    };
    let inner = common::storage::factory::create_object_store(&slate.object_store).unwrap();
    let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(SlowStore {
        inner,
        latency,
        gets: Arc::clone(gets),
    });
    let cache = SharedDbCache::from_config(&config.storage).await.unwrap();
    let storage = create_storage_read(
        &config.storage,
        StorageReaderRuntime::new()
            .with_shared_cache(&cache)
            .with_object_store(store),
        storage_semantics(),
        // `PROFILE_SKIP_WAL` opens without replaying the WAL, for stores
        // whose WAL is already compacted.
        DbReaderOptions {
            skip_wal_replay: std::env::var("PROFILE_SKIP_WAL").is_ok(),
            ..DbReaderOptions::default()
        },
    )
    .await
    .unwrap();
    let db = LogDb {
        storage,
        writer: None,
        segment_ns: duration_ns(config.segment_duration).unwrap(),
        rollup_ns: config
            .discovery_rollup
            .map(duration_ns)
            .transpose()
            .unwrap(),
        write_handle: None,
        write_coordinator: Mutex::new(None),
        caches: DiscoveryCaches::new(),
        blocks: BlockCache::new(config.block_cache_capacity_bytes),
        database_id: block_cache::next_database_id(),
        memory: MemoryBudget::new(&config.memory),
        span_scan_min_streams: SPAN_SCAN_MIN_STREAMS,
        span_scan_max_spread: std::env::var("PROFILE_SPAN_SPREAD")
            .map_or(SPAN_SCAN_MAX_SPREAD, |value| value.parse().unwrap()),
        // `PROFILE_FULL_LABELS` reads every stream's full labels.
        project_labels: std::env::var("PROFILE_FULL_LABELS").is_err(),
        tuning: profile_tuning(),
        rolled: tokio::sync::RwLock::new(HashMap::new()),
        rollup_lock: Mutex::new(()),
        merge_holds: Default::default(),
        retention: config.retention,
        settle_after: config.compaction.finalize_after,
        summary_bucket: crate::db::summary::SUMMARY_BUCKET,
        summarized: Default::default(),
        period_summaries: Default::default(),
    };
    (db, cache)
}

/// [`ReadTuning::default`] with `PROFILE_LIST_CONCURRENCY`,
/// `PROFILE_READ_CONCURRENCY`, `PROFILE_POSTINGS_VALIDATE` and
/// `PROFILE_OVERLAP_RUNS` (`0` or `1`) overriding its fields.
fn profile_tuning() -> ReadTuning {
    fn var<T: std::str::FromStr<Err: std::fmt::Debug>>(name: &str) -> Option<T> {
        std::env::var(name).ok().map(|value| value.parse().unwrap())
    }
    let flag = |name: &str| var(name).map(|value: u8| value != 0);
    let default = ReadTuning::default();
    ReadTuning {
        list_concurrency: var("PROFILE_LIST_CONCURRENCY").unwrap_or(default.list_concurrency),
        read_concurrency: var("PROFILE_READ_CONCURRENCY").unwrap_or(default.read_concurrency),
        postings_validate: flag("PROFILE_POSTINGS_VALIDATE").unwrap_or(default.postings_validate),
        overlap_runs: flag("PROFILE_OVERLAP_RUNS").unwrap_or(default.overlap_runs),
        segment_lookahead: var("PROFILE_SEGMENT_LOOKAHEAD").unwrap_or(default.segment_lookahead),
        pipeline_units: var("PROFILE_PIPELINE_UNITS").unwrap_or(default.pipeline_units),
        unfiltered_reads: flag("PROFILE_UNFILTERED_READS").unwrap_or(default.unfiltered_reads),
        cached_filters: flag("PROFILE_CACHED_FILTERS").unwrap_or(default.cached_filters),
    }
}

/// Times `request` `runs` times, each on fresh cold readers: the whole
/// query on one, then on another its `scan_targets` and the load of what it
/// listed; then the query warm on the last. Prints each run and the median,
/// min and max of every measure.
async fn cold_runs(
    config: &Config,
    latency: Duration,
    namespace: &Namespace,
    request: &QueryRequest,
    runs: usize,
) {
    let options = QueryOptions::default();
    let parsed = crate::logql::parse(&request.query).unwrap();
    let plan = crate::query::ScanPlan::new(request, &parsed, &options).unwrap();
    println!("tuning: {:?} latency {latency:?}", profile_tuning());
    // `PROFILE_SEGMENT_TIMES` lists each segment alone, then all at once,
    // each on a fresh cold reader.
    if std::env::var("PROFILE_SEGMENT_TIMES").is_ok() {
        let now_unix_ms = unix_time_ms().unwrap();
        let range = (plan.scan_start, request.end_ns);
        let gets = Arc::new(AtomicUsize::new(0));
        let db = cold_reader(config, latency, &gets).await;
        let segments = db.visible_segment_ids(namespace, range, false).unwrap();
        let manifest = db.storage.slate_read().map(|slate| slate.manifest());
        gets.store(0, Ordering::Relaxed);
        for segment in &segments {
            // L0 SSTs and sorted runs a cold read of the segment may walk.
            let prefix = segment_prefix(namespace, *segment);
            let shape = manifest
                .as_ref()
                .and_then(|manifest| {
                    manifest
                        .segments()
                        .iter()
                        .find(|live| live.prefix().as_ref() == prefix.as_ref())
                        .map(|live| (live.l0().len(), live.compacted().len()))
                })
                .unwrap_or_default();
            take_get_keys();
            let started = Instant::now();
            let streams = db
                .segment_streams(namespace, *segment, &plan.streams, range, now_unix_ms)
                .await
                .unwrap();
            println!(
                "  alone {}: l0 {} sorted runs {} {:.0}ms gets {} streams {} runs {} ({})",
                segment.start_ns,
                shape.0,
                shape.1,
                ms(started.elapsed()),
                gets.swap(0, Ordering::Relaxed),
                streams.len(),
                streams
                    .iter()
                    .map(|stream| stream.runs.len())
                    .sum::<usize>(),
                take_get_keys(),
            );
        }
        db.close().await.unwrap();
        let db = cold_reader(config, latency, &gets).await;
        let started = Instant::now();
        let filter = &plan.streams;
        let done = futures::future::join_all(segments.iter().map(|segment| {
            let db = &db;
            async move {
                db.segment_streams(namespace, *segment, filter, range, now_unix_ms)
                    .await
                    .unwrap();
                ms(started.elapsed())
            }
        }))
        .await;
        println!("  together, done at: {done:.0?}");
        db.close().await.unwrap();
    }
    let names = [
        "total ms",
        "total gets",
        "scan ms",
        "scan gets",
        "load ms",
        "load gets",
        "warm ms",
        "warmscan ms",
    ];
    // `PROFILE_PREWARM=N` warms the filters and indexes of the newest N
    // segments in range that have several SSTs (all with a non-number) while
    // the query runs, as the continuous cache warmer would have beforehand.
    let prewarm = async |db: &LogDb| {
        if std::env::var("PROFILE_PREWARM").is_err() {
            return;
        }
        let Some(slate) = db.storage.slate_read() else {
            return;
        };
        let range = (plan.scan_start, request.end_ns);
        let newest: usize = std::env::var("PROFILE_PREWARM")
            .unwrap()
            .parse()
            .unwrap_or(usize::MAX);
        let segments = db.visible_segment_ids(namespace, range, false).unwrap();
        let open: Vec<Bytes> = segments
            .iter()
            .rev()
            .take(newest)
            .map(|segment| segment_prefix(namespace, *segment))
            .filter(|prefix| {
                slate.manifest().segments().iter().any(|live| {
                    live.prefix().as_ref() == prefix.as_ref()
                        && live.l0().len() + live.compacted().len() > 1
                })
            })
            .collect();
        slate
            .warm_prefixes("logs", &open, false, 64, &CancellationToken::new(), None)
            .await
            .unwrap();
    };
    let mut measures = vec![Vec::new(); names.len()];
    for run in 0..runs {
        let gets = Arc::new(AtomicUsize::new(0));
        let db = cold_reader(config, latency, &gets).await;
        gets.store(0, Ordering::Relaxed);
        let started = Instant::now();
        let (result, ()) =
            tokio::join!(db.query(namespace, request, options.clone()), prewarm(&db));
        result.unwrap();
        let total = (ms(started.elapsed()), gets.swap(0, Ordering::Relaxed));
        db.close().await.unwrap();

        let db = cold_reader(config, latency, &gets).await;
        gets.store(0, Ordering::Relaxed);
        let started = Instant::now();
        let targets = db
            .scan_targets(namespace, plan.scan_start, request.end_ns, &plan.streams)
            .await
            .unwrap();
        let scan = (ms(started.elapsed()), gets.swap(0, Ordering::Relaxed));
        let estimate = targets.estimate(plan.is_lineless());
        let started = Instant::now();
        plan.load_targets(
            &db,
            namespace,
            &PageBudget::new(estimate.read_units),
            targets,
        )
        .await
        .unwrap();
        let load = (ms(started.elapsed()), gets.swap(0, Ordering::Relaxed));
        let warm_runs =
            std::env::var("PROFILE_WARM_RUNS").map_or(3, |value| value.parse().unwrap());
        let mut warm = Vec::new();
        let mut warm_scan = Vec::new();
        for _ in 0..warm_runs {
            let started = Instant::now();
            db.query(namespace, request, options.clone()).await.unwrap();
            warm.push(ms(started.elapsed()));
            let started = Instant::now();
            db.scan_targets(namespace, plan.scan_start, request.end_ns, &plan.streams)
                .await
                .unwrap();
            warm_scan.push(ms(started.elapsed()));
        }
        warm.sort_by(f64::total_cmp);
        warm_scan.sort_by(f64::total_cmp);
        db.close().await.unwrap();
        let values = [
            total.0,
            total.1 as f64,
            scan.0,
            scan.1 as f64,
            load.0,
            load.1 as f64,
            warm[warm.len() / 2],
            warm_scan[warm_scan.len() / 2],
        ];
        println!(
            "  run {run}: {}",
            names
                .iter()
                .zip(values)
                .map(|(name, value)| format!("{name} {value:.0}"))
                .collect::<Vec<_>>()
                .join(" | ")
        );
        for (measure, value) in measures.iter_mut().zip(values) {
            measure.push(value);
        }
    }
    for (name, mut values) in names.iter().zip(measures) {
        values.sort_by(f64::total_cmp);
        println!(
            "  {name}: median {:.0} min {:.0} max {:.0}",
            values[values.len() / 2],
            values[0],
            values[values.len() - 1]
        );
    }
}

/// Times a day-long per-cluster log metric query, as Console's log chart
/// sends it, against a day of production-shaped data at `PROFILE_STORE`
/// (ingested once), cold with `PROFILE_LATENCY_MS` of object-store latency
/// and then warm, with storage reads by record type.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_cluster_day() {
    let store = std::path::PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    let hours: i64 = std::env::var("PROFILE_HOURS").map_or(25, |value| value.parse().unwrap());
    let latency = Duration::from_millis(
        std::env::var("PROFILE_LATENCY_MS").map_or(20, |value| value.parse().unwrap()),
    );
    let cluster = std::env::var("PROFILE_CLUSTER").unwrap_or_else(|_| "mgmt".to_owned());
    let step: i64 = std::env::var("PROFILE_STEP").map_or(1800, |value| value.parse().unwrap());
    let namespace = Namespace::new("default").unwrap();
    let config = production_config(&store);
    let marker = store.join("ingested");
    if !marker.exists() {
        std::fs::create_dir_all(&store).unwrap();
        let ingested_end = profile_end(FLUSH_SECONDS);
        ingest_clusters(&config, &namespace, hours).await;
        std::fs::write(&marker, ingested_end.to_string()).unwrap();
    }

    if std::env::var("PROFILE_ROLLUP").is_ok() && !store.join("rolled").exists() {
        let writer = LogDb::open(config.clone()).await.unwrap();
        if let Some(slate) = writer.storage.slate_read() {
            let mut sizes = slate
                .segment_sizes()
                .into_iter()
                .map(|(_, bytes)| bytes)
                .collect::<Vec<_>>();
            sizes.sort_unstable();
            println!(
                "segments {} sst bytes total {} median {} max {}",
                sizes.len(),
                sizes.iter().sum::<u64>(),
                sizes.get(sizes.len() / 2).copied().unwrap_or(0),
                sizes.last().copied().unwrap_or(0),
            );
        }
        // `PROFILE_ROLLUP_MAX_GB` overrides the largest rollup segment.
        let max_segment_bytes = std::env::var("PROFILE_ROLLUP_MAX_GB").map_or(
            common::storage::rollup::RollupConfig::default().max_segment_bytes,
            |value| value.parse::<u64>().unwrap() << 30,
        );
        // `PROFILE_ROLLUP_BUFFER_MB` overrides the copy buffer.
        let copy_buffer_bytes = std::env::var("PROFILE_ROLLUP_BUFFER_MB").map_or(
            common::storage::rollup::DEFAULT_COPY_BUFFER_BYTES,
            |value| value.parse::<u64>().unwrap() << 20,
        );
        let started = Instant::now();
        let sealed = writer
            .rollup_at(
                &common::storage::rollup::RollupConfig {
                    delay: Duration::ZERO,
                    max_segment_bytes,
                    copy_buffer_bytes,
                    ..Default::default()
                },
                common::time::now_ns(),
            )
            .await
            .unwrap();
        println!(
            "rollup: sealed {sealed} in {:.1}s",
            started.elapsed().as_secs_f64()
        );
        writer.close().await.unwrap();
        std::fs::write(store.join("rolled"), b"").unwrap();
        if std::env::var("PROFILE_ROLLUP_ONLY").is_ok() {
            return;
        }
    }

    // `PROFILE_SUMMARIZE` summarizes the store's settled segments once.
    if std::env::var("PROFILE_SUMMARIZE").is_ok() && !store.join("summarized").exists() {
        let writer = LogDb::open(config.clone()).await.unwrap();
        let started = Instant::now();
        let mut summarized = 0;
        loop {
            let pass = writer.summarize().await.unwrap();
            if pass == 0 {
                break;
            }
            summarized += pass;
        }
        println!(
            "summarized {summarized} segments in {:.1}s",
            started.elapsed().as_secs_f64()
        );
        writer.close().await.unwrap();
        std::fs::write(store.join("summarized"), b"").unwrap();
    }

    // `PROFILE_END=ingested` ends the day where the ingest did, so every run
    // against one store reads the same segments.
    let end = match std::env::var("PROFILE_END").as_deref() {
        Ok("ingested") => {
            let ingested: i64 = std::fs::read_to_string(&marker).unwrap().parse().unwrap();
            ingested / (step * SECOND) * (step * SECOND)
        }
        _ => profile_end(step),
    };
    let start = end - 86_400 * SECOND;
    // `PROFILE_QUERY` overrides the query, with `CLUSTER` and `STEP` filled in.
    let query = std::env::var("PROFILE_QUERY").map_or_else(
        |_| format!(r#"sum(count_over_time({{cluster="{cluster}"}} [{step}s]))"#),
        |query| {
            query
                .replace("CLUSTER", &cluster)
                .replace("STEP", &step.to_string())
        },
    );
    println!("query: {query}");
    let request = QueryRequest::range(query, start, end, step * SECOND).frontend_step_aligned();
    let options = QueryOptions::default();
    // `PROFILE_COLD_RUNS` only times that many cold runs.
    if let Ok(runs) = std::env::var("PROFILE_COLD_RUNS") {
        cold_runs(
            &config,
            latency,
            &namespace,
            &request,
            runs.parse().unwrap(),
        )
        .await;
        return;
    }

    let gets = Arc::new(AtomicUsize::new(0));
    let mut db = cold_reader(&config, latency, &gets).await;
    let counting = Arc::new(common::storage::counting::CountingStorage::new(
        db.storage.clone(),
        classify_key,
    ));
    db.storage = counting.clone();
    gets.store(0, Ordering::Relaxed);
    take_get_keys();
    let report = |label: &str, elapsed: Duration| {
        let mut io = counting.take().into_iter().collect::<Vec<_>>();
        io.sort_by_key(|(_, stats)| std::cmp::Reverse(stats.elapsed));
        println!(
            "{label}: {:.0}ms object-store gets {} ({})",
            ms(elapsed),
            gets.swap(0, Ordering::Relaxed),
            take_get_keys()
        );
        for ((op, class), stats) in io {
            println!(
                "    {op}/{class}: calls {} records {} bytes {} summed {:.0}ms",
                stats.calls,
                stats.records,
                stats.bytes,
                ms(stats.elapsed)
            );
        }
    };
    let started = Instant::now();
    let result = db
        .query(&namespace, &request, options.clone())
        .await
        .unwrap();
    report("query cold", started.elapsed());
    if let crate::QueryResult::Matrix(series) = &result {
        let rows: f64 = series
            .iter()
            .flat_map(|series| &series.samples)
            .map(|sample| sample.value)
            .sum();
        println!("  counted rows {rows}");
    }
    let warm_runs: usize =
        std::env::var("PROFILE_WARM_RUNS").map_or(3, |value| value.parse().unwrap());
    for run in 0..warm_runs {
        let started = Instant::now();
        db.query(&namespace, &request, options.clone())
            .await
            .unwrap();
        report(&format!("query warm {run}"), started.elapsed());
    }

    let parsed = crate::logql::parse(&request.query).unwrap();
    let plan = crate::query::ScanPlan::new(&request, &parsed, &options).unwrap();
    let started = Instant::now();
    let targets = db
        .scan_targets(&namespace, plan.scan_start, request.end_ns, &plan.streams)
        .await
        .unwrap();
    report("scan_targets warm", started.elapsed());
    let (mut streams, mut runs, mut isolated, mut rows) = (0, 0, 0, 0u64);
    let spreads = targets
        .segments
        .iter()
        .filter_map(|(_, segment_streams)| {
            let first = segment_streams.first()?.stream_id;
            let last = segment_streams.last()?.stream_id;
            Some(format!("{}/{}", segment_streams.len(), last - first + 1))
        })
        .collect::<Vec<_>>();
    println!("  selected/spread per segment: {}", spreads.join(" "));
    for (_, segment_streams) in &targets.segments {
        streams += segment_streams.len();
        for stream in segment_streams {
            runs += stream.runs.len();
            isolated += isolated_runs(&stream.runs)
                .into_iter()
                .filter(|alone| *alone)
                .count();
            rows += stream
                .runs
                .iter()
                .map(|(_, run)| u64::from(run.rows))
                .sum::<u64>();
        }
    }
    let estimate = targets.estimate(true);
    println!(
        "  segments {} stream-segments {streams} runs {runs} (isolated {isolated}) rows {rows} rows/run {:.1} read-units {} compressed bytes {}",
        targets.segments.len(),
        rows as f64 / runs.max(1) as f64,
        estimate.read_units,
        estimate.compressed_bytes,
    );

    let cold_gets = Arc::new(AtomicUsize::new(0));
    let cold = cold_reader(&config, latency, &cold_gets).await;
    cold_gets.store(0, Ordering::Relaxed);
    let started = Instant::now();
    cold.scan_targets(&namespace, plan.scan_start, request.end_ns, &plan.streams)
        .await
        .unwrap();
    println!(
        "scan_targets cold: {:.0}ms object-store gets {}",
        ms(started.elapsed()),
        cold_gets.load(Ordering::Relaxed)
    );

    // Stream selection by phase on fresh cold readers: postings, then the
    // forward labels of the streams they found, then those labels again
    // warm, and the labels alone on a reader that read no postings first.
    let segments = cold
        .visible_segment_ids(&namespace, (plan.scan_start, request.end_ns), false)
        .unwrap();
    let (exact, namespace) = (&plan.streams.exact, &namespace);
    let postings = async |db: &LogDb| {
        futures::stream::iter(segments.iter().copied())
            .map(|segment| async move {
                let ids = db.stream_ids(namespace, segment, exact).await?;
                Ok::<_, Error>((segment, ids))
            })
            .buffered(SEGMENT_LIST_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    };
    let labels = async |db: &LogDb, ids: &[(SegmentId, Vec<StreamId>)]| {
        futures::stream::iter(ids.iter().cloned())
            .map(|(segment, ids)| db.stream_labels(namespace, segment, ids))
            .buffered(SEGMENT_LIST_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    };
    let phase_gets = Arc::new(AtomicUsize::new(0));
    let mut phased = cold_reader(&config, latency, &phase_gets).await;
    let counting = Arc::new(common::storage::counting::CountingStorage::new(
        phased.storage.clone(),
        classify_key,
    ));
    phased.storage = counting.clone();
    phase_gets.store(0, Ordering::Relaxed);
    let started = Instant::now();
    let ids = postings(&phased).await;
    let postings_cold = (started.elapsed(), phase_gets.swap(0, Ordering::Relaxed));
    let started = Instant::now();
    let labelled = labels(&phased, &ids).await;
    let labels_cold = (started.elapsed(), phase_gets.swap(0, Ordering::Relaxed));
    let started = Instant::now();
    labels(&phased, &ids).await;
    let labels_warm = started.elapsed();
    let io = counting.take();
    let alone_gets = Arc::new(AtomicUsize::new(0));
    let alone = cold_reader(&config, latency, &alone_gets).await;
    alone_gets.store(0, Ordering::Relaxed);
    let started = Instant::now();
    labels(&alone, &ids).await;
    let labels_alone = (started.elapsed(), alone_gets.load(Ordering::Relaxed));
    // The same streams' labels built from postings, as the query projects
    // them, on a reader that read the postings first.
    let projected = async |db: &LogDb, ids: &[(SegmentId, Vec<StreamId>)]| {
        let Some(names) = &plan.streams.labels else {
            return (0, 0);
        };
        let built = futures::stream::iter(ids.iter().cloned())
            .map(|(segment, ids)| async move {
                db.projected_labels(namespace, segment, exact, &ids, names)
                    .await
            })
            .buffered(SEGMENT_LIST_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let fallbacks = built.iter().filter(|built| built.is_none()).count();
        (built.len() - fallbacks, fallbacks)
    };
    let projected_gets = Arc::new(AtomicUsize::new(0));
    let mut projecting = cold_reader(&config, latency, &projected_gets).await;
    let projected_io = Arc::new(common::storage::counting::CountingStorage::new(
        projecting.storage.clone(),
        classify_key,
    ));
    projecting.storage = projected_io.clone();
    postings(&projecting).await;
    projected_io.take();
    projected_gets.store(0, Ordering::Relaxed);
    let started = Instant::now();
    let (built, fallbacks) = projected(&projecting, &ids).await;
    let projected_cold = (started.elapsed(), projected_gets.load(Ordering::Relaxed));
    println!(
        "    projected labels {:?}: segments {built} (fell back {fallbacks}) after postings cold {:.0}ms gets {}",
        plan.streams.labels,
        ms(projected_cold.0),
        projected_cold.1,
    );
    for ((op, class), stats) in projected_io.take() {
        println!(
            "      {op}/{class}: calls {} records {} bytes {} summed {:.0}ms",
            stats.calls,
            stats.records,
            stats.bytes,
            ms(stats.elapsed)
        );
    }
    projecting.close().await.unwrap();
    let label_bytes: usize = labelled
        .iter()
        .flatten()
        .map(|(_, labels)| {
            labels
                .iter()
                .map(|label| label.name.len() + label.value.len())
                .sum::<usize>()
        })
        .sum();
    println!(
        "selection phases: segments {} streams {} label bytes {label_bytes}",
        segments.len(),
        labelled.iter().map(Vec::len).sum::<usize>(),
    );
    println!(
        "    postings cold {:.0}ms gets {} | labels after postings cold {:.0}ms gets {} | labels warm {:.1}ms | labels alone cold {:.0}ms gets {}",
        ms(postings_cold.0),
        postings_cold.1,
        ms(labels_cold.0),
        labels_cold.1,
        ms(labels_warm),
        ms(labels_alone.0),
        labels_alone.1,
    );
    for ((op, class), stats) in io {
        println!(
            "    {op}/{class}: calls {} records {} bytes {} summed {:.0}ms",
            stats.calls,
            stats.records,
            stats.bytes,
            ms(stats.elapsed)
        );
    }
    phased.close().await.unwrap();
    alone.close().await.unwrap();
    db.close().await.unwrap();
    cold.close().await.unwrap();
}

/// Now rounded down to `step_seconds`, or with `PROFILE_END=midnight` the
/// last UTC midnight, so a day-long query reads exactly one closed day.
fn profile_end(step_seconds: i64) -> i64 {
    let step = if std::env::var("PROFILE_END").as_deref() == Ok("midnight") {
        86_400 * SECOND
    } else {
        step_seconds * SECOND
    };
    common::time::now_ns() / step * step
}

fn plan_segments(db: &LogDb, start: i64, end: i64) -> usize {
    db.segment_ids((start, end), false).unwrap().len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_log_query_phases() {
    profile(false).await;
    profile(true).await;
}

/// Bytes of every file under `path`. Garbage collection keeps replaced SSTs
/// for an hour, so over a shorter run this is every byte written.
fn dir_bytes(path: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| match entry.file_type() {
            Ok(kind) if kind.is_dir() => dir_bytes(&entry.path()),
            _ => entry.metadata().map_or(0, |meta| meta.len()),
        })
        .sum()
}

/// Waits until `db`'s manifest stops changing for `quiet`, at most `limit`,
/// writing nothing so the writer's compactor drains its queue.
async fn settle(db: &LogDb, quiet: Duration, limit: Duration) -> Duration {
    let started = Instant::now();
    let Some(slate) = db.storage.slate_read() else {
        return Duration::ZERO;
    };
    let layout = || {
        let manifest = slate.manifest();
        manifest
            .segments()
            .iter()
            .map(|segment| (segment.l0().len(), segment.compacted().len()))
            .collect::<Vec<_>>()
    };
    let mut last = layout();
    let mut stable = Instant::now();
    while started.elapsed() < limit {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = layout();
        if now != last {
            last = now;
            stable = Instant::now();
        } else if stable.elapsed() >= quiet {
            break;
        }
    }
    started.elapsed()
}

/// Visible segments a query over `[start, end]` reads, and their SSTs
/// sources (L0s plus sorted runs) in total and at most.
fn read_layout(db: &LogDb, namespace: &Namespace, start: i64, end: i64) -> (usize, usize, usize) {
    let segments = db
        .visible_segment_ids(namespace, (start, end), false)
        .unwrap();
    let manifest = db.storage.slate_read().unwrap().manifest();
    let sources = segments
        .iter()
        .map(|segment| {
            let mut prefix = bytes::BytesMut::new();
            crate::codec::KEY_SCOPE.write(&mut prefix, namespace, *segment);
            manifest
                .segments()
                .iter()
                .find(|live| live.prefix().as_ref() == prefix.as_ref())
                .map_or(0, |live| live.l0().len() + live.compacted().len())
        })
        .collect::<Vec<_>>();
    (
        segments.len(),
        sources.iter().sum(),
        sources.iter().copied().max().unwrap_or(0),
    )
}

/// For each segment the last day of `PROFILE_STORE` reads, the runs of the
/// `mgmt` cluster's streams: how many, how many are isolated, their time
/// spans, and how many a 5 or 30 minute step boundary splits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_run_spans() {
    let store = std::path::PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    let config = production_config(&store);
    let namespace = Namespace::new("default").unwrap();
    let db = cold_reader(&config, Duration::ZERO, &Arc::new(AtomicUsize::new(0))).await;
    let end = common::time::now_ns();
    let request = QueryRequest::range(
        r#"sum(count_over_time({cluster="mgmt"} [300s]))"#,
        end - 86_400 * SECOND,
        end,
        300 * SECOND,
    );
    let options = QueryOptions::default();
    let parsed = crate::logql::parse(&request.query).unwrap();
    let plan = crate::query::ScanPlan::new(&request, &parsed, &options).unwrap();
    let targets = db
        .scan_targets(&namespace, plan.scan_start, end, &plan.streams)
        .await
        .unwrap();
    let splits = |run: &StoredRun, step: i64| {
        run.min_timestamp_ns.div_euclid(step) != run.max_timestamp_ns.div_euclid(step)
    };
    for (segment, streams) in &targets.segments {
        let mut spans = Vec::new();
        let (mut isolated, mut split_5m, mut split_30m, mut rows) = (0, 0, 0, 0u64);
        for stream in streams {
            for ((_, run), alone) in stream.runs.iter().zip(isolated_runs(&stream.runs)) {
                spans.push((run.max_timestamp_ns - run.min_timestamp_ns) / SECOND);
                isolated += usize::from(alone);
                split_5m += usize::from(splits(run, 300 * SECOND));
                split_30m += usize::from(splits(run, 1_800 * SECOND));
                rows += u64::from(run.rows);
            }
        }
        if spans.is_empty() {
            continue;
        }
        spans.sort_unstable();
        let at = |q: f64| spans[((spans.len() - 1) as f64 * q) as usize];
        println!(
            "{segment}: streams {} runs {} rows/run {:.0} isolated {isolated} span p50 {}s p90 {}s max {}s split 5m {split_5m} 30m {split_30m}",
            streams.len(),
            spans.len(),
            rows as f64 / spans.len() as f64,
            at(0.5),
            at(0.9),
            spans[spans.len() - 1],
        );
    }
    db.close().await.unwrap();
}

/// Object-store reads since the last call, by file kind: reads, distinct
/// files and bytes requested.
fn get_breakdown() -> String {
    let keys = std::mem::take(&mut *GET_KEYS.lock().unwrap());
    let mut kinds = std::collections::BTreeMap::<String, (usize, HashSet<String>, u64)>::new();
    for key in keys {
        let (path, range) = key.split_once(' ').unwrap_or((key.as_str(), ""));
        let kind = path.rsplit('/').nth(1).unwrap_or("?").to_owned();
        let bytes = range
            .split_once("Bounded(")
            .and_then(|(_, rest)| rest.split_once(')'))
            .and_then(|(span, _)| span.split_once(".."))
            .and_then(|(from, to)| Some(to.parse::<u64>().ok()? - from.parse::<u64>().ok()?))
            .unwrap_or(0);
        let entry = kinds.entry(kind).or_default();
        entry.0 += 1;
        entry.1.insert(path.to_owned());
        entry.2 += bytes;
    }
    kinds
        .into_iter()
        .map(|(kind, (reads, files, bytes))| {
            format!(
                "{kind}: reads {reads} files {} KiB {}",
                files.len(),
                bytes >> 10
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Runs Console's per-cluster log chart over the last hour, 4 hours and day
/// ending at `end`, each cold on a fresh reader and then warm.
async fn profile_ranges(
    phase: &str,
    writer: &LogDb,
    config: &Config,
    namespace: &Namespace,
    latency: Duration,
    end: i64,
) {
    for (label, range, step) in [
        ("1h", 3_600, 60),
        ("4h", 4 * 3_600, 300),
        ("1d", 86_400, 1_800),
    ] {
        let start = end - range * SECOND;
        let (segments, sources, widest) = read_layout(writer, namespace, start, end);
        let request = QueryRequest::range(
            r#"sum(count_over_time({cluster="mgmt"} [STEPs]))"#.replace("STEP", &step.to_string()),
            start,
            end,
            step * SECOND,
        )
        .frontend_step_aligned();
        let gets = Arc::new(AtomicUsize::new(0));
        take_get_keys();
        let mut db = cold_reader(config, latency, &gets).await;
        let opened = gets.swap(0, Ordering::Relaxed);
        let open_reads = get_breakdown();
        let counting = Arc::new(common::storage::counting::CountingStorage::new(
            db.storage.clone(),
            classify_key,
        ));
        db.storage = counting.clone();
        let timed = Instant::now();
        db.query(namespace, &request, QueryOptions::default())
            .await
            .unwrap();
        let cold = timed.elapsed();
        let cold_gets = gets.swap(0, Ordering::Relaxed);
        let cold_reads = get_breakdown();
        let io = counting.take();
        let mut warm = Vec::new();
        for _ in 0..3 {
            let timed = Instant::now();
            db.query(namespace, &request, QueryOptions::default())
                .await
                .unwrap();
            warm.push(ms(timed.elapsed()));
        }
        counting.take();
        take_get_keys();
        println!(
            "{phase} {label}: segments {segments} sources {sources} (max {widest}) cold {:.0}ms gets {cold_gets} (+{opened} to open) warm {:.0}/{:.0}/{:.0}ms",
            ms(cold),
            warm[0],
            warm[1],
            warm[2],
        );
        println!("    open: {open_reads}");
        println!("    query: {cold_reads}");
        for ((op, class), stats) in io {
            println!(
                "    {op}/{class}: calls {} records {} KiB {}",
                stats.calls,
                stats.records,
                stats.bytes >> 10,
            );
        }
        db.close().await.unwrap();
    }
}

/// Ingests `PROFILE_HOURS` (8) of production-shaped logs ending now into a
/// fresh `PROFILE_STORE`, times Console's per-cluster log chart over the last
/// hour, 4 hours and day cold (`PROFILE_LATENCY_MS` per object-store read)
/// and warm, runs one rollup pass with the default config, lets compaction
/// settle, and times the same queries again over the same data, with what
/// each read from the object store and the bytes written by each phase.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_query_ranges() {
    let store = std::path::PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    assert!(!store.exists(), "PROFILE_STORE must not exist yet");
    let hours: i64 = std::env::var("PROFILE_HOURS").map_or(8, |value| value.parse().unwrap());
    let latency = Duration::from_millis(
        std::env::var("PROFILE_LATENCY_MS").map_or(20, |value| value.parse().unwrap()),
    );
    let namespace = Namespace::new("default").unwrap();
    let config = production_config(&store);
    std::fs::create_dir_all(&store).unwrap();

    let started = Instant::now();
    ingest_clusters(&config, &namespace, hours).await;
    let writer = LogDb::open(config.clone()).await.unwrap();
    let settled = settle(&writer, Duration::from_secs(10), Duration::from_secs(180)).await;
    let ingested = dir_bytes(&store);
    println!(
        "ingest: {hours}h in {:.0}s (settled {:.0}s), store {:.2} GiB",
        started.elapsed().as_secs_f64(),
        settled.as_secs_f64(),
        ingested as f64 / f64::from(1 << 30),
    );
    let end = common::time::now_ns() / (60 * SECOND) * (60 * SECOND);
    profile_ranges("before", &writer, &config, &namespace, latency, end).await;

    let started = Instant::now();
    let sealed = writer
        .rollup_at(
            &common::storage::rollup::RollupConfig::default(),
            common::time::now_ns(),
        )
        .await
        .unwrap();
    let rolled = started.elapsed();
    let settled = settle(&writer, Duration::from_secs(10), Duration::from_secs(600)).await;
    let total = dir_bytes(&store);
    println!(
        "rollup: sealed {sealed} in {:.0}s (settled {:.0}s), wrote {:.2} GiB, store {:.2} GiB",
        rolled.as_secs_f64(),
        settled.as_secs_f64(),
        (total - ingested) as f64 / f64::from(1 << 30),
        total as f64 / f64::from(1 << 30),
    );
    if let Some(rollups) = writer
        .storage
        .slate_read()
        .map(|slate| Rollups::load_all(&slate, crate::codec::KEY_SCOPE))
    {
        for (_, rollups) in rollups {
            let sealed: Vec<String> = rollups
                .sealed()
                .iter()
                .map(|segment| format!("{segment}"))
                .collect();
            println!("  sealed: {}", sealed.join(" "));
        }
    }
    profile_ranges("after", &writer, &config, &namespace, latency, end).await;
    writer.close().await.unwrap();
}

/// The shape of timed object-store reads, as the cold battery's
/// `request_waves` reports the proxy's: reads, wall span from the first
/// start to the last end, time with at least one in flight, the most in
/// flight at once, and the longest chain of reads each starting after the
/// previous ended, a lower bound on dependent round trips.
#[derive(Debug, Default, serde::Serialize)]
struct Waves {
    reads: usize,
    span_ms: f64,
    busy_ms: f64,
    peak: usize,
    waves: usize,
}

fn waves(spans: &[(Instant, Instant, String)]) -> Waves {
    let mut timed = spans
        .iter()
        .map(|(start, end, _)| (*start, *end))
        .collect::<Vec<_>>();
    timed.sort();
    let Some(&(first, _)) = timed.first() else {
        return Waves::default();
    };
    let last = timed.iter().map(|(_, end)| *end).max().unwrap_or(first);
    let (mut busy, mut open) = (Duration::ZERO, timed[0]);
    for &(start, end) in &timed[1..] {
        if start > open.1 {
            busy += open.1 - open.0;
            open = (start, end);
        } else {
            open.1 = open.1.max(end);
        }
    }
    busy += open.1 - open.0;
    let mut edges = timed
        .iter()
        .flat_map(|&(start, end)| [(start, 1i32), (end, -1)])
        .collect::<Vec<_>>();
    edges.sort();
    let (mut inflight, mut peak) = (0i32, 0i32);
    for (_, step) in edges {
        inflight += step;
        peak = peak.max(inflight);
    }
    // Longest chain, by end time: a read extends the best chain of those
    // that ended before it started.
    let mut by_end = timed.clone();
    by_end.sort_by_key(|&(_, end)| end);
    let mut best = Vec::<(Instant, usize)>::with_capacity(by_end.len());
    for &(start, end) in &by_end {
        let before = best.partition_point(|&(ended, _)| ended <= start);
        let chain = 1 + if before == 0 { 0 } else { best[before - 1].1 };
        let running = best.last().map_or(0, |&(_, longest)| longest);
        best.push((end, chain.max(running)));
    }
    Waves {
        reads: timed.len(),
        span_ms: ms(last - first),
        busy_ms: ms(busy),
        peak: usize::try_from(peak).unwrap_or(0),
        waves: best.last().map_or(0, |&(_, longest)| longest),
    }
}

#[test]
fn waves_count_dependent_reads() {
    let origin = Instant::now();
    let at = |from: u64, to: u64| {
        (
            origin + Duration::from_millis(from),
            origin + Duration::from_millis(to),
            String::new(),
        )
    };
    // Two overlapping reads, then one after both, then a gap and a fourth.
    let shape = waves(&[at(0, 20), at(5, 25), at(25, 45), at(60, 80)]);
    assert_eq!((shape.reads, shape.peak, shape.waves), (4, 2, 3));
    assert!((shape.busy_ms - 65.0).abs() < 1e-6);
    assert!((shape.span_ms - 80.0).abs() < 1e-6);
    assert_eq!(waves(&[]).reads, 0);
}

/// Replays cold-battery cases (`PROFILE_CASES`, a JSON list of
/// [`FuzzCase`]s) against a copy of a cold-battery store (`PROFILE_STORE`,
/// a local object-store root, holding the database at `PROFILE_DATA_PATH`),
/// each `PROFILE_RUNS` (3) times on a fresh reader whose object store waits
/// `PROFILE_LATENCY_MS` (20) per read, then once warm. Prints, and writes
/// as JSON lines to `PROFILE_OUT`, each case's median cold time, its reads'
/// wave shape and its storage operations by record type; `PROFILE_TIMELINE`
/// also prints every read of the last run. `PROFILE_FILTER` keeps cases
/// whose ID or family contains it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "manual profiling"]
async fn profile_cold_store() {
    use std::io::Write as _;

    let store = std::path::PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    let cases: Vec<FuzzCase> =
        serde_json::from_slice(&std::fs::read(std::env::var("PROFILE_CASES").unwrap()).unwrap())
            .unwrap();
    let runs: usize = std::env::var("PROFILE_RUNS").map_or(3, |value| value.parse().unwrap());
    let latency = Duration::from_millis(
        std::env::var("PROFILE_LATENCY_MS").map_or(20, |value| value.parse().unwrap()),
    );
    let filter = std::env::var("PROFILE_FILTER").ok();
    let namespace = Namespace::new(
        std::env::var("PROFILE_NAMESPACE").unwrap_or_else(|_| "regression".to_owned()),
    )
    .unwrap();
    let mut config = production_config(&store);
    if let StorageConfig::SlateDb(storage) = &mut config.storage {
        storage.path =
            std::env::var("PROFILE_DATA_PATH").unwrap_or_else(|_| "fuzz/shard-0000".to_owned());
    }
    let mut out = std::env::var("PROFILE_OUT")
        .ok()
        .map(|path| std::io::BufWriter::new(std::fs::File::create(path).unwrap()));
    println!("tuning: {:?} latency {latency:?}", profile_tuning());
    for case in cases.iter().filter(|case| {
        filter
            .as_deref()
            .is_none_or(|filter| case.id.contains(filter) || case.family.contains(filter))
    }) {
        let mut cold = Vec::new();
        let mut last = None;
        for _ in 0..runs {
            let gets = Arc::new(AtomicUsize::new(0));
            let mut db = cold_reader(&config, latency, &gets).await;
            let counting = Arc::new(common::storage::counting::CountingStorage::new(
                db.storage.clone(),
                classify_key,
            ));
            db.storage = counting.clone();
            GET_SPANS.lock().unwrap().clear();
            GET_KEYS.lock().unwrap().clear();
            let started = Instant::now();
            let results = run_case(&db, &namespace, case).await.unwrap();
            let elapsed = ms(started.elapsed());
            let spans = std::mem::take(&mut *GET_SPANS.lock().unwrap());
            let io = counting.take();
            let warm = Instant::now();
            run_case(&db, &namespace, case).await.unwrap();
            let warm = ms(warm.elapsed());
            db.close().await.unwrap();
            cold.push(elapsed);
            last = Some((elapsed, warm, results, spans, io, started));
        }
        cold.sort_by(f64::total_cmp);
        let median = cold[cold.len() / 2];
        let (_, warm, results, spans, io, started) = last.unwrap();
        let shape = waves(&spans);
        let first_read = spans
            .iter()
            .map(|(start, _, _)| ms(*start - started))
            .fold(f64::INFINITY, f64::min);
        println!(
            "{:<32} cold {median:>7.1}ms (runs {cold:.0?}) warm {warm:>6.1}ms results {results} | reads {} waves {} busy {:.0}ms span {:.0}ms peak {} first read at {first_read:.1}ms",
            case.family, shape.reads, shape.waves, shape.busy_ms, shape.span_ms, shape.peak,
        );
        let mut classes = io.into_iter().collect::<Vec<_>>();
        classes.sort_by_key(|(_, stats)| std::cmp::Reverse(stats.elapsed));
        for ((op, class), stats) in &classes {
            println!(
                "    {op}/{class}: calls {} records {} bytes {} summed {:.0}ms",
                stats.calls,
                stats.records,
                stats.bytes,
                ms(stats.elapsed)
            );
        }
        if std::env::var("PROFILE_TIMELINE").is_ok() {
            let mut spans = spans;
            spans.sort_by_key(|(start, _, _)| *start);
            for (start, end, key) in &spans {
                println!(
                    "      {:>8.1} {:>6.1} {key}",
                    ms(*start - started),
                    ms(*end - *start)
                );
            }
        }
        if let Some(out) = &mut out {
            let row = serde_json::json!({
                "id": case.id,
                "family": case.family,
                "cold_ms": median,
                "runs_ms": cold,
                "warm_ms": warm,
                "results": results,
                "waves": shape,
                "io": classes
                    .iter()
                    .map(|((op, class), stats)| {
                        (
                            format!("{op}/{class}"),
                            serde_json::json!({
                                "calls": stats.calls,
                                "records": stats.records,
                                "bytes": stats.bytes,
                                "ms": ms(stats.elapsed),
                            }),
                        )
                    })
                    .collect::<serde_json::Map<_, _>>(),
            });
            writeln!(out, "{row}").unwrap();
        }
    }
    if let Some(out) = &mut out {
        out.flush().unwrap();
    }
}

/// This process's resident set in MiB, as `ps` reports it.
fn rss_mib() -> u64 {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u64>()
        .unwrap_or(0)
        / 1024
}

/// Metadata cache bytes in use and evictions so far, when it is metered.
fn meta_usage(cache: &SharedDbCache) -> (u64, u64) {
    cache
        .meta()
        .and_then(|meta| meta.usage.as_ref())
        .map_or((0, 0), |usage| {
            (usage.usage_bytes() >> 20, usage.evictions())
        })
}

/// SSTs, data bytes and planned metadata of the segments of the last
/// `hours`, without warming anything.
async fn plan_window(db: &LogDb, namespace: &Namespace, hours: i64) -> (usize, u64, u64, u64) {
    let window = WarmWindow {
        metadata: Some(Duration::from_secs(hours.unsigned_abs() * 3600)),
        payloads: None,
    };
    let mut plan = WarmPlan::default();
    db.plan_warm(std::slice::from_ref(namespace), window, &mut plan);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let report = warm_planned(
        "logs",
        plan,
        WarmBudget::UNBOUNDED,
        false,
        1,
        &cancel,
        &WarmLedger::new(true),
    )
    .await;
    let end = common::time::now_ns();
    let start = end - hours * 3600 * SECOND;
    let (mut data, mut l0, mut filters, mut index) = (0u64, 0u64, 0u64, 0u64);
    let slate = db.storage.slate_read().unwrap();
    let manifest = slate.manifest();
    for segment in manifest.segments() {
        let Ok((parsed, id, _)) = crate::codec::KEY_SCOPE.parse(segment.prefix()) else {
            continue;
        };
        if &parsed != namespace || id.start_ns >= end || id.end_ns().unwrap_or(i64::MAX) <= start {
            continue;
        }
        for view in segment.l0().iter().chain(
            segment
                .compacted()
                .iter()
                .flat_map(|run| run.sst_views.iter()),
        ) {
            filters += view.sst.info.filter_len;
            index += view.sst.info.index_len;
        }
        l0 += segment.l0().len() as u64;
        data += segment
            .l0()
            .iter()
            .map(|view| view.estimate_size())
            .sum::<u64>();
        data += segment
            .compacted()
            .iter()
            .map(|run| run.estimate_size())
            .sum::<u64>();
    }
    println!(
        "  window {hours:>2}h encoded filters {:.2} MiB index {:.2} MiB",
        filters as f64 / f64::from(1 << 20),
        index as f64 / f64::from(1 << 20)
    );
    (report.planned_ssts, data, report.planned_bytes, l0)
}

/// One budgeted metadata warm pass over the last `PROFILE_WARM_HOURS` (24)
/// into `PROFILE_WARM_PERCENT` (75) of the metadata cache, with data blocks
/// too under `PROFILE_WARM_PAYLOADS`.
async fn warm_pass(
    db: &LogDb,
    cache: &SharedDbCache,
    namespace: &Namespace,
    ledger: &WarmLedger,
    cancel: &CancellationToken,
) -> WarmReport {
    let hours: u64 = std::env::var("PROFILE_WARM_HOURS").map_or(24, |value| value.parse().unwrap());
    let percent: u8 =
        std::env::var("PROFILE_WARM_PERCENT").map_or(75, |value| value.parse().unwrap());
    let payloads = std::env::var("PROFILE_WARM_PAYLOADS").is_ok();
    let mut plan = WarmPlan::default();
    let range = Duration::from_secs(hours * 3600);
    let window = WarmWindow {
        metadata: Some(range),
        payloads: payloads.then_some(range),
    };
    db.plan_warm(std::slice::from_ref(namespace), window, &mut plan);
    warm_planned(
        "logs",
        plan,
        WarmBudget::of(
            cache,
            WarmShares {
                metadata_percent: percent,
                ..Default::default()
            },
        )
        .unwrap(),
        payloads,
        2,
        cancel,
        ledger,
    )
    .await
}

/// Queries a Console-like workload sends: cluster charts, a needle search
/// and one pod's recent lines, each over its range ending at `end`.
fn warm_queries(end: i64) -> Vec<(&'static str, QueryRequest, QueryOptions)> {
    let chart = |range: i64, step: i64| {
        QueryRequest::range(
            format!(r#"sum by (namespace) (count_over_time({{cluster="mgmt"}} [{step}s]))"#),
            end - range * SECOND,
            end,
            step * SECOND,
        )
        .frontend_step_aligned()
    };
    let logs = |query: &str, range: i64| {
        QueryRequest::range(
            query,
            end - range * SECOND,
            end,
            (range / 100).max(1) * SECOND,
        )
    };
    let options = QueryOptions {
        limit: 100,
        direction: Direction::Backward,
        ..QueryOptions::default()
    };
    vec![
        ("chart.1h", chart(3600, 60), options.clone()),
        ("chart.6h", chart(6 * 3600, 300), options.clone()),
        ("chart.24h", chart(86_400, 1800), options.clone()),
        (
            "needle.1h",
            logs(r#"{cluster="demo-prod"} |= "trace_id=00000""#, 3600),
            options.clone(),
        ),
        (
            "needle.24h",
            logs(r#"{cluster="demo-prod"} |= "trace_id=00000""#, 86_400),
            options.clone(),
        ),
        (
            "pod.1h",
            logs(r#"{cluster="mgmt", pod="mgmt-pod-7"}"#, 3600),
            options.clone(),
        ),
        (
            "pod.24h",
            logs(
                r#"{cluster="gke-fleet-dev", pod="gke-fleet-dev-pod-40"}"#,
                86_400,
            ),
            options.clone(),
        ),
    ]
}

/// Metadata warming over a production-shaped store at `PROFILE_STORE`,
/// ingesting `PROFILE_HOURS` (24) first if it is empty and rolling it up
/// once with `PROFILE_ROLLUP`. `PROFILE_MODE` picks the measurement:
///
/// - `shape`: SSTs, data and planned metadata of the last 1 to 24 hours,
///   then one unbounded pass into a `PROFILE_META_CACHE` cache measuring
///   what the metadata really weighs, with RSS before and after.
/// - `arms` (default): each query on a fresh cold reader with
///   `PROFILE_LATENCY_MS` (20) per object-store read, reading through
///   `PROFILE_UNFILTERED_READS`. `PROFILE_WARM=1` first runs a budgeted
///   warm pass to completion; `PROFILE_WARM=ready` starts it in the
///   background and queries at once, as right after readiness.
/// - `cycles`: one reader warming every `PROFILE_INTERVAL_S` (10) for
///   `PROFILE_MINUTES` (20), printing RSS, cache use and the ledger after
///   each pass, while a `writer` run ingests and rolls up beside it.
/// - `writer`: ingests the hour ending now and runs a rollup pass, over and
///   over for `PROFILE_MINUTES`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_metadata_warm() {
    let store = std::path::PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    let hours: i64 = std::env::var("PROFILE_HOURS").map_or(24, |value| value.parse().unwrap());
    let latency = Duration::from_millis(
        std::env::var("PROFILE_LATENCY_MS").map_or(20, |value| value.parse().unwrap()),
    );
    let minutes: u64 = std::env::var("PROFILE_MINUTES").map_or(20, |value| value.parse().unwrap());
    let mode = std::env::var("PROFILE_MODE").unwrap_or_else(|_| "arms".to_owned());
    let namespace = Namespace::new("default").unwrap();
    let config = production_config(&store);
    let started = Instant::now();
    if !store.join("ingested").exists() {
        std::fs::create_dir_all(&store).unwrap();
        ingest_clusters(&config, &namespace, hours).await;
        let writer = LogDb::open(config.clone()).await.unwrap();
        let settled = settle(&writer, Duration::from_secs(10), Duration::from_secs(300)).await;
        writer.close().await.unwrap();
        std::fs::write(store.join("ingested"), common::time::now_ns().to_string()).unwrap();
        println!(
            "ingest: {hours}h in {:.0}s (settled {:.0}s), store {:.2} GiB",
            started.elapsed().as_secs_f64(),
            settled.as_secs_f64(),
            dir_bytes(&store) as f64 / f64::from(1 << 30)
        );
    }
    if std::env::var("PROFILE_ROLLUP").is_ok() && !store.join("rolled").exists() {
        let writer = LogDb::open(config.clone()).await.unwrap();
        let timed = Instant::now();
        let sealed = writer
            .rollup_at(
                &common::storage::rollup::RollupConfig::default(),
                common::time::now_ns(),
            )
            .await
            .unwrap();
        let settled = settle(&writer, Duration::from_secs(10), Duration::from_secs(600)).await;
        writer.close().await.unwrap();
        std::fs::write(store.join("rolled"), "").unwrap();
        println!(
            "rollup: sealed {sealed} in {:.0}s (settled {:.0}s)",
            timed.elapsed().as_secs_f64(),
            settled.as_secs_f64()
        );
    }
    let ingested_end: i64 = std::fs::read_to_string(store.join("ingested"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let end = ingested_end / (60 * SECOND) * (60 * SECOND);
    match mode.as_str() {
        "shape" => {
            let gets = Arc::new(AtomicUsize::new(0));
            let (db, cache) = cold_reader_cached(&config, Duration::ZERO, &gets).await;
            for window in [1, 2, 4, 8, 12, 24, 48] {
                let (ssts, data, meta, l0) = plan_window(&db, &namespace, window).await;
                println!(
                    "window {window:>2}h: ssts {ssts:>5} (l0 {l0:>4}) data {:>8.1} MiB planned metadata {:>7.2} MiB ({:.2}% of data)",
                    data as f64 / f64::from(1 << 20),
                    meta as f64 / f64::from(1 << 20),
                    meta as f64 * 100.0 / data.max(1) as f64,
                );
            }
            let before = rss_mib();
            let timed = Instant::now();
            let ledger = WarmLedger::new(true);
            let report =
                warm_pass(&db, &cache, &namespace, &ledger, &CancellationToken::new()).await;
            let (used, evictions) = meta_usage(&cache);
            println!(
                "warm: {:.1}s planned {} ssts {:.2} MiB, warmed {} ssts {:.2} MiB measured, skipped {} ({:.2} MiB), cache {used} MiB evictions {evictions}, rss {before} -> {} MiB",
                timed.elapsed().as_secs_f64(),
                report.planned_ssts,
                report.planned_bytes as f64 / f64::from(1 << 20),
                report.warmed_ssts,
                report.warmed_bytes as f64 / f64::from(1 << 20),
                report.skipped_ssts,
                report.skipped_bytes as f64 / f64::from(1 << 20),
                rss_mib(),
            );
            db.close().await.unwrap();
        }
        "arms" => {
            let warm = std::env::var("PROFILE_WARM").unwrap_or_else(|_| "0".to_owned());
            let runs: usize =
                std::env::var("PROFILE_RUNS").map_or(3, |value| value.parse().unwrap());
            println!(
                "arm: unfiltered {} warm {warm} latency {latency:?}",
                profile_tuning().unfiltered_reads
            );
            let mut peak = 0;
            for (name, request, options) in warm_queries(end) {
                let mut cold = Vec::new();
                let mut summary = String::new();
                for _ in 0..runs {
                    let gets = Arc::new(AtomicUsize::new(0));
                    let (db, cache) = cold_reader_cached(&config, latency, &gets).await;
                    let db = Arc::new(db);
                    let ledger = Arc::new(WarmLedger::new(true));
                    let cancel = CancellationToken::new();
                    let mut warmed = None;
                    let mut background = None;
                    match warm.as_str() {
                        "1" => {
                            let timed = Instant::now();
                            let report = warm_pass(&db, &cache, &namespace, &ledger, &cancel).await;
                            warmed = Some((timed.elapsed(), report));
                        }
                        "ready" => {
                            let (db, cache, ledger, cancel, namespace) = (
                                Arc::clone(&db),
                                cache.clone(),
                                Arc::clone(&ledger),
                                cancel.clone(),
                                namespace.clone(),
                            );
                            background = Some(tokio::spawn(async move {
                                warm_pass(&db, &cache, &namespace, &ledger, &cancel).await
                            }));
                        }
                        _ => {}
                    }
                    let warm_gets = gets.swap(0, Ordering::Relaxed);
                    let timed = Instant::now();
                    let rows = match db
                        .query(&namespace, &request, options.clone())
                        .await
                        .unwrap()
                    {
                        crate::QueryResult::Streams(streams) => streams.len(),
                        crate::QueryResult::Vector(samples) => samples.len(),
                        crate::QueryResult::Matrix(series) => series.len(),
                        crate::QueryResult::Scalar(_) => 1,
                    };
                    let elapsed = ms(timed.elapsed());
                    let query_gets = gets.swap(0, Ordering::Relaxed);
                    let timed = Instant::now();
                    db.query(&namespace, &request, options.clone())
                        .await
                        .unwrap();
                    let hot = ms(timed.elapsed());
                    cancel.cancel();
                    if let Some(task) = background {
                        task.await.unwrap();
                    }
                    let rss = rss_mib();
                    peak = peak.max(rss);
                    let (used, _) = meta_usage(&cache);
                    summary = format!(
                        "rows {rows} gets {query_gets} hot {hot:.0}ms meta {used} MiB rss {rss} MiB{}",
                        warmed.map_or(
                            String::new(),
                            |(took, report): (Duration, WarmReport)| format!(
                                " | warm {:.1}s {} ssts {:.1} MiB skipped {} gets {warm_gets}",
                                took.as_secs_f64(),
                                report.warmed_ssts,
                                report.warmed_bytes as f64 / f64::from(1 << 20),
                                report.skipped_ssts,
                            )
                        )
                    );
                    cold.push(elapsed);
                    Arc::into_inner(db).unwrap().close().await.unwrap();
                }
                let mut sorted = cold.clone();
                sorted.sort_by(f64::total_cmp);
                println!(
                    "{name:<11} cold p50 {:>7.1}ms runs {cold:.0?} {summary}",
                    sorted[sorted.len() / 2]
                );
            }
            println!("peak rss {peak} MiB");
        }
        "cycles" => {
            let interval = Duration::from_secs(
                std::env::var("PROFILE_INTERVAL_S").map_or(10, |value| value.parse().unwrap()),
            );
            let gets = Arc::new(AtomicUsize::new(0));
            let (db, cache) = cold_reader_cached(&config, Duration::ZERO, &gets).await;
            let ledger = WarmLedger::new(true);
            let cancel = CancellationToken::new();
            let deadline = Instant::now() + Duration::from_secs(minutes * 60);
            let queries = warm_queries(common::time::now_ns() / SECOND * SECOND);
            let mut pass = 0usize;
            println!("cycles: open rss {} MiB", rss_mib());
            while Instant::now() < deadline {
                let timed = Instant::now();
                let report = warm_pass(&db, &cache, &namespace, &ledger, &cancel).await;
                let took = timed.elapsed();
                let warm_gets = gets.swap(0, Ordering::Relaxed);
                let (name, request, options) = &queries[pass % queries.len()];
                db.query(&namespace, request, options.clone())
                    .await
                    .unwrap();
                let (used, evictions) = meta_usage(&cache);
                let l0: usize = db
                    .storage
                    .slate_read()
                    .unwrap()
                    .manifest()
                    .segments()
                    .iter()
                    .map(|segment| segment.l0().len())
                    .sum();
                println!(
                    "t={:>5.0}s pass {pass:>3} {:.1}s gets {warm_gets:>5} warmed {:>3} evicted {:>3} displaced {:>3} skipped {:>4} ledger {:>3} ssts {:>6.1} MiB | meta {used} MiB evictions {evictions} l0 {l0} | {name} | rss {} MiB",
                    started.elapsed().as_secs_f64(),
                    took.as_secs_f64(),
                    report.warmed_ssts,
                    report.evicted_ssts,
                    report.displaced_ssts,
                    report.skipped_ssts,
                    ledger.ssts(),
                    ledger.bytes() as f64 / f64::from(1 << 20),
                    rss_mib(),
                );
                pass += 1;
                tokio::time::sleep(interval.saturating_sub(took)).await;
            }
            db.close().await.unwrap();
        }
        "writer" => {
            let deadline = Instant::now() + Duration::from_secs(minutes * 60);
            let mut round = 0;
            while Instant::now() < deadline {
                ingest_clusters(&config, &namespace, 1).await;
                let writer = LogDb::open(config.clone()).await.unwrap();
                let sealed = writer
                    .rollup_at(
                        &common::storage::rollup::RollupConfig::default(),
                        common::time::now_ns(),
                    )
                    .await;
                writer.close().await.unwrap();
                println!(
                    "t={:>5.0}s writer round {round} sealed {sealed:?}",
                    started.elapsed().as_secs_f64()
                );
                round += 1;
            }
        }
        other => panic!("unknown PROFILE_MODE {other}"),
    }
}
