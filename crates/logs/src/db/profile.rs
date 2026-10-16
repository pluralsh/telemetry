//! Temporary phase profiler for the log query read path.

use std::time::{Duration, Instant};

use common::storage::config::{
    BlockCacheConfig, FoyerMemoryCacheConfig, LocalObjectStoreConfig, ObjectStoreConfig,
    SlateDbStorageConfig, StorageConfig,
};
use slatedb::config::DbReaderOptions;

use super::*;
use crate::config::{CompactionConfig, PageConfig};
use crate::{Direction, LogBatch, LogEntry, QueryOptions, QueryRequest};

// The server's allocator, so profiles weigh allocation as it does.
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
        GET_KEYS
            .lock()
            .unwrap()
            .push(format!("{location} {:?}", options.range));
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

/// A reader with empty caches whose object store sleeps `latency` per
/// request, as a cold read against S3 does.
async fn cold_reader(config: &Config, latency: Duration, gets: &Arc<AtomicUsize>) -> LogDb {
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
        DbReaderOptions::default(),
    )
    .await
    .unwrap();
    LogDb {
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
    }
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
        gets.store(0, Ordering::Relaxed);
        for segment in &segments {
            let started = Instant::now();
            let streams = db
                .segment_streams(namespace, *segment, &plan.streams, range, now_unix_ms)
                .await
                .unwrap();
            println!(
                "  alone {}: {:.0}ms gets {} streams {} runs {}",
                segment.start_ns,
                ms(started.elapsed()),
                gets.swap(0, Ordering::Relaxed),
                streams.len(),
                streams
                    .iter()
                    .map(|stream| stream.runs.len())
                    .sum::<usize>(),
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
    let mut measures = vec![Vec::new(); names.len()];
    for run in 0..runs {
        let gets = Arc::new(AtomicUsize::new(0));
        let db = cold_reader(config, latency, &gets).await;
        gets.store(0, Ordering::Relaxed);
        let started = Instant::now();
        db.query(namespace, request, options.clone()).await.unwrap();
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
