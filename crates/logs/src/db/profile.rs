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
        }),
        segment_duration: Duration::from_secs(if production { 3600 } else { 60 }),
        discovery_rollup: Some(Duration::from_secs(24 * 3600)),
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
        ingest(&config, &namespace, rounds).await;
    }
    let db = LogDb::open_reader(config, DbReaderOptions::default())
        .await
        .unwrap();
    replay_queries(&db, &namespace, &queries).await;
    db.close().await.unwrap();
}

async fn ingest(config: &Config, namespace: &Namespace, rounds: Vec<Vec<ReplayStream>>) {
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
        tokio::time::sleep(Duration::from_secs(
            std::env::var("PROFILE_COMPACT").unwrap().parse().unwrap(),
        ))
        .await;
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
        let estimate = targets.estimate();
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
            "{} total {:>7.1}ms (fuzz impl {:.0}ms, loki {:.0}ms) | scan_targets {:.1}ms read {:.1}ms rest {:.1}ms | segments {} streams {streams} pages {} ({} compressed bytes) rows {} steps {}",
            query.id,
            ms(total),
            query.impl_ms,
            query.oracle_ms,
            ms(scan_t),
            ms(read_t),
            ms(total.saturating_sub(scan_t + read_t)),
            plan_segments(db, plan.scan_start, request.end_ns),
            estimate.pages,
            estimate.compressed_bytes,
            rows.len(),
            request
                .step_ns
                .map_or(1, |step| (request.end_ns - request.start_ns) / step + 1),
        );
    }
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
