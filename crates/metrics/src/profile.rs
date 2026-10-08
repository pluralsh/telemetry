//! Native replay of a recorded fuzz run, for profiling the read path.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::storage::config::{
    BlockCacheConfig, FoyerMemoryCacheConfig, LocalObjectStoreConfig, ObjectStoreConfig,
    SlateDbStorageConfig,
};

use crate::model::{Labels, QueryValue, RangeSample};
use crate::promql::response::{LabelValuesResponse, LabelsResponse, SeriesResponse};
use crate::remote_write::{Protocol, parse_remote_write};
use crate::{
    Config, Namespace, QueryError, TimeSeriesDb, TimeSeriesDbReader, Visibility,
    query_value_to_response, range_result_to_response,
};

// The server's allocator, so profiles weigh allocation as it does.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// The metrics server's default `reader_cache_capacity`.
const READER_CACHE_BYTES: u64 = 256 << 20;

/// One matched fuzz case, as flattened by `python -m harness.fuzz.replay`.
#[derive(serde::Deserialize)]
struct FuzzCase {
    id: String,
    family: String,
    kind: String,
    impl_ms: f64,
    query: Option<String>,
    name: Option<String>,
    #[serde(default)]
    matchers: Vec<String>,
    time_ms: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    step_ms: Option<u64>,
}

#[allow(clippy::large_enum_variant)]
enum Engine {
    Writer(TimeSeriesDb),
    Reader(TimeSeriesDbReader),
}

/// A case's evaluated result, before the HTTP handlers' JSON encoding.
enum Evaluated {
    Instant(QueryValue),
    Range(Vec<RangeSample>),
    Series(Vec<Labels>),
    Names(Vec<String>),
    Values(Vec<String>),
}

impl Evaluated {
    /// The number of series or names, and the response body.
    fn encode(self) -> (usize, Vec<u8>) {
        match self {
            Self::Instant(value) => instant_answer(value),
            Self::Range(value) => range_answer(value),
            Self::Series(value) => series_answer(value),
            Self::Names(value) => names_answer(value, false),
            Self::Values(value) => names_answer(value, true),
        }
    }
}

fn at(ms: Option<u64>) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms.unwrap())
}

fn encoded(value: &impl serde::Serialize) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

fn instant_answer(value: QueryValue) -> (usize, Vec<u8>) {
    let count = match &value {
        QueryValue::Vector(samples) => samples.len(),
        QueryValue::Matrix(series) => series.len(),
        QueryValue::Scalar { .. } => 1,
    };
    (count, encoded(&query_value_to_response(Ok(value))))
}

fn range_answer(value: Vec<RangeSample>) -> (usize, Vec<u8>) {
    let count = value.len();
    (count, encoded(&range_result_to_response(Ok(value))))
}

fn series_answer(value: Vec<Labels>) -> (usize, Vec<u8>) {
    let count = value.len();
    let response = SeriesResponse {
        status: "success".to_owned(),
        data: Some(value),
        error: None,
        error_type: None,
    };
    (count, encoded(&response))
}

fn names_answer(value: Vec<String>, values: bool) -> (usize, Vec<u8>) {
    let count = value.len();
    let body = if values {
        encoded(&LabelValuesResponse {
            status: "success".to_owned(),
            data: Some(value),
            error: None,
            error_type: None,
        })
    } else {
        encoded(&LabelsResponse {
            status: "success".to_owned(),
            data: Some(value),
            error: None,
            error_type: None,
        })
    };
    (count, body)
}

macro_rules! on_engine {
    ($engine:expr, $db:ident => $body:expr) => {
        match $engine {
            Engine::Writer($db) => $body,
            Engine::Reader($db) => $body,
        }
    };
}

async fn run_case(
    engine: &Engine,
    namespace: &Namespace,
    case: &FuzzCase,
) -> Result<Evaluated, QueryError> {
    let matchers: Vec<&str> = case.matchers.iter().map(String::as_str).collect();
    let matchers = (!matchers.is_empty()).then_some(matchers.as_slice());
    match case.kind.as_str() {
        "instant" => on_engine!(engine, db => db
            .query(namespace, case.query.as_deref().unwrap(), Some(at(case.time_ms)))
            .await
            .map(Evaluated::Instant)),
        "range" => {
            let range = at(case.start_ms)..=at(case.end_ms);
            let step = Duration::from_millis(case.step_ms.unwrap());
            on_engine!(engine, db => db
                .query_range(namespace, case.query.as_deref().unwrap(), range, step)
                .await
                .map(Evaluated::Range))
        }
        "series" => {
            let range = at(case.start_ms)..=at(case.end_ms);
            on_engine!(engine, db => db
                .series(namespace, matchers.unwrap_or_default(), range)
                .await
                .map(Evaluated::Series))
        }
        "labels" => {
            let range = at(case.start_ms)..=at(case.end_ms);
            on_engine!(engine, db => db
                .labels(namespace, matchers, range)
                .await
                .map(Evaluated::Names))
        }
        "label_values" => {
            let range = at(case.start_ms)..=at(case.end_ms);
            let name = case.name.as_deref().unwrap();
            on_engine!(engine, db => db
                .label_values(namespace, name, matchers, range)
                .await
                .map(Evaluated::Values))
        }
        other => panic!("unknown case kind {other}"),
    }
}

fn storage(path: &Path) -> SlateDbStorageConfig {
    SlateDbStorageConfig {
        path: "profile".to_owned(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: path.display().to_string(),
        }),
        settings_path: std::env::var("PROFILE_SLATEDB_SETTINGS").ok(),
        block_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
            capacity: 64 << 20,
            shards: None,
        })),
        meta_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
            capacity: 16 << 20,
            shards: None,
        })),
    }
}

async fn ingest(input: &Path, store: &Path, namespace: &Namespace) {
    let mut bodies: Vec<PathBuf> = std::fs::read_dir(input.join("rounds"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "pb"))
        .collect();
    bodies.sort();
    let writer = TimeSeriesDb::open(Config {
        storage: storage(store),
        ..Default::default()
    })
    .await
    .unwrap();
    for path in bodies {
        let batch = parse_remote_write(&std::fs::read(&path).unwrap(), Protocol::V1).unwrap();
        writer
            .write_with_visibility(namespace, batch.series, Visibility::Durable)
            .await
            .unwrap();
    }
    writer.close().await.unwrap();
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn round_of(name: &str) -> usize {
    name.trim_start_matches("round-")
        .trim_start_matches('r')
        .get(..4)
        .and_then(|digits| digits.parse().ok())
        .unwrap_or_else(|| panic!("no round number in {name}"))
}

fn file_count(dir: &Path) -> usize {
    std::fs::read_dir(dir).map_or(0, Iterator::count)
}

/// Replays a fuzz run as the split stack ran it: a writer ingests each round
/// durably with the regression config's 1 s flush while a separate
/// `DbReader`-backed reader, with the regression reader's cache budget, waits
/// for the round to become visible and then answers that round's cases twice
/// (`cold_ms`, then `warm_ms`). Rows go to `PROFILE_OUT` with the store's WAL
/// and compacted SST counts at that round. `PROFILE_REOPEN` reopens the
/// reader before each round's cases; `PROFILE_SLATEDB_SETTINGS` points both
/// sides at a SlateDB settings file. `PROFILE_HOLD_SECS` then loops the last
/// quarter's cases for that long, for attaching a sampling profiler.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_fuzz_cases_live() {
    let input = PathBuf::from(std::env::var("PROFILE_CASES").unwrap());
    let store = PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    let cache_bytes: u64 =
        std::env::var("PROFILE_READER_CACHE").map_or(64 << 20, |value| value.parse().unwrap());
    let poll_ms: u64 =
        std::env::var("PROFILE_POLL_MS").map_or(1000, |value| value.parse().unwrap());
    let _ = std::fs::remove_dir_all(&store);
    std::fs::create_dir_all(&store).unwrap();
    let namespace = Namespace::new("regression").unwrap();

    let mut bodies: Vec<PathBuf> = std::fs::read_dir(input.join("rounds"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "pb"))
        .collect();
    bodies.sort();
    let cases: Vec<FuzzCase> =
        serde_json::from_slice(&std::fs::read(input.join("cases.json")).unwrap()).unwrap();
    let rounds = bodies
        .iter()
        .map(|path| round_of(path.file_name().unwrap().to_str().unwrap()))
        .max()
        .unwrap()
        + 1;

    let writer = TimeSeriesDb::open(Config {
        storage: storage(&store),
        flush_interval: Duration::from_secs(1),
        ..Default::default()
    })
    .await
    .unwrap();
    let reopen = std::env::var("PROFILE_REOPEN").is_ok();
    let open_reader = || async {
        Engine::Reader(
            TimeSeriesDbReader::open(
                storage(&store),
                slatedb::config::DbReaderOptions {
                    manifest_poll_interval: Duration::from_millis(poll_ms),
                    skip_wal_replay: false,
                    ..slatedb::config::DbReaderOptions::default()
                },
                cache_bytes,
            )
            .await
            .unwrap(),
        )
    };
    let mut engine = open_reader().await;

    let mut out = std::io::BufWriter::new(
        std::fs::File::create(std::env::var("PROFILE_OUT").unwrap()).unwrap(),
    );
    let everything = UNIX_EPOCH..=UNIX_EPOCH + Duration::from_secs(4_000_000_000);
    for round in 0..rounds {
        let started = Instant::now();
        for path in bodies
            .iter()
            .filter(|path| round_of(path.file_name().unwrap().to_str().unwrap()) == round)
        {
            let batch = parse_remote_write(&std::fs::read(path).unwrap(), Protocol::V1).unwrap();
            writer
                .write_with_visibility(&namespace, batch.series, Visibility::Durable)
                .await
                .unwrap();
        }
        let ingest = started.elapsed();
        let probe = format!(r#"{{fuzz_round="{round}"}}"#);
        let started = Instant::now();
        let mut visible = true;
        let Engine::Reader(reader) = &engine else {
            unreachable!()
        };
        while reader
            .series(&namespace, &[probe.as_str()], everything.clone())
            .await
            .unwrap()
            .is_empty()
        {
            if started.elapsed() > Duration::from_secs(15) {
                let written = writer
                    .series(&namespace, &[probe.as_str()], everything.clone())
                    .await
                    .unwrap()
                    .len();
                eprintln!("round {round} not visible to the reader; writer sees {written}");
                visible = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let visible = if visible { ms(started.elapsed()) } else { -1.0 };
        if reopen {
            reader.close().await.unwrap();
            engine = open_reader().await;
        }
        let root = store.join("profile");
        let (wal, ssts) = (
            file_count(&root.join("wal")),
            file_count(&root.join("compacted")),
        );
        for case in cases.iter().filter(|case| round_of(&case.id) == round) {
            let mut timings = [Duration::ZERO; 2];
            let mut error = None;
            for timing in &mut timings {
                let started = Instant::now();
                match run_case(&engine, &namespace, case).await {
                    Ok(value) => drop(value.encode()),
                    Err(err) => error = Some(err.to_string()),
                }
                *timing = started.elapsed();
            }
            let row = serde_json::json!({
                "id": case.id,
                "round": round,
                "family": case.family,
                "kind": case.kind,
                "query": case.query,
                "ms": ms(timings[0]),
                "cold_ms": ms(timings[0]),
                "warm_ms": ms(timings[1]),
                "impl_ms": case.impl_ms,
                "ingest_ms": ms(ingest),
                "visible_ms": visible,
                "wal": wal,
                "ssts": ssts,
                "error": error,
            });
            writeln!(out, "{row}").unwrap();
        }
        out.flush().unwrap();
    }
    if let Ok(secs) = std::env::var("PROFILE_HOLD_SECS") {
        let late: Vec<&FuzzCase> = cases
            .iter()
            .filter(|case| round_of(&case.id) * 4 >= rounds * 3)
            .collect();
        let end = Instant::now() + Duration::from_secs(secs.parse().unwrap());
        eprintln!("BEGIN hold pid={} cases={}", std::process::id(), late.len());
        while Instant::now() < end {
            for case in &late {
                let _ = run_case(&engine, &namespace, case)
                    .await
                    .map(Evaluated::encode);
            }
        }
        eprintln!("END hold");
    }
    if let Engine::Reader(reader) = &engine {
        reader.close().await.unwrap();
    }
    writer.close().await.unwrap();
}

/// Replays every matched case of a fuzz run (`PROFILE_CASES`, written by
/// `python -m harness.fuzz.replay metrics RUN OUT`) and writes per-query
/// evaluation and JSON encoding time to `PROFILE_OUT` as JSON lines. The store
/// at `PROFILE_STORE` is ingested once and reused across runs; queries go
/// through a reader unless `PROFILE_MODE=writer`. Each case runs
/// `PROFILE_REPS` times (default 3) and the fastest is kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_fuzz_cases() {
    let input = PathBuf::from(std::env::var("PROFILE_CASES").unwrap());
    let store = PathBuf::from(std::env::var("PROFILE_STORE").unwrap());
    let reps: usize = std::env::var("PROFILE_REPS").map_or(3, |value| value.parse().unwrap());
    let namespace = Namespace::new("regression").unwrap();
    let marker = store.join("ingested");
    if !marker.exists() {
        std::fs::create_dir_all(&store).unwrap();
        ingest(&input, &store, &namespace).await;
        std::fs::write(&marker, b"").unwrap();
    }
    let cases: Vec<FuzzCase> =
        serde_json::from_slice(&std::fs::read(input.join("cases.json")).unwrap()).unwrap();
    let engine = if std::env::var("PROFILE_MODE").as_deref() == Ok("writer") {
        Engine::Writer(
            TimeSeriesDb::open(Config {
                storage: storage(&store),
                ..Default::default()
            })
            .await
            .unwrap(),
        )
    } else {
        Engine::Reader(
            TimeSeriesDbReader::open(
                storage(&store),
                slatedb::config::DbReaderOptions::default(),
                READER_CACHE_BYTES,
            )
            .await
            .unwrap(),
        )
    };
    let mut out = std::io::BufWriter::new(
        std::fs::File::create(std::env::var("PROFILE_OUT").unwrap()).unwrap(),
    );
    for case in &cases {
        let mut best: Option<(Duration, Duration)> = None;
        let mut outcome = Ok((0, 0));
        for _ in 0..reps {
            let started = Instant::now();
            let evaluated = run_case(&engine, &namespace, case).await;
            let evaluate = started.elapsed();
            let started = Instant::now();
            outcome = evaluated.map(|value| {
                let (results, body) = value.encode();
                (results, body.len())
            });
            let encode = started.elapsed();
            if best
                .is_none_or(|(best_eval, best_encode)| evaluate + encode < best_eval + best_encode)
            {
                best = Some((evaluate, encode));
            }
        }
        let (evaluate, encode) = best.unwrap();
        let (results, bytes, error) = match outcome {
            Ok((results, bytes)) => (results, bytes, None),
            Err(error) => (0, 0, Some(error.to_string())),
        };
        let row = serde_json::json!({
            "id": case.id,
            "family": case.family,
            "kind": case.kind,
            "query": case.query,
            "ms": ms(evaluate + encode),
            "eval_ms": ms(evaluate),
            "encode_ms": ms(encode),
            "impl_ms": case.impl_ms,
            "results": results,
            "bytes": bytes,
            "error": error,
        });
        writeln!(out, "{row}").unwrap();
    }
    out.flush().unwrap();
    match engine {
        Engine::Writer(db) => db.close().await.unwrap(),
        Engine::Reader(reader) => reader.close().await.unwrap(),
    }
}
