//! Replays the promqltest corpus as range queries against two sharded
//! databases holding the same data, one with the result cache and one
//! without, over windows that exercise cold, partially reused, and fully
//! reused evaluations.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sharding::{DEFAULT_IO_CONCURRENCY_LIMIT, ShardId, ShardMap, ShardingOptions};

use crate::model::{HistogramSample, Label, MetricType, RangeSample, Sample, Series};
use crate::promql::promqltest::dsl::{Command, SeriesLoad, parse_test_file};
use crate::{Config, Namespace, QueryCacheConfig, ShardedMetrics, Visibility};

const SHARDS: u32 = 2;
const STEP: Duration = Duration::from_secs(60);

struct Pair {
    cached: ShardedMetrics,
    uncached: ShardedMetrics,
}

async fn open(path: &str, result_cache_enabled: bool) -> ShardedMetrics {
    let config = Config {
        storage: common::storage::config::SlateDbStorageConfig {
            path: path.to_string(),
            object_store: common::storage::config::ObjectStoreConfig::InMemory,
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        },
        query_cache: QueryCacheConfig {
            result_cache_enabled,
            ..Default::default()
        },
        ..Default::default()
    };
    ShardedMetrics::open_writers(
        config,
        ShardingOptions::new(SHARDS, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap(),
        (0..SHARDS).map(ShardId::new),
    )
    .await
    .unwrap()
}

async fn open_pair(generation: usize) -> Pair {
    Pair {
        cached: open(&format!("cached-{generation}"), true).await,
        uncached: open(&format!("uncached-{generation}"), false).await,
    }
}

fn assignment() -> ShardMap {
    ShardMap::new(
        sharding::AssignmentGeneration::new(1),
        SHARDS,
        vec![sharding::Assignment::new(
            sharding::Owner::new("metrics-0", 0),
            sharding::ShardRange::within(0, SHARDS, SHARDS).unwrap(),
            sharding::AssignmentState::Active,
        )],
    )
    .unwrap()
}

fn to_series(interval: Duration, load: &SeriesLoad) -> Series {
    let at = |step: i64| (interval.as_millis() as i64) * step;
    let mut samples: Vec<Sample> = load
        .values
        .iter()
        .map(|(step, value)| Sample::new(at(*step), *value))
        .collect();
    samples.sort_by_key(|sample| sample.timestamp_ms);
    samples.dedup_by_key(|sample| sample.timestamp_ms);
    let mut histograms: Vec<HistogramSample> = load
        .histograms
        .iter()
        .map(|(step, histogram)| HistogramSample {
            timestamp_ms: at(*step),
            histogram: histogram.clone(),
        })
        .collect();
    histograms.sort_by_key(|sample| sample.timestamp_ms);
    histograms.dedup_by_key(|sample| sample.timestamp_ms);
    Series {
        labels: load
            .labels
            .iter()
            .map(|(name, value)| Label::new(name.as_str(), value.as_str()))
            .collect(),
        metric_type: Some(MetricType::Gauge),
        unit: None,
        description: None,
        samples,
        histograms,
    }
}

type Normalized = Vec<(String, Vec<(i64, u64)>, Vec<(i64, String)>)>;

fn normalized(result: Result<Vec<RangeSample>, String>) -> Result<Normalized, ()> {
    let mut series: Normalized = result
        .map_err(|_| ())?
        .into_iter()
        .map(|series| {
            (
                format!("{:?}", series.labels),
                series
                    .samples
                    .into_iter()
                    .map(|(t, value)| (t, value.to_bits()))
                    .collect(),
                series
                    .histograms
                    .into_iter()
                    .map(|(t, histogram)| (t, format!("{histogram:?}")))
                    .collect(),
            )
        })
        .collect();
    series.sort();
    Ok(series)
}

async fn range(
    db: &ShardedMetrics,
    query: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<RangeSample>, String> {
    let at = |ms: i64| UNIX_EPOCH + Duration::from_millis(ms as u64);
    db.query_range(
        &Namespace::default(),
        query,
        at(start_ms)..=at(end_ms),
        STEP,
    )
    .await
    .map_err(|error| error.to_string())
}

async fn compare(pair: &Pair, name: &str, query: &str, time: SystemTime) {
    let step_ms = STEP.as_millis() as i64;
    let t = time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let start = (t - 15 * step_ms).max(0);
    let end = t + 5 * step_ms;
    let shifted = (start + 4 * step_ms, end + 4 * step_ms);
    for (start, end) in [(start, end), shifted, shifted] {
        let cached = normalized(range(&pair.cached, query, start, end).await);
        let uncached = normalized(range(&pair.uncached, query, start, end).await);
        assert_eq!(
            cached, uncached,
            "{name}: query {query} over [{start}, {end}] differs with the result cache"
        );
    }
}

#[tokio::test]
async fn should_match_uncached_range_queries_across_the_corpus() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/promql/promqltest/testdata");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "test"))
        .collect();
    files.sort();
    let routing = assignment();
    let namespace = Namespace::default();
    let mut generation = 0;
    for path in files {
        let name = path.file_stem().unwrap().to_string_lossy().into_owned();
        let commands = parse_test_file(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let mut pair = open_pair(generation).await;
        let mut ignoring = false;
        for command in commands {
            match command {
                Command::Clear(_) => {
                    generation += 1;
                    pair = open_pair(generation).await;
                }
                Command::Ignore(_) => ignoring = true,
                Command::Resume(_) => ignoring = false,
                Command::Load(load) if !ignoring => {
                    let series: Vec<Series> = load
                        .series
                        .iter()
                        .map(|series| to_series(load.interval, series))
                        .collect();
                    for db in [&pair.cached, &pair.uncached] {
                        db.write(&routing, &namespace, series.clone(), Visibility::Written)
                            .await
                            .unwrap();
                    }
                }
                Command::EvalInstant(eval) if !ignoring => {
                    compare(&pair, &name, &eval.query, eval.time).await;
                }
                Command::Load(_) | Command::EvalInstant(_) => {}
            }
        }
        generation += 1;
    }
}
