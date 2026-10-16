//! Every read tuning and write layout must answer exactly as the defaults
//! do, with or without label projection, and no read tuning may fail a
//! query that loading one chunk at a time would answer within its memory
//! limit.

use super::*;
use crate::model::{MetricType, Sample};
use crate::promql::memory::MemoryReservation;
use crate::promql::plan::LoweringContext;
use crate::read_tuning::{
    ReadTuning, WriteLayout, every_layout, every_tuning, override_read_tuning,
    override_write_layout,
};
use crate::storage::in_memory_storage;

const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 60 * MINUTE_MS;
/// 2024-01-01.
const DAY_MS: i64 = 19_723 * 24 * HOUR_MS;
const START_MS: i64 = DAY_MS + 10 * HOUR_MS;
const END_MS: i64 = START_MS + 2 * HOUR_MS - MINUTE_MS;

const CPU: &str = "container_cpu_usage_seconds_total";
const MEMORY: &str = "container_memory_working_set_bytes";
const NETWORK: &str = "container_network_receive_bytes_total";

/// Selectors over several 512-series chunks, a sparse selection, a metric
/// too sparse to cluster, and a join of two.
const QUERIES: &[&str] = &[
    "container_cpu_usage_seconds_total",
    "sum by (namespace, pod) (rate(container_cpu_usage_seconds_total[5m]))",
    r#"max by (namespace) (container_memory_working_set_bytes{pod=~"pod-.7"})"#,
    "sum by (namespace) (rate(container_network_receive_bytes_total[5m]))",
    "sum by (namespace, pod) (rate(container_cpu_usage_seconds_total[5m])) / sum by (namespace, pod) (container_memory_working_set_bytes)",
];

/// 1,300 containers written container-major with a cpu and a memory series
/// each, so a metric's IDs are 2 apart, and a network series on every 37th.
/// Every 11th container starts in the second hour.
async fn fixture() -> Arc<Storage> {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::with_retention(storage.clone(), None);
    let mut batch = Vec::new();
    for container in 0..1300 {
        let namespace = format!("ns-{}", container % 7);
        let pod = format!("pod-{:02}", container % 100);
        let id = format!("/c{container}");
        let from = if container % 11 == 0 {
            START_MS + HOUR_MS
        } else {
            START_MS - 10 * MINUTE_MS
        };
        let metrics = [CPU, MEMORY]
            .into_iter()
            .chain((container % 37 == 0).then_some(NETWORK));
        for metric in metrics {
            let labels = vec![
                Label::metric_name(metric),
                Label::new("namespace", namespace.as_str()),
                Label::new("pod", pod.as_str()),
                Label::new("id", id.as_str()),
            ];
            for timestamp_ms in (from..END_MS).step_by(120_000) {
                batch.push(Series {
                    labels: labels.clone(),
                    unit: None,
                    metric_type: Some(MetricType::Gauge),
                    description: None,
                    samples: vec![Sample::new(
                        timestamp_ms,
                        f64::from(container) + ((timestamp_ms - from) / 1_000) as f64,
                    )],
                    histograms: Vec::new(),
                });
            }
        }
    }
    tsdb.ingest_samples(batch, None).await.unwrap();
    tsdb.flush().await.unwrap();
    storage
}

type Answer = Vec<(String, Vec<(i64, f64)>)>;

/// `query` through a fresh engine over `storage`, so no cache of an earlier
/// run serves it.
async fn run(
    storage: &Arc<Storage>,
    query: &str,
    projection: bool,
    memory: usize,
) -> std::result::Result<Answer, QueryError> {
    let tsdb = Tsdb::with_retention(storage.clone(), None);
    let opts = QueryOptions::default();
    let ctx = LoweringContext::new(
        START_MS,
        END_MS,
        5 * MINUTE_MS,
        duration_to_ms(opts.lookback_delta),
    )
    .with_label_projection(projection);
    let ranges = preload_ranges_for_query(query, START_MS, END_MS, opts.lookback_delta).unwrap();
    let reader = tsdb.make_query_reader_for_ranges(&ranges).await.unwrap();
    let reader =
        crate::query::LimitedQueryReader::new(reader, crate::query::QueryLimits::new(&opts));
    let source = Arc::new(crate::promql::source_adapter::QueryReaderSource::new(
        Arc::new(reader),
    ));
    let outcome =
        execute_query_source(query, source, ctx, false, MemoryReservation::new(memory)).await?;
    let mut series: Answer = outcome
        .value
        .into_matrix()
        .into_iter()
        .map(|sample| (format!("{:?}", sample.labels), sample.samples))
        .collect();
    series.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(series)
}

async fn run_with(
    tuning: ReadTuning,
    storage: &Arc<Storage>,
    query: &str,
    projection: bool,
    memory: usize,
) -> std::result::Result<Answer, QueryError> {
    let _tuning = override_read_tuning(tuning).await;
    run(storage, query, projection, memory).await
}

/// [`fixture`] written under `layout`, then rolled up into its 4h window
/// when `rolled`.
async fn fixture_with(layout: WriteLayout, rolled: bool) -> Arc<Storage> {
    let _layout = override_write_layout(layout).await;
    let storage = fixture().await;
    if rolled {
        storage.flush_memtable().await.unwrap();
        let tsdb = Tsdb::with_retention(storage.clone(), None);
        let config = crate::RollupConfig {
            copy_buffer_bytes: 64 << 10,
            ..Default::default()
        };
        let sealed = tsdb.rollup(&config, START_MS + 4 * HOUR_MS).await.unwrap();
        assert_eq!(sealed, 1);
    }
    storage
}

#[tokio::test]
async fn should_answer_the_same_under_every_write_layout() {
    let baseline = fixture_with(every_layout()[0], false).await;
    let mut expected = Vec::new();
    for query in QUERIES {
        for projection in [false, true] {
            let answer = run_with(
                ReadTuning::DEFAULT,
                &baseline,
                query,
                projection,
                DEFAULT_MEMORY_CAP_BYTES,
            )
            .await
            .unwrap();
            expected.push(answer);
        }
    }
    for layout in every_layout() {
        for rolled in [false, true] {
            let storage = fixture_with(layout, rolled).await;
            let mut expected = expected.iter();
            for query in QUERIES {
                for projection in [false, true] {
                    let expected = expected.next().unwrap();
                    for tuning in [ReadTuning::DEFAULT, every_tuning()[0]] {
                        let answer = run_with(
                            tuning,
                            &storage,
                            query,
                            projection,
                            DEFAULT_MEMORY_CAP_BYTES,
                        )
                        .await
                        .unwrap();
                        assert_eq!(
                            &answer, expected,
                            "{query} projection={projection} {layout:?} rolled={rolled} {tuning:?}"
                        );
                    }
                }
            }
        }
    }
}

fn over_limit(error: &QueryError) -> bool {
    matches!(error, QueryError::Execution(message) if message.starts_with("memory limit exceeded"))
}

#[tokio::test]
async fn should_answer_the_same_under_every_read_tuning() {
    let storage = fixture().await;
    for query in QUERIES {
        for projection in [false, true] {
            let expected = run_with(
                ReadTuning::DEFAULT,
                &storage,
                query,
                projection,
                DEFAULT_MEMORY_CAP_BYTES,
            )
            .await
            .unwrap();
            assert!(!expected.is_empty(), "{query} matched nothing");
            for tuning in every_tuning() {
                let answer = run_with(
                    tuning,
                    &storage,
                    query,
                    projection,
                    DEFAULT_MEMORY_CAP_BYTES,
                )
                .await
                .unwrap();
                assert_eq!(
                    answer, expected,
                    "{query} projection={projection} {tuning:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn should_fall_back_to_one_chunk_at_a_time_when_prefetch_exceeds_the_memory_limit() {
    // given: the smallest memory limit each query fits loading one chunk at
    // a time, which leaves no room for chunks loaded ahead
    let storage = fixture().await;
    let serial = ReadTuning {
        chunk_prefetch: 0,
        ..ReadTuning::DEFAULT
    };
    for query in &QUERIES[..2] {
        let expected = run_with(serial, &storage, query, false, DEFAULT_MEMORY_CAP_BYTES)
            .await
            .unwrap();
        let (mut too_small, mut fits) = (1, DEFAULT_MEMORY_CAP_BYTES);
        while fits - too_small > 1 {
            let memory = too_small + (fits - too_small) / 2;
            match run_with(serial, &storage, query, false, memory).await {
                Ok(_) => fits = memory,
                Err(error) if over_limit(&error) => too_small = memory,
                Err(error) => panic!("{query}: {error}"),
            }
        }

        for chunk_prefetch in [1, 2, 4] {
            let tuning = ReadTuning {
                chunk_prefetch,
                ..ReadTuning::DEFAULT
            };

            // when
            let at_limit = run_with(tuning, &storage, query, false, fits).await;
            let below = run_with(tuning, &storage, query, false, too_small).await;

            // then
            assert_eq!(
                at_limit.unwrap(),
                expected,
                "{query} prefetch={chunk_prefetch}"
            );
            assert!(
                below.as_ref().is_err_and(over_limit),
                "{query} prefetch={chunk_prefetch}: {below:?}"
            );
        }
    }
}
