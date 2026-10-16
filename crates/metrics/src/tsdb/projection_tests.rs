//! Aggregations resolving only their grouping labels must answer exactly as
//! with full label sets, whatever mix of buckets with and without stored
//! fingerprints they read.

use proptest::prelude::*;

use super::*;
use crate::config::RollupConfig;
use crate::model::{MetricType, Sample, SeriesId};
use crate::promql::plan::LoweringContext;
use crate::promql::trace::TraceCollector;
use crate::serde::key::SeriesFingerprintKey;
use crate::storage::in_memory_storage;
use common::storage::RecordOp;

const METRIC: &str = "container_cpu_usage_seconds_total";
const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 60 * MINUTE_MS;
/// 2024-01-01 in minutes.
const DAY: u32 = 19_723 * 24 * 60;
const DAY_MS: i64 = DAY as i64 * MINUTE_MS;
const START_MS: i64 = DAY_MS + 10 * HOUR_MS;
const END_MS: i64 = START_MS + 2 * HOUR_MS - MINUTE_MS;

const CPU_QUERY: &str = r#"sum by (namespace, pod) (rate(container_cpu_usage_seconds_total{container!="", pod!=""}[5m]))"#;

const QUERIES: &[&str] = &[
    CPU_QUERY,
    "max by (namespace) (container_cpu_usage_seconds_total)",
    "count by (pod) (container_cpu_usage_seconds_total)",
    "avg by (namespace, pod) (irate(container_cpu_usage_seconds_total[5m]))",
    "max by (namespace) (sum by (namespace, pod) (rate(container_cpu_usage_seconds_total[5m])))",
    "sum(container_cpu_usage_seconds_total)",
    "group by (container) (abs(container_cpu_usage_seconds_total))",
    "stddev by (namespace) (last_over_time(container_cpu_usage_seconds_total[10m]))",
    "min by (absent_label) (container_cpu_usage_seconds_total)",
    "sum by (namespace) (container_cpu_usage_seconds_total) / 2",
];

fn hour(index: u32) -> TimeBucket {
    TimeBucket::hour(DAY + (10 + index) * 60)
}

fn day() -> TimeBucket {
    TimeBucket {
        start: DAY,
        size: 24,
    }
}

/// One container's counter, scraped every 30s over `[from_ms, to_ms)`.
fn container(
    labels: &[(&str, &str)],
    rate: f64,
    from_ms: i64,
    to_ms: i64,
) -> impl Iterator<Item = Series> {
    let mut labels: Vec<Label> = labels
        .iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(name, value)| Label::new(*name, *value))
        .collect();
    labels.push(Label::metric_name(METRIC));
    (from_ms..to_ms)
        .step_by(30_000)
        .map(move |timestamp_ms| Series {
            labels: labels.clone(),
            unit: None,
            metric_type: Some(MetricType::Gauge),
            description: None,
            samples: vec![Sample::new(
                timestamp_ms,
                rate * ((timestamp_ms - from_ms) / 1_000) as f64,
            )],
            histograms: Vec::new(),
        })
}

/// Series sharing `(namespace, pod)` across containers, one without a
/// namespace, one only in the second hour and one only in the first.
fn cluster() -> Vec<Series> {
    let mut batch = Vec::new();
    let mut rate = 1.0;
    for namespace in ["a", "b"] {
        for pod in ["p0", "p1", "p2"] {
            for container_name in ["app", "sidecar", ""] {
                let id = format!("/kubepods/{namespace}/{pod}/{container_name}");
                let labels = [
                    ("namespace", namespace),
                    ("pod", pod),
                    ("container", container_name),
                    ("id", id.as_str()),
                    ("image", "registry/app:1.2.3"),
                ];
                let (from, to) = match (namespace, pod, container_name) {
                    ("b", "p2", _) => (START_MS + HOUR_MS + 5 * MINUTE_MS, END_MS),
                    ("a", "p0", "sidecar") => {
                        (START_MS - 10 * MINUTE_MS, START_MS + 40 * MINUTE_MS)
                    }
                    _ => (START_MS - 10 * MINUTE_MS, END_MS),
                };
                batch.extend(container(&labels, rate, from, to));
                rate += 0.5;
            }
        }
    }
    batch.extend(container(
        &[("pod", "orphan"), ("container", "app"), ("id", "/orphan")],
        3.0,
        START_MS - 10 * MINUTE_MS,
        END_MS,
    ));
    batch
}

async fn ingest(tsdb: &Tsdb, batch: Vec<Series>) {
    tsdb.ingest_samples(batch, None).await.unwrap();
    tsdb.flush().await.unwrap();
}

async fn fixture() -> (Tsdb, Arc<Storage>) {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::with_retention(storage.clone(), None);
    ingest(&tsdb, cluster()).await;
    (tsdb, storage)
}

#[derive(Debug)]
struct Run {
    series: Vec<(String, Vec<(i64, f64)>)>,
    forward_index_bytes: u64,
    forward_index_calls: u64,
    fingerprint_calls: u64,
    /// `(kind, calls, bytes)` of every storage read.
    io: Vec<(&'static str, u64, u64)>,
    build_physical_ms: f64,
}

async fn run(tsdb: &Tsdb, query: &str, projection: bool) -> Run {
    let opts = QueryOptions::default();
    let ctx = LoweringContext::new(
        START_MS,
        END_MS,
        MINUTE_MS,
        duration_to_ms(opts.lookback_delta),
    )
    .with_label_projection(projection)
    .with_trace(TraceCollector::new());
    let ranges = preload_ranges_for_query(query, START_MS, END_MS, opts.lookback_delta).unwrap();
    let reader = tsdb.make_query_reader_for_ranges(&ranges).await.unwrap();
    let outcome = execute_query(query, reader, ctx, false, &opts)
        .await
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let trace = outcome.trace.expect("trace");
    let io = |kind: &str| {
        trace
            .io
            .iter()
            .find(|stats| stats.kind == kind)
            .map_or((0, 0), |stats| (stats.call_count, stats.bytes))
    };
    let (forward_index_calls, forward_index_bytes) = io("forward_index_fetch");
    let (fingerprint_calls, _) = io("series_fingerprint_fetch");
    let build_physical_ms = trace
        .phases
        .iter()
        .find(|phase| phase.name == "build_physical")
        .map_or(0.0, |phase| phase.elapsed_ms);
    let io = trace
        .io
        .iter()
        .map(|stats| (stats.kind, stats.call_count, stats.bytes))
        .collect();
    let mut series: Vec<_> = outcome
        .value
        .into_matrix()
        .into_iter()
        .map(|sample| (format!("{:?}", sample.labels), sample.samples))
        .collect();
    series.sort_by(|a, b| a.0.cmp(&b.0));
    Run {
        series,
        forward_index_bytes,
        forward_index_calls,
        fingerprint_calls,
        io,
        build_physical_ms,
    }
}

/// Projected first: a full resolution it ran after would be served from
/// the selector cache instead of the index.
async fn assert_same(tsdb: &Tsdb, query: &str) -> (Run, Run) {
    let projected = run(tsdb, query, true).await;
    let full = run(tsdb, query, false).await;
    assert!(!full.series.is_empty(), "{query} matched nothing");
    assert_eq!(projected.series, full.series, "{query}");
    (projected, full)
}

async fn series_ids(tsdb: &Tsdb, bucket: TimeBucket) -> (TsdbQueryReader, Vec<SeriesId>) {
    let reader = tsdb
        .query_reader(bucket.start_ms() / 1000, bucket.end_ms() / 1000 - 1)
        .await
        .unwrap();
    let term = Label::metric_name(METRIC);
    let ids = reader
        .inverted_index(&bucket, std::slice::from_ref(&term))
        .await
        .unwrap()
        .intersect(vec![term])
        .iter()
        .collect();
    (reader, ids)
}

async fn assert_fingerprints_stored(tsdb: &Tsdb, bucket: TimeBucket) {
    let (reader, ids) = series_ids(tsdb, bucket).await;
    assert!(!ids.is_empty());
    let specs = reader.forward_index_many(&bucket, &ids).await.unwrap();
    let stored = reader
        .series_fingerprints(&bucket, &ids)
        .await
        .unwrap()
        .expect("storage readers serve fingerprints");
    for (spec, fingerprint) in specs.iter().zip(stored) {
        let labels = &spec.as_ref().expect("forward index entry").labels;
        assert_eq!(fingerprint, Some(labels.series_fingerprint()), "{labels:?}");
    }
}

/// The bucket as a writer from before fingerprint records left it.
async fn forget_fingerprints(tsdb: &Tsdb, storage: &Storage, bucket: TimeBucket) {
    let (_, ids) = series_ids(tsdb, bucket).await;
    let deletes = ids
        .iter()
        .map(|&series_id| {
            let key = SeriesFingerprintKey {
                namespace: Namespace::default(),
                bucket,
                series_id,
            };
            RecordOp::Delete(key.encode())
        })
        .collect();
    storage.apply(deletes).await.unwrap();
    let (reader, ids) = series_ids(tsdb, bucket).await;
    let stored = reader.series_fingerprints(&bucket, &ids).await.unwrap();
    assert!(stored.unwrap().iter().all(Option::is_none));
}

#[tokio::test]
async fn flusher_stores_a_fingerprint_per_series() {
    let (tsdb, _storage) = fixture().await;
    assert_fingerprints_stored(&tsdb, hour(0)).await;
    assert_fingerprints_stored(&tsdb, hour(1)).await;
}

#[tokio::test]
async fn should_answer_projected_aggregations_as_full_ones() {
    for query in QUERIES {
        // A fresh store per query, so no selector cache serves the
        // projected run.
        let (tsdb, _storage) = fixture().await;
        assert_same(&tsdb, query).await;
    }
}

#[tokio::test]
async fn should_resolve_the_cpu_query_from_the_index_without_the_forward_index() {
    let (tsdb, _storage) = fixture().await;

    let (projected, full) = assert_same(&tsdb, CPU_QUERY).await;

    assert_eq!(projected.forward_index_calls, 0, "{projected:?}");
    assert!(projected.fingerprint_calls > 0, "{projected:?}");
    assert!(full.forward_index_calls > 0, "{full:?}");
    assert_eq!(full.fingerprint_calls, 0, "{full:?}");
    // Six (namespace, pod) groups plus the orphan pod.
    assert_eq!(projected.series.len(), 7);
}

#[tokio::test]
async fn should_answer_the_same_over_buckets_without_fingerprints() {
    for query in QUERIES {
        let (tsdb, storage) = fixture().await;
        forget_fingerprints(&tsdb, &storage, hour(0)).await;

        let (projected, _) = assert_same(&tsdb, query).await;

        if *query == CPU_QUERY {
            // The old bucket reads the forward index, the new one does not.
            assert!(projected.forward_index_calls > 0, "{projected:?}");
            assert!(projected.fingerprint_calls > 0, "{projected:?}");
        }
    }
}

#[tokio::test]
async fn should_answer_the_same_when_every_bucket_lacks_fingerprints() {
    let (tsdb, storage) = fixture().await;
    forget_fingerprints(&tsdb, &storage, hour(0)).await;
    forget_fingerprints(&tsdb, &storage, hour(1)).await;
    for query in QUERIES {
        assert_same(&tsdb, query).await;
    }
}

#[tokio::test]
async fn should_answer_the_same_over_a_rolled_up_day() {
    let (tsdb, storage) = fixture().await;
    storage.flush_memtable().await.unwrap();
    let config = RollupConfig {
        max_bucket_bytes: u64::MAX,
        ..RollupConfig::default()
    };

    let sealed = tsdb
        .rollup(&config, day().end_ms() + 2 * HOUR_MS)
        .await
        .unwrap();

    assert_eq!(sealed, 1);
    assert_fingerprints_stored(&tsdb, day()).await;
    for query in QUERIES {
        assert_same(&tsdb, query).await;
    }
}

/// Prints the cpu query's index reads with and without projection over
/// cAdvisor-shaped series: `cargo test -p plural-metrics --lib
/// measure_cpu_query_index_reads -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "measurement"]
async fn measure_cpu_query_index_reads() {
    let mut report = Vec::new();
    for projection in [true, false] {
        // Each run on its own store, so neither reads the other's caches.
        let tsdb = cadvisor().await;
        let started = std::time::Instant::now();
        let outcome = run(&tsdb, CPU_QUERY, projection).await;
        report.push((projection, started.elapsed(), outcome));
    }
    let (_, _, projected) = &report[0];
    let (_, _, full) = &report[1];
    assert_eq!(projected.series, full.series);
    println!("{} result series", full.series.len());
    for (projection, elapsed, outcome) in &report {
        println!(
            "projection={projection}: build_physical={:.1}ms elapsed={elapsed:?}",
            outcome.build_physical_ms
        );
        for (kind, calls, bytes) in &outcome.io {
            println!("  {kind}: calls={calls} bytes={bytes}");
        }
    }
}

/// 4,000 containers of 1,000 pods, 13 labels each.
async fn cadvisor() -> Tsdb {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::with_retention(storage, None);
    let mut series = 0;
    for namespace in 0..40 {
        let namespace = format!("namespace-{namespace:02}");
        let mut batch = Vec::new();
        for pod in 0..25 {
            let pod = format!("deployment-{pod:02}-7d9f8b6c5d-x{pod:03}");
            for container_name in ["app", "istio-proxy", "POD", ""] {
                let id = format!(
                    "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod{namespace}_{pod}.slice/cri-containerd-{container_name}.scope"
                );
                let name = format!("{:064x}", series as u128 * 0x9e37_79b9_7f4a_7c15);
                let image = format!("registry.example.com/{namespace}/{container_name}:v1.2.3");
                let labels = [
                    ("namespace", namespace.as_str()),
                    ("pod", pod.as_str()),
                    ("container", container_name),
                    ("id", id.as_str()),
                    ("image", image.as_str()),
                    ("name", name.as_str()),
                    ("cpu", "total"),
                    ("endpoint", "https-metrics"),
                    ("instance", "10.0.12.34:10250"),
                    ("job", "kubelet"),
                    ("metrics_path", "/metrics/cadvisor"),
                    ("node", "ip-10-0-12-34.ec2.internal"),
                    ("service", "kube-prometheus-stack-kubelet"),
                ];
                batch
                    .extend(container(&labels, 0.25, START_MS - 10 * MINUTE_MS, END_MS).step_by(4));
                series += 1;
            }
        }
        ingest(&tsdb, batch).await;
    }
    tsdb
}

/// Labels drawn from a small alphabet so groups collide, some absent.
fn arb_series() -> impl Strategy<Value = Vec<Vec<(&'static str, &'static str)>>> {
    let value = prop::sample::select(vec!["", "x", "y"]);
    let labels = (value.clone(), value.clone(), value)
        .prop_map(|(a, b, c)| vec![("a", a), ("b", b), ("c", c)]);
    prop::collection::vec(labels, 1..12)
}

fn arb_query() -> impl Strategy<Value = String> {
    let op = prop::sample::select(vec!["sum", "min", "max", "avg", "count", "group", "stdvar"]);
    let by = prop::sample::subsequence(vec!["a", "b", "c"], 0..=3);
    let inner = prop::sample::select(vec![
        METRIC.to_string(),
        format!("rate({METRIC}[5m])"),
        format!("abs(delta({METRIC}[10m]))"),
    ]);
    (op, by, inner).prop_map(|(op, by, inner)| format!("{op} by ({}) ({inner})", by.join(", ")))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn projection_never_changes_an_aggregation(
        series in arb_series(),
        query in arb_query(),
        forget_first_hour in any::<bool>(),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let storage = Arc::new(in_memory_storage().await);
            let tsdb = Tsdb::with_retention(storage.clone(), None);
            let mut batch = Vec::new();
            for (index, labels) in series.iter().enumerate() {
                let id = index.to_string();
                let mut labels: Vec<(&str, &str)> = labels.to_vec();
                labels.push(("id", id.as_str()));
                let from = if index % 3 == 2 { START_MS + HOUR_MS } else { START_MS };
                batch.extend(container(&labels, index as f64 + 1.0, from, END_MS));
            }
            ingest(&tsdb, batch).await;
            if forget_first_hour {
                forget_fingerprints(&tsdb, &storage, hour(0)).await;
            }
            let projected = run(&tsdb, &query, true).await;
            let full = run(&tsdb, &query, false).await;
            assert_eq!(projected.series, full.series, "{query}");
        });
    }
}
