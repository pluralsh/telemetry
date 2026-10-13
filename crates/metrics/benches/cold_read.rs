//! Cold dashboard queries through a reader over a slow object store.
//!
//! A writer loads a Kubernetes-shaped corpus (cAdvisor container CPU and
//! memory, node CPU, and filler metrics) into a local object store, one
//! write per series per hour as after compaction. Each query then runs on a
//! freshly opened reader, as the metrics server opens one, whose object
//! store sleeps `--latency-ms` per request and `--mib-per-sec` per byte, and
//! whose block cache starts empty; a second run on the same reader is the
//! warm case. Not a criterion bench.
//!
//! ```text
//! cargo bench -p plural-metrics --features bench-internals --bench cold_read -- \
//!     [--hours H] [--pods N] [--latency-ms MS] [--mib-per-sec N] [--step SECS] \
//!     [--query NAME|all] [--trace] [--repeat N] [--writes-per-hour W]
//! ```

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::storage::config::{
    BlockCacheConfig, FoyerMemoryCacheConfig, LocalObjectStoreConfig, ObjectStoreConfig,
    SlateDbStorageConfig,
};
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use plural_metrics::bench_support::{open_reader_over, reader_query_range_traced, storage_layout};
use plural_metrics::{Config, Label, MetricType, Namespace, Sample, Series, TimeSeriesDb};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::throttle::{ThrottleConfig, ThrottledStore};

// The server's allocator, so profiles weigh allocation as it does.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const HOUR_MS: i64 = 60 * 60 * 1000;
const T0_MS: i64 = 1_699_999_200_000;
const SCRAPE_MS: i64 = 15_000;
const NAMESPACES: usize = 30;
const NODES: usize = 20;
const CPUS: usize = 8;
const MODES: [&str; 8] = [
    "idle", "user", "system", "iowait", "irq", "softirq", "steal", "nice",
];
const FILLER_METRICS: usize = 25;
const FILLER_SERIES: usize = 200;

const QUERIES: [(&str, &str); 5] = [
    (
        "cpu_by_namespace",
        "sum by (namespace) (rate(container_cpu_usage_seconds_total[5m]))",
    ),
    (
        "memory_by_pod",
        r#"sum by (pod) (container_memory_working_set_bytes{namespace="ns-3"})"#,
    ),
    (
        "node_cpu",
        r#"1 - avg by (node) (rate(node_cpu_seconds_total{mode="idle"}[5m]))"#,
    ),
    (
        "top_pods",
        "topk(10, sum by (pod) (rate(container_cpu_usage_seconds_total[5m])))",
    ),
    ("memory_total", "sum(container_memory_working_set_bytes)"),
];

struct Args {
    hours: i64,
    pods: usize,
    latency: Duration,
    mib_per_sec: u64,
    step: Duration,
    query: Option<String>,
    trace: bool,
    /// Extra warm runs per query, for an external sampler.
    repeat: usize,
    /// Writes per series per hour: 1 is a fully compacted bucket; more
    /// model a live writer, each write a merge operand per series.
    writes_per_hour: i64,
}

fn parse_args() -> Args {
    let mut args = Args {
        hours: 24,
        pods: 1500,
        latency: Duration::from_millis(30),
        mib_per_sec: 100,
        step: Duration::from_secs(60),
        query: None,
        trace: false,
        repeat: 0,
        writes_per_hour: 1,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut value = || it.next().unwrap_or_else(|| panic!("{arg} needs a value"));
        match arg.as_str() {
            "--hours" => args.hours = value().parse().expect("hours"),
            "--pods" => args.pods = value().parse().expect("pods"),
            "--latency-ms" => {
                args.latency = Duration::from_millis(value().parse().expect("latency"));
            }
            "--mib-per-sec" => args.mib_per_sec = value().parse().expect("MiB/s"),
            "--step" => args.step = Duration::from_secs(value().parse().expect("step")),
            "--query" => args.query = Some(value()).filter(|q| q != "all"),
            "--trace" => args.trace = true,
            "--repeat" => args.repeat = value().parse().expect("repeat"),
            "--writes-per-hour" => {
                args.writes_per_hour = value().parse().expect("writes per hour");
            }
            "--bench" => {}
            other => panic!("unknown argument {other}"),
        }
    }
    args
}

/// Counts SlateDB's object-store requests by API.
mod requests {
    use super::*;

    static COUNTS: LazyLock<Mutex<BTreeMap<String, Arc<AtomicU64>>>> =
        LazyLock::new(Default::default);

    struct Capture;

    impl Recorder for Capture {
        fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
            if key.name() != "slatedb.object_store.request_count" {
                return Counter::noop();
            }
            let api = key
                .labels()
                .find(|label| label.key() == "api")
                .map_or("?".to_owned(), |label| label.value().to_owned());
            let mut counts = COUNTS.lock().expect("counts");
            Counter::from_arc(counts.entry(api).or_default().clone())
        }

        fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    pub fn install() {
        metrics::set_global_recorder(Capture).expect("install recorder");
    }

    pub fn snapshot() -> BTreeMap<String, u64> {
        COUNTS
            .lock()
            .expect("counts")
            .iter()
            .map(|(api, count)| (api.clone(), count.load(Ordering::Relaxed)))
            .collect()
    }

    pub fn since(before: &BTreeMap<String, u64>) -> String {
        snapshot()
            .into_iter()
            .map(|(api, count)| (api.clone(), count - before.get(&api).copied().unwrap_or(0)))
            .filter(|(_, count)| *count > 0)
            .map(|(api, count)| format!("{api}={count}"))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

fn label(name: &str, value: impl Into<String>) -> Label {
    Label::new(name, value.into())
}

/// Every series of the corpus, with whether it is a counter.
fn corpus(pods: usize) -> Vec<(Vec<Label>, bool)> {
    let mut series = Vec::new();
    for pod in 0..pods {
        let common = |metric: &str| {
            vec![
                Label::metric_name(metric),
                label("namespace", format!("ns-{}", pod % NAMESPACES)),
                label("pod", format!("pod-{pod}")),
                label("container", "app"),
                label("node", format!("node-{}", pod % NODES)),
            ]
        };
        series.push((common("container_cpu_usage_seconds_total"), true));
        series.push((common("container_memory_working_set_bytes"), false));
    }
    for node in 0..NODES {
        for cpu in 0..CPUS {
            for mode in MODES {
                series.push((
                    vec![
                        Label::metric_name("node_cpu_seconds_total"),
                        label("node", format!("node-{node}")),
                        label("cpu", cpu.to_string()),
                        label("mode", mode),
                    ],
                    true,
                ));
            }
        }
    }
    for metric in 0..FILLER_METRICS {
        for index in 0..FILLER_SERIES {
            series.push((
                vec![
                    Label::metric_name(format!("filler_{metric:02}")),
                    label("instance", format!("i-{index}")),
                ],
                false,
            ));
        }
    }
    series
}

fn value(series: usize, counter: bool, scrape: i64) -> f64 {
    let seed = (series as f64).mul_add(0.37, 1.0);
    if counter {
        scrape as f64 * seed
    } else {
        seed * 1e6 + (scrape % 97) as f64
    }
}

async fn load(storage: &SlateDbStorageConfig, args: &Args) -> usize {
    let writer = TimeSeriesDb::open(Config {
        storage: storage.clone(),
        retention: None,
        ..Default::default()
    })
    .await
    .expect("open writer");
    let namespace = Namespace::default();
    let corpus = corpus(args.pods);
    let per_hour = HOUR_MS / SCRAPE_MS;
    let per_write = (per_hour / args.writes_per_hour.clamp(1, per_hour)).max(1);
    for write in 0..args.hours * per_hour / per_write {
        let batch: Vec<Series> = corpus
            .iter()
            .enumerate()
            .map(|(index, (labels, counter))| {
                let mut series = Series::new(
                    labels[0].value.clone(),
                    labels[1..].to_vec(),
                    (0..per_write)
                        .map(|offset| {
                            let scrape = write * per_write + offset;
                            Sample::new(T0_MS + scrape * SCRAPE_MS, value(index, *counter, scrape))
                        })
                        .collect(),
                );
                if *counter {
                    series.metric_type = Some(MetricType::Sum {
                        monotonic: true,
                        temporality: plural_metrics::Temporality::Cumulative,
                    });
                }
                series
            })
            .collect();
        writer.write(&namespace, batch).await.expect("write");
        writer.flush().await.expect("flush");
    }
    settle(&writer).await;
    let (segments, l0, runs, run_ssts) = storage_layout(&writer);
    println!("layout: segments={segments} l0_ssts={l0} sorted_runs={runs} run_ssts={run_ssts}");
    writer.close().await.expect("close writer");
    corpus.len()
}

/// Waits until the live layout holds across three compactor polls, giving
/// up after 300 s.
async fn settle(writer: &TimeSeriesDb) {
    let deadline = Instant::now() + Duration::from_secs(300);
    let mut last = (storage_layout(writer), Instant::now());
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let layout = storage_layout(writer);
        if layout != last.0 {
            last = (layout, Instant::now());
        } else if last.1.elapsed() >= Duration::from_secs(16) {
            return;
        }
    }
    eprintln!("compactor did not settle within 300 s");
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e3
}

fn print_trace(trace: &serde_json::Value) {
    for phase in trace["phases"].as_array().into_iter().flatten() {
        println!(
            "    phase {:<16} {:>9.1} ms",
            phase["name"].as_str().unwrap_or("?"),
            phase["elapsedMs"].as_f64().unwrap_or(0.0)
        );
    }
    for io in trace["io"].as_array().into_iter().flatten() {
        println!(
            "    io {:<22} wall {:>9.1} ms  cumulative {:>10.1} ms  calls {:>8}  bytes {:>11}",
            io["kind"].as_str().unwrap_or("?"),
            io["wallMs"].as_f64().unwrap_or(0.0),
            io["cumulativeMs"].as_f64().unwrap_or(0.0),
            io["callCount"].as_u64().unwrap_or(0),
            io["bytes"].as_u64().unwrap_or(0),
        );
    }
    for op in trace["operators"].as_array().into_iter().flatten() {
        println!(
            "    op {:<3} {:<22} {:>9.1} ms  calls {:>6}",
            op["nodeId"],
            op["opName"].as_str().unwrap_or("?"),
            op["totalMs"].as_f64().unwrap_or(0.0),
            op["callCount"].as_u64().unwrap_or(0),
        );
    }
}

fn main() {
    let args = parse_args();
    requests::install();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let directory = tempfile::tempdir().expect("tempdir");
        let cache = |capacity| {
            Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
                capacity,
                shards: None,
            }))
        };
        let storage = SlateDbStorageConfig {
            path: "cold-read".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: directory.path().to_string_lossy().into_owned(),
            }),
            settings_path: None,
            block_cache: cache(512 << 20),
            meta_cache: cache(128 << 20),
            disk: Default::default(),
        };
        let started = Instant::now();
        let series = load(&storage, &args).await;
        println!(
            "loaded series={series} hours={} in {:.1}s; latency={:?} mib_per_sec={} step={:?}",
            args.hours,
            started.elapsed().as_secs_f64(),
            args.latency,
            args.mib_per_sec,
            args.step,
        );

        // The throttle only delays streamed payloads, which the local store
        // does not return.
        let local: Arc<dyn slatedb::object_store::ObjectStore> = {
            use futures::TryStreamExt;
            use slatedb::object_store::{ObjectStore, ObjectStoreExt};
            let disk = LocalFileSystem::new_with_prefix(directory.path()).expect("local store");
            let memory = InMemory::new();
            let objects: Vec<_> = disk.list(None).try_collect().await.expect("list");
            let compacted: Vec<_> = objects
                .iter()
                .filter(|object| object.location.as_ref().contains("/compacted/"))
                .collect();
            let segments: std::collections::BTreeSet<_> = compacted
                .iter()
                .filter_map(|object| object.location.as_ref().split("/compacted/").next())
                .collect();
            println!(
                "storage: compacted_ssts={} segments={} compacted_mib={:.1}",
                compacted.len(),
                segments.len(),
                compacted.iter().map(|object| object.size).sum::<u64>() as f64 / (1 << 20) as f64,
            );
            for object in objects {
                let bytes = disk
                    .get(&object.location)
                    .await
                    .expect("get")
                    .bytes()
                    .await
                    .expect("bytes");
                memory
                    .put(&object.location, bytes.into())
                    .await
                    .expect("put");
            }
            Arc::new(memory)
        };
        let throttle = ThrottleConfig {
            wait_get_per_call: args.latency,
            wait_get_per_byte: Duration::from_secs(1)
                / (args.mib_per_sec.max(1) as u32 * 1024 * 1024),
            wait_list_per_call: args.latency,
            ..ThrottleConfig::default()
        };
        let at = |ms: i64| UNIX_EPOCH + Duration::from_millis(ms as u64);
        let end_ms = T0_MS + args.hours * HOUR_MS;
        let range = || -> std::ops::RangeInclusive<SystemTime> {
            at(end_ms - args.hours * HOUR_MS)..=at(end_ms)
        };
        let namespace = Namespace::default();
        for (name, query) in QUERIES
            .iter()
            .filter(|(name, _)| args.query.as_deref().is_none_or(|q| q == *name))
        {
            let store: Arc<dyn slatedb::object_store::ObjectStore> =
                Arc::new(ThrottledStore::new(local.clone(), throttle));
            let reader = open_reader_over(&storage, store, 256 << 20).await;
            for run in ["cold", "warm"] {
                let before = requests::snapshot();
                let started = Instant::now();
                let (result, trace) =
                    reader_query_range_traced(&reader, &namespace, query, range(), args.step).await;
                println!(
                    "{run:<4} {name:<18} {:>9.1} ms  series={:<5} requests: {}",
                    ms(started.elapsed()),
                    result.len(),
                    requests::since(&before),
                );
                if args.trace {
                    print_trace(&trace);
                }
            }
            if args.repeat > 0 {
                println!("BEGIN {name} pid={}", std::process::id());
                let started = Instant::now();
                for _ in 0..args.repeat {
                    reader_query_range_traced(&reader, &namespace, query, range(), args.step).await;
                }
                println!(
                    "END {name} warm_mean_ms={:.1}",
                    ms(started.elapsed()) / args.repeat as f64
                );
            }
        }
    });
}
