//! Cold 1-day range queries over a day of cAdvisor and kube-state-metrics
//! shaped data, read through an object store that sleeps before every read
//! as S3's first-byte latency does.
//!
//! `PROFILE_STORE_DIR` (default `/tmp/metrics-coldpath/store`) is ingested
//! once and reused. `PROFILE_LATENCY_MS` (comma-separated, default `20`),
//! `PROFILE_RUNS` (default 5), `PROFILE_QUERIES` (indexes into [`QUERIES`]),
//! `PROFILE_PROJECTION` (`on`, `off` or `both`) and `PROFILE_WARM` pick the
//! runs; `PROFILE_OUT` receives one JSON line per run. `PROFILE_LAYOUT`
//! (see [`write_layout`]) picks how a new store is written, and
//! `PROFILE_VARIANTS` read tunings and reader starts (see [`tuning`] and
//! [`Start`]).

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use async_trait::async_trait;
use common::storage::config::{
    BlockCacheConfig, FoyerMemoryCacheConfig, LocalObjectStoreConfig, ObjectStoreConfig,
    SlateDbStorageConfig,
};
use sharding::{ShardId, ShardingOptions};
use slatedb::object_store::{self, ObjectStore, path::Path as ObjectPath};

use crate::model::{Label, Sample, Series, TimeBucket};
use crate::read_tuning::{ReadTuning, WriteLayout};
use crate::sharded::ShardedMetrics;
use crate::storage::StorageReader;
use crate::{Config, Namespace, RangeSample, TimeSeriesDb, Visibility};

mod memory;

const STORE_DIR: &str = "/tmp/metrics-coldpath/store";
const BASE_PATH: &str = "metrics";
const SCRAPE_MS: i64 = 30_000;
const ROUND_MS: i64 = 60_000;
const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 24 * HOUR_MS;
/// Console's step for a one-day range: the candidate nearest 240 points.
const STEP: Duration = Duration::from_secs(300);
/// Series per write call, as a remote-write shard batches them.
const WRITE_BATCH: usize = 5_000;
/// The metrics server's default `reader_cache_capacity`.
const READER_CACHE_CAPACITY: u64 = 256 << 20;

pub(super) const QUERIES: &[(&str, &str)] = &[
    (
        "cpu",
        r#"sum by (namespace,pod) (rate(container_cpu_usage_seconds_total{container!="",pod!=""}[5m]))"#,
    ),
    (
        "memory",
        r#"sum by (namespace,pod) (container_memory_working_set_bytes{container!="",pod!=""})"#,
    ),
    (
        "network",
        "sum by (namespace) (rate(container_network_receive_bytes_total[5m]))",
    ),
    (
        "ratio",
        r#"sum by (namespace,pod)(rate(container_cpu_usage_seconds_total{container!=""}[5m])) / sum by (namespace,pod)(kube_pod_container_resource_requests{resource="cpu"})"#,
    ),
];

fn env<T: std::str::FromStr<Err: std::fmt::Debug>>(name: &str) -> Option<T> {
    std::env::var(name).ok().map(|value| value.parse().unwrap())
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn namespace() -> Namespace {
    Namespace::new("kube").unwrap()
}

// ── Data ─────────────────────────────────────────────────────────────

const CADVISOR_CONTAINER: &[&str] = &[
    "container_cpu_usage_seconds_total",
    "container_memory_working_set_bytes",
    "container_memory_rss",
    "container_memory_cache",
    "container_fs_reads_bytes_total",
    "container_fs_writes_bytes_total",
    "container_cpu_cfs_throttled_seconds_total",
    "container_spec_memory_limit_bytes",
];
const CADVISOR_POD: &[&str] = &[
    "container_network_receive_bytes_total",
    "container_network_transmit_bytes_total",
];
const CONTAINERS: &[&str] = &["app", "istio-proxy", "POD", ""];
const KSM_CONTAINERS: &[&str] = &["app", "istio-proxy"];
const PHASES: &[&str] = &["Pending", "Running", "Succeeded", "Failed", "Unknown"];
const PODS_PER_NAMESPACE: usize = 25;
const NODES: usize = 100;
/// One pod in this many restarts, under a new name, every [`CHURN_MS`].
const CHURN_EVERY: usize = 5;
const CHURN_MS: i64 = 6 * HOUR_MS;

fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

/// One series' identity and how its value moves.
struct Template {
    labels: Vec<Label>,
    /// Hash of the labels: remote-write shards series by it, so it orders
    /// the first samples of a bucket and hence its series IDs.
    order: u64,
    counter: bool,
    rate: f64,
}

/// The series of one pod incarnation.
fn pod_series(ns: usize, pod: usize, generation: i64) -> Vec<Template> {
    let namespace = format!("namespace-{ns:02}");
    let hash = mix(((ns * PODS_PER_NAMESPACE + pod) as u64) << 20 | generation as u64);
    let pod_name = format!(
        "deployment-{pod:02}-{:010x}-{:05x}",
        hash & 0xff_ffff_ffff,
        hash >> 44
    );
    let uid = format!("{:032x}", u128::from(hash) * 0x9e37_79b9_7f4a_7c15);
    let node = format!(
        "ip-10-0-{}-{}.ec2.internal",
        (hash as usize + pod) % NODES / 10,
        (hash as usize + pod) % NODES
    );
    let instance = format!("10.0.{}.{}:10250", (hash as usize + pod) % NODES / 10, pod);
    let mut out = Vec::new();
    let mut push = |name: &str, labels: Vec<(&str, String)>, counter: bool, rate: f64| {
        let mut labels: Vec<Label> = labels
            .into_iter()
            .filter(|(_, value)| !value.is_empty())
            .map(|(name, value)| Label::new(name, value))
            .collect();
        labels.push(Label::metric_name(name));
        labels.sort();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        labels.hash(&mut hasher);
        out.push(Template {
            labels,
            order: hasher.finish(),
            counter,
            rate,
        });
    };
    let cadvisor = |container: &str| {
        vec![
            ("namespace", namespace.clone()),
            ("pod", pod_name.clone()),
            ("container", container.to_string()),
            (
                "id",
                format!(
                    "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod{uid}.slice/cri-containerd-{container}.scope"
                ),
            ),
            (
                "image",
                if container.is_empty() {
                    String::new()
                } else {
                    format!("registry.example.com/{namespace}/{container}:v1.2.3")
                },
            ),
            (
                "name",
                format!("{:064x}", u128::from(mix(hash ^ container.len() as u64))),
            ),
            ("endpoint", "https-metrics".to_string()),
            ("instance", instance.clone()),
            ("job", "kubelet".to_string()),
            ("metrics_path", "/metrics/cadvisor".to_string()),
            ("node", node.clone()),
            ("service", "kube-prometheus-stack-kubelet".to_string()),
        ]
    };
    for (index, name) in CADVISOR_CONTAINER.iter().enumerate() {
        for container in CONTAINERS {
            let mut labels = cadvisor(container);
            if index == 0 {
                labels.push(("cpu", "total".to_string()));
            }
            let counter = name.ends_with("_total");
            push(name, labels, counter, 0.05 + (hash % 100) as f64 / 100.0);
        }
    }
    for name in CADVISOR_POD {
        let mut labels = cadvisor("");
        labels.push(("interface", "eth0".to_string()));
        push(name, labels, true, 1_000.0 + (hash % 5_000) as f64);
    }
    let ksm = |container: &str| {
        vec![
            ("namespace", namespace.clone()),
            ("pod", pod_name.clone()),
            ("container", container.to_string()),
            ("uid", uid.clone()),
            ("node", node.clone()),
            ("endpoint", "http".to_string()),
            ("instance", "10.0.200.7:8080".to_string()),
            ("job", "kube-state-metrics".to_string()),
            (
                "service",
                "kube-prometheus-stack-kube-state-metrics".to_string(),
            ),
        ]
    };
    for name in [
        "kube_pod_container_resource_requests",
        "kube_pod_container_resource_limits",
    ] {
        for container in KSM_CONTAINERS {
            for (resource, unit) in [("cpu", "core"), ("memory", "byte")] {
                let mut labels = ksm(container);
                labels.push(("resource", resource.to_string()));
                labels.push(("unit", unit.to_string()));
                push(name, labels, false, 0.0);
            }
        }
    }
    let mut info = ksm("");
    info.push(("host_ip", instance.clone()));
    info.push(("created_by_kind", "ReplicaSet".to_string()));
    push("kube_pod_info", info, false, 0.0);
    for phase in PHASES {
        let mut labels = ksm("");
        labels.push(("phase", phase.to_string()));
        push("kube_pod_status_phase", labels, false, 0.0);
    }
    out
}

/// Which incarnation of `(ns, pod)` runs at `t`, and since when.
fn incarnation(ns: usize, pod: usize, t: i64) -> (i64, i64) {
    if !pod.is_multiple_of(CHURN_EVERY) {
        return (0, i64::MIN);
    }
    let phase = ((ns * 7 + pod) % 6) as i64 * HOUR_MS;
    let generation = (t - phase).div_euclid(CHURN_MS);
    (generation + 1, generation * CHURN_MS + phase)
}

struct Generator {
    namespaces: usize,
    pods: HashMap<(usize, usize, i64), Arc<Vec<Template>>>,
}

impl Generator {
    /// Every series' samples in `[t, t + ROUND_MS)`, in remote-write order.
    fn round(&mut self, t: i64) -> Vec<Series> {
        let mut live = Vec::new();
        for ns in 0..self.namespaces {
            for pod in 0..PODS_PER_NAMESPACE {
                let (generation, since) = incarnation(ns, pod, t);
                let templates = self
                    .pods
                    .entry((ns, pod, generation))
                    .or_insert_with(|| Arc::new(pod_series(ns, pod, generation)))
                    .clone();
                live.push((templates, since.max(0)));
            }
        }
        self.pods
            .retain(|&(ns, pod, generation), _| incarnation(ns, pod, t).0 == generation);
        let mut series: Vec<(u64, Series)> = Vec::new();
        for (templates, since) in &live {
            for template in templates.iter() {
                let samples = (0..ROUND_MS / SCRAPE_MS)
                    .map(|scrape| {
                        let at = t + scrape * SCRAPE_MS;
                        let value = if template.counter {
                            template.rate * ((at - since) / 1_000) as f64
                        } else {
                            let wave = ((at / SCRAPE_MS) as f64 / 40.0).sin();
                            1e6 * (1.0 + template.rate) * (2.0 + wave)
                        };
                        Sample::new(at, value)
                    })
                    .collect();
                series.push((
                    template.order,
                    Series {
                        labels: template.labels.clone(),
                        unit: None,
                        metric_type: None,
                        description: None,
                        samples,
                        histograms: Vec::new(),
                    },
                ));
            }
        }
        series.sort_unstable_by_key(|(order, _)| *order);
        series.into_iter().map(|(_, series)| series).collect()
    }
}

// ── Store ────────────────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize)]
struct Meta {
    /// Exclusive end of the ingested data, and of the queries' range.
    end_ms: i64,
    namespaces: usize,
    ingest_ms: f64,
    /// The [`write_layout`] switches the store was written with.
    #[serde(default)]
    layout: String,
}

/// `arrival`, `sorted` (series IDs only) or `default` for
/// [`WriteLayout::DEFAULT`].
fn write_layout(name: &str) -> WriteLayout {
    match name {
        "arrival" => WriteLayout {
            sorted_series_ids: false,
            rollup_presort: false,
        },
        "sorted" => WriteLayout {
            sorted_series_ids: true,
            rollup_presort: false,
        },
        "default" => WriteLayout::DEFAULT,
        other => panic!("unknown layout {other}"),
    }
}

fn storage_config(dir: &Path, path: String) -> SlateDbStorageConfig {
    SlateDbStorageConfig {
        path,
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: dir.display().to_string(),
        }),
        settings_path: std::env::var("PROFILE_SLATEDB_SETTINGS").ok(),
        // The metrics example config unless overridden: 512 MiB of blocks,
        // 128 MiB of metadata.
        block_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
            capacity: env("PROFILE_BLOCK_CACHE_BYTES").unwrap_or(512 << 20),
            shards: None,
        })),
        meta_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
            capacity: env("PROFILE_META_CACHE_BYTES").unwrap_or(128 << 20),
            shards: None,
        })),
        disk: Default::default(),
    }
}

/// Yesterday 14:20 UTC: a day rollup, three 4h windows, and the hours of an
/// open window behind it.
fn default_end_ms() -> i64 {
    let now = common::time::now_ms();
    now - now.rem_euclid(DAY_MS) - DAY_MS + 14 * HOUR_MS + 20 * 60_000
}

/// Ingests 25 hours ending at `end_ms` through the write path, flushing
/// each simulated minute and rolling up each simulated hour as the
/// production rollup task would at that time.
async fn ingest(dir: &Path, end_ms: i64, namespaces: usize, layout: String) {
    let _layout = crate::read_tuning::override_write_layout(write_layout(&layout)).await;
    let shard_path = ShardingOptions::shard_path(BASE_PATH, ShardId::new(0));
    let config = Config {
        storage: storage_config(dir, shard_path),
        rollup: crate::RollupConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let rollup = crate::RollupConfig::default();
    let writer = TimeSeriesDb::open(config).await.unwrap();
    let namespace = namespace();
    let start_ms = end_ms - 25 * HOUR_MS;
    let mut generator = Generator {
        namespaces,
        pods: HashMap::new(),
    };
    // Simulated minutes between durable flushes and between memtable
    // flushes to L0.
    let durable_every: i64 = env("PROFILE_DURABLE_EVERY").unwrap_or(1);
    let memtable_every: i64 = env("PROFILE_MEMTABLE_EVERY").unwrap_or(15);
    let started = Instant::now();
    let mut spent = [Duration::ZERO; 5];
    let mut samples = 0usize;
    let mut t = start_ms;
    while t < end_ms {
        let mut step = Instant::now();
        let mut lap = |slot: usize, step: &mut Instant| {
            spent[slot] += step.elapsed();
            *step = Instant::now();
        };
        let round = generator.round(t);
        samples += round
            .iter()
            .map(|series| series.samples.len())
            .sum::<usize>();
        lap(0, &mut step);
        let mut round = round.into_iter().peekable();
        while round.peek().is_some() {
            let batch: Vec<Series> = round.by_ref().take(WRITE_BATCH).collect();
            writer
                .write_with_visibility(&namespace, batch, Visibility::Applied)
                .await
                .unwrap();
        }
        lap(1, &mut step);
        let minute = (t - start_ms) / ROUND_MS + 1;
        if minute % durable_every == 0 {
            writer.flush().await.unwrap();
        }
        lap(2, &mut step);
        if minute % memtable_every == 0 {
            writer.storage().flush_memtable().await.unwrap();
        }
        lap(3, &mut step);
        t += ROUND_MS;
        if t % HOUR_MS == 0 {
            let sealed = writer.rollup_now(&rollup, t).await.unwrap();
            lap(4, &mut step);
            let (segments, l0, runs, run_ssts) = writer.storage().layout();
            eprintln!(
                "ingested to {:>5.1}h: {:>6.1}s (generate {:.0}s write {:.0}s flush {:.0}s l0 {:.0}s rollup {:.0}s), {samples} samples, sealed {sealed}, segments {segments} l0 {l0} runs {runs} run ssts {run_ssts}",
                (t - start_ms) as f64 / HOUR_MS as f64,
                started.elapsed().as_secs_f64(),
                spent[0].as_secs_f64(),
                spent[1].as_secs_f64(),
                spent[2].as_secs_f64(),
                spent[3].as_secs_f64(),
                spent[4].as_secs_f64(),
            );
        }
    }
    // Lets compaction settle the way time would before anyone queries.
    let mut last = writer.storage().layout();
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let layout = writer.storage().layout();
        if layout == last {
            break;
        }
        last = layout;
    }
    eprintln!("final layout (segments, l0, runs, run ssts): {last:?}");
    writer.close().await.unwrap();
    let meta = Meta {
        end_ms,
        namespaces,
        ingest_ms: ms(started.elapsed()),
        layout,
    };
    std::fs::write(dir.join("meta.json"), serde_json::to_vec(&meta).unwrap()).unwrap();
}

// ── Latency ──────────────────────────────────────────────────────────

/// One object-store read: path, bytes, and `[start, end)` since the
/// query began.
#[derive(Clone, Debug)]
struct Get {
    path: String,
    bytes: u64,
    start: Duration,
    end: Duration,
}

static RECORDING: AtomicBool = AtomicBool::new(false);
static GETS: Mutex<Option<(Instant, Vec<Get>)>> = Mutex::new(None);

fn start_recording() {
    *GETS.lock().unwrap() = Some((Instant::now(), Vec::new()));
    RECORDING.store(true, Ordering::SeqCst);
}

fn stop_recording() -> Vec<Get> {
    RECORDING.store(false, Ordering::SeqCst);
    GETS.lock()
        .unwrap()
        .take()
        .map(|(_, gets)| gets)
        .unwrap_or_default()
}

#[derive(Debug)]
struct SlowStore {
    inner: Arc<dyn ObjectStore>,
    latency: Duration,
}

impl std::fmt::Display for SlowStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SlowStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let began = Instant::now();
        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }
        let result = self.inner.get_opts(location, options).await;
        if RECORDING.load(Ordering::Relaxed)
            && let Some((query_start, gets)) = GETS.lock().unwrap().as_mut()
        {
            let bytes = result
                .as_ref()
                .map_or(0, |result| result.range.end - result.range.start);
            gets.push(Get {
                path: location.to_string(),
                bytes,
                start: began.saturating_duration_since(*query_start),
                end: query_start.elapsed(),
            });
        }
        result
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<object_store::ListResult> {
        tokio::time::sleep(self.latency).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A reader process with empty caches whose object store sleeps `latency`
/// per read, wrapped the way production wraps S3.
async fn cold_reader(dir: &Path, latency: Duration, result_cache: bool) -> ShardedMetrics {
    reader(dir, latency, result_cache, None).await
}

/// A [`cold_reader`] whose SST reads go through SlateDB's on-disk object
/// store cache at `disk_cache`, kept across readers as a persistent volume
/// keeps it across restarts.
async fn reader(
    dir: &Path,
    latency: Duration,
    result_cache: bool,
    disk_cache: Option<&Path>,
) -> ShardedMetrics {
    let mut options = slatedb::config::DbReaderOptions::default();
    options.object_store_cache_options.root_folder = disk_cache.map(Path::to_path_buf);
    if let Some(part) = env("PROFILE_DISK_PART_BYTES") {
        options.object_store_cache_options.part_size_bytes = part;
    }
    let mut config = Config {
        storage: storage_config(dir, BASE_PATH.to_string()),
        ..Default::default()
    };
    config.query_cache.result_cache_enabled = result_cache;
    let inner = common::create_object_store(&config.storage.object_store).unwrap();
    let slow: Arc<dyn ObjectStore> = Arc::new(SlowStore { inner, latency });
    let store: Arc<dyn ObjectStore> = Arc::new(
        common::storage::coalescing::CoalescingObjectStore::new(slow, "s3:profile"),
    );
    ShardedMetrics::open_readers_over(
        config,
        ShardingOptions::new(1, sharding::DEFAULT_IO_CONCURRENCY_LIMIT).unwrap(),
        [ShardId::new(0)],
        options,
        READER_CACHE_CAPACITY,
        Some(store),
    )
    .await
    .unwrap()
}

/// Which bucket each SST serves, keyed by the id as it appears in paths.
async fn sst_buckets(dir: &Path) -> HashMap<String, (TimeBucket, bool)> {
    let config = storage_config(dir, ShardingOptions::shard_path(BASE_PATH, ShardId::new(0)));
    let store = common::create_object_store(&config.object_store).unwrap();
    let reader = StorageReader::try_new_with_object_store(
        &config,
        slatedb::config::DbReaderOptions::default(),
        None,
        store,
    )
    .await
    .unwrap();
    let id = |debug: &str| {
        debug
            .trim_start_matches("SsTableId::")
            .trim_start_matches("Compacted(")
            .trim_start_matches("Wal(")
            .trim_end_matches(')')
            .trim_start_matches('0')
            .to_string()
    };
    let mut out = HashMap::new();
    eprintln!("bucket layout (l0 ssts, sorted runs x ssts):");
    let mut layout = reader.bucket_ssts(&namespace());
    layout.sort_by_key(|(bucket, _, _)| (bucket.start, bucket.size));
    for (bucket, l0, runs) in layout {
        eprintln!(
            "  {} {:>2}h: l0 {:>3} runs {:?}",
            bucket_name(bucket),
            bucket.size,
            l0.len(),
            runs.iter().map(Vec::len).collect::<Vec<_>>()
        );
        for sst in &l0 {
            out.insert(id(sst), (bucket, true));
        }
        for sst in runs.iter().flatten() {
            out.insert(id(sst), (bucket, false));
        }
    }
    reader.close().await.unwrap();
    out
}

fn bucket_name(bucket: TimeBucket) -> String {
    let at = UNIX_EPOCH + Duration::from_secs(u64::from(bucket.start) * 60);
    let secs = at.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let day = secs.div_euclid(86_400);
    let hour = secs.rem_euclid(86_400) / 3_600;
    format!(
        "d{:+}+{hour:02}h",
        day - common::time::now_secs().div_euclid(86_400)
    )
}

// ── Runs ─────────────────────────────────────────────────────────────

/// How a cold run's reader starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Start {
    /// With empty caches.
    Cold,
    /// After warming the filters and indexes of the queried day in
    /// process, as the cache warmer's metadata-only pass does.
    MetaWarm,
    /// Over a disk cache a previous reader's metadata warm filled.
    DiskMeta,
    /// Over a disk cache a previous reader running the same query filled.
    DiskWarm,
}

fn start_switch(switch: &str) -> Option<Start> {
    match switch {
        "metawarm" => Some(Start::MetaWarm),
        "diskmeta" => Some(Start::DiskMeta),
        "diskwarm" => Some(Start::DiskWarm),
        _ => None,
    }
}

fn start(variant: &str) -> Start {
    variant
        .split('+')
        .find_map(start_switch)
        .unwrap_or(Start::Cold)
}

/// A `+`-joined set of switches over the all-off baseline (`base`) or
/// [`ReadTuning::DEFAULT`] (`default`), plus at most one [`Start`].
fn tuning(variant: &str) -> ReadTuning {
    let switches: Vec<&str> = variant
        .split('+')
        .filter(|switch| start_switch(switch).is_none())
        .collect();
    if switches == ["default"] {
        return ReadTuning::DEFAULT;
    }
    let mut tuning = ReadTuning {
        cluster_scans: false,
        point_get_concurrency: 64,
        chunk_prefetch: 0,
        scan_watermark: false,
        unproject_dense: false,
    };
    for switch in switches.into_iter().filter(|switch| *switch != "base") {
        match switch {
            "clusters" => tuning.cluster_scans = true,
            "watermark" => tuning.scan_watermark = true,
            "dense" => tuning.unproject_dense = true,
            concurrency if concurrency.starts_with("conc") => {
                tuning.point_get_concurrency = concurrency["conc".len()..].parse().unwrap();
            }
            depth if depth.starts_with("prefetch") => {
                tuning.chunk_prefetch = depth["prefetch".len()..].parse().unwrap();
            }
            other => panic!("unknown switch {other}"),
        }
    }
    tuning
}

fn day_range(end_ms: i64) -> std::ops::RangeInclusive<std::time::SystemTime> {
    let end = UNIX_EPOCH + Duration::from_millis(end_ms as u64);
    end - Duration::from_millis(DAY_MS as u64)..=end
}

/// Warms the filters and indexes of every bucket from an hour before the
/// queried day to now.
async fn warm_metadata(db: &ShardedMetrics, end_ms: i64) {
    let since = common::time::now_ms() - (end_ms - DAY_MS - HOUR_MS);
    db.warm_recent(
        &[namespace()],
        common::WarmWindow {
            metadata: Some(Duration::from_millis(since as u64)),
            payloads: None,
        },
        16,
        &tokio_util::sync::CancellationToken::new(),
        &common::storage::slate::warm::WarmLedger::new(true),
        common::storage::slate::warm::WarmShares {
            metadata_percent: 100,
            ..Default::default()
        },
    )
    .await
    .unwrap();
}

/// A reader started as `start` says, and the milliseconds its warming took
/// (in a previous reader, for the disk starts).
async fn started_reader(
    dir: &Path,
    latency: Duration,
    start: Start,
    text: &str,
    end_ms: i64,
    projection: bool,
) -> (ShardedMetrics, f64) {
    let disk_cache = dir.with_extension("diskcache");
    let started = Instant::now();
    match start {
        Start::Cold => (cold_reader(dir, latency, true).await, 0.0),
        Start::MetaWarm => {
            let db = cold_reader(dir, latency, true).await;
            warm_metadata(&db, end_ms).await;
            (db, ms(started.elapsed()))
        }
        Start::DiskMeta | Start::DiskWarm => {
            let _ = std::fs::remove_dir_all(&disk_cache);
            std::fs::create_dir_all(&disk_cache).unwrap();
            let previous = reader(dir, latency, false, Some(&disk_cache))
                .await
                .with_label_projection(projection);
            if start == Start::DiskMeta {
                warm_metadata(&previous, end_ms).await;
            } else {
                previous
                    .query_range_with_trace(&namespace(), text, day_range(end_ms), STEP, false)
                    .await
                    .unwrap();
            }
            previous.close().await.unwrap();
            let warm_ms = ms(started.elapsed());
            (reader(dir, latency, true, Some(&disk_cache)).await, warm_ms)
        }
    }
}

#[derive(Clone, Debug, Default, serde::Serialize)]
struct Run {
    variant: String,
    query: &'static str,
    projection: bool,
    latency_ms: u64,
    cold: bool,
    total_ms: f64,
    /// Spent warming before the query, outside `total_ms`.
    warm_ms: f64,
    reader_setup_ms: f64,
    build_physical_ms: f64,
    execute_ms: f64,
    gets: usize,
    get_bytes: u64,
    plan_gets: usize,
    exec_gets: usize,
    /// GETs in flight at once, at most.
    peak_gets: usize,
    series: usize,
    samples: usize,
    checksum: f64,
    /// `(kind, calls, bytes, wall_ms)` from the trace.
    io: Vec<(String, u64, u64, f64)>,
    subphases: Vec<(String, f64, u64)>,
    /// `(bucket, resolve_ms, series, sample_ms, batches, gets, l0 gets)`.
    buckets: Vec<(String, f64, u64, f64, u64, usize, usize)>,
}

fn peak(gets: &[Get]) -> usize {
    let mut edges: Vec<(Duration, i32)> = gets
        .iter()
        .flat_map(|get| [(get.start, 1), (get.end, -1)])
        .collect();
    edges.sort();
    let mut live = 0;
    let mut most = 0;
    for (_, delta) in edges {
        live += delta;
        most = most.max(live);
    }
    most as usize
}

fn checksum(result: &[RangeSample]) -> (usize, f64) {
    let samples = result.iter().map(|series| series.samples.len()).sum();
    let sum = result
        .iter()
        .flat_map(|series| &series.samples)
        .map(|(_, value)| if value.is_finite() { *value } else { 0.0 })
        .sum();
    (samples, sum)
}

#[allow(clippy::too_many_arguments)]
async fn run_once(
    db: &ShardedMetrics,
    query: &'static str,
    text: &str,
    end_ms: i64,
    projection: bool,
    latency_ms: u64,
    cold: bool,
    ssts: &HashMap<String, (TimeBucket, bool)>,
) -> Run {
    let range = day_range(end_ms);
    start_recording();
    let started = Instant::now();
    let (result, trace) = db
        .query_range_with_trace(&namespace(), text, range, STEP, true)
        .await
        .unwrap();
    let total = started.elapsed();
    let gets = stop_recording();
    let trace = trace.unwrap();
    let phase = |name: &str| {
        trace["phases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|phase| phase["name"] == name)
            .map_or(0.0, |phase| phase["elapsedMs"].as_f64().unwrap())
    };
    let execute = phase("execute") + phase("reshape");
    let exec_start = total.saturating_sub(Duration::from_secs_f64(execute / 1000.0));
    let exec_gets = gets.iter().filter(|get| get.start >= exec_start).count();
    let id_of = |path: &str| {
        path.rsplit('/')
            .next()
            .unwrap_or_default()
            .trim_end_matches(".sst")
            .trim_start_matches('0')
            .to_string()
    };
    if std::env::var("PROFILE_TIMELINE").is_ok() {
        print_timeline(&gets, ssts, latency_ms.max(1));
    }
    let mut bucket_gets: HashMap<TimeBucket, (usize, usize)> = HashMap::new();
    for get in &gets {
        if let Some(&(bucket, l0)) = ssts.get(&id_of(&get.path)) {
            let slot = bucket_gets.entry(bucket).or_default();
            slot.0 += 1;
            slot.1 += usize::from(l0);
        }
    }
    let buckets = trace["buckets"]
        .as_array()
        .map(|buckets| {
            buckets
                .iter()
                .map(|bucket| {
                    let tb = TimeBucket {
                        start: bucket["startMinute"].as_u64().unwrap() as u32,
                        size: bucket["sizeHours"].as_u64().unwrap() as u8,
                    };
                    let (gets, l0) = bucket_gets.get(&tb).copied().unwrap_or_default();
                    (
                        format!("{} {:>2}h", bucket_name(tb), tb.size),
                        bucket["resolveMs"].as_f64().unwrap(),
                        bucket["series"].as_u64().unwrap(),
                        bucket["sampleMs"].as_f64().unwrap(),
                        bucket["sampleBatches"].as_u64().unwrap(),
                        gets,
                        l0,
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let io = trace["io"]
        .as_array()
        .map(|io| {
            io.iter()
                .map(|stats| {
                    (
                        stats["kind"].as_str().unwrap().to_string(),
                        stats["callCount"].as_u64().unwrap(),
                        stats["bytes"].as_u64().unwrap(),
                        stats["wallMs"].as_f64().unwrap(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let subphases = trace["subphases"]
        .as_array()
        .map(|subphases| {
            subphases
                .iter()
                .map(|stats| {
                    (
                        stats["label"].as_str().unwrap().to_string(),
                        stats["elapsedMs"].as_f64().unwrap(),
                        stats["callCount"].as_u64().unwrap(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let (samples, sum) = checksum(&result);
    Run {
        variant: String::new(),
        query,
        projection,
        latency_ms,
        cold,
        total_ms: ms(total),
        warm_ms: 0.0,
        reader_setup_ms: phase("reader_setup"),
        build_physical_ms: phase("build_physical"),
        execute_ms: execute,
        gets: gets.len(),
        get_bytes: gets.iter().map(|get| get.bytes).sum(),
        plan_gets: gets.len() - exec_gets,
        exec_gets,
        peak_gets: peak(&gets),
        series: result.len(),
        samples,
        checksum: sum,
        io,
        subphases,
        buckets,
    }
}

/// Each bucket's GETs as `slot:count/KB` per `slot_ms` window of start
/// time, so serial chains show as long runs of small slots.
fn print_timeline(gets: &[Get], ssts: &HashMap<String, (TimeBucket, bool)>, slot_ms: u64) {
    let mut by_bucket: BTreeMap<String, BTreeMap<u64, (usize, u64)>> = BTreeMap::new();
    for get in gets {
        let file = get.path.rsplit('/').next().unwrap_or_default();
        let id = file.trim_end_matches(".sst").trim_start_matches('0');
        let name = match ssts.get(id) {
            Some(&(bucket, l0)) => format!(
                "{} {:>2}h{}",
                bucket_name(bucket),
                bucket.size,
                if l0 { " l0" } else { "" }
            ),
            None => format!("other {}", get.path.split('/').rev().nth(1).unwrap_or("")),
        };
        let slot = get.start.as_millis() as u64 / slot_ms;
        let entry = by_bucket.entry(name).or_default().entry(slot).or_default();
        entry.0 += 1;
        entry.1 += get.bytes;
    }
    for (name, slots) in by_bucket {
        let line: Vec<String> = slots
            .iter()
            .map(|(slot, (count, bytes))| format!("{slot}:{count}/{}", bytes / 1024))
            .collect();
        eprintln!("    timeline {name}: {}", line.join(" "));
    }
}

fn stats(values: impl IntoIterator<Item = f64>) -> String {
    let mut values: Vec<f64> = values.into_iter().collect();
    values.sort_by(f64::total_cmp);
    if values.is_empty() {
        return "-".to_string();
    }
    format!(
        "{:.0} [{:.0}-{:.0}]",
        values[values.len() / 2],
        values[0],
        values[values.len() - 1]
    )
}

fn summarize(runs: &[Run]) {
    let first = &runs[0];
    eprintln!(
        "{:<8} proj={:<5} lat={:>2}ms {}: total {} | prewarm {} setup {} plan {} exec {} | gets {} (plan {}, exec {}, peak {}) {:.1} MB | {} series {} samples",
        first.query,
        first.projection,
        first.latency_ms,
        if first.cold { "cold" } else { "warm" },
        stats(runs.iter().map(|run| run.total_ms)),
        stats(runs.iter().map(|run| run.warm_ms)),
        stats(runs.iter().map(|run| run.reader_setup_ms)),
        stats(runs.iter().map(|run| run.build_physical_ms)),
        stats(runs.iter().map(|run| run.execute_ms)),
        stats(runs.iter().map(|run| run.gets as f64)),
        stats(runs.iter().map(|run| run.plan_gets as f64)),
        stats(runs.iter().map(|run| run.exec_gets as f64)),
        stats(runs.iter().map(|run| run.peak_gets as f64)),
        runs.iter().map(|run| run.get_bytes).sum::<u64>() as f64 / runs.len() as f64 / 1e6,
        first.series,
        first.samples,
    );
    let mut sorted: Vec<&Run> = runs.iter().collect();
    sorted.sort_by(|a, b| a.total_ms.total_cmp(&b.total_ms));
    let median = sorted[sorted.len() / 2];
    for (kind, calls, bytes, wall) in &median.io {
        eprintln!("    io {kind:<26} calls {calls:>7} bytes {bytes:>10} wall {wall:>7.1}ms");
    }
    for (label, elapsed, calls) in &median.subphases {
        eprintln!("    sub {label:<25} {elapsed:>7.1}ms x{calls}");
    }
    if std::env::var("PROFILE_BUCKETS").is_ok() {
        for (name, resolve, series, sample, batches, gets, l0) in &median.buckets {
            eprintln!(
                "    bucket {name}: resolve {resolve:>6.1}ms series {series:>6} samples {sample:>7.1}ms x{batches:<3} gets {gets:>4} (l0 {l0})"
            );
        }
    }
}

/// Times [`QUERIES`] cold (each run on a fresh reader) and warm over the
/// reused store; see the module docs for the knobs. `cargo test -p
/// plural-metrics --release --all-features --lib profile_cold_queries --
/// --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_cold_queries() {
    let dir = PathBuf::from(std::env::var("PROFILE_STORE_DIR").unwrap_or(STORE_DIR.to_string()));
    let meta_path = dir.join("meta.json");
    if !meta_path.exists() {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let namespaces = env("PROFILE_NAMESPACES").unwrap_or(40);
        let layout = std::env::var("PROFILE_LAYOUT").unwrap_or("default".to_string());
        ingest(&dir, default_end_ms(), namespaces, layout).await;
    }
    let meta: Meta = serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
    eprintln!(
        "store {} ({} namespaces, {} layout, ingested in {:.0}s)",
        dir.display(),
        meta.namespaces,
        meta.layout,
        meta.ingest_ms / 1000.0
    );
    if std::env::var("PROFILE_INGEST_ONLY").is_ok() {
        return;
    }
    let ssts = sst_buckets(&dir).await;

    let latencies: Vec<u64> = std::env::var("PROFILE_LATENCY_MS")
        .unwrap_or("20".to_string())
        .split(',')
        .map(|value| value.trim().parse().unwrap())
        .collect();
    let runs: usize = env("PROFILE_RUNS").unwrap_or(5);
    let queries: Vec<usize> = std::env::var("PROFILE_QUERIES").map_or_else(
        |_| (0..QUERIES.len()).collect(),
        |value| {
            value
                .split(',')
                .map(|index| index.parse().unwrap())
                .collect()
        },
    );
    let projections: &[bool] = match std::env::var("PROFILE_PROJECTION").as_deref() {
        Ok("on") => &[true],
        Ok("off") => &[false],
        _ => &[false, true],
    };
    let mut out = std::env::var("PROFILE_OUT").ok().map(|path| {
        std::io::BufWriter::new(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap(),
        )
    });
    let tag = std::env::var("PROFILE_TAG").unwrap_or_default();
    let variants: Vec<String> = std::env::var("PROFILE_VARIANTS")
        .unwrap_or("default".to_string())
        .split(',')
        .map(str::to_string)
        .collect();
    let mut answers: HashMap<&str, (usize, usize, f64)> = HashMap::new();
    for (latency_ms, variant) in latencies
        .iter()
        .flat_map(|&latency| variants.iter().map(move |variant| (latency, variant)))
    {
        let latency = Duration::from_millis(latency_ms);
        let _tuning = crate::read_tuning::override_read_tuning(tuning(variant)).await;
        for &index in &queries {
            let (name, text) = QUERIES[index];
            for &projection in projections {
                let mut cold = Vec::new();
                for _ in 0..runs {
                    let (db, warm_ms) = started_reader(
                        &dir,
                        latency,
                        start(variant),
                        text,
                        meta.end_ms,
                        projection,
                    )
                    .await;
                    let db = db.with_label_projection(projection);
                    let mut run = run_once(
                        &db,
                        name,
                        text,
                        meta.end_ms,
                        projection,
                        latency_ms,
                        true,
                        &ssts,
                    )
                    .await;
                    run.warm_ms = warm_ms;
                    db.close().await.unwrap();
                    cold.push(run);
                }
                eprint!("[{variant}] ");
                summarize(&cold);
                let mut all = cold.clone();
                if std::env::var("PROFILE_WARM").is_ok() {
                    let db = cold_reader(&dir, latency, false)
                        .await
                        .with_label_projection(projection);
                    run_once(
                        &db,
                        name,
                        text,
                        meta.end_ms,
                        projection,
                        latency_ms,
                        true,
                        &ssts,
                    )
                    .await;
                    let mut warm = Vec::new();
                    for _ in 0..runs {
                        warm.push(
                            run_once(
                                &db,
                                name,
                                text,
                                meta.end_ms,
                                projection,
                                latency_ms,
                                false,
                                &ssts,
                            )
                            .await,
                        );
                    }
                    db.close().await.unwrap();
                    eprint!("[{variant}] ");
                    summarize(&warm);
                    all.extend(warm);
                }
                for run in &mut all {
                    run.variant = variant.clone();
                }
                for run in &all {
                    let answer = (run.series, run.samples, run.checksum);
                    let expected = *answers.entry(name).or_insert(answer);
                    assert_eq!(
                        (answer.0, answer.1),
                        (expected.0, expected.1),
                        "{name}: answers differ"
                    );
                    assert!(
                        (answer.2 - expected.2).abs() <= 1e-9 * expected.2.abs().max(1.0),
                        "{name}: checksums differ: {} vs {}",
                        answer.2,
                        expected.2
                    );
                    if let Some(out) = out.as_mut() {
                        let mut row = serde_json::to_value(run).unwrap();
                        row["tag"] = serde_json::Value::String(tag.clone());
                        writeln!(out, "{row}").unwrap();
                    }
                }
                if let Some(out) = out.as_mut() {
                    out.flush().unwrap();
                }
            }
        }
    }
    let _ = std::fs::remove_dir_all(dir.with_extension("diskcache"));
}

/// Discovery calls over the queried day: `(name, kind, label, matcher)`,
/// where `kind` is `series`, `labels` or `values`.
const DISCOVERY: &[(&str, &str, &str, &str)] = &[
    (
        "series",
        "series",
        "",
        r#"{namespace="namespace-01", __name__=~"container_cpu_usage_seconds_total|container_memory_working_set_bytes"}"#,
    ),
    (
        "labels",
        "labels",
        "",
        r#"{job=~"kubelet|kube-state-metrics"}"#,
    ),
    (
        "values_pod",
        "values",
        "pod",
        "container_cpu_usage_seconds_total",
    ),
    (
        "values_namespace",
        "values",
        "namespace",
        r#"{job=~"kubelet|kube-state-metrics"}"#,
    ),
];

/// The longest chain of GETs where each starts after the previous one
/// ended: a lower bound on dependent round trips.
fn waves(gets: &[Get]) -> usize {
    let mut by_end: Vec<&Get> = gets.iter().collect();
    by_end.sort_by_key(|get| get.end);
    let mut chain: Vec<usize> = Vec::with_capacity(by_end.len());
    for (index, get) in by_end.iter().enumerate() {
        let prior = by_end[..index]
            .iter()
            .zip(&chain)
            .filter(|(earlier, _)| earlier.end <= get.start)
            .map(|(_, length)| *length)
            .max()
            .unwrap_or(0);
        chain.push(prior + 1);
    }
    chain.into_iter().max().unwrap_or(0)
}

/// Times [`DISCOVERY`] calls on fresh readers over the [`profile_cold_queries`]
/// store. `PROFILE_DISCOVERY` picks calls by name (default all),
/// `PROFILE_LATENCY_MS`, `PROFILE_RUNS`, `PROFILE_VARIANTS`, `PROFILE_TIMELINE`
/// and `PROFILE_OUT` as there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_cold_discovery() {
    let dir = PathBuf::from(std::env::var("PROFILE_STORE_DIR").unwrap_or(STORE_DIR.to_string()));
    let meta_path = dir.join("meta.json");
    if !meta_path.exists() {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let namespaces = env("PROFILE_NAMESPACES").unwrap_or(40);
        let layout = std::env::var("PROFILE_LAYOUT").unwrap_or("default".to_string());
        ingest(&dir, default_end_ms(), namespaces, layout).await;
    }
    let meta: Meta = serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
    let ssts = sst_buckets(&dir).await;
    let latency_ms: u64 = env("PROFILE_LATENCY_MS").unwrap_or(20);
    let runs: usize = env("PROFILE_RUNS").unwrap_or(5);
    let picked = std::env::var("PROFILE_DISCOVERY").ok();
    let variants: Vec<String> = std::env::var("PROFILE_VARIANTS")
        .unwrap_or("default".to_string())
        .split(',')
        .map(str::to_string)
        .collect();
    let mut out = std::env::var("PROFILE_OUT").ok().map(|path| {
        std::io::BufWriter::new(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap(),
        )
    });
    let tag = std::env::var("PROFILE_TAG").unwrap_or_default();
    let range = day_range(meta.end_ms);
    let mut answers: HashMap<&str, usize> = HashMap::new();
    for variant in &variants {
        let _tuning = crate::read_tuning::override_read_tuning(tuning(variant)).await;
        for &(name, kind, label, matcher) in DISCOVERY {
            if picked
                .as_deref()
                .is_some_and(|picked| !picked.split(',').any(|item| item == name))
            {
                continue;
            }
            let mut totals = Vec::new();
            for _ in 0..runs {
                let db = cold_reader(&dir, Duration::from_millis(latency_ms), false).await;
                match start(variant) {
                    Start::Cold => {}
                    Start::MetaWarm => warm_metadata(&db, meta.end_ms).await,
                    other => panic!("{other:?} is not supported for discovery"),
                }
                start_recording();
                let started = Instant::now();
                let matchers = [matcher];
                let found = match kind {
                    "series" => db
                        .series(&namespace(), &matchers, range.clone())
                        .await
                        .unwrap()
                        .len(),
                    "labels" => db
                        .labels(&namespace(), Some(&matchers), range.clone())
                        .await
                        .unwrap()
                        .len(),
                    _ => db
                        .label_values(&namespace(), label, Some(&matchers), range.clone())
                        .await
                        .unwrap()
                        .len(),
                };
                let total = started.elapsed();
                let gets = stop_recording();
                let expected = *answers.entry(name).or_insert(found);
                assert_eq!(found, expected, "{name}: answers differ");
                let mut per_bucket: BTreeMap<String, usize> = BTreeMap::new();
                for get in &gets {
                    let file = get.path.rsplit('/').next().unwrap_or_default();
                    let id = file.trim_end_matches(".sst").trim_start_matches('0');
                    let key = match ssts.get(id) {
                        Some(&(bucket, l0)) => format!(
                            "{} {}h{}",
                            bucket_name(bucket),
                            bucket.size,
                            if l0 { " l0" } else { "" }
                        ),
                        None => "other".to_string(),
                    };
                    *per_bucket.entry(key).or_default() += 1;
                }
                eprintln!(
                    "[{variant}] {name:<16} {:>7.1}ms found {found:>6} gets {:>4} {:>8.1}KB waves {:>3} peak {:>3} buckets {}",
                    ms(total),
                    gets.len(),
                    gets.iter().map(|get| get.bytes).sum::<u64>() as f64 / 1024.0,
                    waves(&gets),
                    peak(&gets),
                    per_bucket.len(),
                );
                if std::env::var("PROFILE_TIMELINE").is_ok() {
                    for (bucket, count) in &per_bucket {
                        eprintln!("    bucket {bucket}: {count} gets");
                    }
                    print_timeline(&gets, &ssts, latency_ms.max(1));
                }
                if let Some(out) = out.as_mut() {
                    let row = serde_json::json!({
                        "tag": tag,
                        "variant": variant,
                        "query": name,
                        "total_ms": ms(total),
                        "found": found,
                        "gets": gets.len(),
                        "get_bytes": gets.iter().map(|get| get.bytes).sum::<u64>(),
                        "waves": waves(&gets),
                        "peak": peak(&gets),
                        "buckets": per_bucket,
                    });
                    writeln!(out, "{row}").unwrap();
                    out.flush().unwrap();
                }
                totals.push(ms(total));
                db.close().await.unwrap();
            }
            eprintln!("[{variant}] {name:<16} p50 {}", stats(totals));
        }
    }
}

/// Console's 1d cluster dashboard over the SlateDB store `PROFILE_S3`
/// (`region:bucket:base_path`), read only, each query on a fresh reader with
/// empty caches and the real object store's latency. `PROFILE_CLUSTER` picks
/// the cluster, `PROFILE_NAMESPACE` the metrics namespace (default
/// `default`) and `PROFILE_CONCURRENT` runs the dashboard's queries at once
/// on one fresh reader, as the browser sends them.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "manual profiling"]
async fn profile_remote_cold_dashboard() {
    let target = std::env::var("PROFILE_S3").unwrap();
    let [region, bucket, base] = target.splitn(3, ':').collect::<Vec<_>>()[..] else {
        panic!("PROFILE_S3 is region:bucket:base_path");
    };
    let cluster = std::env::var("PROFILE_CLUSTER").unwrap_or("mgmt".to_string());
    let namespace = Namespace::new(
        std::env::var("PROFILE_NAMESPACE").unwrap_or("default".to_string()),
    )
    .unwrap();
    let queries = [
        ("cpu", format!(r#"sum(rate(container_cpu_usage_seconds_total{{container!="",cluster="{cluster}"}}[5m]))"#)),
        ("mem", format!(r#"sum(container_memory_working_set_bytes{{image!="",cluster="{cluster}",container!=""}})"#)),
        ("ns_cpu", format!(r#"sum(rate(container_cpu_usage_seconds_total{{container!="",cluster="{cluster}"}}[5m])) by (namespace)"#)),
        ("pod_mem", format!(r#"sum(container_memory_working_set_bytes{{cluster="{cluster}",image!="",container!=""}}) by (pod)"#)),
        ("node_cpu", format!(r#"(1 - avg(irate(node_cpu_seconds_total{{mode="idle",cluster="{cluster}"}}[5m])))"#)),
        ("pods", format!(r#"count(kube_pod_info{{cluster="{cluster}"}})"#)),
    ];
    let open = || async {
        let mut config = Config {
            storage: SlateDbStorageConfig {
                object_store: ObjectStoreConfig::Aws(common::storage::config::AwsObjectStoreConfig {
                    region: region.to_string(),
                    bucket: bucket.to_string(),
                    endpoint: None,
                    allow_http: false,
                    virtual_hosted_style: false,
                }),
                ..storage_config(Path::new("/nonexistent"), base.to_string())
            },
            ..Default::default()
        };
        config.query_cache.result_cache_enabled = false;
        let inner = common::create_object_store(&config.storage.object_store).unwrap();
        let slow: Arc<dyn ObjectStore> = Arc::new(SlowStore {
            inner,
            latency: Duration::ZERO,
        });
        let store: Arc<dyn ObjectStore> = Arc::new(
            common::storage::coalescing::CoalescingObjectStore::new(slow, "s3:remote-profile"),
        );
        let started = Instant::now();
        let db = ShardedMetrics::open_readers_over(
            config,
            ShardingOptions::new(1, sharding::DEFAULT_IO_CONCURRENCY_LIMIT).unwrap(),
            [ShardId::new(0)],
            slatedb::config::DbReaderOptions::default(),
            READER_CACHE_CAPACITY,
            Some(store),
        )
        .await
        .unwrap();
        eprintln!("opened reader in {:.0} ms", ms(started.elapsed()));
        db
    };
    let now = common::time::now_ms();
    let end_ms = now - now.rem_euclid(STEP.as_millis() as i64);
    let report = |name: &str, total: Duration, trace: &serde_json::Value, gets: &[Get]| {
        let phases: Vec<String> = trace["phases"]
            .as_array()
            .map(|phases| {
                phases
                    .iter()
                    .map(|phase| format!("{} {:.0}", phase["name"].as_str().unwrap(), phase["elapsedMs"].as_f64().unwrap()))
                    .collect()
            })
            .unwrap_or_default();
        let buckets: Vec<String> = trace["buckets"]
            .as_array()
            .map(|buckets| {
                buckets
                    .iter()
                    .map(|bucket| {
                        format!(
                            "{}h:{}s/{:.0}+{:.0}ms",
                            bucket["sizeHours"],
                            bucket["series"],
                            bucket["resolveMs"].as_f64().unwrap(),
                            bucket["sampleMs"].as_f64().unwrap()
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        eprintln!(
            "{name:>9} {:>7.0} ms gets {:>5} {:>7.1} MiB waves {:>3} peak {:>3} | {} | {}",
            ms(total),
            gets.len(),
            gets.iter().map(|get| get.bytes).sum::<u64>() as f64 / f64::from(1 << 20),
            waves(gets),
            peak(gets),
            phases.join(" "),
            buckets.join(" "),
        );
    };
    if std::env::var("PROFILE_CONCURRENT").is_ok() {
        let db = open().await;
        start_recording();
        let started = Instant::now();
        let results = futures::future::join_all(queries.iter().map(|(name, text)| {
            let db = &db;
            let namespace = &namespace;
            async move {
                let began = Instant::now();
                let (_, trace) = db
                    .query_range_with_trace(namespace, text, day_range(end_ms), STEP, true)
                    .await
                    .unwrap();
                (*name, began.elapsed(), trace.unwrap())
            }
        }))
        .await;
        let total = started.elapsed();
        let gets = stop_recording();
        for (name, took, trace) in &results {
            report(name, *took, trace, &[]);
        }
        report("dashboard", total, &serde_json::Value::Null, &gets);
        db.close().await.unwrap();
        return;
    }
    for (name, text) in &queries {
        let db = open().await;
        // `PROFILE_META_WARM_HOURS` warms that many hours of SST metadata
        // first, as a startup warmer would.
        if let Some(hours) = env::<u64>("PROFILE_META_WARM_HOURS") {
            let started = Instant::now();
            let ledger = common::storage::slate::warm::WarmLedger::new(true);
            db.warm_recent(
                &[namespace.clone()],
                common::WarmWindow {
                    metadata: Some(Duration::from_secs(hours * 3600)),
                    payloads: None,
                },
                16,
                &tokio_util::sync::CancellationToken::new(),
                &ledger,
                common::storage::slate::warm::WarmShares {
                    metadata_percent: 100,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            eprintln!(
                "warmed {hours}h of metadata in {:.0} ms, {:.1} MiB",
                ms(started.elapsed()),
                ledger.bytes() as f64 / f64::from(1 << 20)
            );
        }
        start_recording();
        let started = Instant::now();
        let (_, trace) = db
            .query_range_with_trace(&namespace, text, day_range(end_ms), STEP, true)
            .await
            .unwrap();
        let total = started.elapsed();
        let gets = stop_recording();
        let trace = trace.unwrap();
        report(name, total, &trace, &gets);
        if std::env::var("PROFILE_DETAIL").is_ok() {
            for key in ["subphases", "io"] {
                for item in trace[key].as_array().into_iter().flatten() {
                    eprintln!("    {key} {item}");
                }
            }
            if let Some(object) = trace.as_object() {
                eprintln!("    keys {:?}", object.keys().collect::<Vec<_>>());
            }
            print_waves(&gets);
        }
        db.close().await.unwrap();
        if std::env::var("PROFILE_DETAIL").is_ok() {
            return;
        }
    }
}

/// Each GET of the longest dependent chain: when it started, how long it
/// took, its bytes and object.
fn print_waves(gets: &[Get]) {
    let mut by_end: Vec<&Get> = gets.iter().collect();
    by_end.sort_by_key(|get| get.end);
    let mut chain: Vec<(usize, Option<usize>)> = Vec::with_capacity(by_end.len());
    for (index, get) in by_end.iter().enumerate() {
        let prior = by_end[..index]
            .iter()
            .enumerate()
            .zip(&chain)
            .filter(|((_, earlier), _)| earlier.end <= get.start)
            .max_by_key(|(_, (length, _))| *length)
            .map(|((at, _), (length, _))| (*length, at));
        chain.push(prior.map_or((1, None), |(length, at)| (length + 1, Some(at))));
    }
    let Some(mut at) = (0..chain.len()).max_by_key(|index| chain[*index].0) else {
        return;
    };
    let mut path = vec![at];
    while let Some(previous) = chain[at].1 {
        path.push(previous);
        at = previous;
    }
    for index in path.into_iter().rev() {
        let get = by_end[index];
        eprintln!(
            "    wave start {:>6.0} took {:>4.0} ms {:>8} B {}",
            ms(get.start),
            ms(get.end - get.start),
            get.bytes,
            get.path
        );
    }
}
