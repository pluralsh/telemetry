//! Temporary phase profiler for the TraceQL read path.

use std::time::{Duration, Instant};

use common::storage::config::{
    BlockCacheConfig, FoyerMemoryCacheConfig, LocalObjectStoreConfig, ObjectStoreConfig,
    SlateDbStorageConfig, StorageConfig,
};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue, any_value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};

use super::*;

// The server's allocator, so profiles weigh allocation as it does.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

struct StdRng(u64);

impl StdRng {
    fn seed_from_u64(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn fill(&mut self, bytes: &mut [u8]) {
        for byte in bytes {
            *byte = self.next() as u8;
        }
    }
    fn random_range<T: TryFrom<u64> + Into<u64> + Copy>(&mut self, range: std::ops::Range<T>) -> T
    where
        <T as TryFrom<u64>>::Error: std::fmt::Debug,
    {
        let (low, high) = (range.start.into(), range.end.into());
        T::try_from(low + self.next() % (high - low)).unwrap()
    }
    fn random_bool(&mut self, p: f64) -> bool {
        (self.next() % 1_000_000) as f64 / 1_000_000.0 < p
    }
}

const RUN: &str = "profile-run";
const SERVICES: [&str; 6] = [
    "frontend",
    "cart",
    "checkout",
    "payments",
    "inventory",
    "auth",
];
const ROUTES: [&str; 4] = ["/api/cart", "/api/checkout", "/login", "/healthz"];
const METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];

fn kv(key: &str, value: any_value::Value) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue { value: Some(value) }),
    }
}

fn random_trace(rng: &mut StdRng, start_ns: u64) -> Trace {
    let mut id = [0u8; 16];
    rng.fill(&mut id);
    let trace_id = TraceId::new(id).unwrap();
    let spans = rng.random_range(1..12u64);
    let mut resource_spans = Vec::new();
    let mut parent: Vec<u8> = Vec::new();
    for index in 0..spans {
        let mut span_id = [0u8; 8];
        rng.fill(&mut span_id);
        let duration = rng.random_range(1_000..50_000_000u64);
        let span_start = start_ns + index * 1_000_000;
        let mut attributes = vec![kv(
            "fuzz.flag",
            any_value::Value::BoolValue(rng.random_bool(0.5)),
        )];
        if index == 0 || rng.random_bool(0.2) {
            attributes.push(kv(
                "http.route",
                any_value::Value::StringValue(
                    ROUTES[rng.random_range(0..ROUTES.len() as u64) as usize].into(),
                ),
            ));
            attributes.push(kv(
                "http.method",
                any_value::Value::StringValue(
                    METHODS[rng.random_range(0..METHODS.len() as u64) as usize].into(),
                ),
            ));
            attributes.push(kv(
                "http.status_code",
                any_value::Value::IntValue(match rng.random_range(0..100u64) {
                    0..3 => 500,
                    3..8 => 404,
                    _ => 200,
                }),
            ));
        }
        let span = Span {
            trace_id: id.to_vec(),
            span_id: span_id.to_vec(),
            parent_span_id: parent.clone(),
            name: format!("op-{}", rng.random_range(0..8u64)),
            kind: rng.random_range(1..4u32) as i32,
            start_time_unix_nano: span_start,
            end_time_unix_nano: span_start + duration,
            attributes,
            status: Some(Status {
                code: rng.random_range(0..3u32) as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        if rng.random_bool(0.6) {
            parent = span_id.to_vec();
        }
        resource_spans.push(ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    kv("fuzz.run", any_value::Value::StringValue(RUN.into())),
                    kv(
                        "service.name",
                        any_value::Value::StringValue(
                            SERVICES[rng.random_range(0..SERVICES.len() as u64) as usize].into(),
                        ),
                    ),
                ],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![span],
                ..Default::default()
            }],
            ..Default::default()
        });
    }
    Trace::new(trace_id, resource_spans).unwrap()
}

fn config(path: &std::path::Path) -> Config {
    Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "profile".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: path.display().to_string(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
                capacity: env_bytes("PROFILE_BLOCK_CACHE_BYTES").unwrap_or(64 << 20),
                shards: None,
            })),
            meta_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
                capacity: env_bytes("PROFILE_META_CACHE_BYTES").unwrap_or(16 << 20),
                shards: None,
            })),
            disk: Default::default(),
        }),
        segment_duration: Duration::from_secs(3600),
        retention: None,
        page: Default::default(),
        write_buffer: Default::default(),
        read_cache: Default::default(),
    }
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn env_bytes(name: &str) -> Option<u64> {
    std::env::var(name).ok().map(|value| value.parse().unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_traceql_phases() {
    let env = |name: &str| std::env::var(name).ok();
    let traces: usize = env("PROFILE_TRACES")
        .and_then(|value| value.parse().ok())
        .unwrap_or(2_500);
    let continued: f64 = env("PROFILE_CONTINUED")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0.0);
    let dir = tempfile::tempdir().unwrap();
    let namespace = Namespace::new("profile").unwrap();
    // `PROFILE_SPREAD_HOURS` spreads traces over the hours before the last
    // UTC midnight instead of the last ten minutes.
    let spread_hours: Option<u64> =
        env("PROFILE_SPREAD_HOURS").and_then(|value| value.parse().ok());
    let (base, now, spread_seconds) = match spread_hours {
        Some(hours) => {
            let day = 86_400 * 1_000_000_000;
            let midnight = common::time::now_ns().unsigned_abs() / day * day;
            (
                midnight - hours * 3_600 * 1_000_000_000,
                midnight - 1,
                hours * 3_600,
            )
        }
        None => {
            let now = common::time::now_ns().unsigned_abs();
            (now - 600 * 1_000_000_000, now, 500)
        }
    };
    let writer = TraceDb::open(config(dir.path())).await.unwrap();
    let mut rng = StdRng::seed_from_u64(7);
    let rounds = 50;
    let mut previous = Vec::new();
    let started = Instant::now();
    for _ in 0..rounds {
        let mut batch = (0..traces / rounds)
            .map(|_| {
                let offset = rng.random_range(0..spread_seconds) * 1_000_000_000;
                random_trace(&mut rng, base + offset)
            })
            .collect::<Vec<_>>();
        for (trace_id, start_ns) in std::mem::take(&mut previous) {
            batch.push(continue_trace(&mut rng, trace_id, start_ns));
        }
        previous = batch
            .iter()
            .filter(|_| rng.random_bool(continued))
            .map(|trace| (trace.trace_id, trace.timestamp_range().1))
            .collect();
        writer
            .write_with_durability(
                &namespace,
                vec![TraceBatch::new(batch)],
                Durability::Durable,
            )
            .await
            .unwrap();
    }
    println!(
        "ingest {:.2}ms over {rounds} durable writes",
        ms(started.elapsed())
    );
    writer
        .rollup_writer()
        .unwrap()
        .flush_memtable()
        .await
        .unwrap();
    if env("PROFILE_ROLLUP").is_some() {
        let started = Instant::now();
        let sealed = writer
            .rollup_at(
                &common::storage::rollup::RollupConfig {
                    delay: Duration::ZERO,
                    ..Default::default()
                },
                common::time::now_ns(),
            )
            .await
            .unwrap();
        println!("rollup: sealed {sealed} in {:.2}ms", ms(started.elapsed()));
    }
    writer.close().await.unwrap();
    let db = TraceDb::open_reader(config(dir.path()), DbReaderOptions::default())
        .await
        .unwrap();
    let (start, end) = (base - 60 * 1_000_000_000, now);
    let queries = [
        format!(r#"{{ resource.fuzz.run = "{RUN}" }}"#),
        format!(r#"{{ resource.fuzz.run = "{RUN}" && span.fuzz.flag = true }}"#),
        format!(
            r#"{{ resource.fuzz.run = "{RUN}" && duration >= 14ms }} >> {{ resource.fuzz.run = "{RUN}" && status != ok && name = "op-3" }}"#
        ),
        format!(
            r#"{{ resource.fuzz.run = "{RUN}" && span.http.route = "/api/cart" }} | count() > 6"#
        ),
        format!(r#"{{ resource.fuzz.run = "{RUN}" && span.http.route =~ "/log.*" }}"#),
        format!(r#"{{ resource.fuzz.run = "{RUN}" && span.http.status_code >= 500 }}"#),
        format!(
            r#"{{ resource.fuzz.run = "{RUN}" && span.http.method = "DELETE" && span.http.status_code >= 500 }}"#
        ),
    ];
    for source in &queries {
        for limit in [20, 1000] {
            let options = QueryOptions {
                limit,
                max_candidate_traces: 10_000,
                max_spans_per_trace: 10_000,
                max_concurrency: 8,
            };
            // Warm caches once, then measure.
            let first = Instant::now();
            db.query_traceql(&namespace, start, end, source, options)
                .await
                .unwrap();
            let first = first.elapsed();
            let total = Instant::now();
            let results = db
                .query_traceql(&namespace, start, end, source, options)
                .await
                .unwrap();
            let total = total.elapsed();

            let plan = crate::traceql::plan(crate::traceql::parse(source).unwrap()).unwrap();
            let unix_now = unix_time_ms().unwrap();
            let started = Instant::now();
            let located = db
                .ordered_candidates(&namespace, start, end, &plan.pushdown, unix_now)
                .await
                .unwrap();
            let candidates = started.elapsed();
            let started = Instant::now();
            let mut loaded = Vec::new();
            let memo = PageMemo::default();
            for batch in located.chunks(MATERIALIZE_BATCH) {
                loaded.extend(
                    db.load_candidates(&namespace, batch.to_vec(), &memo, None)
                        .await
                        .unwrap(),
                );
            }
            let load = started.elapsed();
            let started = Instant::now();
            let mut matched = 0;
            for trace in &loaded {
                if crate::traceql::execute(trace, &plan.query, 10_000)
                    .unwrap()
                    .is_some()
                {
                    matched += 1;
                }
            }
            let execute = started.elapsed();
            println!(
                "limit {limit:>4} results {:>4} | first {:>7.2}ms total {:>7.2}ms | candidates {:>5} select {:>6.2} load-all {:>6.2} exec-all {:>6.2} (matched {matched}) | {}",
                results.len(),
                ms(first),
                ms(total),
                located.len(),
                ms(candidates),
                ms(load),
                ms(execute),
                &source[..source.len().min(90)],
            );
        }
    }
    let ids = db.scan_trace_ids(&namespace, 500).await.unwrap();
    let started = Instant::now();
    for scanned in &ids {
        db.get_trace(&namespace, scanned.trace_id)
            .await
            .unwrap()
            .unwrap();
    }
    println!(
        "trace by id {:.3}ms each over {}",
        ms(started.elapsed()) / ids.len() as f64,
        ids.len()
    );
    db.close().await.unwrap();
}

/// Round-scoped exact queries as rounds accumulate, split into phases, to
/// find what still grows with data the query does not match.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_round_scoped_growth() {
    let dir = tempfile::tempdir().unwrap();
    let namespace = Namespace::new("profile").unwrap();
    let now = common::time::now_ns().unsigned_abs();
    let base = now - 600 * 1_000_000_000;
    let writer = TraceDb::open(config(dir.path())).await.unwrap();
    let mut reader = None;
    let mut rng = StdRng::seed_from_u64(11);
    for round in 0..40u64 {
        let batch = (0..100)
            .map(|_| {
                let offset = rng.random_range(0..500u64) * 1_000_000_000;
                let mut trace = random_trace(&mut rng, base + offset);
                for resource in &mut trace.resource_spans {
                    if let Some(resource) = &mut resource.resource {
                        resource
                            .attributes
                            .push(kv("fuzz.round", any_value::Value::IntValue(round as i64)));
                    }
                }
                trace
            })
            .collect::<Vec<_>>();
        writer
            .write_with_durability(
                &namespace,
                vec![TraceBatch::new(batch)],
                Durability::Durable,
            )
            .await
            .unwrap();
        if round % 5 != 4 {
            continue;
        }
        let db = if std::env::var("PROFILE_READER").is_ok() {
            if reader.is_none() {
                let options = DbReaderOptions {
                    manifest_poll_interval: Duration::from_secs(1),
                    ..DbReaderOptions::default()
                };
                reader = Some(
                    TraceDb::open_reader(config(dir.path()), options)
                        .await
                        .unwrap(),
                );
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            reader.as_ref().unwrap()
        } else {
            &writer
        };
        let source = format!(
            r#"{{ resource.fuzz.run = "{RUN}" && resource.fuzz.round = {} && span.fuzz.flag = true }}"#,
            round / 2
        );
        let plan = crate::traceql::plan(crate::traceql::parse(&source).unwrap()).unwrap();
        let (start, end) = (base - 60 * 1_000_000_000, now);
        let mut phases = [Duration::ZERO; 4];
        let samples = 20;
        for _ in 0..samples {
            let unix_now = unix_time_ms().unwrap();
            let started = Instant::now();
            let segments = db.catalog_segments(&namespace, start, end).await.unwrap();
            phases[0] += started.elapsed();
            let started = Instant::now();
            let located = db
                .ordered_candidates(&namespace, start, end, &plan.pushdown, unix_now)
                .await
                .unwrap();
            phases[1] += started.elapsed();
            let started = Instant::now();
            let loaded = db
                .load_candidates(&namespace, located, &PageMemo::default(), None)
                .await
                .unwrap();
            phases[2] += started.elapsed();
            let started = Instant::now();
            db.query_traceql(&namespace, start, end, &source, QueryOptions::default())
                .await
                .unwrap();
            phases[3] += started.elapsed();
            assert!(
                !segments.is_empty() && !loaded.is_empty(),
                "segments {segments:?} loaded {}",
                loaded.len()
            );
        }
        let [catalog, candidates, load, total] = phases.map(|phase| ms(phase) / samples as f64);
        println!(
            "rounds {:>2} | catalog {catalog:>6.3} candidates {candidates:>6.3} load {load:>6.3} total {total:>6.3} ms",
            round + 1
        );
    }
    if let Some(reader) = reader {
        reader.close().await.unwrap();
    }
    writer.close().await.unwrap();
}

#[derive(serde::Deserialize)]
enum FuzzValue {
    #[serde(rename = "s")]
    String(String),
    #[serde(rename = "i")]
    Int(i64),
    #[serde(rename = "d")]
    Double(f64),
    #[serde(rename = "b")]
    Bool(bool),
}

type FuzzAttributes = Vec<(String, FuzzValue)>;

#[derive(serde::Deserialize)]
struct FuzzSpan {
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    name: String,
    kind: i32,
    start: u64,
    end: u64,
    attributes: Vec<(String, FuzzValue)>,
    events: Vec<(u64, String, FuzzAttributes)>,
    status_code: i32,
    status_message: String,
}

#[derive(serde::Deserialize)]
struct FuzzScope {
    name: String,
    version: String,
    spans: Vec<FuzzSpan>,
}

#[derive(serde::Deserialize)]
struct FuzzResource {
    resource: Vec<(String, FuzzValue)>,
    scopes: Vec<FuzzScope>,
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
    trace_id: Option<String>,
    start: Option<u64>,
    end: Option<u64>,
    limit: Option<usize>,
    scope: Option<String>,
    name: Option<String>,
}

fn fuzz_attributes(attributes: Vec<(String, FuzzValue)>) -> Vec<KeyValue> {
    attributes
        .into_iter()
        .map(|(key, value)| {
            kv(
                &key,
                match value {
                    FuzzValue::String(value) => any_value::Value::StringValue(value),
                    FuzzValue::Int(value) => any_value::Value::IntValue(value),
                    FuzzValue::Double(value) => any_value::Value::DoubleValue(value),
                    FuzzValue::Bool(value) => any_value::Value::BoolValue(value),
                },
            )
        })
        .collect()
}

fn hex_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
        .collect()
}

fn fuzz_resource_spans(resource: FuzzResource) -> ResourceSpans {
    ResourceSpans {
        resource: Some(Resource {
            attributes: fuzz_attributes(resource.resource),
            dropped_attributes_count: 0,
        }),
        scope_spans: resource
            .scopes
            .into_iter()
            .map(|scope| ScopeSpans {
                scope: Some(
                    opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                        name: scope.name,
                        version: scope.version,
                        ..Default::default()
                    },
                ),
                spans: scope
                    .spans
                    .into_iter()
                    .map(|span| Span {
                        trace_id: hex_bytes(&span.trace_id),
                        span_id: hex_bytes(&span.span_id),
                        parent_span_id: hex_bytes(&span.parent_span_id),
                        name: span.name,
                        kind: span.kind,
                        start_time_unix_nano: span.start,
                        end_time_unix_nano: span.end,
                        attributes: fuzz_attributes(span.attributes),
                        events: span
                            .events
                            .into_iter()
                            .map(|(time_unix_nano, name, attributes)| {
                                opentelemetry_proto::tonic::trace::v1::span::Event {
                                    time_unix_nano,
                                    name,
                                    attributes: fuzz_attributes(attributes),
                                    dropped_attributes_count: 0,
                                }
                            })
                            .collect(),
                        status: Some(Status {
                            code: span.status_code,
                            message: span.status_message,
                        }),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn classify_key(key: &[u8]) -> &'static str {
    match crate::codec::key_record_type(key) {
        None => "segment",
        Some(1) => "next_page_sequence",
        Some(2) => "page_metadata",
        Some(3) => "page_payload",
        Some(4) => "trace_head",
        Some(5) => "attribute_posting",
        Some(6) => "trace_continuation",
        Some(_) => "catalog",
    }
}

async fn run_case(db: &TraceDb, namespace: &Namespace, case: &FuzzCase) -> Result<usize> {
    let seconds =
        |value: Option<u64>, default: u64| value.unwrap_or(default).saturating_mul(1_000_000_000);
    let start = seconds(case.start, 0);
    let end = seconds(case.end, u64::MAX / 1_000_000_000);
    let scoped = |name: &str| {
        let scope = match case.scope.as_deref() {
            Some("resource") => Some(AttributeScope::Resource),
            Some("span") => Some(AttributeScope::Span),
            _ => None,
        };
        if let Some(name) = name.strip_prefix("resource.") {
            (Some(AttributeScope::Resource), name.to_owned())
        } else if let Some(name) = name.strip_prefix("span.") {
            (Some(AttributeScope::Span), name.to_owned())
        } else {
            (scope, name.to_owned())
        }
    };
    Ok(match case.kind.as_str() {
        "by_id" => {
            let trace_id = case.trace_id.as_deref().unwrap().parse()?;
            usize::from(db.get_trace(namespace, trace_id).await?.is_some())
        }
        "search" => db
            .query_traceql(
                namespace,
                start,
                end,
                case.query.as_deref().unwrap(),
                QueryOptions {
                    limit: case.limit.unwrap_or(20),
                    max_candidate_traces: 10_000,
                    max_spans_per_trace: 100_000,
                    max_concurrency: 8,
                },
            )
            .await?
            .len(),
        "tags" => {
            let scopes = match case.scope.as_deref() {
                None => vec![AttributeScope::Resource, AttributeScope::Span],
                Some("resource") => vec![AttributeScope::Resource],
                Some("span") => vec![AttributeScope::Span],
                Some(_) => Vec::new(),
            };
            let mut names = 0;
            for scope in scopes {
                names += db
                    .catalog_names(namespace, start, end, Some(scope))
                    .await?
                    .len();
            }
            names
        }
        "tag_values" => {
            let (scope, name) = scoped(case.name.as_deref().unwrap());
            db.catalog_values(namespace, start, end, scope, &name)
                .await?
                .len()
        }
        other => panic!("unknown case kind {other}"),
    })
}

/// Replays every matched case of a fuzz run (`PROFILE_CASES/{rounds,cases}.json`,
/// written by `python -m harness.fuzz.replay traces`) against a reader, writing per-query latency
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
    let mut config = config(&store);
    config.page = PageConfig {
        target_size_bytes: 1 << 20,
        max_size_bytes: 4 << 20,
        max_traces: 1024,
    };
    // The fuzz regression stack's layout: two-trace pages, one-minute segments.
    if std::env::var("PROFILE_LAYOUT").as_deref() == Ok("regression") {
        config.page = PageConfig {
            target_size_bytes: 16 << 10,
            max_size_bytes: 1 << 20,
            max_traces: 2,
        };
        config.segment_duration = Duration::from_secs(60);
    }
    let marker = store.join("ingested");
    if !marker.exists() {
        std::fs::create_dir_all(&store).unwrap();
        let rounds: Vec<Vec<Vec<FuzzResource>>> =
            serde_json::from_slice(&std::fs::read(input.join("rounds.json")).unwrap()).unwrap();
        let writer = TraceDb::open(config.clone()).await.unwrap();
        for round in rounds {
            for request in round {
                let resources = request.into_iter().map(fuzz_resource_spans).collect();
                let batches = crate::trace_batches_from_resource_spans(resources).unwrap();
                writer
                    .write_with_durability(&namespace, batches, Durability::Durable)
                    .await
                    .unwrap();
            }
        }
        writer.close().await.unwrap();
        std::fs::write(&marker, b"").unwrap();
    }
    let cases: Vec<FuzzCase> =
        serde_json::from_slice(&std::fs::read(input.join("cases.json")).unwrap()).unwrap();

    // `PROFILE_MODE=writer` queries through the writing process, whose read
    // cache is kept current by its own writes.
    let mut db = if std::env::var("PROFILE_MODE").as_deref() == Ok("writer") {
        TraceDb::open(config).await.unwrap()
    } else {
        TraceDb::open_reader(config, DbReaderOptions::default())
            .await
            .unwrap()
    };
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

/// More spans for an already written trace, starting after it ends.
fn continue_trace(rng: &mut StdRng, trace_id: TraceId, start_ns: u64) -> Trace {
    let mut extra = random_trace(rng, start_ns + 1_000_000);
    for span in extra
        .resource_spans
        .iter_mut()
        .flat_map(|resource| &mut resource.scope_spans)
        .flat_map(|scope| &mut scope.spans)
    {
        span.trace_id = trace_id.as_bytes().to_vec();
    }
    Trace::new(trace_id, extra.resource_spans).unwrap()
}

// ── Cold reads ───────────────────────────────────────────────────────

/// One object-store read: path, bytes, and milliseconds since the
/// operation began.
#[derive(Clone, Debug, serde::Serialize)]
struct ColdGet {
    path: String,
    bytes: u64,
    start_ms: f64,
    end_ms: f64,
}

static COLD_GETS: std::sync::Mutex<Option<(Instant, Vec<ColdGet>)>> = std::sync::Mutex::new(None);

/// An object store that sleeps `latency` before every read, as S3's first
/// byte does, and records the reads made while [`COLD_GETS`] is set.
#[derive(Debug)]
struct SlowStore {
    inner: Arc<dyn slatedb::object_store::ObjectStore>,
    latency: Duration,
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
        let began = Instant::now();
        tokio::time::sleep(self.latency).await;
        let result = self.inner.get_opts(location, options).await;
        if let Some((origin, gets)) = COLD_GETS.lock().unwrap().as_mut() {
            let bytes = result
                .as_ref()
                .map_or(0, |result| result.range.end - result.range.start);
            gets.push(ColdGet {
                path: location.to_string(),
                bytes,
                start_ms: ms(began.saturating_duration_since(*origin)),
                end_ms: ms(origin.elapsed()),
            });
        }
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

/// The longest chain of reads where each starts after the previous one
/// ended: a lower bound on the operation's dependent round trips.
fn cold_waves(gets: &[ColdGet]) -> usize {
    let mut by_end: Vec<&ColdGet> = gets.iter().collect();
    by_end.sort_by(|a, b| a.end_ms.total_cmp(&b.end_ms));
    let mut chain: Vec<usize> = Vec::with_capacity(by_end.len());
    let mut best = 0;
    for (index, get) in by_end.iter().enumerate() {
        let prior = by_end[..index]
            .iter()
            .zip(&chain)
            .filter(|(earlier, _)| earlier.end_ms <= get.start_ms)
            .map(|(_, length)| *length)
            .max()
            .unwrap_or(0);
        chain.push(prior + 1);
        best = best.max(prior + 1);
    }
    best
}

fn cold_peak(gets: &[ColdGet]) -> usize {
    let mut edges: Vec<(f64, i32)> = gets
        .iter()
        .flat_map(|get| [(get.start_ms, 1), (get.end_ms, -1)])
        .collect();
    edges.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    let (mut live, mut most) = (0, 0);
    for (_, delta) in edges {
        live += delta;
        most = most.max(live);
    }
    most as usize
}

/// A day of hourly segments shaped like the fuzz harness's cold battery.
fn cold_trace(rng: &mut StdRng, start_ns: u64) -> Trace {
    let mut trace = random_trace(rng, start_ns);
    let tag = format!("t{}", rng.random_range(0..1_000u64));
    for resource in &mut trace.resource_spans {
        for span in resource
            .scope_spans
            .iter_mut()
            .flat_map(|scope| &mut scope.spans)
        {
            span.attributes
                .push(kv("fuzz.tag", any_value::Value::StringValue(tag.clone())));
        }
    }
    trace
}

/// Operations of the cold battery a profile can run.
async fn cold_op(db: &TraceDb, namespace: &Namespace, op: &str, start: u64, end: u64) -> usize {
    let options = |limit| QueryOptions {
        limit,
        max_candidate_traces: 10_000,
        max_spans_per_trace: 100_000,
        max_concurrency: 8,
    };
    match op {
        // `/api/v2/search/tags` with no scope: resource names, then span names.
        "names" => {
            let mut names = 0;
            for scope in [AttributeScope::Resource, AttributeScope::Span] {
                names += db
                    .catalog_names(namespace, start, end, Some(scope))
                    .await
                    .unwrap()
                    .len();
            }
            names
        }
        "names_joined" => {
            let (resource, span) = futures::future::try_join(
                db.catalog_names(namespace, start, end, Some(AttributeScope::Resource)),
                db.catalog_names(namespace, start, end, Some(AttributeScope::Span)),
            )
            .await
            .unwrap();
            resource.len() + span.len()
        }
        "values" => db
            .catalog_values(
                namespace,
                start,
                end,
                Some(AttributeScope::Resource),
                "service.name",
            )
            .await
            .unwrap()
            .len(),
        "broad" => db
            .query_traceql(
                namespace,
                start,
                end,
                &format!(r#"{{ resource.fuzz.run = "{RUN}" }}"#),
                options(20),
            )
            .await
            .unwrap()
            .len(),
        "selective" => db
            .query_traceql(
                namespace,
                start,
                end,
                &format!(
                    r#"{{ resource.fuzz.run = "{RUN}" && resource.service.name = "cart" && span.http.status_code = 500 }}"#
                ),
                options(1_000),
            )
            .await
            .unwrap()
            .len(),
        "tag" => db
            .query_traceql(
                namespace,
                start,
                end,
                &format!(r#"{{ resource.fuzz.run = "{RUN}" && span.fuzz.tag = "t7" }}"#),
                options(1_000),
            )
            .await
            .unwrap()
            .len(),
        other => panic!("unknown cold op {other}"),
    }
}

/// Times cold-battery operations on fresh readers over a store whose reads
/// each sleep `PROFILE_LATENCY_MS` (default 20). `PROFILE_STORE` (default
/// `/tmp/traces-coldpath/store`) is ingested once with `PROFILE_TRACES`
/// (default 30000) traces over `PROFILE_HOURS` (default 22) hourly segments,
/// flushed hour by hour, rolled up as the server would when `PROFILE_ROLLUP`
/// is set. `PROFILE_OPS` picks operations (see [`cold_op`]), `PROFILE_RUNS`
/// the cold runs of each, `PROFILE_TIMELINE` prints every read, and
/// `PROFILE_OUT` receives one JSON line per run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_cold_catalog() {
    use std::io::Write as _;

    let env = |name: &str| std::env::var(name).ok();
    let store = std::path::PathBuf::from(
        env("PROFILE_STORE").unwrap_or("/tmp/traces-coldpath/store".to_owned()),
    );
    let hours: u64 = env("PROFILE_HOURS").map_or(22, |value| value.parse().unwrap());
    let total: u64 = env("PROFILE_TRACES").map_or(30_000, |value| value.parse().unwrap());
    let latency =
        Duration::from_millis(env("PROFILE_LATENCY_MS").map_or(20, |value| value.parse().unwrap()));
    let runs: usize = env("PROFILE_RUNS").map_or(3, |value| value.parse().unwrap());
    let namespace = Namespace::new("regression").unwrap();
    let mut config = config(&store);
    config.page = PageConfig {
        target_size_bytes: 1 << 20,
        max_size_bytes: 4 << 20,
        max_traces: 1024,
    };
    config.retention = Some(Duration::from_secs(86_400));
    let hour_ns = 3_600 * 1_000_000_000u64;
    let marker = store.join("ingested.json");
    if !marker.exists() {
        let _ = std::fs::remove_dir_all(&store);
        std::fs::create_dir_all(&store).unwrap();
        let now = common::time::now_ns().unsigned_abs();
        let end = now / hour_ns * hour_ns;
        let writer = TraceDb::open(config.clone()).await.unwrap();
        let mut rng = StdRng::seed_from_u64(7);
        for hour in 0..hours {
            let base = end - (hours - hour) * hour_ns;
            let traces: Vec<Trace> = (0..total / hours)
                .map(|_| {
                    let offset = rng.random_range(0..3_590u64) * 1_000_000_000;
                    cold_trace(&mut rng, base + offset)
                })
                .collect();
            for chunk in traces.chunks(500) {
                writer
                    .write_with_durability(
                        &namespace,
                        vec![TraceBatch::new(chunk.to_vec())],
                        Durability::Durable,
                    )
                    .await
                    .unwrap();
            }
            writer
                .rollup_writer()
                .unwrap()
                .flush_memtable()
                .await
                .unwrap();
        }
        if env("PROFILE_ROLLUP").is_some() {
            let sealed = writer
                .rollup_at(
                    &common::storage::rollup::RollupConfig {
                        merge_wait: Duration::from_secs(5),
                        ..Default::default()
                    },
                    common::time::now_ns(),
                )
                .await
                .unwrap();
            println!("rollup: sealed {sealed}");
        }
        writer.close().await.unwrap();
        std::fs::write(&marker, serde_json::json!({ "end_ns": end }).to_string()).unwrap();
    }
    let meta: serde_json::Value = serde_json::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
    let end = meta["end_ns"].as_u64().unwrap();
    let start = end - 24 * hour_ns;
    let ops = env("PROFILE_OPS").unwrap_or("names,values,broad,selective,tag".to_owned());
    let mut out = env("PROFILE_OUT").map(|path| {
        std::io::BufWriter::new(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap(),
        )
    });
    let tag = env("PROFILE_TAG").unwrap_or_default();
    // A variant is `default` or `+`-joined switches: `seq` reads segment
    // catalogs one at a time, as before they were read concurrently, and
    // `batchN` caps TraceQL search rounds at N candidates (256 before).
    let variants = env("PROFILE_VARIANTS").unwrap_or("default".to_owned());
    for (variant, op) in variants
        .split(',')
        .flat_map(|variant| ops.split(',').map(move |op| (variant, op)))
    {
        let (mut catalog, mut batch) = (0, 0);
        for switch in variant.split('+') {
            match switch {
                "default" => {}
                "seq" => catalog = 1,
                cap if cap.starts_with("batch") => batch = cap["batch".len()..].parse().unwrap(),
                other => panic!("unknown switch {other}"),
            }
        }
        CATALOG_CONCURRENCY_OVERRIDE.store(catalog, std::sync::atomic::Ordering::Relaxed);
        SEARCH_BATCH_OVERRIDE.store(batch, std::sync::atomic::Ordering::Relaxed);
        let mut totals = Vec::new();
        for _ in 0..runs {
            let inner = common::create_object_store(match &config.storage {
                StorageConfig::SlateDb(slate) => &slate.object_store,
                _ => unreachable!(),
            })
            .unwrap();
            let slow: Arc<dyn slatedb::object_store::ObjectStore> =
                Arc::new(SlowStore { inner, latency });
            let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(
                common::storage::coalescing::CoalescingObjectStore::new(slow, "s3:profile"),
            );
            let mut db = TraceDb::open_reader(config.clone(), DbReaderOptions::default())
                .await
                .unwrap();
            db.storage = create_storage_read(
                &config.storage,
                StorageReaderRuntime::new().with_object_store(store),
                storage_semantics(),
                DbReaderOptions::default(),
            )
            .await
            .unwrap();
            *COLD_GETS.lock().unwrap() = Some((Instant::now(), Vec::new()));
            let started = Instant::now();
            let results = cold_op(&db, &namespace, op, start, end).await;
            let total_ms = ms(started.elapsed());
            let gets = COLD_GETS.lock().unwrap().take().unwrap().1;
            let bytes: u64 = gets.iter().map(|get| get.bytes).sum();
            println!(
                "[{variant}] {op:<12} {total_ms:>7.1}ms results {results:>5} gets {:>4} bytes {bytes:>9} waves {:>3} peak {:>3}",
                gets.len(),
                cold_waves(&gets),
                cold_peak(&gets),
            );
            if env("PROFILE_TIMELINE").is_some() {
                for get in &gets {
                    println!(
                        "    {:>7.1}-{:>7.1}ms {:>8}B {}",
                        get.start_ms, get.end_ms, get.bytes, get.path
                    );
                }
            }
            if let Some(out) = out.as_mut() {
                let row = serde_json::json!({
                    "tag": tag,
                    "variant": variant,
                    "op": op,
                    "total_ms": total_ms,
                    "results": results,
                    "gets": gets.len(),
                    "bytes": bytes,
                    "waves": cold_waves(&gets),
                    "peak": cold_peak(&gets),
                    "timeline": gets,
                });
                writeln!(out, "{row}").unwrap();
            }
            totals.push(total_ms);
            db.close().await.unwrap();
        }
        totals.sort_by(f64::total_cmp);
        println!(
            "[{variant}] {op:<12} p50 {:>7.1}ms over {runs}",
            totals[totals.len() / 2]
        );
    }
    CATALOG_CONCURRENCY_OVERRIDE.store(0, std::sync::atomic::Ordering::Relaxed);
    SEARCH_BATCH_OVERRIDE.store(0, std::sync::atomic::Ordering::Relaxed);
}

// ── Warmer memory ────────────────────────────────────────────────────

fn rss(system: &mut sysinfo::System, pid: sysinfo::Pid) -> u64 {
    system.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[pid]),
        false,
        sysinfo::ProcessRefreshKind::nothing().with_memory(),
    );
    system.process(pid).map_or(0, |process| process.memory())
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / f64::from(1u32 << 20)
}

/// The cold catalog store's config, ingesting `PROFILE_TRACES` traces over
/// `PROFILE_HOURS` hourly segments into `PROFILE_STORE` when missing.
async fn warm_memory_store() -> (std::path::PathBuf, Config, u64) {
    let env = |name: &str| std::env::var(name).ok();
    let store = std::path::PathBuf::from(
        env("PROFILE_STORE").unwrap_or("/tmp/warm-mt/traces-store".to_owned()),
    );
    let hours: u64 = env("PROFILE_HOURS").map_or(22, |value| value.parse().unwrap());
    let total: u64 = env("PROFILE_TRACES").map_or(300_000, |value| value.parse().unwrap());
    let retention_hours: u64 =
        env("PROFILE_RETENTION_HOURS").map_or(24, |value| value.parse().unwrap());
    let mut config = config(&store);
    config.page = PageConfig {
        target_size_bytes: 1 << 20,
        max_size_bytes: 4 << 20,
        max_traces: 1024,
    };
    config.retention = Some(Duration::from_secs(retention_hours * 3_600));
    let hour_ns = 3_600 * 1_000_000_000u64;
    let marker = store.join("ingested.json");
    if !marker.exists() {
        let _ = std::fs::remove_dir_all(&store);
        std::fs::create_dir_all(&store).unwrap();
        let end = common::time::now_ns().unsigned_abs() / hour_ns * hour_ns;
        let writer = TraceDb::open(config.clone()).await.unwrap();
        let namespace = Namespace::new("regression").unwrap();
        let mut rng = StdRng::seed_from_u64(7);
        for hour in 0..hours {
            write_hour(
                &writer,
                &namespace,
                &mut rng,
                end - (hours - hour) * hour_ns,
                total / hours,
            )
            .await;
            eprintln!("ingested hour {hour}");
        }
        writer.close().await.unwrap();
        std::fs::write(&marker, serde_json::json!({ "end_ns": end }).to_string()).unwrap();
    }
    let meta: serde_json::Value = serde_json::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
    (store, config, meta["end_ns"].as_u64().unwrap())
}

async fn write_hour(
    writer: &TraceDb,
    namespace: &Namespace,
    rng: &mut StdRng,
    base: u64,
    traces: u64,
) {
    let traces: Vec<Trace> = (0..traces)
        .map(|_| {
            let offset = rng.random_range(0..3_590u64) * 1_000_000_000;
            cold_trace(rng, base + offset)
        })
        .collect();
    for chunk in traces.chunks(500) {
        writer
            .write_with_durability(
                namespace,
                vec![TraceBatch::new(chunk.to_vec())],
                Durability::Durable,
            )
            .await
            .unwrap();
    }
    writer
        .rollup_writer()
        .unwrap()
        .flush_memtable()
        .await
        .unwrap();
}

/// A reader's resident set size, metadata cache usage and warmer ledger
/// over the store of [`warm_memory_store`]: after opening, after the
/// startup pass (`PROFILE_WARM_PERCENT` of the metadata cache, sized by
/// `PROFILE_META_CACHE_BYTES`), after each of `PROFILE_CYCLES` continuous
/// passes `PROFILE_CYCLE_SECONDS` apart, and while `PROFILE_QUERY_TASKS`
/// tasks run the cold operations for `PROFILE_QUERY_SECONDS`. Run
/// [`profile_warm_memory_writer`] alongside so passes see SSTs removed.
/// `PROFILE_OUT` receives one JSON line per phase.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_warm_memory() {
    use common::storage::slate::warm::{
        WarmBudget, WarmLedger, WarmPlan, WarmReport, warm_planned,
    };
    use std::io::Write as _;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

    let env = |name: &str| std::env::var(name).ok();
    let parse =
        |name: &str, default: u64| env(name).map_or(default, |value| value.parse().unwrap());
    let (_store, config, end) = warm_memory_store().await;
    let latency = Duration::from_millis(parse("PROFILE_LATENCY_MS", 20));
    let percent = parse("PROFILE_WARM_PERCENT", 75) as u8;
    let concurrency = parse("PROFILE_WARM_CONCURRENCY", 2) as usize;
    let cycles = parse("PROFILE_CYCLES", 18);
    let cycle = Duration::from_secs(parse("PROFILE_CYCLE_SECONDS", 10));
    let query_tasks = parse("PROFILE_QUERY_TASKS", 8);
    let query_time = Duration::from_secs(parse("PROFILE_QUERY_SECONDS", 60));
    let payloads = env("PROFILE_CONTINUOUS_PAYLOADS").as_deref() != Some("off");
    let metadata = match parse("PROFILE_WARM_RANGE_SECONDS", 26 * 3_600) {
        0 => None,
        seconds => Some(Duration::from_secs(seconds)),
    };
    let namespace = Namespace::new("regression").unwrap();
    let namespaces = [namespace.clone()];
    let cancel = tokio_util::sync::CancellationToken::new();

    let peak = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = std::thread::spawn({
        let (peak, stop) = (Arc::clone(&peak), Arc::clone(&stop));
        move || {
            let pid = sysinfo::get_current_pid().unwrap();
            let mut system = sysinfo::System::new();
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(rss(&mut system, pid), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    });
    let mut out = env("PROFILE_OUT").map(|path| {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap()
    });
    let pid = sysinfo::get_current_pid().unwrap();
    let mut system = sysinfo::System::new();
    let began = Instant::now();
    let queries = Arc::new(AtomicUsize::new(0));

    let cache = SharedDbCache::from_config(&config.storage).await.unwrap();
    let inner = common::create_object_store(match &config.storage {
        StorageConfig::SlateDb(slate) => &slate.object_store,
        _ => unreachable!(),
    })
    .unwrap();
    let slow: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(SlowStore { inner, latency });
    let mut db =
        TraceDb::open_reader_with_cache(config.clone(), DbReaderOptions::default(), &cache)
            .await
            .unwrap();
    db.storage = create_storage_read(
        &config.storage,
        StorageReaderRuntime::new()
            .with_object_store(slow)
            .with_shared_cache(&cache),
        storage_semantics(),
        DbReaderOptions::default(),
    )
    .await
    .unwrap();
    let db = Arc::new(db);
    let ledger = WarmLedger::new(true);
    let budget = WarmBudget::of(
        &cache,
        common::storage::slate::warm::WarmShares {
            metadata_percent: percent,
            ..Default::default()
        },
    )
    .unwrap();

    let mut record = |phase: &str, report: WarmReport, gets: &[ColdGet], elapsed: Duration| {
        let meta = cache.meta().and_then(|meta| meta.usage.as_ref());
        let now = rss(&mut system, pid);
        let row = serde_json::json!({
            "phase": phase,
            "unix_ms": common::time::now_ms(),
            "at_s": began.elapsed().as_secs_f64(),
            "rss_mib": mib(now),
            "peak_rss_mib": mib(peak.swap(0, Ordering::Relaxed).max(now)),
            "meta_usage_mib": mib(meta.map_or(0, |meta| meta.usage_bytes())),
            "meta_capacity_mib": mib(meta.map_or(0, |meta| meta.capacity_bytes())),
            "meta_evictions": meta.map_or(0, |meta| meta.evictions()),
            "data_usage_mib": mib(cache.data_usage().map_or(0, |data| data.usage_bytes())),
            "budget_mib": mib(budget.bytes),
            "ledger_mib": mib(ledger.bytes()),
            "ledger_ssts": ledger.ssts(),
            "planned_ssts": report.planned_ssts,
            "planned_mib": mib(report.planned_bytes),
            "warmed_ssts": report.warmed_ssts,
            "warmed_mib": mib(report.warmed_bytes),
            "skipped_ssts": report.skipped_ssts,
            "skipped_mib": mib(report.skipped_bytes),
            "evicted_ssts": report.evicted_ssts,
            "displaced_ssts": report.displaced_ssts,
            "gets": gets.len(),
            "get_mib": mib(gets.iter().map(|get| get.bytes).sum()),
            "elapsed_ms": ms(elapsed),
            "queries": queries.load(Ordering::Relaxed),
        });
        eprintln!("{row}");
        if let Some(out) = out.as_mut() {
            writeln!(out, "{row}").unwrap();
        }
    };
    let pass = |include_payloads: bool| {
        let (db, ledger, cancel, namespaces) = (&db, &ledger, &cancel, &namespaces);
        let window = common::WarmWindow {
            metadata,
            payloads: include_payloads.then_some(common::storage::config::ALL_LIVE_DATA),
        };
        async move {
            let mut plan = WarmPlan::default();
            db.plan_warm(namespaces, window, &mut plan).await.unwrap();
            warm_planned(
                "traces",
                plan,
                budget,
                include_payloads,
                concurrency,
                cancel,
                ledger,
            )
            .await
        }
    };

    record("opened", WarmReport::default(), &[], Duration::ZERO);
    *COLD_GETS.lock().unwrap() = Some((Instant::now(), Vec::new()));
    let started = Instant::now();
    let report = pass(false).await;
    let gets = COLD_GETS.lock().unwrap().take().unwrap().1;
    record("startup", report, &gets, started.elapsed());
    for n in 0..cycles {
        tokio::time::sleep(cycle).await;
        *COLD_GETS.lock().unwrap() = Some((Instant::now(), Vec::new()));
        let started = Instant::now();
        let report = pass(payloads).await;
        let gets = COLD_GETS.lock().unwrap().take().unwrap().1;
        record(&format!("cycle-{n}"), report, &gets, started.elapsed());
    }

    let deadline = Instant::now() + query_time;
    let start = end - 24 * 3_600 * 1_000_000_000;
    let ops: Arc<Vec<String>> = Arc::new(
        env("PROFILE_OPS")
            .unwrap_or("names,values,broad,selective,tag".to_owned())
            .split(',')
            .map(str::to_owned)
            .collect(),
    );
    let tasks: Vec<_> = (0..query_tasks)
        .map(|task| {
            let (db, namespace, queries, ops) = (
                Arc::clone(&db),
                namespace.clone(),
                Arc::clone(&queries),
                Arc::clone(&ops),
            );
            tokio::spawn(async move {
                let mut n = task as usize;
                while Instant::now() < deadline {
                    cold_op(&db, &namespace, &ops[n % ops.len()], start, end).await;
                    queries.fetch_add(1, Ordering::Relaxed);
                    n += 1;
                }
            })
        })
        .collect();
    let mut n = 0;
    while Instant::now() < deadline {
        tokio::time::sleep(cycle.min(deadline.saturating_duration_since(Instant::now()))).await;
        let started = Instant::now();
        let report = pass(payloads).await;
        record(&format!("queries-{n}"), report, &[], started.elapsed());
        n += 1;
    }
    for task in tasks {
        task.await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    record("after-queries", WarmReport::default(), &[], Duration::ZERO);
    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
}

/// Keeps writing [`warm_memory_store`]'s current hour: `PROFILE_WRITER_ROUNDS`
/// rounds, one every `PROFILE_WRITER_PACE_SECONDS`, each a quarter hour's
/// traces flushed to a new L0 SST and followed by a rollup as of now, so
/// readers see new SSTs while compaction, rollups and retention remove old
/// ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual profiling"]
async fn profile_warm_memory_writer() {
    let env = |name: &str| std::env::var(name).ok();
    let parse =
        |name: &str, default: u64| env(name).map_or(default, |value| value.parse().unwrap());
    let (_store, config, end) = warm_memory_store().await;
    let rounds = parse("PROFILE_WRITER_ROUNDS", 8);
    let pace = Duration::from_secs(parse("PROFILE_WRITER_PACE_SECONDS", 15));
    let per_round = parse("PROFILE_TRACES", 300_000) / parse("PROFILE_HOURS", 22) / 4;
    let writer = TraceDb::open(config).await.unwrap();
    let namespace = Namespace::new("regression").unwrap();
    let mut rng = StdRng::seed_from_u64(common::time::now_ns().unsigned_abs());
    let rollup = common::storage::rollup::RollupConfig {
        merge_wait: Duration::from_secs(5),
        ..Default::default()
    };
    for round in 0..rounds {
        let started = Instant::now();
        write_hour(&writer, &namespace, &mut rng, end, per_round).await;
        let sealed = writer
            .rollup_at(&rollup, common::time::now_ns())
            .await
            .unwrap();
        eprintln!("round {round}: wrote {per_round} traces, sealed {sealed}");
        tokio::time::sleep(pace.saturating_sub(started.elapsed())).await;
    }
    writer.close().await.unwrap();
}
