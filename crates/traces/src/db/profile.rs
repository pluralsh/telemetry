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
                capacity: 64 << 20,
                shards: None,
            })),
            meta_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
                capacity: 16 << 20,
                shards: None,
            })),
        }),
        segment_duration: Duration::from_secs(3600),
        retention: None,
        page: Default::default(),
        write_buffer: Default::default(),
    }
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
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
    let now = common::time::now_ns().unsigned_abs();
    let base = now - 600 * 1_000_000_000;
    let writer = TraceDb::open(config(dir.path())).await.unwrap();
    let mut rng = StdRng::seed_from_u64(7);
    let rounds = 50;
    let mut previous = Vec::new();
    let started = Instant::now();
    for _ in 0..rounds {
        let mut batch = (0..traces / rounds)
            .map(|_| {
                let offset = rng.random_range(0..500u64) * 1_000_000_000;
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
            db.query_traceql(&namespace, start, end, source, options)
                .await
                .unwrap();
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
            let complete = located
                .iter()
                .filter(|trace| trace.locators.is_some())
                .count();
            let started = Instant::now();
            let mut loaded = Vec::new();
            let memo = PageMemo::default();
            for batch in located.chunks(MATERIALIZE_BATCH) {
                loaded.extend(
                    db.load_candidates(&namespace, batch.to_vec(), &memo)
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
                "limit {limit:>4} results {:>4} | total {:>7.2}ms | candidates {:>5} (complete {complete:>5}) select {:>6.2} load-all {:>6.2} exec-all {:>6.2} (matched {matched}) | {}",
                results.len(),
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
                .load_candidates(&namespace, located, &PageMemo::default())
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
