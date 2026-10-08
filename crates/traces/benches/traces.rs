use std::hint::black_box;
use std::time::Duration;

use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig, StorageConfig};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};
use plural_traces::bench_support::{
    columns, execute, prefilter, prepare, ruled_out, ruled_out_by, sidecar_len,
};
use plural_traces::{
    Config, Durability, Namespace, Page, PageConfig, QueryOptions, Trace, TraceBatch, TraceDb,
    TraceId,
};

const TRACES: usize = 1_000;
const PAGE_TRACES: usize = 128;
const BASE_NS: u64 = 1_790_000_000_000_000_000;
const MAX_SPANS: usize = 100_000;

const QUERIES: [(&str, &str); 12] = [
    ("duration_gt", "{ duration > 100ms }"),
    ("status_error", "{ status = error }"),
    ("http_500", "{ span.http.status_code = 500 }"),
    (
        "service_and_duration",
        r#"{ resource.service.name = "checkout" && duration > 50ms }"#,
    ),
    ("avg_duration", "{ } | avg(duration) > 10ms"),
    (
        "structural",
        r#"{ resource.service.name = "frontend" } >> { status = error }"#,
    ),
    ("name_eq", r#"{ name = "SELECT orders" }"#),
    ("error_slow", "{ status = error && duration > 1s }"),
    (
        "rare_consumer_error",
        "{ kind = consumer && status = error && duration > 1s }",
    ),
    ("route_regex", r#"{ span.http.route =~ "/log.*" }"#),
    ("http_5xx", "{ span.http.status_code >= 500 }"),
    (
        "method_delete_5xx",
        r#"{ span.http.method = "DELETE" && span.http.status_code >= 500 }"#,
    ),
];

const SERVICES: [&str; 8] = [
    "frontend",
    "checkout",
    "cart",
    "payments",
    "inventory",
    "auth",
    "shipping",
    "recommendations",
];
const OPERATIONS: [&str; 10] = [
    "GET /api/cart",
    "POST /api/checkout",
    "GET /api/products",
    "SELECT orders",
    "INSERT payments",
    "redis GET",
    "grpc.Inventory/Reserve",
    "kafka.produce",
    "render",
    "authorize",
];
const ROUTES: [&str; 5] = [
    "/api/cart",
    "/api/checkout",
    "/api/products/{id}",
    "/login",
    "/healthz",
];
const METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];

/// xorshift64*, so every run benchmarks the same corpus.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.below(items.len() as u64) as usize]
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

fn kv(key: &str, value: any_value::Value) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue { value: Some(value) }),
    }
}

fn string(key: &str, value: impl Into<String>) -> KeyValue {
    kv(key, any_value::Value::StringValue(value.into()))
}

fn synthetic_trace(rng: &mut Rng, index: usize) -> Trace {
    let mut id = [0_u8; 16];
    id[..8].copy_from_slice(&rng.next().to_be_bytes());
    id[8..].copy_from_slice(&(index as u64 + 1).to_be_bytes());
    let trace_id = TraceId::new(id).unwrap();
    let span_count = 5 + rng.below(196) as usize;
    let services = (0..1 + rng.below(4))
        .map(|_| rng.pick(&SERVICES))
        .collect::<Vec<_>>();
    let start = BASE_NS + rng.below(3_600) * 1_000_000_000;
    let mut by_service = vec![Vec::new(); services.len()];
    let mut span_ids: Vec<Vec<u8>> = Vec::with_capacity(span_count);
    for position in 0..span_count {
        let span_id = rng.next().to_be_bytes().to_vec();
        let parent = match position {
            0 => Vec::new(),
            _ => span_ids[rng.below(position as u64) as usize].clone(),
        };
        span_ids.push(span_id.clone());
        let duration = (10_000_u64 << rng.below(18)) + rng.below(10_000);
        let span_start = start + rng.below(1_000_000_000);
        let kind = match position {
            0 => 2,
            _ => 1 + rng.below(5) as i32,
        };
        let status = match rng.below(100) {
            0..4 => 2,
            4..24 => 1,
            _ => 0,
        };
        let mut attributes = vec![
            string("http.method", rng.pick(&METHODS)),
            string("http.route", rng.pick(&ROUTES)),
            kv(
                "http.status_code",
                any_value::Value::IntValue(match rng.below(100) {
                    0..3 => 500,
                    3..8 => 404,
                    _ => 200,
                }),
            ),
        ];
        if kind == 3 {
            attributes.push(string("db.system", "postgresql"));
            attributes.push(string("net.peer.name", "db.internal"));
        }
        if rng.chance(30) {
            attributes.push(kv(
                "app.items",
                any_value::Value::IntValue(rng.below(50) as i64),
            ));
        }
        let service = match position {
            0 => 0,
            _ => rng.below(services.len() as u64) as usize,
        };
        by_service[service].push(Span {
            trace_id: id.to_vec(),
            span_id,
            parent_span_id: parent,
            name: rng.pick(&OPERATIONS).to_owned(),
            kind,
            start_time_unix_nano: span_start,
            end_time_unix_nano: span_start + duration,
            attributes,
            status: Some(Status {
                code: status,
                message: if status == 2 {
                    "internal error".to_owned()
                } else {
                    String::new()
                },
            }),
            ..Default::default()
        });
    }
    let resource_spans = services
        .iter()
        .zip(by_service)
        .filter(|(_, spans)| !spans.is_empty())
        .map(|(service, spans)| ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    string("service.name", *service),
                    string("service.version", "1.4.2"),
                    string("deployment.environment", "production"),
                    string("host.name", format!("node-{}", rng.below(16))),
                    string(
                        "k8s.pod.name",
                        format!("{service}-{:x}", rng.below(1 << 20)),
                    ),
                ],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "io.opentelemetry".to_owned(),
                    version: "1.30.0".to_owned(),
                    ..Default::default()
                }),
                spans,
                ..Default::default()
            }],
            ..Default::default()
        })
        .collect();
    Trace::new(trace_id, resource_spans).unwrap()
}

fn corpus() -> Vec<Trace> {
    let mut rng = Rng(0x5eed_cafe_f00d_d00d);
    (0..TRACES)
        .map(|index| synthetic_trace(&mut rng, index))
        .collect()
}

fn span_count(traces: &[Trace]) -> u64 {
    traces
        .iter()
        .map(|trace| trace.spans().count() as u64)
        .sum()
}

fn page_benches(c: &mut Criterion, traces: &[Trace]) {
    let page_traces = &traces[..PAGE_TRACES];
    let page = Page::from_traces(page_traces, usize::MAX >> 1).unwrap();
    let bytes = page.bytes();
    println!(
        "page: {} traces, {} spans, {} bytes",
        PAGE_TRACES,
        span_count(page_traces),
        bytes.len()
    );
    for page_len in [16, PAGE_TRACES] {
        let (mut sidecar, mut total) = (0, 0);
        for chunk in traces.chunks(page_len) {
            let page = Page::from_traces(chunk, usize::MAX >> 1).unwrap();
            sidecar += sidecar_len(&page);
            total += page.bytes().len();
        }
        println!(
            "sidecar: {page_len}-trace pages, {sidecar} of {total} bytes, {:.2}% over trace data",
            100.0 * sidecar as f64 / (total - sidecar) as f64
        );
    }
    let mut group = c.benchmark_group("page");
    group.throughput(Throughput::Elements(span_count(page_traces)));
    group.bench_function("encode", |b| {
        b.iter(|| Page::from_traces(black_box(page_traces), usize::MAX >> 1).unwrap())
    });
    group.bench_function("decode_directory", |b| {
        b.iter(|| Page::decode(black_box(bytes.clone())).unwrap())
    });
    group.bench_function("decode_all", |b| {
        b.iter(|| {
            let page = Page::decode(black_box(bytes.clone())).unwrap();
            (0..page.directory().len())
                .map(|index| page.decode_trace(index).unwrap())
                .collect::<Vec<_>>()
        })
    });
    let decoded = Page::decode(bytes.clone()).unwrap();
    let ids = page_traces
        .iter()
        .step_by(7)
        .map(|trace| trace.trace_id)
        .collect::<Vec<_>>();
    group.throughput(Throughput::Elements(ids.len() as u64));
    group.bench_function("get_trace", |b| {
        b.iter(|| {
            for id in &ids {
                black_box(decoded.get_trace(*id).unwrap());
            }
        })
    });
    group.finish();
}

fn execution_benches(c: &mut Criterion, traces: &[Trace]) {
    let mut group = c.benchmark_group("execute");
    group.sample_size(20);
    group.throughput(Throughput::Elements(span_count(traces)));
    for (name, source) in QUERIES {
        let query = prepare(source).unwrap();
        let matched = traces
            .iter()
            .filter(|trace| execute(trace, &query, MAX_SPANS).unwrap().is_some())
            .count();
        println!("execute {name}: {matched}/{} traces match", traces.len());
        group.bench_with_input(BenchmarkId::from_parameter(name), &query, |b, query| {
            b.iter(|| {
                traces
                    .iter()
                    .filter(|trace| execute(trace, query, MAX_SPANS).unwrap().is_some())
                    .count()
            })
        });
    }
    group.finish();
}

fn prune_benches(c: &mut Criterion, traces: &[Trace]) {
    let pages = traces
        .chunks(PAGE_TRACES)
        .map(|chunk| Page::from_traces(chunk, usize::MAX >> 1).unwrap().bytes())
        .collect::<Vec<_>>();
    let mut group = c.benchmark_group("prune");
    group.sample_size(20);
    group.throughput(Throughput::Elements(traces.len() as u64));
    for (name, source) in QUERIES {
        let filter = prefilter(source).unwrap();
        let count = || {
            pages
                .iter()
                .map(|bytes| ruled_out(bytes.clone(), &filter).unwrap())
                .sum::<usize>()
        };
        println!(
            "prune {name}: {}/{} traces ruled out",
            count(),
            traces.len()
        );
        group.bench_function(name, |b| b.iter(count));
    }
    group.finish();
}

fn column_benches(c: &mut Criterion, traces: &[Trace]) {
    let pages = traces
        .chunks(PAGE_TRACES)
        .map(|chunk| columns(&Page::from_traces(chunk, usize::MAX >> 1).unwrap()).unwrap())
        .collect::<Vec<_>>();
    let mut group = c.benchmark_group("columns");
    group.throughput(Throughput::Elements(span_count(traces)));
    for (name, source) in QUERIES {
        let filter = prefilter(source).unwrap();
        group.bench_function(name, |b| {
            b.iter(|| {
                pages
                    .iter()
                    .map(|columns| ruled_out_by(columns, black_box(&filter)).unwrap())
                    .sum::<usize>()
            })
        });
    }
    group.finish();
}

fn end_to_end_benches(c: &mut Criterion, traces: &[Trace]) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let namespace = Namespace::new("bench").unwrap();
    let db = runtime.block_on(async {
        let db = TraceDb::open(Config {
            storage: StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "traces-bench".to_owned(),
                object_store: ObjectStoreConfig::InMemory,
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            }),
            segment_duration: Duration::from_secs(3_600),
            retention: None,
            page: PageConfig::default(),
            write_buffer: Default::default(),
            read_cache: Default::default(),
        })
        .await
        .unwrap();
        for chunk in traces.chunks(100) {
            db.write_with_durability(
                &namespace,
                vec![TraceBatch::new(chunk.to_vec())],
                Durability::Written,
            )
            .await
            .unwrap();
        }
        db
    });
    let options = QueryOptions {
        limit: TRACES,
        max_candidate_traces: TRACES * 2,
        ..QueryOptions::default()
    };
    let (start, end) = (BASE_NS - 1, BASE_NS + 7_200 * 1_000_000_000);
    let mut group = c.benchmark_group("query");
    group.sample_size(10);
    for (name, source) in QUERIES {
        let found = runtime
            .block_on(db.query_traceql(&namespace, start, end, source, options))
            .unwrap();
        println!("query {name}: {} results", found.len());
        group.bench_function(name, |b| {
            b.iter(|| {
                runtime
                    .block_on(db.query_traceql(&namespace, start, end, source, options))
                    .unwrap()
                    .len()
            })
        });
    }
    group.finish();
    runtime.block_on(db.close()).unwrap();
}

fn benches(c: &mut Criterion) {
    let traces = corpus();
    page_benches(c, &traces);
    execution_benches(c, &traces);
    prune_benches(c, &traces);
    column_benches(c, &traces);
    end_to_end_benches(c, &traces);
}

criterion_group!(traces_benches, benches);
criterion_main!(traces_benches);
