use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use plural_logs::bench_support::{
    EncodedBlock, count_line_matches, decode_block, decode_samples, encode_block, run_entries,
};
use plural_logs::logql::{LineFilter, LineFilterBranch, LineFilterOp, LineFilterTerm};
use plural_logs::{Field, Fields, LogEntry};

const ROWS_PER_BLOCK: usize = 256;
const LARGE_BLOCK_ROWS: usize = 1024;
const RUN_BLOCKS: usize = 16;
const BASE_TIMESTAMP_NS: i64 = 1_790_000_000_000_000_000;

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

    fn uuid(&mut self) -> String {
        let (a, b) = (self.next(), self.next());
        format!(
            "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
            a >> 32,
            (a >> 16) & 0xffff,
            a & 0xfff,
            (b >> 48) & 0x3fff | 0x8000,
            b & 0xffff_ffff_ffff
        )
    }

    fn hex(&mut self, digits: usize) -> String {
        let mut out = String::with_capacity(digits);
        while out.len() < digits {
            out.push_str(&format!("{:016x}", self.next()));
        }
        out.truncate(digits);
        out
    }

    fn ip(&mut self) -> String {
        format!(
            "10.{}.{}.{}",
            self.below(256),
            self.below(256),
            self.below(256)
        )
    }
}

const LEVELS: &[&str] = &["info", "info", "info", "debug", "warn", "error"];
const NAMESPACES: &[&str] = &["payments", "checkout", "identity", "search", "ingest"];
const PATHS: &[&str] = &[
    "/api/v1/users",
    "/api/v1/orders",
    "/healthz",
    "/api/v2/search",
    "/metrics",
];
const MESSAGES: &[&str] = &[
    "Reconciling deployment",
    "Observed a panic in reconciler",
    "Starting workers",
    "Successfully synced",
    "Updated status subresource",
    "Requeue after transient error",
];
const WORDS: &[&str] = &[
    "upstream",
    "request",
    "completed",
    "retry",
    "backoff",
    "cache",
    "miss",
    "hit",
    "shard",
    "replica",
    "lease",
    "acquired",
    "token",
    "refresh",
    "pool",
    "connection",
];

fn timestamp(nanos: i64) -> String {
    let seconds = nanos / 1_000_000_000;
    let millis = (nanos / 1_000_000) % 1000;
    format!(
        "2026-10-04T{:02}:{:02}:{:02}.{millis:03}Z",
        (seconds / 3600) % 24,
        (seconds / 60) % 60,
        seconds % 60
    )
}

fn padding(rng: &mut Rng, line: &mut String, target: usize) {
    while line.len() < target {
        line.push(' ');
        line.push_str(rng.pick(WORDS));
    }
}

fn line(rng: &mut Rng, timestamp_ns: i64) -> String {
    let target = 80 + rng.below(320) as usize;
    let mut line = match rng.below(3) {
        0 => format!(
            r#"{{"level":"{}","ts":"{}","logger":"controller.deployment","msg":"{}","namespace":"{}","name":"api-{}","reconcileID":"{}","duration_ms":{}.{}"#,
            rng.pick(LEVELS),
            timestamp(timestamp_ns),
            rng.pick(MESSAGES),
            rng.pick(NAMESPACES),
            rng.hex(5),
            rng.uuid(),
            rng.below(500),
            rng.below(10)
        ),
        1 => format!(
            "level={} ts={} caller=handler.go:{} method=GET path={}/{} status={} duration={}ms client={} trace_id={}",
            rng.pick(LEVELS),
            timestamp(timestamp_ns),
            rng.below(400),
            rng.pick(PATHS),
            rng.below(100_000),
            [200, 200, 200, 404, 500][rng.below(5) as usize],
            rng.below(2000),
            rng.ip(),
            rng.hex(32)
        ),
        _ => format!(
            "{} {} [http-nio-8080-exec-{}] c.e.OrderService - Processed order {} for {} in {}ms",
            timestamp(timestamp_ns),
            rng.pick(LEVELS).to_uppercase(),
            rng.below(32),
            rng.uuid(),
            rng.ip(),
            rng.below(900)
        ),
    };
    padding(rng, &mut line, target);
    if line.starts_with('{') {
        line.push('}');
    }
    line
}

fn metadata(rng: &mut Rng) -> Fields {
    let mut fields = Vec::new();
    if rng.below(3) == 0 {
        fields.push(Field::new("trace_id", rng.hex(32)));
        fields.push(Field::new("span_id", rng.hex(16)));
    }
    if rng.below(4) == 0 {
        fields.push(Field::new("pod", format!("api-{}", rng.hex(5))));
    }
    Fields::new(fields).expect("unique names")
}

fn corpus(seed: u64, rows: usize) -> Vec<LogEntry> {
    let mut rng = Rng(seed);
    let mut timestamp_ns = BASE_TIMESTAMP_NS;
    (0..rows)
        .map(|_| {
            timestamp_ns += rng.below(50_000_000) as i64;
            let line = line(&mut rng, timestamp_ns);
            LogEntry::with_structured_metadata(timestamp_ns, line, metadata(&mut rng))
        })
        .collect()
}

fn line_bytes(entries: &[LogEntry]) -> u64 {
    entries.iter().map(|entry| entry.line.len() as u64).sum()
}

/// `[start, end]` covering the `fraction` of `entries` starting at `at`.
fn range_of(entries: &[LogEntry], at: f64, fraction: f64) -> (i64, i64) {
    let first = (entries.len() as f64 * at) as usize;
    let last = first + ((entries.len() as f64 * fraction) as usize).max(1) - 1;
    (entries[first].timestamp_ns, entries[last].timestamp_ns)
}

fn block_codec(c: &mut Criterion) {
    let block_rows = corpus(1, ROWS_PER_BLOCK);
    let large_rows = corpus(2, LARGE_BLOCK_ROWS);
    let block = encode_block(&block_rows).unwrap();
    let large = encode_block(&large_rows).unwrap();
    eprintln!(
        "block sizes: {ROWS_PER_BLOCK} rows {} line bytes -> {} encoded ({} meta); {LARGE_BLOCK_ROWS} rows {} line bytes -> {} encoded ({} meta)",
        line_bytes(&block_rows),
        block.len(),
        block.meta_len(),
        line_bytes(&large_rows),
        large.len(),
        large.meta_len()
    );

    let mut group = c.benchmark_group("block");
    for (name, rows, encoded) in [("256", &block_rows, &block), ("1024", &large_rows, &large)] {
        group.throughput(Throughput::Bytes(line_bytes(rows)));
        group.bench_with_input(BenchmarkId::new("encode", name), rows, |b, rows| {
            b.iter(|| encode_block(black_box(rows)).unwrap());
        });
        group.bench_with_input(BenchmarkId::new("decode", name), encoded, |b, encoded| {
            b.iter(|| decode_block(black_box(encoded)).unwrap());
        });
        group.bench_with_input(BenchmarkId::new("samples", name), encoded, |b, encoded| {
            b.iter(|| decode_samples(black_box(encoded)).unwrap());
        });
        let blocks = std::slice::from_ref(encoded);
        group.bench_with_input(BenchmarkId::new("read_full", name), blocks, |b, blocks| {
            b.iter(|| run_entries(black_box(blocks), i64::MIN, i64::MAX).unwrap());
        });
        let (start, end) = range_of(rows, 0.45, 0.1);
        group.bench_with_input(BenchmarkId::new("read_10pct", name), blocks, |b, blocks| {
            b.iter(|| run_entries(black_box(blocks), start, end).unwrap());
        });
    }
    group.finish();

    let run_rows = corpus(3, ROWS_PER_BLOCK * RUN_BLOCKS);
    let run: Vec<EncodedBlock> = run_rows
        .chunks(ROWS_PER_BLOCK)
        .map(|chunk| encode_block(chunk).unwrap())
        .collect();
    let mut group = c.benchmark_group("run");
    group.throughput(Throughput::Bytes(line_bytes(&run_rows)));
    group.bench_function("read_full/16x256", |b| {
        b.iter(|| run_entries(black_box(&run), i64::MIN, i64::MAX).unwrap());
    });
    // Straddles a block boundary, so two blocks overlap only partially.
    let (start, end) = range_of(&run_rows, 0.47, 0.1);
    group.bench_function("read_10pct/16x256", |b| {
        b.iter(|| run_entries(black_box(&run), start, end).unwrap());
    });
    group.finish();

    let lines: Vec<String> = corpus(4, 4096)
        .into_iter()
        .map(|entry| entry.line)
        .collect();
    let mut group = c.benchmark_group("line_filter");
    group.throughput(Throughput::Bytes(
        lines.iter().map(|line| line.len() as u64).sum(),
    ));
    for (name, op, needle) in [
        ("contains_hit", LineFilterOp::Contains, "reconcileID"),
        ("contains_rare", LineFilterOp::Contains, "status=500"),
        (
            "contains_miss",
            LineFilterOp::Contains,
            "connection refused by peer",
        ),
        ("not_contains", LineFilterOp::NotContains, "healthz"),
    ] {
        let filter = LineFilter {
            branches: vec![LineFilterBranch {
                op,
                term: LineFilterTerm::String(needle.to_owned()),
            }],
        };
        group.bench_function(name, |b| {
            b.iter(|| count_line_matches(&filter, lines.iter().map(String::as_str)).unwrap());
        });
    }
    group.finish();
}

fn compressors(c: &mut Criterion) {
    let rows = corpus(1, ROWS_PER_BLOCK);
    let buffer: Vec<u8> = rows
        .iter()
        .flat_map(|entry| entry.line.as_bytes())
        .copied()
        .collect();
    let snappy = snap::raw::Encoder::new().compress_vec(&buffer).unwrap();
    let lz4 = lz4_flex::block::compress(&buffer);
    let zstd1 = zstd::bulk::compress(&buffer, 1).unwrap();
    let zstd3 = zstd::bulk::compress(&buffer, 3).unwrap();
    for (name, compressed) in [
        ("snappy", &snappy),
        ("lz4", &lz4),
        ("zstd-1", &zstd1),
        ("zstd-3", &zstd3),
    ] {
        eprintln!(
            "line buffer {} bytes, {name}: {} bytes (ratio {:.2})",
            buffer.len(),
            compressed.len(),
            buffer.len() as f64 / compressed.len() as f64
        );
    }

    let mut group = c.benchmark_group("compressor");
    group.throughput(Throughput::Bytes(buffer.len() as u64));
    let mut output = vec![0; buffer.len()];
    group.bench_function("compress/snappy", |b| {
        let mut encoder = snap::raw::Encoder::new();
        b.iter(|| encoder.compress_vec(black_box(&buffer)).unwrap());
    });
    group.bench_function("compress/lz4", |b| {
        b.iter(|| lz4_flex::block::compress(black_box(&buffer)));
    });
    for level in [1, 3] {
        group.bench_function(format!("compress/zstd-{level}"), |b| {
            let mut compressor = zstd::bulk::Compressor::new(level).unwrap();
            b.iter(|| compressor.compress(black_box(&buffer)).unwrap());
        });
    }
    group.bench_function("decompress/snappy", |b| {
        let mut decoder = snap::raw::Decoder::new();
        b.iter(|| decoder.decompress(black_box(&snappy), &mut output).unwrap());
    });
    group.bench_function("decompress/lz4", |b| {
        b.iter(|| lz4_flex::block::decompress_into(black_box(&lz4), &mut output).unwrap());
    });
    for (level, compressed) in [(1, &zstd1), (3, &zstd3)] {
        group.bench_function(format!("decompress/zstd-{level}"), |b| {
            let mut decompressor = zstd::bulk::Decompressor::new().unwrap();
            let mut output = Vec::with_capacity(buffer.len());
            b.iter(|| {
                output.clear();
                decompressor
                    .decompress_to_buffer(black_box(compressed.as_slice()), &mut output)
                    .unwrap()
            });
        });
    }
    group.finish();
}

criterion_group!(benches, block_codec, compressors);
criterion_main!(benches);
