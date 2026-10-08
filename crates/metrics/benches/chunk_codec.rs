//! The production chunk codec (`bench_support::chunk`) on hourly chunks:
//! sizes, scheme mix, SlateDB SST bytes of stored values, and encode,
//! decode, ranged decode and decode + re-encode timings.
//!
//! Real data is newline-delimited JSON (`{"labels": {...}, "samples":
//! [[ts_ms, "value"], ...]}`) at `$COLFMT_DATA`, default
//! `/tmp/colfmt-data/series.jsonl`, and is skipped when absent; its 30s and
//! 60s variants keep every 2nd / 4th sample. Timings are the best of
//! `$CHUNK_CODEC_REPS` (default 15) passes over a whole dataset, since
//! criterion swings on a loaded machine; `CHUNK_CODEC_CRITERION=1` also
//! runs a few criterion cases.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput};
use futures::TryStreamExt;
use plural_metrics::Sample;
use plural_metrics::bench_support::chunk::{self, Layout, Options, TimestampScheme, ValueScheme};
use plural_metrics::bench_support::{decode_series_columns, encode_series};
use slatedb::DbBuilder;
use slatedb::config::{
    CompressionCodec, FlushOptions, FlushType, PutOptions, Settings, SstBlockSize, Ttl,
    WriteOptions,
};
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;

const BASE_TIMESTAMP_MS: i64 = 1_790_000_000_000;
const SERIES_PER_CLASS: usize = 100;
/// Scrape interval, label, and the samples it puts in an hourly chunk.
const RESOLUTIONS: [(i64, &str, usize); 3] = [
    (15_000, "15s", 240),
    (30_000, "30s", 120),
    (60_000, "60s", 60),
];
const DEFAULT_DATA: &str = "/tmp/colfmt-data/series.jsonl";
/// Prometheus' `value.NormalNaN`, what a scraped `NaN` is stored as.
const NORMAL_NAN: u64 = 0x7ff8_0000_0000_0001;

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

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[derive(Clone, Copy)]
enum Synthetic {
    CounterInt,
    CounterDecimal,
    CounterFloatSum,
    BytesGauge,
    DecimalGauge,
    IntGauge,
    SmoothFloat,
    Ratio,
    Constant,
}

impl Synthetic {
    const ALL: [Self; 9] = [
        Self::CounterInt,
        Self::CounterDecimal,
        Self::CounterFloatSum,
        Self::BytesGauge,
        Self::DecimalGauge,
        Self::IntGauge,
        Self::SmoothFloat,
        Self::Ratio,
        Self::Constant,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::CounterInt => "counter_int",
            Self::CounterDecimal => "counter_2dp",
            Self::CounterFloatSum => "counter_float_sum",
            Self::BytesGauge => "gauge_bytes",
            Self::DecimalGauge => "gauge_3dp",
            Self::IntGauge => "gauge_int",
            Self::SmoothFloat => "gauge_smooth_float",
            Self::Ratio => "gauge_ratio_entropy",
            Self::Constant => "constant",
        }
    }
}

/// A scrape at a per-series offset into the interval, landing a few ms
/// late (now and then up to 15ms).
fn synthetic_series(class: Synthetic, interval_ms: i64, len: usize, seed: u64) -> Vec<Sample> {
    let mut rng = Rng(seed | 1);
    let offset = rng.below(interval_ms as u64) as i64;
    let mut counter = rng.below(1_000_000) as f64;
    let mut units = rng.below(100_000) as i64;
    let mut sum = rng.unit() * 1000.0;
    let phase = rng.unit() * 6.0;
    (0..len as i64)
        .map(|i| {
            let jitter = if rng.below(10) == 0 {
                rng.below(15)
            } else {
                rng.below(4)
            } as i64;
            let reset = rng.below(2000) == 0;
            let value = match class {
                Synthetic::CounterInt => {
                    counter = if reset {
                        0.0
                    } else {
                        counter + rng.below(50) as f64
                    };
                    counter
                }
                Synthetic::CounterDecimal => {
                    units = if reset {
                        0
                    } else {
                        units + rng.below(300) as i64
                    };
                    units as f64 / 100.0
                }
                Synthetic::CounterFloatSum => {
                    sum = if reset { 0.0 } else { sum + rng.unit() * 0.05 };
                    sum
                }
                Synthetic::BytesGauge => {
                    units = (units + rng.below(2001) as i64 - 1000).max(0);
                    (units * 4096 + 1_000_000_000) as f64
                }
                Synthetic::DecimalGauge => {
                    units = (units + rng.below(201) as i64 - 100).max(0);
                    units as f64 / 1000.0
                }
                Synthetic::IntGauge => {
                    units = (units % 500 + rng.below(7) as i64 - 3).max(0);
                    units as f64
                }
                Synthetic::SmoothFloat => 50.0 + 10.0 * (i as f64 * 0.05 + phase).sin(),
                Synthetic::Ratio => rng.unit(),
                Synthetic::Constant => 1.0,
            };
            Sample {
                timestamp_ms: BASE_TIMESTAMP_MS + offset + i * interval_ms + jitter,
                value,
            }
        })
        .collect()
}

struct Chunk {
    class: &'static str,
    samples: Vec<Sample>,
    ts: Vec<i64>,
    vs: Vec<f64>,
    /// The stored series value: format byte and one chunk.
    value: Bytes,
    bytes: Vec<u8>,
    layout: Layout,
    /// Ends the first quarter of the samples.
    head_end: i64,
    /// Precedes the last quarter.
    tail_start: i64,
}

impl Chunk {
    fn new(class: &'static str, samples: Vec<Sample>) -> Self {
        let ts: Vec<i64> = samples.iter().map(|s| s.timestamp_ms).collect();
        let vs: Vec<f64> = samples.iter().map(|s| s.value).collect();
        let n = samples.len();
        let (bytes, layout) = encode(&ts, &vs, Options::default());
        let quarter = (n / 4).max(1);
        Self {
            class,
            head_end: ts[quarter - 1],
            tail_start: if n > quarter {
                ts[n - quarter - 1]
            } else {
                i64::MIN
            },
            value: encode_series(&samples),
            samples,
            ts,
            vs,
            bytes,
            layout,
        }
    }
}

fn encode(ts: &[i64], vs: &[f64], options: Options) -> (Vec<u8>, Layout) {
    let mut out = Vec::new();
    let layout = chunk::encode_chunk(ts, vs, options, &mut out);
    (out, layout)
}

struct Dataset {
    source: &'static str,
    scrape: &'static str,
    chunk_len: usize,
    chunks: Vec<Chunk>,
}

fn synthetic_datasets() -> Vec<Dataset> {
    RESOLUTIONS
        .iter()
        .map(|&(interval_ms, scrape, chunk_len)| {
            let mut chunks = Vec::new();
            for (c, class) in Synthetic::ALL.into_iter().enumerate() {
                for s in 0..SERIES_PER_CLASS {
                    let seed = 0x9e37_79b9_7f4a_7c15 ^ ((c * 1000 + s) as u64).wrapping_mul(31);
                    let samples = synthetic_series(class, interval_ms, chunk_len, seed);
                    chunks.push(Chunk::new(class.name(), samples));
                }
            }
            Dataset {
                source: "synthetic",
                scrape,
                chunk_len,
                chunks,
            }
        })
        .collect()
}

struct RealSeries {
    name: String,
    samples: Vec<Sample>,
}

fn parse_value(text: &str) -> Option<f64> {
    match text {
        "NaN" => Some(f64::from_bits(NORMAL_NAN)),
        "+Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        _ => text.parse().ok(),
    }
}

fn load_real(path: &str) -> Option<Vec<RealSeries>> {
    let text = std::fs::read_to_string(path).ok()?;
    let mut series = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(json) = serde_json::from_str::<serde_json::Value>(line) else {
            eprintln!("skipping unparsable line in {path}");
            continue;
        };
        let name = json["labels"]["__name__"]
            .as_str()
            .unwrap_or("")
            .to_string();
        let Some(raw) = json["samples"].as_array() else {
            continue;
        };
        let mut samples: Vec<Sample> = raw
            .iter()
            .filter_map(|pair| {
                let timestamp_ms = pair[0]
                    .as_i64()
                    .or_else(|| pair[0].as_f64().map(|t| t as i64))?;
                let value = parse_value(pair[1].as_str()?)?;
                Some(Sample {
                    timestamp_ms,
                    value,
                })
            })
            .collect();
        samples.sort_by_key(|s| s.timestamp_ms);
        samples.dedup_by_key(|s| s.timestamp_ms);
        if !samples.is_empty() {
            series.push(RealSeries { name, samples });
        }
    }
    Some(series)
}

fn classify(name: &str, samples: &[Sample]) -> &'static str {
    let first = samples[0].value.to_bits();
    if samples.iter().all(|s| s.value.to_bits() == first) {
        "constant"
    } else if ["_total", "_count", "_bucket", "_sum"]
        .iter()
        .any(|suffix| name.ends_with(suffix))
    {
        "counter"
    } else {
        "gauge"
    }
}

fn real_datasets(series: &[RealSeries]) -> Vec<Dataset> {
    RESOLUTIONS
        .iter()
        .enumerate()
        .map(|(k, &(_, scrape, chunk_len))| {
            let step = 1 << k;
            let mut chunks = Vec::new();
            for s in series {
                let kept: Vec<Sample> = s.samples.iter().step_by(step).cloned().collect();
                for piece in kept.chunks(chunk_len) {
                    chunks.push(Chunk::new(classify(&s.name, piece), piece.to_vec()));
                }
            }
            Dataset {
                source: "real",
                scrape,
                chunk_len,
                chunks,
            }
        })
        .collect()
}

/// Best wall time in ns of `reps` runs of `run`, after one warm-up run.
fn best(reps: usize, mut run: impl FnMut()) -> f64 {
    let mut best = f64::MAX;
    for rep in 0..=reps {
        let start = Instant::now();
        run();
        if rep > 0 {
            best = best.min(start.elapsed().as_nanos() as f64);
        }
    }
    best
}

/// One pass over a dataset.
#[derive(Clone, Copy)]
enum Pass {
    /// `encode_chunk` alone.
    Encode,
    /// The stored value, as writers build it.
    EncodeValue,
    Decode,
    /// The query read path: the stored value through `SeriesData` into
    /// columns.
    DecodeValue,
    Head,
    Tail,
    Reencode,
}

struct Runner<'a> {
    chunks: &'a [&'a Chunk],
    ts: Vec<i64>,
    vs: Vec<f64>,
}

impl<'a> Runner<'a> {
    fn new(chunks: &'a [&'a Chunk]) -> Self {
        Self {
            chunks,
            ts: Vec::with_capacity(256),
            vs: Vec::with_capacity(256),
        }
    }

    fn run(&mut self, pass: Pass) {
        let (ts, vs) = (&mut self.ts, &mut self.vs);
        for chunk in self.chunks {
            ts.clear();
            vs.clear();
            let bytes = black_box(&chunk.bytes[..]);
            match pass {
                Pass::Encode => {
                    black_box(encode(black_box(&chunk.ts), &chunk.vs, Options::default()));
                }
                Pass::EncodeValue => {
                    black_box(encode_series(black_box(&chunk.samples)));
                }
                Pass::Decode => chunk::decode(bytes, ts, vs).expect("decode"),
                Pass::DecodeValue => {
                    decode_series_columns(black_box(&chunk.value), i64::MIN, i64::MAX, ts, vs);
                }
                Pass::Head => {
                    chunk::decode_range(bytes, i64::MIN, chunk.head_end, ts, vs).expect("decode")
                }
                Pass::Tail => {
                    chunk::decode_range(bytes, chunk.tail_start, i64::MAX, ts, vs).expect("decode")
                }
                Pass::Reencode => {
                    chunk::decode(bytes, ts, vs).expect("decode");
                    black_box(encode(ts, vs, Options::default()));
                }
            }
            black_box((&ts, &vs));
        }
    }

    /// ns per stored sample.
    fn time(&mut self, reps: usize, pass: Pass) -> f64 {
        let samples = self.chunks.iter().map(|c| c.samples.len()).sum::<usize>() as f64;
        best(reps, || self.run(pass)) / samples
    }
}

#[derive(Default)]
struct Sizes {
    chunks: usize,
    samples: usize,
    values: usize,
    header: usize,
    ts: usize,
    vs: usize,
    alp_exceptions: usize,
    exceptions: usize,
    fallbacks: usize,
    value_schemes: BTreeMap<String, usize>,
    ts_schemes: BTreeMap<String, usize>,
}

impl Sizes {
    fn of<'a>(chunks: impl IntoIterator<Item = &'a Chunk>) -> Self {
        let mut sizes = Self::default();
        for chunk in chunks {
            let layout = &chunk.layout;
            sizes.chunks += 1;
            sizes.samples += layout.samples;
            sizes.values += chunk.value.len();
            sizes.header += layout.header_bytes;
            sizes.ts += layout.timestamp_bytes;
            sizes.vs += layout.value_bytes;
            sizes.alp_exceptions += layout.alp_exceptions;
            sizes.exceptions += layout.exceptions;
            sizes.fallbacks += usize::from(layout.value_scheme.is_fallback());
            *sizes
                .value_schemes
                .entry(format!("{:?}", layout.value_scheme))
                .or_default() += 1;
            *sizes
                .ts_schemes
                .entry(format!("{:?}", layout.timestamp_scheme))
                .or_default() += 1;
        }
        sizes
    }

    fn per_sample(&self, bytes: usize) -> f64 {
        bytes as f64 / self.samples.max(1) as f64
    }

    fn bits(&self, bytes: usize) -> f64 {
        8.0 * self.per_sample(bytes)
    }

    fn pct(&self, part: usize, whole: usize) -> String {
        format!("{:.1}%", 100.0 * part as f64 / whole.max(1) as f64)
    }

    fn mix(&self, schemes: &BTreeMap<String, usize>) -> String {
        let mut entries: Vec<_> = schemes.iter().collect();
        entries.sort_by(|a, b| b.1.cmp(a.1));
        entries
            .iter()
            .map(|(name, count)| format!("{name} {}", self.pct(**count, self.chunks)))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn label(dataset: &Dataset) -> String {
    format!(
        "{} {} ({})",
        dataset.source, dataset.scrape, dataset.chunk_len
    )
}

fn size_table(datasets: &[Dataset], out: &mut String) {
    out.push_str(
        "\n### Size (stored value bytes/sample; chunk bits/sample per part)\n\n\
         | dataset (chunk) | chunks | samples | value B/s | header bits | ts bits | value bits \
         | ALP exc | stored exc | fallback chunks | value schemes | ts schemes |\n\
         |---|---|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for dataset in datasets {
        let s = Sizes::of(&dataset.chunks);
        let _ = writeln!(
            out,
            "| {} | {} | {} | {:.3} | {:.2} | {:.2} | {:.2} | {} | {} | {} | {} | {} |",
            label(dataset),
            s.chunks,
            s.samples,
            s.per_sample(s.values),
            s.bits(s.header),
            s.bits(s.ts),
            s.bits(s.vs),
            s.pct(s.alp_exceptions, s.samples),
            s.pct(s.exceptions, s.samples),
            s.pct(s.fallbacks, s.chunks),
            s.mix(&s.value_schemes),
            s.mix(&s.ts_schemes),
        );
    }
}

fn class_table(datasets: &[Dataset], reps: usize, out: &mut String) {
    out.push_str(
        "\n### By series class (decode ns/stored sample, best of N)\n\n\
         | dataset (chunk) | class | chunks | value B/s | ALP exc | fallback chunks \
         | value schemes | decode |\n\
         |---|---|---|---|---|---|---|---|\n",
    );
    for dataset in datasets {
        let mut classes: Vec<&str> = dataset.chunks.iter().map(|c| c.class).collect();
        classes.sort_unstable();
        classes.dedup();
        for class in classes {
            let chunks: Vec<&Chunk> = dataset.chunks.iter().filter(|c| c.class == class).collect();
            let s = Sizes::of(chunks.iter().copied());
            let decode = Runner::new(&chunks).time(reps, Pass::Decode);
            let _ = writeln!(
                out,
                "| {} | {} | {} | {:.3} | {} | {} | {} | {:.2} |",
                label(dataset),
                class,
                s.chunks,
                s.per_sample(s.values),
                s.pct(s.alp_exceptions, s.samples),
                s.pct(s.fallbacks, s.chunks),
                s.mix(&s.value_schemes),
                decode,
            );
        }
    }
}

fn speed_table(datasets: &[Dataset], reps: usize, out: &mut String) {
    out.push_str(
        "\n### Speed (ns/stored sample, best of N passes)\n\n\
         | dataset (chunk) | encode | encode value | decode | decode value (read path) \
         | head 25% | tail 25% | decode+re-encode |\n\
         |---|---|---|---|---|---|---|---|\n",
    );
    for dataset in datasets {
        let chunks: Vec<&Chunk> = dataset.chunks.iter().collect();
        let mut runner = Runner::new(&chunks);
        let _ = writeln!(
            out,
            "| {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |",
            label(dataset),
            runner.time(reps, Pass::Encode),
            runner.time(reps, Pass::EncodeValue),
            runner.time(reps, Pass::Decode),
            runner.time(reps, Pass::DecodeValue),
            runner.time(reps, Pass::Head),
            runner.time(reps, Pass::Tail),
            runner.time(reps, Pass::Reencode),
        );
    }
}

/// Timestamp column bits/sample under each forced scheme.
fn timestamp_table(datasets: &[Dataset], out: &mut String) {
    out.push_str(
        "\n### Timestamp column (bits/sample, chunk header excluded)\n\n\
         | dataset (chunk) | varint | grid | delta | delta-of-delta | smallest per chunk |\n\
         |---|---|---|---|---|---|\n",
    );
    for dataset in datasets {
        let s = Sizes::of(&dataset.chunks);
        let forced = |scheme: TimestampScheme| {
            let options = Options {
                timestamps: Some(scheme),
                values: Some(ValueScheme::Xor),
            };
            let bytes: usize = dataset
                .chunks
                .iter()
                .map(|c| encode(&c.ts, &c.vs, options).1.timestamp_bytes)
                .sum();
            s.bits(bytes)
        };
        let _ = writeln!(
            out,
            "| {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |",
            label(dataset),
            forced(TimestampScheme::Varint),
            forced(TimestampScheme::Grid),
            forced(TimestampScheme::Delta),
            forced(TimestampScheme::DeltaOfDelta),
            s.bits(s.ts),
        );
    }
}

fn verify(datasets: &[Dataset]) {
    let (mut ts, mut vs) = (Vec::new(), Vec::new());
    let same = |ts: &[i64], vs: &[f64], chunk: &Chunk| {
        ts == chunk.ts
            && vs.len() == chunk.vs.len()
            && vs
                .iter()
                .zip(&chunk.vs)
                .all(|(a, b)| a.to_bits() == b.to_bits())
    };
    for chunk in datasets.iter().flat_map(|d| &d.chunks) {
        ts.clear();
        vs.clear();
        chunk::decode(&chunk.bytes, &mut ts, &mut vs).expect("decode");
        assert!(same(&ts, &vs, chunk));
        ts.clear();
        vs.clear();
        decode_series_columns(&chunk.value, i64::MIN, i64::MAX, &mut ts, &mut vs);
        assert!(same(&ts, &vs, chunk));
    }
}

/// Bytes of the L0 SSTs (WAL SSTs excluded) after writing one `(key, value)`
/// per chunk into a fresh SlateDB (4 KiB blocks, 30-day TTL) and flushing
/// the memtable.
async fn sst_bytes(values: &[&[u8]], codec: Option<CompressionCodec>) -> u64 {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let settings = Settings {
        compression_codec: codec,
        ..Settings::default()
    };
    let db = DbBuilder::new("/sst", object_store.clone())
        .with_settings(settings)
        .with_sst_block_size(SstBlockSize::Block4Kib)
        .build()
        .await
        .unwrap();
    let put = PutOptions {
        ttl: Ttl::ExpireAfter(30 * 24 * 3_600_000),
    };
    let write = WriteOptions {
        await_durable: false,
        ..WriteOptions::default()
    };
    for (series_id, value) in values.iter().enumerate() {
        let mut key = Vec::with_capacity(20);
        key.extend_from_slice(&[0x02, 0x01]);
        key.extend_from_slice(b"default\0");
        key.extend_from_slice(&497_541u32.to_be_bytes());
        key.extend_from_slice(&[0x01, 0x05]);
        key.extend_from_slice(&(series_id as u32).to_be_bytes());
        db.put_with_options(&key, value, &put, &write)
            .await
            .unwrap();
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    db.close().await.unwrap();
    let objects: Vec<_> = object_store.list(None).try_collect().await.unwrap();
    objects
        .iter()
        .filter(|meta| {
            let path = meta.location.as_ref();
            path.ends_with(".sst") && path.contains("compacted")
        })
        .map(|meta| meta.size)
        .sum()
}

fn sst_table(datasets: &[Dataset], out: &mut String) {
    out.push_str(
        "\n### SlateDB SST bytes of stored values (4 KiB blocks, one key per chunk, TTL set)\n\n\
         | dataset (chunk) | order | raw value B | SST none B | SST zstd B \
         | SST none / raw | zstd / none |\n\
         |---|---|---|---|---|---|---|\n",
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let controls: Vec<Dataset> = datasets
        .iter()
        .filter(|dataset| dataset.source == "real")
        .flat_map(|dataset| {
            let changing = dataset
                .chunks
                .iter()
                .filter(|chunk| chunk.class != "constant")
                .map(|chunk| Chunk::new(chunk.class, chunk.samples.clone()))
                .collect();
            let mut rng = Rng(0x0dd_ba11);
            let mut rejitter = |chunk: &Chunk| {
                let samples = chunk
                    .samples
                    .iter()
                    .map(|s| Sample {
                        timestamp_ms: s.timestamp_ms + rng.below(5) as i64 - 2,
                        value: s.value,
                    })
                    .collect();
                Chunk::new(chunk.class, samples)
            };
            let rejittered = dataset.chunks.iter().map(&mut rejitter).collect();
            let changing_rejittered = dataset
                .chunks
                .iter()
                .filter(|chunk| chunk.class != "constant")
                .map(&mut rejitter)
                .collect();
            [
                Dataset {
                    source: "real, non-constant, per-series jitter",
                    scrape: dataset.scrape,
                    chunk_len: dataset.chunk_len,
                    chunks: changing_rejittered,
                },
                Dataset {
                    source: "real, non-constant",
                    scrape: dataset.scrape,
                    chunk_len: dataset.chunk_len,
                    chunks: changing,
                },
                Dataset {
                    source: "real, per-series jitter",
                    scrape: dataset.scrape,
                    chunk_len: dataset.chunk_len,
                    chunks: rejittered,
                },
            ]
        })
        .collect();
    for dataset in datasets.iter().chain(&controls) {
        let mut shuffled: Vec<usize> = (0..dataset.chunks.len()).collect();
        let mut rng = Rng(0x5eed_5eed_5eed_5eed);
        for i in (1..shuffled.len()).rev() {
            shuffled.swap(i, rng.below(i as u64 + 1) as usize);
        }
        let in_order: Vec<usize> = (0..dataset.chunks.len()).collect();
        for (order, indices) in [("label", &in_order), ("shuffled", &shuffled)] {
            let values: Vec<&[u8]> = indices
                .iter()
                .map(|&i| &dataset.chunks[i].value[..])
                .collect();
            let raw: u64 = values.iter().map(|v| v.len() as u64).sum();
            let none = runtime.block_on(sst_bytes(&values, None));
            let zstd = runtime.block_on(sst_bytes(&values, Some(CompressionCodec::Zstd)));
            let _ = writeln!(
                out,
                "| {} | {order} | {raw} | {none} | {zstd} | {:.3} | {:.3} |",
                label(dataset),
                none as f64 / raw as f64,
                zstd as f64 / none as f64,
            );
        }
    }
}

fn criterion_cases(datasets: &[Dataset]) {
    let mut c = Criterion::default().configure_from_args();
    let mut group = c.benchmark_group("chunk_codec/decode");
    for dataset in datasets.iter().filter(|d| d.chunk_len == 240) {
        let mut seen = Vec::new();
        for chunk in &dataset.chunks {
            if seen.contains(&chunk.class) {
                continue;
            }
            seen.push(chunk.class);
            let name = format!("{}/{}", dataset.source, chunk.class);
            group.throughput(Throughput::Elements(chunk.samples.len() as u64));
            group.bench_function(BenchmarkId::new("chunk", &name), |b| {
                let (mut ts, mut vs) = (Vec::new(), Vec::new());
                b.iter(|| {
                    ts.clear();
                    vs.clear();
                    chunk::decode(black_box(&chunk.bytes), &mut ts, &mut vs).expect("decode");
                })
            });
            group.bench_function(BenchmarkId::new("value", &name), |b| {
                let (mut ts, mut vs) = (Vec::new(), Vec::new());
                b.iter(|| {
                    ts.clear();
                    vs.clear();
                    let value = black_box(&chunk.value);
                    decode_series_columns(value, i64::MIN, i64::MAX, &mut ts, &mut vs);
                })
            });
        }
    }
    group.finish();
    c.final_summary();
}

fn main() {
    let reps: usize = std::env::var("CHUNK_CODEC_REPS")
        .ok()
        .and_then(|r| r.parse().ok())
        .unwrap_or(15);
    let path = std::env::var("COLFMT_DATA").unwrap_or_else(|_| DEFAULT_DATA.to_string());
    let mut datasets = synthetic_datasets();
    match load_real(&path) {
        Some(series) => {
            println!("real data: {} series from {path}", series.len());
            datasets.extend(real_datasets(&series));
        }
        None => println!("real data: {path} not readable, skipping"),
    }
    verify(&datasets);

    let mut out = String::new();
    let _ = writeln!(
        out,
        "## chunk_codec (arch {}, best of {reps})",
        std::env::consts::ARCH
    );
    size_table(&datasets, &mut out);
    sst_table(&datasets, &mut out);
    timestamp_table(&datasets, &mut out);
    print!("{out}");
    out.clear();
    speed_table(&datasets, reps, &mut out);
    print!("{out}");
    out.clear();
    class_table(&datasets, reps, &mut out);
    print!("{out}");

    if std::env::var("CHUNK_CODEC_CRITERION").is_ok_and(|v| v == "1") {
        criterion_cases(&datasets);
    }
}
