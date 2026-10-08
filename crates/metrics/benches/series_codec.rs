//! Series values as the write and merge paths see them: operand sizes and
//! encode timings for the few-sample values writers emit, and merge
//! operator / read-path
//! decode timings for one key's entries as SlateDB hands them over
//! (oldest first, `existing` unset).
//!
//! Timings are the best of `$SERIES_CODEC_REPS` (default 15) passes over
//! 256 keys (four value shapes) and are reported per key.

use std::fmt::Write as _;
use std::hint::black_box;
use std::time::Instant;

use bytes::Bytes;
use plural_metrics::Sample;
use plural_metrics::bench_support::{decode_series_columns, encode_series, merge_series};

const BASE_TIMESTAMP_MS: i64 = 1_790_000_000_000;
const SCRAPE_INTERVAL_MS: i64 = 15_000;
const SERIES_PER_SHAPE: usize = 64;
const HOUR: usize = 240;
const OPERAND_COUNTS: [usize; 7] = [1, 2, 4, 8, 16, 32, 64];
const OPERAND_SIZES: [usize; 8] = [1, 2, 3, 4, 6, 8, 12, 16];

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
enum Shape {
    Counter,
    Gauge2dp,
    RandomWalk,
    Constant,
}

impl Shape {
    const ALL: [Shape; 4] = [
        Shape::Counter,
        Shape::Gauge2dp,
        Shape::RandomWalk,
        Shape::Constant,
    ];

    fn name(self) -> &'static str {
        match self {
            Shape::Counter => "counter",
            Shape::Gauge2dp => "gauge_2dp",
            Shape::RandomWalk => "random_walk",
            Shape::Constant => "constant",
        }
    }
}

/// A 15s scrape at a per-series phase, landing a few ms late.
fn series(shape: Shape, len: usize, seed: u64) -> Vec<Sample> {
    let mut rng = Rng(seed | 1);
    let phase = rng.below(SCRAPE_INTERVAL_MS as u64) as i64;
    let mut counter = rng.below(1_000_000) as f64;
    let mut level = 500.0;
    (0..len as i64)
        .map(|i| {
            let timestamp_ms =
                BASE_TIMESTAMP_MS + phase + i * SCRAPE_INTERVAL_MS + rng.below(6) as i64;
            let value = match shape {
                Shape::Counter => {
                    counter += rng.below(40) as f64;
                    counter
                }
                Shape::Gauge2dp => {
                    level += (rng.unit() - 0.5) * 4.0;
                    (level * 100.0f64).round() / 100.0
                }
                Shape::RandomWalk => {
                    level += (rng.unit() - 0.5) * 0.37;
                    level
                }
                Shape::Constant => 1.0,
            };
            Sample {
                timestamp_ms,
                value,
            }
        })
        .collect()
}

/// `SERIES_PER_SHAPE` series of `len` samples per shape.
fn corpus(len: usize) -> Vec<(Shape, Vec<Sample>)> {
    Shape::ALL
        .into_iter()
        .flat_map(|shape| {
            (0..SERIES_PER_SHAPE).map(move |s| {
                let seed = 0x9e37_79b9_7f4a_7c15 ^ ((shape as u64) << 32 | s as u64);
                (shape, series(shape, len, seed))
            })
        })
        .collect()
}

/// Per shape and operand size, bytes per value and the best-of-`reps` ns
/// to encode one.
fn operand_tables(reps: usize, out: &mut String) {
    let header = |out: &mut String, title: &str| {
        let _ = write!(out, "\n### {title}\n\n| shape |");
        for n in OPERAND_SIZES {
            let _ = write!(out, " {n} |");
        }
        out.push_str("\n|---|");
        out.push_str(&"---|".repeat(OPERAND_SIZES.len()));
        out.push('\n');
    };
    let corpus = corpus(16 * 8);
    let mut sizes = String::new();
    let mut times = String::new();
    header(
        &mut sizes,
        "Operand size (bytes per value, mean over 256 series x 8 windows)",
    );
    header(&mut times, "Operand encode (ns per value, best of N)");
    for shape in Shape::ALL {
        let _ = write!(sizes, "| {} |", shape.name());
        let _ = write!(times, "| {} |", shape.name());
        let series: Vec<&Vec<Sample>> = corpus
            .iter()
            .filter(|(s, _)| s.name() == shape.name())
            .map(|(_, samples)| samples)
            .collect();
        for n in OPERAND_SIZES {
            let windows: Vec<&[Sample]> = series
                .iter()
                .flat_map(|samples| samples.chunks(16).map(move |c| &c[..n]))
                .collect();
            let bytes: usize = windows.iter().map(|w| encode_series(w).len()).sum();
            let ns = best(reps, windows.len(), || {
                for window in &windows {
                    black_box(encode_series(black_box(window)));
                }
            });
            let _ = write!(sizes, " {:.1} |", bytes as f64 / windows.len() as f64);
            let _ = write!(times, " {ns:.0} |");
        }
        sizes.push('\n');
        times.push('\n');
    }
    out.push_str(&sizes);
    out.push_str(&times);
}

/// One key's merge operator call: SlateDB's entries, oldest first.
struct Case {
    name: String,
    keys: Vec<Vec<Bytes>>,
}

fn operands(samples: &[Sample]) -> Vec<Bytes> {
    samples
        .iter()
        .map(|s| encode_series(std::slice::from_ref(s)))
        .collect()
}

fn cases() -> Vec<Case> {
    let corpus = corpus(HOUR + 64);
    let mut cases = Vec::new();
    for k in OPERAND_COUNTS {
        cases.push(Case {
            name: format!("{k} operands"),
            keys: corpus.iter().map(|(_, s)| operands(&s[..k])).collect(),
        });
    }
    for k in OPERAND_COUNTS {
        cases.push(Case {
            name: format!("hour value + {k} operands"),
            keys: corpus
                .iter()
                .map(|(_, s)| {
                    let mut entries = vec![encode_series(&s[..HOUR])];
                    entries.extend(operands(&s[HOUR..HOUR + k]));
                    entries
                })
                .collect(),
        });
    }
    cases.push(Case {
        name: "4 x 60-sample values".to_string(),
        keys: corpus
            .iter()
            .map(|(_, s)| s[..HOUR].chunks(60).map(encode_series).collect())
            .collect(),
    });
    cases.push(Case {
        name: "4 x 60-sample values + 20 operands".to_string(),
        keys: corpus
            .iter()
            .map(|(_, s)| {
                let mut entries = vec![encode_series(&s[..60])];
                entries.extend(s[60..HOUR].chunks(60).map(encode_series));
                entries.extend(operands(&s[HOUR..HOUR + 20]));
                entries
            })
            .collect(),
    });
    cases.push(Case {
        name: "hour value + rewrite of last 24".to_string(),
        keys: corpus
            .iter()
            .map(|(_, s)| {
                let rewrite: Vec<Sample> = s[HOUR - 24..HOUR]
                    .iter()
                    .map(|p| Sample {
                        timestamp_ms: p.timestamp_ms,
                        value: p.value + 1.0,
                    })
                    .collect();
                vec![encode_series(&s[..HOUR]), encode_series(&rewrite)]
            })
            .collect(),
    });
    cases
}

/// Best wall time in ns per key of `reps` passes of `run`.
fn best(reps: usize, keys: usize, mut run: impl FnMut()) -> f64 {
    run();
    let mut best = f64::MAX;
    for _ in 0..reps {
        let start = Instant::now();
        run();
        best = best.min(start.elapsed().as_nanos() as f64);
    }
    best / keys as f64
}

fn merge_table(reps: usize, out: &mut String) {
    out.push_str(
        "\n### Merge operator (ns per key, best of N)\n\n\
         | entries | merged B | merge | merge + decode |\n|---|---|---|---|\n",
    );
    let (mut ts, mut vs) = (Vec::with_capacity(512), Vec::with_capacity(512));
    for case in cases() {
        let merged: Vec<Bytes> = case.keys.iter().map(|k| merge_series(None, k)).collect();
        let merged_bytes =
            merged.iter().map(Bytes::len).sum::<usize>() as f64 / merged.len() as f64;
        let merge = best(reps, case.keys.len(), || {
            for entries in &case.keys {
                black_box(merge_series(None, black_box(entries)));
            }
        });
        let read = best(reps, case.keys.len(), || {
            for entries in &case.keys {
                let value = merge_series(None, black_box(entries));
                ts.clear();
                vs.clear();
                decode_series_columns(&value, i64::MIN, i64::MAX, &mut ts, &mut vs);
                black_box((&ts, &vs));
            }
        });
        let _ = writeln!(
            out,
            "| {} | {merged_bytes:.1} | {merge:.0} | {read:.0} |",
            case.name
        );
    }
}

fn decode_table(reps: usize, out: &mut String) {
    out.push_str(
        "\n### Read-path decode of an hour value (ns per key, best of N)\n\n\
         | range | ns |\n|---|---|\n",
    );
    let corpus = corpus(HOUR);
    let values: Vec<(Bytes, i64)> = corpus
        .iter()
        .map(|(_, s)| (encode_series(s), s[HOUR - 21].timestamp_ms))
        .collect();
    let (mut ts, mut vs) = (Vec::with_capacity(512), Vec::with_capacity(512));
    let mut run = |tail: bool| {
        best(reps, values.len(), || {
            for (value, tail_start) in &values {
                ts.clear();
                vs.clear();
                let start = if tail { *tail_start } else { i64::MIN };
                decode_series_columns(black_box(value), start, i64::MAX, &mut ts, &mut vs);
                black_box((&ts, &vs));
            }
        })
    };
    let full = run(false);
    let tail = run(true);
    let _ = writeln!(
        out,
        "| all 240 | {full:.0} |\n| last 20 (5m lookback) | {tail:.0} |"
    );
}

fn main() {
    let reps: usize = std::env::var("SERIES_CODEC_REPS")
        .ok()
        .and_then(|r| r.parse().ok())
        .unwrap_or(15);
    let mut out = String::new();
    let _ = writeln!(
        out,
        "## series_codec (arch {}, best of {reps})",
        std::env::consts::ARCH
    );
    operand_tables(reps, &mut out);
    merge_table(reps, &mut out);
    decode_table(reps, &mut out);
    print!("{out}");
}
