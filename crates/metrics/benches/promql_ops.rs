use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use plural_metrics::bench_support::{
    Arith, BinaryKind, BinaryPath, Compare, InstantFn, Tile, binary_vector_scalar,
    binary_vector_vector, instant_fn,
};

const STEPS: usize = 64;
const SERIES: usize = 512;

/// xorshift64*, so every run benchmarks the same corpus.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// A full tile of gauge-like values, `valid_pct` percent of cells present.
fn tile(seed: u64, valid_pct: u64) -> Tile {
    let mut rng = Rng(seed);
    let cells = STEPS * SERIES;
    let values = (0..cells).map(|_| rng.unit() * 1000.0 - 50.0).collect();
    let validity: Vec<bool> = (0..cells).map(|_| rng.next() % 100 < valid_pct).collect();
    Tile::new(STEPS, SERIES, values, &validity)
}

fn validities() -> [(&'static str, u64); 2] {
    [("valid95", 95), ("valid100", 100)]
}

fn bench_instant_fns(c: &mut Criterion) {
    let mut group = c.benchmark_group("promql/instant_fn");
    group.throughput(Throughput::Elements((STEPS * SERIES) as u64));
    let kinds = [
        ("abs", InstantFn::Abs),
        ("ceil", InstantFn::Ceil),
        ("sqrt", InstantFn::Sqrt),
        (
            "clamp",
            InstantFn::Clamp {
                min: 10.0,
                max: 500.0,
            },
        ),
        ("ln", InstantFn::Ln),
        ("timestamp", InstantFn::Timestamp),
    ];
    for (validity, pct) in validities() {
        let input = tile(0x1234_5678 ^ pct, pct);
        for (name, kind) in kinds {
            group.bench_function(BenchmarkId::new(name, validity), |b| {
                b.iter_batched(
                    || input.clone(),
                    |t| black_box(instant_fn(kind, t)),
                    BatchSize::LargeInput,
                )
            });
        }
    }
    group.finish();
}

fn binary_ops() -> [(&'static str, BinaryKind); 6] {
    [
        ("add", Arith::Add.into()),
        ("mul", Arith::Mul.into()),
        ("div", Arith::Div.into()),
        ("gt", Compare::Gt.into()),
        ("gt_bool", Compare::GtBool.into()),
        ("ne", Compare::Ne.into()),
    ]
}

fn paths() -> [(&'static str, BinaryPath); 2] {
    [("generic", BinaryPath::Generic), ("fast", BinaryPath::Fast)]
}

fn bench_binary_vector_vector(c: &mut Criterion) {
    let mut group = c.benchmark_group("promql/binary_vv");
    group.throughput(Throughput::Elements((STEPS * SERIES) as u64));
    // A fixed shuffle: the two sides' rosters are ordered independently.
    let mut rng = Rng(0xfeed);
    let mut one_to_one: Vec<Option<u32>> = (0..SERIES as u32).map(Some).collect();
    for i in (1..one_to_one.len()).rev() {
        one_to_one.swap(i, (rng.next() % (i as u64 + 1)) as usize);
    }
    // Eight LHS series per RHS series, e.g. per-pod series against a
    // per-node one.
    let group_left: Vec<Option<u32>> = (0..SERIES)
        .map(|_| Some((rng.next() % (SERIES as u64 / 8)) as u32))
        .collect();
    let matchings = [
        ("one_to_one", &one_to_one, false),
        ("group_left", &group_left, true),
    ];
    for (validity, pct) in validities() {
        let lhs = tile(0xaaaa ^ pct, pct);
        let rhs = tile(0xbbbb ^ pct, pct);
        for (name, op) in binary_ops() {
            for (matching, map, is_group_left) in matchings {
                for (path_name, path) in paths() {
                    let id = BenchmarkId::new(format!("{name}/{matching}/{path_name}"), validity);
                    group.bench_function(id, |b| {
                        b.iter_batched(
                            || (lhs.clone(), rhs.clone()),
                            |(l, r)| {
                                black_box(binary_vector_vector(op, l, r, map, is_group_left, path))
                            },
                            BatchSize::LargeInput,
                        )
                    });
                }
            }
        }
    }
    group.finish();
}

fn bench_binary_vector_scalar(c: &mut Criterion) {
    let mut group = c.benchmark_group("promql/binary_vs");
    group.throughput(Throughput::Elements((STEPS * SERIES) as u64));
    for (validity, pct) in validities() {
        let lhs = tile(0xcccc ^ pct, pct);
        for (name, op) in binary_ops() {
            // Mid-range, so a filtering comparison keeps about half the cells.
            let scalar = if name.starts_with("gt") {
                450.0
            } else {
                1024.0
            };
            for (path_name, path) in paths() {
                let id = BenchmarkId::new(format!("{name}/{path_name}"), validity);
                group.bench_function(id, |b| {
                    b.iter_batched(
                        || lhs.clone(),
                        |l| black_box(binary_vector_scalar(op, l, scalar, path)),
                        BatchSize::LargeInput,
                    )
                });
            }
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_instant_fns,
    bench_binary_vector_vector,
    bench_binary_vector_scalar
);
criterion_main!(benches);
