use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use plural_metrics::bench_support::{Aggregate, Tile, aggregate, aggregate_reference};

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

/// A full tile of gauge-like values, 95% of cells present.
fn tile(seed: u64) -> Tile {
    let mut rng = Rng(seed);
    let cells = STEPS * SERIES;
    let values = (0..cells).map(|_| rng.unit() * 1000.0 - 50.0).collect();
    let validity: Vec<bool> = (0..cells).map(|_| rng.next() % 100 < 95).collect();
    Tile::new(STEPS, SERIES, values, &validity)
}

fn bench_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("promql/aggregate");
    group.throughput(Throughput::Elements((STEPS * SERIES) as u64));
    let kinds = [
        ("sum", Aggregate::Sum),
        ("avg", Aggregate::Avg),
        ("max", Aggregate::Max),
        ("count", Aggregate::Count),
        ("stddev", Aggregate::Stddev),
    ];
    let input = tile(0x5eed);
    for group_count in [1, 16, SERIES] {
        let groups: Vec<Option<u32>> = (0..SERIES)
            .map(|i| Some((i % group_count) as u32))
            .collect();
        for (name, kind) in kinds {
            let param = format!("groups{group_count}");
            group.bench_function(BenchmarkId::new(format!("{name}/old"), &param), |b| {
                b.iter_batched(
                    || input.clone(),
                    |t| black_box(aggregate_reference(kind, t, &groups, group_count)),
                    BatchSize::LargeInput,
                )
            });
            group.bench_function(BenchmarkId::new(format!("{name}/new"), &param), |b| {
                b.iter_batched(
                    || input.clone(),
                    |t| black_box(aggregate(kind, t, &groups, group_count)),
                    BatchSize::LargeInput,
                )
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_aggregate);
criterion_main!(benches);
