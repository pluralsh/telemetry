//! Criterion microbenchmark for 1024-dimensional L2 distance and dot product.
//!
//! Run with:
//!   cargo bench -p plural-vector --features bench-internals --bench l2_distance

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use rayon::prelude::*;
use std::hint::black_box;
use vector::math::distance::{
    bench_dot_product_naive, bench_dot_product_runtime, bench_l2_distance_naive,
    bench_l2_distance_runtime, bench_l2_distance_scalar,
};

const DIMENSIONS: usize = 1024;
const DATASET_SIZE: usize = 10_000;

type Kernel = fn(&[f32], &[f32]) -> f32;

fn make_vector(multiplier: f32, offset: f32) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|i| ((i % 97) as f32 * multiplier) + offset)
        .collect()
}

fn make_random_vector(seed: u64) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..DIMENSIONS).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

fn make_random_dataset(seed: u64) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut dataset = Vec::with_capacity(DATASET_SIZE * DIMENSIONS);
    for _ in 0..(DATASET_SIZE * DIMENSIONS) {
        dataset.push(rng.gen_range(-1.0..1.0));
    }
    dataset
}

fn score_dataset(query: &[f32], dataset: &[f32], kernel: Kernel) -> f32 {
    dataset
        .as_chunks::<DIMENSIONS>()
        .0
        .iter()
        .map(|candidate| kernel(query, candidate))
        .sum()
}

fn score_dataset_rayon(query: &[f32], dataset: &[f32], kernel: Kernel) -> f32 {
    dataset
        .par_chunks_exact(DIMENSIONS)
        .map(|candidate| kernel(query, candidate))
        .sum()
}

const L2_KERNELS: [(&str, Kernel); 3] = [
    ("naive", bench_l2_distance_naive),
    ("portable", bench_l2_distance_scalar),
    ("runtime", bench_l2_distance_runtime),
];

const DOT_KERNELS: [(&str, Kernel); 2] = [
    ("naive", bench_dot_product_naive),
    ("runtime", bench_dot_product_runtime),
];

fn bench_single_pair(c: &mut Criterion) {
    // given
    let a = make_vector(0.03125, -2.0);
    let b = make_vector(0.046875, 1.5);

    for (group_name, kernels) in [
        ("distance/l2/1024d", &L2_KERNELS[..]),
        ("distance/dot/1024d", &DOT_KERNELS[..]),
    ] {
        let mut group = c.benchmark_group(group_name);
        group.throughput(Throughput::Elements(DIMENSIONS as u64));
        for &(name, kernel) in kernels {
            group.bench_with_input(
                BenchmarkId::from_parameter(name),
                &(&a, &b),
                |bench, (lhs, rhs)| {
                    bench.iter(|| black_box(kernel(black_box(lhs), black_box(rhs))))
                },
            );
        }
        group.finish();
    }
}

fn bench_l2_distance_dataset(c: &mut Criterion) {
    // given
    let query = make_random_vector(42);
    let dataset = make_random_dataset(7);

    let mut group = c.benchmark_group("distance/l2/1024d_dataset_10k");
    group.throughput(Throughput::Elements(DATASET_SIZE as u64));

    for (name, kernel) in L2_KERNELS {
        group.bench_with_input(
            BenchmarkId::new("serial", name),
            &(&query, &dataset),
            |bench, (query, dataset)| {
                bench
                    .iter(|| black_box(score_dataset(black_box(query), black_box(dataset), kernel)))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("rayon", name),
            &(&query, &dataset),
            |bench, (query, dataset)| {
                bench.iter(|| {
                    black_box(score_dataset_rayon(
                        black_box(query),
                        black_box(dataset),
                        kernel,
                    ))
                })
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = bench_single_pair, bench_l2_distance_dataset
}
criterion_main!(benches);
