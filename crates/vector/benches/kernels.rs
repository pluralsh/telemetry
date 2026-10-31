//! Criterion microbenchmarks for the BM25 batch scorer and the vector row
//! codec.
//!
//! Run with:
//!   cargo bench -p plural-vector --features bench-internals --bench kernels

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::hint::black_box;
use vector::math::BenchScoreContext;
use vector::serde::vector_data::{Field, VectorDataValue};

const POSTING_BLOCK: usize = 128;
const DIMENSIONS: usize = 1536;

fn bench_score_hits(c: &mut Criterion) {
    // given
    let mut rng = StdRng::seed_from_u64(3);
    let freqs: Vec<u32> = (0..POSTING_BLOCK).map(|_| rng.gen_range(1..20)).collect();
    let norms: Vec<u8> = (0..POSTING_BLOCK).map(|_| rng.r#gen()).collect();
    let ctx = BenchScoreContext::new(42.0);
    let mut out = vec![0.0f32; POSTING_BLOCK];

    let mut group = c.benchmark_group("bm25/score_hits/128");
    group.throughput(Throughput::Elements(POSTING_BLOCK as u64));
    group.bench_function("scalar", |bench| {
        bench.iter(|| {
            ctx.score_hits_scalar(
                black_box(1.7),
                black_box(&freqs),
                black_box(&norms),
                &mut out,
            );
            black_box(&out);
        })
    });
    group.bench_function("runtime", |bench| {
        bench.iter(|| {
            ctx.score_hits(
                black_box(1.7),
                black_box(&freqs),
                black_box(&norms),
                &mut out,
            );
            black_box(&out);
        })
    });
    group.finish();
}

fn bench_vector_codec(c: &mut Criterion) {
    // given
    let mut rng = StdRng::seed_from_u64(5);
    let vector: Vec<f32> = (0..DIMENSIONS).map(|_| rng.gen_range(-1.0..1.0)).collect();
    let value = VectorDataValue::new(
        "doc-0001",
        vec![
            Field::vector(vector),
            Field::string("category", "shoes"),
            Field::int64("price", 99),
        ],
    );
    let encoded = value.encode_to_bytes();

    let mut group = c.benchmark_group("serde/vector_data/1536d");
    group.throughput(Throughput::Bytes(encoded.len() as u64));
    group.bench_function("encode", |bench| {
        bench.iter(|| black_box(black_box(&value).encode_to_bytes()))
    });
    group.bench_function("decode", |bench| {
        bench.iter(|| {
            black_box(VectorDataValue::decode_from_bytes(black_box(&encoded), DIMENSIONS).unwrap())
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(30);
    targets = bench_score_hits, bench_vector_codec
}
criterion_main!(benches);
