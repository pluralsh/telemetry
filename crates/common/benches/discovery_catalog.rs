use std::hint::black_box;
use std::io::Cursor;
use std::time::Duration;

use common::Ttl;
use common::discovery::{CatalogBatch, DiscoveryValue};
use common::storage::RecordOp;
use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};

const CARDINALITIES: [usize; 4] = [1_000, 10_000, 100_000, 1_000_000];

fn catalog_batch(cardinality: usize) -> CatalogBatch {
    let mut batch = CatalogBatch::default();
    for value in 0..cardinality {
        batch.insert(
            "span",
            format!("attribute_{:03}", value % 128),
            DiscoveryValue::String(format!("value_{value:08}")),
        );
    }
    batch
}

fn catalog_bytes(cardinality: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for op in catalog_batch(cardinality).into_ops(b"tenant/partition", Ttl::NoExpiry) {
        let RecordOp::Put(put) = op else {
            unreachable!("catalogs emit only put operations");
        };
        bytes.extend_from_slice(&(put.record.key.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&put.record.key);
        bytes.extend_from_slice(&(put.record.value.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&put.record.value);
    }
    bytes
}

fn benchmark_catalog_assembly(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("discovery_catalog_assembly");
    group
        .sampling_mode(SamplingMode::Flat)
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5));
    for cardinality in CARDINALITIES {
        group.throughput(Throughput::Elements(cardinality as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(cardinality),
            &cardinality,
            |bencher, cardinality| {
                bencher.iter(|| {
                    black_box(
                        catalog_batch(*cardinality).into_ops(b"tenant/partition", Ttl::NoExpiry),
                    )
                });
            },
        );
    }
    group.finish();
}

fn benchmark_catalog_compression(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("discovery_catalog_compression");
    group
        .sampling_mode(SamplingMode::Flat)
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5));
    for cardinality in CARDINALITIES {
        let input = catalog_bytes(cardinality);
        let lz4_size = lz4_flex::compress_prepend_size(&input).len();
        let zstd_size = zstd::encode_all(Cursor::new(&input), 3).unwrap().len();
        eprintln!(
            "terms={cardinality} raw={} lz4={lz4_size} zstd={zstd_size}",
            input.len()
        );
        group.throughput(Throughput::Bytes(input.len() as u64));
        group.bench_function(BenchmarkId::new("uncompressed", cardinality), |bencher| {
            bencher.iter(|| black_box(input.clone()));
        });
        group.bench_function(BenchmarkId::new("lz4", cardinality), |bencher| {
            bencher.iter(|| black_box(lz4_flex::compress_prepend_size(&input)));
        });
        group.bench_function(BenchmarkId::new("zstd", cardinality), |bencher| {
            bencher.iter(|| black_box(zstd::encode_all(Cursor::new(&input), 3).unwrap()));
        });
    }
    group.finish();
}

criterion_group!(
    discovery_catalog,
    benchmark_catalog_assembly,
    benchmark_catalog_compression
);
criterion_main!(discovery_catalog);
