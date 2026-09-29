# Discovery catalog

Meter, Line, and Track use the same partition-local catalog for dashboard
metadata discovery. The catalog is a derived index: its records are written in
the same atomic batch and with the same TTL as the data and query indexes that
produced them.

Catalog records use routing slot `65535`. Data routing uses only slots
`0..4096`, so catalog records remain in the product's existing namespace and
time-partition key space without belonging to a data shard range.

The shared suffix is:

```text
catalog slot (u16 BE) | format version (u8 = 1) | record kind | scope | name | typed value
```

Scope and name are terminated byte strings. Values are strings, booleans,
sortable signed integers, or sortable floating-point numbers. Name records and
value records are separate so listing names does not scan high-cardinality
values. Metric metadata is stored as a small product-specific value keyed by
metric name. The catalog format version covers the key suffix, typed-value
encoding, record kinds, and record values for all products; incompatible
changes increment it independently of Meter, Line, or Track key versions.

Each distinct term is an idempotent key with an empty value. This supports
incremental writes and partition TTL without read-modify-write operations.
Keys are ordered by scope, name, type, and value to maximize SlateDB's in-block
prefix compression. Catalog readers use prefix scans and merge sorted,
deduplicated results across time partitions and storage shards.

SlateDB block compression applies to both catalog keys and values. Builds
include LZ4 and Zstd support; deployments can select a codec through SlateDB
settings. Zstd generally favors object-store size and transfer reduction while
LZ4 favors lower CPU overhead.

The reproducible `cargo bench -p common --bench discovery_catalog` workload
measures catalog assembly plus uncompressed, LZ4, and Zstd level-3 encoding at
1K, 10K, 100K, and 1M unique terms. On the initial 1M-term run, 62,005,888 raw
bytes compressed to 6,094,472 bytes with LZ4 and 972,016 bytes with Zstd.
Catalog assembly took 5.03 seconds; Zstd compression added 31.3 milliseconds
(about 0.6%), while LZ4 added 31.1 milliseconds. Zstd therefore exceeded the
2x size-reduction target without a 10% ingest-throughput regression and is the
recommended object-store codec. Re-run the benchmark on production-class
hardware before changing an existing deployment.

Discovery reflects Written or Durable data. Applied-but-unflushed terms are not
maintained in a separate catalog buffer.
