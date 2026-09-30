# Meter storage format

Meter opens exactly one SlateDB database per owned storage shard. The database
path is `storage.path/shard-NNNN`; namespaces do not add another path level.
All tenant isolation is encoded in the keys described below, and every read,
write, ingest cache, and metadata catalog lookup remains namespace-scoped.

Records are grouped into time buckets (normally one hour). Every key begins
with the following layout. The SlateDB segment boundary remains immediately
after `bucket size`; the record type is outside that boundary.

```text
Common key scope
┌───────────┬─────────┬─────────────────┬──────────────┬─────────────┬─────────────┐
│ subsystem │ version │    namespace    │ bucket start │ bucket size │ record type │
│ 0x01      │ 0x03    │ TerminatedBytes │ u32 BE       │ u8          │ u8          │
└───────────┴─────────┴─────────────────┴──────────────┴─────────────┴─────────────┘
                                                               ▲
                                                   SlateDB segment boundary
```

Keys use big-endian numeric fields for lexical ordering. Values use their
record-specific encoding. `TerminatedBytes` escapes embedded delimiters and
ends with `0x00`; bucket size `0` is reserved. Keys carry no routing
information: shard selection hashes the canonical Meter routing key used by
`ShardedMeter` (namespace bytes followed by sorted `(label name, label value)`
pairs separated with zero bytes) against the routing epoch in effect at each
sample's timestamp, and a series written across a routing-epoch cutover
appears in more than one shard. The SlateDB segment extractor is named
`meter-timeseries/v3`. Version 3 is a hard format switch: version-2 databases
are not read.

Each namespace/time-bucket partition also contains the shared discovery
catalog under the reserved record type `0xff` and catalog format version `1`.
Meter records label names,
string values, and metric type/unit/help metadata there with the same TTL and
in the same atomic storage apply as the primary indexes and samples. Catalog
reads therefore scan one compact prefix per stored bucket, then union results
across physical shards. Existing
prerelease data must be reset and reingested; there is no legacy discovery
fallback or backfill.

Series IDs are dense `u32` values allocated per bucket within each storage
shard, starting at `0`. They are only meaningful inside one shard's bucket;
cross-shard queries join series by label fingerprint, never by ID.

| ID | Record | Purpose |
| --- | --- | --- |
| `0x02` | Series dictionary | Label fingerprint to series ID |
| `0x03` | Forward index | Series ID to labels and metric metadata |
| `0x04` | Inverted index | Label term to series-ID postings |
| `0x05` | Time series | Compressed samples for one series |

## Record types

### Series dictionary (`0x02`)

Assigns a bucket-local compact ID to a canonical label-set fingerprint.

```text
KEY   common scope │ fingerprint: u128 BE
VALUE ┌───────────────────┐
      │ series_id: u32 LE │
      └───────────────────┘
```

### Forward index (`0x03`)

Resolves a series ID back to everything needed to return or interpret it.

```text
KEY   common scope │ series_id: u32 BE
VALUE ┌─────────────────────────────────────────────────────────────┐
      │ optional metric unit                                       │
      │ metric type: u8 │ flags: u8 (temporality + monotonic)       │
      │ label count: u16 LE                                        │
      │ labels: [(name: length-prefixed UTF-8, value: UTF-8), ...]  │
      └─────────────────────────────────────────────────────────────┘
```

### Inverted index (`0x04`)

Maps an exact label term to matching series IDs. `__name__` is indexed like
any other label.

```text
KEY   common scope │ label name: TerminatedBytes │ label value: raw UTF-8
VALUE ┌──────────────────────────────────────┐
      │ RoaringBitmap<series_id: u32>        │
      └──────────────────────────────────────┘
```

The terminated label name allows an efficient prefix scan for all values of
one label. SlateDB merge operations union posting fragments.

### Time series (`0x05`)

Stores samples for one series. The metric-name prefix groups cold reads for a
single PromQL metric.

```text
KEY   common scope │ metric name: TerminatedBytes │ series_id: u32 BE
VALUE ┌────────────────────────────────────────────────────┐
      │ Gorilla stream of (timestamp_ms: u64, value: f64)  │
      └────────────────────────────────────────────────────┘
```

Timestamps are sorted before encoding. SlateDB's merge operator combines
sample fragments and applies last-write-wins for duplicate timestamps.

## Durability and visibility

- `applied`: acknowledged in the in-memory delta.
- `written`: entered into SlateDB mutable state and visible to a fresh snapshot.
- `durable`: flushed to the object store before acknowledgement.

The periodic writer flush bounds persistence and split-reader visibility for
the first two levels. Object-store data is authoritative; block and metadata
caches are disposable.
