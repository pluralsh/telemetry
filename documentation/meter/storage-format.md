# Meter storage format

Meter opens exactly one SlateDB database per owned storage shard. The database
path is `storage.path/shard-NNNN`; namespaces do not add another path level.
All tenant isolation is encoded in the keys described below, and every read,
write, ingest cache, and metadata catalog lookup remains namespace-scoped.

Records are grouped into time buckets (normally one hour). Every key begins
with the following layout. The SlateDB segment boundary remains immediately
after `bucket size`; the routing slot and record type are outside that boundary.

```text
Common key scope
┌───────────┬─────────┬─────────────────┬──────────────┬─────────────┬──────────────┬─────────────┐
│ subsystem │ version │    namespace    │ bucket start │ bucket size │ routing slot │ record type │
│ 0x01      │ 0x02    │ TerminatedBytes │ u32 BE       │ u8          │ u16 BE       │ u8          │
└───────────┴─────────┴─────────────────┴──────────────┴─────────────┴──────────────┴─────────────┘
                                                               ▲
                                                   SlateDB segment boundary
```

Keys use big-endian numeric fields for lexical ordering. Values use their
record-specific encoding. `TerminatedBytes` escapes embedded delimiters and
ends with `0x00`; bucket size `0` is reserved. Only the low 12 bits of the
routing-slot field are valid (`0..4096`). The slot is the high 12 bits of
BLAKE3 over the exact canonical Meter routing key used by `ShardedMeter`:
namespace bytes followed by sorted `(label name, label value)` pairs separated
with zero bytes.

Each namespace/time-bucket partition also contains the shared discovery
catalog under reserved routing slot `0xffff` and catalog format version `1`.
Meter records label names,
string values, and metric type/unit/help metadata there with the same TTL and
in the same atomic storage apply as the primary indexes and samples. Catalog
reads therefore scan one compact prefix per stored bucket instead of all 4096
data routing slots, then union results across physical shards. Existing
prerelease data must be reset and reingested; there is no legacy discovery
fallback or backfill.

Series IDs are allocated independently per `(bucket, routing slot)`. The
stored `u32` sequence starts at the slot and advances by 4096, preserving
bucket-wide uniqueness when query indexes combine multiple owned slots while
allowing the slot to be recovered as `series_id % 4096`.

| ID | Record | Purpose |
| --- | --- | --- |
| `0x02` | Series dictionary | Label fingerprint to series ID |
| `0x03` | Forward index | Series ID to labels and metric metadata |
| `0x04` | Inverted index | Label term to series-ID postings |
| `0x05` | Time series | Compressed samples for one series |

## Record types

### Series dictionary (`0x02`)

Assigns a bucket-and-slot-local compact ID to a canonical label-set fingerprint.

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
