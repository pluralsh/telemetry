# Meter storage format

Meter stores every namespace in an independent SlateDB database over the
configured object store. Namespace paths are deterministic hashes beneath
`storage.path`; sharded deployments add a shard suffix.

Records are grouped into time buckets (normally one hour). Every key begins
with the following routing scope; the scope is also the SlateDB segment
boundary.

```text
Common key scope
┌───────────┬─────────┬─────────────────┬──────────────┬─────────────┬─────────────┐
│ subsystem │ version │    namespace    │ bucket start │ bucket size │ record type │
│ 0x01      │ 0x01    │ TerminatedBytes │ u32 BE       │ u8          │ u8          │
└───────────┴─────────┴─────────────────┴──────────────┴─────────────┴─────────────┘
```

Keys use big-endian numeric fields for lexical ordering. Values use their
record-specific encoding. `TerminatedBytes` escapes embedded delimiters and
ends with `0x00`; bucket size `0` is reserved.

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
