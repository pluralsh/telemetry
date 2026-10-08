# Metrics storage format

Metrics opens exactly one SlateDB database per owned storage shard. The database
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
information: shard selection hashes the canonical Metrics routing key used by
`ShardedMetrics` (namespace bytes followed by sorted `(label name, label value)`
pairs separated with zero bytes) against the routing epoch in effect at each
sample's timestamp, and a series written across a routing-epoch cutover
appears in more than one shard. The SlateDB segment extractor is named
`metrics-timeseries/v3`. Version 3 is a hard format switch: version-2 databases
are not read.

Each namespace/time-bucket partition also contains the shared discovery
catalog under the reserved record type `0xff` and catalog format version `1`.
Metrics records label names,
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
| `0x06` | Bucket generation | Write generation of the bucket |

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
VALUE float-only:      ┌──────┬───────────────┐
                       │ 0x01 │ float section │
                       └──────┴───────────────┘
      with histograms: ┌──────┬──────────────────────┬───────────────┬───────────────────┐
                       │ 0x81 │ section len: uvarint │ float section │ histogram section │
                       └──────┴──────────────────────┴───────────────┴───────────────────┘
```

An empty value is empty bytes. The leading byte is the value format version
(`1`), with `0x80` set when native histograms follow; any other version is
rejected, so values written before this format must be reset and reingested.
Samples are sorted by timestamp, and a timestamp holds at most one sample:
of floats sharing one the first is kept, and a histogram wins over a float.
The histogram section is a sample count, then per sample a delta-of-delta
timestamp and the histogram encoded against the previous one.

#### Float section

A float section is a sequence of chunks of 1 to 1024 samples whose
timestamps strictly increase within and across chunks. Longer runs split into
balanced chunks. Each chunk is self-describing:

```text
CHUNK ┌────────────┬──────────────────┬───────────────────────┬───────────────────┐
      │ n: uvarint │ first_ts: zigzag │ last - first: uvarint │ body len: uvarint │
      └────────────┴──────────────────┴───────────────────────┴───────────────────┘
BODY  ┌──────────────────────────────────────────┬──────────────────┬──────────────┐
      │ layout: u8 (ts scheme | value scheme<<4) │ timestamp column │ value column │
      └──────────────────────────────────────────┴──────────────────┴──────────────┘
```

The header bounds the chunk without decoding it: ranged reads skip chunks
outside `(start, end]`, and the merge operator copies chunks that do not
overlap.

Packed columns hold one `width`-bit lane per sample, LSB-first in
little-endian `u64` words truncated to whole bytes, so every 64-lane block
starts on a byte boundary and unpacks independently. A frame of reference
precedes each packed column: `min` (zigzag varint), a width byte (`0x80` set
when a shift byte follows), and the optional shift; a lane holds
`(x - min) >> shift`.

| Timestamp scheme | Column | `ts[i]` |
| --- | --- | --- |
| `0` varint | `n - 2` uvarint deltas | previous plus delta; the last is `first + span` |
| `1` grid | interval (zigzag), frame, lanes | `first + i * interval + min + (lane[i] << shift)` |
| `2` delta | frame, lanes | running sum of deltas from `first` |
| `3` delta-of-delta | first delta (zigzag), frame, lanes | running sum of a running sum |

The encoder picks the smallest. A ranged read of a grid chunk binary-searches
the first lane of each block and unpacks only the blocks it needs.

| Value scheme | Column |
| --- | --- |
| `0` byte XOR | first value raw (8 bytes LE), then per value `0x80` if unchanged or a header byte (leading / trailing zero bytes in the high / low nibble) and the XOR's middle bytes big-endian |
| `1` ALP | exponent `e`, factor `f`, frame, lanes, exceptions |
| `2` ALP delta | `e`, `f`, first int (zigzag), frame, lanes of deltas, exceptions |
| `3` ALP-RD | right width, dictionary of up to eight `u16` left parts, packed codes, packed right parts, exceptions (`u16` left parts) |
| `4` XOR | Gorilla XOR bit stream with the first value raw |
| `5` constant | one raw value (8 bytes LE) |

ALP stores each value as an integer `d` with `d * 10^f / 10^e` reproducing its
bits exactly. Values that do not round-trip (NaN payloads including the stale
marker, `-0.0`, infinities, full-precision values) are exceptions: a uvarint
count, `u16` positions, then the raw 8-byte values, patched in after
unpacking. ALP is used while it misses at most half the values; otherwise the
chunk takes the smallest of ALP, ALP-RD (chunks over 64 samples) and XOR, or
constant when every value has identical bits. Chunks of up to 16 samples also
consider byte XOR, which is what single-sample merge operands use.

SlateDB's merge operator combines sample fragments with last-write-wins for
duplicate timestamps (the newest operand wins). Fragments whose chunks are
disjoint merge by copying chunks; runs of four or more chunks under 32
samples are re-encoded into one, and more than four chunks of 32 to 511
samples, or any overlap, re-encode the whole section into balanced chunks.

### Bucket generation (`0x06`)

One record per bucket, rewritten (a put, not a merge) by every flush into the
bucket, in the same atomic batch as that flush's index and sample records,
late writes into an older bucket included. A reader that sees a flush's
samples therefore also sees its generation, so an unchanged generation means
the bucket's contents are unchanged.

```text
KEY   common scope (no suffix)
VALUE ┌─────────────────────┐
      │ generation: u64 LE  │
      └─────────────────────┘
```

The generation strictly increases across flushes of a bucket within a shard,
including across writer restarts and shard handoffs: each flush stores
`max(previous + 1, wall-clock microseconds)`, where `previous` is the larger
of the generation the bucket was loaded with and the last one the writer
process issued. Values are not comparable across buckets or shards. The record
shares the bucket's TTL, so it expires with the data it describes; an absent
record means the bucket was never flushed.

The range-query result cache compares the generations of the buckets a step
depends on against those recorded when the step was cached.

## Durability and visibility

- `applied`: acknowledged in the in-memory delta.
- `written`: entered into SlateDB mutable state and visible to a fresh snapshot.
- `durable`: flushed to the object store before acknowledgement.

The periodic writer flush bounds persistence and split-reader visibility for
the first two levels. Object-store data is authoritative; block and metadata
caches are disposable.
