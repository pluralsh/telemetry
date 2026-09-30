# Track storage format

Track stores OTLP traces in SlateDB by namespace and time segment. A write
groups traces into bounded segment-local pages and atomically publishes page
metadata, payload, trace locators, and attribute postings.

```text
Common key scope
┌───────────┬─────────┬─────────────────┬──────────────────┬─────────────┐
│ subsystem │ version │    namespace    │     segment      │ record type │
│ 0x05      │ 0x03    │ TerminatedBytes │ sortable i64 BE  │ u8          │
└───────────┴─────────┴─────────────────┴──────────────────┴─────────────┘
```

`TerminatedBytes` escapes embedded delimiters and ends with `0x00`. Keys carry
no routing information: shard selection hashes the canonical ShardedTrack
routing key (the namespace length as `u32` BE, namespace bytes, then the
16-byte trace ID) against the routing epoch in effect at the trace's earliest
span start. The SlateDB segment extractor is named `track-trace/v3`. Version 3
is a hard format switch: version-2 databases are not read.

Each namespace/time-segment partition also contains the shared discovery
catalog under the reserved record type `0xff` and catalog format version `1`.
Track records resource- and
span-scoped attribute names plus typed scalar values there with the same TTL
and in the same atomic storage apply as page records and attribute postings.
Tempo tag discovery scans these compact partition prefixes and does not fetch
trace payloads. A compact set of live segment IDs is stored as typed catalog
values in the existing locator segment (`i64::MIN`), so unbounded tag requests
discover partitions without scanning trace locators. Results
are unioned across physical shards. Existing prerelease data must be reset and
reingested; there is no legacy discovery fallback or backfill.

| ID | Record | Purpose |
| --- | --- | --- |
| `0x01` | Next page sequence | Allocates page IDs within a segment |
| `0x02` | Page metadata | Pruning and retention metadata |
| `0x03` | Page payload | Compressed OTLP traces |
| `0x04` | Trace locator | Trace ID to page location |
| `0x05` | Attribute posting | Attribute term to trace indexes |

## Page records

```text
NextPageSequence (0x01)
KEY   common scope
VALUE next sequence: u64 BE

PageMetadata (0x02)
KEY   common scope │ page sequence: u64 BE
VALUE ┌─────────┬───────┬────────────────────┬──────────┬──────────────┬─────────┐
      │ version │ flags │ expiry ms          │ min ts   │ max − min ts │ traces  │
      │ u8 = 1  │ u8    │ var_u64 if flags&1 │ var_u64  │ var_u64      │ var_u32 │
      └─────────┴───────┴────────────────────┴──────────┴──────────────┴─────────┘
      ┌──────────────────────────────────────────────────────────────┐
      │ trace summary × traces                                       │
      │ trace_id:16 │ min ts − page min ts:var_u64 │ max − min ts:var_u64 │
      └──────────────────────────────────────────────────────────────┘

PagePayload (0x03)
KEY   common scope │ page sequence: u64 BE
VALUE immutable TRAK page (layout below)
```

Metadata allows time and expiry pruning before loading a payload. Its trace
summaries mirror the payload directory in the same order, so search resolves
posting indexes and time-filters candidate trace IDs without fetching payloads.
Readers reject summaries that are unordered or disagree with the page bounds.

```text
TRAK page payload
┌──────────┬─────────┬─────────────┬───────────────┐
│ "TRAK"   │ version │ trace count │ total length  │
│ 4 bytes  │ u8      │ u32 BE      │ u32 BE        │
└──────────┴─────────┴─────────────┴───────────────┘
┌────────────────────────────────────────────────────────────────────────┐
│ trace directory × N                                                    │
│ trace_id:16 │ min_ts:u64 │ max_ts:u64 │ resource_spans:u32            │
│ offset:u32 │ compressed_len:u32 │ uncompressed_len:u32                 │
└────────────────────────────────────────────────────────────────────────┘
┌──────────────────┬──────────────────┬─────┐
│ Snappy OTLP trace│ Snappy OTLP trace│ ... │
└──────────────────┴──────────────────┴─────┘
```

Directory entries sort strictly by trace ID. Each payload is a
length-delimited sequence of OTLP `ResourceSpans` protobuf messages and
decompresses independently.

## Lookup and search records

```text
TraceLocator (0x04, stored in reserved segment i64::MIN)
KEY   common scope │ trace_id: 16 bytes │ data segment: sortable i64 │ page sequence:u64
VALUE ┌─────────┬───────┬────────────────────┬──────────────┬───────────────┬─────────────┐
      │ version │ flags │ expiry ms          │ data segment │ page sequence │ trace index │
      │ u8 = 1  │ u8    │ var_u64 if flags&1 │ i64 BE       │ var_u64       │ var_u32     │
      └─────────┴───────┴────────────────────┴──────────────┴───────────────┴─────────────┘

AttributePosting (0x05)
KEY   common scope │ scope:u8 │ name:TerminatedBytes │ typed value │ page sequence:u64
VALUE ┌─────────────────────────────────────────────┐
      │ count:u32 BE │ trace indexes:u32 BE × count│
      └─────────────────────────────────────────────┘
```

The trace-ID locator uses a reserved segment (`i64::MIN`) per namespace.
Locator keys are immutable sequence-suffixed fragments, avoiding a hot
read-modify-write record while making trace-by-ID a prefix lookup per storage
shard followed by a page fetch. A trace written across a routing-epoch cutover
can have locators in more than one shard; readers merge the partial traces.

Attribute keys distinguish resource from span scope and string, boolean,
integer, and double values. Postings identify candidate traces within one page;
Track decodes candidates to verify the complete query.

```text
typed value
┌────────────┬─────────────────────────────────────┐
│ type: u8   │ payload                             │
├────────────┼─────────────────────────────────────┤
│ 0x01       │ string: TerminatedBytes             │
│ 0x02       │ boolean: u8 (0 or 1)                │
│ 0x03       │ integer: sortable i64 BE            │
│ 0x04       │ double: IEEE-754 f64 bits, u64 BE   │
└────────────┴─────────────────────────────────────┘
scope: 0x01 = resource, 0x02 = span
```

`var_u32`/`var_u64` are the length-prefixed varints from `common::serde::varint`.
Metadata and locator values begin with a version byte (currently 1); readers
reject values with any other leading byte.

Retention is recorded on metadata and locators; expired records are ignored.
Object-store data is authoritative and local caches are disposable.
Page sequences, metadata, and postings are local to one segment of one storage
shard. The SlateDB segment extractor ends immediately after the
namespace/segment prefix.
