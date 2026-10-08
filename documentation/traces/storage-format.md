# Traces storage format

Traces stores OTLP traces in SlateDB by namespace and time segment. A flush
groups traces into bounded segment-local pages and atomically publishes page
metadata, payload, trace heads, continuations, and attribute postings. It never
reads stored records first: page sequences are allocated in memory by the
shard's only writer, and trace heads are merge records.

```text
Common key scope
┌───────────┬─────────┬─────────────────┬──────────────────┬─────────────┐
│ subsystem │ version │    namespace    │     segment      │ record type │
│ 0x05      │ 0x06    │ TerminatedBytes │ sortable i64 BE  │ u8          │
└───────────┴─────────┴─────────────────┴──────────────────┴─────────────┘
```

`TerminatedBytes` escapes embedded delimiters and ends with `0x00`. Keys carry
no routing information: shard selection hashes the canonical ShardedTraces
routing key (the namespace length as `u32` BE, namespace bytes, then the
16-byte trace ID) against the routing epoch in effect at the trace's earliest
span start. The SlateDB segment extractor is named `traces-trace/v6`. The
format is not backward compatible: databases of earlier versions are not read
and there is no migration; reset and reingest them.

Each namespace/time-segment partition also contains the shared discovery
catalog under the reserved record type `0xff` and catalog format version `1`.
Traces records resource- and
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
| `0x04` | Trace head | A trace's first page and page count (merge record) |
| `0x05` | Attribute posting | Attribute term to trace indexes |
| `0x06` | Trace continuation | One page of a trace |

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
      continued bitmap: ⌈traces / 8⌉ bytes, bit i of byte i/8 per summary

PagePayload (0x03)
KEY   common scope │ page sequence: u64 BE
VALUE immutable TRAK page (layout below)
```

Metadata allows time and expiry pruning before loading a payload. Its trace
summaries mirror the payload directory in the same order, so search resolves
posting indexes and time-filters candidate trace IDs without fetching payloads.
Readers reject summaries that are unordered or disagree with the page bounds.
A trace's continued bit is set when an earlier page of the same flush holds
it. A clear bit does not mean the trace has no other pages: a later flush may
add pages without reading this one, and only the trace head counts them.

`NextPageSequence` is rewritten by every flush to its segment. The flusher
keeps each segment's next sequence in memory and reads the record only the
first time the process writes to the segment (after a restart, or after 15
idle minutes evicted it).

```text
TRAK page payload
┌──────────┬─────────┬─────────────┬───────────────┐
│ "TRAK"   │ version │ trace count │ total length  │
│ 4 bytes  │ u8 = 3  │ u32 BE      │ u32 BE        │
└──────────┴─────────┴─────────────┴───────────────┘
┌────────────────────────────────────────────────────────────────────────┐
│ trace directory × N                                                    │
│ trace_id:16 │ min_ts:u64 │ max_ts:u64 │ resource_spans:u32            │
│ offset:u32 │ compressed_len:u32 │ uncompressed_len:u32                 │
└────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────┐
│ column sidecar                                          │
│ compressed_len:u32 BE │ uncompressed_len:u32 BE │ Snappy │
└─────────────────────────────────────────────────────────┘
┌──────────────────┬──────────────────┬─────┐
│ Snappy OTLP trace│ Snappy OTLP trace│ ... │
└──────────────────┴──────────────────┴─────┘
```

Directory entries sort strictly by trace ID. Each payload is a
length-delimited sequence of OTLP `ResourceSpans` protobuf messages and
decompresses independently. Directory offsets are absolute, so the first
payload starts immediately after the sidecar. Readers reject any other
version as corrupt.

Every page carries a sidecar. `page.target_size_bytes` and
`page.max_size_bytes` measure the trace data alone: header, directory, and
trace payloads. The sidecar is written in addition, so a page within
`page.max_size_bytes` always keeps its sidecar, and only the encoded total
must fit the u32 length field.

### Column sidecar

The sidecar holds intrinsic and structural columns and columns of a few
common attributes for every span on the page, so a query can rule traces out,
or decide and summarize matches, without decompressing any trace payload.
Spans are ordered by trace in directory order, then by `ResourceSpans`,
`ScopeSpans`, and span order within each trace.

```text
column sidecar, uncompressed
var_u32 traces │ var_u32 spans │ (var_u32 span count │ u8 trace flags) × traces
                                trace flag 1 = two of its spans share a span ID
name dictionary                 var_u32 count, then (var_u32 length, UTF-8) × count
service.name dictionary         same encoding
var_u64 duration ns × spans     end − start, saturating at 0
var_u64 start offset × spans    start − the trace's earliest start on the page
var_u32 parent × spans          0 = no parent span ID, 1 = parent not among the
                                trace's spans on the page, 2 + its index among
                                them (a repeated span ID names its last span)
u8 flags × spans                status code | kind code << 2
var_u32 name id × spans         index into the name dictionary
var_u32 service code × spans    0 = no service.name, 1 = non-string value,
                                2 + index into the service.name dictionary
dedicated column × 8            in the order of the table below

dedicated column
var_u32 byte length of the rest; zero when no span holds the attribute
var_u32 spans holding the attribute
string column:  dictionary │ var_u32 code × spans
                0 = missing, 1 = non-string value, 2 + dictionary index
integer column: u8 code × spans │ zigzag var_u64 value × spans with code 2
                0 = missing, 1 = non-integer value, 2 = integer
```

Status codes are 0 unset, 1 ok, 2 error. Kind codes follow OTLP: 0
unspecified through 5 consumer.

| Column | Scope | Type |
| --- | --- | --- |
| `http.status_code` | span | integer |
| `http.response.status_code` | span | integer |
| `http.method` | span | string |
| `http.request.method` | span | string |
| `http.route` | span | string |
| `db.system` | span | string |
| `rpc.service` | span | string |
| `k8s.namespace.name` | resource | string |

Each attribute column, like `service.name`, holds the first attribute of
that key on the span (or on its resource, repeated for each of its spans),
which is the attribute the query interpreter reads. A first attribute whose
value is not a string, boolean, integer, or double counts as missing, as it
does for the interpreter. Zigzag maps `v` to `(v << 1) ^ (v >> 63)`.

The reader validates the sidecar bounds when a page is decoded. It decodes
the sidecar the first time a query consults it, and each attribute column
only when a query's filter reads that column; the byte lengths let the
others be skipped. A sidecar is corrupt if any of these hold:

- its lengths disagree with the page or with its Snappy stream;
- its trace count differs from the directory;
- its span counts do not sum to its span total;
- a trace flag is unknown, or no span of a trace has start offset 0;
- a dictionary id, code, or parent index is out of range;
- an attribute column's count of spans holding it is zero, exceeds the span
  total, or disagrees with its codes;
- an attribute column's byte length disagrees with its contents;
- bytes remain after the last column.

An attribute column's own corruption is only detected, and only fails a
query, once a filter reads that column.

A corrupt sidecar fails the query with a corruption error. Trace-by-ID reads
never consult the sidecar.

### Query pruning

TraceQL search derives a conservative trace filter from the query: a
boolean formula over sidecar predicates that every matching trace must
satisfy. Sidecar predicates compare `duration`, `status`, `kind`, `name`,
`resource.service.name`, or an explicitly scoped attribute column (such as
`span.http.route`, but not `.http.route`) against a constant. String columns
evaluate `=`, `!=`, orderings, `=~`, and `!~` against a string exactly as the
interpreter does. Integer columns evaluate `=`, `!=`, and orderings against a
number, and take a value of another type, such as a double, to satisfy the
comparison, so they only bound the result and never appear under negation. A
loaded candidate is skipped without decoding its payload only when all of
the following hold:

- each locator agrees with its page directory;
- the trace's combined span count is within the query's span limit;
- the trace's pages all belong to the shard being loaded;
- the filter is false when each sidecar predicate counts as satisfied if any
  span on any of the trace's pages satisfies it.

Otherwise the trace is decoded and evaluated as before. Any span limit
violation is also reported as before.

An existential query — one spanset filter, or a union of them, refined only by
spanset filter stages over span-local fields — matches a trace when any span
the index saw matches, so search drops candidates whose pages no clause
confirmed and reads heads only for matches. When the whole query compiles to
exact sidecar predicates (or is `{}`), the sidecar decides each part without
decoding it. Search responses, which list only summaries, then take a match's
summary from the sidecar when the trace is wholly on one live page of one
shard, within the span limit, with no repeated span ID and a unique earliest
root candidate: start and end come from the directory, and the root (the
earliest span without a parent on the page) gives the service and span
names. Other matches are decoded and evaluated whole.

## Lookup and search records

```text
locator
┌─────────┬───────┬────────────────────┬──────────────┬───────────────┬─────────────┐
│ version │ flags │ expiry ms          │ data segment │ page sequence │ trace index │
│ u8 = 1  │ u8    │ var_u64 if flags&1 │ i64 BE       │ var_u64       │ var_u32     │
└─────────┴───────┴────────────────────┴──────────────┴───────────────┴─────────────┘

TraceHead (0x04, stored in reserved segment i64::MIN)
KEY   common scope │ trace_id: 16 bytes
VALUE locator of the first page │ pages: u32 BE │ flags: u8 (bit 0 = continued)

TraceContinuation (0x06, stored in reserved segment i64::MIN)
KEY   common scope │ trace_id: 16 bytes │ data segment: sortable i64 │ page sequence:u64
VALUE locator

AttributePosting (0x05)
KEY   common scope │ scope:u8 │ name:TerminatedBytes │ typed value │ page sequence:u64
VALUE ┌─────────────────────────────────────────────┐
      │ count:u32 BE │ trace indexes:u32 BE × count│
      └─────────────────────────────────────────────┘
```

Heads and continuations use a reserved segment (`i64::MIN`) per namespace, so
trace-by-ID is a lookup in one segment per storage shard followed by page
fetches. Every page of a trace gets a continuation record. The head is a
SlateDB merge record: each flush writes an operand for the traces it holds,
naming the first of its pages, counting them, and setting `continued` when
there are several. The traces merge operator keeps the oldest operand's first
page, sums page counts, sets `continued` once operands combine, and gives the
first locator the latest expiry among them (none if any has none), so the head
outlives every page it reaches. Readers open the database with the same merge
operator.

A head that is not continued names the trace's only page; otherwise readers
scan the trace's continuations, skipping expired ones. Cached locators are
keyed by the head's page count, and the writing process invalidates those of
every trace it writes. Search orders candidates by the pages the index saw and
reads their heads at load time; a candidate some clause missed, seen on one
page with a clear continued bit, is kept only if its head counts more pages. A
trace written across a routing-epoch cutover can have heads in more than one
shard; readers merge the partial traces.

Attribute keys distinguish resource and span attributes and span intrinsics,
and string, boolean, integer, and double values. Postings identify candidate
traces within one page; Traces decodes candidates to verify the complete
query.

```text
typed value
┌────────────┬──────────────────────────────────────────┐
│ type: u8   │ payload                                  │
├────────────┼──────────────────────────────────────────┤
│ 0x01       │ string: TerminatedBytes                  │
│ 0x02       │ boolean: u8 (0 or 1)                     │
│ 0x03       │ integer: sortable i64 BE                 │
│ 0x04       │ double: sortable f64 bits, u64 BE        │
└────────────┴──────────────────────────────────────────┘
scope: 0x01 = resource, 0x02 = span, 0x03 = intrinsic
```

A sortable double is its IEEE-754 bits with the sign bit flipped when it is
clear, and every bit flipped when it is set, so integer and double keys of a
field both sort numerically. `-0.0` sorts just below `+0.0`, and NaNs sort
beyond the infinity of their sign. Intrinsic postings index span `name`,
`status` and `kind` codes, and `duration` as its bit length (bucket `b > 0`
holds durations in `[2^(b-1), 2^b)`), an integer.

An ordering (or an equality that cannot name its values) against a number
scans only the integer and double key ranges that can hold a matching
value, and a duration comparison only the buckets that can. Bounds are
conservative where integers convert inexactly to doubles; every scanned value
is still checked against the comparison. Other non-exact comparisons scan
every value of the field.

`var_u32`/`var_u64` are the length-prefixed varints from `common::serde::varint`.
Metadata, head, and continuation values begin with a version byte (currently
1); readers reject values with any other leading byte.

Retention is recorded on metadata and locators; expired records are ignored.
Object-store data is authoritative and local caches are disposable.
Page sequences, metadata, and postings are local to one segment of one storage
shard. The SlateDB segment extractor ends immediately after the
namespace/segment prefix.
