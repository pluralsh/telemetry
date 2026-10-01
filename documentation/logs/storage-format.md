# Logs storage format

Logs stores logs in SlateDB, partitioned by namespace and fixed-duration time
segment. Each segment remains a SlateDB routing/compaction boundary. Inside a
segment, records sort by record type.

```text
Record prefix
┌───────────┬─────────┬─────────────────┬──────────────────┬─────────────┐
│ subsystem │ version │    namespace    │   time segment   │ record type │
│ 0x03      │ 0x03    │ TerminatedBytes │ sortable i64 BE  │ u8          │
└───────────┴─────────┴─────────────────┴──────────────────┴─────────────┘
```

`TerminatedBytes` escapes embedded delimiters and ends with `0x00`. Keys carry
no routing information: shard selection hashes the canonical `ShardedLogs`
routing key against the routing epoch in effect at each entry's timestamp. The
SlateDB segment extractor is named `logs-log/v3`. Version 3 is a hard format
switch: version-2 Logs databases are not read by this format. In the layouts below, `record prefix` means the complete prefix
above through the record-type byte.

Each namespace/time-segment partition also contains the shared discovery
catalog under the reserved record type `0xff` and catalog format version `1`.
Logs records every stream-label
name and string value there with the same TTL and in the same atomic storage
apply as label postings and pages. Metadata reads scan one compact catalog
prefix per requested time segment and union results across physical shards. Existing prerelease
data must be reset and reingested; there is no legacy discovery fallback or
backfill.

| ID | Record | Purpose |
| --- | --- | --- |
| `0x01` | Next stream ID | Allocates IDs within a segment |
| `0x02` | Stream dictionary | Label fingerprint to stream ID |
| `0x03` | Forward labels | Stream ID to canonical labels |
| `0x04` | Label postings | Label term to stream-ID bitmap |
| `0x05` | Page metadata | Pruning and retention metadata |
| `0x06` | Page payload | Compressed log entries |
| `0x07` | Next page sequence | Prevents page-key collisions |
| `0x0c` | Page tombstone | Replaced payload awaiting deletion |

## Stream and label records

```text
NextStreamId (0x01)
KEY   record prefix
VALUE stream_id: u32 BE

StreamDictionary (0x02)
KEY   record prefix │ label fingerprint: 16 bytes
VALUE stream_id: u32 BE

ForwardLabels (0x03)
KEY   record prefix │ stream_id: u32 BE
VALUE 0x01 │ count: var_u32 │ (name len: var_u32 │ name │ value len: var_u32 │ value) × count

LabelPostings (0x04)
KEY   record prefix │ label name: TerminatedBytes │ label value: raw UTF-8
VALUE RoaringBitmap<stream_id: u32>
```

IDs are local to one time segment of one storage shard. The dictionary
deduplicates complete stream label sets; forward labels reconstruct results;
postings intersect exact label matchers with one lookup per matcher.

## Page records

```text
PageMetadata (0x05)
KEY   record prefix │ stream_id: u32 │ first timestamp: sortable i64 │ sequence: u64
VALUE ┌─────────┬───────┬─────────────────────┬──────────────┬─────────────────┬──────────┬───────────────┐
      │ version │ flags │ expiry ms           │ min ts       │ max − min ts    │ rows     │ payload bytes │
      │ u8 = 2  │ u8    │ var_u64 if flags&1  │ i64 BE       │ var_u64         │ var_u32  │ var_u32       │
      └─────────┴───────┴─────────────────────┴──────────────┴─────────────────┴──────────┴───────────────┘
      ┌─────────┬────────────────┬──────────────┬──────────────────────┐
      │ level   │ written at ms  │ leaf count   │ leaf rows × count    │
      │ u8      │ var_u64        │ var_u32      │ var_u32 each         │
      └─────────┴────────────────┴──────────────┴──────────────────────┘

PagePayload (0x06)
KEY   same page address as PageMetadata │ level: u8
VALUE immutable LOGS page (layout below)

NextPageSequence (0x07)
KEY   record prefix │ stream_id: u32
VALUE next sequence: u64 BE

PageTombstone (0x0c)
KEY   same address as PagePayload
VALUE delete-after Unix ms: u64 BE
```

The sequence prevents collisions when pages have the same first timestamp.
Metadata is read first for time and expiry pruning, so it is kept to a few
dozen bytes; the block directory lives only in the payload. Metadata values
begin with a version byte (currently 2) and forward-label values with a format
byte (currently 1); readers reject values with any other leading byte.

### Compaction

Each write-buffer flush writes at least one level-0 page per stream, so
low-volume streams accumulate many small pages. The writer merges runs of
`compaction.fan_in` consecutive same-level pages of a stream into one page one
level higher, and after `compaction.finalize_after_seconds` past a segment's
end merges whatever small pages remain. A run is merged only when its pages
are time-ordered without overlap and cover consecutive sequences, and the
result stays within the page size and row limits.

A merged page keeps the metadata key of the first page it replaces and covers
the written pages ("leaves") at consecutive sequences from there; the leaf row
counts map full-text postings, which keep addressing leaves, to merged rows.
The merge is one atomic write: it replaces the first metadata record, deletes
the others, writes the new payload under its level, and writes a tombstone
for every replaced payload. Queries that listed the old metadata can still
read the old payloads until the tombstone's deadline
(`compaction.delete_delay_seconds`), after which the writer deletes both. A
writer that takes over a segment rebuilds its compaction state from page
metadata and tombstones.

```text
LOGS page payload
┌──────────┬─────────┬─────────────┬───────────┐
│ "LOGS"   │ version │ block count │ row count │
│ 4 bytes  │ u8      │ u32 BE      │ u32 BE    │
└──────────┴─────────┴─────────────┴───────────┘
┌───────────────────────────────────────────────────────────────────┐
│ block directory × N                                               │
│ min_ts:i64 │ max_ts:i64 │ rows:u32 │ offset:u32 │ compressed:u32 │
│ uncompressed:u32                                                   │
└───────────────────────────────────────────────────────────────────┘
┌────────────────┬────────────────┬─────┐
│ Snappy block 0 │ Snappy block 1 │ ... │
└────────────────┴────────────────┴─────┘
```

Each block contains timestamp-ordered rows with length-delimited log text and
structured metadata. Blocks decompress independently.

## Full-text search records

```text
SearchFieldStats (0x08)
KEY   record prefix
VALUE 0x01 │ documents: var_u64 │ total tokens: var_u64

SearchTermStats (0x09)
KEY   record prefix │ term: TerminatedBytes
VALUE 0x01 │ document frequency: var_u64 │ posting blocks: var_u32

SearchTermDirectory (0x0a)
KEY   record prefix │ term: TerminatedBytes │ directory ordinal: u32
VALUE 0x01 │ count: var_u32 │ entry × count
      entry: ordinal Δ │ postings │ max frequency │ min length   (var_u32 each)

SearchPostingBlock (0x0b)
KEY   record prefix │ term: TerminatedBytes │ block ordinal: u32
VALUE 0x01 │ count: var_u32 │ posting × count, sorted by address
      posting: stream Δ: var_u32
               stream Δ ≠ 0 → page sequence: var_u64 │ row ID: var_u32
               stream Δ = 0 → page Δ: var_u64 │ (page Δ = 0 ? row Δ : row ID): var_u32
               frequency: var_u32 │ length: var_u32
```

`var_u32`/`var_u64` are the length-prefixed varints from `common::serde::varint`.
Posting addresses are delta-encoded against their predecessor, so a typical
posting costs about five bytes. The leading `0x01` is the value format version;
readers reject values with any other leading byte.

Directory and posting values are bounded, so a common term never becomes one
unbounded SlateDB value. Writes top up a term's partially filled trailing block
before allocating new ones, so frequent small writes do not fragment postings
into one block per write. Queries load directories first, fetch posting blocks
concurrently, visit the rarest term first, and fetch only blocks whose impact
bound can still enter a single-term top-k result. Field statistics, term
statistics, directories, posting blocks, and write deltas are all
segment-local.

Retention uses both SlateDB TTL and logical expiry in page metadata, so expired
pages disappear from reads before compaction physically removes them.
`segment_duration_seconds`, page limits, and rows per block determine write
batching versus read amplification.
