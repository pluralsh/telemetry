# Line storage format

Line stores logs in SlateDB, partitioned by namespace and fixed-duration time
segment. Each segment is a SlateDB routing/compaction boundary.

```text
Common key scope
┌───────────┬─────────┬─────────────────┬──────────────────┬─────────────┐
│ subsystem │ version │    namespace    │   time segment   │ record type │
│ 0x03      │ 0x01    │ TerminatedBytes │ sortable i64 BE  │ u8          │
└───────────┴─────────┴─────────────────┴──────────────────┴─────────────┘
```

`TerminatedBytes` escapes embedded delimiters and ends with `0x00`.

| ID | Record | Purpose |
| --- | --- | --- |
| `0x01` | Next stream ID | Allocates IDs within a segment |
| `0x02` | Stream dictionary | Label fingerprint to stream ID |
| `0x03` | Forward labels | Stream ID to canonical labels |
| `0x04` | Label postings | Label term to stream-ID bitmap |
| `0x05` | Page metadata | Pruning and retention metadata |
| `0x06` | Page payload | Compressed log entries |
| `0x07` | Next page sequence | Prevents page-key collisions |

## Stream and label records

```text
NextStreamId (0x01)
KEY   common scope
VALUE stream_id: u32 BE

StreamDictionary (0x02)
KEY   common scope │ label fingerprint: 16 bytes
VALUE stream_id: u32 BE

ForwardLabels (0x03)
KEY   common scope │ stream_id: u32 BE
VALUE 0x01 │ count: var_u32 │ (name len: var_u32 │ name │ value len: var_u32 │ value) × count

LabelPostings (0x04)
KEY   common scope │ label name: TerminatedBytes │ label value: raw UTF-8
VALUE RoaringBitmap<stream_id: u32>
```

IDs are local to one time segment. The dictionary deduplicates complete stream
label sets; forward labels reconstruct results; postings intersect exact label
matchers.

## Page records

```text
PageMetadata (0x05)
KEY   common scope │ stream_id: u32 │ first timestamp: sortable i64 │ sequence: u64
VALUE ┌─────────┬───────┬─────────────────────┬──────────────┬─────────────────┬──────────┬───────────────┐
      │ version │ flags │ expiry ms           │ min ts       │ max − min ts    │ rows     │ payload bytes │
      │ u8 = 1  │ u8    │ var_u64 if flags&1  │ i64 BE       │ var_u64         │ var_u32  │ var_u32       │
      └─────────┴───────┴─────────────────────┴──────────────┴─────────────────┴──────────┴───────────────┘

PagePayload (0x06)
KEY   same page address as PageMetadata
VALUE immutable LINE page (layout below)

NextPageSequence (0x07)
KEY   common scope │ stream_id: u32
VALUE next sequence: u64 BE
```

The sequence prevents collisions when pages have the same first timestamp.
Metadata is read first for time and expiry pruning, so it is kept to a few
dozen bytes; the block directory lives only in the payload. Metadata values
begin with a version byte (currently 1) and forward-label values with a format
byte (currently 1); readers reject values with any other leading byte.

```text
LINE page payload
┌──────────┬─────────┬─────────────┬───────────┐
│ "LINE"   │ version │ block count │ row count │
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
KEY   common scope
VALUE 0x01 │ documents: var_u64 │ total tokens: var_u64

SearchTermStats (0x09)
KEY   common scope │ term: TerminatedBytes
VALUE 0x01 │ document frequency: var_u64 │ posting blocks: var_u32

SearchTermDirectory (0x0a)
KEY   common scope │ term: TerminatedBytes │ directory ordinal: u32
VALUE 0x01 │ count: var_u32 │ entry × count
      entry: ordinal Δ │ postings │ max frequency │ min length   (var_u32 each)

SearchPostingBlock (0x0b)
KEY   common scope │ term: TerminatedBytes │ block ordinal: u32
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
bound can still enter a single-term top-k result.

Retention uses both SlateDB TTL and logical expiry in page metadata, so expired
pages disappear from reads before compaction physically removes them.
`segment_duration_seconds`, page limits, and rows per block determine write
batching versus read amplification.
