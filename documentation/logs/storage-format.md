# Logs storage format

Logs stores logs in SlateDB, partitioned by namespace and fixed-duration time
segment. Each segment remains a SlateDB routing/compaction boundary. Inside a
segment, records sort by record type.

```text
Record prefix
┌───────────┬─────────┬─────────────────┬──────────────────┬─────────────┐
│ subsystem │ version │    namespace    │   time segment   │ record type │
│ 0x03      │ 0x04    │ TerminatedBytes │ sortable i64 BE  │ u8          │
└───────────┴─────────┴─────────────────┴──────────────────┴─────────────┘
```

`TerminatedBytes` escapes embedded delimiters and ends with `0x00`. Keys carry
no routing information: shard selection hashes the canonical `ShardedLogs`
routing key against the routing epoch in effect at each entry's timestamp. The
SlateDB segment extractor is named `logs-log/v5`. Version 5 is a hard format
switch: databases written by earlier versions are not read and there is no
migration; reset and reingest them. In the layouts below, `record prefix`
means the complete prefix above through the record-type byte.

Each namespace/time-segment partition also contains the shared discovery
catalog under the reserved record type `0xff` and catalog format version `1`.
Logs records every stream-label
name and string value there with the same TTL and in the same atomic storage
apply as label postings and objects. Metadata reads scan one compact catalog
prefix per requested time segment and union results across physical shards. Existing prerelease
data must be reset and reingested; there is no legacy discovery fallback or
backfill.

| ID | Record | Purpose |
| --- | --- | --- |
| `0x01` | Next stream ID | Allocates IDs within a segment |
| `0x02` | Stream dictionary | Label fingerprint to stream ID |
| `0x03` | Forward labels | Stream ID to canonical labels |
| `0x04` | Label postings | Label term to stream-ID bitmap |
| `0x05` | Run | One stream's blocks in one object: pruning and retention |
| `0x06` | Object block | Compressed rows of one stream, as a meta and a lines value |
| `0x07` | Next object ID | Allocates object IDs within a segment |
| `0x08`–`0x0b` | Search records | Full-text term index |
| `0x0c` | Object tombstone | Replaced object awaiting deletion |
| `0x0d` | Rollup | Discovery records of a whole rollup period |
| `0x0e` | Object directory | Every run of one object, with its leaves |

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

Writes never read these records. The flusher is a shard's only writer, so it
keeps each segment's dictionary and counters in memory and allocates IDs
there; it reads them once, the first time the process writes to a segment
(after a restart, or after 15 idle minutes evicted them), by scanning the
dictionary and taking the next ID as the larger of the counter and the
highest scanned ID plus one. Every write rewrites the dictionary, forward
labels and counters of the streams it touches, refreshing their TTLs. Label
postings are SlateDB merge operands holding only the IDs the write added;
the logs merge operator unions them, so readers must open the database with
it. Rollup periods are handled the same way.

## Objects, runs and blocks

Log rows are stored in **objects**. An object holds the rows written together
by one write-buffer flush (or produced by one merge) for many streams of one
segment. Inside an object the rows are grouped by stream, ordered by stream
ID, and cut into **blocks** of at most `page.rows_per_block` rows of a single
stream. A stream's consecutive blocks within one object form its **run**.

```text
object 17, level 0                        blocks, two keys each (group 0 meta, group 1 lines)
┌──────────────────────────────────────────┐
│ run: stream 3   blocks 0..2              │  ObjectBlock(17, 0, meta, 0)  stream 3
│                                          │  ObjectBlock(17, 0, meta, 1)  stream 3
│ run: stream 4   block  2                 │  ObjectBlock(17, 0, meta, 2)  stream 4
│ run: stream 9   blocks 3..6              │  ObjectBlock(17, 0, meta, 3)  stream 9 …
│                                          │  ObjectBlock(17, 0, lines, 0) stream 3 …
└──────────────────────────────────────────┘
Run(stream 3, object 17)  Run(stream 4, object 17)  Run(stream 9, object 17)
ObjectDirectory(17, 0)
```

A flush cuts a new object once it reaches `page.target_size_bytes` or
`page.max_rows`, so one large stream may span several objects; small streams
share one. Compared with one object per stream per flush, this writes far
fewer, larger records, and a query selecting many streams reads them with
one range scan per object instead of one read per stream.

Object IDs are allocated per segment from `NextObjectId` in write order, by
the flusher's in-memory counter, which advances only once a write succeeds. An
object is named by `(id, level)`: flushes write level 0, and a merge writes
its result at one level above its highest input, under its first input's ID.

```text
Run (0x05)
KEY   record prefix │ stream_id: u32 BE │ object_id: u64 BE
VALUE ┌─────────┬───────┬────────────────────┬────────┬───────────────┬─────────┬─────────┐
      │ version │ flags │ expiry ms          │ min ts │ max − min ts  │ rows    │ bytes   │
      │ u8 = 1  │ u8    │ var_u64 if flags&1 │ i64 BE │ var_u64       │ var_u32 │ var_u32 │
      └─────────┴───────┴────────────────────┴────────┴───────────────┴─────────┴─────────┘
      ┌────────────┬───────────┬───────┬─────────────┬─────────┐
      │ line bytes │ run flags │ level │ first block │ blocks  │
      │ var_u64    │ u8        │ u8    │ var_u32     │ var_u32 │
      └────────────┴───────────┴───────┴─────────────┴─────────┘

ObjectBlock (0x06)
KEY   record prefix │ object_id: u64 BE │ level: u8 │ group: u8 │ block: u32 BE

group 0, meta:
VALUE ┌─────────┬────────┬────────┬──────────┬────────────┬────────────────────┬────────────┐
      │ format  │ min ts │ max ts │ rows     │ line bytes │ uncompressed bytes │ body       │
      │ u8 = 3  │ i64 BE │ i64 BE │ u32 BE   │ u32 BE     │ u32 BE             │ zstd frame │
      └─────────┴────────┴────────┴──────────┴────────────┴────────────────────┴────────────┘
      decompressed body, every integer a var_u64:
      ts − min ts × rows │ line len × rows
      │ name count │ (name len │ name) × name count
      │ (field count │ (name index │ value len) × field count) × rows
      │ values, concatenated

group 1, lines:
VALUE ┌─────────┬────────────┬──────────────────────────────────┐
      │ format  │ line bytes │ body                             │
      │ u8 = 4  │ u32 BE     │ zstd frame of lines concatenated │
      └─────────┴────────────┴──────────────────────────────────┘

NextObjectId (0x07)
KEY   record prefix
VALUE next object ID: u64 BE

ObjectTombstone (0x0c)
KEY   record prefix │ object_id: u64 BE │ level: u8
VALUE delete-after Unix ms: u64 BE │ block count: u32 BE

ObjectDirectory (0x0e)
KEY   record prefix │ object_id: u64 BE │ level: u8
VALUE ┌─────────┬───────┬────────────────────┬───────────────┬─────────┬───────────┐
      │ version │ flags │ expiry ms          │ written at ms │ span    │ run count │
      │ u8 = 1  │ u8    │ var_u64 if flags&1 │ var_u64       │ var_u64 │ var_u32   │
      └─────────┴───────┴────────────────────┴───────────────┴─────────┴───────────┘
      run × count, ordered by stream ID:
      ┌──────────┬────────┬──────────────┬─────────┬─────────┬────────────┬───────────┬─────────┬────────────┐
      │ stream Δ │ min ts │ max − min ts │ rows    │ bytes   │ line bytes │ run flags │ blocks  │ leaf count │
      │ var_u32  │ i64 BE │ var_u64      │ var_u32 │ var_u32 │ var_u64    │ u8        │ var_u32 │ var_u32    │
      └──────────┴────────┴──────────────┴─────────┴─────────┴────────────┴───────────┴─────────┴────────────┘
      leaf × leaf count: object_id: var_u64 │ rows: var_u32
```

Run records are what queries read: label postings select stream IDs, and one
scan per stream (or one scan over a dense span of stream IDs) lists each
stream's runs with the time range, expiry, row count, line bytes and block
range needed to prune and plan reads. `bytes` is the stored size of both
groups of the run's blocks; `line bytes` is the total length of its lines.
`run flags` bit 0 marks a run with no two rows sharing a timestamp, line and
structured metadata (the rows queries deduplicate): set by flushes that
checked their rows, and by merges whose inputs were all set and had disjoint
time ranges. Bit 1 marks a run with a row carrying `__error__` structured
metadata. Readers reject other bits. Runs are keyed by stream before object,
so a stream's runs are contiguous, and carry no timestamp in the key. Run
records are a few dozen bytes.

Each block stands alone: its meta header holds the block's time range, row
count and line bytes, so readers skip blocks outside the query range without
decompressing them. Rows inside a block are in stored order, which is
timestamp order except in blocks re-blocked by a merge. A block stores its
rows as columns: timestamps first, so a reader of a sorted block
binary-searches the rows in range and materializes only those, then line
lengths, a per-block dictionary of structured metadata names, each row's
fields and the value bytes. The lines are a separate value in group 1, so a
query that needs no line content (a count, a rate, bytes over time) reads
group 0 alone, and a query that keeps no row of a block never decompresses
its lines. Every meta value of an object sorts before every lines value, so
either group of a block range is one contiguous scan. Merges copy both values
of full blocks unchanged and re-encode the rest; a tombstoned object's
deletion removes both groups.

The object directory lists every run of the object in stream order; a run's
first block is the sum of the block counts before it. The directory is read by
compaction and by full-text queries over merged objects, never by plain
scans. `span` counts the consecutive level-0 IDs the object covers, starting
at its own ID: 1 for a flushed object, and the sum of its inputs' spans for a
merged one. Run, directory and forward-label values begin with format byte
`1`, block meta values with `3` and block lines values with `4`; readers
reject values with any other leading byte.

### Read planning

A segment's selected runs are grouped by object. Within an object, runs whose
block ranges are separated by at most 8 unselected blocks are coalesced into
one **read unit**; a unit of a single block is a point `get` per group read,
any other unit is one range scan per group over `[first block, end block)` of
the object's group prefix. Reads that need lines fetch both groups
concurrently.
Units are fetched concurrently in order of their nearest timestamp (earliest
for forward scans, latest for backward scans), and rows are released to the
query engine once no unfetched unit can precede them, so a satisfied limit
stops reading mid-segment. The query page budget (`max_query_pages`) is
charged per read unit, and query estimates count read units.

### Lineless metric reads

A metric query whose every log expression is a stage-free selector under
`count_over_time`, `rate`, `bytes_over_time` or `bytes_rate` reads no lines.
Its windows `(t − range − offset, t − offset]` give a set of boundaries; a
span of rows `[min, max]` no boundary `b` splits (`min ≤ b < max`) lands in
the same windows as its first row, so it is counted as one weighted sample of
its row count and line bytes. A run is isolated when it is duplicate-free and
its time range meets no other run of its stream in the segment, so none of
its rows is a duplicate. Per segment, an isolated run no boundary splits is
counted from its run record alone, with no block read; the remaining runs are
read from group 0 only, where an isolated run's unsplit blocks count from
their header and the rest decode their meta columns. Rows of non-isolated
runs are compared by timestamp, line length and structured metadata; only if
two collide are the stream's lines read and its rows deduplicated exactly.
Structured metadata is decoded when the grouping can see it (anything but
`by ()`) and for every run of a stream with a run flagged for `__error__`
metadata, which fails the query like the full read does.

Rows with equal timestamps are ordered by stream fingerprint, then object ID,
then position in the run, so results are deterministic and preserve write
order within a stream.

### Compaction

Each flush writes one or more level-0 objects per segment it touches, so a
segment receiving a trickle of writes accumulates many small objects. An
object is small while it is under half of both `page.target_size_bytes` and
`page.max_rows`. The writer merges runs of `compaction.fan_in` adjacent
same-level small objects into one object one level higher. A run cut short
because the next object would push it past the page limits merges as soon as
it has two objects: its inputs are each under half the limits, so the result
is no longer small, and waiting for `fan_in` inputs that cannot fit would
leave them unmerged until the segment settles. Once
`compaction.finalize_after_seconds` has passed after a segment's end it merges
whatever small objects remain (with a balance rule so each row is rewritten a
logarithmic number of times). Two objects are adjacent when the first's ID
plus its span is the second's ID. Merging only adjacent objects keeps every
stream's rows in write order across merges. The result must stay within the
page size and row limits.

A merge concatenates each stream's blocks in input order and lays the
streams out in stream order. A block already holding `page.rows_per_block`
rows is copied unchanged as long as no partial block's rows precede it in the
stream; partial blocks are decoded and their rows re-blocked, so low-volume
streams end up in full blocks without re-encoding the rows of busy streams.
Merge decoding and encoding run off the async runtime. Rows of a merged run
are in write order, not necessarily timestamp order, when writes arrived out
of order; readers stable-sort a run whose rows are out of order after
decoding.

The merge is one atomic write. It puts the new blocks, run records and
directory, deletes the run records of every input except the first (whose run
keys the merged object reuses and overwrites), and writes a tombstone for
every input. Queries that listed the old runs can still read the old blocks
and directories until the tombstone's deadline
(`compaction.delete_delay_seconds`). After the deadline the writer deletes
them together with the tombstone. A writer that takes over a segment rebuilds
its compaction state by reading tombstones (re-queueing their deletions and
treating their objects as gone) and then the remaining object directories.

### Leaves and full-text addresses

Every merged run lists its **leaves**: the level-0 objects whose rows it
concatenates, in order, with each leaf's row count. A level-0 run has no
stored leaves; it is its own single leaf. Full-text postings address a row as
`(stream ID, leaf object ID, row in leaf)` and are never rewritten by merges.
A full-text query maps each selected run's leaves to `(run object, first row
of the leaf in the run)` by reading the directories of the merged objects
among them, then adds the posting's row to that offset.

## Full-text search records

```text
SearchFieldStats (0x08)
KEY   record prefix
VALUE 0x01 │ documents: var_u64 │ total tokens: var_u64

SearchTermStats (0x09)
KEY   record prefix │ term: TerminatedBytes
VALUE 0x01 │ document frequency: var_u64 │ posting blocks: var_u32

SearchTermDirectory (0x0a), one fragment per flush and 256 entries
KEY   record prefix │ term: TerminatedBytes │ first block ID: u64 BE
VALUE 0x01 │ count: var_u32 │ entry × count
      entry: block ID Δ: var_u64 │ postings │ max frequency │ min length   (var_u32)

SearchPostingBlock (0x0b)
KEY   record prefix │ term: TerminatedBytes │ block ID: u64 BE
VALUE 0x01 │ count: var_u32 │ posting × count, sorted by address
      posting: stream Δ: var_u32
               stream Δ ≠ 0 → leaf object ID: var_u64 │ row ID: var_u32
               stream Δ = 0 → leaf Δ: var_u64 │ (leaf Δ = 0 ? row Δ : row ID): var_u32
               frequency: var_u32 │ length: var_u32
```

`var_u32`/`var_u64` are the length-prefixed varints from `common::serde::varint`.
Posting addresses are delta-encoded against their predecessor, so a typical
posting costs about five bytes. The leading `0x01` is the value format version;
readers reject values with any other leading byte.

Directory and posting values are bounded, so a common term never becomes one
unbounded SlateDB value. Writes never read the index: each flush writes new
blocks of at most 128 postings per term, named `first object ID of the flush
<< 24 | block index`, so IDs never collide and increase across flushes, plus
directory fragments keyed by their first block ID. Field and term statistics
are merge operands that the logs merge operator sums. A reader scans a term's
fragments and requires at least the blocks its statistics count; a concurrent
flush may add more. The cost is that every flush starts new blocks, so small
frequent flushes leave partially filled blocks. Queries load directories first, fetch posting blocks
concurrently, visit the rarest term first, and fetch only blocks whose impact
bound can still enter a single-term top-k result. Field statistics, term
statistics, directories, posting blocks, and write deltas are all
segment-local.

Postings also narrow plain `|=` filters. Each single-branch `|=` that runs
before the line is rewritten (`line_format`, `decolorize`, `unpack`)
contributes the terms of every needle chunk bounded by ASCII whitespace on
both sides; the first and last chunks may be parts of longer words. Analyzed
with its bounding whitespace, a chunk yields the same terms as inside any
line holding the needle, since neither link detection nor word boundaries
reach across ASCII whitespace. Per segment, a term with no postings rules
the segment out; otherwise the rarest term's postings, mapped through the
runs' leaves, select the candidate rows of each run, and runs without one are
not read. The filter itself still decides every candidate. A segment without
field statistics, or whose rarest term is in more than a fifth of its rows or
has more posting blocks than the selected runs have blocks, is read whole.
Postings are written in the batch of the rows they index, so a read only
misses rows of a merged object that are already past their own retention.

Retention uses both SlateDB TTL and logical expiry in run records and object
directories, so expired runs disappear from reads before compaction physically
removes them. A merged object expires with the latest of its inputs.
`segment_duration_seconds`, the `page` limits (object size and rows), and rows
per block determine write batching versus read amplification.
