# Storage compaction and garbage collection

Metrics, Logs and Traces each route every `(namespace, time segment)` key
prefix to its own SlateDB segment: a separate LSM tree with its own L0 SSTs
and sorted runs. All segments share one `compacted/` directory. A compaction
only ever reads sources from one segment, so data is never merged across
segments.

## Closed-segment compaction

SlateDB's size-tiered scheduler only merges runs of similar size. Once a
segment stops taking writes it keeps whatever it had when writes stopped:
several sorted runs plus a few L0 SSTs. Every cold read of that segment then
pays for each of them in index and filter fetches, block reads, and a merge
per key. SlateDB enforces TTLs only when it rewrites data, so a segment that
is never compacted again also never loses its expired data.

All three databases wrap the size-tiered scheduler with a shared policy
(`common::storage::compaction::SegmentCompaction`). Each time the compactor
asks for work, size-tiered proposals go first, then the policy fills the
remaining `max_concurrent_compactions` slots, oldest segment first:

- **Drain.** A segment is dropped without being rewritten once both its end
  and its newest SST are older than the database's retention. Without a
  retention, nothing is drained.
- **Merge.** Once a segment has been closed for 5 minutes, all of its L0
  SSTs and sorted runs are merged into one sorted run. A late write adds an
  L0 SST again; the segment is merged again once the late SSTs add up to a
  sixteenth of its largest run, or a sixty-fourth once it has more than 8
  sources, so a trickle of late writes doesn't rewrite a large segment each
  time.
- **Cap.** A segment still taking writes is merged whenever it has more than
  8 L0 SSTs and sorted runs, so a query over the open hour never reads more
  than 8 of them. Size-tiered compaction alone leaves up to about 10.
- **Hold.** A [rollup](segment-rollup.md) segment is not merged while it is
  being copied, since reads can't see it until it is sealed. Once copied,
  its merge goes ahead of every other one, and the rollup is sealed after.
- **Supersede.** A database can mark segments whose data now lives in another
  segment, and the policy drains them like expired ones instead of merging
  them. Metrics marks an hour bucket once a sealed
  [rollup](../metrics/storage-format.md#rollup) bucket covers it; Logs and
  Traces mark a segment once a sealed [rollup](segment-rollup.md) segment
  covers it. That includes a window's rollup once the day's covers it.

Segments that already have a compaction running are skipped. Segments with
no fixed end, such as the Traces locator segment, are never merged or
drained by this policy.

On the `cold_read` bench (1,500 pods over 4 hours, 240 writes per hour), each
closed hour ends as one sorted run with no L0 SSTs. Before this policy, the
same load left about 20 SSTs per hour and 2.75x the final bytes, and cold
reads were 1.7–2.6x slower than over the merged layout. The extra merge adds
about half a segment's final size in compaction writes, roughly 20% more
total writes.

The policy reports:

| Metric | Labels | Meaning |
| --- | --- | --- |
| `storage_segment_compactions_total` | `path`, `kind` (`merge`, `drain`, `supersede`) | Compactions the policy started |
| `storage_compaction_oldest_active_seconds` | `path` | Age of the oldest running compaction |

Alert on `storage_compaction_oldest_active_seconds` staying high. Garbage
collection never deletes anything newer than the oldest running compaction,
so a stuck compaction stops `compacted/` from shrinking.

## SlateDB defaults

Every database starts from SlateDB's defaults with these overrides. Each
database can add its own on top (see the Metrics configuration), and a
settings file or `SLATEDB_` variables override any key.

| Setting | Default | SlateDB |
| --- | --- | --- |
| `garbage_collector_options.compacted_options.min_age` | 1 h | 24 h |
| `compactor_options.enable_trivial_move` | `true` | `false` |

With an L0 flush every 10 seconds and constant compaction, SlateDB's one-day
`min_age` keeps a day of dead SSTs in `compacted/`. One hour is still far
longer than a reader checkpoint lives (10 minutes). Garbage collection never
deletes an SST that a manifest or checkpoint references, so the shorter age
only affects files nothing reads. A trivial move relinks a compaction input
that doesn't overlap the others instead of rewriting it.

## Format changes

Metrics `metrics-timeseries/v4`, Logs `logs-log/v6` and Traces
`traces-trace/v7` changed their key formats together. Logs and Traces keys
now record each segment's width next to its start, and Metrics' bucket size
always means whole hours that divide 24. SlateDB stores the segment
extractor name and refuses to open a database written with a different
one, so a writer or reader started on an old database fails at startup.
There is no migration: delete each database's storage path (every
`shard-NNNN` prefix) and reingest.
