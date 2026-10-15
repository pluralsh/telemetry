# Segment rollup

SlateDB never compacts across segments, and Logs and Traces route every
`(namespace, time segment)` into its own segment, so a day of hour segments
stays 24 segments however long it sits, and a query over the day opens all 24.
A writer therefore rolls each closed 4-hour window up into one segment, and
each closed day into the fewest segments its windows fit, so the app's 1h,
4h, 1d, 2d and 7d ranges each open a handful of segments. Metrics does
the same with its own bucket records; see
[Metrics rollup](../metrics/storage-format.md#rollup).

## Markers

A rollup segment's state is recorded by two empty records, each the whole
key scope of the rollup segment with a flag set in the width:

```text
KEY   ┌───────────┬─────────┬─────────────────┬───────────────┬──────────────────────────┐
      │ subsystem │ version │    namespace    │ segment start │ segment width | flag     │
      └───────────┴─────────┴─────────────────┴───────────────┴──────────────────────────┘
VALUE empty

flag  1 << 61  pending: the rollup segment takes the writes of the segments it covers
      1 << 62  sealed: the rollup segment holds every record of the segments it covers
```

Each marker is its own SlateDB segment, so the manifest's segment list alone
tells which rollups exist. Markers expire with the retention and are dropped
with the span of their rollup segment.

## Rolling up windows and days

1. Each UTC day is split into aligned windows of `rollup.window_seconds`
   (4 hours by default). Once a window ended `rollup.delay_seconds` ago and
   holds no rollup, the writer tiles it with the fewest aligned segments
   (whole multiples of `segment_duration_seconds`; for Logs also dividing
   `discovery_rollup_seconds`) whose segments' stored SST bytes sum to at
   most `rollup.max_segment_bytes`. A segment too large to share stays as it
   is, and a tile with at most one stored segment is left alone.
2. Once the day ended `rollup.delay_seconds` ago and every window's rollup
   is sealed, it tiles the day the same way from those rollups, with
   segments of 24, 12 or 8 hours wider than the window. A day with no
   rollup yet, such as one written by a backfill, is tiled straight from its
   segments.
3. For each tile it writes the pending marker and, with no write in flight,
   routes new writes for the tile's segments to the rollup segment, then
   writes out whatever was pending for them. Compaction leaves the rollup
   segment alone while it is copied.
4. It re-writes every row (Logs) or trace (Traces) of what
   reads see of the tile, each window's sealed rollup in place of its
   segments, through the ordinary write path into the rollup segment, which
   so gets its own streams, objects, pages, postings and discovery catalog.
   A trace whose only page is in a covered segment gets a head that replaces
   its old one, so trace-by-ID stays one point read; any other trace gets a
   continuation into the rollup segment. While copying a batch of traces the
   writer holds new writes and flushes pending ones, so no head it reads
   goes stale before its copy is applied.
   The writer accepts copied data before writing it, so once
   `rollup.copy_buffer_bytes` of it are queued the copy waits until they are
   written. Traces copies in steps of about that many page bytes.
5. It flushes the copy, asks compaction to merge the rollup segment ahead of
   anything else, and waits until the copy is one SST or sorted run, at most
   `rollup.merge_wait_seconds`, so a sealed rollup reads as one file.
6. It writes the sealed marker durably and flushes the memtable so the
   manifest lists it.

So a window is readable as one merged segment a little over
`rollup.delay_seconds` after it ends, plus the copy and the merge, and the
day the same after the day ends.

## Reads

Reads load the markers from the manifest. A sealed rollup segment replaces
every segment it covers; an unsealed one is ignored, so a query sees either
the segments or their rollup, never both. Traces also skips locators into a
covered segment. It follows locators into an unsealed rollup, since a
replaced head points only there, and merging a trace's pages drops spans
read twice.
Compaction then drains the covered segments (see
[storage compaction](storage-compaction.md)).

A rollup interrupted before its seal is copied again from the start on the
next pass. Logs queries drop identical rows, Traces skips traces whose head
already points into the rollup segment and drops identical spans when
merging a trace's continuations, so twice-copied data reads once.

Copied data expires a retention after the copy, not after its first write, so
a rolled-up day is kept up to `rollup.delay_seconds` plus a day longer.

Each record is written about three times more than without rollups: into
its hour, its window and its day. A day over `rollup.max_segment_bytes`
stays as its windows or rolls into 8- or 12-hour segments, so is not copied
again.

## Configuration

```yaml
rollup:
  enabled: true
  max_segment_bytes: 4294967296 # Largest rollup segment by covered bytes; 4 GiB.
  delay_seconds: 600 # How long after a window or UTC day ends before it is rolled up.
  window_seconds: 14400 # Width of the windows rolled up as the day goes; 4 hours.
  merge_wait_seconds: 1800 # Longest a rollup waits for its merge before it is sealed anyway.
  interval_seconds: 300 # How often closed windows and days are looked for.
  copy_buffer_bytes: 67108864 # Copied bytes queued before waiting for storage; the operator sizes it from the memory limit.
```

Reader-mode servers never roll up.

A rollup holds up to about one and a half times its segment's stored size in
memory, so with the 4 GiB default the operator gives writers and standalone
servers an 8Gi memory limit unless one is set, and does not roll out a writer
whose limit is below 6Gi.
