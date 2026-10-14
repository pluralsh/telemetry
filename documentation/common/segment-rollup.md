# Segment rollup

SlateDB never compacts across segments, and Logs and Traces route every
`(namespace, time segment)` into its own segment, so a day of hour segments
stays 24 segments however long it sits, and a query over the day opens all 24.
A writer therefore rolls each closed day up into wider segments. Metrics does
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

## Rolling up a day

1. Once a UTC day ended `rollup.delay_seconds` ago, the writer tiles it with
   the fewest aligned segments of 24, 12, 8, 6, 4, 3 or 2 hours (whole
   multiples of `segment_duration_seconds`; for Logs also dividing
   `discovery_rollup_seconds`) whose segments' stored SST bytes sum to at most
   `rollup.max_segment_bytes`. A segment too large to share stays as it is,
   and a window with at most one stored segment is left alone.
2. For each window it writes the pending marker and, with no write in flight,
   routes new writes for the window's segments to the rollup segment, then
   writes out whatever was pending for them.
3. It re-writes every row (Logs) or trace (Traces) of the covered segments
   through the ordinary write path into the rollup segment, which so gets
   its own streams, objects, pages, postings and discovery catalog. A trace
   whose only page is in a covered segment gets a head that replaces its old
   one, so trace-by-ID stays one point read; any other trace gets a
   continuation into the rollup segment. While copying a batch of traces the
   writer holds new writes and flushes pending ones, so no head it reads
   goes stale before its copy is applied.
   The writer accepts copied data before writing it, so once
   `rollup.copy_buffer_bytes` of it are queued the copy waits until they are
   written. Traces copies in steps of about that many page bytes.
4. It writes the sealed marker durably, after the copy, and flushes the
   memtable so the manifest lists it.

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

## Configuration

```yaml
rollup:
  enabled: true
  max_segment_bytes: 4294967296 # Largest rollup segment by covered bytes; 4 GiB.
  delay_seconds: 3600 # How long after a UTC day ends before it is rolled up.
  interval_seconds: 300 # How often closed days are looked for.
  copy_buffer_bytes: 67108864 # Copied bytes queued before waiting for storage; the operator renders 5% of the memory limit, at least 32 MiB.
```

Reader-mode servers never roll up.

A rollup holds up to about one and a half times its segment's stored size in
memory, so with the 4 GiB default the operator gives writers and standalone
servers an 8Gi memory limit unless one is set, and does not roll out a writer
whose limit is below 6Gi.
