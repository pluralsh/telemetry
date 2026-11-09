# Writer concurrency and memory, end to end

What bounds a sharded writer's memory and throughput at each stage from an
HTTP write to compaction, and which signal shows that stage is the limit.
Every stage has a fixed or derived bound; none grows with load. The operator
sizes the derived ones from the writer's CPU and memory limits; see
[storage compaction](storage-compaction.md) for the compaction budget.

## Stages

| Stage | Concurrency | Memory bound | Full when |
|---|---|---|---|
| Request bodies | request size and in-flight body limits | `max_in_flight_body_bytes` (Logs 512 MiB) | HTTP 429 or 413 before decoding |
| Admission | per request | none | `storage_admission_rejections_total` |
| Write budget | one process-wide budget, and beside it half its size for rollup copies | `write_budget_bytes`/`memory.write_bytes`: Logs 16 MiB per core (at least 24 MiB), Traces 48 MiB, Metrics 192 MiB; with the copies' allowance resident at about 12, 9 and 6 times | ingest waits up to 1 s, then HTTP 429 (`logs_memory_budget_rejections_total` for Logs) |
| Write coordinator | one per database (per hour bucket for Metrics); encodes on the rayon pool; one commit in flight | a delta frozen at a third of the write budget | the write budget fills behind it |
| SlateDB memtables | one filling, `l0_flush_parallelism` uploading: Logs and Traces one per core from 2 to 6, Metrics 2 | `max_unflushed_bytes`, a 64 MiB memtable per upload and one filling (192 MiB at 2), about 2.5 times resident, plus about 90 MiB per upload | admission sheds from five eighths of it; at it SlateDB holds writes ("unflushed bytes" warnings) and the write budget fills behind it |
| L0 | one L0 compaction per segment at a time | `l0_max_ssts` (24 up to 2 uploads, 32 at 4, 48 at 6; Metrics 48) SSTs per segment on the object store | admission sheds from five eighths of the limit |
| Compaction | subcompactions up to the cores; compactions as the budget fits | `memory.compaction_bytes`, a subcompaction holding about `sources × prefetch + 90 MiB` | proposals deferred for memory; L0 depth rises |

## How the stages hold each other up

Each stage only fills when the one after it is slower. The chain runs from
the back:

1. Compaction falls behind when it waits to be scheduled, not only when it
   is busy. A segment takes one L0 compaction at a time, so the wait between
   two (the compactor's 1 s poll, then its worker's 1 s poll) adds L0 SSTs
   whatever the cores. Admission sees that as L0 depth and sheds.
2. L0 uploads fall behind when one 64 MiB SST is still uploading as the next
   memtable fills. SlateDB then holds every write until the upload is done.
   With one upload at a time this happened every few seconds at 10k logs/s
   on 4 cores, while the cores were half idle. Two uploads and 192 MiB of
   unflushed memtables leave room for one memtable filling beside two
   uploading. Each upload encodes its memtable on one task, though, so two
   cap L0 flushes at about two cores: on S3, 2-, 4- and 6-core Logs writers
   all flushed about 35 MB/s on under two cores and shed the rest of their
   ingest at five eighths of 192 MiB. Logs and Traces writers therefore
   upload one SST per core, from 2 to 6, while what the uploads past two
   hold fits an eighth of the memory limit, with a memtable of unflushed
   room per upload.
3. A held SlateDB write holds the coordinator's commit, so frozen deltas
   stay in the write budget, and ingest past it waits a second and gets
   a 429. At 10k logs/s on 4 cores, the write budget turned away 88 requests
   and admission 19.

So a 429 names its stage by its metric, not its message: both read
"writer is at capacity, retry later".

The write budget holds what ingest has applied until its flush is written,
so it must grow with the ingest rate the cores sustain: Logs' fixed 24 MiB
turned ingest away on a 2-core writer near its capacity whenever a commit
slowed. Rollup copies write through the same path, and when they drew on
the same budget a rollup on a busy writer took the room ingest needed; they
now wait in an allowance of their own.

## Derived limits

- **L0 limit.** Admission stops taking writes `backlog` SSTs short of
  `l0_max_ssts`, where the backlog is `max_unflushed_bytes /
  l0_sst_size_bytes` (rounded up) plus `l0_flush_parallelism`: 5 for Logs and
  Traces at 2 uploads (9 at 4, 13 at 6), 14 for Metrics' 16 MiB SSTs. It
  sheds from five eighths of the
  limit, so the limit is at least `8 (backlog + 3) / 3` for shedding to taper
  over 4 SSTs; `l0_max_ssts_for` rounds that up to a multiple of 8.
- **Compaction memory.** SlateDB splits a compaction into ranges of at least
  its largest input SST, up to `max_subcompactions`, and every range reads
  every source. An L0 compaction of four 64 MiB SSTs therefore runs as four
  subcompactions, however small it is beside `max_sst_size`, and the
  admission estimate counts it that way, each holding its prefetch and up
  to its share of the input as output. Merges take at most half the budget
  and `max_subcompactions` is what that half fits, so relief always has room
  for one compaction of the most subcompactions beside them. With a quarter
  kept for relief, a merge of a segment's runs held the next L0 compaction
  back 143 times in 8 minutes at 10k logs/s on 4 cores.
- **Operator reservations.** A writer's memory limit holds its caches and
  budgets plus fixed reservations: memtables and uploads (576 MiB for
  Traces and Metrics: 192 MiB at 2.5 times plus a second upload), the write
  path at its resident factor, and request bodies. A Logs writer reserves
  1,096 MiB, its write path at 12 times its write budget (288 MiB on 1 core,
  768 MiB on 4) and 512 MiB of bodies, and a Logs reader 736 MiB for
  replaying the writer's unflushed WAL. Every upload past two adds 250 MiB
  to a Logs or Traces writer (a 64 MiB memtable at 2.5 times and 90 MiB of
  parts) and 160 MiB to a Logs reader's replay. The operator renders the
  uploads as `storage.l0_flush_parallelism` on writers above 2; the server
  derives `max_unflushed_bytes` and `l0_max_ssts` from it, and a SlateDB
  settings file or `SLATEDB_` variables still override any of them.
