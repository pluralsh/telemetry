# Capacity planning and scaling

This page gives measured single-writer capacity for Metrics, Logs, and Traces,
CPU and memory recommendations derived from it, and the formulas for sizing a
sharded deployment. The numbers come from the [writer scale test](#how-the-numbers-were-measured)
on S3 Standard; they are not service-level objectives. Re-run it on your own
object store and instance type before committing to tight limits.

## Executive guidance

Run each writer, which is one storage shard, at **4 vCPU** and plan it at the
recommended rate below. Add writer replicas (shards) to grow, rather than
giving a writer more cores: each product's write path has one serial stage
per shard, so a writer stops gaining throughput at about 4 vCPU (see
[What limits one writer](#what-limits-one-writer)).

| Product | Recommended writer | Plan per writer at | Validated sustained | Saturation (knee) |
|---|---|---|---|---|
| Logs | 4 vCPU, 8 GiB | 39,000 entries/s (≈2.1 TB/day) | 52,500 entries/s | 85,400 entries/s |
| Metrics | 2–4 vCPU, 6 GiB | 300,000 (2 vCPU) – 450,000 (4 vCPU) samples/s | 406,000 – 599,000 samples/s | 477,500 – 705,000 samples/s |
| Traces | 4 vCPU, 6 GiB | 21,000 spans/s (≈10 MiB/s) | 28,400 spans/s | 39,400 spans/s |

The planning rate is 75% of the validated sustained rate, which leaves room for
traffic bursts, rollups, compaction catch-up, and shard handoff. The validated
rate is itself the highest rate a writer held for 10 minutes with no errors, a
p99 under 1 second, and an L0 backlog that never reached admission shedding.

For context, one 4-vCPU Logs writer at its planning rate ingests about the
2 TB/day tier that [OpenShift's Loki profiles][openshift-loki] size at
54 requested vCPU across a replicated cluster, and one Metrics writer covers
roughly 5,600–8,400 node-exporter targets at a 15-second scrape interval.

These numbers assume batched ingestion, default `applied` durability, no
writer-side reads, and an object store in the same region. Do not multiply
them by the writer count without checking shared object-store, network, and
request-rate limits.

Increase writer replicas when any one writer sustains more than its planning
rate, starts turning writes away with `429`, or holds more than 65% of its
memory. CPU is a poor trigger: a writer reaches its limit with half its cores
idle. Writer replicas are storage
shards, so an increase appends a routing epoch: records timestamped after the
next cutover spread across the larger shard count, while existing data stays on
its original shards. Scale-down remains blocked.

Scale readers independently from writers. Query concurrency, cache hit rate,
scan width, and object-store GET latency determine reader count and are not
covered by the ingestion estimates on this page. With
[reader groups](sharding.md#reader-groups), readers scale per storage shard:
see [Reader groups and cost](#reader-groups-and-cost).

## Measured writer capacity

Each row is one writer at the given CPU limit. "Validated" is the highest
10-minute soak that passed; "knee" is where a slow ramp first saw sustained
rejections, latency, or falling behind the offered rate. CPU and peak RSS are
the writer's mean cores and peak memory during the validated soak, or the
lowest soak tried where none passed.

| Writer | Validated sustained | Knee | p99 | Mean CPU | Peak RSS |
|---|---|---|---|---|---|
| Logs, 2 vCPU / 8 GiB | none at ≥ 40,000 entries/s | 65,200 entries/s | — | 1.3 | 2.8 GiB |
| Logs, 4 vCPU / 12 GiB | 52,500 entries/s, 34 MiB/s | 85,400 entries/s | 13 ms | 1.7 | 2.7 GiB |
| Logs, 6 vCPU / 16 GiB | 58,900 entries/s, 39 MiB/s | 95,900 entries/s | 14 ms | 1.8 | 3.2 GiB |
| Metrics, 2 vCPU / 8 GiB | 406,000 samples/s | 477,500 samples/s | 161 ms | 1.1 | 0.9 GiB |
| Metrics, 4 vCPU / 12 GiB | 599,000 samples/s | 705,000 samples/s | 60 ms | 1.8 | 1.2 GiB |
| Metrics, 6 vCPU / 16 GiB | 644,000 samples/s | 757,500 samples/s | 70 ms | 1.9 | 1.7 GiB |
| Traces, 2 vCPU / 8 GiB | none at ≥ 17,300 spans/s | 28,200 spans/s | — | 1.4 | 1.1 GiB |
| Traces, 4 vCPU / 12 GiB | 28,400 spans/s, 14 MiB/s | 39,400 spans/s | 38 ms | 2.4 | 1.5 GiB |
| Traces, 6 vCPU / 16 GiB | 26,800 spans/s, 13 MiB/s | 43,700 spans/s | 25 ms | 3.7 | 1.9 GiB |

Byte rates are encoded request bodies. A Logs entry averages about 636 bytes of
line text (688 bytes on the wire as Loki JSON), so the 4- and 6-vCPU writers
sustain about 2.9 and 3.2 TB/day of log lines. At overload, peak RSS reached
3.9 GiB for the 4-vCPU Logs writer and 4.6 GiB for the 6-vCPU one, and
2.2 GiB for Traces; the memory recommendations above leave at least twice that.

The smallest sizes did not pass a soak at the rates tried. The 2-vCPU Logs
writer turned away about 11% of requests even at 40,000 entries/s, and the
2-vCPU Traces writer turned away 6 of 5,200 requests in 10 minutes at
17,300 spans/s, just over the 0.1% a soak allows.
Until a lower soak validates them, plan 2-vCPU Logs writers at no more than
25,000 entries/s and 2-vCPU Traces writers at no more than 12,000 spans/s, or
use 4 vCPU.

### What limits one writer

Writers use only 2–4 of their cores at their limit because each product's
write path has one stage that runs serially per shard, and that stage
saturates first:

- **Logs**: committing each flushed delta to SlateDB. Below the knee a commit
  is in flight about 5% of the time; past it, one commit is always in flight
  and each takes 0.3–0.9 seconds instead of 10 ms, so requests queue behind
  the write buffer and are turned away with `429`.
- **Traces**: encoding each flushed delta into pages and indexes, about
  0.6 seconds per flush, busy 100% of the time from about 35,000 spans/s.
- **Metrics**: applying samples to the live delta in the write coordinator,
  busy 99% of the time from about 690,000 samples/s.

More cores therefore help only until that stage is the limit: from 4 to 6 vCPU
Logs gained 12%, Metrics 7%, and Traces nothing. Spare cores still matter for
rollups, compaction, and recovery, which is why the recommendation is 4 vCPU
rather than 2. To grow past one writer's rate, add writer replicas.

### How the numbers were measured

The [scalability runner][scalability-runner] sent traffic over 64 concurrent
connections straight to each writer's Service, on dedicated `m8i.2xlarge`
nodes in `us-east-2` writing to S3 Standard in the same region, with readers
idle. For each writer it ran:

1. A 15-minute linear ramp of the offered rate, from well below to well above
   the writer's capacity, to find the knee.
2. A 10-minute soak at 85% of the knee, retried lower up to three times until
   one passed. Before each soak the writer drained its L0 backlog and
   restarted, so soaks start on a populated store rather than an empty one.

A 10-second window was clean when fewer than 2% of its requests got `429`,
nothing else failed, p99 stayed under 1 second, SlateDB did not stall, and RSS
stayed under 90% of the limit. A soak passed only if every window was clean,
fewer than 0.1% of all its requests were turned away, and L0 depth never
reached where admission starts shedding.

The workloads were the runner's defaults except where noted:

- Logs: Loki JSON, 1,600 entries per request across 128 streams, rotating
  through 10,000 active streams; 70% 250-byte, 25% 1 KiB, and 5% 4 KiB lines.
- Metrics: Snappy remote-write, 5,000 samples per request over 500,000 stable
  series with no churn.
- Traces: OTLP/HTTP protobuf, 2,000 spans per request, five spans per trace,
  and a 256-byte attribute per span, about 512 bytes per span on the wire.

The runs used release v0.3.29 and lasted about 90 minutes per writer, so they
include compaction and the first window rollups but not the daily rollup.
They do not measure reads, series churn, or object-store request costs; size
readers and churn-heavy Metrics workloads separately.

## What limits a SlateDB writer

SlateDB writes its WAL and LSM data to object storage. Local disk is an optional,
disposable cache tier; it is not the primary durability path. The important
writer limits are:

1. CPU used by encoding, indexing, compression, and compaction.
2. Network bandwidth generated by WAL, L0, and compaction traffic.
3. Object-store PUT latency, throughput, and request limits.
4. Memory for unflushed data, active product state, and caches.
5. L0 or compaction backlog that causes write throttling.

Fast local NVMe helps Foyer read caching and mixed read/write workloads, but it
does not remove object-store durability latency.

SlateDB groups many application writes into WAL and L0 objects. One telemetry
request does not normally equal one S3 PUT. Application batching still matters
because it reduces product-level locks, page fragmentation, metadata updates,
and indexing work before data reaches SlateDB.

## Public SlateDB reference points

The [SlateDB 0.15 release benchmarks][slatedb-benchmarks] use 20-byte keys,
400-byte values, 64 closed-loop clients, S3 in `us-east-1`, and a large ARM
runner. The dataset for steady-state workloads contains 300 million records
and approximately 120 GiB of logical data.

The [sustained-ingest workload][slatedb-sustained] reached approximately:

- 299,000 puts/s and 120 MiB/s of logical application writes.
- 588 MiB/s of object-store PUT traffic and 464 MiB/s of GET traffic.
- 8.6 CPU cores and 28 GiB RSS on average.

This is an optimistic ceiling: the database begins empty, keys are unique and
sequential, and WAL is disabled for that workload. Its roughly 5x object-store
PUT amplification also shows why network and compaction must be measured.

The [update-heavy workload][slatedb-update] is closer to a live indexed
database. It uses WAL, runs 95% updates and 5% point reads, and reached:

- 84,000 puts/s and 34 MiB/s of logical application writes.
- 272 MiB/s of object-store PUT traffic, about 8x logical write bandwidth.
- 5.3 CPU cores and 20 GiB RSS on average.
- 246 ms average time from write acceptance to remote durability.

The [balanced workload][slatedb-balanced] reached only 6,200 puts/s alongside
6,200 gets/s because remote cache misses hold back a task that mixes reads and
writes. This supports Telemetry's separation of writer and reader roles.

SlateDB's [tuning guidance][slatedb-tuning] quotes expected object-store write
latency of 50–100 ms for S3 Standard, 5–10 ms for S3 Express One Zone, and
5–20 ms for MinIO. Synchronous small writes therefore require group commit or
application batching. `written` ingestion does not wait for each object-store
round trip; `durable` ingestion does.

## Shared write-buffer behavior

Metrics, Logs, and Traces use the same bounded, per-storage-shard write
coordinator. A request is validated and merged into a live in-memory delta;
the coordinator freezes deltas by size, time, or an explicit durability
barrier. Logs coalesces entries by namespace, segment, and stream fingerprint.
Traces coalesces traces by namespace and segment.
This allows separate client requests to share product page and index work.

The requested durability level controls acknowledgement:

- `applied` acknowledges after the write has entered the live delta. It gives
  the most opportunity for cross-request coalescing but can lose acknowledged
  data if the process exits before a flush. This is the default for telemetry
  throughput.
- `written` freezes through the request's epoch and waits until its coalesced
  batch has entered SlateDB's writable state. It does not force a remote
  object-store flush.
- `durable` also waits for SlateDB's object-store flush. Its latency therefore
  includes object-store PUT latency.

All three products default `write.flush_interval_seconds` to 10 seconds. For
`applied` and `written` writes, read-only replicas normally observe data within
roughly 0–10 seconds plus manifest polling and flush latency; size-triggered or
explicit flushes can make it visible sooner. This durable interval is separate
from `buffer_flush_interval_milliseconds`, which controls when Logs and Traces
freeze and apply their live in-memory deltas.

The queue is deliberately bounded. A full queue produces protocol-specific
backpressure (`429` plus `Retry-After` for HTTP and `UNAVAILABLE` for OTLP or
Jaeger gRPC) rather than unbounded memory growth. Storage or drain
unavailability returns `503` or `UNAVAILABLE`; clients should reroute or retry.
OpenTelemetry Collector exporters and Grafana Alloy retry these responses with
backoff by default. Grafana Alloy also retries Loki and Prometheus remote-write
`429` responses by default. Stock Prometheus requires
`remote_write.queue_config.retry_on_http_429: true`; without it, Prometheus
drops a batch rejected with `429`. All conforming remote-write senders retry
the `5xx` responses used for transient storage and drain failures.
Legacy applications that send directly with native Zipkin or Jaeger client
reporters commonly do not retry rejected batches at all. Put an OpenTelemetry
Collector or another durable agent between those applications and Telemetry
when overload loss is unacceptable; changing the response code cannot make
those legacy reporters replay a dropped batch.

Plan coordinator memory per open storage shard as:

```text
live delta + up to two frozen deltas + queued request payloads
```

`buffer_size_threshold_bytes` bounds each delta approximately, not total
process memory. `buffer_queue_capacity` counts queued commands rather than
bytes. Lower `buffer_flush_interval_milliseconds` reduces `applied` latency and
drain work but also reduces batching. Raise the size threshold only after
measuring process RSS and flush latency.

During a shard handoff, the drain gate waits for already-admitted writes to
finish, rejects later local writes, then runs `drain → flush → close → lease
release`. The flush advances buffered writes through durable storage before the
next owner opens the shard, so every acknowledged pre-drain write is included.

## Product write behavior

### Metrics

Metrics first combines samples in per-bucket deltas. `written` flushes those
deltas into SlateDB's writable state without waiting for object storage, and a
periodic flush establishes remote durability and reader visibility. Repeated
samples for stable series reuse schema and posting state.

Metrics capacity depends on both samples/s and active-series behavior:

- Stable series primarily exercise sample encoding and delta merging.
- New or changed series also create schema and posting records.
- High churn can become the limit even when sample throughput is moderate.
- Classic histograms create one series per bucket, plus `_sum` and `_count`. Native histograms (remote-write histograms and OTLP exponential histograms) are stored as one series with larger samples.

### Logs

Logs turns each frozen delta into coalesced SlateDB `WriteBatch` operations.
Default `applied` requests can coalesce across the configured time and size
window. `written` requests force a coordinator barrier but not an object-store
flush. The common periodic durable flush makes complete accepted batches
visible to read-only replicas.

Large batches let Logs fill its approximately 1 MiB pages, resolve a stream once
for more entries, and combine posting updates. Small requests increase lock,
sequence, page, and index work per log byte. Stream cardinality and churn should
therefore be tested independently from byte throughput.

### Traces

Traces also coalesces pages, locators, metadata, and posting fragments before
committing atomic SlateDB `WriteBatch` operations, followed by periodic
durable flushes that establish reader visibility.
Trace batches should be large enough to fill approximately 1 MiB pages without
turning an entire object-store buffer object into one oversized transaction.

Traces capacity is sensitive to average span size, spans per trace, attribute
count, indexed-attribute cardinality, and continuation writes to existing
traces.

## Batching and an optional ingest buffer

Clients should normally send or coalesce 1–4 MiB per-shard writes with a
10–50 ms maximum aggregation delay. This is large enough to amortize product
overhead while bounding memory, tail latency, and handoff drain time.

An OpenData buffer in front of Telemetry can durably absorb bursts and merge
many small requests before calling a writer. To improve database throughput,
the consumer must decode entries, group them by product, namespace, and routing
shard, and submit larger product-level writes. Replaying every original request
individually preserves availability benefits but leaves most database overhead
unchanged.

The buffer does not increase SlateDB's physical compaction or network ceiling.
It can increase useful telemetry throughput by reducing application-level
write amplification. For example:

```text
raw telemetry capacity
    = measured SlateDB logical-write capacity
      / measured application write amplification
```

Reducing application amplification from 3x to 1.5x doubles useful telemetry
throughput at the same SlateDB logical-byte ceiling.

Exactly-once buffered ingestion requires care. A single queue sequence that
fans out to several physical shards cannot be committed atomically without a
distributed transaction. Prefer buffer partitions that follow the storage-shard
hash ranges of the current routing epoch so each queue sequence targets one
physical database, or accept at-least-once delivery and make product writes
idempotent.

## Translating throughput into production units

### Logs: logs/day and entries/s

Grafana's [Loki deployment guidance][loki-modes] positions monolithic mode for
approximately 20 GB/day and simple scalable mode close to 1 TB/day before
microservices mode is preferred. Red Hat's [OpenShift Loki profiles][openshift-loki]
define production tiers of:

- 100 GB/day with 14 requested vCPU and 31 GiB memory cluster-wide.
- 500 GB/day with 34 requested vCPU and 67 GiB memory.
- 2 TB/day with 54 requested vCPU and 139 GiB memory.

Those figures include replicated, distributed Loki components and query
capacity, so they are context rather than an apples-to-apples writer comparison.
Telemetry stores one unreplicated SlateDB shard per writer and scales readers
separately.

Use an approximately 650-byte average event for an initial mixed workload:
70% 250-byte events, 25% 1 KiB events, and 5% 4 KiB events. With that mix:

- 500 GB/day is approximately 5.8 MB/s and 8,900 entries/s.
- 1 TB/day is approximately 11.6 MB/s and 17,800 entries/s.
- 2 TB/day is approximately 23.1 MB/s and 35,500 entries/s.
- 4 TB/day is approximately 46.3 MB/s and 71,000 entries/s.

The scale test used a similar mix, about 636 bytes per line. One 4-vCPU writer
sustained 52,500 entries/s, about 2.9 TB/day, and its 39,000 entries/s planning
rate is about 2.1 TB/day: above the OpenShift 2 TB/day tier on one writer.

Grafana recommends 5–10 stable labels, reasonably sized lines, and explicit
tenant rate limits. Benchmark at least 1,000, 10,000, and 100,000 active streams
because stream churn and posting cardinality can force scaling before bytes/s.

### Metrics: samples/s and active series

Grafana's [Mimir capacity guide][mimir-capacity] estimates distributor resources
at 1 CPU and 1 GiB memory per 25,000 samples/s. It estimates ingester resources
at 1 CPU, 2.5 GiB memory, and 5 GB disk per 300,000 in-memory series. Mimir
normally replicates ingested series, while Telemetry currently relies on the
shared object store and does not replicate shards in-cluster.

A production-like node-exporter workload has approximately 800 series per
target. At a 15-second scrape interval:

- 1,000 targets produce approximately 53,000 samples/s and 800,000 series.
- 5,000 targets produce approximately 267,000 samples/s and 4 million series.
- 10,000 targets produce approximately 533,000 samples/s and 8 million series.
- 20,000 targets produce approximately 1.07 million samples/s and 16 million
  series.

One 4-vCPU writer sustained 599,000 samples/s over 500,000 stable series using
about 1.8 cores, and plans at 450,000 samples/s. Mimir's distributor ratio
alone would put 450,000 samples/s at 18 CPUs before its separate, replicated
ingesters. The comparison covers ingestion only: the test held series stable,
and churn creates schema and posting work it did not exercise.

Run separate steady-state, 1% hourly churn, and 10% hourly churn tests. Report
active series and new series/s alongside samples/s. Prometheus remote-write
wire size varies from a few bytes to tens of bytes per sample depending on
protocol version, labels, and batching, so network bytes/sample is not a stable
capacity unit by itself.

### Traces: spans/s

Tempo's [ingestion guidance][tempo-ingestion] uses an approximately 500-byte
typical span. Its documented 15 MB/s per-distributor default corresponds to
about 30,000 spans/s, and three distributors with local rate limiting provide
about 90,000 spans/s of aggregate admission capacity.

At the same 500-byte average:

- 10 MiB/s is approximately 21,000 spans/s.
- 20 MiB/s is approximately 42,000 spans/s.
- 30 MiB/s is approximately 63,000 spans/s.

One 4-vCPU writer sustained 28,400 spans/s of about 512 bytes, roughly one
Tempo default distributor admission envelope, and plans at 21,000 spans/s. Tempo
uses separate downstream components and different durability, replication, and
query architecture, so this comparison is a workload translation rather than a
claim of equivalent end-to-end capacity.

Benchmark small and large traces separately. Keep average bytes/span, spans per
trace, attributes/span, indexed attributes, and concurrently live traces fixed
and reported.

## Writer resources

Set writer requests equal to limits, so a writer's CPU is not taken by
neighbours when a rollup or compaction catches up:

```yaml
spec:
  writer:
    resources:
      requests: { cpu: "4", memory: 8Gi } # Logs; 6Gi for Metrics and Traces.
      limits: { cpu: "4", memory: 8Gi }
```

The operator sizes the writer's memory budgets, L0 flush parallelism, and
rollup concurrency from these limits, so they also bound what the writer
buffers. Writers need at least a 6 GiB limit for rollups of closed days, and
the operator does not roll out one with less. Logs' 8 GiB is about twice the
peak RSS the scale test saw at overload; Metrics and Traces peaked under
2.2 GiB and get the 6 GiB minimum. The test ran with more memory than this
(12 GiB at 4 vCPU), so check RSS against the limit when first deploying.
Metrics memory grows with active series: the test used 500,000.

Metrics is the most CPU-efficient at 2 vCPU, which reaches about 70% of a
4-vCPU writer's rate with half the cores; use 2-vCPU Metrics writers for small
deployments or where more shards are cheaper than larger pods. Logs and Traces
should use 4 vCPU.

Provide at least 1 Gbit/s of network per writer. SlateDB's public results show
object-store traffic 5–8 times logical application write bandwidth during
compaction, so application ingress alone understates required network
capacity.

## Reader groups and cost

Kubernetes sharded deployments run one reader group per storage shard by
default. For `S` storage shards and `r` replicas per group, the reader tier
is `S × r` pods, and each pod opens one shard. Without groups, `R` reader pods
each open all `S` shards.

What changes for the same per-shard availability (`r = R`):

- **Pods and fixed memory.** Groups run `S` times as many reader pods, so the
  process baseline (runtime, connection pools, and the configured block and
  meta cache budgets) is paid `S × r` times instead of `R` times. Per-shard
  SlateDB reader state and WAL replay buffers are the same in total
  (`r × S` copies either way), but each group pod holds only one.
- **Cache efficiency.** A group pod's cache serves one shard's working set
  rather than the whole dataset's. A per-pod cache of about `1 / S` of the
  former size gives a similar hit rate, which offsets most of the extra
  baseline. Size `storage.block_cache` for one shard's hot data, not the
  dataset's.
- **Object-store requests.** Manifest polling scales with the number of
  (pod, shard) pairs, `r × S`, which is unchanged at `r = R`. Cache-miss GETs
  per shard scale with that shard's group size, and better hit rates
  usually lower them.
- **Network transfer.** A query moves the partial results of `S - 1` remote
  shards to the coordinating reader: Metrics moves the selected series'
  chunk-encoded samples, Logs moves matched lines or per-shard aggregates,
  and Traces moves matching traces and hits. Reading the local shard costs
  nothing. When groups span availability zones, most remote traffic crosses a
  zone, which cloud providers bill per GiB in each direction (for example
  $0.01/GiB each way between AWS zones in one region):

  ```text
  monthly cross-zone cost ≈
      queries per month
      × average remote response bytes per query
      × (S - 1) / S
      × fraction of peers in another zone (≈ 2/3 across three zones)
      × billed price per GiB (both directions)
  ```

  Metrics range queries over many series move the most bytes, because PromQL
  runs on the coordinator. Aggregating Logs metric queries move the least.
- **CPU.** Shard work runs in parallel across groups rather than in one
  process, which lowers latency for wide queries. Total CPU is about the
  same plus protobuf encoding and decoding of remote results.
- **Hedging.** A unary call that has not answered after `hedge_after_ms` is
  started on a second replica, so the slowest calls can run twice. Streaming
  reads, which carry nearly all the bytes and memory, are never hedged. Raise
  the delay or set it to zero to trade tail latency for less duplicate work.

Groups pay off when the dataset, shard count, or query load is large enough
that one process cannot open, cache, and scan every shard efficiently. With
one storage shard, groups only change resource names. Small deployments can
set `spec.reader.readGroups.enabled: false` to keep a single reader
StatefulSet whose pods open every shard.

### Reader autoscaling

`spec.reader.autoscaling` creates one `HorizontalPodAutoscaler` per reader
StatefulSet, one per group when groups are enabled. A hot shard's group
therefore scales without scaling the others:

```yaml
spec:
  reader:
    replicas: 2 # Used as minReplicas when autoscaling omits it.
    autoscaling:
      enabled: true
      minReplicas: 2
      maxReplicas: 8
      targetCPUUtilizationPercentage: 70 # Default when no target is set.
      # targetMemoryUtilizationPercentage: 80
      # behavior: {} # autoscaling/v2 scale-up and scale-down policies.
    podTemplate:
      spec:
        containers:
          - name: metrics
            resources:
              requests: { cpu: "2", memory: 8Gi }
```

CPU is the default target because query load is CPU-bound once the cache is
warm. A memory target is optional and is combined with the CPU target when
both are set. Utilization is relative to the container's requests, so the
reader container must request the targeted resources. The operator leaves the
replica count of an autoscaled StatefulSet to its HorizontalPodAutoscaler.
Every reader StatefulSet also gets a PodDisruptionBudget with
`maxUnavailable: 1`.

The floor of the reader tier is `S × minReplicas` pods. Keep `minReplicas` at
two or more where a shard must stay readable during node drains, because a
group with one replica cannot serve its shard while that pod restarts.

## Required benchmark matrix

Use the opt-in [deployed scalability runner][scalability-runner] to generate
Logs, Metrics, and Traces traffic. It is a standalone package and is not part of
the standard regression suite.

Run each product for at least 30 minutes after warmup, then continue until
compaction reaches steady state. Short tests that stop before compaction catches
up overstate sustainable throughput.

For every workload, sweep load until one of the following occurs:

- Request latency or error rate violates the intended service objective.
- CPU remains above 80% or memory approaches its configured bound.
- SlateDB applies sustained write backpressure.
- L0 or compaction backlog grows without recovering.
- Object-store traffic or request throttling reaches its limit.
- Reader visibility lag exceeds the configured interval plus an allowed margin.

Record:

- Raw request bytes/s and decoded telemetry bytes/s.
- Entries/s, samples/s, or spans/s.
- Active streams, active series, or live traces and their churn.
- Batch-size and request-latency distributions.
- Logical bytes and operation count submitted to SlateDB.
- Object-store GET, PUT, and transferred bytes.
- CPU, RSS, cache hit rate, network, and local cache I/O.
- L0 count, compaction backlog, write stalls, and durability lag.
- Read latency under a representative concurrent query workload.

The minimum workload set is:

1. Direct, small client requests to expose request overhead.
2. Direct, 1–4 MiB client batches.
3. Buffer-coalesced 1–4 MiB per-shard writes when the buffer integration exists.
4. Default `applied` durability, with `written` as a throughput comparison.
5. `durable` mode as a latency-focused comparison.
6. Stable-cardinality and high-churn variants.
7. Concurrent representative reads to detect cache and object-store contention.
8. A scale-up across a routing-epoch cutover while running at 65% of validated
   steady-state capacity.

Publish the saturation result, the validated sustained result, and the
planning rate after applying 25% operational headroom to the validated one.

## Scaling calculation

For a measured product and workload:

```text
required writers =
    ceil(
        peak telemetry rate
        / validated per-writer sustained rate
        / 0.75
    )
```

`validated sustained rate × 0.75` is the planning rate in
[Executive guidance](#executive-guidance). For example, 150,000 log entries/s
at peak needs `ceil(150000 / 52500 / 0.75) = 4` writers of 4 vCPU.

When only byte measurements are available:

```text
required writers =
    ceil(
        peak raw bytes/s
        * measured application write amplification
        / measured SlateDB logical bytes/s per writer
        / 0.65
    )
```

Use peak sustained rate rather than daily average. If an ingest buffer is
deployed, writers may instead be sized to drain bursts within a stated backlog
SLO:

```text
drain time =
    buffered burst bytes
    / (aggregate writer drain bytes/s - continuing ingest bytes/s)
```

The denominator must remain positive. A buffer absorbs a burst; it does not
make an under-provisioned steady-state write path sustainable.

[slatedb-benchmarks]: https://slatedb.io/docs/operations/benchmarks/
[slatedb-sustained]: https://benchmark.slatedb.io/0.15.0/workload/sustained-ingest/
[slatedb-update]: https://benchmark.slatedb.io/0.15.0/workload/update-heavy/
[slatedb-balanced]: https://benchmark.slatedb.io/0.15.0/workload/balanced/
[slatedb-tuning]: https://slatedb.io/docs/operations/tuning/
[loki-modes]: https://grafana.com/docs/loki/latest/get-started/deployment-modes/
[openshift-loki]: https://docs.redhat.com/en/documentation/openshift_container_platform/4.16/html/logging/log-storage
[mimir-capacity]: https://grafana.com/docs/mimir/latest/manage/run-production-environment/planning-capacity/
[tempo-ingestion]: https://grafana.com/docs/tempo/latest/operations/manage-trace-ingestion/
[scalability-runner]: ../../tests/scalability/README.md
