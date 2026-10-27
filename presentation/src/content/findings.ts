export type Finding = { title: string; body: string; stat?: string; tag?: string; color?: string };

/** Optimisations beyond ALP and BM25. Sources noted per item. */
export const OPTIMIZATIONS: Finding[] = [
  {
    title: "SIMD + columnar execution",
    tag: "query",
    body: "Hot filters and aggregations run over decoded column vectors with SIMD instead of row-at-a-time iterators.",
  },
  {
    title: "Rollups + rollup-aware planner",
    tag: "query",
    body: "Summary rollups are maintained per window, and the planner answers wide aggregations from them instead of raw samples.",
  },
  {
    title: "Foyer hybrid cache",
    tag: "storage",
    body: "Role-aware RAM + NVMe block cache, sized against query memory headroom so small deployments don't OOM.",
  },
  {
    title: "Cache warming",
    tag: "storage",
    body: "Freshly flushed SSTs are pushed into reader caches on the flush cadence, so new data is hot before it's queried.",
  },
  {
    title: "Object-store request dedupe",
    tag: "storage",
    body: "Concurrent reads of the same range collapse into a single GET.",
  },
  {
    title: "Log page compaction",
    tag: "logs",
    body: "Small log pages are merged into larger ones in the background, so reads touch fewer, bigger objects.",
  },
  {
    title: "Traces page cache by decoded size",
    tag: "traces",
    body: "The traces cache is weighted by decoded column bytes rather than compressed size, so its memory bound is real.",
  },
  {
    title: "Native histogram compression",
    tag: "metrics",
    body: "Native histograms are delta-encoded against the previous sample in the same chunk.",
  },
];

/** Divergences in upstream systems surfaced by differential fuzzing. */
export const UPSTREAM_BUGS: Finding[] = [
  {
    title: "Floating point in Go",
    tag: "Prometheus / Mimir",
    body: "Aggregations accumulate floats in different orders, so the same query can return different last digits. The harness compares floats with a tolerance instead of exact bits.",
  },
  {
    title: "Mimir disagrees with Prometheus",
    tag: "Mimir",
    body: "Two engines that are supposed to give identical PromQL answers don't always. Fuzzing against both shows which one is the reference behaviour.",
  },
  {
    title: "Tempo returns duplicate spans",
    tag: "Tempo",
    body: "Spans re-sent by a client come back twice until Tempo compacts the trace. Plural Traces deduplicates them at query time.",
  },
];
