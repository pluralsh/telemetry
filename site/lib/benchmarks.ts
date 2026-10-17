import "server-only";
import fs from "node:fs";
import path from "node:path";
import type { ProductId } from "./nav";
import { PERCENTILES, type BenchCold, type BenchRun, type BenchUsage, type ColdSide, type ColdStartup, type Latency } from "./benchmark-types";

export type { BenchCold, BenchFamily, BenchRun, BenchUsage, ColdFamily, Latency, Percentile } from "./benchmark-types";

/** `FUZZ_HISTORY_DIR` previews another history root, as the harness writes it. */
const FUZZ_DIR = process.env.FUZZ_HISTORY_DIR ?? path.join(process.cwd(), "..", "documentation", "benchmarks", "fuzz");

const ORACLE_NAMES: Record<string, string> = {
  loki: "Loki",
  prometheus: "Prometheus",
  mimir: "Mimir",
  "mimir-blocks": "Mimir store-gateway",
  tempo: "Tempo",
  "tempo-s3": "Tempo on S3",
};

const DEFAULT_ORACLE: Record<ProductId, string> = { logs: "loki", metrics: "prometheus", traces: "tempo" };
const REPO_TREE = "https://github.com/pluralsh/telemetry/blob/main/documentation/benchmarks/fuzz";

type Raw = Record<string, any>;

function latency(raw: Raw | undefined): Latency {
  const out: Latency = {};
  for (const p of PERCENTILES) if (typeof raw?.[p] === "number") out[p] = raw[p];
  return out;
}

function usage(raw: Raw | undefined): BenchUsage {
  return {
    cpuMean: raw?.cpu_cores?.mean ?? 0,
    cpuP95: raw?.cpu_cores?.p95 ?? 0,
    memMean: raw?.memory_mib?.mean ?? 0,
    memP95: raw?.memory_mib?.p95 ?? 0,
  };
}

function coldSide(raw: Raw | undefined): ColdSide {
  return { cold: latency(raw?.cold_ms), warmP50: raw?.warm_ms?.p50 ?? undefined, getP50: raw?.get?.p50 ?? undefined, getBytesP50: raw?.get_bytes?.p50 ?? undefined };
}

function coldStartup(raw: Raw | undefined): ColdStartup {
  return { restarts: raw?.restarts ?? 0, readyP50: raw?.ready_s?.p50 ?? undefined, getP50: raw?.get?.p50 ?? undefined };
}

function cold(raw: Raw | undefined): BenchCold | undefined {
  if (!raw) return undefined;
  return {
    spanS: raw.span_s ?? 0,
    repeats: raw.repeats ?? 0,
    firstByteMs: raw.first_byte_ms ?? 0,
    planned: raw.planned ?? 0,
    measured: raw.measured ?? 0,
    dataset: raw.dataset ?? {},
    families: Object.entries<Raw>(raw.families ?? {}).map(([name, f]) => ({ name, cases: f.cases ?? 0, impl: coldSide(f.impl), oracle: coldSide(f.oracle) })),
    totals: { impl: coldSide(raw.totals?.impl), oracle: coldSide(raw.totals?.oracle) },
    startup: { impl: coldStartup(raw.startup?.impl), oracle: coldStartup(raw.startup?.oracle) },
  };
}

function oracleLabel(product: ProductId, d: Raw) {
  const key = d.settings?.oracle ?? DEFAULT_ORACLE[product];
  const base = key.split("-")[0];
  const image = Object.values<Raw>(d.images ?? {}).find((i) => String(i.image).includes(`/${base}:`));
  const version = image ? String(image.image).match(/:v?(\d[\d.]*)/)?.[1] : undefined;
  return `${ORACLE_NAMES[key] ?? key}${version ? ` ${version}` : ""}`;
}

function toRun(product: ProductId, d: Raw): BenchRun {
  const r = d.results;
  const scenario = d.config?.scenario ?? "recent";
  const oracle = oracleLabel(product, d);
  return {
    name: d.name,
    startedAt: d.started_at,
    durationS: r.elapsed_s ?? d.config?.duration_s ?? 0,
    commit: d.git?.short ?? "",
    commitUrl: `https://github.com/pluralsh/telemetry/commit/${d.git?.commit}`,
    dirty: !!d.git?.dirty,
    branch: d.git?.branch ?? "",
    scenario,
    oracle,
    track: `${scenario} · ${oracle}`,
    storage: d.settings?.storage ?? "s3",
    status: r.status === "passed" ? "passed" : "failed",
    reasons: r.reasons ?? [],
    cases: r.cases ?? 0,
    nonMatch: r.non_match ?? 0,
    outcomes: r.outcomes ?? {},
    query: { impl: latency(r.latency?.impl_ms), oracle: latency(r.latency?.oracle_ms), ratio: latency(r.latency?.ratio) },
    ingest: { batches: r.ingest?.batches ?? 0, impl: latency(r.ingest?.impl_ms), oracle: latency(r.ingest?.oracle_ms) },
    families: Object.entries<Raw>(r.families ?? {})
      .map(([name, f]) => ({
        name,
        cases: f.cases ?? 0,
        nonMatch: f.non_match ?? 0,
        impl: latency(f.impl_ms),
        oracle: latency(f.oracle_ms),
        ratioP50: f.ratio?.p50,
      }))
      .sort((a, b) => b.cases - a.cases),
    usage: { impl: usage(r.resources?.roles?.impl), oracle: usage(r.resources?.roles?.oracle) },
    host: [d.host?.docker?.os ?? d.host?.os, d.host?.arch, d.host?.docker?.cpus && `${d.host.docker.cpus} CPUs`, d.host?.docker?.memory_gib && `${d.host.docker.memory_gib} GiB`]
      .filter(Boolean)
      .join(" · "),
    markdownUrl: `${REPO_TREE}/history/${product}/${d.name}.md`,
    cold: cold(r.cold),
  };
}

/** Every recorded fuzz benchmark run for a product, newest first. */
export function loadBenchRuns(product: ProductId): BenchRun[] {
  const index = JSON.parse(fs.readFileSync(path.join(FUZZ_DIR, "index.json"), "utf8"));
  return (index.entries as Raw[])
    .filter((e) => e.product === product)
    .map((e) => toRun(product, JSON.parse(fs.readFileSync(path.join(FUZZ_DIR, e.json), "utf8"))))
    .sort((a, b) => b.startedAt.localeCompare(a.startedAt));
}
