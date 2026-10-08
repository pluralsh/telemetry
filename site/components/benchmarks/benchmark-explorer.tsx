"use client";

import { useState } from "react";
import type { BenchRun, Percentile } from "@/lib/benchmark-types";
import { PERCENTILES } from "@/lib/benchmark-types";

/** Max latency is a single outlier and dwarfs every other bar on a linear axis. */
const CHARTED = PERCENTILES.filter((p) => p !== "max");
import { PRODUCTS, type ProductId } from "@/lib/nav";
import { Figure, Segmented } from "../diagrams/figure";
import { FamilyTable, HistoryChart, OutcomeBar, PercentileChart, UsageBars, type Series } from "./charts";
import { fmtDate, fmtMs, fmtRatio, ratioTone } from "./format";
import { RunPicker, StatusDot } from "./run-picker";

function Stat({ label, value, sub, tone = "text-ink" }: { label: string; value: React.ReactNode; sub?: React.ReactNode; tone?: string }) {
  return (
    <div className="border-line px-4 py-3.5 [&:not(:first-child)]:border-l">
      <div className={`whitespace-nowrap font-serif text-[20px] font-light tabular-nums ${tone}`}>{value}</div>
      <div className="mt-0.5 text-[11.5px] leading-tight text-muted">{label}</div>
      {sub && <div className="mt-0.5 font-mono text-[10.5px] text-faint">{sub}</div>}
    </div>
  );
}

function Legend({ series }: { series: Series[] }) {
  return (
    <div className="flex flex-wrap gap-4 font-mono text-[11.5px] text-fg">
      {series.map((s) => (
        <span key={s.key} className="flex items-center gap-2">
          <span className="h-2 w-2" style={{ background: s.color }} />
          {s.label}
        </span>
      ))}
    </div>
  );
}

export function BenchmarkExplorer({ product, runs }: { product: ProductId; runs: BenchRun[] }) {
  const [selectedName, setSelectedName] = useState(runs[0]?.name);
  const [path, setPath] = useState<"query" | "ingest">("query");
  const [familyP, setFamilyP] = useState<Percentile>("p99");
  const [historyP, setHistoryP] = useState<Percentile>("p50");

  if (!runs.length) return <p className="text-[14px] text-muted">No benchmark runs have been recorded for this database yet.</p>;

  const run = runs.find((r) => r.name === selectedName) ?? runs[0];
  const track = runs.filter((r) => r.track === run.track);
  const series: Series[] = [
    { key: "impl", label: `Plural ${PRODUCTS[product].name}`, color: PRODUCTS[product].color },
    { key: "oracle", label: run.oracle, color: "var(--muted)" },
  ];
  const latency = path === "query" ? run.query : run.ingest;
  const mismatches = run.outcomes.mismatch ?? 0;

  return (
    <div className="not-prose">
      <div className="card mb-6 flex flex-wrap items-center gap-x-4 gap-y-2 px-3 py-2.5">
        <span className="text-[12px] italic text-muted">Run</span>
        <RunPicker runs={runs} value={run} onChange={setSelectedName} />
        <span className="kbd">{run.scenario}</span>
        <span className="kbd">vs {run.oracle}</span>
        <span className="kbd">{run.storage}</span>
        <a href={run.markdownUrl} target="_blank" rel="noreferrer" className="ml-auto text-[12.5px] text-accent-text hover:underline">
          Full report ↗
        </a>
      </div>

      <div className="card overflow-hidden">
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-line px-4 py-2.5 text-[12.5px]">
          <StatusDot status={run.status} />
          <span className={run.status === "passed" ? "text-ink" : "text-[#b42f2f] dark:text-[#f08a8a]"}>{run.status === "passed" ? "Passed" : "Failed"}</span>
          {run.reasons.length > 0 && <span className="text-muted">· {run.reasons.join(", ")}</span>}
          <span className="ml-auto font-mono text-[11px] text-muted">
            {fmtDate(run.startedAt)} ·{" "}
            <a href={run.commitUrl} target="_blank" rel="noreferrer" className="hover:text-ink">
              {run.commit}
              {run.dirty ? " (dirty)" : ""}
            </a>{" "}
            · {run.branch} · {Math.round(run.durationS / 60)} min · {run.host}
          </span>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6">
          <Stat label="queries compared" value={run.cases.toLocaleString()} />
          <Stat label="mismatches" value={mismatches} tone={mismatches ? "text-[#b42f2f] dark:text-[#f08a8a]" : "text-ink"} sub={`${run.nonMatch} non-match total`} />
          <Stat label="p50 query" value={fmtMs(run.query.impl.p50)} sub={`${fmtMs(run.query.oracle.p50)} ${run.oracle.split(" ")[0]}`} />
          <Stat label="p99 query" value={fmtMs(run.query.impl.p99)} sub={`${fmtMs(run.query.oracle.p99)} ${run.oracle.split(" ")[0]}`} />
          <Stat label="p50 latency ratio" value={fmtRatio(run.query.ratio.p50)} tone={ratioTone(run.query.ratio.p50)} sub="per query, impl / oracle" />
          <Stat label="mean CPU cores" value={run.usage.impl.cpuMean.toFixed(2)} sub={`${run.usage.oracle.cpuMean.toFixed(2)} ${run.oracle.split(" ")[0]}`} />
        </div>
      </div>

      <Figure
        label="Latency by percentile"
        controls={
          <Segmented
            value={path}
            onChange={setPath}
            options={[
              { value: "query", label: "Queries" },
              { value: "ingest", label: `Ingest · ${run.ingest.batches} batches` },
            ]}
          />
        }
        caption={
          path === "query" ? (
            <>
              Latency of every query both sides answered. Each query runs against both systems with the same data, so the percentiles
              compare like with like.
            </>
          ) : (
            <>Time to accept one ingest batch on each side. Durability settings differ between systems, so read this as indicative.</>
          )
        }
      >
        <div className="px-4 pb-2 pt-4">
          <Legend series={series} />
          <PercentileChart impl={latency.impl} oracle={latency.oracle} series={series} percentiles={CHARTED} />
        </div>
      </Figure>

      <Figure
        label="By query family"
        controls={<Segmented value={familyP} onChange={setFamilyP} options={(["p50", "p90", "p99"] as const).map((p) => ({ value: p, label: p }))} />}
        caption="Bars share one linear scale across families. The ratio is the median of per-query implementation / oracle latency, so values below 1× mean Plural answered faster."
      >
        <FamilyTable families={run.families} percentile={familyP} series={series} />
      </Figure>

      <div className="grid gap-x-6 lg:grid-cols-2">
        <Figure label="Correctness" caption="Inconclusive results are differences the API contract allows, such as both sides truncating at a limit." className="lg:!mx-0">
          <div className="p-4">
            <OutcomeBar outcomes={run.outcomes} color={PRODUCTS[product].color} />
          </div>
        </Figure>
        <Figure label="Resources" caption="Sampled from the Docker Engine API every two seconds over the run. Object storage is excluded from both sides." className="lg:!mx-0">
          <div className="p-4">
            <UsageBars
              series={series}
              rows={[
                { label: "CPU, mean cores", unit: "", impl: run.usage.impl.cpuMean, oracle: run.usage.oracle.cpuMean },
                { label: "Memory, p95 working set", unit: "MiB", impl: run.usage.impl.memP95, oracle: run.usage.oracle.memP95 },
              ]}
            />
          </div>
        </Figure>
      </div>

      <Figure
        label={`History · ${run.track}`}
        controls={<Segmented value={historyP} onChange={setHistoryP} options={(["p50", "p90", "p99"] as const).map((p) => ({ value: p, label: p }))} />}
        caption={
          track.length > 1
            ? "Query latency for every run on this scenario and oracle. Click a point to open that run."
            : "Only one run has been recorded on this scenario and oracle so far. Later runs will chart here."
        }
      >
        <div className="px-4 pb-2 pt-4">
          <Legend series={series} />
          <HistoryChart runs={track} selected={run.name} onSelect={setSelectedName} series={series} percentile={historyP} />
        </div>
      </Figure>
    </div>
  );
}
