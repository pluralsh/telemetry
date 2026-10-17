"use client";

import { useState } from "react";
import type { BenchRun, ColdFamily } from "@/lib/benchmark-types";
import { PRODUCTS, type ProductId } from "@/lib/nav";
import { Figure, Segmented } from "../diagrams/figure";
import { Legend, Stat } from "./benchmark-explorer";
import type { Series } from "./charts";
import { fmtDate, fmtMs, fmtRatio, ratioTone } from "./format";
import { RunPicker, StatusDot } from "./run-picker";

type ColdPercentile = "p50" | "p99";

const fmtCount = (v: number | undefined) => (v === undefined ? "–" : Math.round(v).toLocaleString());
const fmtKib = (v: number | undefined) => (v === undefined ? "–" : v >= 1 << 20 ? `${(v / (1 << 20)).toFixed(1)}\u00a0MiB` : `${Math.round(v / 1024)}\u00a0KiB`);

function ratio(impl: number | undefined, oracle: number | undefined) {
  return impl !== undefined && oracle ? impl / oracle : undefined;
}

function ColdTable({ families, percentile, series }: { families: ColdFamily[]; percentile: ColdPercentile; series: Series[] }) {
  const max = Math.max(...families.flatMap((f) => [f.impl.cold[percentile] ?? 0, f.oracle.cold[percentile] ?? 0]), 0) || 1;
  const width = (v?: number) => (v && v > 0 ? `${Math.max(0.5, (v / max) * 100)}%` : "0%");
  return (
    <div className="thin-scroll overflow-x-auto">
      <table className="w-full min-w-[760px] border-collapse text-[12px]">
        <thead>
          <tr className="border-b border-line text-left font-mono text-[10.5px] uppercase tracking-wide text-muted">
            <th className="py-2 pl-4 pr-3 font-normal">Query family</th>
            <th className="px-3 py-2 text-right font-normal">Runs</th>
            <th className="w-[38%] px-3 py-2 font-normal">Cold {percentile}</th>
            <th className="px-3 py-2 text-right font-normal">Ratio</th>
            <th className="px-3 py-2 text-right font-normal">GETs</th>
            <th className="px-3 py-2 text-right font-normal">Read</th>
            <th className="py-2 pl-3 pr-4 text-right font-normal">Warm p50</th>
          </tr>
        </thead>
        <tbody>
          {families.map((f) => {
            const r = ratio(f.impl.cold[percentile], f.oracle.cold[percentile]);
            return (
              <tr key={f.name} className="border-b border-line last:border-b-0">
                <td className="py-2.5 pl-4 pr-3 font-mono text-ink">{f.name}</td>
                <td className="px-3 py-2.5 text-right font-mono tabular-nums text-fg">{f.cases}</td>
                <td className="px-3 py-2.5">
                  <div className="flex flex-col gap-1">
                    {series.map((s) => (
                      <div key={s.key} className="flex items-center gap-2">
                        <div className="h-[7px] flex-1">
                          <div className="h-full" style={{ width: width(f[s.key].cold[percentile]), background: s.color }} />
                        </div>
                        <span className="w-16 text-right font-mono text-[11px] tabular-nums text-muted">{fmtMs(f[s.key].cold[percentile])}</span>
                      </div>
                    ))}
                  </div>
                </td>
                <td className={`px-3 py-2.5 text-right font-mono tabular-nums ${ratioTone(r)}`}>{fmtRatio(r)}</td>
                {(
                  [
                    (side: ColdFamily["impl"]) => fmtCount(side.getP50),
                    (side: ColdFamily["impl"]) => fmtKib(side.getBytesP50),
                    (side: ColdFamily["impl"]) => fmtMs(side.warmP50),
                  ] as const
                ).map((cell, i) => (
                  <td key={i} className={`py-2.5 text-right font-mono text-[11px] tabular-nums ${i === 2 ? "pl-3 pr-4" : "px-3"}`}>
                    <div className="text-ink">{cell(f.impl)}</div>
                    <div className="text-muted">{cell(f.oracle)}</div>
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function ColdReads({ product, runs }: { product: ProductId; runs: BenchRun[] }) {
  const [selectedName, setSelectedName] = useState(runs[0]?.name);
  const [percentile, setPercentile] = useState<ColdPercentile>("p50");

  if (!runs.length) return <p className="text-[14px] text-muted">No cold-read runs have been recorded for this database yet.</p>;

  const run = runs.find((r) => r.name === selectedName) ?? runs[0];
  const cold = run.cold!;
  const series: Series[] = [
    { key: "impl", label: `Plural ${PRODUCTS[product].name}`, color: PRODUCTS[product].color },
    { key: "oracle", label: run.oracle, color: "var(--muted)" },
  ];
  const oracleName = run.oracle.split(" ")[0];
  const { impl, oracle } = cold.totals;
  const dataset = Object.entries(cold.dataset)
    .filter(([name]) => !name.includes("."))
    .map(([name, value]) => `${value.toLocaleString()} ${name}`)
    .join(" · ");

  return (
    <div className="not-prose">
      <div className="card mb-6 flex flex-wrap items-center gap-x-4 gap-y-2 px-3 py-2.5">
        <span className="text-[12px] italic text-muted">Run</span>
        <RunPicker runs={runs} value={run} onChange={setSelectedName} />
        <span className="kbd">vs {run.oracle}</span>
        <span className="kbd">+{cold.firstByteMs} ms first byte</span>
        <a href={run.markdownUrl} target="_blank" rel="noreferrer" className="ml-auto text-[12.5px] text-accent-text hover:underline">
          Full report ↗
        </a>
      </div>

      <div className="card overflow-hidden">
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-line px-4 py-2.5 text-[12.5px]">
          <StatusDot status={run.status} />
          <span className="text-ink">
            {cold.measured} of {cold.planned} cold measurements
          </span>
          <span className="text-muted">· {(cold.spanS / 3600).toFixed(0)} h of data · {dataset}</span>
          <span className="ml-auto font-mono text-[11px] text-muted">
            {fmtDate(run.startedAt)} · {run.commit}
            {run.dirty ? " (dirty)" : ""} · {run.host}
          </span>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5">
          <Stat label="p50 cold query" value={fmtMs(impl.cold.p50)} sub={`${fmtMs(oracle.cold.p50)} ${oracleName}`} />
          <Stat label="p99 cold query" value={fmtMs(impl.cold.p99)} sub={`${fmtMs(oracle.cold.p99)} ${oracleName}`} />
          <Stat
            label="p50 cold ratio"
            value={fmtRatio(ratio(impl.cold.p50, oracle.cold.p50))}
            tone={ratioTone(ratio(impl.cold.p50, oracle.cold.p50))}
            sub="impl / oracle"
          />
          <Stat label="object-store GETs per query" value={fmtCount(impl.getP50)} sub={`${fmtCount(oracle.getP50)} ${oracleName}, p50`} />
          <Stat
            label="restart to ready"
            value={`${cold.startup.impl.readyP50?.toFixed(1) ?? "–"} s`}
            sub={`${cold.startup.oracle.readyP50?.toFixed(1) ?? "–"} s ${oracleName}, p50`}
          />
        </div>
      </div>

      <Figure
        label="Cold latency by query family"
        controls={<Segmented value={percentile} onChange={setPercentile} options={(["p50", "p99"] as const).map((p) => ({ value: p, label: p }))} />}
        caption={
          <>
            Each row is one battery query run {cold.repeats === 1 ? "once" : `${cold.repeats} times`} per side. GETs, bytes read and warm latency show Plural on top and {oracleName}{" "}
            below; GETs and bytes count only the object-store reads made while that query ran, not startup. Warm is the same query re-sent at
            once to the now-warm process.
          </>
        }
      >
        <div className="px-4 pt-4">
          <Legend series={series} />
        </div>
        <ColdTable families={cold.families} percentile={percentile} series={series} />
      </Figure>
    </div>
  );
}
