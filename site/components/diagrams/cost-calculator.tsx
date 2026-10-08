"use client";

import { useMemo, useState } from "react";
import { motion } from "motion/react";
import { DEFAULT_INPUTS, estimate, usd, type CostInputs } from "@/lib/cost-model";
import { Figure, Segmented } from "./figure";

const COLORS = ["var(--accent-text)", "var(--faint)", "var(--ink)"];

const FIELDS: { key: keyof CostInputs; label: string; min: number; max: number; step: number; fmt: (v: number) => string }[] = [
  { key: "gbPerDay", label: "Ingest", min: 100, max: 20000, step: 100, fmt: (v) => (v >= 1000 ? `${(v / 1000).toFixed(1)} TB/day` : `${v} GB/day`) },
  { key: "retentionDays", label: "Retention", min: 3, max: 30, step: 1, fmt: (v) => `${v} days` },
  { key: "eventBytes", label: "Avg event", min: 200, max: 4000, step: 50, fmt: (v) => `${v} B` },
  { key: "compression", label: "Compression", min: 3, max: 15, step: 0.5, fmt: (v) => `${v}×` },
  { key: "peakFactor", label: "Peak / avg", min: 1, max: 3, step: 0.1, fmt: (v) => `${v.toFixed(1)}×` },
  { key: "vcpuHour", label: "vCPU-hour", min: 0.02, max: 0.08, step: 0.001, fmt: (v) => `$${v.toFixed(3)}` },
];

export function CostCalculator() {
  const [inputs, setInputs] = useState<CostInputs>(DEFAULT_INPUTS);
  const [scaleMode, setScaleMode] = useState<"log" | "linear">("log");
  const log = scaleMode === "log";
  const results = useMemo(() => estimate(inputs), [inputs]);
  const max = Math.max(...results.map((r) => r.total));
  const scale = (v: number) => (log ? Math.log10(Math.max(v, 1)) / Math.log10(max) : v / max);
  const ours = results[0].total;

  return (
    <Figure
      label="Fig. — Monthly cost of log ingestion"
      controls={
        <>
          <Segmented
            size="xs"
            value={scaleMode}
            onChange={setScaleMode}
            options={[
              { value: "log", label: "log" },
              { value: "linear", label: "linear" },
            ]}
          />
          <button type="button" onClick={() => setInputs(DEFAULT_INPUTS)} className="btn btn-ghost btn-sm text-muted">
            Reset to 5 TB/day
          </button>
        </>
      }
      caption={
        <span className="text-muted">
          Estimates for comparison only, using AWS us-east-1 on-demand prices and Datadog list prices with an annual commitment. They exclude engineering
          time, Datadog Flex Logs, and committed-use discounts. The full set of assumptions is listed below.
        </span>
      }
    >
      <div className="grid lg:grid-cols-[264px_1fr]">
        <div className="flex flex-col gap-3.5 border-b border-line p-4 lg:border-b-0 lg:border-r">
          {FIELDS.map((f) => (
            <label key={f.key} className="block">
              <div className="mb-0.5 flex items-baseline gap-2 text-[12.5px]">
                <span className="text-fg">{f.label}</span>
                <span className="leader" />
                <span className="font-mono text-[12px] text-ink">{f.fmt(inputs[f.key])}</span>
              </div>
              <input
                type="range"
                min={f.min}
                max={f.max}
                step={f.step}
                value={inputs[f.key]}
                onChange={(e) => setInputs({ ...inputs, [f.key]: Number(e.target.value) })}
                className="range w-full"
              />
            </label>
          ))}
        </div>

        <div className="min-w-0">
          <div className="flex flex-col gap-5 p-5">
            {results.map((r, i) => (
              <div key={r.name}>
                <div className="mb-1.5 flex items-baseline gap-3">
                  <span className="font-serif text-[16px] text-ink">{r.name}</span>
                  <span className="leader" />
                  <span className="font-serif text-[22px] font-light tabular-nums text-ink">
                    {usd(r.total)}
                    <span className="ml-1 text-[12px] text-muted">/mo</span>
                  </span>
                </div>
                <div className="hatch h-3.5 border border-line">
                  <motion.div
                    className="h-full"
                    style={{ background: COLORS[i] }}
                    animate={{ width: `${Math.max(1.5, scale(r.total) * 100)}%` }}
                    transition={{ type: "spring", stiffness: 140, damping: 22 }}
                  />
                </div>
                <div className="mt-1 flex justify-between text-[11.5px] text-muted">
                  <span className="italic">{r.footprint}</span>
                  {i > 0 && <span className="font-mono text-ink">{(r.total / ours).toFixed(1)}× Plural Telemetry</span>}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="min-w-0">
          <div className="grid border-t border-line md:grid-cols-3">
            {results.map((r, i) => (
              <div key={r.name} className={i > 0 ? "border-t border-line md:border-l md:border-t-0" : ""}>
                <div className="flex items-center gap-2 border-b border-line px-3 py-2 font-serif text-[13.5px] text-ink">
                  <span className="h-2 w-2" style={{ background: COLORS[i] }} />
                  {r.name}
                </div>
                <table className="w-full text-[11.5px]">
                  <tbody>
                    {r.lines.map((l) => (
                      <tr key={l.label} className="border-b border-line last:border-0">
                        <td className="px-3 py-1.5">
                          <div className="text-ink">{l.label}</div>
                          <div className="text-[10.5px] text-muted">{l.detail}</div>
                        </td>
                        <td className="px-3 py-1.5 text-right align-top font-mono tabular-nums text-ink">{usd(l.usd)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ))}
          </div>
      </div>
    </Figure>
  );
}
