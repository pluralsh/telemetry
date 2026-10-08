"use client";

import { Bar, BarChart, CartesianGrid, LabelList, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { BenchFamily, BenchRun, Latency, Percentile } from "@/lib/benchmark-types";
import { fmtMs, fmtRatio, fmtShortDate, ratioTone } from "./format";

const TICK = { fill: "var(--muted)", fontSize: 11, fontFamily: "var(--font-mono)" };
const AXIS = { stroke: "var(--line-strong)" };
const fmtTick = (v: number) => (v === 0 ? "0" : fmtMs(v));

export type Series = { key: "impl" | "oracle"; label: string; color: string };

function ChartTooltip({ active, payload, label, series }: { active?: boolean; payload?: any[]; label?: string; series: Series[] }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-sm border border-line-strong bg-panel px-3 py-2 font-mono text-[11.5px] shadow-[0_8px_24px_-12px_rgba(0,0,0,0.25)]">
      <div className="mb-1 text-muted">{label}</div>
      {payload.map((p) => {
        const s = series.find((x) => p.dataKey === x.key || String(p.dataKey).startsWith(x.key));
        return (
          <div key={String(p.dataKey)} className="flex items-center gap-2">
            <span className="h-2 w-2" style={{ background: p.color ?? s?.color }} />
            <span className="text-fg">{p.name}</span>
            <span className="leader" />
            <span className="text-ink">{fmtMs(p.value)}</span>
          </div>
        );
      })}
    </div>
  );
}

export function PercentileChart({ impl, oracle, series, percentiles }: { impl: Latency; oracle: Latency; series: Series[]; percentiles: readonly Percentile[] }) {
  const rows = percentiles
    .filter((p) => impl[p] !== undefined || oracle[p] !== undefined)
    .map((p) => ({ p, impl: impl[p], oracle: oracle[p], implLabel: fmtMs(impl[p]), oracleLabel: fmtMs(oracle[p]) }));
  return (
    <ResponsiveContainer width="100%" height={280}>
      <BarChart data={rows} barGap={3} barCategoryGap="26%" margin={{ top: 22, right: 8, bottom: 0, left: 0 }}>
        <CartesianGrid vertical={false} stroke="var(--line)" />
        <XAxis dataKey="p" tick={TICK} axisLine={AXIS} tickLine={false} />
        <YAxis domain={[0, "auto"]} tick={TICK} axisLine={false} tickLine={false} tickFormatter={fmtTick} width={64} />
        <Tooltip content={<ChartTooltip series={series} />} cursor={{ fill: "color-mix(in srgb, var(--fg) 5%, transparent)" }} />
        {series.map((s) => (
          <Bar key={s.key} dataKey={s.key} name={s.label} fill={s.color} isAnimationActive animationDuration={500}>
            <LabelList dataKey={`${s.key}Label`} position="top" style={{ fill: "var(--muted)", fontSize: 10, fontFamily: "var(--font-mono)" }} />
          </Bar>
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}

export function HistoryChart({
  runs,
  selected,
  onSelect,
  series,
  percentile,
}: {
  runs: BenchRun[];
  selected: string;
  onSelect: (name: string) => void;
  series: Series[];
  percentile: Percentile;
}) {
  const rows = [...runs].reverse().map((r) => ({ name: r.name, at: fmtShortDate(r.startedAt), impl: r.query.impl[percentile], oracle: r.query.oracle[percentile] }));
  const dot = (color: string) =>
    function Dot(props: { cx?: number; cy?: number; index?: number }) {
      const { cx = 0, cy = 0, index = 0 } = props;
      const on = rows[index]?.name === selected;
      const s = on ? 10 : 7;
      return (
        <rect
          key={index}
          x={cx - s / 2}
          y={cy - s / 2}
          width={s}
          height={s}
          fill={on ? color : "var(--card)"}
          stroke={color}
          strokeWidth={1.5}
          className="cursor-pointer"
          onClick={() => onSelect(rows[index].name)}
        />
      );
    };
  return (
    <ResponsiveContainer width="100%" height={220}>
      <LineChart data={rows} margin={{ top: 12, right: 24, bottom: 0, left: 0 }}>
        <CartesianGrid vertical={false} stroke="var(--line)" />
        <XAxis dataKey="at" tick={TICK} axisLine={AXIS} tickLine={false} padding={{ left: 24, right: 24 }} />
        <YAxis domain={[0, "auto"]} tick={TICK} axisLine={false} tickLine={false} tickFormatter={fmtTick} width={64} />
        <Tooltip content={<ChartTooltip series={series} />} cursor={{ stroke: "var(--line-strong)" }} />
        {series.map((s) => (
          <Line key={s.key} dataKey={s.key} name={s.label} stroke={s.color} strokeWidth={1.5} dot={dot(s.color)} activeDot={false} isAnimationActive={false} />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}

export function FamilyTable({ families, percentile, series }: { families: BenchFamily[]; percentile: Percentile; series: Series[] }) {
  const max = Math.max(...families.flatMap((f) => [f.impl[percentile] ?? 0, f.oracle[percentile] ?? 0]), 0) || 1;
  const width = (v?: number) => (v && v > 0 ? `${Math.max(0.5, (v / max) * 100)}%` : "0%");
  return (
    <div className="thin-scroll overflow-x-auto">
      <table className="w-full min-w-[640px] border-collapse text-[12px]">
        <thead>
          <tr className="border-b border-line text-left font-mono text-[10.5px] uppercase tracking-wide text-muted">
            <th className="py-2 pl-4 pr-3 font-normal">Query family</th>
            <th className="px-3 py-2 text-right font-normal">Cases</th>
            <th className="w-[44%] px-3 py-2 font-normal">{percentile} latency</th>
            <th className="px-3 py-2 text-right font-normal">p50 ratio</th>
            <th className="py-2 pl-3 pr-4 text-right font-normal">Non-match</th>
          </tr>
        </thead>
        <tbody>
          {families.map((f) => (
            <tr key={f.name} className="border-b border-line last:border-b-0">
              <td className="py-2.5 pl-4 pr-3 font-mono text-ink">{f.name}</td>
              <td className="px-3 py-2.5 text-right font-mono tabular-nums text-fg">{f.cases.toLocaleString()}</td>
              <td className="px-3 py-2.5">
                <div className="flex flex-col gap-1">
                  {series.map((s) => (
                    <div key={s.key} className="flex items-center gap-2">
                      <div className="h-[7px] flex-1">
                        <div className="h-full" style={{ width: width(f[s.key][percentile]), background: s.color }} />
                      </div>
                      <span className="w-16 text-right font-mono text-[11px] tabular-nums text-muted">{fmtMs(f[s.key][percentile])}</span>
                    </div>
                  ))}
                </div>
              </td>
              <td className={`px-3 py-2.5 text-right font-mono tabular-nums ${ratioTone(f.ratioP50)}`}>{fmtRatio(f.ratioP50)}</td>
              <td className="py-2.5 pl-3 pr-4 text-right font-mono tabular-nums text-muted">{f.nonMatch}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const OUTCOME_STYLE: Record<string, { label: string; color: string; hatch?: boolean }> = {
  match: { label: "match", color: "var(--c)" },
  inconclusive: { label: "inconclusive", color: "var(--faint)", hatch: true },
  both_error: { label: "both error", color: "var(--line-strong)" },
  oracle_error: { label: "oracle error", color: "var(--muted)" },
  oracle_timeout: { label: "oracle timeout", color: "var(--muted)" },
  unstable_oracle: { label: "oracle unstable", color: "var(--faint)" },
  impl_error: { label: "impl error", color: "#b4590f" },
  impl_timeout: { label: "impl timeout", color: "#b4590f" },
  unstable_impl: { label: "impl unstable", color: "#b4590f" },
  mismatch: { label: "mismatch", color: "#b42f2f" },
};

export function OutcomeBar({ outcomes, color }: { outcomes: Record<string, number>; color: string }) {
  const entries = Object.entries(outcomes).sort((a, b) => b[1] - a[1]);
  const total = entries.reduce((a, [, n]) => a + n, 0) || 1;
  return (
    <div style={{ ["--c" as string]: color }}>
      <div className="flex h-3 w-full overflow-hidden border border-line">
        {entries.map(([k, n]) => {
          const s = OUTCOME_STYLE[k] ?? { label: k, color: "var(--muted)" };
          return <div key={k} title={`${s.label}: ${n}`} className={s.hatch ? "hatch" : ""} style={{ width: `${(n / total) * 100}%`, background: s.hatch ? undefined : s.color, minWidth: n ? 2 : 0 }} />;
        })}
      </div>
      <ul className="mt-3 grid grid-cols-2 gap-x-6 gap-y-1 font-mono text-[11.5px] sm:grid-cols-3">
        {entries.map(([k, n]) => {
          const s = OUTCOME_STYLE[k] ?? { label: k, color: "var(--muted)" };
          return (
            <li key={k} className="flex items-center gap-2">
              <span className={`h-2 w-2 shrink-0 ${s.hatch ? "hatch border border-line-strong" : ""}`} style={{ background: s.hatch ? undefined : s.color }} />
              <span className="text-fg">{s.label}</span>
              <span className="leader" />
              <span className="tabular-nums text-ink">{n.toLocaleString()}</span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

export function UsageBars({ rows, series }: { rows: { label: string; unit: string; impl: number; oracle: number }[]; series: Series[] }) {
  return (
    <div className="flex flex-col gap-4">
      {rows.map((r) => {
        const max = Math.max(r.impl, r.oracle) || 1;
        return (
          <div key={r.label}>
            <div className="mb-1.5 text-[12px] italic text-muted">{r.label}</div>
            {series.map((s) => (
              <div key={s.key} className="flex items-center gap-2 py-[2px]">
                <span className="w-20 truncate font-mono text-[11px] text-fg">{s.key === "impl" ? "Plural" : s.label.split(" ")[0]}</span>
                <div className="h-[7px] flex-1">
                  <div className="h-full" style={{ width: `${(r[s.key] / max) * 100}%`, background: s.color }} />
                </div>
                <span className="w-20 text-right font-mono text-[11px] tabular-nums text-ink">
                  {r[s.key] >= 100 ? Math.round(r[s.key]).toLocaleString() : r[s.key].toFixed(2)} {r.unit}
                </span>
              </div>
            ))}
          </div>
        );
      })}
    </div>
  );
}
