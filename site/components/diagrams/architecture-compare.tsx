"use client";

import { useState } from "react";
import { AnimatePresence, motion } from "motion/react";
import type { ProductId } from "@/lib/nav";
import { ARCH, type ArchNode, type ArchSide, type Mode } from "./arch-configs";
import { Figure, Segmented } from "./figure";

const TONE: Record<NonNullable<ArchNode["tone"]>, { fill: string; stroke: string; dash?: string }> = {
  ours: { fill: "color-mix(in srgb, var(--c) 7%, var(--panel))", stroke: "var(--c)" },
  stateful: { fill: "var(--panel)", stroke: "var(--ink)" },
  stateless: { fill: "var(--panel)", stroke: "var(--line-strong)" },
  cache: { fill: "var(--panel)", stroke: "var(--line-strong)", dash: "3 3" },
  store: { fill: "url(#arch-hatch)", stroke: "var(--line-strong)" },
  client: { fill: "var(--bg)", stroke: "var(--line)" },
  ring: { fill: "transparent", stroke: "var(--line-strong)", dash: "2 4" },
};

const POD = 9;
const GAP = 3;

function Pods({ node, count, color }: { node: ArchNode; count: number; color: string }) {
  const perRow = Math.max(1, Math.floor((node.w - 16 + GAP) / (POD + GAP)));
  const top = node.y + (node.sub ? 34 : 24);
  const rows = Math.max(1, Math.floor((node.y + node.h - 6 - top + GAP) / (POD + GAP)));
  const cap = perRow * rows;
  const shown = Math.min(count, count > cap ? cap - 1 : cap);
  return (
    <g>
      <AnimatePresence>
        {Array.from({ length: shown }, (_, i) => (
          <motion.rect
            key={i}
            x={node.x + 8 + (i % perRow) * (POD + GAP)}
            y={top + Math.floor(i / perRow) * (POD + GAP)}
            width={POD}
            height={POD}
            fill={color}
            fillOpacity={node.tone === "ours" ? 0.9 : 0.5}
            initial={{ scale: 0, opacity: 0 }}
            animate={{ scale: 1, opacity: 1 }}
            exit={{ scale: 0, opacity: 0 }}
            transition={{ delay: (i % 12) * 0.015, type: "spring", stiffness: 420, damping: 24 }}
            style={{ transformBox: "fill-box", transformOrigin: "center" }}
          />
        ))}
      </AnimatePresence>
      {count > shown && (
        <text x={node.x + 8 + (shown % perRow) * (POD + GAP) + 1} y={top + Math.floor(shown / perRow) * (POD + GAP) + 8} fontSize={8.5} className="fill-muted font-mono">
          +{count - shown}
        </text>
      )}
      <text x={node.x + node.w - 8} y={node.y + 15} textAnchor="end" fontSize={10} className="fill-muted font-mono">
        ×{count}
      </text>
    </g>
  );
}

function Panel({ side, mode, level, q, color, accent }: { side: ArchSide; mode: Mode | "both"; level: number; q: number; color: string; accent: string }) {
  const active = (n: ArchNode) => mode === "both" || !n.modes || n.modes.includes(mode);
  return (
    <div className="flex min-w-0 flex-1 flex-col">
      <div className="flex items-baseline gap-2 px-4 pb-2.5 pt-4">
        <div className="font-serif text-[17px] text-ink">{side.title}</div>
        <span className="leader" />
        <div className="eyebrow">{side.subtitle}</div>
      </div>
      <div className="border-y border-line bg-bg">
        <svg viewBox="0 0 460 418" className="h-auto w-full" style={{ ["--c" as string]: color }}>
          <defs>
            <pattern id="arch-hatch" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
              <path d="M0 0v6" stroke="var(--fg)" strokeOpacity="0.1" />
            </pattern>
          </defs>
          {side.nodes.map((n) => {
            const t = TONE[n.tone ?? "stateless"];
            const on = active(n);
            return (
              <g key={n.id} style={{ opacity: on ? 1 : 0.3, transition: "opacity .35s" }}>
                <rect x={n.x} y={n.y} width={n.w} height={n.h} rx={2} fill={t.fill} stroke={t.stroke} strokeOpacity={n.tone === "stateful" ? 0.6 : 1} strokeDasharray={t.dash} />
                <text x={n.x + 8} y={n.y + 16} fontSize={11.5} className="fill-ink font-mono">
                  {n.label}
                </text>
                {n.sub && (
                  <text x={n.x + 8} y={n.y + 29} fontSize={9.5} className="fill-muted" fontStyle="italic">
                    {n.sub}
                  </text>
                )}
                {n.pods && <Pods node={n} count={n.pods(level, q)} color={n.tone === "ours" ? color : n.tone === "cache" ? "var(--faint)" : "var(--fg)"} />}
              </g>
            );
          })}
          {side.flows
            .filter((f) => mode === "both" || f.mode === mode)
            .map((f, i) => {
              const c = f.color ?? (f.mode === "write" ? color : accent);
              const dur = f.dur ?? 2.4;
              return (
                <g key={`${mode}-${i}`}>
                  <path d={f.d} fill="none" stroke={c} strokeOpacity={0.45} strokeWidth={1.25} className="flow-dash" />
                  {Array.from({ length: f.count ?? 1 }, (_, k) => (
                    <rect key={k} x={-3} y={-3} width={6} height={6} fill={c}>
                      <animateMotion dur={`${dur}s`} begin={`${(k * dur) / (f.count ?? 1) + i * 0.13}s`} repeatCount="indefinite" path={f.d} />
                    </rect>
                  ))}
                </g>
              );
            })}
        </svg>
      </div>
      <dl className="grid grid-cols-4 border-b border-line">
        {side.stats(level, q).map((s, i) => (
          <div key={s.label} className={`px-3 py-3 ${i > 0 ? "border-l border-line" : ""}`}>
            <dd className="whitespace-nowrap font-serif text-[20px] font-light leading-none tabular-nums text-ink">{s.value}</dd>
            <dt className="mt-1.5 text-[11px] leading-tight text-muted">{s.label}</dt>
          </div>
        ))}
      </dl>
      <p className="px-4 py-3 text-[12.5px] leading-relaxed text-fg">{side.scaleNote}</p>
    </div>
  );
}

export function ArchitectureCompare({ product = "logs", caption }: { product?: ProductId; caption?: string }) {
  const cfg = ARCH[product];
  const [mode, setMode] = useState<Mode | "both">("write");
  const [level, setLevel] = useState(cfg.defaultLevel);
  const [q, setQ] = useState<"1" | "2" | "3">("2");
  const color = `var(--${product})`;

  return (
    <Figure
      label={caption ?? "Fig. — Architecture, side by side"}
      caption={
        <span className="text-muted">
          Pod counts are illustrative sizing for comparison, derived from vendor capacity guides and our provisional writer envelope. They are not
          benchmark results.
        </span>
      }
      controls={
        <Segmented
          value={mode}
          onChange={setMode}
          options={[
            { value: "write", label: "Write path" },
            { value: "read", label: "Read path" },
            { value: "both", label: "Both" },
          ]}
        />
      }
    >
      <div className="flex flex-wrap items-center gap-x-6 gap-y-3 border-b border-line px-4 py-2.5 text-[12.5px] text-fg">
        <label className="flex items-center gap-3">
          <span>{cfg.levelLabel}</span>
          <input
            type="range"
            min={0}
            max={cfg.levels.length - 1}
            value={level}
            onChange={(e) => setLevel(Number(e.target.value))}
            className="range w-36"
          />
          <span className="kbd min-w-[88px] text-ink">{cfg.levels[level]}</span>
        </label>
        <div className="flex items-center gap-2.5">
          <span>Query load</span>
          <Segmented
            size="xs"
            value={q}
            onChange={setQ}
            options={[
              { value: "1", label: "low" },
              { value: "2", label: "med" },
              { value: "3", label: "high" },
            ]}
          />
        </div>
      </div>
      <div className="flex flex-col md:flex-row">
        <Panel side={cfg.ours} mode={mode} level={level} q={Number(q)} color={color} accent="var(--accent-text)" />
        <div className="h-px bg-line md:h-auto md:w-px" />
        <Panel side={cfg.peer} mode={mode} level={level} q={Number(q)} color="var(--faint)" accent="var(--accent-text)" />
      </div>
    </Figure>
  );
}
