import { AnimatePresence, motion } from "motion/react";
import type { ArchNode, ArchSide, Mode } from "@/components/diagrams/arch-configs";
import { HatchDef } from "../ui/primitives";

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
            transition={{ delay: (i % 16) * 0.02, type: "spring", stiffness: 420, damping: 24 }}
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

/** Slide-scale version of the docs' ArchitectureCompare panel, driven by props instead of controls. */
export function ArchPanel({
  side,
  mode,
  level,
  q = 2,
  color,
  showStats = true,
  dim = false,
}: {
  side: ArchSide;
  mode: Mode | "both" | "none";
  level: number;
  q?: number;
  color: string;
  showStats?: boolean;
  dim?: boolean;
}) {
  const active = (n: ArchNode) => mode === "both" || mode === "none" || !n.modes || n.modes.includes(mode);
  return (
    <div className="flex min-w-0 flex-1 flex-col transition-opacity duration-500" style={{ opacity: dim ? 0.35 : 1 }}>
      <div className="flex items-baseline gap-3 px-5 pb-3 pt-4">
        <div className="text-[26px] font-medium text-ink">{side.title}</div>
        <span className="leader" />
        <div className="eyebrow text-[16px]">{side.subtitle}</div>
      </div>
      <div className="border-y border-line bg-bg">
        <svg viewBox="0 0 460 418" className="block h-auto w-full" style={{ ["--c" as string]: color }}>
          <defs>
            <HatchDef id="arch-hatch" />
          </defs>
          {side.nodes.map((n) => {
            const t = TONE[n.tone ?? "stateless"];
            const on = active(n);
            return (
              <g key={n.id} style={{ opacity: on ? 1 : 0.3, transition: "opacity .35s" }}>
                <rect x={n.x} y={n.y} width={n.w} height={n.h} rx={2} fill={t.fill} stroke={t.stroke} strokeOpacity={n.tone === "stateful" ? 0.6 : 1} strokeDasharray={t.dash} />
                <text x={n.x + 8} y={n.y + 16} fontSize={n.w < 120 ? 9.5 : 11.5} className="fill-ink font-mono">
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
          {mode !== "none" &&
            side.flows
              .filter((f) => mode === "both" || f.mode === mode)
              .map((f, i) => {
                const c = f.color ?? (f.mode === "write" ? color : "var(--accent-text)");
                const dur = f.dur ?? 2.4;
                return (
                  <g key={`${mode}-${i}`}>
                    <path d={f.d} fill="none" stroke={c} strokeOpacity={0.45} strokeWidth={1.25} className="flow-dash" />
                    {Array.from({ length: f.count ?? 1 }, (_, k) => (
                      <rect key={k} x={-3} y={-3} width={6} height={6} fill={c} visibility="hidden">
                        <set attributeName="visibility" to="visible" begin={`${(k * dur) / (f.count ?? 1) + i * 0.13}s`} />
                        <animateMotion dur={`${dur}s`} begin={`${(k * dur) / (f.count ?? 1) + i * 0.13}s`} repeatCount="indefinite" path={f.d} />
                      </rect>
                    ))}
                  </g>
                );
              })}
        </svg>
      </div>
      {showStats && <ArchStats side={side} level={level} q={q} />}
    </div>
  );
}

export function ArchStats({ side, level, q = 2, className = "" }: { side: ArchSide; level: number; q?: number; className?: string }) {
  return (
    <dl className={`grid grid-cols-4 ${className}`}>
      {side.stats(level, q).map((s, i) => (
        <div key={s.label} className={`px-4 py-3 ${i > 0 ? "border-l border-line" : ""}`}>
          <dd className="whitespace-nowrap font-serif text-[40px] leading-none tabular-nums text-ink">{s.value}</dd>
          <dt className="mt-2 text-[16px] leading-tight text-muted">{s.label}</dt>
        </div>
      ))}
    </dl>
  );
}
