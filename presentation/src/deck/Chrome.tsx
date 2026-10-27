import type { ReactNode } from "react";
import { PluralLogo } from "@/components/icons";

export const RAIL_L = 112;
export const RAIL_R = 1808;
export const HEAD = 92;
export const FOOT = 1000;

function Node({ x, y }: { x: number; y: number }) {
  return <span aria-hidden className="node" style={{ left: x - 5, top: y - 5, width: 10, height: 10 }} />;
}

/** Docs-shell framing shared by every slide: two rails, a header and footer rule, and nodes where they cross. */
export function Chrome({ section, index, total, children }: { section: string; index: number; total: number; children: ReactNode }) {
  const progress = (index + 1) / total;
  return (
    <div className="grain absolute inset-0 overflow-hidden bg-[var(--bg)] text-[var(--ink)]">
      <div aria-hidden className="absolute inset-y-0 border-l border-[var(--line)]" style={{ left: RAIL_L }} />
      <div aria-hidden className="absolute inset-y-0 border-l border-[var(--line)]" style={{ left: RAIL_R }} />
      <div aria-hidden className="absolute inset-x-0 border-t border-[var(--line)]" style={{ top: HEAD }} />
      <div aria-hidden className="absolute inset-x-0 border-t border-[var(--line)]" style={{ top: FOOT }} />
      <div
        aria-hidden
        className="absolute h-[2px] bg-[var(--accent)] transition-[width] duration-500 ease-out"
        style={{ top: FOOT - 1, left: RAIL_L, width: (RAIL_R - RAIL_L) * progress }}
      />
      <Node x={RAIL_L} y={HEAD} />
      <Node x={RAIL_R} y={HEAD} />
      <Node x={RAIL_L} y={FOOT} />
      <Node x={RAIL_R} y={FOOT} />

      <header className="absolute flex items-center justify-between" style={{ left: RAIL_L + 40, right: 1920 - RAIL_R + 40, top: 0, height: HEAD }}>
        <div className="flex items-center gap-4 text-[var(--ink)]">
          <PluralLogo height={30} />
          <span className="h-6 w-px bg-[var(--line-strong)]" />
          <span className="text-[22px] font-medium tracking-[0.01em]">Telemetry</span>
        </div>
        <div className="eyebrow text-[17px]">{section}</div>
      </header>

      <main className="absolute" style={{ left: RAIL_L, right: 1920 - RAIL_R, top: HEAD, bottom: 1080 - FOOT }}>
        {children}
      </main>

      <footer
        className="absolute flex items-center justify-between font-mono text-[16px] text-[var(--muted)]"
        style={{ left: RAIL_L + 40, right: 1920 - RAIL_R + 40, top: FOOT, bottom: 0 }}
      >
        <span>Rebuilding LGTM in Rust</span>
        <span className="tabular-nums">
          <span className="text-[var(--ink)]">{String(index + 1).padStart(2, "0")}</span> / {String(total).padStart(2, "0")}
        </span>
      </footer>
    </div>
  );
}
