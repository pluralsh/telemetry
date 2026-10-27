import { AnimatePresence, motion } from "motion/react";
import { useStep } from "../deck/context";
import { EASE } from "../ui/primitives";

const QUERIES = [
  { q: 'sum by (level) (count_over_time({app="api"}[5m]))', ok: true },
  { q: '{app="checkout"} |= "timeout" | json | status >= 500', ok: true },
  { q: 'rate({env="prod"} | logfmt | unwrap latency_ms [1m])', ok: true },
  { q: 'topk(3, sum by (pod) (bytes_over_time({ns="pay"}[10m])))', ok: true },
  { q: 'quantile_over_time(0.99, {app="api"} | unwrap dur [5m])', ok: false },
];

function Node({ x, y, w, h, title, sub, color }: { x: number; y: number; w: number; h: number; title: string; sub: string; color: string }) {
  return (
    <g>
      <rect x={x} y={y} width={w} height={h} rx={4} fill="var(--panel)" stroke={color} strokeWidth={1.5} />
      <rect x={x} y={y} width={w} height={4} fill={color} />
      <text x={x + 20} y={y + 44} fontSize={28} className="fill-ink font-serif">
        {title}
      </text>
      <text x={x + 20} y={y + 72} fontSize={16} className="fill-muted font-mono">
        {sub}
      </text>
    </g>
  );
}

function Packets({ d, color, n = 3, dur = 1.8, delay = 0 }: { d: string; color: string; n?: number; dur?: number; delay?: number }) {
  return (
    <g>
      <path d={d} fill="none" stroke={color} strokeOpacity={0.4} strokeWidth={1.5} className="flow-dash" />
      {Array.from({ length: n }, (_, k) => (
        <rect key={k} x={-5} y={-5} width={10} height={10} rx={1} fill={color} visibility="hidden">
          <set attributeName="visibility" to="visible" begin={`${delay + (k * dur) / n}s`} />
          <animateMotion dur={`${dur}s`} begin={`${delay + (k * dur) / n}s`} repeatCount="indefinite" path={d} />
        </rect>
      ))}
    </g>
  );
}

export function OracleDiff() {
  const { step } = useStep();
  return (
    <div className="grid h-full grid-cols-[860px_1fr] gap-10">
      <svg viewBox="0 0 860 560" className="block h-auto w-full overflow-visible">
        <Node x={0} y={210} w={230} h={130} title="fuzzer" sub="seed 0x5eed" color="var(--ink)" />
        <Node x={420} y={40} w={300} h={130} title="Plural Logs" sub="implementation" color="var(--logs)" />
        <Node x={420} y={380} w={300} h={130} title="Grafana Loki" sub="oracle" color="var(--faint)" />

        <Packets d="M230 250 C 330 250, 330 105, 420 105" color="var(--logs)" />
        <Packets d="M230 300 C 330 300, 330 445, 420 445" color="var(--faint)" />
        <text x={245} y={235} fontSize={15} className="fill-muted font-mono">
          {step >= 1 ? "same queries" : "same records"}
        </text>

        <AnimatePresence>
          {step >= 1 && (
            <motion.g initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
              <Packets d="M720 105 C 800 105, 800 260, 840 260" color="var(--accent-text)" n={2} dur={1.4} delay={0.4} />
              <Packets d="M720 445 C 800 445, 800 300, 840 300" color="var(--accent-text)" n={2} dur={1.4} delay={0.4} />
              <circle cx={844} cy={280} r={30} fill="var(--panel)" stroke="var(--accent-text)" strokeWidth={2} />
              <text x={844} y={287} textAnchor="middle" fontSize={20} className="font-mono" fill="var(--accent-text)">
                diff
              </text>
            </motion.g>
          )}
        </AnimatePresence>
        <text x={0} y={390} fontSize={16} className="fill-muted">
          NaN, ±Inf, odd Unicode, clock skew,
        </text>
        <text x={0} y={414} fontSize={16} className="fill-muted">
          out-of-order bursts, cardinality extremes
        </text>
      </svg>

      <div className="flex flex-col gap-3">
        <div className="label text-[15px]">results · nightly, all three products</div>
        <AnimatePresence>
          {step >= 1 &&
            QUERIES.slice(0, step >= 2 ? 5 : 4).map((r, i) => (
              <motion.div
                key={r.q}
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.45, ease: EASE, delay: r.ok ? 0.5 + i * 0.45 : 0.3 }}
                className={`flex items-start gap-4 rounded-[3px] border px-4 py-3 ${r.ok ? "border-line bg-panel" : "border-danger bg-[color-mix(in_srgb,var(--danger)_8%,var(--panel))]"}`}
              >
                <span className={`code mt-0.5 text-[22px] ${r.ok ? "text-ok" : "text-danger"}`}>{r.ok ? "✓" : "✗"}</span>
                <span className="code text-[18px] leading-snug text-ink">{r.q}</span>
              </motion.div>
            ))}
        </AnimatePresence>
        <AnimatePresence>
          {step >= 2 && (
            <motion.div
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0 }}
              transition={{ delay: 0.8, duration: 0.5 }}
              className="mt-2 border-l-2 border-accent pl-5 text-[22px] leading-snug text-fg"
            >
              Every mismatch is replayable from its seed. It gets fixed, or traced to a bug in the oracle, and then becomes a permanent regression fixture.
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}
