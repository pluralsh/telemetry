import { motion } from "motion/react";
import { useStep } from "../deck/context";
import { EASE, Frame, Reveal } from "../ui/primitives";

const ROWS = [
  { label: "RAM", sub: "Foyer memory tier", ns: 100, txt: "~100 ns", color: "var(--logs)" },
  { label: "NVMe", sub: "Foyer disk tier", ns: 100e3, txt: "~100 µs", color: "var(--accent-text)" },
  { label: "S3 GET", sub: "first byte, p50 → tail", ns: 25e6, tail: 150e6, txt: "~25 ms → 150 ms+", color: "var(--warning)" },
];

const HI = Math.log10(400e6);
const pct = (ns: number) => (Math.log10(ns) / HI) * 100;

export function LatencyLadder() {
  const { step, live } = useStep();
  return (
    <div className="flex h-full flex-col gap-8">
      <div className="relative">
        <div className="pointer-events-none absolute inset-y-0 left-[230px] right-[260px]">
          {[
            [1, "1 ns"],
            [1e3, "1 µs"],
            [1e6, "1 ms"],
            [1e8, "100 ms"],
          ].map(([v, t]) => (
            <div key={t} className="absolute inset-y-0 border-l border-dashed border-line" style={{ left: `${v === 1 ? 0 : pct(v as number)}%` }}>
              <span className="code absolute -top-7 -translate-x-1/2 whitespace-nowrap text-[15px] text-faint">{t}</span>
            </div>
          ))}
        </div>
        <div className="flex flex-col gap-3 pt-4">
          {ROWS.map((r, i) => (
            <div key={r.label} className="flex h-[64px] items-center">
              <div className="w-[230px]">
                <div className="code text-[26px] text-ink">{r.label}</div>
                <div className="text-[16px] text-muted">{r.sub}</div>
              </div>
              <div className="relative h-full flex-1">
                <motion.div
                  className="absolute left-0 top-1/2 h-[26px] -translate-y-1/2 rounded-r-[3px]"
                  style={{ background: r.color }}
                  initial={live ? { width: 0 } : false}
                  animate={{ width: `${pct(r.ns)}%` }}
                  transition={{ duration: 0.9, ease: EASE, delay: 0.2 + i * 0.35 }}
                />
                {r.tail && (
                  <motion.div
                    className="absolute top-1/2 h-[26px] -translate-y-1/2 rounded-r-[3px] hatch"
                    style={{ left: `${pct(r.ns)}%`, border: `1.5px dashed ${r.color}`, borderLeft: "none" }}
                    initial={live ? { width: 0 } : false}
                    animate={{ width: `${pct(r.tail) - pct(r.ns)}%` }}
                    transition={{ duration: 0.6, ease: EASE, delay: 1.3 }}
                  />
                )}
              </div>
              <div className="code w-[260px] pl-6 text-[22px] text-ink">{r.txt}</div>
            </div>
          ))}
        </div>
        <Reveal at={1} className="absolute right-[260px] top-[150px] -translate-y-full">
          <div className="font-serif text-[44px] italic leading-none text-warning">×250,000</div>
        </Reveal>
      </div>

      <div className="grid flex-1 grid-cols-2 gap-8">
        <Reveal at={2} className="h-full">
          <Frame label="GET latency → tiered cache" right="Foyer · RAM + NVMe" className="h-full" bodyClassName="p-6 text-[22px] leading-snug text-fg">
            <div className="flex items-center gap-3">
              {["query", "RAM", "NVMe", "S3"].map((t, i) => (
                <div key={t} className="flex items-center gap-3">
                  <motion.span
                    className="code rounded-[3px] border px-3 py-1 text-[20px]"
                    style={{ borderColor: "var(--line-strong)" }}
                    animate={step >= 2 && live ? { borderColor: ["var(--line-strong)", "var(--logs)", "var(--line-strong)"] } : {}}
                    transition={{ duration: 1.2, delay: i * 0.3, repeat: Infinity, repeatDelay: 1.2 }}
                  >
                    {t}
                  </motion.span>
                  {i < 3 && <span className="text-faint">→</span>}
                </div>
              ))}
            </div>
            <ul className="mt-5 space-y-2">
              <li>Blocks that miss RAM fall back to NVMe, and only then to S3.</li>
              <li>Freshly flushed SSTs are pushed into the cache before anyone asks for them.</li>
              <li>Concurrent misses on the same range share one GET.</li>
            </ul>
          </Frame>
        </Reveal>
        <Reveal at={3} className="h-full">
          <Frame label="PUT cost → group commit" right="$0.005 / 1k PUT" className="h-full" bodyClassName="p-6 text-[22px] leading-snug text-fg">
            <div className="grid grid-cols-[1fr_auto] items-baseline gap-x-6 gap-y-3">
              <span>One PUT per write at 50k writes/s</span>
              <span className="code text-[30px] text-danger line-through decoration-2">$648,000/mo</span>
              <span>One PUT per 100 ms WAL flush</span>
              <span className="code text-[30px] text-ok">$130/mo</span>
            </div>
            <p className="mt-5">
              Batching trades a little durability latency for a 5,000× smaller request bill. The flush interval is the main tuning knob: writers can acknowledge at
              <span className="code"> applied</span>, <span className="code">written</span> or <span className="code">durable</span>.
            </p>
          </Frame>
        </Reveal>
      </div>
    </div>
  );
}
