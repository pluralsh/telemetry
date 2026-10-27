import { motion } from "motion/react";
import { useStep } from "../deck/context";
import { EASE } from "../ui/primitives";

/** Monthly cost ranges (USD) and shape notes from Mat Duggan, "Clickhouse is winning the Observability Wars" (2026). */
const TIERS = ["1 TB/day", "5 TB/day", "10 TB/day"];
const STACKS: { name: string; color: string; ranges: [number, number][]; notes: string[] }[] = [
  {
    name: "Datadog",
    color: "var(--warning)",
    ranges: [
      [45e3, 75e3],
      [180e3, 350e3],
      [600e3, 1.2e6],
    ],
    notes: ["install the agent, go home", "+ a pipeline team to cut the bill", "six to seven figures a month"],
  },
  {
    name: "Elasticsearch",
    color: "var(--faint)",
    ranges: [
      [6e3, 9e3],
      [40e3, 55e3],
      [95e3, 140e3],
    ],
    notes: ["ILM policies, mapping templates", "+ Kafka, shard math, licensing", "three federated clusters"],
  },
  {
    name: "LGTM",
    color: "var(--accent-text)",
    ranges: [
      [3.5e3, 5e3],
      [22e3, 32e3],
      [55e3, 85e3],
    ],
    notes: ["Alloy + label hygiene", "65+ pods, three hash rings", "180+ pods, 3–5 person team"],
  },
];

const LO = Math.log10(2e3);
const HI = Math.log10(1.5e6);
const pct = (v: number) => ((Math.log10(v) - LO) / (HI - LO)) * 100;
const fmt = (v: number) => (v >= 1e6 ? `$${+(v / 1e6).toFixed(1)}M` : `$${Math.round(v / 1e3)}K`);

export function StackTiers() {
  const { step } = useStep();
  const tier = Math.min(step, 2);
  return (
    <div className="flex h-full flex-col">
      <div className="mb-6 flex items-center gap-3">
        {TIERS.map((t, i) => (
          <div
            key={t}
            className={`code rounded-[3px] border px-4 py-1.5 text-[22px] transition-colors duration-300 ${i === tier ? "border-accent bg-accent text-white" : "border-line-strong text-muted"}`}
          >
            {t}
          </div>
        ))}
        <span className="ml-auto eyebrow text-[17px]">monthly cost, log scale</span>
      </div>
      <div className="relative flex flex-1 flex-col justify-between">
        <div className="pointer-events-none absolute inset-y-0 left-[260px] right-[440px]">
          {[1e4, 1e5, 1e6].map((v) => (
            <div key={v} className="absolute inset-y-0 border-l border-dashed border-line" style={{ left: `${pct(v)}%` }}>
              <span className="code absolute -bottom-8 -translate-x-1/2 text-[16px] text-faint">{fmt(v)}</span>
            </div>
          ))}
        </div>
        {STACKS.map((s) => {
          const [lo, hi] = s.ranges[tier];
          return (
            <div key={s.name} className="relative flex h-[84px] items-center">
              <div className="w-[260px] text-[30px] text-ink">{s.name}</div>
              <div className="relative h-full flex-1">
                <motion.div
                  className="absolute top-1/2 h-[30px] -translate-y-1/2 rounded-[3px]"
                  style={{ background: `color-mix(in srgb, ${s.color} 22%, transparent)`, border: `1.5px solid ${s.color}` }}
                  initial={false}
                  animate={{ left: `${pct(lo)}%`, width: `${Math.max(pct(hi) - pct(lo), 1.2)}%` }}
                  transition={{ duration: 0.9, ease: EASE }}
                />
                <motion.div
                  className="code absolute top-1/2 whitespace-nowrap px-3 text-[20px] text-ink"
                  initial={false}
                  animate={pct(hi) > 70 ? { left: `${pct(lo)}%`, x: "-100%", y: "-50%" } : { left: `${pct(hi)}%`, x: "0%", y: "-50%" }}
                  transition={{ duration: 0.9, ease: EASE }}
                >
                  {fmt(lo)}–{fmt(hi)}
                  {s.name === "Datadog" && tier === 2 ? "+" : ""}
                </motion.div>
              </div>
              <motion.div key={`${s.name}${tier}`} initial={{ opacity: 0, x: 10 }} animate={{ opacity: 1, x: 0 }} transition={{ duration: 0.5 }} className="w-[440px] pl-8 text-[21px] italic text-muted">
                {s.notes[tier]}
              </motion.div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
