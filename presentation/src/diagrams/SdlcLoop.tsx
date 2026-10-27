import { AnimatePresence, motion } from "motion/react";
import { useStep } from "../deck/context";
import { EASE } from "../ui/primitives";

const CX = 420;
const CY = 330;
const R = 250;

const STATIONS = [
  {
    angle: -90,
    title: "Implement + unit test",
    tag: "cargo test · clippy · rustc",
    color: "var(--accent-text)",
    body: "The agent implements against the upstream behaviour (docs, source, and the oracle itself). The compiler and unit tests reject most mistakes within seconds.",
    fail: "doesn't compile, or a unit test fails",
  },
  {
    angle: 30,
    title: "Differential fuzz",
    tag: "same data · same queries",
    color: "var(--logs)",
    body: "Load an identical random dataset into Plural and the source database, generate queries recursively, and diff every answer. A mismatch is a failing seed.",
    fail: "mismatch → replay seed → fix",
  },
  {
    angle: 150,
    title: "Benchmark",
    tag: "target ≥ 2× the peer",
    color: "var(--traces)",
    body: "The same runs record latency per query family, CPU and memory for both sides. If we aren't comfortably faster, the profile becomes the next task.",
    fail: "too slow → profile → optimise",
  },
];

const pt = (deg: number, r = R) => ({ x: CX + r * Math.cos((deg * Math.PI) / 180), y: CY + r * Math.sin((deg * Math.PI) / 180) });
const CIRCLE = `M${CX} ${CY - R} A ${R} ${R} 0 1 1 ${CX - 0.01} ${CY - R} Z`;

export function SdlcLoop() {
  const { step } = useStep();
  const active = step - 1;
  return (
    <div className="grid h-full grid-cols-[860px_1fr] items-center gap-12">
      <svg viewBox="0 0 860 660" className="block h-auto w-full overflow-visible">
        <defs>
          <marker id="loop-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="10" markerHeight="10" orient="auto">
            <path d="M0 0L10 5L0 10z" fill="var(--line-strong)" />
          </marker>
        </defs>
        <circle cx={CX} cy={CY} r={R} fill="none" stroke="var(--faint)" strokeWidth={2} strokeDasharray="3 9" />
        {[-30, 90, 210].map((a) => {
          const p = pt(a);
          const tangent = a + 90;
          return <path key={a} d={`M${p.x} ${p.y} l ${Math.cos((tangent * Math.PI) / 180) * 1} ${Math.sin((tangent * Math.PI) / 180) * 1}`} stroke="var(--line-strong)" markerEnd="url(#loop-arrow)" />;
        })}

        <text x={CX} y={CY - 10} textAnchor="middle" fontSize={44} className="fill-ink font-serif">
          agents
        </text>
        <text x={CX} y={CY + 26} textAnchor="middle" fontSize={18} className="fill-muted font-mono">
          24 hours a day
        </text>

        {STATIONS.map((s, i) => {
          const p = pt(s.angle);
          const on = active === i;
          return (
            <g key={s.title}>
              <motion.rect
                x={p.x - 150}
                y={p.y - 48}
                width={300}
                height={96}
                rx={4}
                fill="var(--panel)"
                stroke={s.color}
                initial={false}
                animate={{ strokeWidth: on ? 3.5 : 1.5, scale: on ? 1.06 : 1 }}
                style={{ transformBox: "fill-box", transformOrigin: "center" }}
              />
              <text x={p.x} y={p.y - 6} textAnchor="middle" fontSize={24} className="fill-ink">
                {s.title}
              </text>
              <text x={p.x} y={p.y + 24} textAnchor="middle" fontSize={15} className="fill-muted font-mono">
                {s.tag}
              </text>
            </g>
          );
        })}

        <circle r={12} fill="var(--accent)" stroke="var(--bg)" strokeWidth={3}>
          <animateMotion dur="6s" repeatCount="indefinite" path={CIRCLE} />
        </circle>
      </svg>

      <div className="relative h-[520px]">
        <AnimatePresence mode="wait">
          {active < 0 || active > 2 ? (
            <motion.div key="intro" initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -12 }} transition={{ duration: 0.4, ease: EASE }} className="space-y-6 text-[30px] leading-snug text-fg">
              <p>
                Every feature, optimisation and bug fix goes around the same loop. Nothing merges until <span className="text-ink">all three gates</span> pass.
              </p>
              <p>Humans pick the next target and review the diff. Agents do the laps.</p>
            </motion.div>
          ) : (
            <motion.div key={active} initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -12 }} transition={{ duration: 0.4, ease: EASE }}>
              <div className="eyebrow text-[20px]">gate {active + 1} / 3</div>
              <h2 className="mt-2 font-serif text-[56px] leading-tight" style={{ color: STATIONS[active].color }}>
                {STATIONS[active].title}
              </h2>
              <p className="mt-5 text-[28px] leading-snug text-fg">{STATIONS[active].body}</p>
              <div className="code mt-8 inline-flex items-center gap-3 rounded-[3px] border border-danger/40 bg-[color-mix(in_srgb,var(--danger)_7%,transparent)] px-4 py-2 text-[20px] text-danger">
                ↺ {STATIONS[active].fail}
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}
