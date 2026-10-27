import { motion } from "motion/react";
import { useStep } from "../deck/context";

const W = 860;
const H = 560;
const PAD = { l: 40, r: 170, t: 30, b: 50 };
const N = 60;

const SERIES = [
  { id: "code", label: "code shipped", color: "var(--accent-text)", f: (t: number) => Math.exp(3.1 * t) },
  { id: "telemetry", label: "telemetry volume", color: "var(--logs)", f: (t: number) => Math.exp(3.3 * t) * 0.92 },
  { id: "bill", label: "vendor bill", color: "var(--warning)", f: (t: number) => Math.exp(3.6 * t) * 0.8 },
  { id: "ops", label: "cost to self-host", color: "var(--ink)", f: (t: number) => Math.exp(3.3 * t) * 0.5 },
];

const MAX = Math.max(...SERIES.map((s) => s.f(1)));
const x = (t: number) => PAD.l + t * (W - PAD.l - PAD.r);
const y = (v: number) => H - PAD.b - (v / MAX) * (H - PAD.t - PAD.b);
const path = (f: (t: number) => number) => Array.from({ length: N + 1 }, (_, i) => `${i ? "L" : "M"}${x(i / N).toFixed(1)} ${y(f(i / N)).toFixed(1)}`).join(" ");

export function ForcesChart() {
  const { step, live } = useStep();
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="block h-full w-full overflow-visible">
      <line x1={PAD.l} x2={W - PAD.r} y1={H - PAD.b} y2={H - PAD.b} stroke="var(--line-strong)" />
      <line x1={PAD.l} x2={PAD.l} y1={PAD.t} y2={H - PAD.b} stroke="var(--line-strong)" />
      {["2020", "2022", "2024", "2026"].map((yr, i) => (
        <text key={yr} x={x(i / 3)} y={H - PAD.b + 30} textAnchor="middle" fontSize={16} className="fill-muted font-mono">
          {yr}
        </text>
      ))}
      <text x={PAD.l + 8} y={PAD.t + 8} fontSize={15} className="fill-faint font-mono">
        illustrative
      </text>

      {SERIES.map((s, i) => {
        const on = step >= i;
        return (
          <g key={s.id}>
            <motion.path
              d={path(s.f)}
              fill="none"
              stroke={s.color}
              strokeWidth={3.5}
              strokeLinecap="round"
              initial={live ? { pathLength: 0, opacity: 0 } : false}
              animate={{ pathLength: on ? 1 : 0, opacity: on ? 1 : 0 }}
              transition={{ duration: 1.1, ease: "easeInOut" }}
            />
            <motion.g initial={false} animate={{ opacity: on ? 1 : 0 }} transition={{ delay: on ? 0.9 : 0 }}>
              <circle cx={x(1)} cy={y(s.f(1))} r={6} fill={s.color} />
              <text x={x(1) + 14} y={y(s.f(1)) + 6} fontSize={19} className="font-mono" fill={s.color}>
                {s.label}
              </text>
            </motion.g>
          </g>
        );
      })}
    </svg>
  );
}
