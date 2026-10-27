import { AnimatePresence, motion } from "motion/react";
import { useStep } from "../deck/context";
import { HatchDef } from "../ui/primitives";

const GX = 330;
const GY = 90;
const CW = 290;
const RH = 74;
const HOURS = ["10:00", "11:00", "12:00", "13:00"];
const COLORS = ["var(--metrics)", "var(--logs)", "var(--traces)", "var(--accent-text)", "var(--ok)"];

function Blocks({ shard, hour, delay }: { shard: number; hour: number; delay: number }) {
  const n = 5 + ((shard * 3 + hour * 7) % 4);
  return (
    <g>
      {Array.from({ length: n }, (_, i) => (
        <motion.rect
          key={i}
          x={GX + hour * CW + 14 + i * 30}
          y={GY + shard * RH + 18}
          width={24}
          height={RH - 36}
          rx={2}
          fill={COLORS[shard]}
          fillOpacity={0.75}
          initial={{ opacity: 0, scaleY: 0 }}
          animate={{ opacity: 1, scaleY: 1 }}
          transition={{ delay: delay + i * 0.06, duration: 0.35 }}
          style={{ transformBox: "fill-box", transformOrigin: "bottom" }}
        />
      ))}
    </g>
  );
}

export const EPOCH_CAPTIONS = [
  "Every record is hashed to a storage shard. Each shard is its own SlateDB database with exactly one writer, so SlateDB's single-writer rule still holds.",
  "To scale, add writers. A new ShardMap epoch takes effect at the next hour boundary. New shards start empty, and old data stays where it was written. Nothing is copied or rebalanced.",
  "Ownership is a Kubernetes Lease. If a writer dies, its replacement takes the Lease and reopens the shard from the bucket. No ZooKeeper, no etcd, no gossip.",
];

export function EpochSharding() {
  const { step, live } = useStep();
  const shards = step >= 1 ? 5 : 3;
  return (
    <svg viewBox="0 0 1536 560" className="block h-[540px] w-full overflow-visible">
      <defs>
        <HatchDef id="epoch-hatch" />
      </defs>

      {/* epochs header */}
      <rect x={GX} y={10} width={CW * 2} height={40} rx={3} fill="var(--bg-subtle)" stroke="var(--line-strong)" />
      <text x={GX + CW} y={37} textAnchor="middle" fontSize={18} className="fill-ink font-mono">
        epoch 1 · 3 shards
      </text>
      <motion.g initial={false} animate={{ opacity: step >= 1 ? 1 : 0.35 }}>
        <rect x={GX + CW * 2 + 6} y={10} width={CW * 2 - 6} height={40} rx={3} fill={step >= 1 ? "var(--accent)" : "var(--bg)"} stroke={step >= 1 ? "var(--accent)" : "var(--line-strong)"} strokeDasharray={step >= 1 ? undefined : "4 4"} />
        <text x={GX + CW * 3} y={37} textAnchor="middle" fontSize={18} className="font-mono" fill={step >= 1 ? "#fff" : "var(--muted)"}>
          {step >= 1 ? "epoch 2 · 5 shards" : "next hour boundary"}
        </text>
      </motion.g>

      {/* hour grid */}
      {HOURS.map((h, i) => (
        <g key={h}>
          <line x1={GX + i * CW} x2={GX + i * CW} y1={GY - 10} y2={GY + 5 * RH + 10} stroke="var(--line)" />
          <text x={GX + i * CW + 8} y={GY + 5 * RH + 36} fontSize={16} className="fill-muted font-mono">
            {h}
          </text>
        </g>
      ))}
      <line x1={GX + 2 * CW} x2={GX + 2 * CW} y1={GY - 14} y2={GY + 5 * RH + 14} stroke="var(--accent-text)" strokeWidth={2} strokeDasharray="5 5" />

      {/* shard rows */}
      {Array.from({ length: 5 }, (_, s) => {
        const exists = s < shards;
        const lease = step >= 2;
        const dead = step >= 2 && s === 2;
        return (
          <motion.g key={s} initial={false} animate={{ opacity: exists ? 1 : 0 }} transition={{ duration: 0.5, delay: exists && s >= 3 ? 0.2 * (s - 2) : 0 }}>
            <rect x={GX} y={GY + s * RH + 6} width={CW * 4} height={RH - 12} rx={2} fill="var(--panel)" stroke="var(--line)" />
            {s >= 3 && <rect x={GX} y={GY + s * RH + 6} width={CW * 2} height={RH - 12} fill="url(#epoch-hatch)" />}
            <text x={0} y={GY + s * RH + 44} fontSize={21} className="fill-ink font-mono">
              shard {s}
            </text>
            <AnimatePresence>
              {lease && (
                <motion.g initial={{ opacity: 0, x: -8 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0 }} transition={{ delay: s * 0.08 }}>
                  <rect x={120} y={GY + s * RH + 18} width={190} height={36} rx={3} fill={dead ? "color-mix(in srgb, var(--ok) 10%, var(--panel))" : "var(--bg)"} stroke={dead ? "var(--ok)" : "var(--line-strong)"} />
                  <text x={215} y={GY + s * RH + 42} textAnchor="middle" fontSize={15} className="font-mono" fill={dead ? "var(--ok)" : "var(--muted)"}>
                    {dead ? "Lease → writer-2′" : `Lease · writer-${s}`}
                  </text>
                </motion.g>
              )}
            </AnimatePresence>
          </motion.g>
        );
      })}

      {/* data blocks */}
      {[0, 1, 2].flatMap((s) => [0, 1].map((h) => <Blocks key={`${s}-${h}`} shard={s} hour={h} delay={live ? 0.2 + h * 0.4 + s * 0.1 : 0} />))}
      {step >= 1 && [0, 1, 2, 3, 4].flatMap((s) => [2, 3].map((h) => <Blocks key={`e2-${s}-${h}`} shard={s} hour={h} delay={0.5 + (h - 2) * 0.5 + s * 0.08} />))}

      <AnimatePresence>
        {step === 1 && (
          <motion.g initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ delay: 1.2 }}>
            <rect x={GX + 20} y={GY + 3 * RH + 16} width={CW * 2 - 40} height={RH * 2 - 32} rx={3} fill="var(--bg)" stroke="var(--line-strong)" />
            <text x={GX + CW} y={GY + 4 * RH + 6} textAnchor="middle" fontSize={20} className="fill-ink font-serif" fontStyle="italic">
              nothing copied, nothing rebalanced
            </text>
          </motion.g>
        )}
      </AnimatePresence>
    </svg>
  );
}
