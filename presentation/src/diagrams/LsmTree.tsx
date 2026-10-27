import { AnimatePresence, motion } from "motion/react";
import { useStep } from "../deck/context";
import { EASE } from "../ui/primitives";

const BATCHES = [
  { id: "A", keys: [42, 7, 88, 23, 61, 15], color: "var(--metrics)" },
  { id: "B", keys: [33, 90, 4, 57, 19, 71], color: "var(--logs)" },
  { id: "C", keys: [12, 66, 45, 2, 80, 28], color: "var(--traces)" },
];

const CW = 54;
const CH = 42;
const PITCH = 60;
const BOX_W = 6 * PITCH - 6 + 24;
const X0 = 250;
const MEM = { x: X0, y: 30 };
const L0Y = 230;
const L1Y = 440;
const L0X = [X0, X0 + BOX_W + 30, X0 + 2 * (BOX_W + 30)];
const L1X = [X0, X0 + BOX_W + 8, X0 + 2 * (BOX_W + 8)];
const BOX_H = 104;

const merged = BATCHES.flatMap((b) => b.keys).sort((a, b) => a - b);

type Place = { x: number; y: number; o: number; delay: number };

function place(batch: number, insertIdx: number, key: number, step: number): Place {
  const sorted = [...BATCHES[batch].keys].sort((a, b) => a - b);
  const j = sorted.indexOf(key);
  const chipY = 44;
  const memAt = batch;
  const l0At = batch + 1;
  if (step >= 4) {
    const k = merged.indexOf(key);
    return { x: L1X[Math.floor(k / 6)] + 12 + (k % 6) * PITCH, y: L1Y + chipY, o: 1, delay: 0.25 + k * 0.035 };
  }
  if (step >= l0At) return { x: L0X[batch] + 12 + j * PITCH, y: L0Y + chipY, o: 1, delay: j * 0.03 };
  if (step >= memAt) return { x: MEM.x + 12 + j * PITCH, y: MEM.y + chipY, o: 1, delay: 0.15 + insertIdx * 0.16 };
  return { x: 40, y: MEM.y + chipY, o: 0, delay: 0 };
}

function Box({ x, y, label, sub, show = true, tone = "var(--line-strong)", fill = "var(--panel)", dash }: { x: number; y: number; label: string; sub?: string; show?: boolean; tone?: string; fill?: string; dash?: string }) {
  return (
    <motion.g initial={false} animate={{ opacity: show ? 1 : 0 }} transition={{ duration: 0.4 }}>
      <rect x={x} y={y} width={BOX_W} height={BOX_H} rx={3} fill={fill} stroke={tone} strokeDasharray={dash} />
      <text x={x + 12} y={y + 26} fontSize={17} className="fill-ink font-mono">
        {label}
      </text>
      {sub && (
        <text x={x + BOX_W - 12} y={y + 26} fontSize={15} textAnchor="end" className="fill-muted font-mono">
          {sub}
        </text>
      )}
    </motion.g>
  );
}

type Probe = { x: number; y: number; w: number; result: string; ok?: boolean };

const PROBES_BEFORE: Probe[] = [
  { x: MEM.x, y: MEM.y, w: BOX_W, result: "miss" },
  { x: L0X[2], y: L0Y, w: BOX_W, result: "bloom ✗" },
  { x: L0X[1], y: L0Y, w: BOX_W, result: "bloom ✗" },
  { x: L0X[0], y: L0Y, w: BOX_W, result: "hit ✓", ok: true },
];
const PROBES_AFTER: Probe[] = [
  { x: MEM.x, y: MEM.y, w: BOX_W, result: "miss" },
  { x: L1X[1], y: L1Y, w: BOX_W, result: "hit ✓", ok: true },
];

function Probes({ probes, k }: { probes: Probe[]; k: string }) {
  return (
    <g key={k}>
      {probes.map((p, i) => (
        <motion.g key={i} initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0, transition: { duration: 0.15 } }} transition={{ delay: 0.4 + i * 0.55, duration: 0.25 }}>
          <rect x={p.x - 5} y={p.y - 5} width={p.w + 10} height={BOX_H + 10} rx={5} fill="none" stroke={p.ok ? "var(--ok)" : "var(--accent-text)"} strokeWidth={2.5} />
          <rect x={p.x + p.w - 112} y={p.y + BOX_H - 2} width={116} height={28} rx={3} fill={p.ok ? "var(--ok)" : "var(--accent)"} />
          <text x={p.x + p.w - 54} y={p.y + BOX_H + 17} fontSize={15} textAnchor="middle" className="font-mono" fill="#fff">
            {i + 1}. {p.result}
          </text>
        </motion.g>
      ))}
    </g>
  );
}

export const LSM_CAPTIONS = [
  "Writes land in an in-memory memtable, kept sorted by key. Nothing is written in place on disk.",
  "When the memtable fills, it is frozen and flushed as an immutable, sorted file (an SST) in level 0.",
  "Flushes keep coming. Each one is a new L0 file. Writes stay sequential and cheap: no file is ever rewritten.",
  "The cost moves to reads. L0 files overlap, so get(23) checks every one, newest first. Bloom filters skip most of them.",
  "Compaction merges L0 files in the background into one sorted run of non-overlapping SSTs, and drops the old files.",
  "Now get(23) is one range lookup. LSM trees trade cheap sequential writes for background merge work, which suits append-heavy telemetry.",
];

export function LsmTree() {
  const { step, live } = useStep();
  return (
    <svg viewBox="0 0 1536 600" className="block h-[540px] w-full overflow-visible">
      {/* level labels */}
      {[
        { y: MEM.y, t: "memtable", s: "RAM" },
        { y: L0Y, t: "L0", s: "SSTs" },
        { y: L1Y, t: "L1", s: "sorted run" },
      ].map((l) => (
        <g key={l.t}>
          <text x={0} y={l.y + 50} fontSize={30} className="fill-ink font-serif">
            {l.t}
          </text>
          <text x={0} y={l.y + 80} fontSize={16} className="fill-muted font-mono">
            {l.s}
          </text>
        </g>
      ))}
      <line x1={0} x2={1536} y1={172} y2={172} stroke="var(--line-strong)" strokeDasharray="6 6" />
      <text x={1536} y={162} fontSize={15} textAnchor="end" className="fill-muted font-mono">
        memory ↑
      </text>
      <text x={1536} y={194} fontSize={15} textAnchor="end" className="fill-muted font-mono">
        durable storage ↓
      </text>

      <Box x={MEM.x} y={MEM.y} label="memtable" sub={step >= 3 ? "empty" : "6 / 6"} tone="var(--accent-text)" fill="color-mix(in srgb, var(--accent-text) 5%, var(--panel))" />
      {L0X.map((x, i) => (
        <Box key={i} x={x} y={L0Y} label={`L0 · sst ${i + 1}`} sub={i === 2 ? "newest" : i === 0 ? "oldest" : undefined} show={step >= i + 1 && step < 4} />
      ))}
      {L1X.map((x, i) => {
        const ks = merged.slice(i * 6, i * 6 + 6);
        return <Box key={i} x={x} y={L1Y} label={`sst ${i + 1}`} sub={`${ks[0]}–${ks[5]}`} show={step >= 4} fill="var(--panel)" />;
      })}

      {/* compaction arrows */}
      <AnimatePresence>
        {step === 4 && (
          <motion.g initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
            {L0X.map((x, i) => (
              <path key={i} d={`M${x + BOX_W / 2} ${L0Y + BOX_H + 8} C ${x + BOX_W / 2} ${L1Y - 50}, ${L1X[1] + BOX_W / 2} ${L0Y + BOX_H + 40}, ${L1X[1] + BOX_W / 2} ${L1Y - 8}`} fill="none" stroke="var(--accent-text)" strokeWidth={2} className="flow-dash" />
            ))}
            <rect x={L1X[1] + BOX_W / 2 - 92} y={L0Y + BOX_H + 40} width={184} height={34} rx={3} fill="var(--panel)" stroke="var(--accent-text)" />
            <text x={L1X[1] + BOX_W / 2} y={L0Y + BOX_H + 63} textAnchor="middle" fontSize={17} className="fill-ink font-mono">
              merge-sort compaction
            </text>
          </motion.g>
        )}
      </AnimatePresence>

      {/* key chips */}
      {BATCHES.map((b, bi) =>
        b.keys.map((key, ii) => {
          const p = place(bi, ii, key, step);
          return (
            <motion.g
              key={key}
              initial={live ? { x: 40, y: MEM.y + 44, opacity: 0 } : false}
              animate={{ x: p.x, y: p.y, opacity: p.o }}
              transition={{ duration: 0.7, ease: EASE, delay: p.delay }}
            >
              <rect width={CW} height={CH} rx={3} fill={`color-mix(in srgb, ${b.color} 14%, var(--panel))`} stroke={b.color} />
              <text x={CW / 2} y={28} textAnchor="middle" fontSize={19} className="fill-ink font-mono">
                {key}
              </text>
            </motion.g>
          );
        }),
      )}

      {/* read probes */}
      <AnimatePresence>
        {(step === 3 || step === 5) && (
          <motion.g key={`q${step}`} initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
            <rect x={X0 + 3 * (BOX_W + 30) - 190} y={MEM.y + 30} width={180} height={46} rx={3} fill="var(--ink)" />
            <text x={X0 + 3 * (BOX_W + 30) - 100} y={MEM.y + 60} textAnchor="middle" fontSize={20} className="font-mono" fill="var(--bg)">
              get(23)
            </text>
          </motion.g>
        )}
      </AnimatePresence>
      <AnimatePresence>{step === 3 && <Probes probes={PROBES_BEFORE} k="before" />}</AnimatePresence>
      <AnimatePresence>{step === 5 && <Probes probes={PROBES_AFTER} k="after" />}</AnimatePresence>
    </svg>
  );
}
