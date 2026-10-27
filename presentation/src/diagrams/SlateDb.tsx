import { AnimatePresence, motion } from "motion/react";
import { useEffect, useState } from "react";
import { useStep } from "../deck/context";
import { EASE, HatchDef } from "../ui/primitives";

const BUCKET_Y = 330;
const WAL_BUF = { x: 30, y: 78, w: 300, h: 110 };
const MEMT = { x: 370, y: 78, w: 300, h: 110 };
const FW = 66;
const FH = 44;

function useTicker(live: boolean, ms: number) {
  const [n, setN] = useState(6);
  useEffect(() => {
    if (!live) return;
    const t = setInterval(() => setN((v) => v + 1), ms);
    return () => clearInterval(t);
  }, [live, ms]);
  return n;
}

function File({ x, y, label, color = "var(--line-strong)", w = FW, from }: { x: number; y: number; label: string; color?: string; w?: number; from?: { x: number; y: number } }) {
  return (
    <motion.g
      initial={from ? { x: from.x, y: from.y, opacity: 0 } : { x, y, opacity: 0 }}
      animate={{ x, y, opacity: 1 }}
      exit={{ opacity: 0, scale: 0.6 }}
      transition={{ duration: 0.8, ease: EASE }}
    >
      <rect width={w} height={FH} rx={3} fill="var(--panel)" stroke={color} strokeWidth={1.5} />
      <path d={`M${w - 14} 0 L${w} 14`} stroke={color} strokeWidth={1.2} />
      <text x={w / 2} y={28} textAnchor="middle" fontSize={15} className="fill-ink font-mono">
        {label}
      </text>
    </motion.g>
  );
}

function Proc({ x, y, w, h, title, sub, color = "var(--ink)", children }: { x: number; y: number; w: number; h: number; title: string; sub?: string; color?: string; children?: React.ReactNode }) {
  return (
    <g>
      <rect x={x} y={y} width={w} height={h} rx={4} fill="var(--panel)" stroke={color} strokeOpacity={0.7} />
      <text x={x + 18} y={y + 34} fontSize={24} className="fill-ink font-serif">
        {title}
      </text>
      {sub && (
        <text x={x + w - 18} y={y + 32} fontSize={15} textAnchor="end" className="fill-muted font-mono">
          {sub}
        </text>
      )}
      {children}
    </g>
  );
}

function Inner({ x, y, w, h, label, sub, color }: { x: number; y: number; w: number; h: number; label: string; sub: string; color: string }) {
  return (
    <g>
      <rect x={x} y={y} width={w} height={h} rx={3} fill={`color-mix(in srgb, ${color} 6%, var(--bg))`} stroke={color} strokeOpacity={0.8} />
      <text x={x + 14} y={y + 30} fontSize={19} className="fill-ink font-mono">
        {label}
      </text>
      <text x={x + 14} y={y + 54} fontSize={15} className="fill-muted" fontStyle="italic">
        {sub}
      </text>
    </g>
  );
}

export const SLATE_CAPTIONS = [
  "Every put goes into a WAL buffer. Each flush interval (100 ms by default) the buffer is uploaded as one object. One PUT carries thousands of writes.",
  "The memtable is flushed as an L0 SST, and a new manifest version records it. The manifest is the single source of truth for which files make up the database.",
  "A compactor merges L0 files into sorted runs, entirely through the bucket. It doesn't need the writer's disk, because the writer has no disk.",
  "Readers fetch only the blocks they need, through a Foyer cache in RAM and on NVMe. Local disk is a disposable cache, not a replica.",
];

export function SlateDb() {
  const { step, live } = useStep();
  const n = useTicker(live, 1100);
  const wal = Array.from({ length: 6 }, (_, i) => n - 5 + i);
  const manifest = 1 + (step >= 1 ? 1 : 0) + (step >= 2 ? 1 : 0);

  return (
    <svg viewBox="0 0 1536 640" className="block h-[560px] w-full overflow-visible">
      <defs>
        <HatchDef id="slate-hatch" />
        <marker id="slate-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="8" markerHeight="8" orient="auto">
          <path d="M0 0L8 4L0 8z" fill="var(--faint)" />
        </marker>
      </defs>

      {/* writer */}
      <Proc x={0} y={0} w={700} h={220} title="writer" sub="one per database · fenced">
        <Inner {...WAL_BUF} label="WAL buffer" sub="group commit" color="var(--accent-text)" />
        <Inner {...MEMT} label="memtable" sub="sorted · in RAM" color="var(--accent-text)" />
        <path d={`M${WAL_BUF.x + WAL_BUF.w + 4} ${WAL_BUF.y + 55} H${MEMT.x - 6}`} stroke="var(--faint)" markerEnd="url(#slate-arrow)" />
      </Proc>
      <text x={0} y={-18} fontSize={17} className="fill-muted font-mono">
        put(k, v) ×50k/s
      </text>
      {[60, 150, 240].map((x, i) => (
        <g key={x}>
          <path d={`M${x} -8 V${WAL_BUF.y - 4}`} stroke="var(--accent-text)" strokeOpacity={0.5} className="flow-dash" />
          <rect x={-3} y={-3} width={6} height={6} fill="var(--accent-text)" visibility="hidden">
            <set attributeName="visibility" to="visible" begin={`${i * 0.3}s`} />
            <animateMotion dur="1s" begin={`${i * 0.3}s`} repeatCount="indefinite" path={`M${x} -8 V${WAL_BUF.y}`} />
          </rect>
        </g>
      ))}

      {/* ack modes */}
      <g transform="translate(730 40)">
        <text fontSize={15} className="fill-muted font-mono">
          ack when…
        </text>
        {[
          ["applied", "in memtable"],
          ["written", "WAL handed off"],
          ["durable", "WAL in S3"],
        ].map(([a, b], i) => (
          <g key={a} transform={`translate(0 ${24 + i * 40})`}>
            <rect width={104} height={30} rx={3} fill="var(--bg)" stroke="var(--line-strong)" />
            <text x={52} y={21} textAnchor="middle" fontSize={15} className="fill-ink font-mono">
              {a}
            </text>
            <text x={116} y={21} fontSize={15} className="fill-muted">
              {b}
            </text>
          </g>
        ))}
      </g>

      {/* compactor */}
      <AnimatePresence>
        {step >= 2 && (
          <motion.g initial={{ opacity: 0, y: -10 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}>
            <Proc x={980} y={20} w={250} h={120} title="compactor" sub="async" color="var(--line-strong)" />
            <path d="M1105 140 V320" stroke="var(--faint)" className="flow-dash" />
          </motion.g>
        )}
      </AnimatePresence>

      {/* reader with cache */}
      <AnimatePresence>
        {step >= 3 && (
          <motion.g initial={{ opacity: 0, y: -10 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}>
            <Proc x={1256} y={0} w={280} h={260} title="reader" sub="read-only" color="var(--logs)">
              <Inner x={1274} y={56} w={244} h={76} label="RAM" sub="hot blocks" color="var(--logs)" />
              <Inner x={1274} y={150} w={244} h={76} label="NVMe" sub="warm blocks" color="var(--logs)" />
            </Proc>
            <path d="M1396 320 V232" stroke="var(--logs)" strokeWidth={2} className="flow-dash" style={{ animationDirection: "reverse" }} />
            <text x={1408} y={300} fontSize={15} className="fill-muted font-mono">
              GET ranges
            </text>
          </motion.g>
        )}
      </AnimatePresence>

      {/* upload path */}
      <path d={`M${WAL_BUF.x + 150} ${WAL_BUF.y + WAL_BUF.h} V${BUCKET_Y + 50}`} stroke="var(--accent-text)" strokeWidth={1.5} strokeOpacity={0.6} className="flow-dash" />
      <text x={WAL_BUF.x + 162} y={278} fontSize={15} className="fill-muted font-mono">
        PUT every 100 ms
      </text>
      <motion.g initial={false} animate={{ opacity: step >= 1 ? 1 : 0.25 }}>
        <path d={`M${MEMT.x + 150} ${MEMT.y + MEMT.h} V${BUCKET_Y + 50}`} stroke="var(--accent-text)" strokeWidth={1.5} strokeOpacity={0.6} className={step >= 1 ? "flow-dash" : ""} />
        <text x={MEMT.x + 162} y={278} fontSize={15} className="fill-muted font-mono">
          flush when full
        </text>
      </motion.g>

      {/* bucket */}
      <rect x={0} y={BUCKET_Y} width={1536} height={300} rx={4} fill="url(#slate-hatch)" stroke="var(--line-strong)" />
      <text x={24} y={BUCKET_Y + 40} fontSize={28} className="fill-ink font-serif">
        s3://acme-telemetry/logs/shard-0
      </text>
      <text x={1512} y={BUCKET_Y + 38} textAnchor="end" fontSize={16} className="fill-muted font-mono">
        the only stateful dependency
      </text>

      {[
        { x: 24, w: 520, t: "wal/" },
        { x: 572, w: 650, t: "compacted/" },
        { x: 1250, w: 262, t: "manifest/" },
      ].map((f) => (
        <g key={f.t}>
          <rect x={f.x} y={BUCKET_Y + 64} width={f.w} height={212} rx={3} fill="var(--bg)" fillOpacity={0.85} stroke="var(--line)" />
          <text x={f.x + 16} y={BUCKET_Y + 94} fontSize={18} className="fill-muted font-mono">
            {f.t}
          </text>
        </g>
      ))}

      {/* WAL files (ticking) */}
      <AnimatePresence initial={false}>
        {wal.map((id, i) => (
          <File
            key={id}
            x={40 + (i % 3) * (FW + 18) + (i >= 3 ? 0 : 0)}
            y={BUCKET_Y + 120 + Math.floor(i / 3) * (FH + 22)}
            label={String(id).padStart(4, "0")}
            color="var(--accent-text)"
            from={{ x: WAL_BUF.x + 120, y: WAL_BUF.y + 40 }}
          />
        ))}
      </AnimatePresence>
      <text x={310} y={BUCKET_Y + 150} fontSize={15} className="fill-muted">
        one object per
      </text>
      <text x={310} y={BUCKET_Y + 172} fontSize={15} className="fill-muted">
        flush interval
      </text>

      {/* L0 + sorted runs */}
      <text x={590} y={BUCKET_Y + 140} fontSize={15} className="fill-muted font-mono">
        L0
      </text>
      <AnimatePresence>
        {step === 1 &&
          [0, 1].map((i) => <File key={`l0-${i}`} x={640 + i * (FW + 16)} y={BUCKET_Y + 112} label={`L0·${i + 1}`} color="var(--ink)" from={{ x: MEMT.x + 120, y: MEMT.y + 40 }} />)}
        {step >= 2 && <File key="l0-3" x={640} y={BUCKET_Y + 112} label="L0·3" color="var(--ink)" from={{ x: MEMT.x + 120, y: MEMT.y + 40 }} />}
      </AnimatePresence>
      <text x={590} y={BUCKET_Y + 212} fontSize={15} className="fill-muted font-mono">
        SR
      </text>
      <AnimatePresence>
        {step >= 2 && <File key="sr" x={640} y={BUCKET_Y + 186} w={540} label="sorted run 0 · non-overlapping SSTs" color="var(--ink)" from={{ x: 980, y: 60 }} />}
      </AnimatePresence>

      {/* manifest */}
      <AnimatePresence mode="popLayout">
        <motion.g key={manifest} initial={{ opacity: 0, scale: 1.25 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0 }} style={{ transformOrigin: "1380px 470px" }}>
          <rect x={1272} y={BUCKET_Y + 116} width={218} height={60} rx={3} fill="var(--panel)" stroke="var(--violet)" strokeWidth={1.5} />
          <text x={1381} y={BUCKET_Y + 144} textAnchor="middle" fontSize={17} className="fill-ink font-mono">
            {String(manifest).padStart(8, "0")}.manifest
          </text>
          <text x={1381} y={BUCKET_Y + 166} textAnchor="middle" fontSize={14} className="fill-muted font-mono">
            writer_epoch · L0 · SRs
          </text>
        </motion.g>
      </AnimatePresence>
      <text x={1272} y={BUCKET_Y + 220} fontSize={15} className="fill-muted">
        compare-and-swap
      </text>
      <text x={1272} y={BUCKET_Y + 242} fontSize={15} className="fill-muted">
        on every update
      </text>
    </svg>
  );
}
