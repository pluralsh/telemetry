import { AnimatePresence, motion } from "motion/react";
import { useEffect, useState } from "react";
import { useStep } from "../deck/context";
import { EASE, HatchDef } from "../ui/primitives";

const BY = 330;
const FW = 72;
const FH = 46;
const WAL_X = 640;
const SHOW = 9;

function useTicker(live: boolean, ms: number, start: number) {
  const [n, setN] = useState(start);
  useEffect(() => {
    if (!live) return;
    const t = setInterval(() => setN((v) => v + 1), ms);
    return () => clearInterval(t);
  }, [live, ms]);
  return n;
}

function Box({ x, y, w, h, title, sub, color, danger, children }: { x: number; y: number; w: number; h: number; title: string; sub?: string; color: string; danger?: boolean; children?: React.ReactNode }) {
  return (
    <g>
      <rect x={x} y={y} width={w} height={h} rx={4} fill="var(--panel)" stroke={danger ? "var(--danger)" : color} strokeWidth={danger ? 2.5 : 1.5} style={{ transition: "stroke .3s" }} />
      <text x={x + 18} y={y + 36} fontSize={26} className="fill-ink font-serif">
        {title}
      </text>
      {sub && (
        <text x={x + w - 16} y={y + 34} textAnchor="end" fontSize={15} className="fill-muted font-mono">
          {sub}
        </text>
      )}
      {children}
    </g>
  );
}

export const REPLAY_CAPTIONS = [
  "One writer per database keeps appending WAL objects. Readers never talk to it.",
  "Each reader polls the manifest, which lists the flushed SSTs and the last WAL already covered by them.",
  "The reader then replays every newer WAL object into its own in-memory memtable. Fresh writes are queryable in about a second, with no RPC to the writer.",
  "On failover, a new writer bumps writer_epoch in the manifest with a compare-and-swap. The old writer's next conditional PUT fails, so a zombie can never corrupt the shard.",
];

export function ReaderReplay() {
  const { step, live } = useStep();
  const n = useTicker(live, 1200, 41);
  const files = Array.from({ length: SHOW }, (_, i) => n - SHOW + 1 + i);
  const covered = n - 4;
  const fenced = step >= 3;

  return (
    <svg viewBox="0 0 1536 620" className="block h-[560px] w-full overflow-visible">
      <defs>
        <HatchDef id="rr-hatch" />
      </defs>

      {/* writers */}
      <Box x={0} y={0} w={400} h={200} title="writer A" sub={fenced ? "epoch 7 · fenced" : "epoch 7"} color="var(--ink)" danger={fenced}>
        <text x={18} y={80} fontSize={17} className="fill-muted">
          owns the shard's Lease
        </text>
        <text x={18} y={106} fontSize={17} className="fill-muted">
          appends WAL every 100 ms
        </text>
      </Box>
      <AnimatePresence>
        {fenced && (
          <motion.g initial={{ opacity: 0, x: -20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0 }} transition={{ duration: 0.5 }}>
            <Box x={430} y={0} w={330} h={200} title="writer B" sub="epoch 8" color="var(--ok)">
              <text x={448} y={80} fontSize={17} className="fill-muted">
                took over the Lease
              </text>
              <text x={448} y={106} fontSize={17} className="fill-muted">
                reopened from S3
              </text>
            </Box>
            <path d={`M595 200 C 595 260, 120 260, 120 ${BY + 90}`} fill="none" stroke="var(--ok)" strokeWidth={2} className="flow-dash" />
            <text x={360} y={262} fontSize={16} className="font-mono" fill="var(--ok)">
              CAS manifest: epoch 7 → 8
            </text>
          </motion.g>
        )}
      </AnimatePresence>

      {/* writer A → WAL */}
      <path d={`M300 200 C 300 270, ${WAL_X + 300} 250, ${WAL_X + 300} ${BY + 94}`} fill="none" stroke={fenced ? "var(--danger)" : "var(--accent-text)"} strokeWidth={1.5} strokeOpacity={0.7} className="flow-dash" />
      <AnimatePresence>
        {fenced && (
          <motion.g initial={{ opacity: 0, scale: 0.6 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0 }} transition={{ delay: 0.7, type: "spring", stiffness: 300, damping: 18 }} style={{ transformOrigin: "760px 266px" }}>
            <rect x={640} y={246} width={280} height={40} rx={3} fill="var(--danger)" />
            <text x={780} y={273} textAnchor="middle" fontSize={17} className="font-mono" fill="#fff">
              ✗ 412 precondition failed
            </text>
          </motion.g>
        )}
      </AnimatePresence>

      {/* readers */}
      {[0, 1].map((r) => {
        const x = 900 + r * 324;
        return (
          <Box key={r} x={x} y={0} w={300} h={200} title={`reader ${r + 1}`} sub="read-only" color="var(--logs)">
            <rect x={x + 16} y={60} width={268} height={120} rx={3} fill="color-mix(in srgb, var(--logs) 6%, var(--bg))" stroke="var(--logs)" strokeOpacity={0.6} />
            <text x={x + 30} y={86} fontSize={15} className="fill-muted font-mono">
              replayed memtable
            </text>
            <AnimatePresence>
              {step >= 2 &&
                files
                  .filter((id) => id > covered)
                  .map((id, i) => (
                    <motion.g
                      key={id}
                      initial={{ opacity: 0, y: 60 }}
                      animate={{ opacity: 1, y: 0 }}
                      exit={{ opacity: 0 }}
                      transition={{ duration: 0.6, ease: EASE, delay: r * 0.15 }}
                    >
                      <rect x={x + 30 + i * 62} y={104} width={54} height={56} rx={3} fill="var(--panel)" stroke="var(--accent-text)" />
                      <text x={x + 57 + i * 62} y={138} textAnchor="middle" fontSize={14} className="fill-ink font-mono">
                        {id}
                      </text>
                    </motion.g>
                  ))}
            </AnimatePresence>
          </Box>
        );
      })}

      {/* manifest polling */}
      <AnimatePresence>
        {step >= 1 && (
          <motion.g initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
            {[0, 1].map((r) => {
              const d = `M${1050 + r * 324} 200 C ${1050 + r * 324} 280, 200 250, 200 ${BY + 90}`;
              return (
                <g key={r}>
                  <path d={d} fill="none" stroke="var(--violet)" strokeOpacity={0.5} strokeWidth={1.5} strokeDasharray="3 6" />
                  <circle r={5} fill="var(--violet)" visibility="hidden">
                    <set attributeName="visibility" to="visible" begin={`${r * 0.5}s`} />
                    <animateMotion dur="1.6s" begin={`${r * 0.5}s`} repeatCount="indefinite" path={d} keyPoints="1;0" keyTimes="0;1" calcMode="linear" />
                  </circle>
                </g>
              );
            })}
            <text x={1060} y={244} fontSize={16} className="font-mono" fill="var(--violet)">
              poll manifest
            </text>
          </motion.g>
        )}
      </AnimatePresence>

      {/* bucket */}
      <rect x={0} y={BY} width={1536} height={280} rx={4} fill="url(#rr-hatch)" stroke="var(--line-strong)" />
      <text x={24} y={BY + 40} fontSize={26} className="fill-ink font-serif">
        object storage
      </text>

      {/* manifest */}
      <rect x={24} y={BY + 64} width={360} height={190} rx={3} fill="var(--panel)" stroke="var(--violet)" strokeWidth={1.5} />
      <text x={44} y={BY + 98} fontSize={18} className="fill-ink font-mono">
        manifest
      </text>
      {[
        ["writer_epoch", fenced ? "8" : "7"],
        ["l0", "[sst 12, sst 13]"],
        ["sorted_runs", "[sr 0, sr 1]"],
        ["replay_after_wal", String(covered)],
      ].map(([k, v], i) => (
        <g key={k}>
          <text x={44} y={BY + 132 + i * 30} fontSize={16} className="fill-muted font-mono">
            {k}
          </text>
          <text x={364} y={BY + 132 + i * 30} textAnchor="end" fontSize={16} className="font-mono" fill={(k === "writer_epoch" && fenced) || k === "replay_after_wal" ? "var(--violet)" : "var(--ink)"}>
            {v}
          </text>
        </g>
      ))}

      {/* compacted */}
      <rect x={410} y={BY + 64} width={200} height={190} rx={3} fill="var(--bg)" fillOpacity={0.85} stroke="var(--line)" />
      <text x={426} y={BY + 98} fontSize={18} className="fill-muted font-mono">
        compacted/
      </text>
      {["sst 12", "sst 13", "sr 0", "sr 1"].map((s, i) => (
        <g key={s}>
          <rect x={426 + (i % 2) * 88} y={BY + 118 + Math.floor(i / 2) * 62} width={78} height={FH} rx={3} fill="var(--panel)" stroke="var(--ink)" strokeOpacity={0.6} />
          <text x={465 + (i % 2) * 88} y={BY + 147 + Math.floor(i / 2) * 62} textAnchor="middle" fontSize={14} className="fill-ink font-mono">
            {s}
          </text>
        </g>
      ))}

      {/* wal */}
      <rect x={WAL_X} y={BY + 64} width={872} height={190} rx={3} fill="var(--bg)" fillOpacity={0.85} stroke="var(--line)" />
      <text x={WAL_X + 16} y={BY + 98} fontSize={18} className="fill-muted font-mono">
        wal/
      </text>
      <AnimatePresence initial={false}>
        {files.map((id, i) => {
          const fresh = id > covered;
          const hl = step >= 2 && fresh;
          return (
            <motion.g
              key={id}
              initial={{ opacity: 0, x: WAL_X + 16 + SHOW * (FW + 22), y: BY + 140 }}
              animate={{ opacity: fresh || step < 1 ? 1 : 0.45, x: WAL_X + 16 + i * (FW + 22), y: BY + 140 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.7, ease: EASE }}
            >
              <rect width={FW} height={FH} rx={3} fill={hl ? "color-mix(in srgb, var(--accent-text) 14%, var(--panel))" : "var(--panel)"} stroke="var(--accent-text)" strokeWidth={hl ? 2 : 1.2} />
              <text x={FW / 2} y={29} textAnchor="middle" fontSize={15} className="fill-ink font-mono">
                {String(id).padStart(4, "0")}
              </text>
            </motion.g>
          );
        })}
      </AnimatePresence>
      <AnimatePresence>
        {step >= 1 && (
          <motion.g initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
            <line x1={WAL_X + 16 + (SHOW - 4) * (FW + 22) - 11} x2={WAL_X + 16 + (SHOW - 4) * (FW + 22) - 11} y1={BY + 116} y2={BY + 220} stroke="var(--violet)" strokeWidth={2} strokeDasharray="4 4" />
            <text x={WAL_X + 16 + (SHOW - 4) * (FW + 22) - 20} y={BY + 236} textAnchor="end" fontSize={15} className="font-mono" fill="var(--muted)">
              covered by SSTs
            </text>
            <text x={WAL_X + 16 + (SHOW - 4) * (FW + 22)} y={BY + 236} fontSize={15} className="font-mono" fill="var(--violet)">
              replay →
            </text>
          </motion.g>
        )}
      </AnimatePresence>
    </svg>
  );
}
