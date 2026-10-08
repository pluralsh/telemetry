"use client";

import { useEffect, useReducer, useRef, useState } from "react";
import { AnimatePresence, motion } from "motion/react";
import { Figure, Segmented } from "./figure";

const X0 = 64;
const X1 = 744;
const Y0 = 34;
const Y1 = 266;
const WINDOW = 240; // sim minutes shown (11:00 → 15:00)
const LEAD = 2;
const QUERY_SPAN = 90;
const TICK_MS = 80;
const SHARD_COLORS = ["var(--metrics)", "var(--logs)", "var(--traces)", "#c4508a", "#3d8fc4", "#8a6ad8", "#b8962a", "#4f9e5a"];

type Epoch = { gen: number; from: number; shards: number };
type Rec = { id: number; t: number; h: number; epoch: number; shard: number; late?: boolean; key: string; via: number };
type State = { now: number; tick: number; epochs: Epoch[]; recs: Rec[]; traced: Rec | null; nextId: number; playing: boolean; writers: number };

const KEYS = [
  'ns=default {app="checkout", pod="checkout-7d9"}',
  'ns=default {app="gateway", pod="gw-2xk"}',
  'ns=payments {app="ledger", pod="ledger-0"}',
  'ns=default {app="search", pod="search-5f1"}',
  'ns=infra {job="node-exporter", instance="10.0.4.12"}',
];

const x = (t: number) => X0 + (t / WINDOW) * (X1 - X0);
const y = (h: number) => Y0 + h * (Y1 - Y0);
const clock = (t: number) => {
  const m = Math.floor(660 + t);
  return `${String(Math.floor(m / 60)).padStart(2, "0")}:${String(m % 60).padStart(2, "0")}`;
};
const hex = (h: number) => "0x" + Math.floor(h * 0xffffffff).toString(16).padStart(8, "0");

function epochAt(epochs: Epoch[], t: number) {
  let e = epochs[0];
  for (const ep of epochs) if (ep.from <= t) e = ep;
  return e;
}

function makeRec(s: State, t: number, late = false): Rec {
  const h = Math.random();
  const e = epochAt(s.epochs, t);
  return {
    id: s.nextId,
    t,
    h,
    epoch: e.gen,
    shard: Math.min(e.shards - 1, Math.floor(h * e.shards)),
    late,
    key: KEYS[Math.floor(Math.random() * KEYS.length)],
    via: Math.floor(Math.random() * s.writers),
  };
}

const INITIAL: State = { now: 18, tick: 0, epochs: [{ gen: 0, from: 0, shards: 2 }], recs: [], traced: null, nextId: 0, playing: true, writers: 2 };

type Action = { type: "tick" } | { type: "scale" } | { type: "late" } | { type: "toggle" } | { type: "reset" };

function reducer(s: State, a: Action): State {
  switch (a.type) {
    case "tick": {
      if (!s.playing) return s;
      const now = s.now + TICK_MS / 100;
      if (now >= WINDOW) return { ...INITIAL, playing: true };
      const tick = s.tick + 1;
      let next: State = { ...s, now, tick };
      if (tick % 4 === 0) {
        const r = makeRec(next, now);
        next = { ...next, recs: [...next.recs.slice(-420), r], nextId: next.nextId + 1 };
        if (tick % 28 === 0) next.traced = r;
      }
      // writers follow the newest effective epoch
      const eff = epochAt(next.epochs, now);
      if (eff.shards !== next.writers && next.epochs[next.epochs.length - 1].from <= now) next.writers = eff.shards;
      return next;
    }
    case "scale": {
      const last = s.epochs[s.epochs.length - 1];
      const pending = last.from > s.now;
      const target = Math.min(8, last.shards + 2);
      if (!pending && target === last.shards) return s;
      const from = Math.ceil((s.now + LEAD) / 60) * 60;
      if (from >= WINDOW) return s;
      const epochs = pending
        ? [...s.epochs.slice(0, -1), { ...last, shards: target }]
        : [...s.epochs, { gen: last.gen + 1, from, shards: target }];
      return { ...s, epochs };
    }
    case "late": {
      const t = Math.max(1, s.now - 25 - Math.random() * 30);
      const r = { ...makeRec(s, t, true) };
      return { ...s, recs: [...s.recs, r], traced: r, nextId: s.nextId + 1 };
    }
    case "toggle":
      return { ...s, playing: !s.playing };
    case "reset":
      return { ...INITIAL };
  }
}

export function EpochSharding() {
  const [s, dispatch] = useReducer(reducer, INITIAL);
  const [mode, setMode] = useState<"write" | "read">("write");
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = ref.current;
    let visible = true;
    const obs = new IntersectionObserver(([e]) => (visible = e.isIntersecting));
    if (el) obs.observe(el);
    const id = setInterval(() => visible && dispatch({ type: "tick" }), TICK_MS);
    return () => {
      clearInterval(id);
      obs.disconnect();
    };
  }, []);

  const last = s.epochs[s.epochs.length - 1];
  const pending = last.from > s.now ? last : null;
  const tr = s.traced;
  const trEpoch = tr ? s.epochs.find((e) => e.gen === tr.epoch)! : null;
  const ownerShards = Math.max(...s.epochs.map((e) => e.shards));

  const reading = mode === "read";
  const qFrom = Math.max(0, s.now - QUERY_SPAN);
  const qTo = s.now;
  const inRange = (r: Rec) => r.t >= qFrom && r.t <= qTo;
  const openShards = Math.max(...s.epochs.filter((e) => e.from <= s.now).map((e) => e.shards));
  const hits = Array.from({ length: openShards }, (_, k) => s.recs.filter((r) => r.shard === k && inRange(r)).length);
  const totalHits = hits.reduce((a, b) => a + b, 0);
  const rangeEpochs = s.epochs.filter((e, i) => e.from <= qTo && (s.epochs[i + 1]?.from ?? WINDOW) > qFrom && e.from <= s.now);
  const newestInRange = rangeEpochs[rangeEpochs.length - 1];
  const mx = Math.min(x(qTo) + 44, 752);
  const mergeLabelRight = mx < X1 - 110;
  const readSteps: [string, string][] = [
    ["query", `{app="checkout"} |= "error" · ns=default · ${clock(qFrom)}–${clock(qTo)}`],
    ["fan out", `open shards [0, ${openShards}) read-only from object storage; no Lease or writer involved`],
    [
      "epochs",
      rangeEpochs.length > 1
        ? `range spans ${rangeEpochs.map((e) => `e${e.gen}`).join(", ")}; shards ${rangeEpochs[0].shards}–${newestInRange.shards - 1} only hold data after ${clock(newestInRange.from)}`
        : `range is inside e${rangeEpochs[0]?.gen ?? 0}; every open shard may hold matches`,
    ],
    ["scan", `each shard reads only blocks overlapping the range · ${totalHits} matching records`],
    ["cache", "all shards share one SlateDB block + meta cache; misses fetch from object storage"],
    ["merge", "streams with equal labels merge by timestamp (Metrics dedupes by fingerprint, Traces joins partial traces)"],
  ];

  return (
    <div ref={ref}>
    <Figure
      label="Fig. — Epoch-based sharding, live"
      controls={
        <>
          <Segmented
            size="sm"
            value={mode}
            onChange={setMode}
            options={[
              { value: "write", label: "Write path" },
              { value: "read", label: "Read path" },
            ]}
          />
          <span className="kbd mr-1 tabular-nums">now {clock(s.now)}</span>
          <button type="button" onClick={() => dispatch({ type: "toggle" })} className="btn btn-secondary btn-sm">
            {s.playing ? "Pause" : "Play"}
          </button>
          {!reading && (
            <button type="button" onClick={() => dispatch({ type: "late" })} className="btn btn-secondary btn-sm">
              Send late record
            </button>
          )}
          <button
            type="button"
            onClick={() => dispatch({ type: "scale" })}
            disabled={last.shards >= 8}
            className="btn btn-primary btn-sm disabled:opacity-40"
          >
            Scale writers {(pending ?? last).shards} → {Math.min(8, (pending ?? last).shards + 2)}
          </button>
          <button type="button" onClick={() => dispatch({ type: "reset" })} className="btn btn-ghost btn-sm text-muted">
            Reset
          </button>
        </>
      }
      caption={
        <>
          {reading ? (
            <>
              Readers never consult routing to find data. They open every shard that has ever existed, scan each one for the query&apos;s time range, and
              merge the results. Because epochs only add shards, a shard created by a scale-up simply has nothing to return for hours before its cutover.
            </>
          ) : (
            <>
              Scaling up schedules a new epoch at the next aligned hour that is at least {LEAD} minutes away, so every writer sees it before any record
              routes by it. Repeated requests before the cutover replace the pending epoch. Old data never moves, and late records route by their own
              timestamp.
            </>
          )}
        </>
      }
    >
      <div>
        <div className="border-b border-line bg-bg p-3">
          <svg viewBox="0 0 760 312" className="h-auto w-full">
            {/* epoch bands */}
            {s.epochs.map((e, i) => {
              const end = s.epochs[i + 1]?.from ?? WINDOW;
              const isPending = e.from > s.now;
              return (
                <g key={e.gen}>
                  {Array.from({ length: e.shards }, (_, k) => (
                    <rect
                      key={k}
                      x={x(e.from)}
                      y={y(k / e.shards)}
                      width={x(end) - x(e.from)}
                      height={(Y1 - Y0) / e.shards}
                      fill={SHARD_COLORS[k]}
                      fillOpacity={isPending ? 0.04 : 0.09}
                      stroke="var(--bg)"
                      strokeWidth={1}
                    />
                  ))}
                  {Array.from({ length: e.shards }, (_, k) => (
                    <text key={`l${k}`} x={x(e.from) + 6} y={y(k / e.shards) + 14} fontSize={10} className="fill-muted font-mono">
                      shard {k}
                      {e.gen > 0 && k >= s.epochs[i - 1].shards ? " · new" : ""}
                    </text>
                  ))}
                  <line x1={x(e.from)} x2={x(e.from)} y1={Y0 - 14} y2={Y1 + 4} stroke={isPending ? "var(--accent-text)" : "var(--ink)"} strokeOpacity={isPending ? 1 : 0.5} strokeDasharray={isPending ? "4 3" : undefined} />
                  <text x={x(e.from) + 4} y={Y0 - 6} fontSize={10.5} className="font-mono" fill={isPending ? "var(--accent-text)" : "var(--muted)"}>
                    epoch {e.gen} · {e.shards} shards{e.gen > 0 ? ` · effective ${clock(e.from)}` : ""}
                    {isPending ? " (pending)" : ""}
                  </text>
                </g>
              );
            })}

            {/* future veil */}
            <rect x={x(s.now)} y={Y0} width={Math.max(0, X1 - x(s.now))} height={Y1 - Y0} fill="var(--bg)" fillOpacity={0.6} />

            {/* records */}
            {s.recs.map((r) => (
              <rect
                key={r.id}
                x={x(r.t) - (r.late ? 3.5 : 2.25)}
                y={y(r.h) - (r.late ? 3.5 : 2.25)}
                width={r.late ? 7 : 4.5}
                height={r.late ? 7 : 4.5}
                fill={SHARD_COLORS[r.shard]}
                fillOpacity={reading && !inRange(r) ? 0.25 : 1}
                stroke={r.late ? "var(--ink)" : "none"}
                strokeWidth={1}
              />
            ))}

            {/* query range: one scan per open shard, fanned in to the reader */}
            {reading && (
              <g>
                <rect x={x(qFrom)} y={Y0} width={x(qTo) - x(qFrom)} height={Y1 - Y0} fill="var(--accent-text)" fillOpacity={0.06} stroke="var(--accent-text)" strokeDasharray="4 3" />
                <text x={x(qFrom) + 4} y={Y1 - 6} fontSize={10.5} className="font-mono" fill="var(--accent-text)">
                  query {clock(qFrom)}–{clock(qTo)}
                </text>
                {Array.from({ length: openShards }, (_, k) => {
                  const top = y(k / openShards);
                  const h = (Y1 - Y0) / openShards;
                  const born = s.epochs.find((e) => e.shards > k)!.from;
                  const start = x(Math.max(qFrom, born));
                  return (
                    <g key={k}>
                      <motion.line
                        y1={top + 2}
                        y2={top + h - 2}
                        stroke={SHARD_COLORS[k]}
                        strokeWidth={2}
                        initial={{ x1: start, x2: start }}
                        animate={{ x1: [start, x(qTo)], x2: [start, x(qTo)] }}
                        transition={{ duration: 1.6, delay: k * 0.12, repeat: Infinity, repeatDelay: 0.6, ease: "easeInOut" }}
                      />
                      <path d={`M${x(qTo)} ${top + h / 2} C${mx - 24} ${top + h / 2}, ${mx - 24} ${(Y0 + Y1) / 2}, ${mx - 6} ${(Y0 + Y1) / 2}`} fill="none" stroke={SHARD_COLORS[k]} strokeOpacity={0.7} />
                    </g>
                  );
                })}
                <rect x={mergeLabelRight ? mx + 6 : mx - 100} y={(Y0 + Y1) / 2 - (mergeLabelRight ? 8 : 24)} width={100} height={16} fill="var(--card)" />
                <rect x={mx - 6} y={(Y0 + Y1) / 2 - 6} width={12} height={12} fill="var(--ink)" />
                <text
                  x={mergeLabelRight ? mx + 10 : mx + 6}
                  y={mergeLabelRight ? (Y0 + Y1) / 2 + 3.5 : (Y0 + Y1) / 2 - 12}
                  textAnchor={mergeLabelRight ? "start" : "end"}
                  fontSize={10.5}
                  className="fill-ink font-mono"
                >
                  merge → reader-0
                </text>
              </g>
            )}

            {!reading && tr && (
              <motion.rect key={tr.id} x={x(tr.t) - 7} y={y(tr.h) - 7} width={14} height={14} fill="none" stroke="var(--ink)" strokeWidth={1.25} initial={{ scale: 2.2, opacity: 0, rotate: 0 }} animate={{ scale: 1, opacity: 1, rotate: 45 }} transition={{ duration: 0.5 }} style={{ transformBox: "fill-box", transformOrigin: "center" }} />
            )}

            {/* now cursor */}
            <line x1={x(s.now)} x2={x(s.now)} y1={Y0} y2={Y1} stroke="var(--accent-text)" strokeWidth={1.5} />
            <text x={x(s.now)} y={Y1 + 18} textAnchor="middle" fontSize={10.5} className="font-mono" fill="var(--accent-text)">
              now
            </text>

            {/* axes */}
            {[0, 60, 120, 180, 240].map((t) => (
              <g key={t}>
                <line x1={x(t)} x2={x(t)} y1={Y1} y2={Y1 + 5} stroke="var(--line-strong)" />
                <text x={x(t)} y={Y1 + 32} textAnchor="middle" fontSize={10.5} className="fill-faint font-mono">
                  {clock(t)}
                </text>
              </g>
            ))}
            {["0x0…", "0x4…", "0x8…", "0xc…", "0xf…"].map((l, i) => (
              <text key={l} x={X0 - 8} y={Y0 + (i / 4) * (Y1 - Y0) + 4} textAnchor="end" fontSize={10} className="fill-faint font-mono">
                {l}
              </text>
            ))}
            <text x={14} y={(Y0 + Y1) / 2} fontSize={10} className="fill-faint font-mono" transform={`rotate(-90 14 ${(Y0 + Y1) / 2})`} textAnchor="middle">
              BLAKE3-128 hash space
            </text>
            <text x={X1} y={Y1 + 18} textAnchor="end" fontSize={10} className="fill-faint font-mono">
              record time →
            </text>
          </svg>
        </div>

        <div className="grid md:grid-cols-[1fr_240px]">
        <div className="flex min-h-[300px] flex-col border-b border-line p-4 md:border-b-0 md:border-r">
          <div className="mb-3 font-serif text-[14px] text-ink">
            {reading ? "Read flow" : "Write flow"} <span className="eyebrow">· {reading ? "range query on reader-0" : "traced record"}</span>
          </div>
          <AnimatePresence mode="wait">
            {reading ? (
              <motion.ol key="read" className="flex flex-col gap-2.5" initial="hidden" animate="show" exit={{ opacity: 0 }} variants={{ show: { transition: { staggerChildren: 0.18 } } }}>
                {readSteps.map(([k, v], i) => (
                  <motion.li key={k} variants={{ hidden: { opacity: 0, x: -6 }, show: { opacity: 1, x: 0 } }} className="grid grid-cols-[18px_1fr] gap-2 text-[12px]">
                    <span className="mt-px flex h-[18px] w-[18px] items-center justify-center rounded-[2px] border border-line-strong bg-panel font-mono text-[9.5px] text-muted">{i + 1}</span>
                    <span>
                      <span className="block text-[11.5px] italic text-muted">{k}</span>
                      <span className="block break-words font-mono text-[11.5px] text-ink">{v}</span>
                    </span>
                  </motion.li>
                ))}
              </motion.ol>
            ) : tr && trEpoch ? (
              <motion.ol key={tr.id} className="flex flex-col gap-2.5" initial="hidden" animate="show" exit={{ opacity: 0 }} variants={{ show: { transition: { staggerChildren: 0.22 } } }}>
                {[
                  ["routing key", tr.key],
                  ["blake3_128", `${hex(tr.h)}…`],
                  ["epoch", `e${trEpoch.gen}: last effective_from ≤ ${clock(tr.t)}${tr.late ? " (late record)" : ""}`],
                  ["shard", `shard ${tr.shard} · [${hex(tr.shard / trEpoch.shards).slice(0, 5)}…, ${hex(Math.min(0.99999, (tr.shard + 1) / trEpoch.shards)).slice(0, 5)}…)`],
                  ["owner", `Lease → writer-${tr.shard}${tr.via === tr.shard ? " (local)" : ` · forwarded from writer-${tr.via} over gRPC`}`],
                  ["put", `SlateDB shard-${String(tr.shard).padStart(4, "0")}/ → WAL → object storage`],
                ].map(([k, v], i) => (
                  <motion.li
                    key={k}
                    variants={{ hidden: { opacity: 0, x: -6 }, show: { opacity: 1, x: 0 } }}
                    className="grid grid-cols-[18px_1fr] gap-2 text-[12px]"
                  >
                    <span className="mt-px flex h-[18px] w-[18px] items-center justify-center rounded-[2px] border border-line-strong bg-panel font-mono text-[9.5px] text-muted">{i + 1}</span>
                    <span>
                      <span className="block text-[11.5px] italic text-muted">{k}</span>
                      <span className={`block break-words font-mono text-[11.5px] ${i === 3 ? "" : "text-ink"}`} style={i === 3 ? { color: SHARD_COLORS[tr.shard] } : undefined}>
                        {v}
                      </span>
                    </span>
                  </motion.li>
                ))}
              </motion.ol>
            ) : (
              <p className="text-[12px] text-muted">Waiting for the first traced record…</p>
            )}
          </AnimatePresence>

        </div>
        <div className="p-4">
          <div className="mb-3 font-serif text-[14px] text-ink">{reading ? "Shards scanned" : "Shard ownership"}</div>
          <div className="flex flex-col gap-1">
            {reading
              ? hits.map((n, k) => (
                  <div key={k} className="flex items-center gap-2 whitespace-nowrap py-0.5 font-mono text-[11.5px]">
                    <span className="h-2 w-2 shrink-0" style={{ background: SHARD_COLORS[k] }} />
                    <span className="text-ink">shard {k}</span>
                    <span className="leader" />
                    <span className="tabular-nums text-muted">{n} hits</span>
                  </div>
                ))
              : Array.from({ length: ownerShards }, (_, k) => {
              const live = k < s.writers;
              return (
                <div key={k} className="flex items-center gap-2 whitespace-nowrap py-0.5 font-mono text-[11.5px]" style={{ opacity: live ? 1 : 0.45 }}>
                  <span className="h-2 w-2 shrink-0" style={{ background: SHARD_COLORS[k] }} />
                  <span className="text-ink">shard {k}</span>
                  <span className="leader" />
                  <span className="text-muted">{live ? `writer-${k}` : "waiting"}</span>
                </div>
              );
            })}
          </div>
        </div>
        </div>
      </div>
    </Figure>
    </div>
  );
}
