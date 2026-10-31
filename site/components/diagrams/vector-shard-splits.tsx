"use client";

import { useEffect, useReducer, useRef } from "react";
import { AnimatePresence, motion } from "motion/react";
import { Figure } from "./figure";

const X0 = 64;
const X1 = 744;
const Y0 = 44;
const Y1 = 264;
const T = 100;
const TICK_MS = 80;
const JOINS = [12, 42, 72];
const SPLIT = 14;
// Offsets into a split, in sim time: plan, cut, checkpoint, clone, topology commit, reader groups.
const PHASES = [0, 2.4, 4.8, 7.2, 9.6, 12];
const COMMIT = PHASES[4];
const WRITER_COLORS = ["var(--metrics)", "var(--logs)", "var(--traces)", "#c4508a"];
const NAMESPACES = [0.04, 0.11, 0.17, 0.26, 0.31, 0.38, 0.45, 0.49, 0.57, 0.63, 0.68, 0.74, 0.8, 0.86, 0.91, 0.97];

type Shard = { id: number; lo: number; hi: number; owner: number; born: number; retired?: number };
type Split = { writer: number; at: number; parent: Shard; cut: number; median: boolean; lower: Shard; upper: Shard; nLo: number; nHi: number; gen: number };

const { shards: SHARDS, splits: SPLITS } = (() => {
  const shards: Shard[] = [{ id: 0, lo: 0, hi: 1, owner: 0, born: 0 }];
  const splits: Split[] = [];
  JOINS.forEach((at, i) => {
    const active = shards.filter((s) => s.retired === undefined);
    const parent = active.reduce((a, b) => (b.hi - b.lo > a.hi - a.lo ? b : a));
    const ids = NAMESPACES.filter((h) => h >= parent.lo && h < parent.hi);
    const median = ids.length >= 2;
    const cut = median ? ids[Math.floor(ids.length / 2)] : (parent.lo + parent.hi) / 2;
    const born = at + COMMIT;
    parent.retired = born;
    const lower = { id: shards.length, lo: parent.lo, hi: cut, owner: parent.owner, born };
    const upper = { id: shards.length + 1, lo: cut, hi: parent.hi, owner: i + 1, born };
    shards.push(lower, upper);
    splits.push({ writer: i + 1, at, parent, cut, median, lower, upper, nLo: ids.filter((h) => h < cut).length, nHi: ids.filter((h) => h >= cut).length, gen: i + 1 });
  });
  return { shards, splits };
})();

const x = (t: number) => X0 + (Math.min(t, T) / T) * (X1 - X0);
const y = (h: number) => Y0 + h * (Y1 - Y0);
const hex = (h: number) => "0x" + Math.min(0xffff, Math.floor(h * 0x10000)).toString(16).padStart(4, "0");
const range = (s: Shard) => `${hex(s.lo)}–${s.hi >= 1 ? "0xffff" : hex(s.hi - 1 / 0x10000)}`;
const nsIn = (s: Shard) => NAMESPACES.filter((h) => h >= s.lo && h < s.hi).length;

function splitAt(now: number) {
  return SPLITS.find((s) => now >= s.at && now < s.at + SPLIT) ?? null;
}

type State = { now: number; playing: boolean };
type Action = { type: "tick" } | { type: "toggle" } | { type: "reset" };

function reducer(s: State, a: Action): State {
  switch (a.type) {
    case "tick": {
      if (!s.playing) return s;
      const now = s.now + (splitAt(s.now) ? 0.09 : 0.25);
      return now >= T + 12 ? { ...s, now: 0 } : { ...s, now };
    }
    case "toggle":
      return { ...s, playing: !s.playing };
    case "reset":
      return { now: 0, playing: true };
  }
}

function steps(sp: Split): [string, string][] {
  const { parent: p, lower, upper } = sp;
  return [
    ["plan", `writer-${sp.writer} is ready; the coordinator marks shard ${p.id}, the widest (${range(p)}), to split for it`],
    ["cut", sp.median ? `writer-${p.owner} cuts at the median namespace id ${hex(sp.cut)}: ${sp.nLo} | ${sp.nHi} namespaces` : `fewer than two namespaces: writer-${p.owner} cuts at the midpoint ${hex(sp.cut)}`],
    ["checkpoint", `requests for shard ${p.id}'s namespaces pause; it flushes and takes a SlateDB checkpoint`],
    ["clone", `shards ${lower.id} and ${upper.id} are clones of the checkpoint, each projected to its half: no data copied`],
    ["commit", `one topology CAS, generation ${sp.gen - 1} → ${sp.gen}, retires shard ${p.id}: writer-${p.owner} keeps ${lower.id}, writer-${sp.writer} takes ${upper.id}`],
    ["readers", `the operator starts reader groups for shards ${lower.id} and ${upper.id}, then removes shard ${p.id}'s`],
  ];
}

export function VectorShardSplits() {
  const [s, dispatch] = useReducer(reducer, { now: 0, playing: true });
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

  const now = Math.min(s.now, T);
  const active = splitAt(now);
  const phase = active ? PHASES.filter((p) => now - active.at >= p).length - 1 : -1;
  const shown = active ?? [...SPLITS].reverse().find((sp) => now >= sp.at + SPLIT) ?? null;
  const done = active ? phase : 5;
  const paused = active && now - active.at >= PHASES[2] && now - active.at < COMMIT ? active.parent : null;
  const live = SHARDS.filter((sh) => sh.born <= now && (sh.retired === undefined || sh.retired > now)).sort((a, b) => a.lo - b.lo);
  const writers = 1 + SPLITS.filter((sp) => now >= sp.at).length;
  const gen = SPLITS.filter((sp) => now >= sp.at + COMMIT).length;
  const ownerOf = (h: number) => live.find((sh) => h >= sh.lo && h < sh.hi)!.owner;

  return (
    <div ref={ref}>
      <Figure
        label="Fig. — Shard splits over time"
        controls={
          <>
            <span className="kbd mr-1 tabular-nums">
              {writers} writer{writers > 1 ? "s" : ""} · {live.length} shard{live.length > 1 ? "s" : ""}
            </span>
            <button type="button" onClick={() => dispatch({ type: "toggle" })} className="btn btn-secondary btn-sm">
              {s.playing ? "Pause" : "Play"}
            </button>
            <button type="button" onClick={() => dispatch({ type: "reset" })} className="btn btn-ghost btn-sm text-muted">
              Reset
            </button>
          </>
        }
        caption={
          <>
            Each row is a namespace, fixed by its id. Bands are shards, colored by the writer that owns them. When a writer joins, the widest shard
            splits at its median namespace: the owner keeps the lower half and the new writer takes the upper. Shards only ever split, so the
            topology grows like a tree and writers can&apos;t be removed.
          </>
        }
      >
        <div>
          <div className="border-b border-line bg-bg p-3">
            <svg viewBox="0 0 760 300" className="h-auto w-full">
              <defs>
                <pattern id="vss-paused" width={6} height={6} patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
                  <line x1={0} x2={0} y1={0} y2={6} stroke="var(--ink)" strokeOpacity={0.45} strokeWidth={2} />
                </pattern>
              </defs>

              {NAMESPACES.map((h) => (
                <line key={h} x1={X0} x2={X1} y1={y(h)} y2={y(h)} stroke="var(--line)" strokeDasharray="2 4" />
              ))}

              {SHARDS.filter((sh) => sh.born <= now).map((sh) => {
                const end = Math.min(sh.retired ?? now, now);
                const w = x(end) - x(sh.born);
                const h = y(sh.hi) - y(sh.lo);
                return (
                  <g key={sh.id}>
                    <rect x={x(sh.born)} y={y(sh.lo)} width={Math.max(0, w)} height={h} fill={WRITER_COLORS[sh.owner]} fillOpacity={sh.retired !== undefined && sh.retired <= now ? 0.1 : 0.2} stroke="var(--bg)" strokeWidth={1.5} />
                    {w > 64 && h > 18 && (
                      <text x={x(sh.born) + 6} y={y(sh.lo) + 14} fontSize={10} className="fill-ink font-mono">
                        shard {sh.id} · writer-{sh.owner}
                      </text>
                    )}
                  </g>
                );
              })}

              {active && phase >= 1 && (
                <line x1={x(active.at + PHASES[1])} x2={x(now)} y1={y(active.cut)} y2={y(active.cut)} stroke="var(--ink)" strokeWidth={1.5} strokeDasharray="5 3" />
              )}
              {paused && (
                <rect x={x(paused.retired! - COMMIT + PHASES[2])} y={y(paused.lo)} width={Math.max(0, x(now) - x(paused.retired! - COMMIT + PHASES[2]))} height={y(paused.hi) - y(paused.lo)} fill="url(#vss-paused)" />
              )}

              {SPLITS.filter((sp) => now >= sp.at).map((sp) => (
                <g key={sp.writer}>
                  <line x1={x(sp.at)} x2={x(sp.at)} y1={Y0 - 10} y2={Y1} stroke="var(--ink)" strokeOpacity={0.35} strokeDasharray="3 3" />
                  <motion.text initial={{ opacity: 0 }} animate={{ opacity: 1 }} x={x(sp.at) + 4} y={Y0 - 14} fontSize={10.5} className="font-mono" fill={WRITER_COLORS[sp.writer]}>
                    + writer-{sp.writer}
                  </motion.text>
                </g>
              ))}

              {NAMESPACES.map((h) => {
                const isPaused = paused && h >= paused.lo && h < paused.hi;
                const color = WRITER_COLORS[ownerOf(h)];
                return (
                  <circle key={h} cx={x(now)} cy={y(h)} r={3.5} fill={isPaused ? "var(--bg)" : color} stroke={color} strokeWidth={1.5} />
                );
              })}

              <line x1={x(now)} x2={x(now)} y1={Y0} y2={Y1} stroke="var(--accent-text)" strokeWidth={1.5} />
              <text x={x(now)} y={Y1 + 16} textAnchor="middle" fontSize={10.5} className="font-mono" fill="var(--accent-text)">
                now
              </text>

              {["0x0000", "0x4000", "0x8000", "0xc000", "0xffff"].map((l, i) => (
                <text key={l} x={X0 - 8} y={Y0 + (i / 4) * (Y1 - Y0) + 4} textAnchor="end" fontSize={10} className="fill-faint font-mono">
                  {l}
                </text>
              ))}
              <text x={14} y={(Y0 + Y1) / 2} fontSize={10} className="fill-faint font-mono" transform={`rotate(-90 14 ${(Y0 + Y1) / 2})`} textAnchor="middle">
                namespace id
              </text>
              <text x={X1} y={Y1 + 30} textAnchor="end" fontSize={10} className="fill-faint font-mono">
                time →
              </text>
            </svg>
          </div>

          <div className="grid md:grid-cols-[1fr_260px]">
            <div className="flex min-h-[260px] flex-col border-b border-line p-4 md:border-b-0 md:border-r">
              <div className="mb-3 text-[14px] font-medium text-ink">
                {shown ? `Split of shard ${shown.parent.id}` : "One writer"} <span className="eyebrow">· {active ? "in progress" : shown ? "done" : "waiting for a writer"}</span>
              </div>
              <AnimatePresence mode="wait">
                {shown ? (
                  <motion.ol key={shown.writer} className="flex flex-col gap-2.5" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                    {steps(shown).map(([k, v], i) => (
                      <li key={k} className="grid grid-cols-[18px_1fr] gap-2 text-[12px] transition-opacity duration-300" style={{ opacity: i <= done ? 1 : 0.3 }}>
                        <span
                          className="mt-px flex h-[18px] w-[18px] items-center justify-center rounded-[2px] border font-mono text-[9.5px]"
                          style={i === phase ? { borderColor: "var(--accent-text)", color: "var(--accent-text)" } : { borderColor: "var(--line-strong)", color: "var(--muted)" }}
                        >
                          {i + 1}
                        </span>
                        <span>
                          <span className="block text-[11.5px] italic text-muted">{k}</span>
                          <span className="block break-words font-mono text-[11.5px] text-ink">{v}</span>
                        </span>
                      </li>
                    ))}
                  </motion.ol>
                ) : (
                  <motion.p key="idle" className="text-[12px] text-muted" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                    writer-0 owns shard 0, the whole namespace id space. Every namespace lives on it.
                  </motion.p>
                )}
              </AnimatePresence>
            </div>
            <div className="p-4">
              <div className="mb-3 text-[14px] font-medium text-ink">
                Topology <span className="eyebrow">· generation {gen}</span>
              </div>
              <div className="flex flex-col gap-1">
                {live.map((sh) => (
                  <div key={sh.id} className="flex items-center gap-2 whitespace-nowrap py-0.5 font-mono text-[11.5px]">
                    <span className="h-2 w-2 shrink-0" style={{ background: WRITER_COLORS[sh.owner] }} />
                    <span className="text-ink">shard {sh.id}</span>
                    <span className="leader" />
                    <span className="text-muted">
                      {active?.parent.id === sh.id ? <span className="text-accent-text">splitting</span> : `w${sh.owner} · ${nsIn(sh)} ns`}
                    </span>
                  </div>
                ))}
              </div>
              <div className="mt-3 flex flex-col font-mono text-[11px] text-faint">
                {live.map((sh) => (
                  <span key={sh.id}>
                    {sh.id}: {range(sh)}
                  </span>
                ))}
              </div>
            </div>
          </div>
        </div>
      </Figure>
    </div>
  );
}
