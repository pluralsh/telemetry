"use client";

import { useState } from "react";
import { MetricsIcon, LogsIcon, TracesIcon } from "../icons";
import { Figure, Segmented } from "./figure";

const COLS = [
  { x: 40, name: "Metrics", peer: "Prometheus · Mimir", proto: "remote write · OTLP", q: "PromQL", color: "var(--metrics)", Icon: MetricsIcon },
  { x: 320, name: "Logs", peer: "Loki", proto: "Loki push · OTLP · _bulk", q: "LogQL", color: "var(--logs)", Icon: LogsIcon },
  { x: 600, name: "Traces", peer: "Tempo", proto: "OTLP · Zipkin · Jaeger", q: "TraceQL", color: "var(--traces)", Icon: TracesIcon },
];
const W = 240;

const SLATE_PARTS = [
  { label: "WAL", sub: "group commit" },
  { label: "memtable", sub: "in-memory" },
  { label: "L0 SSTs", sub: "flushed" },
  { label: "sorted runs", sub: "compacted" },
  { label: "manifest", sub: "writer fencing" },
  { label: "block cache", sub: "Foyer · RAM + NVMe" },
];

export function StackDiagram() {
  const [mode, setMode] = useState<"write" | "read">("write");
  const write = mode === "write";

  return (
    <Figure
      label="Fig. 1 — Three databases, one storage engine"
      controls={
        <Segmented
          value={mode}
          onChange={setMode}
          options={[
            { value: "write", label: "Write path" },
            { value: "read", label: "Read path" },
          ]}
        />
      }
      caption={
        write
          ? "Writes are batched into a WAL and memtable, flushed as SSTs to object storage, and compacted in the background. Each storage shard is its own SlateDB database owned by exactly one writer."
          : "Readers open every shard read-only, follow the manifest, and serve queries through a shared RAM + NVMe block cache, so hot data never round-trips to S3."
      }
      bodyClassName="px-2 py-4"
    >
      <svg viewBox="0 0 880 560" className="h-auto w-full" role="img" aria-label="Metrics, Logs and Traces built on SlateDB over object storage">
        <defs>
          <pattern id="stack-hatch" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
            <path d="M0 0v6" stroke="var(--fg)" strokeOpacity="0.09" />
          </pattern>
          <marker id="arrow" viewBox="0 0 6 6" refX="5" refY="3" markerWidth="6" markerHeight="6" orient="auto">
            <path d="M0 0L6 3L0 6z" fill="var(--line-strong)" />
          </marker>
        </defs>

        {/* protocol row */}
        {COLS.map((c) => (
          <g key={c.name}>
            <rect x={c.x} y={14} width={W} height={30} rx={2} fill="var(--bg)" stroke="var(--line-strong)" />
            <text x={c.x + W / 2} y={33} textAnchor="middle" className="fill-muted font-mono" fontSize={11.5}>
              {write ? c.proto : `Grafana · ${c.q}`}
            </text>
          </g>
        ))}

        {/* product cards */}
        {COLS.map((c) => (
          <g key={c.name + "card"}>
            <rect x={c.x} y={82} width={W} height={124} rx={3} fill="var(--panel)" stroke="var(--line-strong)" />
            <rect x={c.x} y={82} width={W} height={3} fill={c.color} />
            <rect x={c.x + 16} y={100} width={44} height={44} fill="var(--bg)" stroke="var(--line-strong)" />
            <g transform={`translate(${c.x + 24} ${108})`}>
              <c.Icon size={28} />
            </g>
            <text x={c.x + 74} y={119} className="fill-ink font-serif" fontSize={21}>
              {c.name}
            </text>
            <text x={c.x + 74} y={137} className="fill-muted" fontSize={11.5} fontStyle="italic">
              {c.peer}-compatible
            </text>
            <g className="font-mono" fontSize={10.5}>
              {["writer", "reader"].map((r, i) => (
                <g key={r}>
                  <rect x={c.x + 16 + i * 108} y={160} width={100} height={28} rx={2} fill="var(--bg)" stroke="var(--line)" />
                  <text x={c.x + 66 + i * 108} y={178} textAnchor="middle" className="fill-muted">
                    {r}s ×N
                  </text>
                </g>
              ))}
            </g>
          </g>
        ))}

        {/* SlateDB band */}
        <rect x={40} y={262} width={800} height={136} rx={3} fill="var(--panel)" stroke="var(--ink)" strokeOpacity={0.55} />
        <text x={62} y={292} className="fill-ink font-serif" fontSize={22}>
          SlateDB
        </text>
        <text x={152} y={292} className="fill-muted" fontSize={12} fontStyle="italic">
          embedded LSM · one database per storage shard · single writer, many readers
        </text>
        {SLATE_PARTS.map((p, i) => {
          const x = 62 + i * 128;
          const lit = write ? i < 4 : i >= 3;
          return (
            <g key={p.label}>
              <rect
                x={x}
                y={314}
                width={116}
                height={62}
                rx={2}
                fill={lit ? "color-mix(in srgb, var(--accent-text) 8%, var(--bg))" : "var(--bg)"}
                stroke={lit ? "var(--accent-text)" : "var(--line)"}
                strokeOpacity={lit ? 0.7 : 1}
                style={{ transition: "all .4s" }}
              />
              <text x={x + 58} y={341} textAnchor="middle" className="fill-ink font-mono" fontSize={12}>
                {p.label}
              </text>
              <text x={x + 58} y={359} textAnchor="middle" className="fill-muted" fontSize={10.5}>
                {p.sub}
              </text>
              {i < SLATE_PARTS.length - 1 && <path d={`M${x + 117} 345h10`} stroke="var(--line-strong)" markerEnd="url(#arrow)" />}
            </g>
          );
        })}

        {/* object storage */}
        <rect x={40} y={446} width={800} height={86} rx={3} fill="url(#stack-hatch)" stroke="var(--line-strong)" />
        <text x={62} y={478} className="fill-ink font-serif" fontSize={20}>
          Object storage
        </text>
        <text x={62} y={500} className="fill-fg" fontSize={12.5}>
          The only stateful dependency: 11 nines of durability, no replicated disks, no cross-AZ replication traffic.
        </text>
        {[
          { s: "S3", x: 586, w: 44 },
          { s: "GCS", x: 638, w: 50 },
          { s: "Azure Blob", x: 696, w: 78 },
          { s: "MinIO", x: 782, w: 54 },
        ].map(({ s, x, w }) => (
          <g key={s}>
            <rect x={x} y={462} width={w} height={22} rx={2} fill="var(--panel)" stroke="var(--line-strong)" />
            <text x={x + w / 2} y={477} textAnchor="middle" className="fill-muted font-mono" fontSize={10.5}>
              {s}
            </text>
          </g>
        ))}

        {/* flows */}
        {COLS.map((c, ci) => {
          const cx = c.x + W / 2;
          const down = `M${cx} 44 V82 M${cx} 206 V314 M${cx} 376 V446`;
          const pathWrite = `M${cx} 44 L${cx} 482`;
          const pathRead = `M${cx} 482 L${cx} 44`;
          return (
            <g key={c.name + "flow"}>
              <path d={down} stroke={c.color} strokeOpacity={0.55} strokeWidth={1.25} className="flow-dash" style={{ animationDirection: write ? "normal" : "reverse" }} />
              {[0, 1, 2].map((k) => (
                <rect key={`${mode}${k}`} x={-3.5} y={-3.5} width={7} height={7} fill={c.color}>
                  <animateMotion dur="3.2s" begin={`${k * 1.05 + ci * 0.35}s`} repeatCount="indefinite" path={write ? pathWrite : pathRead} />
                  <animate attributeName="opacity" values="0;1;1;0" keyTimes="0;0.08;0.9;1" dur="3.2s" begin={`${k * 1.05 + ci * 0.35}s`} repeatCount="indefinite" />
                </rect>
              ))}
            </g>
          );
        })}
      </svg>
    </Figure>
  );
}
