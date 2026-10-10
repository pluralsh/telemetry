import type { ProductId } from "@/lib/nav";
import { GLYPHS, SPACE_COLORS } from "./icons";

const W = 240;
const H = 112;
const BADGE = { x: 200, y: 56 };

type Pt = [number, number];

function Grid() {
  const dots: Pt[] = [];
  for (let x = 10; x < W; x += 10) for (let y = 6; y < H; y += 10) dots.push([x, y]);
  return (
    <g fill="var(--fg)" fillOpacity={0.2}>
      {dots.map(([x, y]) => (
        <circle key={`${x}-${y}`} cx={x} cy={y} r={0.7} />
      ))}
    </g>
  );
}

function Node({ at: [x, y], color, ring }: { at: Pt; color: string; ring?: boolean }) {
  return (
    <g>
      {ring && <circle cx={x} cy={y} r={6} fill="var(--bg)" stroke="var(--line-strong)" />}
      <circle cx={x} cy={y} r={2.4} fill={color} />
    </g>
  );
}

/** The focal badge: a monochrome tile carrying the product glyph. */
function Badge({ product }: { product: ProductId }) {
  const { x, y } = BADGE;
  const color = SPACE_COLORS[product];
  return (
    <g>
      <rect x={x - 26} y={y - 26} width={52} height={52} fill="none" stroke="var(--line)" strokeDasharray="2 3" />
      <rect x={x - 18} y={y - 18} width={36} height={36} fill="var(--bg)" stroke="var(--line-strong)" />
      <rect x={x - 13} y={y - 13} width={26} height={26} fill={color} />
      <g color="var(--bg)" transform={`translate(${x - 8} ${y - 8}) scale(${16 / 24})`}>
        {GLYPHS[product]}
      </g>
    </g>
  );
}

const wire = { fill: "none", stroke: "var(--fg)", strokeOpacity: 0.4, strokeWidth: 1 } as const;
const into = (x: number, y: number) => `M${x} ${y}C${x + 24} ${y} ${BADGE.x - 44} ${BADGE.y} ${BADGE.x - 18} ${BADGE.y}`;

/** Scattered colour on the matrix, fixed so the scene is stable between renders. */
const SPECKLE: Pt[] = [
  [30, 16], [70, 26], [120, 16], [150, 96], [40, 106], [90, 106], [130, 76], [20, 56], [160, 16], [110, 96],
];

function Speckle({ color }: { color: string }) {
  return (
    <g>
      {SPECKLE.map(([x, y], i) => (
        <circle key={i} cx={x} cy={y} r={1.6} fill={i % 3 === 0 ? color : "var(--faint)"} fillOpacity={i % 3 === 0 ? 0.7 : 0.6} />
      ))}
    </g>
  );
}

function MetricsScene() {
  const c = SPACE_COLORS.metrics;
  const pts: Pt[] = [[20, 86], [50, 66], [80, 76], [110, 46], [140, 56], [166, 36]];
  return (
    <>
      <path d={`M${pts.map((p) => p.join(" ")).join("L")}`} {...wire} />
      <path d={into(166, 36)} {...wire} />
      {pts.map((p, i) => (
        <Node key={i} at={p} ring={i === 3 || i === 5} color={i % 2 ? c : "var(--ink)"} />
      ))}
    </>
  );
}

function LogsScene() {
  const c = SPACE_COLORS.logs;
  const rows: [number, number][] = [[26, 150], [46, 110], [66, 130], [86, 90]];
  return (
    <>
      {rows.map(([y, end]) => (
        <path key={y} d={`M20 ${y}H${end}${into(end, y).replace(`M${end} ${y}`, "")}`} {...wire} />
      ))}
      <rect x={44} y={40} width={32} height={12} rx={6} fill="var(--bg)" stroke="var(--line-strong)" />
      {rows.flatMap(([y, end]) =>
        Array.from({ length: Math.floor((end - 20) / 10) + 1 }, (_, i) => {
          const x = 20 + i * 10;
          const lead = i === 0;
          return <circle key={`${x}-${y}`} cx={x} cy={y} r={lead ? 2.4 : 1.5} fill={lead ? "var(--ink)" : (x + y) % 30 === 0 ? c : "var(--faint)"} />;
        }),
      )}
      <Node at={[130, 66]} ring color={c} />
    </>
  );
}

function TracesScene() {
  const c = SPACE_COLORS.traces;
  return (
    <>
      <path d="M20 26H100M20 26V56H60M60 56V86H100M60 56H130" {...wire} />
      <path d={into(100, 26)} {...wire} />
      <path d={`M130 56H${BADGE.x - 18}`} {...wire} />
      <path d={into(100, 86)} {...wire} />
      <Node at={[20, 26]} ring color="var(--ink)" />
      <Node at={[100, 26]} color={c} />
      <Node at={[60, 56]} ring color={c} />
      <Node at={[130, 56]} color="var(--ink)" />
      <Node at={[100, 86]} color={c} />
    </>
  );
}

const SCENES: Record<ProductId, () => React.ReactNode> = {
  metrics: MetricsScene,
  logs: LogsScene,
  traces: TracesScene,
};

/** Dot-matrix illustration in Plural's style, converging on the product badge. */
export function ProductScene({ product, className }: { product: ProductId; className?: string }) {
  const Scene = SCENES[product];
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className={className} aria-hidden>
      <Grid />
      <Speckle color={SPACE_COLORS[product]} />
      <Scene />
      <Badge product={product} />
    </svg>
  );
}
