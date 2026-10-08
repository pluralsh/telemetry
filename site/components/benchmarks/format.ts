/** Number and unit are joined by a non-breaking space so chart labels never wrap between them. */
export function fmtMs(v: number | undefined) {
  if (v === undefined || Number.isNaN(v)) return "–";
  if (v >= 1000) return `${(v / 1000).toFixed(v >= 10000 ? 0 : 1)}\u00a0s`;
  if (v >= 100) return `${Math.round(v)}\u00a0ms`;
  if (v >= 10) return `${v.toFixed(1)}\u00a0ms`;
  return `${v.toFixed(2)}\u00a0ms`;
}

export function fmtRatio(v: number | undefined) {
  if (v === undefined) return "–";
  return `${v < 0.1 ? v.toFixed(2) : v.toFixed(v < 10 ? 2 : 1)}×`;
}

export function fmtDate(iso: string) {
  const d = new Date(iso);
  return `${d.toISOString().slice(0, 10)} ${d.toISOString().slice(11, 16)}Z`;
}

export function fmtShortDate(iso: string) {
  const d = new Date(iso);
  return `${d.toLocaleString("en-US", { month: "short", day: "numeric", timeZone: "UTC" })} ${d.toISOString().slice(11, 16)}`;
}

/** Lower is better: green below parity, amber above. */
export function ratioTone(v: number | undefined) {
  if (v === undefined) return "text-muted";
  if (v <= 0.95) return "text-[#1d7a55] dark:text-[#5cc79b]";
  if (v >= 1.05) return "text-[#b4590f] dark:text-[#ef9f58]";
  return "text-fg";
}
