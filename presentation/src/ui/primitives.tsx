import { animate, motion, useMotionValue, useTransform } from "motion/react";
import { useEffect, type CSSProperties, type ReactNode } from "react";
import { useStep } from "../deck/context";

export const EASE = [0.22, 1, 0.36, 1] as const;

/** True once the slide has reached `at` (and, with `until`, hasn't passed it). */
export function useAt(at: number, until?: number) {
  const { step } = useStep();
  return step >= at && (until === undefined || step <= until);
}

/** Fades and lifts children in when the slide reaches build step `at`. */
export function Reveal({
  at = 0,
  until,
  children,
  y = 18,
  x = 0,
  delay = 0,
  className = "",
  style,
}: {
  at?: number;
  until?: number;
  children: ReactNode;
  y?: number;
  x?: number;
  delay?: number;
  className?: string;
  style?: CSSProperties;
}) {
  const { live } = useStep();
  const on = useAt(at, until);
  return (
    <motion.div
      className={className}
      style={style}
      initial={live ? { opacity: 0, y, x } : false}
      animate={on ? { opacity: 1, y: 0, x: 0 } : { opacity: 0, y, x }}
      transition={{ duration: 0.55, ease: EASE, delay: on ? delay : 0 }}
    >
      {children}
    </motion.div>
  );
}

/** Standard slide layout: mono eyebrow, serif title, optional lede, then free-form content. */
export function Slide({
  eyebrow,
  title,
  lede,
  children,
  className = "",
  titleClass = "",
}: {
  eyebrow?: ReactNode;
  title?: ReactNode;
  lede?: ReactNode;
  children?: ReactNode;
  className?: string;
  titleClass?: string;
}) {
  const { live } = useStep();
  return (
    <div className={`absolute inset-0 flex flex-col px-[72px] pb-[44px] pt-[56px] ${live ? "" : "static"} ${className}`}>
      {eyebrow && <div className="eyebrow mb-4 text-[20px]">{eyebrow}</div>}
      {title && <h1 className={`font-serif text-[68px] font-normal leading-[1.04] tracking-[-0.02em] text-ink ${titleClass}`}>{title}</h1>}
      {lede && <p className="mt-5 max-w-[1240px] text-[28px] leading-[1.45] text-fg">{lede}</p>}
      {children && <div className="relative mt-10 min-h-0 flex-1">{children}</div>}
    </div>
  );
}

/** Framed panel with corner nodes, mirroring the docs' Figure. */
export function Frame({
  label,
  right,
  children,
  className = "",
  bodyClassName = "",
  style,
}: {
  label?: ReactNode;
  right?: ReactNode;
  children: ReactNode;
  className?: string;
  bodyClassName?: string;
  style?: CSSProperties;
}) {
  return (
    <div className={`relative flex flex-col border border-line-strong bg-panel ${className}`} style={style}>
      <span aria-hidden className="node" style={{ left: -4.5, top: -4.5 }} />
      <span aria-hidden className="node" style={{ right: -4.5, top: -4.5 }} />
      <span aria-hidden className="node" style={{ left: -4.5, bottom: -4.5 }} />
      <span aria-hidden className="node" style={{ right: -4.5, bottom: -4.5 }} />
      {label && (
        <div className="flex items-center justify-between gap-4 border-b border-line px-5 py-3">
          <span className="text-[20px] font-medium text-ink">{label}</span>
          {right && <span className="eyebrow text-[16px]">{right}</span>}
        </div>
      )}
      <div className={`relative min-h-0 flex-1 ${bodyClassName}`}>{children}</div>
    </div>
  );
}

/** Animated number that counts up from 0 when `on` becomes true. */
export function CountUp({ to, on = true, decimals = 0, prefix = "", suffix = "", duration = 1.4 }: { to: number; on?: boolean; decimals?: number; prefix?: string; suffix?: string; duration?: number }) {
  const { live } = useStep();
  const mv = useMotionValue(live ? 0 : to);
  const text = useTransform(mv, (v) => `${prefix}${v.toFixed(decimals)}${suffix}`);
  useEffect(() => {
    if (!live) {
      mv.set(to);
      return;
    }
    const ctrl = animate(mv, on ? to : 0, { duration: on ? duration : 0.3, ease: EASE });
    return () => ctrl.stop();
  }, [on, to, live, mv, duration]);
  return <motion.span className="tabular-nums">{text}</motion.span>;
}

export function Tag({ children, color = "var(--muted)", className = "" }: { children: ReactNode; color?: string; className?: string }) {
  return (
    <span
      className={`code inline-flex items-center rounded-[3px] border px-2 py-0.5 text-[0.62em] ${className}`}
      style={{ color, borderColor: `color-mix(in srgb, ${color} 35%, transparent)`, background: `color-mix(in srgb, ${color} 7%, transparent)` }}
    >
      {children}
    </span>
  );
}

/** Small numbered bullet with serif numerals, used for enumerations. */
export function Numeral({ n, color = "var(--accent-text)" }: { n: number | string; color?: string }) {
  return (
    <span className="font-serif text-[1.3em] leading-none tabular-nums" style={{ color }}>
      {typeof n === "number" ? String(n).padStart(2, "0") : n}
    </span>
  );
}

/** SVG hatch pattern definition matching the docs' object-storage fill. */
export function HatchDef({ id }: { id: string }) {
  return (
    <pattern id={id} width="7" height="7" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
      <path d="M0 0v7" stroke="var(--fg)" strokeOpacity="0.12" />
    </pattern>
  );
}
