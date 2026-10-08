"use client";

import { motion } from "motion/react";
import { useId } from "react";

/** Framed figure: rule-separated header, body, caption, and nodes on the frame corners. */
export function Figure({
  label,
  controls,
  caption,
  children,
  className = "",
  bodyClassName = "",
}: {
  label: React.ReactNode;
  controls?: React.ReactNode;
  caption?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
  bodyClassName?: string;
}) {
  return (
    <figure className={`not-prose relative my-10 border border-line-strong bg-card 2xl:-mx-10 ${className}`}>
      <span aria-hidden className="node" style={{ left: -4.5, top: -4.5 }} />
      <span aria-hidden className="node" style={{ right: -4.5, top: -4.5 }} />
      <span aria-hidden className="node" style={{ left: -4.5, bottom: -4.5 }} />
      <span aria-hidden className="node" style={{ right: -4.5, bottom: -4.5 }} />
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-line px-4 py-2.5">
        <span className="font-serif text-[14px] text-ink">{label}</span>
        {controls && <div className="flex flex-wrap items-center gap-2">{controls}</div>}
      </div>
      <div className={bodyClassName}>{children}</div>
      {caption && <figcaption className="border-t border-line px-4 py-3 text-[13px] leading-relaxed text-fg">{caption}</figcaption>}
    </figure>
  );
}

export function Segmented<T extends string>({
  value,
  onChange,
  options,
  size = "sm",
}: {
  value: T;
  onChange: (v: T) => void;
  options: { value: T; label: React.ReactNode }[];
  size?: "sm" | "xs";
}) {
  const id = useId();
  return (
    <div
      role="radiogroup"
      className={`flex items-stretch rounded-sm border border-line-strong bg-card p-[2px] ${size === "xs" ? "h-[26px] text-[11.5px]" : "h-[30px] text-[12.5px]"}`}
    >
      {options.map((o, i) => {
        const on = o.value === value;
        return (
          <div key={o.value} className="flex items-stretch">
            {i > 0 && <span aria-hidden className="my-1 w-px bg-line" />}
            <button
              type="button"
              role="radio"
              aria-checked={on}
              onClick={() => onChange(o.value)}
              className={`relative px-2.5 tracking-[-0.2px] transition-colors ${on ? "text-ink" : "text-muted hover:text-ink"}`}
            >
              {on && (
                <motion.span
                  layoutId={`seg-${id}`}
                  className="absolute inset-0 rounded-[2px] border border-line-strong bg-panel shadow-[inset_0_-2px_0_0_var(--edge)]"
                  transition={{ type: "spring", stiffness: 520, damping: 40 }}
                />
              )}
              <span className="relative">{o.label}</span>
            </button>
          </div>
        );
      })}
    </div>
  );
}
