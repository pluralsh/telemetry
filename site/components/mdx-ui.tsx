import Link from "next/link";
import { PRODUCTS, PRODUCT_IDS } from "@/lib/nav";
import { ArrowRight, PluralMark, SpaceIcon } from "./icons";
import { ProductScene } from "./scenes";

const CALLOUT_TONES = {
  note: {
    color: "var(--violet)",
    label: "Info",
    icon: (
      <>
        <circle cx="8" cy="8" r="6.25" />
        <path d="M8 7.25v4M8 4.75v.5" />
      </>
    ),
  },
  warn: {
    color: "var(--warning)",
    label: "Warning",
    icon: (
      <>
        <path d="M8 2 14.5 13.5h-13L8 2Z" strokeLinejoin="round" />
        <path d="M8 6.5v3.25M8 11.5v.5" />
      </>
    ),
  },
} as const;

/** Tinted callout matching the main docs: colored label row, then title and body in ink. */
export function Callout({ title, children, tone = "note" }: { title?: string; children: React.ReactNode; tone?: "note" | "warn" }) {
  const { color, label, icon } = CALLOUT_TONES[tone];
  return (
    <aside
      className="not-prose flex flex-col gap-2 rounded-[4px] px-5 py-4 text-[15px] leading-[1.65]"
      style={{
        backgroundColor: `color-mix(in srgb, ${color} var(--callout-tint), var(--panel))`,
        border: `1px solid color-mix(in srgb, ${color} var(--callout-border), transparent)`,
      }}
    >
      <div className="label flex items-center gap-2" style={{ color }}>
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" aria-hidden>
          {icon}
        </svg>
        {label}
      </div>
      {title && <div className="font-medium text-ink">{title}</div>}
      <div className="text-ink [&_a]:text-accent-text [&_a]:underline [&_a]:decoration-[var(--accent-ring)] [&_a]:underline-offset-[3px] [&_code]:rounded-[4px] [&_code]:bg-code-inline [&_code]:px-[0.36em] [&_code]:py-[0.12em] [&_code]:font-mono [&_code]:text-[0.86em]">
        {children}
      </div>
    </aside>
  );
}

/** Zed-style product bands: tinted hatch, a large mark, and a dotted-leader caption. */
export function ProductCards() {
  return (
    <div className="not-prose my-10 grid gap-3 sm:grid-cols-3">
      {PRODUCT_IDS.map((id) => {
        const p = PRODUCTS[id];
        return (
          <Link key={id} href={`/${id}/`} className="card group flex flex-col overflow-hidden">
            <div className="border-b border-line-strong bg-bg">
              <ProductScene product={id} className="block h-auto w-full" />
            </div>
            <div className="flex flex-1 flex-col gap-2 p-4">
              <div className="flex items-center gap-2">
                <SpaceIcon space={id} size={18} />
                <span className="text-[17px] font-medium text-ink">{p.name}</span>
                <span className="leader" />
                <span className="eyebrow">{p.peer}</span>
              </div>
              <p className="text-[13px] leading-[1.55] text-fg">{p.tagline}.</p>
              <span className="mt-auto flex items-center gap-1 pt-2 text-[13px] text-muted group-hover:text-accent-text">
                Read the docs <ArrowRight />
              </span>
            </div>
          </Link>
        );
      })}
    </div>
  );
}

export function Hero() {
  return (
    <header className="not-prose relative mb-10">
      <h1 className="font-serif text-[2.9rem] font-normal leading-[1.05] tracking-[-0.02em] text-ink sm:text-[3.4rem]">
        Unbelievably <em className="text-accent-text">Simple</em> Observability
      </h1>
      <p className="mt-6 max-w-[600px] text-[16px] leading-[1.6] tracking-[-0.4px] text-fg">
        Plural Telemetry is a Rust reimplementation of Prometheus, Loki and Tempo, built with{" "}
        <a href="https://slatedb.io" className="text-ink underline decoration-line-strong underline-offset-4 hover:decoration-ink">
          SlateDB
        </a>
        {" "}and designed to work seamlessly on Kubernetes at any scale.
      </p>
      <div className="mt-8 flex flex-wrap items-center gap-2.5">
        <Link href="/installation/" className="btn btn-primary">
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none" aria-hidden>
            <path d="M7 1.5v8M3.5 6 7 9.5 10.5 6M2 12.5h10" stroke="currentColor" strokeWidth="1.3" />
          </svg>
          Install the operator
        </Link>
        <Link href="/architecture/" className="btn btn-secondary">
          How it works
        </Link>
      </div>
      <p className="mt-4 flex items-center gap-1.5 text-[13px] text-muted">
        AGPL v3 · Kubernetes 1.28+ · sponsored by
        <a href="https://plural.sh" className="inline-flex items-center gap-1 text-ink hover:underline">
          <span>
            <PluralMark size={12} />
          </span>
          Plural
        </a>
      </p>
    </header>
  );
}

export function Stat({ value, label }: { value: string; label: string }) {
  return (
    <div className="px-5 py-5 [&+&]:border-l [&+&]:border-line max-md:[&:nth-child(3)]:border-l-0 max-md:[&:nth-child(n+3)]:border-t">
      <div className="font-serif text-[32px] leading-none tabular-nums text-ink">{value}</div>
      <div className="mt-2 text-[12.5px] leading-snug text-fg">{label}</div>
    </div>
  );
}

export function Stats({ children }: { children: React.ReactNode }) {
  return <div className="not-prose my-10 grid grid-cols-2 border-y border-line md:grid-cols-4">{children}</div>;
}
