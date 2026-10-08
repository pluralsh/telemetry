import Link from "next/link";
import { PRODUCTS, PRODUCT_IDS } from "@/lib/nav";
import { ArrowRight, PluralMark, SpaceIcon } from "./icons";
import { ProductScene } from "./scenes";

export function Callout({ title, children, tone = "note" }: { title?: string; children: React.ReactNode; tone?: "note" | "warn" }) {
  const color = tone === "warn" ? "var(--traces)" : "var(--accent-text)";
  return (
    <div className="not-prose card relative px-4 py-3.5 text-[14px] leading-relaxed">
      <span aria-hidden className="absolute inset-y-3 left-0 w-px" style={{ background: color }} />
      {title && (
        <div className="mb-1 flex items-center gap-2 font-serif text-[14.5px] text-ink">
          <span aria-hidden className="inline-block size-[6px] rotate-45" style={{ background: color }} />
          {title}
        </div>
      )}
      <div className="text-fg [&_a]:text-accent-text [&_a]:underline [&_a]:underline-offset-2 [&_code]:font-mono [&_code]:text-[12.5px] [&_code]:text-ink">
        {children}
      </div>
    </div>
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
                <span className="font-serif text-[18px] text-ink">{p.name}</span>
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
      <Link
        href="/manifesto/"
        className="mb-10 flex w-fit items-center gap-2 border-y border-line py-1.5 pr-1 text-[13.5px] tracking-[-0.2px] text-ink"
        style={{ backgroundImage: "linear-gradient(90deg, color-mix(in srgb, var(--accent) 9%, transparent), transparent)" }}
      >
        <span className="pl-2 font-serif tracking-wide text-accent-text">Manifesto:</span>
        <span className="font-serif">why observability should cost less than the thing it observes</span>
        <ArrowRight />
      </Link>
      <h1 className="font-serif text-[2.9rem] font-light leading-[1.08] tracking-[-0.025em] text-ink sm:text-[3.4rem]">
        Observability should be{" "}
        <em className="font-light text-accent-text">cheap</em> and <em className="font-light text-accent-text">easy</em>.
      </h1>
      <p className="mt-6 max-w-[600px] text-[16px] leading-[1.6] tracking-[-0.4px] text-fg">
        Plural Telemetry is a Rust reimplementation of Prometheus, Loki and Tempo, built directly on object storage with{" "}
        <a href="https://slatedb.io" className="text-ink underline decoration-line-strong underline-offset-4 hover:decoration-ink">
          SlateDB
        </a>
        . You run two kinds of pods instead of ten, keep your Grafana dashboards, and the only stateful dependency is a bucket.
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
        Apache 2.0 · Kubernetes 1.28+ · sponsored by
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
      <div className="font-serif text-[30px] font-light leading-none tabular-nums text-ink">{value}</div>
      <div className="mt-2 text-[12.5px] leading-snug text-fg">{label}</div>
    </div>
  );
}

export function Stats({ children }: { children: React.ReactNode }) {
  return <div className="not-prose my-10 grid grid-cols-2 border-y border-line md:grid-cols-4">{children}</div>;
}
