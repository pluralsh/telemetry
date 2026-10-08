import Link from "next/link";
import { Fragment } from "react";

const TOKEN = /(`[^`]+`|\*\*[^*]+\*\*|\[[^\]]+\]\([^)]+\))/g;

/** Renders the inline subset of Markdown used in API copy: `code`, **bold**, and [links](href). */
export function InlineMd({ text, className }: { text?: string; className?: string }) {
  if (!text) return null;
  const parts = text.split(TOKEN).filter(Boolean);
  return (
    <span className={className}>
      {parts.map((p, i) => {
        if (p.startsWith("`")) {
          return (
            <code key={i} className="rounded-[2px] border border-line bg-code px-[0.3em] py-[0.05em] font-mono text-[0.88em] text-ink">
              {p.slice(1, -1)}
            </code>
          );
        }
        if (p.startsWith("**")) return <strong key={i} className="font-bold text-ink">{p.slice(2, -2)}</strong>;
        const link = p.match(/^\[([^\]]+)\]\(([^)]+)\)$/);
        if (link) {
          const [, label, href] = link;
          return href.startsWith("/") ? (
            <Link key={i} href={href} className="text-accent-text underline decoration-line-strong underline-offset-2 hover:decoration-current">
              {label}
            </Link>
          ) : (
            <a key={i} href={href} target="_blank" rel="noreferrer" className="text-accent-text underline decoration-line-strong underline-offset-2 hover:decoration-current">
              {label}
            </a>
          );
        }
        return <Fragment key={i}>{p}</Fragment>;
      })}
    </span>
  );
}
