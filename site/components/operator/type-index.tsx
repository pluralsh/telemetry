"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { InlineMd } from "@/components/inline-md";
import { operatorKindHref } from "@/lib/nav";

export type IndexedType = { name: string; anchor: string; summary: string; kinds: string[] };

/** Every API type with the resource pages that document it, filterable by name. */
export function TypeIndex({ types }: { types: IndexedType[] }) {
  const [query, setQuery] = useState("");
  const shown = useMemo(() => {
    const q = query.trim().toLowerCase();
    return q ? types.filter((t) => t.name.toLowerCase().includes(q) || t.summary.toLowerCase().includes(q)) : types;
  }, [query, types]);

  return (
    <div className="not-prose flex flex-col gap-3">
      <input
        type="search"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder={`Filter ${types.length} types`}
        aria-label="Filter types"
        className="h-9 w-full rounded-[4px] border border-line-strong bg-panel px-3 text-[14px] text-ink outline-none placeholder:text-faint focus:border-accent-ring"
      />
      <ul className="rounded-sm border border-line px-3.5">
        {shown.map((t) => (
          <li key={t.name} className="flex flex-col gap-0.5 border-t border-line py-2.5 first:border-t-0">
            <div className="flex flex-wrap items-baseline gap-x-2">
              <Link href={`${operatorKindHref(t.kinds[0])}#${t.anchor}`} className="font-mono text-[13.5px] font-medium text-accent-text hover:underline">
                {t.name}
              </Link>
              {t.kinds.length > 1 && (
                <span className="text-[12px] text-muted">
                  also on{" "}
                  {t.kinds.slice(1).map((k, i) => (
                    <span key={k}>
                      {i > 0 && ", "}
                      <Link href={`${operatorKindHref(k)}#${t.anchor}`} className="hover:text-ink">
                        {k}
                      </Link>
                    </span>
                  ))}
                </span>
              )}
            </div>
            {t.summary && <InlineMd text={t.summary} className="text-[13px] leading-[1.5] text-muted" />}
          </li>
        ))}
        {!shown.length && <li className="py-3 text-[13px] text-muted">No types match “{query}”.</li>}
      </ul>
    </div>
  );
}
