"use client";

import { useEffect, useState } from "react";
import { usePathname } from "next/navigation";

type Heading = { id: string; text: string; level: number };

export function Toc() {
  const pathname = usePathname();
  const [headings, setHeadings] = useState<Heading[]>([]);
  const [active, setActive] = useState<string>("");

  useEffect(() => {
    const els = Array.from(document.querySelectorAll<HTMLElement>("article.prose-docs h2[id], article.prose-docs h3[id]"));
    setHeadings(els.map((el) => ({ id: el.id, text: el.textContent ?? "", level: el.tagName === "H2" ? 2 : 3 })));
    const obs = new IntersectionObserver(
      (entries) => {
        const visible = entries.filter((e) => e.isIntersecting).sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top);
        if (visible[0]) setActive(visible[0].target.id);
      },
      { rootMargin: "-72px 0px -65% 0px" },
    );
    els.forEach((el) => obs.observe(el));
    return () => obs.disconnect();
  }, [pathname]);

  if (headings.length < 2) return null;
  return (
    <div className="text-[12.5px] tracking-[-0.2px]">
      <div className="mb-3 font-serif text-[13px] text-ink">On this page</div>
      <ul className="flex flex-col gap-[3px]">
        {headings.map((h) => (
          <li key={h.id}>
            <a
              href={`#${h.id}`}
              className={`flex items-baseline gap-2 py-[2px] transition-colors ${
                active === h.id ? "text-accent-text" : "text-muted hover:text-ink"
              } ${h.level === 3 ? "pl-4" : ""}`}
            >
              <span
                aria-hidden
                className={`inline-block size-[5px] shrink-0 -translate-y-px rotate-45 border ${
                  active === h.id ? "border-accent-text bg-accent-text" : "border-line-strong"
                }`}
              />
              {h.text}
            </a>
          </li>
        ))}
      </ul>
    </div>
  );
}
