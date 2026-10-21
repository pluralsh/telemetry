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
  const current = active || headings[0].id;
  return (
    <nav aria-label="On this page" className="pb-8">
      <div className="label mb-3 mt-9 text-[12px]">On this page</div>
      <ul className="border-l-2 border-line">
        {headings.map((h) => {
          const on = current === h.id;
          return (
            <li key={h.id}>
              <a
                href={`#${h.id}`}
                className={`-ml-[2px] block border-l-2 py-1 pr-3 text-[14px] leading-[1.45] transition-colors ${
                  on ? "border-accent text-accent-text" : "border-transparent text-muted hover:text-ink"
                } ${h.level === 3 ? "pl-6" : "pl-3"}`}
              >
                {h.text}
              </a>
            </li>
          );
        })}
      </ul>
    </nav>
  );
}
