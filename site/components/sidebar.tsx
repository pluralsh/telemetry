"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { navFor, spaceFromPath, PRODUCTS, type ProductId } from "@/lib/nav";
import { SpaceIcon } from "./icons";
import { MethodBadge } from "./method-badge";

export type ApiNav = Record<
  ProductId,
  { tag: string; ops: { anchor: string; title: string; method: string }[] }[]
>;

function norm(p: string) {
  return p.endsWith("/") ? p : p + "/";
}

const ITEM = "flex rounded-[4px] px-2.5 py-1.5 transition-colors";
const IDLE = "text-ink-light hover:bg-bg-subtle hover:text-ink";
const ACTIVE = "bg-accent-tint font-medium text-accent-text";

export function Sidebar({ apiNav, onNavigate }: { apiNav: ApiNav; onNavigate?: () => void }) {
  const pathname = norm(usePathname());
  const space = spaceFromPath(pathname);
  const groups = navFor(space);

  return (
    <nav className="pb-12 pl-4 pr-3 pt-5 text-[14px] leading-[1.45]">
      <div className="flex flex-col gap-5">
        {groups.map((g) => (
          <div key={g.title}>
            <div className="label px-2.5 py-1.5">{g.title}</div>
            <ul className="flex flex-col gap-px">
              {g.items.map((item) => {
                const active = norm(item.href) === pathname;
                const isApi = space !== "overview" && item.href.endsWith("/api/");
                return (
                  <li key={item.href}>
                    <Link href={item.href} onClick={onNavigate} className={`${ITEM} ${active ? ACTIVE : IDLE}`}>
                      {item.title}
                    </Link>
                    {isApi && active && (
                      <div className="mb-1 ml-3 mt-0.5 flex flex-col gap-3 border-l border-line pl-2">
                        {apiNav[space].map((t) => (
                          <div key={t.tag}>
                            <a href={`#tag-${t.tag}`} onClick={onNavigate} className="label block px-2.5 py-1 hover:text-ink">
                              {t.tag}
                            </a>
                            {t.ops.map((op) => (
                              <a
                                key={op.anchor}
                                href={`#${op.anchor}`}
                                onClick={onNavigate}
                                className="flex items-center gap-2 rounded-[4px] px-2.5 py-1 text-[13px] text-muted transition-colors hover:bg-bg-subtle hover:text-ink"
                              >
                                <MethodBadge method={op.method} compact />
                                <span className="truncate">{op.title}</span>
                              </a>
                            ))}
                          </div>
                        ))}
                      </div>
                    )}
                  </li>
                );
              })}
            </ul>
          </div>
        ))}

        {space === "overview" && (
          <div>
            <div className="label px-2.5 py-1.5">Databases</div>
            <ul className="flex flex-col gap-px">
              {(Object.keys(PRODUCTS) as ProductId[]).map((id) => (
                <li key={id}>
                  <Link href={`/${id}/`} onClick={onNavigate} className={`${ITEM} ${IDLE} items-center gap-2.5`}>
                    <SpaceIcon space={id} size={16} />
                    <span>{PRODUCTS[id].name}</span>
                    <span className="leader" />
                    <span className="eyebrow">{PRODUCTS[id].peer}</span>
                  </Link>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </nav>
  );
}
