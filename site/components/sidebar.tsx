"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { AnimatePresence, motion } from "motion/react";
import { navFor, spaceFromPath, PRODUCTS, VECTOR, type ApiProduct, type NavItem, type ProductId } from "@/lib/nav";
import { SpaceIcon } from "./icons";
import { MethodBadge } from "./method-badge";

export type ApiNav = Record<
  ApiProduct,
  { tag: string; ops: { anchor: string; title: string; method: string }[] }[]
>;

function norm(p: string) {
  return p.endsWith("/") ? p : p + "/";
}

const ITEM = "flex rounded-[4px] px-2.5 py-1.5 transition-colors";
const IDLE = "text-ink-light hover:bg-bg-subtle hover:text-ink";
const ACTIVE = "bg-accent-tint font-medium text-accent-text";

function NavSection({ item, pathname, onNavigate }: { item: NavItem; pathname: string; onNavigate?: () => void }) {
  const children = item.children ?? [];
  const within = norm(item.href) === pathname || children.some((c) => norm(c.href) === pathname);
  const [open, setOpen] = useState(within);
  useEffect(() => {
    if (within) setOpen(true);
  }, [within]);
  const active = norm(item.href) === pathname;

  return (
    <>
      <div className={`flex rounded-[4px] transition-colors ${active ? ACTIVE : IDLE}`}>
        <Link href={item.href} onClick={onNavigate} className="flex-1 px-2.5 py-1.5">
          {item.title}
        </Link>
        <button
          type="button"
          onClick={() => setOpen((o) => !o)}
          aria-label={open ? "Collapse" : "Expand"}
          aria-expanded={open}
          className="flex items-center pl-[7.5px] pr-2.5 text-muted transition-colors hover:text-ink"
        >
          <svg width="12" height="12" viewBox="0 0 16 16" fill="none" aria-hidden>
            <path d={open ? "M2 8h12" : "M2 8h12M8 2v12"} stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
        </button>
      </div>
      <AnimatePresence initial={false}>
        {open && (
          <motion.div
            initial={{ height: 0 }}
            animate={{ height: "auto" }}
            exit={{ height: 0 }}
            transition={{ duration: 0.2, ease: "easeOut" }}
            className="overflow-hidden"
          >
            <ul className="ml-3 mt-px flex flex-col gap-px border-l border-line pl-[7.5px]">
              {children.map((child) => (
                <li key={child.href}>
                  <Link href={child.href} onClick={onNavigate} className={`${ITEM} ${norm(child.href) === pathname ? ACTIVE : IDLE}`}>
                    {child.title}
                  </Link>
                </li>
              ))}
            </ul>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
}

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
                    {item.children ? (
                      <NavSection item={item} pathname={pathname} onNavigate={onNavigate} />
                    ) : (
                      <Link href={item.href} onClick={onNavigate} className={`${ITEM} ${active ? ACTIVE : IDLE}`}>
                        {item.title}
                      </Link>
                    )}
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
              <li>
                <Link href="/vector/" onClick={onNavigate} className={`${ITEM} ${IDLE} items-center gap-2.5`}>
                  <SpaceIcon space="vector" size={16} />
                  <span>{VECTOR.name}</span>
                  <span className="leader" />
                  <span className="eyebrow">{VECTOR.peer}</span>
                </Link>
              </li>
            </ul>
          </div>
        )}
      </div>
    </nav>
  );
}
