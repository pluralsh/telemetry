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

export function Sidebar({ apiNav, onNavigate }: { apiNav: ApiNav; onNavigate?: () => void }) {
  const pathname = norm(usePathname());
  const space = spaceFromPath(pathname);
  const groups = navFor(space);

  return (
    <nav className="flex flex-col text-[13.5px] tracking-[-0.2px]">
      <div className="flex items-center gap-3 border-b border-line px-5 py-5">
        <SpaceIcon space={space} size={28} />
        <div className="min-w-0">
          <div className="font-serif text-[17px] leading-tight text-ink">
            {space === "overview" ? "Plural Telemetry" : PRODUCTS[space].name}
          </div>
          <div className="eyebrow mt-0.5 truncate">
            {space === "overview" ? "Documentation" : `${PRODUCTS[space].peerLong}-compatible`}
          </div>
        </div>
      </div>

      <div className="flex flex-col gap-7 px-5 py-6">
        {groups.map((g) => (
          <div key={g.title}>
            <div className="mb-2 font-serif text-[13.5px] text-ink">{g.title}</div>
            <ul className="flex flex-col border-l border-line">
              {g.items.map((item) => {
                const active = norm(item.href) === pathname;
                const isApi = space !== "overview" && item.href.endsWith("/api/");
                return (
                  <li key={item.href}>
                    <Link
                      href={item.href}
                      onClick={onNavigate}
                      className={`-ml-px block border-l py-[5px] pl-3.5 transition-colors ${
                        active
                          ? "border-accent-text text-accent-text"
                          : "border-transparent text-fg hover:border-line-strong hover:text-ink"
                      }`}
                    >
                      {item.title}
                    </Link>
                    {isApi && active && (
                      <div className="mb-1 ml-3.5 mt-1 flex flex-col gap-3">
                        {apiNav[space].map((t) => (
                          <div key={t.tag}>
                            <a
                              href={`#tag-${t.tag}`}
                              onClick={onNavigate}
                              className="eyebrow block py-1 capitalize hover:text-ink"
                            >
                              {t.tag}
                            </a>
                            {t.ops.map((op) => (
                              <a
                                key={op.anchor}
                                href={`#${op.anchor}`}
                                onClick={onNavigate}
                                className="flex items-center gap-2 py-[3px] text-[12.5px] text-muted hover:text-ink"
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
            <div className="mb-2 font-serif text-[13.5px] text-ink">Databases</div>
            <ul className="flex flex-col">
              {(Object.keys(PRODUCTS) as ProductId[]).map((id) => (
                <li key={id}>
                  <Link
                    href={`/${id}/`}
                    onClick={onNavigate}
                    className="group flex items-center gap-2.5 py-[5px] text-fg hover:text-ink"
                  >
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
