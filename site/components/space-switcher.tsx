"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import * as Dropdown from "@radix-ui/react-dropdown-menu";
import { motion } from "motion/react";
import { PRODUCTS, PRODUCT_IDS, spaceFromPath, type SpaceId } from "@/lib/nav";
import { SpaceIcon } from "./icons";

const SPACES: { id: SpaceId; label: string; blurb: string }[] = [
  { id: "overview", label: "Overview", blurb: "Project, install and concepts" },
  ...PRODUCT_IDS.map((id) => ({ id, label: PRODUCTS[id].name, blurb: `${PRODUCTS[id].peerLong}-compatible` })),
];

/** Keep the reader on the equivalent page when hopping between databases. */
function targetHref(current: string, from: SpaceId, to: SpaceId) {
  if (to === "overview") return "/";
  if (from === "overview") return `/${to}/`;
  const rest = current.split("/").filter(Boolean).slice(1).join("/");
  return `/${to}/${rest ? rest + "/" : ""}`;
}

export function SpaceSwitcher() {
  const pathname = usePathname();
  const router = useRouter();
  const active = spaceFromPath(pathname);
  const current = SPACES.find((s) => s.id === active)!;

  return (
    <>
      <div
        role="tablist"
        aria-label="Documentation space"
        className="relative hidden h-[34px] items-stretch rounded-sm border border-line-strong bg-card p-[2px] md:flex"
      >
        {SPACES.map((s, i) => {
          const on = s.id === active;
          return (
            <div key={s.id} className="flex items-stretch">
              {i > 0 && <span aria-hidden className="my-1.5 w-px bg-line" />}
              <Link
                role="tab"
                aria-selected={on}
                href={targetHref(pathname, active, s.id)}
                className={`relative flex items-center gap-2 px-3 text-[13px] tracking-[-0.3px] transition-colors ${
                  on ? "text-ink" : "text-muted hover:text-ink"
                }`}
              >
                {on && (
                  <motion.span
                    layoutId="space-tab"
                    className="absolute inset-0 rounded-[2px] border border-line-strong bg-panel shadow-[inset_0_-2px_0_0_var(--edge)]"
                    transition={{ type: "spring", stiffness: 520, damping: 40 }}
                  />
                )}
                <span className="relative">
                  <SpaceIcon space={s.id} size={15} />
                </span>
                <span className="relative">{s.label}</span>
              </Link>
            </div>
          );
        })}
      </div>

      <Dropdown.Root>
        <Dropdown.Trigger className="btn btn-secondary btn-sm md:hidden">
          <SpaceIcon space={current.id} size={15} />
          {current.label}
          <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden>
            <path d="M2 4l3 3 3-3" stroke="currentColor" fill="none" strokeWidth="1.2" />
          </svg>
        </Dropdown.Trigger>
        <Dropdown.Portal>
          <Dropdown.Content
            align="start"
            sideOffset={6}
            className="z-50 w-max min-w-72 max-w-[calc(100vw-1.5rem)] rounded-sm border border-line-strong bg-panel p-1 shadow-[0_8px_24px_-12px_rgba(0,0,0,0.25)]"
          >
            {SPACES.map((s) => (
              <Dropdown.Item
                key={s.id}
                onSelect={() => router.push(targetHref(pathname, active, s.id))}
                className="flex cursor-pointer items-center gap-3 rounded-[2px] px-2 py-2 outline-none data-[highlighted]:bg-bg-subtle"
              >
                <SpaceIcon space={s.id} size={20} className="shrink-0" />
                <span className="w-16 shrink-0 whitespace-nowrap text-[13px] text-ink">{s.label}</span>
                <span className="leader" />
                <span className="eyebrow whitespace-nowrap">{s.blurb}</span>
              </Dropdown.Item>
            ))}
          </Dropdown.Content>
        </Dropdown.Portal>
      </Dropdown.Root>
    </>
  );
}
