"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { usePathname } from "next/navigation";
import { SpaceSwitcher } from "./space-switcher";
import { ThemeToggle } from "./theme-toggle";
import { Sidebar, type ApiNav } from "./sidebar";
import { GithubIcon, PluralLogo } from "./icons";
import { REPO_URL } from "@/lib/nav";

export function DocsShell({ apiNav, children }: { apiNav: ApiNav; children: React.ReactNode }) {
  const [open, setOpen] = useState(false);
  const pathname = usePathname();
  useEffect(() => setOpen(false), [pathname]);

  return (
    <div className="relative z-[1] min-h-screen">
      <header className="sticky top-0 z-40 flex h-[60px] items-center justify-between gap-4 border-b border-line bg-[color-mix(in_srgb,var(--bg)_88%,transparent)] px-[clamp(16px,3vw,28px)] backdrop-blur-[12px]">
        <div className="flex min-w-0 items-center gap-6">
          <Link href="/" aria-label="Plural Telemetry home" className="flex shrink-0 items-center gap-2.5 text-ink">
            <PluralLogo height={22} />
            <span className="eyebrow relative top-px">telemetry</span>
          </Link>
          <SpaceSwitcher />
        </div>

        <nav aria-label="Site" className="flex items-center gap-2">
          <a
            href="https://plural.sh"
            className="hidden rounded-[2px] px-2.5 py-2 text-[14px] leading-[1.4] text-ink transition-colors hover:bg-bg-subtle xl:flex"
          >
            plural.sh
          </a>
          <a href={REPO_URL} aria-label="GitHub repository" className="btn btn-icon">
            <GithubIcon size={16} />
          </a>
          <ThemeToggle />
          <Link href="/installation/" className="btn btn-primary ml-1 hidden lg:inline-flex">
            Get started
          </Link>
          <button
            type="button"
            className="btn btn-icon lg:hidden"
            aria-label={open ? "Close menu" : "Open menu"}
            aria-expanded={open}
            onClick={() => setOpen((o) => !o)}
          >
            <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden>
              {open ? (
                <path d="M3.5 3.5l9 9M12.5 3.5l-9 9" stroke="currentColor" strokeWidth="1.4" />
              ) : (
                <path d="M2 4h12M2 8h12M2 12h12" stroke="currentColor" strokeWidth="1.4" />
              )}
            </svg>
          </button>
        </nav>
      </header>

      <div className="grid min-h-[calc(100vh-60px)] grid-cols-[minmax(0,1fr)] lg:grid-cols-[272px_minmax(0,1fr)]">
        <aside className="hidden min-w-0 bg-sidebar lg:block">
          <div className="thin-scroll sticky top-[60px] h-[calc(100vh-60px)] overflow-y-auto border-r border-line">
            <Sidebar apiNav={apiNav} />
          </div>
        </aside>
        {open && (
          <div className="fixed inset-0 top-[60px] z-30 lg:hidden">
            <div className="absolute inset-0 bg-black/25" onClick={() => setOpen(false)} />
            <aside className="thin-scroll absolute inset-y-0 left-0 w-[288px] overflow-y-auto border-r border-line bg-sidebar">
              <Sidebar apiNav={apiNav} onNavigate={() => setOpen(false)} />
            </aside>
          </div>
        )}
        <main className="min-w-0">{children}</main>
      </div>
    </div>
  );
}
