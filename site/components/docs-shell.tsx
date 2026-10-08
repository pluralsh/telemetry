"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { usePathname } from "next/navigation";
import { SpaceSwitcher } from "./space-switcher";
import { ThemeToggle } from "./theme-toggle";
import { Sidebar, type ApiNav } from "./sidebar";
import { GithubIcon, PluralMark } from "./icons";
import { REPO_URL } from "@/lib/nav";

const SIDEBAR = 272;

const RELEASE = process.env.NEXT_PUBLIC_DOCS_VERSION ?? "";
const DOCS_VERSION = /^\d+\.\d+\.\d+/.test(RELEASE) ? `v${RELEASE}` : "dev";

function Node({ style }: { style: React.CSSProperties }) {
  return <span aria-hidden className="node hidden lg:block" style={style} />;
}

export function DocsShell({ apiNav, children }: { apiNav: ApiNav; children: React.ReactNode }) {
  const [open, setOpen] = useState(false);
  const pathname = usePathname();
  useEffect(() => setOpen(false), [pathname]);

  return (
    <div className="relative min-h-screen">
      {/* outer gutters with ruler ticks against the frame */}
      <span
        aria-hidden
        className="pointer-events-none absolute inset-y-0 left-0 hidden w-12 border-r border-line lg:block"
        style={{
          backgroundImage: "linear-gradient(var(--line-strong) 1px, transparent 1px)",
          backgroundSize: "7px 8px",
          backgroundRepeat: "repeat-y",
          backgroundPosition: "right top",
        }}
      />
      <span
        aria-hidden
        className="pointer-events-none absolute inset-y-0 right-0 hidden w-12 border-l border-line lg:block"
        style={{
          backgroundImage: "linear-gradient(var(--line-strong) 1px, transparent 1px)",
          backgroundSize: "7px 8px",
          backgroundRepeat: "repeat-y",
          backgroundPosition: "left top",
        }}
      />

      <header className="sticky top-0 z-40 h-[57px] border-b border-line bg-bg">
        <div className="relative flex h-full items-stretch lg:mx-12">
          <Node style={{ left: -4.5, bottom: -4.5 }} />
          <Node style={{ left: SIDEBAR - 4.5, bottom: -4.5 }} />
          <Node style={{ right: -4.5, bottom: -4.5 }} />

          <div className="flex items-center gap-2 px-3 lg:w-[272px] lg:shrink-0 lg:border-r lg:border-line lg:px-5">
            <button
              type="button"
              className="btn btn-ghost btn-sm -ml-1 px-2 lg:hidden"
              aria-label="Toggle navigation"
              onClick={() => setOpen((o) => !o)}
            >
              <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden>
                <path d="M1 3.5h12M1 7h12M1 10.5h12" stroke="currentColor" strokeWidth="1.25" />
              </svg>
            </button>
            <Link
              href="/"
              aria-label="Plural Telemetry home"
              className="flex items-center gap-2"
            >
              <PluralMark size={20} className="text-ink" />
              <span className="font-serif text-[18px] tracking-[-0.01em] text-ink">Plural Telemetry</span>
              <span className="hidden font-mono text-[10.5px] text-faint sm:inline">{DOCS_VERSION}</span>
            </Link>
          </div>

          <div className="flex min-w-0 flex-1 items-center gap-1 px-3 lg:px-4">
            <SpaceSwitcher />
            <div className="flex-1" />
            <a href="https://plural.sh" className="btn btn-ghost btn-sm hidden text-muted md:inline-flex">
              <span className="text-ink">
                <PluralMark size={13} />
              </span>
              <span>
                A <span className="text-ink">Plural</span> project
              </span>
            </a>
            <span aria-hidden className="mx-1 hidden h-4 w-px bg-line-strong md:block" />
            <a href={REPO_URL} aria-label="GitHub repository" className="btn btn-ghost btn-sm px-2">
              <GithubIcon size={15} />
            </a>
            <ThemeToggle />
          </div>
        </div>
      </header>

      <div className="flex lg:mx-12">
        <aside
          className="thin-scroll sticky top-[57px] hidden h-[calc(100vh-57px)] shrink-0 overflow-y-auto border-r border-line lg:block"
          style={{ width: SIDEBAR }}
        >
          <Sidebar apiNav={apiNav} />
        </aside>
        {open && (
          <div className="fixed inset-0 top-[57px] z-30 lg:hidden">
            <div className="absolute inset-0 bg-black/25" onClick={() => setOpen(false)} />
            <aside className="thin-scroll absolute inset-y-0 left-0 w-[288px] overflow-y-auto border-r border-line bg-bg">
              <Sidebar apiNav={apiNav} onNavigate={() => setOpen(false)} />
            </aside>
          </div>
        )}
        <main className="min-w-0 flex-1">{children}</main>
      </div>
    </div>
  );
}
