"use client";

import Link from "next/link";
import { Fragment } from "react";
import { usePathname } from "next/navigation";
import { PRODUCTS, REPO_URL, navFor, spaceFromPath } from "@/lib/nav";

function norm(p: string) {
  return p.endsWith("/") ? p : p + "/";
}

type Crumb = { label: string; href?: string };

function crumbsFor(pathname: string): Crumb[] {
  const space = spaceFromPath(pathname);
  const crumbs: Crumb[] = [{ label: "Docs", href: "/" }];
  if (space !== "overview") crumbs.push({ label: PRODUCTS[space].name, href: `/${space}/` });
  for (const g of navFor(space)) {
    const item = g.items.find((i) => norm(i.href) === pathname);
    if (item) {
      crumbs.push({ label: g.title }, { label: item.title });
      break;
    }
  }
  return crumbs;
}

export function Breadcrumbs() {
  const crumbs = crumbsFor(norm(usePathname()));
  if (crumbs.length <= 1) return null;

  return (
    <nav aria-label="Breadcrumb" className="eyebrow mb-5">
      <ol className="flex flex-wrap items-center gap-1.5">
        {crumbs.map((c, i) => {
          const last = i === crumbs.length - 1;
          return (
            <Fragment key={`${c.label}-${i}`}>
              <li>
                {last || !c.href ? (
                  <span aria-current={last ? "page" : undefined} className={last ? "text-ink" : undefined}>
                    {c.label}
                  </span>
                ) : (
                  <Link href={c.href} className="transition-colors hover:text-accent-text">
                    {c.label}
                  </Link>
                )}
              </li>
              {!last && (
                <li aria-hidden className="text-faint">
                  /
                </li>
              )}
            </Fragment>
          );
        })}
      </ol>
    </nav>
  );
}

/** Previous/next walk the sidebar in reading order within the current space. */
export function PrevNext() {
  const pathname = norm(usePathname());
  const flat = navFor(spaceFromPath(pathname)).flatMap((g) => g.items);
  const idx = flat.findIndex((i) => norm(i.href) === pathname);
  if (idx < 0) return null;
  const prev = flat[idx - 1];
  const next = flat[idx + 1];
  if (!prev && !next) return null;

  const card =
    "flex flex-col gap-0.5 rounded-[4px] border border-line-strong bg-panel px-4 py-3.5 transition-colors hover:border-accent-ring [&:hover_.t]:text-accent-text";

  return (
    <nav aria-label="Previous and next pages" className="grid grid-cols-[repeat(auto-fit,minmax(220px,1fr))] gap-3">
      {prev ? (
        <Link href={prev.href} className={card}>
          <span className="eyebrow text-[11px]">← Previous</span>
          <span className="t text-[15px] font-medium text-ink transition-colors">{prev.title}</span>
        </Link>
      ) : (
        <span />
      )}
      {next && (
        <Link href={next.href} className={`${card} text-right`}>
          <span className="eyebrow text-[11px]">Next →</span>
          <span className="t text-[15px] font-medium text-ink transition-colors">{next.title}</span>
        </Link>
      )}
    </nav>
  );
}

export function PageFooter() {
  const link = "transition-colors hover:text-ink";
  return (
    <footer className="eyebrow mt-auto flex flex-wrap gap-6 border-t border-line pt-5">
      <span>© Plural {new Date().getFullYear()}</span>
      <a href="https://plural.sh" className={link}>
        plural.sh
      </a>
      <a href={REPO_URL} className={link}>
        GitHub
      </a>
      <a href={`${REPO_URL}/blob/main/LICENSE`} className={link}>
        License
      </a>
    </footer>
  );
}
