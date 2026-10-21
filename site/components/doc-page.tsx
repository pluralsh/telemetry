import { Breadcrumbs, PageFooter, PrevNext } from "./page-chrome";
import { Toc } from "./toc";

/**
 * Centered content column with breadcrumbs, prev/next and footer. `wide` is for
 * reference pages (API, benchmarks) that need room for tables and charts.
 */
export function PageFrame({ children, wide = false }: { children: React.ReactNode; wide?: boolean }) {
  return (
    <div
      className={`flex min-h-[calc(100vh-60px)] w-full min-w-0 flex-col pb-6 pt-9 ${
        wide ? "max-w-[1120px] min-[1588px]:max-w-[1280px]" : "max-w-[820px] min-[1588px]:max-w-[880px]"
      }`}
    >
      <Breadcrumbs />
      {children}
      <div className="mb-6 mt-16 border-t border-line pt-6">
        <PrevNext />
      </div>
      <PageFooter />
    </div>
  );
}

/** Prose page: article column plus a sticky "On this page" rail on wide screens. */
export function DocPage({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex items-start justify-center px-6 lg:px-8 min-[1588px]:px-16">
      <PageFrame>
        <article className="prose-docs">{children}</article>
      </PageFrame>
      <aside className="thin-scroll sticky top-[60px] ml-8 hidden max-h-[calc(100vh-60px)] w-[220px] shrink-0 overflow-y-auto xl:block">
        <Toc />
      </aside>
    </div>
  );
}

export function PageHeader({ title, lede }: { title: string; lede?: React.ReactNode }) {
  return (
    <header className="not-prose mb-8 flex flex-col gap-2.5 border-b border-line pb-[18px]">
      <h1 className="font-serif text-[clamp(36px,4.4vw,52px)] font-normal leading-[1.05] tracking-[-0.02em] text-ink">{title}</h1>
      {lede && <p className="text-[18px] leading-[1.5] text-muted">{lede}</p>}
    </header>
  );
}
