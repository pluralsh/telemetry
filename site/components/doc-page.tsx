import { Toc } from "./toc";

/** Prose page frame: article column plus an "On this page" rail behind a vertical rule. */
export function DocPage({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex min-h-[calc(100vh-57px)]">
      <div className="min-w-0 flex-1">
        <article className="prose-docs mx-auto max-w-[800px] px-6 pb-24 pt-12 sm:px-10 lg:pt-16">{children}</article>
      </div>
      <div className="hidden w-60 shrink-0 border-l border-line xl:block">
        <div className="sticky top-[57px] px-6 py-12 lg:py-16">
          <Toc />
        </div>
      </div>
    </div>
  );
}

export function PageHeader({
  eyebrow,
  title,
  lede,
  icon,
}: {
  eyebrow: string;
  title: string;
  lede?: React.ReactNode;
  icon?: React.ReactNode;
}) {
  return (
    <header className="not-prose mb-12">
      <div className="mb-5 flex items-center gap-2.5">
        {icon}
        <span className="eyebrow">{eyebrow}</span>
      </div>
      <h1 className="font-serif text-[2.6rem] font-light leading-[1.12] tracking-[-0.02em] text-ink">{title}</h1>
      {lede && <p className="mt-4 max-w-[620px] text-[16px] leading-[1.6] tracking-[-0.4px] text-fg">{lede}</p>}
    </header>
  );
}
