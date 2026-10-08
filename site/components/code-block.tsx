import { highlight } from "@/lib/highlight";
import { CopyButton } from "./copy-button";

export async function CodeBlock({
  code,
  lang = "text",
  title,
  className = "",
}: {
  code: string;
  lang?: string;
  title?: string;
  className?: string;
}) {
  const html = await highlight(code.replace(/\n$/, ""), lang);
  return (
    <div className={`code-block not-prose card group relative overflow-hidden bg-code ${className}`}>
      <div className="flex h-8 items-center justify-between border-b border-line pl-3 pr-1.5">
        <span className="font-mono text-[11px] text-muted">{title ?? lang}</span>
        <CopyButton text={code} />
      </div>
      <div className="thin-scroll" dangerouslySetInnerHTML={{ __html: html }} />
    </div>
  );
}
