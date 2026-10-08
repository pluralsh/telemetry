const STYLES: Record<string, string> = {
  GET: "text-[#1d7a55] border-[#1d7a55]/30 dark:text-[#5cc79b] dark:border-[#5cc79b]/30",
  POST: "text-[#0751cf] border-[#0751cf]/30 dark:text-[#9fc3f5] dark:border-[#9fc3f5]/30",
  PUT: "text-[#a8550f] border-[#a8550f]/30 dark:text-[#ef9f58] dark:border-[#ef9f58]/30",
  DELETE: "text-[#b42f2f] border-[#b42f2f]/30 dark:text-[#f08a8a] dark:border-[#f08a8a]/30",
};

export function MethodBadge({ method, compact }: { method: string; compact?: boolean }) {
  const m = method.toUpperCase();
  return (
    <span
      className={`inline-flex shrink-0 items-center justify-center rounded-[2px] border font-mono ${
        compact ? "w-9 py-px text-[9.5px]" : "px-1.5 py-0.5 text-[11px]"
      } ${STYLES[m] ?? "border-line text-muted"}`}
    >
      {m}
    </span>
  );
}
