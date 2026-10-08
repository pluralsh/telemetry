"use client";

import * as Dropdown from "@radix-ui/react-dropdown-menu";
import type { BenchRun } from "@/lib/benchmark-types";
import { fmtDate } from "./format";

export function StatusDot({ status }: { status: BenchRun["status"] }) {
  return (
    <span
      aria-label={status}
      className={`inline-block h-2 w-2 shrink-0 ${status === "passed" ? "bg-[#1d7a55] dark:bg-[#5cc79b]" : "bg-[#b42f2f] dark:bg-[#f08a8a]"}`}
    />
  );
}

export function RunPicker({ runs, value, onChange }: { runs: BenchRun[]; value: BenchRun; onChange: (name: string) => void }) {
  return (
    <Dropdown.Root>
      <Dropdown.Trigger className="btn btn-secondary btn-sm max-w-full justify-start gap-2.5 font-mono !text-[12px]">
        <StatusDot status={value.status} />
        <span className="truncate">
          {fmtDate(value.startedAt)} · {value.commit}
        </span>
        {value.name === runs[0].name && <span className="kbd !h-[18px]">latest</span>}
        <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden className="shrink-0">
          <path d="M2 4l3 3 3-3" stroke="currentColor" fill="none" strokeWidth="1.2" />
        </svg>
      </Dropdown.Trigger>
      <Dropdown.Portal>
        <Dropdown.Content
          align="start"
          sideOffset={6}
          className="z-50 w-[min(560px,calc(100vw-2rem))] rounded-sm border border-line-strong bg-panel p-1 shadow-[0_8px_24px_-12px_rgba(0,0,0,0.25)]"
        >
          <div className="eyebrow px-2 pb-1 pt-1.5">{runs.length} recorded runs, newest first</div>
          {runs.map((r, i) => (
            <Dropdown.Item
              key={r.name}
              onSelect={() => onChange(r.name)}
              className={`grid cursor-pointer grid-cols-[8px_150px_84px_1fr] items-center gap-3 rounded-[2px] px-2 py-1.5 font-mono text-[11.5px] outline-none data-[highlighted]:bg-bg-subtle ${
                r.name === value.name ? "text-ink" : "text-fg"
              }`}
            >
              <StatusDot status={r.status} />
              <span>{fmtDate(r.startedAt)}</span>
              <span className="text-muted">
                {r.commit}
                {r.dirty ? "*" : ""}
              </span>
              <span className="flex items-center gap-2 truncate text-muted">
                <span className="truncate">{r.track}</span>
                {i === 0 && <span className="kbd !h-[16px] !text-[10px]">latest</span>}
              </span>
            </Dropdown.Item>
          ))}
        </Dropdown.Content>
      </Dropdown.Portal>
    </Dropdown.Root>
  );
}
