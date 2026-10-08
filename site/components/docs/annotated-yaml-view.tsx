"use client";

import { useRef, useState } from "react";
import { CopyButton } from "../copy-button";
import { InlineMd } from "../inline-md";

export type YamlNote = { n: number; line: number; key: string; path: string; text: string };

function Marker({ n, active }: { n: number; active: boolean }) {
  return (
    <span
      className={`inline-flex h-[15px] min-w-[15px] items-center justify-center border px-[3px] font-mono text-[9.5px] leading-none tabular-nums transition-colors ${
        active ? "border-accent-text bg-accent-text text-bg" : "border-line-strong bg-card text-muted"
      }`}
    >
      {n}
    </span>
  );
}

export function AnnotatedYamlView({ code, lines, notes, title }: { code: string; lines: string[]; notes: YamlNote[]; title?: string }) {
  const [active, setActive] = useState<number | null>(null);
  const notesRef = useRef<HTMLOListElement>(null);
  const byLine = new Map(notes.map((n) => [n.line, n]));

  const focusNote = (n: number | null) => {
    setActive(n);
    const list = notesRef.current;
    const item = n ? list?.querySelector<HTMLElement>(`[data-note="${n}"]`) : null;
    if (!list || !item || list.scrollHeight <= list.clientHeight) return;
    const top = item.offsetTop - list.offsetTop;
    if (top < list.scrollTop || top + item.offsetHeight > list.scrollTop + list.clientHeight) {
      list.scrollTop = top - 12;
    }
  };

  return (
    <div className="not-prose card my-8 overflow-hidden bg-code 2xl:-mx-10">
      <div className="flex h-8 items-center justify-between border-b border-line pl-3 pr-1.5">
        <span className="font-mono text-[11px] text-muted">{title ?? "yaml"}</span>
        <span className="flex items-center gap-3">
          <span className="hidden font-mono text-[11px] text-faint sm:inline">{notes.length} annotated fields</span>
          <CopyButton text={code} />
        </span>
      </div>
      <div className="grid md:grid-cols-[minmax(0,1fr)_minmax(0,290px)]">
        <pre className="thin-scroll overflow-x-auto py-3 font-mono text-[12.5px] leading-[1.7]" onMouseLeave={() => setActive(null)}>
          <code className="block min-w-max">
            {lines.map((html, i) => {
              const note = byLine.get(i);
              const on = note !== undefined && note.n === active;
              return (
                <span
                  key={i}
                  onMouseEnter={note ? () => focusNote(note.n) : undefined}
                  className={`flex pr-4 transition-colors ${on ? "bg-[color-mix(in_srgb,var(--accent-text)_9%,transparent)]" : ""} ${note ? "cursor-default" : ""}`}
                >
                  <span className="w-9 shrink-0 select-none pr-3 text-right text-faint tabular-nums">{i + 1}</span>
                  <span className="w-6 shrink-0 select-none">{note && <Marker n={note.n} active={on} />}</span>
                  <span className="shiki" dangerouslySetInnerHTML={{ __html: html || " " }} />
                </span>
              );
            })}
          </code>
        </pre>
        <div className="relative border-t border-line bg-panel md:border-l md:border-t-0">
          <ol
            ref={notesRef}
            className="thin-scroll max-h-[420px] overflow-y-auto md:absolute md:inset-0 md:max-h-none"
            onMouseLeave={() => setActive(null)}
          >
            {notes.map((note) => (
              <li
                key={note.n}
                data-note={note.n}
                onMouseEnter={() => setActive(note.n)}
                className={`border-b border-line px-4 py-3 text-[13px] leading-[1.55] transition-colors last:border-b-0 ${
                  note.n === active ? "bg-card" : ""
                }`}
              >
                <div className="mb-1 flex items-center gap-2">
                  <Marker n={note.n} active={note.n === active} />
                  <span className="font-mono text-[12px] text-ink" title={note.path}>
                    {note.key}
                  </span>
                </div>
                <InlineMd text={note.text} className="text-fg" />
              </li>
            ))}
          </ol>
        </div>
      </div>
    </div>
  );
}
