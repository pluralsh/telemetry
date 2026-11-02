import Link from "next/link";
import { InlineMd } from "@/components/inline-md";
import { operatorKindHref } from "@/lib/nav";
import { typeAnchor, type CelRule, type OperatorField, type OperatorKind, type OperatorType } from "@/lib/operator-api";

/** A Go doc comment: paragraphs split by blank lines, with `-` or `*` lines as lists. */
export function GoDoc({ text, className = "" }: { text: string; className?: string }) {
  const blocks: { list: boolean; lines: string[] }[] = [];
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    const item = /^[-*]\s+/.test(line);
    const last = blocks.at(-1);
    if (!line) {
      if (last?.lines.length) blocks.push({ list: false, lines: [] });
    } else if (item) {
      if (last?.list) last.lines.push(line.replace(/^[-*]\s+/, ""));
      else blocks.push({ list: true, lines: [line.replace(/^[-*]\s+/, "")] });
    } else if (last && !last.list) {
      last.lines.push(line);
    } else if (last?.list && /^\s{2,}/.test(raw)) {
      last.lines[last.lines.length - 1] += ` ${line}`;
    } else {
      blocks.push({ list: false, lines: [line] });
    }
  }
  const nonEmpty = blocks.filter((b) => b.lines.length);
  if (!nonEmpty.length) return null;
  return (
    <div className={`flex flex-col gap-2 text-[14px] leading-[1.65] text-fg ${className}`}>
      {nonEmpty.map((b, i) =>
        b.list ? (
          <ul key={i} className="ml-4 list-disc marker:text-faint">
            {b.lines.map((l, j) => (
              <li key={j}>
                <InlineMd text={l} />
              </li>
            ))}
          </ul>
        ) : (
          <p key={i}>
            <InlineMd text={b.lines.join(" ")} />
          </p>
        ),
      )}
    </div>
  );
}

function Rules({ rules }: { rules: CelRule[] }) {
  if (!rules.length) return null;
  return (
    <ul className="mt-2 flex flex-col gap-1.5">
      {rules.map((r) => (
        <li key={r.rule} className="rounded-sm border border-line bg-bg-subtle px-2.5 py-1.5 text-[12.5px] leading-[1.5]">
          <span className="text-ink">{r.message || "Validated by a CEL rule"}</span>
          <code className="mt-0.5 block break-words font-mono text-[11.5px] text-muted">{r.rule}</code>
        </li>
      ))}
    </ul>
  );
}

function Validation({ items }: { items: string[] }) {
  if (!items.length) return null;
  return (
    <div className="mt-1.5 flex flex-wrap items-center gap-1.5 text-[12px] text-muted">
      {items.map((v) => (
        <span key={v} className="rounded-[2px] border border-line px-1.5 py-px font-mono text-[11px]">
          <InlineMd text={v} />
        </span>
      ))}
    </div>
  );
}

const METADATA_DOC = "Standard object metadata. See the Kubernetes API reference for its fields.";

function FieldItem({ field }: { field: OperatorField }) {
  const doc = field.doc || (field.name === "metadata" ? METADATA_DOC : "");
  return (
    <li className="border-t border-line py-3.5 first:border-t-0">
      <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
        <code className="font-mono text-[13.5px] font-medium text-ink">{field.name}</code>
        <InlineMd text={field.type} className="font-mono text-[12px] text-muted" />
        {field.default && (
          <span className="font-mono text-[11.5px] text-muted">
            default <code className="text-ink">{field.default}</code>
          </span>
        )}
      </div>
      {doc && <GoDoc text={doc} className="mt-1 !text-[13.5px]" />}
      <Validation items={field.validation} />
      <Rules rules={field.rules} />
    </li>
  );
}

function AppearsIn({ names, onPage }: { names: string[]; onPage: Set<string> }) {
  const here = names.filter((n) => onPage.has(n));
  if (!here.length) return null;
  return (
    <p className="text-[13px] text-muted">
      Used by{" "}
      {here.map((n, i) => (
        <span key={n}>
          {i > 0 && ", "}
          <a href={`#${typeAnchor(n)}`} className="font-mono text-[12.5px] text-accent-text hover:underline">
            {n}
          </a>
        </span>
      ))}
    </p>
  );
}

/** One API type: its doc, constraints and fields, headed for the page's "On this page" rail. */
export function TypeSection({
  type,
  groupVersion,
  onPage,
}: {
  type: OperatorType;
  groupVersion: string;
  onPage: Set<string>;
}) {
  const fields: OperatorField[] = type.kind
    ? [
        { name: "apiVersion", type: "string", doc: `\`${groupVersion}\``, default: "", validation: [], rules: [] },
        { name: "kind", type: "string", doc: `\`${type.kind}\``, default: "", validation: [], rules: [] },
        ...type.fields,
      ]
    : type.fields;
  // A kind's first paragraph is the page's lede.
  const doc = type.kind ? type.doc.split(/\n\s*\n/).slice(1).join("\n\n") : type.doc;
  return (
    <section className="[contain-intrinsic-size:auto_480px] [content-visibility:auto]">
      <h2 id={typeAnchor(type.name)}>{type.name}</h2>
      <div className="not-prose flex flex-col gap-2">
        <AppearsIn names={type.appearsIn} onPage={onPage} />
        {doc && <GoDoc text={doc} />}
        {type.underlying && (
          <p className="text-[13px] text-muted">
            Underlying type <InlineMd text={type.underlying} className="font-mono text-[12.5px] text-ink" />
          </p>
        )}
        <Validation items={type.validation} />
        <Rules rules={type.rules} />
        {type.enumValues.length > 0 && (
          <ul className="mt-1 rounded-sm border border-line px-3">
            {type.enumValues.map((v) => (
              <li key={v.name} className="border-t border-line py-2.5 first:border-t-0">
                <code className="font-mono text-[13px] text-ink">{v.name}</code>
                {v.doc && <GoDoc text={v.doc} className="mt-0.5 !text-[13.5px]" />}
              </li>
            ))}
          </ul>
        )}
        {fields.length > 0 && (
          <ul className="mt-1 rounded-sm border border-line px-3.5">
            {fields.map((f) => (
              <FieldItem key={f.name} field={f} />
            ))}
          </ul>
        )}
      </div>
    </section>
  );
}

export function KindFacts({ kind, groupVersion }: { kind: OperatorKind; groupVersion: string }) {
  const facts: [string, string][] = [
    ["API version", groupVersion],
    ["Kind", kind.kind],
    ["Scope", kind.scope],
    ...(kind.shortNames.length ? ([["Short names", kind.shortNames.join(", ")]] as [string, string][]) : []),
  ];
  return (
    <dl className="not-prose grid grid-cols-[auto_minmax(0,1fr)] gap-x-6 gap-y-1.5 rounded-sm border border-line bg-panel px-4 py-3 text-[13.5px]">
      {facts.map(([k, v]) => (
        <div key={k} className="contents">
          <dt className="text-muted">{k}</dt>
          <dd className="font-mono text-[13px] text-ink">{v}</dd>
        </div>
      ))}
    </dl>
  );
}

export function KindLink({ kind }: { kind: string }) {
  return (
    <Link href={operatorKindHref(kind)} className="font-mono text-[13.5px]">
      {kind}
    </Link>
  );
}
