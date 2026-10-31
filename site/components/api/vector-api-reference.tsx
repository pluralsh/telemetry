import Link from "next/link";
import type { ApiSpec } from "@/lib/openapi";
import { VECTOR_BASE_URL, VECTOR_ERROR_EXAMPLE } from "@/lib/vector-api";
import { CodeBlock } from "../code-block";
import { InlineMd } from "../inline-md";
import { MethodBadge } from "../method-badge";
import { PageFrame, PageHeader } from "../doc-page";
import { Endpoint, EndpointPath } from "./endpoint";

const STATUSES: [string, string][] = [
  ["400", "Malformed JSON, unknown fields, type mismatches, invalid filters or rankings"],
  ["404", "Unknown namespace, row, or shard"],
  ["409", "Namespace exists, or the shard can't split"],
  ["500", "Storage or internal failure; safe to retry"],
];

const code = "font-mono text-[13px] text-ink";

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="border-t border-line py-6 first:border-t-0 first:pt-0">
      <h3 className="mb-2 text-[16px] font-medium text-ink">{title}</h3>
      <div className="text-[14px] leading-[1.7] text-fg">{children}</div>
    </div>
  );
}

export async function VectorApiReference({ spec }: { spec: ApiSpec<"vector"> }) {
  const allOps = spec.tags.flatMap((t) => t.ops);

  return (
    <div className="flex justify-center px-6 lg:px-8 min-[1588px]:px-16">
      <PageFrame wide>
        <PageHeader
          title="Vector HTTP API"
          lede={<>{spec.description} Generated from the OpenAPI 3.1 document (v{spec.version}) shipped with the server.</>}
        />

        <div className="grid gap-x-12 gap-y-8 border-b border-line pb-14 xl:grid-cols-[minmax(0,1fr)_minmax(0,440px)]">
          <div className="min-w-0">
            <Section title="Base URL">
              <code className={code}>plural-vector cluster --config cluster.yaml</code> serves every route on one port (
              <code className={code}>--port</code>, default <code className={code}>8080</code>). Any server process owns the whole
              cluster, so run one per storage path. See the <Link href="/vector/installation/standalone/#as-a-process" className="text-accent-text underline decoration-line-strong underline-offset-2">standalone guide</Link>{" "}
              for the configuration file.
            </Section>
            <Section title="Requests">
              Bodies are JSON. Unknown fields are rejected rather than ignored, so typos surface as{" "}
              <code className={code}>400</code>. Row ids are strings or non-negative integers of up to 256 bytes; integers are stored
              as their decimal string.
            </Section>
            <Section title="Consistency">
              Writes return once applied in memory. Queries and row reads are strongly consistent by default: they first publish
              every acknowledged write, so they see their own writes. Pass{" "}
              <code className={code}>{'"consistency": {"level": "eventual"}'}</code> to skip that step and read the last flushed
              snapshot. Data is durable on object storage within the flush interval (1 second by default).
            </Section>
            <Section title="Errors">
              Errors carry a <code className={code}>status</code> of <code className={code}>error</code> and a message:
              <ul className="mt-2 flex flex-col gap-1">
                {STATUSES.map(([status, text]) => (
                  <li key={status} className="flex gap-3">
                    <code className="w-9 shrink-0 font-mono text-[13px] text-ink">{status}</code>
                    <span>{text}</span>
                  </li>
                ))}
              </ul>
            </Section>
          </div>

          <div className="flex min-w-0 flex-col gap-4 xl:self-start">
            <div className="card overflow-hidden">
              <div className="flex h-8 items-center border-b border-line px-3 font-mono text-[11px] text-muted">Base URL</div>
              <div className="truncate px-3 py-2 font-mono text-[12px] text-ink">{VECTOR_BASE_URL}</div>
            </div>
            <div className="card overflow-hidden">
              <div className="flex h-8 items-center justify-between border-b border-line px-3 font-mono text-[11px] text-muted">
                <span>Endpoints</span>
                <span>{allOps.length}</span>
              </div>
              <ul className="thin-scroll max-h-[340px] overflow-y-auto py-1.5">
                {allOps.map((op) => (
                  <li key={op.anchor}>
                    <a href={`#${op.anchor}`} className="flex items-center gap-2.5 px-3 py-[3px] hover:bg-bg-subtle">
                      <MethodBadge method={op.method} compact />
                      <EndpointPath path={op.path.replace("/namespaces/{namespace}", "/namespaces/…")} className="!text-[11.5px] !text-muted" />
                    </a>
                  </li>
                ))}
              </ul>
            </div>
            <CodeBlock code={VECTOR_ERROR_EXAMPLE} lang="json" title="Error · 400" />
          </div>
        </div>

        {spec.tags.map((tag) => (
          <div key={tag.name} id={`tag-${tag.name}`} className="scroll-mt-[72px]">
            <div className="relative mt-16 border-b border-line-strong pb-4">
              <span className="label">Resource</span>
              <h2 className="mt-1 text-[24px] font-medium capitalize leading-[1.3] tracking-[-0.005em] text-ink">{tag.name}</h2>
              {tag.description && <InlineMd text={tag.description} className="mt-2 block max-w-[720px] text-[14.5px] leading-[1.7] text-fg" />}
            </div>
            {tag.ops.map((op) => (
              <Endpoint key={op.anchor} product="vector" op={op} />
            ))}
          </div>
        ))}
      </PageFrame>
    </div>
  );
}
