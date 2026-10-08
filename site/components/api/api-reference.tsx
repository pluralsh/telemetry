import Link from "next/link";
import { PRODUCTS, type ProductId } from "@/lib/nav";
import type { ApiSpec } from "@/lib/openapi";
import { serviceUrl } from "@/lib/api-examples";
import { CodeBlock } from "../code-block";
import { InlineMd } from "../inline-md";
import { MethodBadge } from "../method-badge";
import { SpaceIcon } from "../icons";
import { Endpoint, EndpointPath } from "./endpoint";

const ERROR_EXAMPLES: Record<ProductId, string> = {
  metrics: JSON.stringify({ status: "error", errorType: "bad_data", error: 'parse error: unexpected "}" in label matching' }, null, 2),
  logs: JSON.stringify({ error: 'parse error at line 1, col 18: syntax error: unexpected "}"' }, null, 2),
  traces: JSON.stringify({ error: "invalid TraceQL query: unexpected token at 1:12" }, null, 2),
};

const EXTRA_PORTS: Partial<Record<ProductId, string>> = {
  traces: "OTLP/gRPC is served on `4317` and Jaeger gRPC on `14250` on every writer, outside this HTTP API.",
};

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="border-t border-line py-6 first:border-t-0 first:pt-0">
      <h3 className="mb-2 font-serif text-[15px] text-ink">{title}</h3>
      <div className="text-[14px] leading-[1.7] text-fg">{children}</div>
    </div>
  );
}

export async function ApiReference({ spec }: { spec: ApiSpec }) {
  const product = PRODUCTS[spec.product];
  const allOps = spec.tags.flatMap((t) => t.ops);

  return (
    <div className="mx-auto max-w-[1180px] px-6 pb-24 pt-12 sm:px-10 lg:pt-16">
      <header className="mb-12 max-w-[720px]">
        <div className="mb-5 flex items-center gap-2.5">
          <SpaceIcon space={spec.product} size={28} />
          <span className="eyebrow">API reference · v{spec.version}</span>
        </div>
        <h1 className="font-serif text-[2.6rem] font-light leading-[1.12] tracking-[-0.02em] text-ink">{product.name} HTTP API</h1>
        <p className="mt-4 text-[16px] leading-[1.6] tracking-[-0.4px] text-fg">
          {spec.description} Generated from the OpenAPI 3.1 document shipped with the server.
        </p>
      </header>

      <div className="grid gap-x-12 gap-y-8 border-b border-line pb-14 xl:grid-cols-[minmax(0,1fr)_minmax(0,440px)]">
        <div className="min-w-0">
          <Section title="Base URLs">
            Writers serve every <code className="font-mono text-[13px] text-ink">/write</code> route and readers serve every{" "}
            <code className="font-mono text-[13px] text-ink">/read</code> route. The operator creates a Service for each, named after the{" "}
            <code className="font-mono text-[13px] text-ink">{product.name}</code> resource. Any writer accepts any write and forwards it to the
            owning shard, so plain round-robin load balancing is enough.{" "}
            {EXTRA_PORTS[spec.product] && <InlineMd text={EXTRA_PORTS[spec.product]} />}
          </Section>
          <Section title="Authentication">
            Requests use HTTP basic auth. Each <Link href={`/${spec.product}/installation/#access`} className="text-accent-text underline decoration-line-strong underline-offset-2">NamespaceAuthentication</Link>{" "}
            grants one username <code className="font-mono text-[13px] text-ink">read</code> or{" "}
            <code className="font-mono text-[13px] text-ink">write</code> on one namespace, with the password held in a Secret. Probes and
            self-metrics are unauthenticated.
          </Section>
          <Section title="Namespaces">
            Data is partitioned into tenant namespaces, and every data route carries one as{" "}
            <code className="font-mono text-[13px] text-ink">/read/ns/{"{namespace}"}</code> or{" "}
            <code className="font-mono text-[13px] text-ink">/write/ns/{"{namespace}"}</code>. A {product.grafana} data source pointed at{" "}
            <code className="font-mono text-[13px] text-ink">{serviceUrl(spec.product, "reader")}/read/ns/default</code> works unchanged.
          </Section>
          <Section title="Errors and backpressure">
            Errors use the {spec.product === "metrics" ? "Prometheus" : product.peer} error envelope with a 4xx or 5xx status. When object
            storage falls behind, writers reject new batches with <code className="font-mono text-[13px] text-ink">429</code> and a{" "}
            <code className="font-mono text-[13px] text-ink">Retry-After</code> header; configure your agent to retry them.
          </Section>
        </div>

        <div className="flex min-w-0 flex-col gap-4 xl:self-start">
          <div className="card overflow-hidden">
            <div className="flex h-8 items-center border-b border-line px-3 font-mono text-[11px] text-muted">Base URLs</div>
            <dl className="px-3 py-2 font-mono text-[12px]">
              {(["writer", "reader"] as const).map((role) => (
                <div key={role} className="flex items-center gap-3 py-1">
                  <dt className="w-12 text-muted">{role}</dt>
                  <dd className="truncate text-ink">{serviceUrl(spec.product, role)}</dd>
                </div>
              ))}
            </dl>
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
                    <EndpointPath path={op.path.replace("/ns/{namespace}", "/ns/…")} className="!text-[11.5px] !text-muted" />
                  </a>
                </li>
              ))}
            </ul>
          </div>
          <CodeBlock code={ERROR_EXAMPLES[spec.product]} lang="json" title="Error · 400" />
        </div>
      </div>

      {spec.tags.map((tag) => (
        <div key={tag.name} id={`tag-${tag.name}`} className="scroll-mt-[72px]">
          <div className="relative mt-16 border-b border-line-strong pb-4">
            <span className="eyebrow">Resource</span>
            <h2 className="mt-1 font-serif text-[2rem] font-light capitalize tracking-[-0.015em] text-ink">{tag.name}</h2>
            {tag.description && <InlineMd text={tag.description} className="mt-2 block max-w-[720px] text-[14.5px] leading-[1.7] text-fg" />}
          </div>
          {tag.ops.map((op) => (
            <Endpoint key={op.anchor} product={spec.product} op={op} />
          ))}
        </div>
      ))}
    </div>
  );
}
