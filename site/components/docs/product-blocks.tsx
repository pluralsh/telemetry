import Link from "next/link";
import { CodeBlock } from "../code-block";
import { Figure } from "../diagrams/figure";
import { ArrowRight, SpaceIcon } from "../icons";
import { InlineMd } from "../inline-md";
import { AnnotatedYaml } from "./annotated-yaml";
import { INSTALL_NAMESPACE, serviceUrl } from "@/lib/api-examples";
import { PRODUCTS, type ProductId } from "@/lib/nav";
import {
  ACCESS_NOTES,
  BUCKET,
  INTEGRATIONS,
  KEY_LAYOUTS,
  TENANT,
  accessSpec,
  collectorConfig,
  grafanaDatasource,
  verifyCommands,
  productionSpec,
  quickstartSpec,
  specNotes,
  type KeyField,
} from "@/lib/product-docs";

export function InstallSpec({ product, variant }: { product: ProductId; variant: "quickstart" | "production" }) {
  const code = variant === "quickstart" ? quickstartSpec(product) : productionSpec(product);
  return <AnnotatedYaml code={code} notes={specNotes(product)} title={`${product}.yaml`} />;
}

export function AccessSpec({ product }: { product: ProductId }) {
  return <AnnotatedYaml code={accessSpec(product)} notes={ACCESS_NOTES} title="namespace-auth.yaml" />;
}

export function PodIdentitySetup({ product }: { product: ProductId }) {
  const policy = {
    Version: "2012-10-17",
    Statement: [
      { Effect: "Allow", Action: "s3:ListBucket", Resource: `arn:aws:s3:::${BUCKET}` },
      { Effect: "Allow", Action: ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], Resource: `arn:aws:s3:::${BUCKET}/${product}/*` },
    ],
  };
  return (
    <>
      <CodeBlock
        lang="bash"
        className="my-6"
        code={`aws eks create-pod-identity-association \\
  --cluster-name prod \\
  --namespace ${INSTALL_NAMESPACE} \\
  --service-account ${product} \\
  --role-arn arn:aws:iam::123456789012:role/plural-telemetry-${product}`}
      />
      <CodeBlock lang="json" title={`plural-telemetry-${product} policy`} className="my-6" code={JSON.stringify(policy, null, 2)} />
    </>
  );
}

export function PasswordSecrets({ product }: { product: ProductId }) {
  const cmd = (role: string) =>
    `kubectl -n ${INSTALL_NAMESPACE} create secret generic ${product}-${TENANT}-${role} \\\n  --from-literal=password="$(openssl rand -hex 24)"`;
  return <CodeBlock lang="bash" className="my-6" code={`${cmd("writer")}\n${cmd("reader")}`} />;
}

export function ClientConfigs({ product }: { product: ProductId }) {
  return (
    <>
      <CodeBlock lang="yaml" title="otel-collector.yaml" className="my-6" code={collectorConfig(product, serviceUrl(product, "writer"))} />
      <CodeBlock lang="yaml" title="grafana-datasource.yaml" className="my-6" code={grafanaDatasource(product, serviceUrl(product, "reader"))} />
    </>
  );
}

export function VerifyInstall({ product }: { product: ProductId }) {
  return <CodeBlock lang="bash" className="my-6" code={verifyCommands(product)} />;
}

export function ToolEndpoints({ product }: { product: ProductId }) {
  return (
    <table>
      <thead>
        <tr>
          <th>Tool</th>
          <th>URL</th>
        </tr>
      </thead>
      <tbody>
        {INTEGRATIONS[product].map((i) => (
          <tr key={i.tool}>
            <td>
              <InlineMd text={i.tool} />
              {i.note && (
                <div className="mt-0.5 text-[12.5px] text-muted">
                  <InlineMd text={i.note} />
                </div>
              )}
            </td>
            <td>
              <code className="break-all">{serviceUrl(product, i.role) + i.path}</code>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

const TONE: Record<Exclude<KeyField["tone"], "time">, string> = {
  fixed: "var(--faint)",
  scope: "var(--accent-text)",
  type: "var(--ink)",
};

/** Byte layout shared by every key of one product, with the record types that follow it. */
export function KeyLayout({ product, caption }: { product: ProductId; caption?: string }) {
  const layout = KEY_LAYOUTS[product];
  const color = PRODUCTS[product].color;
  const total = layout.fields.reduce((a, f) => a + f.width, 0);
  const boundaryAt = layout.fields.slice(0, layout.boundary + 1).reduce((a, f) => a + f.width, 0) / total;
  return (
    <Figure label="Key layout" caption={caption} bodyClassName="px-4 pb-5 pt-6 sm:px-6">
      <div className="relative">
        <div className="flex border border-line-strong">
          {layout.fields.map((f, i) => {
            const tone = f.tone === "time" ? color : TONE[f.tone];
            return (
              <div
                key={f.label}
                className={`relative min-w-0 px-2 py-2.5 ${i > 0 ? "border-l border-line-strong" : ""}`}
                style={{ flex: f.width, background: `color-mix(in srgb, ${tone} 8%, var(--card))` }}
              >
                <span aria-hidden className="absolute inset-x-0 top-0 h-[2px]" style={{ background: tone }} />
                <div className="truncate font-mono text-[11px] text-muted">{f.label}</div>
                <div className="mt-0.5 truncate font-mono text-[12.5px] text-ink">{f.value}</div>
              </div>
            );
          })}
        </div>
        <div className="pointer-events-none absolute -bottom-3 -top-3 w-px bg-ink" style={{ left: `${boundaryAt * 100}%` }}>
          <span className="absolute -bottom-5 left-1/2 -translate-x-1/2 whitespace-nowrap font-mono text-[10.5px] text-ink">
            SlateDB segment ▲
          </span>
        </div>
      </div>
      <p className="mt-9 text-[13px] leading-relaxed text-fg">
        Every key starts with this prefix, so each tenant namespace and {layout.unit} is one contiguous key range. Retention drops whole ranges, and
        queries scan only the ranges in their time window. The record type comes next:
      </p>
      <div className="mt-4 grid gap-x-6 sm:grid-cols-2">
        {layout.records.map((r) => (
          <div key={r.id} className="flex items-baseline gap-3 border-t border-line py-2">
            <span className="w-[54px] shrink-0 font-mono text-[11.5px] text-muted">{r.id}</span>
            <span className="min-w-0">
              <span className="text-[13px] text-ink">{r.name}</span>
              <span className="block text-[12.5px] leading-snug text-muted">
                <InlineMd text={r.purpose} />
              </span>
            </span>
          </div>
        ))}
      </div>
    </Figure>
  );
}

type Next = "architecture" | "installation" | "api" | "benchmarks";

const NEXT: Record<Next, { title: string; path: string; blurb: (p: ProductId) => string }> = {
  architecture: { title: "Architecture", path: "", blurb: (p) => `How ${PRODUCTS[p].name} compares with ${PRODUCTS[p].peer}.` },
  installation: { title: "Installation", path: "installation/", blurb: () => "Annotated custom resources, access and scaling." },
  api: { title: "API reference", path: "api/", blurb: (p) => `Every ${PRODUCTS[p].query} and ingest route, with examples.` },
  benchmarks: { title: "Benchmarks", path: "benchmarks/", blurb: (p) => `Latency against ${PRODUCTS[p].peer}, run by run.` },
};

export function NextSteps({ product, links }: { product: ProductId; links: Next[] }) {
  return (
    <div className={`not-prose my-10 grid gap-3 ${links.length === 3 ? "sm:grid-cols-3" : "sm:grid-cols-2"}`}>
      {links.map((k) => {
        const n = NEXT[k];
        return (
          <Link key={k} href={`/${product}/${n.path}`} className="card group flex items-start gap-3 p-4 transition-colors hover:border-line-strong">
            <SpaceIcon space={product} size={18} />
            <span className="min-w-0 flex-1">
              <span className="flex items-center gap-1.5 font-serif text-[16px] text-ink">
                {n.title}
                <span className="text-muted transition-transform group-hover:translate-x-0.5 group-hover:text-accent-text">
                  <ArrowRight />
                </span>
              </span>
              <span className="mt-1 block text-[13px] leading-snug text-fg">{n.blurb(product)}</span>
            </span>
          </Link>
        );
      })}
    </div>
  );
}
