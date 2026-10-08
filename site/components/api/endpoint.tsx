import type { ProductId } from "@/lib/nav";
import type { Operation } from "@/lib/openapi";
import { curlFor, responseExampleFor } from "@/lib/api-examples";
import { CodeBlock } from "../code-block";
import { InlineMd } from "../inline-md";
import { MethodBadge } from "../method-badge";
import { ParamList, SchemaFields, fieldsOf } from "./fields";

export function EndpointPath({ path, className = "" }: { path: string; className?: string }) {
  return (
    <code className={`break-all font-mono text-[12.5px] text-fg ${className}`}>
      {path.split(/(\{[^}]+\})/).map((seg, i) =>
        seg.startsWith("{") ? (
          <span key={i} className="text-accent-text">
            {seg}
          </span>
        ) : (
          seg
        ),
      )}
    </code>
  );
}

function statusTone(status: string) {
  if (status.startsWith("2")) return "text-[#1d7a55] dark:text-[#5cc79b]";
  if (status.startsWith("4")) return "text-[#b4590f] dark:text-[#ef9f58]";
  if (status.startsWith("5")) return "text-[#b42f2f] dark:text-[#f08a8a]";
  return "text-muted";
}

function Subhead({ children, aside }: { children: React.ReactNode; aside?: React.ReactNode }) {
  return (
    <div className="mb-1 mt-9 flex items-baseline gap-3 border-b border-line-strong pb-2">
      <h4 className="font-serif text-[15px] text-ink">{children}</h4>
      {aside && <span className="ml-auto flex flex-wrap justify-end gap-1">{aside}</span>}
    </div>
  );
}

function MediaChips({ types }: { types: string[] }) {
  return (
    <>
      {types.map((t) => (
        <span key={t} className="kbd !text-[10.5px]">
          {t}
        </span>
      ))}
    </>
  );
}

const IN_LABEL: Record<string, string> = { path: "Path parameters", query: "Query parameters", header: "Headers" };

export async function Endpoint({ product, op }: { product: ProductId; op: Operation }) {
  const groups = (["path", "query", "header"] as const)
    .map((where) => ({ where, params: op.params.filter((p) => p.in === where) }))
    .filter((g) => g.params.length);
  const response = responseExampleFor(product, op);
  const bodySchema = op.body?.media.find((m) => fieldsOf(m.schema))?.schema;
  const binaryOnly = op.body && op.body.media.every((m) => m.schema?.format === "binary" || !m.schema);

  return (
    <section id={op.anchor} className="scroll-mt-[72px] border-t border-line py-14 first:border-t-0">
      <div className="grid gap-x-12 gap-y-8 xl:grid-cols-[minmax(0,1fr)_minmax(0,440px)]">
        <div className="min-w-0">
          <h3 className="font-serif text-[1.6rem] font-light leading-tight tracking-[-0.01em] text-ink">
            <a href={`#${op.anchor}`} className="hover:text-accent-text">
              {op.title}
            </a>
          </h3>
          <div className="mt-3 flex items-center gap-2.5">
            <MethodBadge method={op.method} />
            <EndpointPath path={op.path} />
          </div>
          {op.description && <InlineMd text={op.description} className="mt-5 block text-[14.5px] leading-[1.7] text-fg" />}

          {groups.map((g) => (
            <div key={g.where}>
              <Subhead>{IN_LABEL[g.where]}</Subhead>
              <ParamList params={g.params} />
            </div>
          ))}

          {op.formFields?.length ? (
            <div>
              <Subhead aside={<MediaChips types={["application/x-www-form-urlencoded"]} />}>Form fields</Subhead>
              <ParamList params={op.formFields} />
            </div>
          ) : null}

          {op.body && !op.formFields?.length && (
            <div>
              <Subhead aside={<MediaChips types={op.body.media.map((m) => m.type)} />}>
                Request body{op.body.required ? "" : " (optional)"}
              </Subhead>
              {op.body.description && <InlineMd text={op.body.description} className="mt-3 block text-[13px] leading-[1.6] text-fg" />}
              {bodySchema ? (
                <SchemaFields schema={bodySchema} />
              ) : binaryOnly ? (
                <p className="mt-3 text-[13px] text-muted">Raw payload in the content type sent. See the request example.</p>
              ) : null}
            </div>
          )}

          <Subhead>Responses</Subhead>
          <ul>
            {op.responses.map((r) => {
              const fields = r.media.find((m) => fieldsOf(m.schema));
              return (
                <li key={r.status} className="border-t border-line py-3 first:border-t-0">
                  <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                    <code className={`font-mono text-[13px] ${statusTone(r.status)}`}>{r.status}</code>
                    <span className="text-[13px] text-fg">{r.description}</span>
                    {r.media.length > 0 && (
                      <span className="ml-auto flex gap-1">
                        <MediaChips types={r.media.map((m) => m.type)} />
                      </span>
                    )}
                  </div>
                  {fields?.schema && (
                    <details className="group mt-2 rounded-sm border border-line">
                      <summary className="flex cursor-pointer list-none items-center gap-1.5 px-3 py-1.5 text-[12px] text-muted hover:text-ink [&::-webkit-details-marker]:hidden">
                        <span className="inline-block font-mono text-[10px] transition-transform group-open:rotate-90">›</span>
                        {fields.schema.refName ?? "Schema"}
                      </summary>
                      <div className="border-t border-line px-3">
                        <SchemaFields schema={fields.schema} />
                      </div>
                    </details>
                  )}
                </li>
              );
            })}
          </ul>
        </div>

        <div className="min-w-0 xl:sticky xl:top-[81px] xl:self-start">
          <div className="flex flex-col gap-4">
            <CodeBlock code={curlFor(product, op)} lang="bash" title="Request" />
            {response?.body ? (
              <CodeBlock
                code={response.body}
                lang={response.body.trimStart().startsWith("{") || response.body.trimStart().startsWith("[") ? "json" : "text"}
                title={`Response · ${response.status}`}
              />
            ) : null}
          </div>
        </div>
      </div>
    </section>
  );
}
