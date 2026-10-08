import type { Param, Schema } from "@/lib/openapi";
import { InlineMd } from "../inline-md";

function nonNull(s: Schema): { schema: Schema; nullable: boolean } {
  if (Array.isArray(s.type)) {
    const types = s.type.filter((t) => t !== "null");
    return { schema: { ...s, type: types.length === 1 ? types[0] : types }, nullable: types.length < s.type.length };
  }
  const variants = s.oneOf ?? s.anyOf;
  if (variants) {
    const rest = variants.filter((v) => v.type !== "null");
    if (rest.length === 1) return { schema: { ...rest[0], description: s.description ?? rest[0].description }, nullable: rest.length < variants.length };
  }
  return { schema: s, nullable: false };
}

export function typeLabel(input: Schema | undefined): { label: string; nullable: boolean } {
  if (!input) return { label: "any", nullable: false };
  const { schema: s, nullable } = nonNull(input);
  const t = Array.isArray(s.type) ? s.type.join(" | ") : s.type;
  let label: string;
  if (s.oneOf || s.anyOf) label = (s.oneOf ?? s.anyOf)!.map((v) => typeLabel(v).label).join(" | ");
  else if (t === "array") label = `array of ${s.items ? typeLabel(s.items).label : "any"}`;
  else if (s.refName && (t === "object" || s.properties)) label = s.refName;
  else if (t === "object" && s.additionalProperties && typeof s.additionalProperties === "object")
    label = `map of ${typeLabel(s.additionalProperties).label}`;
  else if (t === "string" && s.format === "binary") label = "binary";
  else label = t ?? (s.properties ? "object" : "any");
  if (s.format && s.format !== "binary" && t !== "array") label += ` · ${s.format}`;
  return { label, nullable };
}

/** Object-like schema whose fields are worth listing, unwrapping arrays and nullable unions. */
export function fieldsOf(input: Schema | undefined): { props: Record<string, Schema>; required: string[] } | null {
  if (!input) return null;
  const { schema: s } = nonNull(input);
  if (s.properties && Object.keys(s.properties).length) return { props: s.properties, required: s.required ?? [] };
  if (s.items) return fieldsOf(s.items);
  return null;
}

function Constraints({ schema }: { schema: Schema }) {
  const { schema: s } = nonNull(schema);
  const values = s.enum ?? (s.items && nonNull(s.items).schema.enum);
  if (!values?.length && s.minimum === undefined) return null;
  return (
    <div className="mt-1.5 flex flex-wrap items-center gap-1.5 text-[12px] text-muted">
      {values?.length ? (
        <>
          <span className="italic">One of</span>
          {values.map((v) => (
            <code key={String(v)} className="kbd !h-[18px] !text-[10.5px]">
              {String(v)}
            </code>
          ))}
        </>
      ) : null}
      {s.minimum !== undefined && <span className="italic">Minimum {s.minimum}</span>}
    </div>
  );
}

export function FieldRow({
  name,
  schema,
  required,
  description,
  depth = 0,
}: {
  name: string;
  schema: Schema;
  required?: boolean;
  description?: string;
  depth?: number;
}) {
  const { label, nullable } = typeLabel(schema);
  const nested = depth < 4 ? fieldsOf(schema) : null;
  const desc = description ?? nonNull(schema).schema.description;
  return (
    <li className="border-t border-line py-3.5 first:border-t-0">
      <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
        <code className="font-mono text-[13px] text-ink">{name}</code>
        <span className="font-mono text-[11.5px] text-muted">
          {label}
          {nullable ? " | null" : ""}
        </span>
        {required && <span className="font-mono text-[11px] text-[#b4590f] dark:text-[#ef9f58]">required</span>}
      </div>
      {desc && <InlineMd text={desc} className="mt-1 block text-[13px] leading-[1.6] text-fg" />}
      <Constraints schema={schema} />
      {nested && (
        <details className="group mt-2.5 rounded-sm border border-line">
          <summary className="flex cursor-pointer list-none items-center gap-1.5 px-3 py-1.5 text-[12px] text-muted hover:text-ink [&::-webkit-details-marker]:hidden">
            <span className="inline-block font-mono text-[10px] transition-transform group-open:rotate-90">›</span>
            <span className="group-open:hidden">Show child attributes</span>
            <span className="hidden group-open:inline">Hide child attributes</span>
          </summary>
          <div className="border-t border-line px-3">
            <SchemaFields schema={schema} depth={depth + 1} />
          </div>
        </details>
      )}
    </li>
  );
}

export function SchemaFields({ schema, depth = 0 }: { schema: Schema; depth?: number }) {
  const f = fieldsOf(schema);
  if (!f) return null;
  return (
    <ul>
      {Object.entries(f.props).map(([name, s]) => (
        <FieldRow key={name} name={name} schema={s} required={f.required.includes(name)} depth={depth} />
      ))}
    </ul>
  );
}

export function ParamList({ params }: { params: Param[] }) {
  return (
    <ul>
      {params.map((p) => (
        <FieldRow key={`${p.in}-${p.name}`} name={p.name} schema={p.schema} required={p.required} description={p.description} />
      ))}
    </ul>
  );
}
