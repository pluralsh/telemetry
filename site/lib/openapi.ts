import "server-only";
import fs from "node:fs";
import path from "node:path";
import type { ApiProduct } from "./nav";
import { API_COPY, PARAM_COPY, TAG_COPY } from "./api-copy";
import { VECTOR_API_COPY, VECTOR_PARAM_COPY, VECTOR_TAG_COPY } from "./vector-api";

const DOCS_DIR = path.join(process.cwd(), "..", "documentation");

export type Schema = {
  type?: string | string[];
  format?: string;
  description?: string;
  properties?: Record<string, Schema>;
  required?: string[];
  items?: Schema;
  additionalProperties?: Schema | boolean;
  oneOf?: Schema[];
  anyOf?: Schema[];
  discriminator?: { propertyName: string; mapping?: Record<string, string> };
  enum?: unknown[];
  minimum?: number;
  $ref?: string;
  refName?: string;
};

export type Param = {
  name: string;
  in: "path" | "query" | "header";
  required: boolean;
  description?: string;
  schema: Schema;
};

export type Media = { type: string; schema?: Schema };

export type Operation = {
  anchor: string;
  method: string;
  path: string;
  tag: string;
  operationId: string;
  title: string;
  description?: string;
  params: Param[];
  formFields?: Param[];
  body?: { description?: string; required: boolean; media: Media[] };
  responses: { status: string; description: string; media: Media[] }[];
};

export type ApiSpec<P extends ApiProduct = ApiProduct> = {
  product: P;
  title: string;
  description: string;
  version: string;
  tags: { name: string; description: string; ops: Operation[] }[];
};

type RawSpec = {
  info: { title: string; description: string; version: string };
  tags?: { name: string }[];
  paths: Record<string, Record<string, any>>;
  components?: { schemas?: Record<string, Schema> };
};

function resolve(schema: Schema | undefined, raw: RawSpec, depth = 0): Schema | undefined {
  if (!schema || depth > 8) return schema;
  if (schema.$ref) {
    const name = schema.$ref.split("/").pop()!;
    const target = raw.components?.schemas?.[name];
    return target ? { ...resolve(target, raw, depth + 1), refName: name } : { type: "object", refName: name };
  }
  const out: Schema = { ...schema };
  if (schema.properties) {
    out.properties = Object.fromEntries(
      Object.entries(schema.properties).map(([k, v]) => [k, resolve(v, raw, depth + 1)!]),
    );
  }
  if (schema.items) out.items = resolve(schema.items, raw, depth + 1);
  if (schema.oneOf) out.oneOf = schema.oneOf.map((s) => resolve(s, raw, depth + 1)!);
  if (schema.anyOf) out.anyOf = schema.anyOf.map((s) => resolve(s, raw, depth + 1)!);
  if (schema.additionalProperties && typeof schema.additionalProperties === "object") {
    out.additionalProperties = resolve(schema.additionalProperties, raw, depth + 1);
  }
  return out;
}

function copyFor(product: ApiProduct) {
  return product === "vector"
    ? { ops: VECTOR_API_COPY, params: VECTOR_PARAM_COPY, tags: VECTOR_TAG_COPY }
    : { ops: API_COPY[product], params: PARAM_COPY[product], tags: TAG_COPY[product] };
}

function baseId(operationId: string) {
  return operationId.replace(/_(get|post)$/, "");
}

function humanize(id: string) {
  const s = baseId(id).replace(/_/g, " ");
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function media(content: Record<string, { schema?: Schema }> | undefined, raw: RawSpec): Media[] {
  return Object.entries(content ?? {}).map(([type, m]) => ({ type, schema: resolve(m.schema, raw) }));
}

export function loadSpec<P extends ApiProduct>(product: P): ApiSpec<P> {
  const raw: RawSpec = JSON.parse(fs.readFileSync(path.join(DOCS_DIR, "openapi", `${product}.json`), "utf8"));
  const ops: Operation[] = [];
  const copies = copyFor(product);

  for (const [p, methods] of Object.entries(raw.paths)) {
    for (const [method, op] of Object.entries(methods)) {
      if (typeof op !== "object" || !op.operationId) continue;
      const copy = copies.ops?.[baseId(op.operationId)];
      const params: Param[] = (op.parameters ?? []).map((x: any) => ({
        name: x.name,
        in: x.in,
        required: !!x.required || x.in === "path",
        description: x.description ?? copies.params?.[x.name],
        schema: resolve(x.schema, raw) ?? {},
      }));
      const methodUp = method.toUpperCase();
      ops.push({
        anchor: op.operationId.replace(/_/g, "-"),
        method: methodUp,
        path: p,
        tag: op.tags?.[0] ?? "other",
        operationId: op.operationId,
        title: copy?.title
          ? methodUp === "POST" && op.operationId.endsWith("_post")
            ? `${copy.title} (form)`
            : copy.title
          : humanize(op.operationId),
        description: copy?.description ?? op.description,
        params,
        body: op.requestBody
          ? {
              description: op.requestBody.description,
              required: !!op.requestBody.required,
              media: media(op.requestBody.content, raw),
            }
          : undefined,
        responses: Object.entries(op.responses ?? {}).map(([status, r]: [string, any]) => ({
          status,
          description: r.description,
          media: media(r.content, raw),
        })),
      });
    }
  }

  // Form-encoded POST twins accept the same fields as their GET counterpart.
  for (const op of ops) {
    if (op.method !== "POST" || !op.body?.media.some((m) => m.type === "application/x-www-form-urlencoded")) continue;
    const twin = ops.find((o) => o.method === "GET" && o.path === op.path);
    if (twin) {
      op.formFields = twin.params.filter((p) => p.in === "query");
      op.description ??= `Same as the GET form, but accepts parameters as an \`application/x-www-form-urlencoded\` body. Use it for long ${product === "logs" ? "LogQL" : "PromQL"} expressions that exceed URL limits.`;
    }
  }

  const tagOrder = (raw.tags ?? []).map((t) => t.name);
  const tags = tagOrder
    .map((name) => ({
      name,
      description: copies.tags?.[name] ?? "",
      ops: ops.filter((o) => o.tag === name),
    }))
    .filter((t) => t.ops.length);

  return {
    product,
    title: raw.info.title,
    description: raw.info.description,
    version: raw.info.version,
    tags,
  };
}
