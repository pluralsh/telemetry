import type { Operation } from "./openapi";

type Copy = { title: string; description: string };

/** Where the examples assume `plural-vector cluster` listens. */
export const VECTOR_BASE_URL = "http://localhost:8080";
const NS = "articles";

export const VECTOR_API_COPY: Record<string, Copy> = {
  list_namespaces: {
    title: "List namespaces",
    description: "Returns every namespace's metadata, sorted by name.",
  },
  create_namespace: {
    title: "Create a namespace",
    description:
      "Creates an empty namespace on the active shard whose namespace-id range holds the hash of its name. Creating one explicitly is optional: the first write to a missing namespace creates it, with `dimensions` taken from its first vector.",
  },
  get_namespace: {
    title: "Get a namespace",
    description: "Returns the namespace's dimensions, metric, shard, and attribute schema, including attribute types inferred from writes.",
  },
  delete_namespace: {
    title: "Delete a namespace",
    description:
      "Deletes the namespace and every row in it. The name can be reused right away. Blocks other requests on the cluster while it runs.",
  },
  get_schema: {
    title: "Get the schema",
    description: "Returns the namespace's attribute schemas by name.",
  },
  update_schema: {
    title: "Update the schema",
    description:
      "Declares new attributes or turns `full_text_search` on or off. An attribute's type never changes once set; a mismatched type is rejected. Full-text indexing applies to rows written after the change, so rewrite existing rows to index them.",
  },
  write: {
    title: "Write rows and documents",
    description:
      "Upserts, patches, and deletes rows, and upserts and deletes multi-passage documents, in one request. The whole request is validated first, so a bad row or filter changes nothing. The response returns once the write is applied; row reads and strongly consistent queries (standalone only) see it immediately, and it reaches object storage, and eventually consistent queries, within the flush interval.",
  },
  query: {
    title: "Query",
    description:
      "Ranks the namespace's rows by vector distance, BM25 relevance, or an attribute, keeps those matching `filters`, and returns the top `limit`. Send `queries` instead to run several queries against one snapshot, optionally fused with reciprocal rank fusion. See [Writing and querying](/vector/queries/) for the full language.",
  },
  get_row: {
    title: "Get a row",
    description: "Returns one row with all its attributes, including `vector`. Reads are strongly consistent.",
  },
  list_shards: {
    title: "List shards",
    description: "Returns every shard with its state, namespace-id range, owning writer, lineage, and the namespaces it holds. Retired shards stay listed after a split, and `split_to` names the writer an active shard is being split for.",
  },
  healthy: {
    title: "Liveness",
    description: "Returns `200` while the process is running.",
  },
  ready: {
    title: "Readiness",
    description: "Returns `200` once the server is listening. Shards a replica serves open in the background, and requests for a shard that isn't open yet get `503`.",
  },
  metrics: {
    title: "Server metrics",
    description: "Prometheus exposition of request, write, query, and SlateDB metrics.",
  },
};

export const VECTOR_PARAM_COPY: Record<string, string> = {};

export const VECTOR_TAG_COPY: Record<string, string> = {
  namespaces: "A namespace is an independent collection of rows with its own dimensions, distance metric, schema, and index.",
  rows: "Rows are an id, a vector, and typed attributes. Documents are stored as one row per passage.",
  shards: "Each shard holds a contiguous range of namespace ids, so each namespace lives on exactly one shard, and each writer owns one shard. Adding a writer splits a shard for it.",
  operations: "Probes and self-monitoring.",
};

const SCHEMA = {
  title: { type: "string", full_text_search: true },
  body: { type: "string", full_text_search: true },
  tags: { type: "[]string" },
  year: { type: "int" },
};

const NAMESPACE = {
  name: NS,
  id: "5c0e4f1d2a9b7e63",
  shard: 1,
  dimensions: 3,
  distance_metric: "cosine_distance",
  schema: SCHEMA,
  created_at_ms: 1791464400000,
};

const PATH_EXAMPLES: Record<string, string> = { namespace: NS, id: "kb-42#0" };

const BODIES: Record<string, unknown> = {
  create_namespace: {
    dimensions: 3,
    distance_metric: "cosine_distance",
    schema: { title: SCHEMA.title, body: SCHEMA.body },
  },
  update_schema: { tags: SCHEMA.tags, year: SCHEMA.year },
  write: {
    upsert_documents: [
      {
        id: "kb-42",
        attributes: { title: "Rotating database credentials", tags: ["security", "ops"], year: 2026 },
        passages: [
          { vector: [0.12, 0.88, 0.05], body: "Credentials rotate every 30 days through the secrets operator." },
          { vector: [0.31, 0.72, 0.11], body: "Rotation restarts pods that mount the secret." },
        ],
      },
    ],
    upsert_rows: [{ id: "faq-7", vector: [0.9, 0.1, 0.2], title: "Where are backups stored?", tags: ["faq"], year: 2025 }],
    patch_rows: [{ id: "faq-3", tags: ["faq", "archived"] }],
    deletes: ["faq-1"],
  },
  query: {
    rank_by: { op: "ANN", vector: [0.1, 0.9, 0.0] },
    filters: {
      op: "And",
      filters: [
        { op: "Contains", attribute: "tags", value: "security" },
        { op: "Gte", attribute: "year", value: 2025 },
      ],
    },
    limit: { total: 5, per: { attributes: ["$doc_id"], limit: 1 } },
    include_attributes: ["title", "$doc_id"],
  },
};

const RESPONSES: Record<string, unknown> = {
  list_namespaces: { namespaces: [NAMESPACE] },
  create_namespace: { ...NAMESPACE, schema: { title: SCHEMA.title, body: SCHEMA.body } },
  get_namespace: NAMESPACE,
  delete_namespace: { status: "ok" },
  get_schema: SCHEMA,
  update_schema: SCHEMA,
  write: { rows_upserted: 3, rows_patched: 1, rows_deleted: 1, documents_upserted: 1, documents_deleted: 0 },
  query: {
    rows: [
      { id: "kb-42#0", $dist: 0.0091, title: "Rotating database credentials", $doc_id: "kb-42" },
      { id: "kb-17#2", $dist: 0.1874, title: "Auditing secret access", $doc_id: "kb-17" },
    ],
    performance: { query_execution_ms: 2.4 },
  },
  get_row: {
    id: "kb-42#0",
    vector: [0.12, 0.88, 0.05],
    title: "Rotating database credentials",
    body: "Credentials rotate every 30 days through the secrets operator.",
    tags: ["security", "ops"],
    year: 2026,
    $doc_id: "kb-42",
    $passage: 0,
  },
  list_shards: {
    shards: [
      { id: 0, state: "retired", range: { start: "0000000000000000" }, children: [1, 2], created_at_ms: 1788872400000, namespaces: [] },
      { id: 1, state: "active", range: { start: "0000000000000000", end: "7f3a000000000000" }, owner: "vector-writer-0", parent: 0, created_at_ms: 1791464400000, namespaces: [NS, "billing"] },
      { id: 2, state: "active", range: { start: "7f3a000000000000" }, owner: "vector-writer-1", parent: 0, created_at_ms: 1791464400000, namespaces: ["support"] },
    ],
  },
};

export const VECTOR_ERROR_EXAMPLE = JSON.stringify(
  { status: "error", message: "Invalid input: invalid filter [\"year\",\"Between\",1]: unknown operator 'Between'" },
  null,
  2,
);

export function vectorCurl(op: Operation): string {
  let url = op.path;
  for (const p of op.params.filter((p) => p.in === "path")) {
    url = url.replace(`{${p.name}}`, encodeURIComponent(PATH_EXAMPLES[p.name] ?? p.name));
  }
  const lines = [op.method === "GET" ? `curl ${VECTOR_BASE_URL}${url}` : `curl -X ${op.method} ${VECTOR_BASE_URL}${url}`];
  const body = BODIES[op.operationId];
  if (body) {
    lines.push(`-H 'Content-Type: application/json'`);
    lines.push(`--data-binary @- <<'EOF'\n${JSON.stringify(body, null, 2)}\nEOF`);
  }
  return lines.map((l, i) => (i === 0 ? l : "  " + l)).join(" \\\n");
}

export function vectorResponse(op: Operation, status: string): { status: string; body: string } | null {
  if (op.operationId === "metrics") {
    return { status, body: "# TYPE vector_upserts_total counter\nvector_upserts_total 18342" };
  }
  const body = RESPONSES[op.operationId];
  if (body === undefined) return { status, body: op.operationId === "healthy" || op.operationId === "ready" ? "OK" : "" };
  return { status, body: JSON.stringify(body, null, 2) };
}
