import { PRODUCTS, type ProductId } from "./nav";
import type { Operation, Schema } from "./openapi";

export const INSTALL_NAMESPACE = "telemetry";

/** In-cluster base URL of a database's writer or reader Service, as created by the operator. */
export function serviceUrl(product: ProductId, role: "writer" | "reader") {
  return `http://${product}-${role}.${INSTALL_NAMESPACE}:${PRODUCTS[product].httpPort}`;
}

const TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";

const PARAM_EXAMPLES: Record<ProductId, Record<string, string | string[]>> = {
  logs: {
    query: '{app="checkout"} |= "error" | logfmt | duration > 250ms',
    start: "2026-10-08T12:00:00Z",
    end: "2026-10-08T13:00:00Z",
    time: "2026-10-08T13:00:00Z",
    since: "1h",
    step: "60s",
    limit: "100",
    direction: "backward",
    "match[]": ['{app="checkout"}'],
    name: "app",
    index: "logstash-2026.10.08",
    _stream_fields: "kubernetes.namespace,app",
  },
  metrics: {
    query: 'sum by (job) (rate(http_requests_total{status=~"5.."}[5m]))',
    start: "2026-10-08T12:00:00Z",
    end: "2026-10-08T13:00:00Z",
    time: "2026-10-08T13:00:00Z",
    step: "30s",
    "match[]": ['up{job="api"}'],
    name: "job",
    metric: "http_requests_total",
    limit: "10",
  },
  traces: {
    q: '{ resource.service.name = "checkout" && duration > 500ms }',
    start: "1791460800",
    end: "1791464400",
    limit: "20",
    name: "resource.service.name",
    scope: "resource",
    trace_id: TRACE_ID,
    tags: "service.name=checkout",
  },
};

const BODY_EXAMPLES: Record<string, string> = {
  "logs.loki_push": JSON.stringify(
    {
      streams: [
        {
          stream: { app: "checkout", env: "prod" },
          values: [["1791464400000000000", "level=error msg=\"payment declined\" duration=412ms", { trace_id: TRACE_ID }]],
        },
      ],
    },
    null,
    2,
  ),
  "logs.otlp_logs": JSON.stringify(
    {
      resourceLogs: [
        {
          resource: { attributes: [{ key: "service.name", value: { stringValue: "checkout" } }] },
          scopeLogs: [{ logRecords: [{ timeUnixNano: "1791464400000000000", severityText: "ERROR", body: { stringValue: "payment declined" } }] }],
        },
      ],
    },
    null,
    2,
  ),
  "logs.elasticsearch_bulk": '{"index":{"_index":"logstash-2026.10.08"}}\n{"@timestamp":"2026-10-08T13:00:00Z","message":"payment declined","app":"checkout"}',
  "logs.elasticsearch_index_bulk": '{"create":{}}\n{"@timestamp":"2026-10-08T13:00:00Z","message":"payment declined","app":"checkout"}',
  "metrics.otlp_metrics": JSON.stringify(
    {
      resourceMetrics: [
        {
          resource: { attributes: [{ key: "service.name", value: { stringValue: "api" } }] },
          scopeMetrics: [
            {
              metrics: [
                {
                  name: "http_requests_total",
                  sum: { isMonotonic: true, aggregationTemporality: 2, dataPoints: [{ asInt: "1027", timeUnixNano: "1791464400000000000" }] },
                },
              ],
            },
          ],
        },
      ],
    },
    null,
    2,
  ),
  "traces.otlp_traces": JSON.stringify(
    {
      resourceSpans: [
        {
          resource: { attributes: [{ key: "service.name", value: { stringValue: "checkout" } }] },
          scopeSpans: [
            {
              spans: [
                {
                  traceId: TRACE_ID,
                  spanId: "00f067aa0ba902b7",
                  name: "POST /pay",
                  kind: 2,
                  startTimeUnixNano: "1791464400000000000",
                  endTimeUnixNano: "1791464400412000000",
                },
              ],
            },
          ],
        },
      ],
    },
    null,
    2,
  ),
  "traces.zipkin_spans": JSON.stringify(
    [
      {
        traceId: TRACE_ID,
        id: "00f067aa0ba902b7",
        name: "post /pay",
        timestamp: 1791464400000000,
        duration: 412000,
        localEndpoint: { serviceName: "checkout" },
        tags: { "http.method": "POST" },
      },
    ],
    null,
    2,
  ),
};

const RESPONSE_EXAMPLES: Record<string, unknown> = {
  "logs.query": {
    status: "success",
    data: {
      resultType: "streams",
      result: [
        {
          stream: { app: "checkout", env: "prod" },
          values: [["1791464400000000000", 'level=error msg="payment declined" duration=412ms']],
        },
      ],
      stats: {},
    },
  },
  "logs.label_names": { status: "success", data: ["app", "env", "namespace"] },
  "logs.label_values": { status: "success", data: ["checkout", "gateway", "ledger"] },
  "logs.series": { status: "success", data: [{ app: "checkout", env: "prod" }] },
  "logs.elasticsearch_bulk": { took: 3, errors: false, items: [{ index: { _index: "logstash", _id: "1", status: 201 } }] },
  "metrics.query": {
    status: "success",
    data: { resultType: "vector", result: [{ metric: { job: "api" }, value: [1791464400, "0.0213"] }] },
  },
  "metrics.query_range": {
    status: "success",
    data: {
      resultType: "matrix",
      result: [{ metric: { job: "api" }, values: [[1791460800, "0.0198"], [1791460830, "0.0213"]] }],
    },
  },
  "metrics.labels": { status: "success", data: ["__name__", "instance", "job", "status"] },
  "metrics.label_values": { status: "success", data: ["api", "gateway", "node-exporter"] },
  "metrics.series": { status: "success", data: [{ __name__: "up", job: "api", instance: "10.0.4.12:9100" }] },
  "metrics.metadata": {
    status: "success",
    data: { http_requests_total: [{ type: "counter", help: "Total HTTP requests.", unit: "" }] },
  },
  "traces.search": {
    traces: [
      {
        traceID: TRACE_ID,
        rootServiceName: "checkout",
        rootTraceName: "POST /pay",
        startTimeUnixNano: "1791464400000000000",
        durationMs: 412,
      },
    ],
    metrics: { inspectedTraces: 1840 },
  },
  "traces.tag_names_v2": {
    scopes: [
      { name: "resource", tags: ["service.name", "k8s.namespace.name"] },
      { name: "span", tags: ["http.method", "http.status_code"] },
    ],
  },
  "traces.tag_values_v2": { tagValues: [{ type: "string", value: "checkout" }, { type: "string", value: "ledger" }] },
  "traces.trace_v1": {
    batches: [{ resource: { attributes: [{ key: "service.name", value: { stringValue: "checkout" } }] }, scopeSpans: [] }],
  },
};

function bid(op: Operation) {
  return op.operationId.replace(/_(get|post)$/, "");
}

function example(product: ProductId, name: string) {
  return PARAM_EXAMPLES[product][name];
}

function shellQuote(s: string) {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

export function curlFor(product: ProductId, op: Operation): string {
  const host = serviceUrl(product, op.path.startsWith("/write/") ? "writer" : "reader");
  let url = op.path.replace("{namespace}", "default");
  for (const p of op.params.filter((p) => p.in === "path" && p.name !== "namespace")) {
    const v = example(product, p.name);
    url = url.replace(`{${p.name}}`, encodeURIComponent(Array.isArray(v) ? v[0] : (v ?? p.name)));
  }
  const lines: string[] = [];
  const isWrite = op.path.startsWith("/write/");
  const isOps = !op.path.includes("/ns/");
  const auth = isOps ? [] : [`-u ${isWrite ? "writer" : "reader"}:$PASSWORD`];

  const query = op.params.filter((p) => p.in === "query");
  const fields = op.formFields ?? [];
  const pick = (ps: typeof query) =>
    ps.filter((p) => p.required || ["start", "end", "limit", "step", "q", "_stream_fields"].includes(p.name));

  if (op.method === "GET") {
    const qs = pick(query);
    lines.push(`curl${qs.length ? " -G" : ""} ${host}${url}`);
    lines.push(...auth);
    for (const p of qs) {
      const v = example(product, p.name) ?? "";
      for (const one of Array.isArray(v) ? v : [v]) lines.push(`--data-urlencode ${shellQuote(`${p.name}=${one}`)}`);
    }
  } else {
    const types = op.body?.media.map((m) => m.type) ?? [];
    const qs = pick(query);
    const qstr = qs.length
      ? "?" + qs.map((p) => `${p.name}=${encodeURIComponent(String(example(product, p.name) ?? ""))}`).join("&")
      : "";
    lines.push(`curl -X POST "${host}${url}${qstr}"`);
    lines.push(...auth);
    const body = BODY_EXAMPLES[`${product}.${bid(op)}`];
    if (types.includes("application/x-www-form-urlencoded")) {
      for (const p of pick(fields)) {
        const v = example(product, p.name) ?? "";
        for (const one of Array.isArray(v) ? v : [v]) lines.push(`--data-urlencode ${shellQuote(`${p.name}=${one}`)}`);
      }
    } else if (types.includes("application/x-ndjson")) {
      lines.push(`-H 'Content-Type: application/x-ndjson'`);
      lines.push(`--data-binary $${shellQuote((body ?? "") + "\n").replace(/\n/g, "\\n")}`);
    } else if (body && types.includes("application/json")) {
      lines.push(`-H 'Content-Type: application/json'`);
      lines.push(`--data-binary @- <<'EOF'\n${body}\nEOF`);
      return lines.map((l, i) => (i === 0 ? l : "  " + l)).join(" \\\n");
    } else if (types.includes("application/x-protobuf")) {
      lines.push(`-H 'Content-Type: application/x-protobuf'`);
      if (product !== "traces") lines.push(`-H 'Content-Encoding: snappy'`);
      lines.push(`--data-binary @payload.pb`);
    }
  }
  return lines.map((l, i) => (i === 0 ? l : "  " + l)).join(" \\\n");
}

function fromSchema(s: Schema | undefined, depth = 0): unknown {
  if (!s || depth > 5) return null;
  const t = Array.isArray(s.type) ? s.type.find((x) => x !== "null") : s.type;
  if (s.oneOf) return fromSchema(s.oneOf.find((o) => o.type !== "null"), depth + 1);
  if (t === "object" || s.properties) {
    if (!s.properties) return {};
    return Object.fromEntries(Object.entries(s.properties).map(([k, v]) => [k, fromSchema(v, depth + 1)]));
  }
  if (t === "array") return [fromSchema(s.items, depth + 1)].filter((x) => x !== null);
  if (t === "integer" || t === "number") return 0;
  if (t === "boolean") return false;
  if (t === "string") return s.format === "binary" ? null : "string";
  return null;
}

export function responseExampleFor(product: ProductId, op: Operation): { status: string; body: string } | null {
  const ok = op.responses.find((r) => r.status.startsWith("2"));
  if (!ok) return null;
  const override = RESPONSE_EXAMPLES[`${product}.${bid(op)}`] ??
    (bid(op) === "query_range" ? RESPONSE_EXAMPLES[`${product}.query`] : undefined) ??
    (bid(op) === "trace_v2" ? { trace: RESPONSE_EXAMPLES["traces.trace_v1"] } : undefined);
  if (override) return { status: ok.status, body: JSON.stringify(override, null, 2) };
  const json = ok.media.find((m) => m.type === "application/json");
  if (json?.schema) {
    const v = fromSchema(json.schema);
    if (v !== null) return { status: ok.status, body: JSON.stringify(v, null, 2) };
  }
  const text = ok.media.find((m) => m.type === "text/plain");
  if (text) {
    return {
      status: ok.status,
      body:
        bid(op) === "federate"
          ? '# TYPE up untyped\nup{instance="10.0.4.12:9100",job="api"} 1 1791464400000'
          : "# HELP telemetry_ingest_samples_total Samples accepted.\n# TYPE telemetry_ingest_samples_total counter\ntelemetry_ingest_samples_total 1.0274e+07",
    };
  }
  return { status: ok.status, body: "" };
}
