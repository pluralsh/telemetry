import type { ProductId } from "./nav";

type Copy = { title: string; description: string };

const OPS: Record<string, Copy> = {
  healthy: {
    title: "Liveness",
    description: "Returns `200` while the process is running. Use it as the Kubernetes liveness probe; it is unprefixed and not namespace-scoped.",
  },
  ready: {
    title: "Readiness",
    description: "Returns `200` once every storage shard assigned to this pod is open, and `503` while shards are still being acquired or handed off. Use it as the readiness probe.",
  },
};

export const API_COPY: Record<ProductId, Record<string, Copy>> = {
  logs: {
    ...OPS,
    query: {
      title: "Instant query",
      description: "Evaluates a LogQL expression at a single point in time. Log selectors return streams of entries; metric queries such as `rate()` or `count_over_time()` return a vector.",
    },
    query_range: {
      title: "Range query",
      description: "Evaluates a LogQL expression over a time range. Log queries return up to `limit` entries in `direction` order; metric queries return a matrix sampled every `step`.",
    },
    label_names: {
      title: "List label names",
      description: "Returns the sorted stream-label names seen in the requested range, read from the per-segment discovery catalog.",
    },
    label_values: {
      title: "List label values",
      description: "Returns the sorted values of one stream label in the requested range.",
    },
    series: {
      title: "Find series",
      description: "Returns the label sets of streams matching at least one `match[]` selector. Per-entry structured metadata is intentionally excluded.",
    },
    loki_push: {
      title: "Push logs",
      description: "Loki push API. Accepts `application/json` or Snappy-compressed `application/x-protobuf`. Point Promtail, Grafana Alloy, or any Loki client at this route.",
    },
    otlp_logs: {
      title: "OTLP logs",
      description: "OTLP/HTTP `ExportLogsServiceRequest`, protobuf or JSON by `Content-Type`. Resource attributes become stream labels per the OTLP mapping; the rest is structured metadata.",
    },
    elasticsearch_bulk: {
      title: "Elasticsearch bulk",
      description: "Elasticsearch `_bulk` NDJSON for Fluent Bit, Fluentd, Vector, Logstash, and Filebeat. `index` and `create` actions are ingested; `update` and `delete` fail per item because logs are append-only.",
    },
    elasticsearch_index_bulk: {
      title: "Elasticsearch bulk (index)",
      description: "Same as `_bulk`, with a default index for actions that omit `_index`.",
    },
    elasticsearch_info: {
      title: "Elasticsearch handshake",
      description: "Version handshake that Elasticsearch shippers perform before sending bulk requests.",
    },
    elasticsearch_cluster_health: {
      title: "Elasticsearch health",
      description: "Health handshake for shippers that check cluster health. Always reports `green`.",
    },
  },
  metrics: {
    ...OPS,
    metrics: {
      title: "Self metrics",
      description: "Prometheus exposition of the server's own metrics: ingest, query, SlateDB, cache, and shard ownership.",
    },
    query: {
      title: "Instant query",
      description: "Evaluates a PromQL expression at a single timestamp, defaulting to now. Returns the standard Prometheus `{status, data}` envelope.",
    },
    query_range: {
      title: "Range query",
      description: "Evaluates a PromQL expression from `start` to `end` every `step`. Returns a Prometheus matrix.",
    },
    labels: {
      title: "List label names",
      description: "Returns label names, optionally restricted to series matching `match[]` within a time range.",
    },
    label_values: {
      title: "List label values",
      description: "Returns the values of one label. Use `__name__` to list metric names.",
    },
    metadata: {
      title: "Metric metadata",
      description: "Returns type, help, and unit metadata recorded from remote write and OTLP ingest.",
    },
    series: {
      title: "Find series",
      description: "Returns the label sets of series matching at least one `match[]` selector.",
    },
    federate: {
      title: "Federate",
      description: "Prometheus federation endpoint. Returns the latest sample of each matching series in text exposition format.",
    },
    remote_write: {
      title: "Remote write",
      description: "Prometheus remote write 1.0 and 2.0 (Snappy-compressed protobuf), including native histograms. With stock Prometheus, set `queue_config.retry_on_http_429: true` so backpressure is retried.",
    },
    otlp_metrics: {
      title: "OTLP metrics",
      description: "OTLP/HTTP `ExportMetricsServiceRequest` as protobuf or JSON, optionally gzip-encoded. Exponential histograms are stored as native histograms.",
    },
  },
  traces: {
    ...OPS,
    echo: {
      title: "Echo",
      description: "Tempo-compatible echo used by Grafana to validate the data source.",
    },
    metrics_query_range: {
      title: "TraceQL metrics",
      description: "Routed for Grafana compatibility. TraceQL metrics are not implemented yet, so this returns `501`.",
    },
    search: {
      title: "Search traces",
      description: "Runs a TraceQL query (`q`) or a legacy `tags` selector and returns matching trace summaries.",
    },
    trace_v1: {
      title: "Get trace (v1)",
      description: "Fetches a full trace by its 32-character hex ID. Returns OTLP JSON by default; send a protobuf `Accept` header for protobuf.",
    },
    trace_v2: {
      title: "Get trace (v2)",
      description: "Tempo v2 trace lookup. Same lookup as v1, wrapped in the v2 response envelope.",
    },
    tag_names_legacy: { title: "List tags (legacy)", description: "Legacy tag-name discovery." },
    tag_values_legacy: { title: "Tag values (legacy)", description: "Legacy tag-value discovery." },
    tag_names_v1: { title: "List tags (v1)", description: "Tempo v1 tag-name discovery." },
    tag_values_v1: { title: "Tag values (v1)", description: "Tempo v1 tag-value discovery." },
    tag_names_v2: {
      title: "List tags (v2)",
      description: "Scoped tag-name discovery. Filter by `scope` (`resource`, `span`, `intrinsic`) and an optional TraceQL `q`.",
    },
    tag_values_v2: {
      title: "Tag values (v2)",
      description: "Typed tag values for one attribute, such as `resource.service.name`.",
    },
    zipkin_spans: {
      title: "Zipkin spans",
      description: "Zipkin v2 JSON span ingestion. Legacy Zipkin reporters rarely retry rejected batches, so put an OpenTelemetry Collector in front when loss is unacceptable.",
    },
    otlp_traces: {
      title: "OTLP traces",
      description: "OTLP/HTTP `ExportTraceServiceRequest`, protobuf or JSON. OTLP/gRPC (`4317`) and Jaeger gRPC (`14250`) are also served on the pod.",
    },
  },
};

const COMMON_PARAMS: Record<string, string> = {
  namespace: "Tenant namespace. A `NamespaceAuthentication` for this database must grant the caller access to it.",
  name: "Label name.",
  limit: "Maximum number of results to return.",
};

const PROM_TIME = "RFC 3339 timestamp or Unix seconds, optionally fractional.";

export const PARAM_COPY: Record<ProductId, Record<string, string>> = {
  metrics: {
    ...COMMON_PARAMS,
    query: "PromQL expression.",
    time: `Evaluation timestamp. ${PROM_TIME} Defaults to now.`,
    start: `Start of the range, inclusive. ${PROM_TIME}`,
    end: `End of the range, inclusive. ${PROM_TIME}`,
    step: "Resolution step as a duration (`30s`, `1m`) or float seconds.",
    "match[]": "Series selector. Repeat to match the union of several selectors.",
    metric: "Restrict metadata to one metric name.",
  },
  logs: {
    ...COMMON_PARAMS,
    query: "LogQL expression.",
    time: "Evaluation timestamp as RFC 3339 or Unix nanoseconds. Defaults to now.",
    start: "Start of the range as RFC 3339 or Unix nanoseconds. Defaults to one hour before `end`.",
    end: "End of the range as RFC 3339 or Unix nanoseconds. Defaults to now.",
    since: "Duration before `end` to use as `start`, such as `1h`. Ignored when `start` is set.",
    step: "Step for metric queries as a duration or float seconds.",
    direction: "`forward` or `backward` (default). Order in which log entries are returned.",
    "match[]": "Stream selector. Repeat to match the union of several selectors.",
    index: "Default index for actions that omit `_index`. Stored as the `index` stream label.",
    _stream_fields: "Comma-separated document fields to promote to stream labels for this request.",
    _msg_field: "Document field to use as the log line for this request.",
    _time_field: "Document field holding the entry timestamp for this request.",
  },
  traces: {
    ...COMMON_PARAMS,
    q: "TraceQL query.",
    tags: "Legacy logfmt tag selector, such as `service.name=checkout`.",
    start: "Start of the search window in Unix seconds.",
    end: "End of the search window in Unix seconds.",
    scope: "Attribute scope: `resource`, `span`, or `intrinsic`.",
    trace_id: "Trace ID as 32 hexadecimal characters.",
    name: "Attribute name, such as `resource.service.name`.",
  },
};

export const TAG_COPY: Record<ProductId, Record<string, string>> = {
  logs: {
    query: "LogQL evaluation. Every read route is namespace-scoped under `/read/ns/{namespace}`.",
    metadata: "Discovery of labels, values, and streams for query builders such as Grafana Explore.",
    ingest: "Write routes, served by writers. Requests are forwarded to the owning shard over internal gRPC.",
    operations: "Probes for Kubernetes and load balancers.",
  },
  metrics: {
    query: "PromQL evaluation with standard Prometheus response envelopes.",
    metadata: "Series, label, and metadata discovery.",
    ingest: "Remote write and OTLP. Samples are routed per routing epoch to their storage shard.",
    operations: "Probes and self-monitoring.",
  },
  traces: {
    traces: "Trace lookup by ID. Readers merge partial traces from every shard the ID could route to.",
    search: "TraceQL search and tag discovery.",
    ingest: "OTLP/HTTP and Zipkin ingestion. OTLP/gRPC and Jaeger gRPC are served on separate ports.",
    operations: "Probes and data source checks.",
  },
};
