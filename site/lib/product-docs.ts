import { INSTALL_NAMESPACE } from "./api-examples";
import { PRODUCTS, type ProductId } from "./nav";

const KIND: Record<ProductId, string> = { metrics: "Metrics", logs: "Logs", traces: "Traces" };
export const BUCKET = "acme-telemetry";
export const TENANT = "payments";

const header = (p: ProductId) => `apiVersion: telemetry.plural.sh/v1alpha1
kind: ${KIND[p]}
metadata:
  name: ${p}
  namespace: ${INSTALL_NAMESPACE}
spec:`;

const objectStore = `    storage:
      path: PRODUCT
      objectStore:
        type: Aws
        aws:
          region: us-east-1
          bucket: ${BUCKET}`;

const PRODUCT_CONFIG: Record<ProductId, string> = {
  metrics: `    request:
      maxRequestBytes: 33554432`,
  logs: `    segmentDurationSeconds: 3600
    request:
      maxQueryEntries: 5000
      queryConcurrency: 16
    elasticsearch:
      streamFields: [kubernetes.namespace_name]`,
  traces: `    segmentDurationSeconds: 3600
    request:
      maxCandidates: 10000
      maxSpansPerTrace: 100000`,
};

const WRITERS: Record<ProductId, number> = { metrics: 2, logs: 3, traces: 3 };

export function quickstartSpec(p: ProductId) {
  return `${header(p)}
  config:
${objectStore.replace("PRODUCT", p)}
`;
}

export function productionSpec(p: ProductId) {
  return `${header(p)}
  mode: Sharded
  config:
    retention: ${PRODUCTS[p].retentionDefault}
${objectStore.replace("PRODUCT", p)}
    write:
      durability: applied
      flushIntervalSeconds: 10
    sharding:
      leaseDurationSeconds: 15
${PRODUCT_CONFIG[p]}
  writer:
    replicas: ${WRITERS[p]}
    resources:
      requests: { cpu: "1", memory: 2Gi }
      limits: { memory: 4Gi }
    cacheVolume:
      persistentVolumeClaim:
        accessModes: [ReadWriteOnce]
        resources: { requests: { storage: 20Gi } }
  reader:
    replicas: 2
    cacheVolume:
      emptyDir: { sizeLimit: 20Gi }
  ingress:
    enabled: true
    hostname: ${p}.acme.internal
    ingressClass: nginx
`;
}

export function specNotes(p: ProductId): Record<string, string> {
  const P = PRODUCTS[p];
  const cachePath = `/var/cache/${p}`;
  const common: Record<string, string> = {
    "metadata.name": `Also names the ServiceAccount the IAM role binds to, and the \`${p}-writer\` and \`${p}-reader\` Services (just \`${p}\` in Standalone mode).`,
    "metadata.namespace": `The Kubernetes namespace the pods run in. Tenant namespaces for your data are separate and come from [NamespaceAuthentication](#access) resources.`,
    "spec.mode": "`Standalone` (the default) runs one pod that reads and writes. `Sharded` runs writer and reader StatefulSets that scale independently.",
    "spec.config": "Rendered into the server's config file. The operator rolls the pods when it changes.",
    "spec.config.retention": `How long data is kept, counted from ingestion, as \`${P.retentionDefault}\`, \`2w\` or \`36h\`. Default \`${P.retentionDefault}\`.`,
    "spec.config.storage.path": `Object-key prefix inside the bucket; shard suffixes are appended. Must match the prefix in the IAM policy. Default \`${p}\`.`,
    "spec.config.storage.objectStore.type": "`Aws`, `Gcp`, `Azure`, `Local` or `InMemory`. With `Aws` and no key references, the pod uses its Pod Identity credentials.",
    "spec.config.storage.objectStore.aws.region": "Bucket region. Add `endpoint` (and `allowHTTP` if needed) for S3-compatible stores such as MinIO.",
    "spec.config.storage.objectStore.aws.bucket": "Can be shared by all three databases, as long as their `path` prefixes differ.",
    "spec.config.write.durability": "When a write is acknowledged: `applied` (default) once in memory, `written` once in SlateDB's mutable state, `durable` once uploaded to object storage.",
    "spec.config.write.flushIntervalSeconds": "How often writers flush to object storage, and so how far readers can lag behind. Default `10`.",
    "spec.config.sharding.leaseDurationSeconds": "How long a shard Lease survives without renewal before another writer may take the shard over. Default `15`; `renewIntervalSeconds` (default `5`) must be lower.",
    "spec.writer.replicas": "Writer count, which is also the storage shard count. Raise it at any time; new shards take writes from the next aligned hour. It cannot be lowered.",
    "spec.writer.resources": "Defaults to 250m CPU and 512Mi memory requests with a 2Gi limit. Budget for the write buffer: 64 MiB per shard, plus up to two frozen buffers being flushed.",
    "spec.writer.cacheVolume": `Mounted at \`${cachePath}\` as the disk tier of the block cache (512 MiB RAM and 10 GiB disk by default). A PVC keeps it warm across restarts. Set exactly one of \`emptyDir\` or \`persistentVolumeClaim\`.`,
    "spec.reader.replicas": "Readers are stateless; each opens every shard read-only and shares one cache across them. Default `2` in Sharded mode. Scale freely.",
    "spec.reader.cacheVolume": "An `emptyDir` is enough: a restarted reader refills its cache from the bucket.",
    "spec.ingress": "Optional. One hostname that routes `/write` to the writers and `/read` to the readers. Add `tls` and `pathPrefix` as needed.",
  };
  const specific: Record<ProductId, Record<string, string>> = {
    metrics: {
      "spec.config.request.maxRequestBytes": "Largest remote-write or OTLP body accepted before decoding. Default 32 MiB; decoded bodies are capped by `maxDecodedRequestBytes` (128 MiB).",
    },
    logs: {
      "spec.config.segmentDurationSeconds": "Width of a time partition. Queries prune whole segments by time. Keep it stable for a dataset. Default `3600`.",
      "spec.config.request.maxQueryEntries": "Most log lines one query returns. Default `5000`.",
      "spec.config.request.queryConcurrency": "Concurrent query work per pod. Default `16`. `maxInFlightQueryBytes` (128 MiB) bounds memory.",
      "spec.config.elasticsearch.streamFields": "For `_bulk` ingest: document fields promoted to stream labels. Keep them low-cardinality; other fields become structured metadata.",
    },
    traces: {
      "spec.config.segmentDurationSeconds": "Width of a time partition. Searches prune whole segments by time. Default `3600`.",
      "spec.config.request.maxCandidates": "Most traces one TraceQL search will decode and evaluate. Default `10000`.",
      "spec.config.request.maxSpansPerTrace": "Safety limit while assembling a trace by ID. Default `100000`.",
    },
  };
  return { ...common, ...specific[p] };
}

export function accessSpec(p: ProductId) {
  const ref = `{ kind: ${KIND[p]}, name: ${p} }`;
  const pair = (role: "writer" | "reader", user: string, permission: "write" | "read") => `apiVersion: telemetry.plural.sh/v1alpha1
kind: NamespaceAuthentication
metadata:
  name: ${p}-${TENANT}-${role}
  namespace: ${INSTALL_NAMESPACE}
spec:
  dataStoreRef: ${ref}
  namespace: ${TENANT}
  username: ${user}
  permission: ${permission}
  secretKeyRef: { name: ${p}-${TENANT}-${role}, key: password }`;
  return `${pair("writer", "otel-collector", "write")}\n---\n${pair("reader", "grafana", "read")}\n`;
}

export const ACCESS_NOTES: Record<string, string> = {
  "spec.dataStoreRef": "The database this credential is for. One NamespaceAuthentication grants access to exactly one database.",
  "spec.namespace": "Tenant namespace. It is created on first use; every route is prefixed with `/ns/{namespace}`.",
  "spec.username": "HTTP basic-auth username. Unique per namespace.",
  "spec.permission": "`write` for ingest routes, `read` for queries.",
  "spec.secretKeyRef": "Secret holding the password, in the same Kubernetes namespace. Rotating it rolls the database's config.",
};

export type Integration = { tool: string; role: "writer" | "reader"; path: string; note?: string };

export const INTEGRATIONS: Record<ProductId, Integration[]> = {
  metrics: [
    { tool: "Grafana Prometheus data source", role: "reader", path: `/read/ns/${TENANT}` },
    { tool: "Prometheus / Alloy `remote_write`", role: "writer", path: `/write/ns/${TENANT}/api/v1/write` },
    { tool: "OTel Collector `otlphttp`", role: "writer", path: `/write/ns/${TENANT}`, note: "The exporter appends `/v1/metrics`." },
  ],
  logs: [
    { tool: "Grafana Loki data source", role: "reader", path: `/read/ns/${TENANT}` },
    { tool: "Alloy / Promtail `loki.write`", role: "writer", path: `/write/ns/${TENANT}/loki/api/v1/push` },
    { tool: "OTel Collector `otlphttp`", role: "writer", path: `/write/ns/${TENANT}/otlp`, note: "The exporter appends `/v1/logs`." },
    { tool: "Fluent Bit / Vector `elasticsearch`", role: "writer", path: `/write/ns/${TENANT}/elasticsearch`, note: "Speaks the `_bulk` protocol." },
  ],
  traces: [
    { tool: "Grafana Tempo data source", role: "reader", path: `/read/ns/${TENANT}` },
    { tool: "OTel Collector `otlphttp`", role: "writer", path: `/write/ns/${TENANT}`, note: "The exporter appends `/v1/traces`." },
    { tool: "Zipkin v2 reporters", role: "writer", path: `/write/ns/${TENANT}/api/v2/spans` },
  ],
};

const OTLP_PATH: Record<ProductId, string> = { metrics: "", logs: "/otlp", traces: "" };
const OTLP_PIPELINE: Record<ProductId, string> = { metrics: "metrics", logs: "logs", traces: "traces" };
const GRAFANA_TYPE: Record<ProductId, string> = { metrics: "prometheus", logs: "loki", traces: "tempo" };
const LABELS_PATH: Record<ProductId, string> = {
  metrics: "/api/v1/labels",
  logs: "/loki/api/v1/labels",
  traces: "/api/v2/search/tags",
};

export function collectorConfig(p: ProductId, writerUrl: string) {
  const env = `PLURAL_${p.toUpperCase()}_PASSWORD`;
  return `extensions:
  basicauth/plural:
    client_auth:
      username: otel-collector
      password: \${env:${env}}

exporters:
  otlphttp/plural-${p}:
    endpoint: ${writerUrl}/write/ns/${TENANT}${OTLP_PATH[p]}
    auth:
      authenticator: basicauth/plural

service:
  extensions: [basicauth/plural]
  pipelines:
    ${OTLP_PIPELINE[p]}:
      exporters: [otlphttp/plural-${p}]
`;
}

export function grafanaDatasource(p: ProductId, readerUrl: string) {
  return `apiVersion: 1
datasources:
  - name: Plural ${PRODUCTS[p].name}
    type: ${GRAFANA_TYPE[p]}
    url: ${readerUrl}/read/ns/${TENANT}
    basicAuth: true
    basicAuthUser: grafana
    secureJsonData:
      basicAuthPassword: $PLURAL_${p.toUpperCase()}_READ_PASSWORD
`;
}

export function verifyCommands(p: ProductId) {
  const port = PRODUCTS[p].httpPort;
  return `kubectl -n ${INSTALL_NAMESPACE} get ${p}
# NAME   MODE      READY   AGE
# ${p.padEnd(6)} Sharded   True    2m

kubectl -n ${INSTALL_NAMESPACE} port-forward svc/${p}-reader ${port} &
curl -u grafana:"$PASSWORD" http://localhost:${port}/read/ns/${TENANT}${LABELS_PATH[p]}
`;
}

export type KeyField = { label: string; value: string; width: number; tone: "fixed" | "scope" | "time" | "type" };
export type KeyLayout = {
  fields: KeyField[];
  /** Index of the last field inside a SlateDB segment. */
  boundary: number;
  unit: string;
  records: { id: string; name: string; purpose: string }[];
};

export const KEY_LAYOUTS: Record<ProductId, KeyLayout> = {
  metrics: {
    fields: [
      { label: "subsystem", value: "0x01", width: 1, tone: "fixed" },
      { label: "version", value: "0x03", width: 1, tone: "fixed" },
      { label: "namespace", value: "bytes · 0x00", width: 2.2, tone: "scope" },
      { label: "bucket start", value: "u32 BE", width: 1.4, tone: "time" },
      { label: "bucket size", value: "u8", width: 1.1, tone: "time" },
      { label: "record", value: "u8", width: 1, tone: "type" },
    ],
    boundary: 4,
    unit: "hour bucket",
    records: [
      { id: "0x02", name: "Series dictionary", purpose: "Label fingerprint → bucket-local series ID" },
      { id: "0x03", name: "Forward index", purpose: "Series ID → labels, type and unit" },
      { id: "0x04", name: "Inverted index", purpose: "Label term → Roaring bitmap of series IDs" },
      { id: "0x05", name: "Time series", purpose: "Compressed samples for one series" },
      { id: "0x06", name: "Bucket generation", purpose: "Write generation, for result-cache invalidation" },
      { id: "0xff", name: "Discovery catalog", purpose: "Label names, values and metric metadata" },
    ],
  },
  logs: {
    fields: [
      { label: "subsystem", value: "0x03", width: 1, tone: "fixed" },
      { label: "version", value: "0x04", width: 1, tone: "fixed" },
      { label: "namespace", value: "bytes · 0x00", width: 2.2, tone: "scope" },
      { label: "time segment", value: "i64 BE", width: 1.8, tone: "time" },
      { label: "record", value: "u8", width: 1, tone: "type" },
    ],
    boundary: 3,
    unit: "segment",
    records: [
      { id: "0x01–03", name: "Stream dictionary", purpose: "Stream IDs and their full label sets" },
      { id: "0x04", name: "Label postings", purpose: "Label term → Roaring bitmap of stream IDs" },
      { id: "0x05", name: "Run", purpose: "One stream's blocks in one object, with min/max time" },
      { id: "0x06", name: "Object block", purpose: "Compressed rows of one stream" },
      { id: "0x08–0b", name: "Search records", purpose: "Full-text term index for `| match`" },
      { id: "0x0c–0e", name: "Object bookkeeping", purpose: "Tombstones, rollups and object directories" },
    ],
  },
  traces: {
    fields: [
      { label: "subsystem", value: "0x05", width: 1, tone: "fixed" },
      { label: "version", value: "0x06", width: 1, tone: "fixed" },
      { label: "namespace", value: "bytes · 0x00", width: 2.2, tone: "scope" },
      { label: "segment", value: "i64 BE", width: 1.8, tone: "time" },
      { label: "record", value: "u8", width: 1, tone: "type" },
    ],
    boundary: 3,
    unit: "segment",
    records: [
      { id: "0x02", name: "Page metadata", purpose: "Per-trace time summaries for pruning" },
      { id: "0x03", name: "Page payload", purpose: "Up to 1,024 compressed OTLP traces" },
      { id: "0x04", name: "Trace head", purpose: "Trace ID → first page and page count" },
      { id: "0x05", name: "Attribute posting", purpose: "Typed attribute term → trace indexes" },
      { id: "0x06", name: "Trace continuation", purpose: "Later pages of a long trace" },
      { id: "0xff", name: "Discovery catalog", purpose: "Tag names and typed values" },
    ],
  },
};
