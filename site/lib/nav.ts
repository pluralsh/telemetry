export type ProductId = "metrics" | "logs" | "traces";
export type SpaceId = "overview" | ProductId;

export type Product = {
  id: ProductId;
  name: string;
  tagline: string;
  peer: string;
  peerLong: string;
  query: string;
  color: string;
  httpPort: number;
  grpcPort: number;
  image: string;
  grafana: string;
  retentionDefault: string;
};

export const PRODUCTS: Record<ProductId, Product> = {
  metrics: {
    id: "metrics",
    name: "Metrics",
    tagline: "Prometheus-compatible TSDB with remote write and OTLP",
    peer: "Mimir",
    peerLong: "Prometheus / Mimir",
    query: "PromQL",
    color: "var(--metrics)",
    httpPort: 8080,
    grpcPort: 9090,
    image: "ghcr.io/pluralsh/metrics",
    grafana: "Prometheus",
    retentionDefault: "60d",
  },
  logs: {
    id: "logs",
    name: "Logs",
    tagline: "Loki-compatible log store with LogQL, OTLP and _bulk ingest",
    peer: "Loki",
    peerLong: "Grafana Loki",
    query: "LogQL",
    color: "var(--logs)",
    httpPort: 3100,
    grpcPort: 9091,
    image: "ghcr.io/pluralsh/logs",
    grafana: "Loki",
    retentionDefault: "14d",
  },
  traces: {
    id: "traces",
    name: "Traces",
    tagline: "Tempo-compatible trace store with TraceQL and OTLP",
    peer: "Tempo",
    peerLong: "Grafana Tempo",
    query: "TraceQL",
    color: "var(--traces)",
    httpPort: 3200,
    grpcPort: 9092,
    image: "ghcr.io/pluralsh/traces",
    grafana: "Tempo",
    retentionDefault: "14d",
  },
};

export const PRODUCT_IDS: ProductId[] = ["metrics", "logs", "traces"];

export function isProductId(v: string): v is ProductId {
  return (PRODUCT_IDS as string[]).includes(v);
}

export type NavItem = { title: string; href: string };
export type NavGroup = { title: string; items: NavItem[] };

export const OVERVIEW_NAV: NavGroup[] = [
  {
    title: "Get started",
    items: [
      { title: "Introduction", href: "/" },
      { title: "Manifesto", href: "/manifesto/" },
      { title: "Installation", href: "/installation/" },
    ],
  },
  {
    title: "Concepts",
    items: [
      { title: "Architecture", href: "/architecture/" },
      { title: "Epoch sharding", href: "/sharding/" },
      { title: "Verification", href: "/verification/" },
      { title: "Cost estimates", href: "/cost/" },
    ],
  },
];

export function productNav(id: ProductId): NavGroup[] {
  return [
    {
      title: "Guides",
      items: [
        { title: "Architecture", href: `/${id}/` },
        { title: "Installation", href: `/${id}/installation/` },
      ],
    },
    {
      title: "Reference",
      items: [
        { title: "API reference", href: `/${id}/api/` },
        { title: "Benchmarks", href: `/${id}/benchmarks/` },
      ],
    },
  ];
}

export function spaceFromPath(pathname: string): SpaceId {
  const seg = pathname.split("/").filter(Boolean)[0];
  return seg && isProductId(seg) ? seg : "overview";
}

export function navFor(space: SpaceId): NavGroup[] {
  return space === "overview" ? OVERVIEW_NAV : productNav(space);
}

export const REPO_URL = "https://github.com/pluralsh/telemetry";
