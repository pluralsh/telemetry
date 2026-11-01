export type ProductId = "metrics" | "logs" | "traces";
/** Databases with a generated HTTP API reference. */
export type ApiProduct = ProductId | "vector";
export type SpaceId = "overview" | ApiProduct;

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

export type NavItem = { title: string; href: string; children?: NavItem[] };
export type NavGroup = { title: string; items: NavItem[] };

/** Every item of `groups` in reading order, children after their parent. */
export function flatItems(groups: NavGroup[]): NavItem[] {
  const flat = (items: NavItem[]): NavItem[] => items.flatMap((i) => [i, ...flat(i.children ?? [])]);
  return groups.flatMap((g) => flat(g.items));
}

/** The path from a top-level item of `items` down to the item at `href`, if any. */
export function trailTo(items: NavItem[], href: string): NavItem[] | undefined {
  for (const item of items) {
    if (item.href === href) return [item];
    const rest = trailTo(item.children ?? [], href);
    if (rest) return [item, ...rest];
  }
  return undefined;
}

export const OPERATOR_REFERENCE = "/installation/operator/";

export function kindSlug(kind: string): string {
  return kind.toLowerCase();
}

export function operatorKindHref(kind: string): string {
  return `${OPERATOR_REFERENCE}${kindSlug(kind)}/`;
}

/** The deployment modes every database's installation guide covers. */
export function installationItems(space: ApiProduct): NavItem {
  const base = `/${space}/installation/`;
  return {
    title: "Installation",
    href: base,
    children: [
      { title: "Standalone", href: `${base}standalone/` },
      { title: "Reader/writer split", href: `${base}read-write/` },
      { title: "Sharded", href: `${base}sharded/` },
    ],
  };
}

/** `operatorKinds` are the operator's resource kinds, from the generated API reference. */
export function overviewNav(operatorKinds: string[]): NavGroup[] {
  return [
    {
      title: "Get started",
      items: [
        { title: "Introduction", href: "/" },
        { title: "Manifesto", href: "/manifesto/" },
        {
          title: "Installation",
          href: "/installation/",
          children: [
            {
              title: "Operator reference",
              href: OPERATOR_REFERENCE,
              children: operatorKinds.map((kind) => ({ title: kind, href: operatorKindHref(kind) })),
            },
          ],
        },
      ],
    },
    ...CONCEPTS_NAV,
  ];
}

const CONCEPTS_NAV: NavGroup[] = [
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
        installationItems(id),
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

/** The vector database: not an observability store, so it has its own nav and no operator. */
export const VECTOR = {
  name: "Vector",
  tagline: "Namespaced vector and full-text search with filters and multi-passage documents",
  peer: "turbopuffer",
  httpPort: 8080,
};

export const VECTOR_NAV: NavGroup[] = [
  {
    title: "Guides",
    items: [
      { title: "Overview", href: "/vector/" },
      installationItems("vector"),
      { title: "Writing and querying", href: "/vector/queries/" },
      { title: "Schemas", href: "/vector/schema/" },
      { title: "Embeddings", href: "/vector/embeddings/" },
      { title: "Sharding and scaling", href: "/vector/sharding/" },
    ],
  },
  {
    title: "Reference",
    items: [{ title: "API reference", href: "/vector/api/" }],
  },
];

export function spaceFromPath(pathname: string): SpaceId {
  const seg = pathname.split("/").filter(Boolean)[0];
  if (seg === "vector") return "vector";
  return seg && isProductId(seg) ? seg : "overview";
}

export function navFor(space: SpaceId, operatorKinds: string[]): NavGroup[] {
  if (space === "overview") return overviewNav(operatorKinds);
  if (space === "vector") return VECTOR_NAV;
  return productNav(space);
}

export const REPO_URL = "https://github.com/pluralsh/telemetry";
