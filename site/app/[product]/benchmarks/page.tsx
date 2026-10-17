import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { BenchmarkExplorer } from "@/components/benchmarks/benchmark-explorer";
import { ColdReads } from "@/components/benchmarks/cold-reads";
import { CodeBlock } from "@/components/code-block";
import { PageHeader } from "@/components/doc-page";
import { SpaceIcon } from "@/components/icons";
import { PRODUCT_IDS, PRODUCTS, REPO_URL, isProductId, type ProductId } from "@/lib/nav";
import { loadBenchRuns } from "@/lib/benchmarks";

export const dynamicParams = false;

export function generateStaticParams() {
  return PRODUCT_IDS.map((product) => ({ product }));
}

export async function generateMetadata(props: PageProps<"/[product]/benchmarks">): Promise<Metadata> {
  const { product } = await props.params;
  return { title: isProductId(product) ? `${PRODUCTS[product].name} benchmarks` : "Benchmarks" };
}

const SETUP: Record<ProductId, string> = {
  logs: "a single Logs process on MinIO, next to Loki",
  metrics: "two writers and a reader on MinIO, next to Prometheus or Mimir",
  traces: "two writers and a reader on MinIO, next to Tempo",
};

const COLD_DATA: Record<ProductId, string> = {
  logs: "22 hours of logs (about a million lines across 144 streams)",
  metrics: "22 hours of 30-second metrics (about 2.7 million samples, including churning high-cardinality series)",
  traces: "22 hours of traces (about 30,000 traces and 190,000 spans)",
};

const COLD_ORACLE: Record<ProductId, string> = {
  logs: "Loki",
  metrics: "Mimir's store-gateway",
  traces: "Tempo",
};

export default async function Page(props: PageProps<"/[product]/benchmarks">) {
  const { product } = await props.params;
  if (!isProductId(product)) notFound();
  const p = PRODUCTS[product];
  const all = loadBenchRuns(product);
  const runs = all.filter((r) => !r.cold);
  const coldRuns = all.filter((r) => r.cold);

  return (
    <div className="mx-auto max-w-[1080px] px-6 pb-24 pt-12 sm:px-10 lg:pt-16">
      <PageHeader
        icon={<SpaceIcon space={product} size={28} />}
        eyebrow="Reference"
        title="Benchmarks"
        lede={
          <>
            Every run loads the same randomized data into {SETUP[product]}, then sends both the same randomized {p.query} queries. Each answer is
            checked for correctness and timed. The newest run is shown by default.
          </>
        }
      />

      <BenchmarkExplorer product={product} runs={runs} />

      <section className="prose-docs mt-16 max-w-[760px]">
        <h2 id="cold">Cold reads from object storage</h2>
        <p>
          The <em>cold</em> scenario loads {COLD_DATA[product]} at the default scale into both systems, waits until everything is flushed and compacted into MinIO, then
          runs a fixed battery of hour-to-day queries. Before every measured query the system answering it is restarted with nothing local: no
          memory or disk caches, no downloaded index or block files. Only the side being measured runs a query at a time, and {COLD_ORACLE[product]}{" "}
          reads only object storage. Each process does whatever it does on its own at startup before the query is sent, and that work is reported
          separately. MinIO adds a fixed delay to every first byte to stand in for S3.
        </p>
      </section>
      <div className="mt-6">
        <ColdReads product={product} runs={coldRuns} />
      </div>
      <div className="mt-4 max-w-[760px]">
        <CodeBlock
          lang="bash"
          code={`PYTHONPATH=tests/regression mise exec -- python -m harness.fuzz.bench --products ${product} --scenarios cold --duration 40m`}
        />
      </div>

      <section className="prose-docs mt-16 max-w-[760px]">
        <h2 id="method">How runs are measured</h2>
        <p>
          The differential fuzzer in <code>tests/regression/harness/fuzz</code> runs both systems on one Docker host, each pinned to its own CPU set.
          A run fails on any mismatch, implementation error or timeout, unstable implementation answer, or rejected write. The <em>recent</em> scenario
          queries data still being ingested; <em>historical</em> queries older data that has been flushed to object storage, with artificial latency
          added to MinIO; <em>cold</em> restarts the answering system before every query, as described above.
        </p>
        <p>To record a new run from the repository root:</p>
      </section>
      <div className="mt-4 max-w-[760px]">
        <CodeBlock
          lang="bash"
          code={`PYTHONPATH=tests/regression mise exec -- python -m harness.fuzz.bench --products ${product} --duration 30m`}
        />
      </div>
      <p className="mt-4 max-w-[760px] text-[13.5px] text-muted">
        Results land in{" "}
        <a className="text-accent-text hover:underline" href={`${REPO_URL}/tree/main/documentation/benchmarks/fuzz`} target="_blank" rel="noreferrer">
          documentation/benchmarks/fuzz
        </a>{" "}
        and appear here on the next docs build.
      </p>
    </div>
  );
}
