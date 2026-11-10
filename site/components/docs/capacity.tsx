import { CAPACITY } from "@/lib/capacity";
import { PRODUCTS, type ProductId } from "@/lib/nav";
import { Figure } from "@/components/diagrams/figure";

const count = (n: number) => n.toLocaleString("en-US");

/** Validated and knee rates per writer size, then every measured figure. */
export function CapacityResults({ product }: { product: ProductId }) {
  const { unit, results } = CAPACITY[product];
  const max = Math.max(...results.map((r) => r.knee));
  return (
    <Figure
      label={`Fig. — One ${PRODUCTS[product].name} writer by CPU limit`}
      caption={
<span className="text-muted">Solid: validated rate. Hatched: knee. Release v0.3.29.</span>
      }
    >
      <div className="flex flex-col gap-4 p-5">
        {results.map((r) => (
          <div key={r.vcpu}>
            <div className="mb-1.5 flex items-baseline gap-3 text-[13px]">
              <span className="font-medium text-ink">{r.vcpu} vCPU</span>
              <span className="leader" />
              <span className="font-mono text-[12px] text-ink">
                {r.validated ? `${count(r.validated)} ${unit}` : `none passed at ≥ ${count(r.failedAt ?? 0)}`}
              </span>
            </div>
            <div className="relative h-3.5 border border-line">
              <div className="hatch absolute inset-y-0 left-0 border-r border-line-strong" style={{ width: `${(r.knee / max) * 100}%` }} />
              {r.validated && <div className="absolute inset-y-0 left-0 bg-[var(--accent-text)]" style={{ width: `${(r.validated / max) * 100}%` }} />}
            </div>
            <div className="mt-1 text-right font-mono text-[11.5px] text-muted">knee {count(r.knee)}</div>
          </div>
        ))}
      </div>
      <div className="overflow-x-auto border-t border-line">
        <table className="w-full text-[12.5px]">
          <thead>
            <tr className="border-b border-line text-left text-muted">
              {["Writer", "Validated", "Knee", "p99", "Mean CPU", "Peak RSS"].map((h) => (
                <th key={h} className="px-3 py-2 font-normal">
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="font-mono tabular-nums text-ink">
            {results.map((r) => (
              <tr key={r.vcpu} className="border-b border-line last:border-0">
                <td className="px-3 py-1.5">
                  {r.vcpu} vCPU / {r.memoryGiB} GiB
                </td>
                <td className="px-3 py-1.5">
                  {r.validated ? `${count(r.validated)}${r.mibPerSecond ? `, ${r.mibPerSecond} MiB/s` : ""}` : "—"}
                </td>
                <td className="px-3 py-1.5">{count(r.knee)}</td>
                <td className="px-3 py-1.5">{r.p99Ms ? `${r.p99Ms} ms` : "—"}</td>
                <td className="px-3 py-1.5">{r.meanCpu.toFixed(1)}</td>
                <td className="px-3 py-1.5">{r.peakRssGiB.toFixed(1)} GiB</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Figure>
  );
}

/** The recommended writer sizes and the rate to plan each at. */
export function CapacityRecommendation({ product }: { product: ProductId }) {
  const { unit, recommended, unvalidatedPlan } = CAPACITY[product];
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Writer</th>
            <th>Plan each writer at</th>
          </tr>
        </thead>
        <tbody>
          {recommended.map((r) => (
            <tr key={r.vcpu}>
              <td>
                {r.vcpu} vCPU, {r.memory}
              </td>
              <td>
                {count(r.plan)} {unit}
              </td>
            </tr>
          ))}
          {unvalidatedPlan && (
            <tr>
              <td>{unvalidatedPlan.vcpu} vCPU, not validated</td>
              <td>
                at most {count(unvalidatedPlan.plan)} {unit}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
