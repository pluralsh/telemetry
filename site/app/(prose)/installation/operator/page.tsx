import type { Metadata } from "next";
import Link from "next/link";
import { PageHeader } from "@/components/doc-page";
import { InlineMd } from "@/components/inline-md";
import { KindLink } from "@/components/operator/reference";
import { TypeIndex } from "@/components/operator/type-index";
import { REPO_URL } from "@/lib/nav";
import { firstParagraph, loadOperatorApi, typeAnchor } from "@/lib/operator-api";

export const metadata: Metadata = { title: "Operator reference" };

export default function Page() {
  const api = loadOperatorApi();
  const types = api.kinds
    .flatMap((k) => k.types)
    .filter((t, i, all) => all.findIndex((o) => o.name === t.name) === i)
    .sort((a, b) => a.name.localeCompare(b.name))
    .map((t) => ({ name: t.name, anchor: typeAnchor(t.name), summary: firstParagraph(t.doc), kinds: api.pagesFor[t.name] }));

  return (
    <>
      <PageHeader
        title="Operator reference"
        lede={
          <>
            Every field of the telemetry-operator&apos;s custom resources in <code>{api.groupVersion}</code>, generated from
            its Go API types.
          </>
        }
      />

      <p>
        Each resource has a page with its own type and every type it contains, so a page covers everything a manifest of that
        kind can set. The validation listed with a field is enforced by the API server when the resource is applied. For a
        guided setup, start with <Link href="/installation/">Installation</Link>.
      </p>

      <h2 id="resources">Resources</h2>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Kind</th>
              <th>Short names</th>
              <th>Description</th>
            </tr>
          </thead>
          <tbody>
            {api.kinds.map((k) => (
              <tr key={k.kind}>
                <td>
                  <KindLink kind={k.kind} />
                </td>
                <td>{k.shortNames.length ? <code>{k.shortNames.join(", ")}</code> : null}</td>
                <td>
                  <InlineMd text={k.summary} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <h2 id="types">All types</h2>
      <TypeIndex types={types} />

      <p>
        The reference is generated with <a href="https://github.com/elastic/crd-ref-docs">crd-ref-docs</a> by{" "}
        <code>make crd-docs</code> in <a href={`${REPO_URL}/tree/main/go/operator`}>go/operator</a>, which also writes the
        Markdown version to <code>go/operator/docs/api.md</code>.
      </p>
    </>
  );
}
