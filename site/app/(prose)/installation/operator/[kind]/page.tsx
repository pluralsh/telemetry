import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { PageHeader } from "@/components/doc-page";
import { InlineMd } from "@/components/inline-md";
import { KindFacts, TypeSection } from "@/components/operator/reference";
import { loadOperatorApi } from "@/lib/operator-api";

export const dynamicParams = false;

export function generateStaticParams() {
  return loadOperatorApi().kinds.map((k) => ({ kind: k.slug }));
}

function kindFor(slug: string) {
  return loadOperatorApi().kinds.find((k) => k.slug === slug);
}

export async function generateMetadata(props: PageProps<"/installation/operator/[kind]">): Promise<Metadata> {
  const { kind } = await props.params;
  const found = kindFor(kind);
  return { title: found ? `${found.kind} reference` : "Operator reference", description: found?.summary };
}

export default async function Page(props: PageProps<"/installation/operator/[kind]">) {
  const { kind: slug } = await props.params;
  const kind = kindFor(slug);
  if (!kind) notFound();
  const { groupVersion } = loadOperatorApi();
  const onPage = new Set(kind.types.map((t) => t.name));

  return (
    <>
      <PageHeader title={kind.kind} lede={kind.summary ? <InlineMd text={kind.summary} /> : undefined} />
      <KindFacts kind={kind} groupVersion={groupVersion} />
      {kind.types.map((t) => (
        <TypeSection key={t.name} type={t} groupVersion={groupVersion} onPage={onPage} />
      ))}
    </>
  );
}
