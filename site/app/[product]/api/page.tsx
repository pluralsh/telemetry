import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { ApiReference } from "@/components/api/api-reference";
import { PRODUCT_IDS, PRODUCTS, isProductId } from "@/lib/nav";
import { loadSpec } from "@/lib/openapi";

export const dynamicParams = false;

export function generateStaticParams() {
  return PRODUCT_IDS.map((product) => ({ product }));
}

export async function generateMetadata(props: PageProps<"/[product]/api">): Promise<Metadata> {
  const { product } = await props.params;
  return { title: isProductId(product) ? `${PRODUCTS[product].name} API reference` : "API reference" };
}

export default async function Page(props: PageProps<"/[product]/api">) {
  const { product } = await props.params;
  if (!isProductId(product)) notFound();
  return <ApiReference spec={loadSpec(product)} />;
}
