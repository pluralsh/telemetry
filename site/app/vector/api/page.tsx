import type { Metadata } from "next";
import { VectorApiReference } from "@/components/api/vector-api-reference";
import { loadSpec } from "@/lib/openapi";

export const metadata: Metadata = { title: "Vector API reference" };

export default function Page() {
  return <VectorApiReference spec={loadSpec("vector")} />;
}
