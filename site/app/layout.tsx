import type { Metadata } from "next";
import { ThemeProvider } from "next-themes";
import "@fontsource/newsreader/400.css";
import "@fontsource/newsreader/400-italic.css";
import "./globals.css";
import { DocsShell } from "@/components/docs-shell";
import { loadSpec } from "@/lib/openapi";
import { PRODUCT_IDS } from "@/lib/nav";
import type { ApiNav } from "@/components/sidebar";

export const metadata: Metadata = {
  metadataBase: new URL("https://telemetry.plural.sh"),
  title: { default: "Plural Telemetry", template: "%s · Plural Telemetry" },
  description: "Observability should be cheap and easy. Metrics, logs and traces on object storage, built on SlateDB.",
  openGraph: {
    images: [
      {
        url: "/social-card.png",
        width: 1200,
        height: 630,
        alt: "Plural Telemetry — observability on object storage",
      },
    ],
    siteName: "Plural Telemetry",
    type: "website",
    url: "/",
  },
  twitter: {
    card: "summary_large_image",
    images: ["/social-card.png"],
  },
};

function buildApiNav(): ApiNav {
  return Object.fromEntries(
    PRODUCT_IDS.map((id) => [
      id,
      loadSpec(id).tags.map((t) => ({
        tag: t.name,
        ops: t.ops.map((o) => ({ anchor: o.anchor, title: o.title, method: o.method })),
      })),
    ]),
  ) as ApiNav;
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <ThemeProvider attribute="class" defaultTheme="light" enableSystem={false} disableTransitionOnChange>
          <DocsShell apiNav={buildApiNav()}>{children}</DocsShell>
        </ThemeProvider>
      </body>
    </html>
  );
}
