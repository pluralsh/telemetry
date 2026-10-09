import type { Metadata } from "next";
import localFont from "next/font/local";
import { ThemeProvider } from "next-themes";
import "@fontsource/ibm-plex-serif/300.css";
import "@fontsource/ibm-plex-serif/300-italic.css";
import "@fontsource/ibm-plex-serif/400.css";
import "@fontsource/ibm-plex-serif/400-italic.css";
import "@fontsource/lilex/400.css";
import "./globals.css";
import { DocsShell } from "@/components/docs-shell";
import { loadSpec } from "@/lib/openapi";
import { PRODUCT_IDS } from "@/lib/nav";
import type { ApiNav } from "@/components/sidebar";

const writer = localFont({
  src: [
    { path: "./fonts/iAWriterQuattroS-Regular.woff2", weight: "400", style: "normal" },
    { path: "./fonts/iAWriterQuattroS-Italic.woff2", weight: "400", style: "italic" },
    { path: "./fonts/iAWriterQuattroS-Bold.woff2", weight: "700", style: "normal" },
  ],
  variable: "--font-writer",
  display: "swap",
});

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
    <html lang="en" className={writer.variable} suppressHydrationWarning>
      <body>
        <ThemeProvider attribute="class" defaultTheme="system" enableSystem disableTransitionOnChange>
          <DocsShell apiNav={buildApiNav()}>{children}</DocsShell>
        </ThemeProvider>
      </body>
    </html>
  );
}
