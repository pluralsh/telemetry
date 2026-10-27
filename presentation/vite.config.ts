import { fileURLToPath, URL } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// `@/` resolves into the docs site so the talk reuses its architecture and cost models verbatim.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  base: "./",
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("../site", import.meta.url)),
    },
  },
  server: { port: 5199 },
});
