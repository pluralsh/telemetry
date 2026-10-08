import "server-only";
import { createHighlighter, type BundledLanguage, type Highlighter } from "shiki";

const LANGS = ["yaml", "bash", "json", "ini", "ruby", "text", "toml", "rust", "go", "python"] as const;
const ALIASES: Record<string, string> = { sh: "bash", shell: "bash", console: "bash", yml: "yaml", txt: "text", "": "text", ndjson: "json" };
const THEMES = { light: "one-light", dark: "one-dark-pro" } as const;

let highlighter: Promise<Highlighter> | null = null;

function get() {
  highlighter ??= createHighlighter({ themes: Object.values(THEMES), langs: [...LANGS] });
  return highlighter;
}

function langOf(lang: string): BundledLanguage {
  const l = ALIASES[lang] ?? lang;
  return ((LANGS as readonly string[]).includes(l) ? l : "text") as BundledLanguage;
}

export async function highlight(code: string, lang = "text"): Promise<string> {
  const h = await get();
  return h.codeToHtml(code, { lang: langOf(lang), themes: THEMES, defaultColor: "light" });
}

/** Highlights line by line so callers can attach annotations to individual lines. */
export async function highlightLines(code: string, lang = "yaml"): Promise<string[]> {
  const h = await get();
  const { tokens } = h.codeToTokens(code, { lang: langOf(lang), themes: THEMES });
  return tokens.map((line) =>
    line
      .map((t) => {
        const light = t.htmlStyle?.color ?? t.color ?? "";
        const dark = t.htmlStyle?.["--shiki-dark"] ?? "";
        const esc = t.content.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
        return `<span style="color:${light};--shiki-dark:${dark}">${esc}</span>`;
      })
      .join(""),
  );
}
