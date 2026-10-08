import { highlightLines } from "@/lib/highlight";
import { AnnotatedYamlView, type YamlNote } from "./annotated-yaml-view";

const KEY_LINE = /^(\s*)(- )?([\w.@/-]+):/;

/** Dotted key path for every line that opens a mapping key; list items join their parent's path. */
export function yamlKeyPaths(code: string): (string | undefined)[] {
  const stack: { indent: number; key: string }[] = [];
  return code.split("\n").map((line) => {
    const m = line.match(KEY_LINE);
    if (!m) return undefined;
    const indent = m[1].length + (m[2] ? 2 : 0);
    while (stack.length && stack[stack.length - 1].indent >= indent) stack.pop();
    stack.push({ indent, key: m[3] });
    return stack.map((s) => s.key).join(".");
  });
}

/**
 * YAML with numbered notes attached by key path (`spec.writer.replicas`). Paths without a matching
 * line are ignored, so one notes map can serve several specs of the same resource; in multi-document
 * YAML only a path's first occurrence is annotated.
 */
export async function AnnotatedYaml({ code, notes, title }: { code: string; notes: Record<string, string>; title?: string }) {
  const src = code.replace(/\n$/, "");
  const paths = yamlKeyPaths(src);
  const lines = await highlightLines(src, "yaml");
  const annotated: YamlNote[] = [];
  const seen = new Set<string>();
  paths.forEach((path, line) => {
    if (!path || !notes[path] || seen.has(path)) return;
    seen.add(path);
    annotated.push({ n: annotated.length + 1, line, key: path.split(".").pop()!, path, text: notes[path] });
  });
  return <AnnotatedYamlView code={src} lines={lines} notes={annotated} title={title} />;
}
