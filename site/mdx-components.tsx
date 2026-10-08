import type { MDXComponents } from "mdx/types";
import Link from "next/link";
import { isValidElement } from "react";
import { CodeBlock } from "@/components/code-block";

function Pre({ children }: { children?: React.ReactNode }) {
  if (isValidElement<{ className?: string; children?: string; title?: string }>(children)) {
    const { className = "", children: code = "" } = children.props;
    const lang = className.replace("language-", "");
    return <CodeBlock code={String(code)} lang={lang} />;
  }
  return <pre>{children}</pre>;
}

const components: MDXComponents = {
  pre: Pre,
  a: ({ href = "", children, ...rest }) =>
    href.startsWith("/") ? (
      <Link href={href} {...rest}>
        {children}
      </Link>
    ) : (
      <a href={href} target={href.startsWith("#") ? undefined : "_blank"} rel="noreferrer" {...rest}>
        {children}
      </a>
    ),
};

export function useMDXComponents(): MDXComponents {
  return components;
}
