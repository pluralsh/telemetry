import type { SpaceId } from "@/lib/nav";

export const SPACE_COLORS: Record<SpaceId, string> = {
  overview: "var(--ink)",
  metrics: "var(--ink)",
  logs: "var(--ink)",
  traces: "var(--ink)",
};

type IconProps = { size?: number; className?: string };

const stroke = {
  stroke: "currentColor",
  strokeWidth: 1.5,
  fill: "none",
  strokeLinecap: "round",
  strokeLinejoin: "round",
} as const;

/** Minimal monochrome glyphs drawn on a 24-unit grid. */
export const GLYPHS: Record<SpaceId, React.ReactNode> = {
  metrics: (
    <>
      <path d="M3.5 17.5 8.5 12l4 3 7.5-9" {...stroke} />
      <circle cx="3.5" cy="17.5" r="1.25" fill="currentColor" />
      <circle cx="8.5" cy="12" r="1.25" fill="currentColor" />
      <circle cx="12.5" cy="15" r="1.25" fill="currentColor" />
      <circle cx="20" cy="6" r="1.25" fill="currentColor" />
    </>
  ),
  logs: (
    <>
      <path d="M7.5 6h13M7.5 12h10M7.5 18h7" {...stroke} />
      <circle cx="3.5" cy="6" r="1.25" fill="currentColor" />
      <circle cx="3.5" cy="12" r="1.25" fill="currentColor" />
      <circle cx="3.5" cy="18" r="1.25" fill="currentColor" />
    </>
  ),
  traces: (
    <>
      <path d="M4 5.5h6M7 5.5V12h6M10 12v6.5h6" {...stroke} />
      <circle cx="4" cy="5.5" r="1.25" fill="currentColor" />
      <circle cx="10" cy="5.5" r="1.25" fill="currentColor" />
      <circle cx="13" cy="12" r="1.25" fill="currentColor" />
      <circle cx="16" cy="18.5" r="1.25" fill="currentColor" />
    </>
  ),
  overview: null,
};

/** The official Plural logomark (pluralsh/design-system), on an 86-unit grid. */
function PluralPaths({ fill }: { fill: string }) {
  return (
    <g fill={fill}>
      <path d="m0 4.62366v81.37634h13.4086v-70.5373c0-1.5321 1.242-2.7742 2.7742-2.7742h43v-12.6885h-54.55914c-2.55358 0-4.62366 2.07008-4.62366 4.62366z" />
      <path d="m42.7688 59.414c9.0652 0 16.414-7.3488 16.414-16.414s-7.3488-16.414-16.414-16.414-16.4139 7.3488-16.4139 16.414 7.3487 16.414 16.4139 16.414z" />
      <path d="m86 81.3763v-81.3763h-13.4086v70.5373c0 1.5321-1.242 2.7742-2.7742 2.7742h-43v12.6885h54.5591c2.5536 0 4.6237-2.0701 4.6237-4.6237z" />
    </g>
  );
}

export function Badge({ space, size = 20, className }: IconProps & { space: SpaceId }) {
  if (space === "overview") {
    return (
      <svg width={size} height={size} viewBox="-11 -11 108 108" fill="none" aria-hidden className={`text-ink ${className ?? ""}`}>
        <PluralPaths fill="currentColor" />
      </svg>
    );
  }
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      aria-hidden
      className={`text-ink ${className ?? ""}`}
    >
      {GLYPHS[space]}
    </svg>
  );
}

export const MetricsIcon = (p: IconProps) => <Badge space="metrics" {...p} />;
export const LogsIcon = (p: IconProps) => <Badge space="logs" {...p} />;
export const TracesIcon = (p: IconProps) => <Badge space="traces" {...p} />;
export const TelemetryIcon = (p: IconProps) => <Badge space="overview" {...p} />;

export function SpaceIcon({ space, ...props }: IconProps & { space: SpaceId }) {
  return <Badge space={space} {...props} />;
}

export function PluralMark({ size = 14, className }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 86 86" fill="none" aria-hidden className={className}>
      <PluralPaths fill="currentColor" />
    </svg>
  );
}

export function GithubIcon({ size = 16 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="currentColor" aria-hidden>
      <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27s1.36.09 2 .27c1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0 0 16 8c0-4.42-3.58-8-8-8Z" />
    </svg>
  );
}

export function ArrowRight({ size = 12 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 12 12" fill="none" aria-hidden>
      <path d="M4.5 2.5 8 6l-3.5 3.5" stroke="currentColor" strokeWidth="1.25" />
    </svg>
  );
}
