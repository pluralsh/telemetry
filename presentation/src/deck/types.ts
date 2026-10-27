import type { ComponentType } from "react";

export type SlideDef = {
  id: string;
  /** Outline section shown in the footer, e.g. "Why self-hosted still sucks". */
  section: string;
  title: string;
  /** Number of build steps after the initial state. 0 means the slide is static. */
  steps?: number;
  notes?: string;
  Component: ComponentType;
};

export type Position = { slide: number; step: number };
