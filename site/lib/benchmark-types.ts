export const PERCENTILES = ["p50", "p90", "p99", "p99.9", "max"] as const;
export type Percentile = (typeof PERCENTILES)[number];
export type Latency = Partial<Record<Percentile, number>>;

export type BenchFamily = {
  name: string;
  cases: number;
  nonMatch: number;
  impl: Latency;
  oracle: Latency;
  ratioP50?: number;
};

export type BenchUsage = { cpuMean: number; cpuP95: number; memMean: number; memP95: number };

/** One side of a cold-read family: latency on a freshly restarted process, and the object-store reads that query made. */
export type ColdSide = { cold: Latency; warmP50?: number; getP50?: number; getBytesP50?: number };

export type ColdFamily = { name: string; cases: number; impl: ColdSide; oracle: ColdSide };

export type ColdStartup = { restarts: number; readyP50?: number; getP50?: number };

export type BenchCold = {
  spanS: number;
  repeats: number;
  firstByteMs: number;
  planned: number;
  measured: number;
  dataset: Record<string, number>;
  families: ColdFamily[];
  totals: { impl: ColdSide; oracle: ColdSide };
  startup: { impl: ColdStartup; oracle: ColdStartup };
};

export type BenchRun = {
  name: string;
  startedAt: string;
  durationS: number;
  commit: string;
  commitUrl: string;
  dirty: boolean;
  branch: string;
  scenario: string;
  oracle: string;
  /** Runs on the same track (scenario and oracle) are comparable over time. */
  track: string;
  storage: string;
  status: "passed" | "failed";
  reasons: string[];
  cases: number;
  nonMatch: number;
  outcomes: Record<string, number>;
  query: { impl: Latency; oracle: Latency; ratio: Latency };
  ingest: { batches: number; impl: Latency; oracle: Latency };
  families: BenchFamily[];
  usage: { impl: BenchUsage; oracle: BenchUsage };
  host: string;
  markdownUrl: string;
  /** Only `cold`-scenario runs: every measured query hit a restarted process with empty caches. */
  cold?: BenchCold;
};
