import type { ProductId } from "./nav";

/**
 * Single-writer results of the v0.3.29 writer scale test
 * (documentation/common/scaling.md). `validated` is the highest 10-minute soak
 * that passed, `null` where none did; `knee` is where the ramp first failed.
 */
export type WriterResult = {
  vcpu: number;
  memoryGiB: number;
  validated: number | null;
  /** The lowest rate soaked, when none passed. */
  failedAt?: number;
  /** Encoded request bytes per second at the validated rate. */
  mibPerSecond?: number;
  knee: number;
  p99Ms?: number;
  meanCpu: number;
  peakRssGiB: number;
};

export type ProductCapacity = {
  unit: string;
  results: WriterResult[];
  /** Fraction of the validated rate to plan a writer at. */
  planning: number;
  recommended: { vcpu: number; memory: string; plan: number }[];
  /** Plan for the sizes no soak validated. */
  unvalidatedPlan?: { vcpu: number; plan: number };
};

export const PLANNING_FRACTION = 0.75;

export const CAPACITY: Record<ProductId, ProductCapacity> = {
  logs: {
    unit: "entries/s",
    results: [
      { vcpu: 2, memoryGiB: 8, validated: null, failedAt: 40_000, knee: 65_200, meanCpu: 1.3, peakRssGiB: 2.8 },
      { vcpu: 4, memoryGiB: 12, validated: 52_500, mibPerSecond: 34, knee: 85_400, p99Ms: 13, meanCpu: 1.7, peakRssGiB: 2.7 },
      { vcpu: 6, memoryGiB: 16, validated: 58_900, mibPerSecond: 39, knee: 95_900, p99Ms: 14, meanCpu: 1.8, peakRssGiB: 3.2 },
    ],
    planning: PLANNING_FRACTION,
    recommended: [{ vcpu: 4, memory: "8Gi", plan: 39_000 }],
    unvalidatedPlan: { vcpu: 2, plan: 25_000 },
  },
  metrics: {
    unit: "samples/s",
    results: [
      { vcpu: 2, memoryGiB: 8, validated: 406_000, mibPerSecond: 10, knee: 477_500, p99Ms: 161, meanCpu: 1.1, peakRssGiB: 0.9 },
      { vcpu: 4, memoryGiB: 12, validated: 599_000, mibPerSecond: 14, knee: 705_000, p99Ms: 60, meanCpu: 1.8, peakRssGiB: 1.2 },
      { vcpu: 6, memoryGiB: 16, validated: 644_000, mibPerSecond: 15, knee: 757_500, p99Ms: 70, meanCpu: 1.9, peakRssGiB: 1.7 },
    ],
    planning: PLANNING_FRACTION,
    recommended: [
      { vcpu: 2, memory: "6Gi", plan: 300_000 },
      { vcpu: 4, memory: "6Gi", plan: 450_000 },
    ],
  },
  traces: {
    unit: "spans/s",
    results: [
      { vcpu: 2, memoryGiB: 8, validated: null, failedAt: 17_300, knee: 28_200, meanCpu: 1.4, peakRssGiB: 1.1 },
      { vcpu: 4, memoryGiB: 12, validated: 28_400, mibPerSecond: 14, knee: 39_400, p99Ms: 38, meanCpu: 2.4, peakRssGiB: 1.5 },
      { vcpu: 6, memoryGiB: 16, validated: 26_800, mibPerSecond: 13, knee: 43_700, p99Ms: 25, meanCpu: 3.7, peakRssGiB: 1.9 },
    ],
    planning: PLANNING_FRACTION,
    recommended: [{ vcpu: 4, memory: "6Gi", plan: 21_000 }],
    unvalidatedPlan: { vcpu: 2, plan: 12_000 },
  },
};

/** Average Logs line in the scale test: 70% 250 B, 25% 1 KiB, 5% 4 KiB. */
export const LOGS_TEST_LINE_BYTES = 636;
