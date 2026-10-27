import { createContext, useContext } from "react";

export type StepState = {
  step: number;
  steps: number;
  /** False for thumbnails (overview, presenter "next" preview), which should render statically. */
  live: boolean;
};

export const StepContext = createContext<StepState>({ step: 0, steps: 0, live: true });

export const useStep = () => useContext(StepContext);
