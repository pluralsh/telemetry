"use client";

import { createContext, useContext } from "react";
import { navFor, type NavGroup, type SpaceId } from "@/lib/nav";

const OperatorKinds = createContext<string[]>([]);

/** Provides the nav data only the server can load, such as the operator's resource kinds. */
export function NavDataProvider({ operatorKinds, children }: { operatorKinds: string[]; children: React.ReactNode }) {
  return <OperatorKinds.Provider value={operatorKinds}>{children}</OperatorKinds.Provider>;
}

export function useNav(space: SpaceId): NavGroup[] {
  return navFor(space, useContext(OperatorKinds));
}
