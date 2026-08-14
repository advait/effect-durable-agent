export const celldVersion: string;

/** Download and verify the pinned celld binary used by host conformance tests. */
export const installCelld: (destination?: string) => Promise<string>;
