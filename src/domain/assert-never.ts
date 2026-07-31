const formatNeverValue = (value: never): string => {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

/** Build the defect used when an exhaustive switch receives an impossible value. */
export const assertNeverError = (value: never, context: string) =>
  new Error(`Unhandled ${context}: ${formatNeverValue(value)}`);

/** Throw for impossible branches in pure code after TypeScript exhaustiveness checking. */
export const assertNever = (value: never, context: string): never => {
  throw assertNeverError(value, context);
};
