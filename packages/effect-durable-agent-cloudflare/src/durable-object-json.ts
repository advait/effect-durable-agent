/** Conservative JSON limit below the Durable Object SQLite per-value ceiling (2 MB). */
export const durableObjectSerializedJsonHardCapBytes = 1_500_000;

const jsonEncoder = new TextEncoder();

/** Serialize and size-check one JSON column before it reaches DO SQLite. */
export const stringifyJsonForColumn = (value: unknown, column: string): string => {
  const json = JSON.stringify(value);
  if (json === undefined) {
    throw new Error(`${column} value is not JSON serializable`);
  }
  const bytes = jsonEncoder.encode(json).byteLength;
  if (bytes > durableObjectSerializedJsonHardCapBytes) {
    throw new Error(
      `${column} serialized JSON is ${bytes} bytes; hard cap is ${durableObjectSerializedJsonHardCapBytes} bytes`,
    );
  }
  return json;
};
