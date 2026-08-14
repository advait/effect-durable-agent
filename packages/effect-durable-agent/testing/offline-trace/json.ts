/** Primitive values accepted by offline trace JSON artifacts. */
export type JsonPrimitive = string | number | boolean | null;

/** Deterministic JSON-compatible value used in persisted trace artifacts. */
export type JsonValue =
  | JsonPrimitive
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };

/** Convert common runtime values into deterministic JSON-compatible data. */
export const toJsonValue = (value: unknown): JsonValue => toJsonValueWithSeen(value, new WeakSet());

const toJsonValueWithSeen = (value: unknown, seen: WeakSet<object>): JsonValue => {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : String(value);
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (value instanceof Uint8Array) {
    return { _tag: "Uint8Array", base64: bytesToBase64(value) };
  }
  if (value instanceof URL) {
    return value.toString();
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (Array.isArray(value)) {
    return value.map((entry) => toJsonValueWithSeen(entry, seen));
  }
  if (typeof value === "object") {
    if (seen.has(value)) {
      return "[Circular]";
    }
    seen.add(value);
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, entry]) => entry !== undefined && typeof entry !== "function")
      .sort(([left], [right]) => left.localeCompare(right));
    const object: Record<string, JsonValue> = {};
    for (const [key, entry] of entries) {
      object[key] = toJsonValueWithSeen(entry, seen);
    }
    seen.delete(value);
    return object;
  }
  return String(value);
};

/** Stable JSON stringify used for prompt/artifact hashing. */
export const stableJsonStringify = (value: unknown): string => JSON.stringify(toJsonValue(value));

const bytesToBase64 = (bytes: Uint8Array): string => {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return globalThis.btoa(binary);
};
