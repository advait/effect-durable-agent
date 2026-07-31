import type { OfflineTraceEvent } from "../harness/trace-recorder";

/** Aggregated provider cache-token metrics parsed from offline trace finishes. */
export interface OfflineTraceCacheMetrics {
  readonly inputTokens?: number;
  readonly cachedInputTokens?: number;
}

/** Aggregate cache-related token metrics from recorded model finish usage payloads. */
export const cacheMetricsFromTrace = (
  trace: ReadonlyArray<OfflineTraceEvent>,
): OfflineTraceCacheMetrics => {
  const totals: {
    inputTokens: number;
    cachedInputTokens: number;
  } = {
    inputTokens: 0,
    cachedInputTokens: 0,
  };
  let seen = false;
  for (const event of trace) {
    if (event.kind !== "model.finish") {
      continue;
    }
    const usage = objectField(event.payload, "usage");
    const inputTokens = objectField(usage, "inputTokens");
    totals.inputTokens +=
      numberField(inputTokens, "total") ?? numberField(usage, "inputTokens") ?? 0;
    totals.cachedInputTokens += numberField(inputTokens, "cacheRead") ?? 0;
    seen = true;
  }
  return seen ? totals : {};
};

const objectField = (value: unknown, key: string): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && key in value
    ? ((value as Record<string, unknown>)[key] as Record<string, unknown> | undefined)
    : undefined;

const numberField = (value: unknown, key: string): number | undefined => {
  if (typeof value !== "object" || value === null || !(key in value)) {
    return undefined;
  }
  const field = (value as Record<string, unknown>)[key];
  return typeof field === "number" ? field : undefined;
};
