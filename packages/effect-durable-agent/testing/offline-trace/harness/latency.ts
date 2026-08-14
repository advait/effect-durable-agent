import type { OfflineTraceEvent } from "./trace-recorder";

/** Derived latency measurements for one model request in an offline trace. */
export interface InferenceLatency {
  readonly index: number;
  readonly firstPartLatencyMs?: number;
  readonly finishLatencyMs?: number;
}

/** Derive per-model-request first-part and finish latencies from trace timestamps. */
export const latencyFromTrace = (
  trace: ReadonlyArray<OfflineTraceEvent>,
): ReadonlyArray<InferenceLatency> => {
  const requests = trace.filter((event) => event.kind === "model.request");
  return requests.map((request) => {
    const index = traceIndex(request);
    const firstPart = trace.find(
      (event) => event.kind === "model.part" && traceIndex(event) === index,
    );
    const finish = trace.find(
      (event) => event.kind === "model.finish" && traceIndex(event) === index,
    );
    return {
      index,
      ...(firstPart === undefined ? {} : { firstPartLatencyMs: firstPart.atMs - request.atMs }),
      ...(finish === undefined ? {} : { finishLatencyMs: finish.atMs - request.atMs }),
    };
  });
};

const traceIndex = (event: OfflineTraceEvent): number => {
  const payload = event.payload;
  if (typeof payload === "object" && payload !== null && !Array.isArray(payload)) {
    const index = (payload as { readonly index?: unknown }).index;
    return typeof index === "number" ? index : -1;
  }
  return -1;
};
