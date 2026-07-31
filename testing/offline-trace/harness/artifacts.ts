import * as Schema from "effect/Schema";

import { type JsonValue } from "../json";
import type { OfflineTraceEvent } from "./trace-recorder";

/** High-level status for an offline scenario run. */
export const OfflineTraceRunStatus = Schema.Literals(["completed", "failed", "blocked"]);
export type OfflineTraceRunStatus = typeof OfflineTraceRunStatus.Type;

/** Validated summary written alongside full offline trace artifacts. */
export const OfflineTraceRunSummary = Schema.Struct({
  runId: Schema.String,
  scenario: Schema.String,
  status: OfflineTraceRunStatus,
  startedAtMs: Schema.Number,
  finishedAtMs: Schema.Number,
  commandCount: Schema.Number,
  durableEventCount: Schema.Number,
  liveEventCount: Schema.Number,
  modelRequestCount: Schema.Number,
  promptPrefix: Schema.Struct({ checked: Schema.Number, failures: Schema.Array(Schema.String) }),
  cacheMetrics: Schema.Struct({
    inputTokens: Schema.optionalKey(Schema.Number),
    cachedInputTokens: Schema.optionalKey(Schema.Number),
  }),
});
export type OfflineTraceRunSummary = typeof OfflineTraceRunSummary.Type;

/** Complete in-memory artifact bundle produced by one offline trace run. */
export interface OfflineTraceArtifacts {
  readonly summary: OfflineTraceRunSummary;
  readonly trace: ReadonlyArray<OfflineTraceEvent>;
  readonly durableEvents: ReadonlyArray<JsonValue>;
  readonly liveEvents: ReadonlyArray<JsonValue>;
  readonly prompts: ReadonlyArray<OfflineTracePromptArtifact>;
}

/** Canonical prompt capture used for prefix/cache verification. */
export interface OfflineTracePromptArtifact {
  readonly index: number;
  readonly promptHash: string;
  readonly prompt: JsonValue;
}

/** Validate the summary shape before writing or consuming an offline run. */
export const makeRunSummary = (summary: OfflineTraceRunSummary): OfflineTraceRunSummary =>
  OfflineTraceRunSummary.make(summary);
