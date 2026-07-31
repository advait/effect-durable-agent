import * as Prompt from "effect/unstable/ai/Prompt";
import * as Schema from "effect/Schema";

import { CompactionId, ContextVersion, SequenceNumber, SummaryId } from "../types/core";

/** Stable identifier for the compaction policy that selected a summary plan. */
export const CompactionPolicyId = Schema.NonEmptyString.pipe(Schema.brand("CompactionPolicyId"));
export type CompactionPolicyId = typeof CompactionPolicyId.Type;

/** Stable identifier for the executor that produced a summary artifact. */
export const CompactionExecutorId = Schema.NonEmptyString.pipe(
  Schema.brand("CompactionExecutorId"),
);
export type CompactionExecutorId = typeof CompactionExecutorId.Type;

/** Authoritative cumulative model-context summary written with `SummaryCreated`. */
export const CompactionSummaryArtifact = Schema.Struct({
  compactionId: CompactionId,
  summaryId: SummaryId,
  previousSummaryId: Schema.optionalKey(SummaryId),
  sourceFromSeq: SequenceNumber,
  sourceToSeq: SequenceNumber,
  retainedFromContextSeq: SequenceNumber,
  text: Schema.NonEmptyString,
  promptMessage: Prompt.UserMessage,
  policyId: CompactionPolicyId,
  executorId: Schema.optionalKey(CompactionExecutorId),
});
export type CompactionSummaryArtifact = typeof CompactionSummaryArtifact.Type;

/** Current durable context cursor projected from framework reducer state and summary rows. */
export const ContextProjection = Schema.Struct({
  contextVersion: ContextVersion,
  currentSummary: Schema.optionalKey(CompactionSummaryArtifact),
});
export type ContextProjection = typeof ContextProjection.Type;

/** Genesis context before any compaction has rebased prompt history. */
export const emptyContextProjection = ContextProjection.make({
  contextVersion: ContextVersion.make(0),
});
