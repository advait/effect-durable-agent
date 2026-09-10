import * as Schema from "effect/Schema";

import { CommandId, RunId, SequenceNumber, ToolCallId, ResumableId } from "../types/core";

/** A small durable continuation handle; external work never retains a live tool invocation. */
export const ResumableHandle = Schema.Struct({
  status: Schema.Literal("waiting"),
  resumableId: ResumableId,
});
export type ResumableHandle = typeof ResumableHandle.Type;

/** Extension-owned meaning and display text for a tool's durable wait boundary. */
export const OpenResumable = Schema.Struct({
  kind: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
  title: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
});
export type OpenResumable = typeof OpenResumable.Type;

/** Immutable origin, supplied by the runtime rather than model parameters. */
export const ResumableOpened = Schema.Struct({
  ...OpenResumable.fields,
  resumableId: ResumableId,
  runId: RunId,
  toolCallId: ToolCallId,
});
export type ResumableOpened = typeof ResumableOpened.Type;

/** Compact external result. Large artifacts remain in extension-owned storage by reference. */
export const ResolveResumable = Schema.Struct({
  resumableId: ResumableId,
  result: Schema.String.check(Schema.isMaxLength(16_384)),
});
export type ResolveResumable = typeof ResolveResumable.Type;

/** The resolution and its sole resume command are committed atomically. */
export const ResumableResolved = Schema.Struct({
  ...ResolveResumable.fields,
  commandId: CommandId,
});
export type ResumableResolved = typeof ResumableResolved.Type;

/** Stop/interrupt abandons external work and fences late resolutions. */
export const ResumableCancelled = Schema.Struct({
  resumableId: ResumableId,
  reason: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
});
export type ResumableCancelled = typeof ResumableCancelled.Type;

/** Current continuation state, derived solely from the session event log. */
export const ResumableRecord = Schema.Struct({
  ...ResumableOpened.fields,
  openedSeq: SequenceNumber,
  settlement: Schema.Union([
    Schema.TaggedStruct("Open", {}),
    Schema.TaggedStruct("Resolved", {
      result: ResolveResumable.fields.result,
      commandId: CommandId,
      seq: SequenceNumber,
    }),
    Schema.TaggedStruct("Cancelled", {
      reason: ResumableCancelled.fields.reason,
      seq: SequenceNumber,
    }),
  ]),
});
export type ResumableRecord = typeof ResumableRecord.Type;

/** Idempotent resolver outcome; settled or absent handles cannot start another run. */
export const ResumableResolution = Schema.Union([
  Schema.TaggedStruct("Resolved", { commandId: CommandId }),
  Schema.TaggedStruct("Cancelled", {}),
  Schema.TaggedStruct("Missing", {}),
]);
export type ResumableResolution = typeof ResumableResolution.Type;

/** Reject invalid tool wait ownership without committing external launch intent. */
export class ResumableOpenError extends Schema.TaggedErrorClass<ResumableOpenError>()(
  "ResumableOpenError",
  { message: Schema.String },
) {}

/** Bound simultaneously outstanding external continuations in one session. */
export const maxOpenResumables = 16;
