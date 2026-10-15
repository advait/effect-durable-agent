import * as Schema from "effect/Schema";
import { EDACommand, GrantRunCommand } from "effect-durable-agent/types/commands";
import {
  CommandId,
  CompactionId,
  RunRequestId,
  SequenceNumber,
  SessionId,
  SummaryId,
} from "effect-durable-agent/types/core";
import { EDATraceMetadata } from "effect-durable-agent/types/tracing";
import { CommittedDurableEvent } from "effect-durable-agent/services/session-store";
import { CommittedCommandTerminalEvent } from "effect-durable-agent/services/runtime";
import {
  EDASessionSnapshot,
  DurableTranscriptMessages,
} from "effect-durable-agent/services/session-query";
import { RunGrantResult, RunRequestOutcome } from "effect-durable-agent/domain/run-scheduling";
import {
  TranscriptBodySource,
  TranscriptLogIncarnation,
  TranscriptWindow,
  TranscriptWindowLimits,
  TranscriptWindowSelector,
} from "effect-durable-agent/transcript";
import { TranscriptStorageStatus } from "./durable-object-transcript";
import { EDARpcSubmittable } from "./rpc-codec";

/** Names the session isolation boundary and carries caller trace metadata. */
export const SessionScopedInput = Schema.Struct({ sessionId: SessionId, trace: EDATraceMetadata });
/** Ordinary command admission; controller validation excludes trusted scheduler grants. */
export const SessionCommandInput = Schema.Struct({
  ...SessionScopedInput.fields,
  command: EDACommand,
});
/** Trusted scheduler permission for a reserved run; ordinary command ingress cannot grant it. */
export const SessionGrantRunInput = Schema.Struct({
  ...SessionScopedInput.fields,
  command: GrantRunCommand,
});
/** Atomic ordered admission of ordinary commands and application events for one session. */
export const SessionBatchInput = Schema.Struct({
  ...SessionScopedInput.fields,
  items: Schema.Array(EDARpcSubmittable),
});
/** Waits for one command terminal after an optional durable replay position. */
export const SessionBlockInput = Schema.Struct({
  ...SessionScopedInput.fields,
  afterSeq: Schema.optionalKey(SequenceNumber),
  commandId: CommandId,
});
/** Reconciles a scheduler request against the named session’s authoritative state. */
export const SessionOutcomeInput = Schema.Struct({
  ...SessionScopedInput.fields,
  requestId: RunRequestId,
});
/** A runtime-free transcript window read; `limits` defaults to the transcript window defaults. */
export const SessionTranscriptWindowInput = Schema.Struct({
  ...SessionScopedInput.fields,
  selector: TranscriptWindowSelector,
  limits: Schema.optionalKey(TranscriptWindowLimits),
});
/**
 * A window; a cursor minted for another incarnation or version (the client takes `Latest`); or a
 * session the host has not enabled for transcripts.
 */
export const SessionTranscriptWindowResult = Schema.Union([
  Schema.TaggedStruct("Window", { window: TranscriptWindow }),
  Schema.TaggedStruct("CursorMismatch", { message: Schema.String }),
  /** The host has not enabled transcript maintenance for this session. */
  Schema.TaggedStruct("Disabled", {}),
]);
/** A runtime-free read of the body a transcript `Ref` names, by its immutable log source. */
export const SessionTranscriptBodyInput = Schema.Struct({
  ...SessionScopedInput.fields,
  source: TranscriptBodySource,
  /**
   * The log incarnation the reference was minted in (the window's `incarnation`). Required: a
   * wiped log invalidates every reference, so a reference never resolves against another log.
   */
  incarnation: TranscriptLogIncarnation,
});
/** The body as the JSON the interpreter would have inlined, or why the log has none there. */
export const SessionTranscriptBodyResult = Schema.Union([
  Schema.TaggedStruct("Body", { value: Schema.Json }),
  Schema.TaggedStruct("NotFound", { message: Schema.String }),
  /** The reference names another log: the client takes a new window. */
  Schema.TaggedStruct("IncarnationMismatch", { incarnation: TranscriptLogIncarnation }),
  /** The session is outside the host's transcript rollout. */
  Schema.TaggedStruct("Disabled", {}),
]);
/** A runtime-free read of the summary a transcript `compaction` record references. */
export const SessionTranscriptSummaryInput = Schema.Struct({
  ...SessionScopedInput.fields,
  summaryId: SummaryId,
  /** The log incarnation the compaction record was read from (the window's `incarnation`). */
  incarnation: TranscriptLogIncarnation,
});
/** The user-visible part of one compaction summary: its text and the log span it summarizes. */
export const TranscriptCompactionSummary = Schema.Struct({
  compactionId: CompactionId,
  summaryId: SummaryId,
  text: Schema.String,
  sourceFromSeq: SequenceNumber,
  sourceToSeq: SequenceNumber,
});
export type TranscriptCompactionSummary = typeof TranscriptCompactionSummary.Type;
/** The summary, or `NotFound` when this log has no summary with that id. */
export const SessionTranscriptSummaryResult = Schema.Union([
  Schema.TaggedStruct("Summary", { summary: TranscriptCompactionSummary }),
  Schema.TaggedStruct("NotFound", { message: Schema.String }),
  Schema.TaggedStruct("IncarnationMismatch", { incarnation: TranscriptLogIncarnation }),
  Schema.TaggedStruct("Disabled", {}),
]);
/** Private session RPC contracts retain the deployed structured-clone shapes, including snapshot Maps. */
export const edaSessionRpc = {
  submit: { input: SessionCommandInput, output: CommittedDurableEvent },
  submitBatch: { input: SessionBatchInput, output: Schema.Array(CommittedDurableEvent) },
  submitAndBlock: { input: SessionCommandInput, output: CommittedCommandTerminalEvent },
  blockOnCommand: { input: SessionBlockInput, output: CommittedCommandTerminalEvent },
  grantRun: { input: SessionGrantRunInput, output: RunGrantResult },
  runRequestOutcome: { input: SessionOutcomeInput, output: RunRequestOutcome },
  snapshot: { input: SessionScopedInput, output: EDASessionSnapshot },
  messages: { input: SessionScopedInput, output: DurableTranscriptMessages },
  destroySession: { input: SessionScopedInput, output: Schema.Void },
  transcriptStatus: { input: SessionScopedInput, output: TranscriptStorageStatus },
  readTranscriptWindow: {
    input: SessionTranscriptWindowInput,
    output: SessionTranscriptWindowResult,
  },
  readTranscriptBody: { input: SessionTranscriptBodyInput, output: SessionTranscriptBodyResult },
  readTranscriptSummary: {
    input: SessionTranscriptSummaryInput,
    output: SessionTranscriptSummaryResult,
  },
};
