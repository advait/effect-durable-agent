import * as Schema from "effect/Schema";
import { EDACommand, GrantRunCommand } from "effect-durable-agent/types/commands";
import {
  CommandId,
  RunRequestId,
  SequenceNumber,
  SessionId,
} from "effect-durable-agent/types/core";
import { EDATraceMetadata } from "effect-durable-agent/types/tracing";
import { CommittedDurableEvent } from "effect-durable-agent/services/session-store";
import { CommittedCommandTerminalEvent } from "effect-durable-agent/services/runtime";
import {
  EDASessionSnapshot,
  DurableTranscriptMessages,
} from "effect-durable-agent/services/session-query";
import { RunGrantResult, RunRequestOutcome } from "effect-durable-agent/domain/run-scheduling";
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
};
