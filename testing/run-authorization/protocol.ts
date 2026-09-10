import * as Schema from "effect/Schema";
import {
  RunSchedulingRequest,
  RunSchedulingRequestRecord,
} from "effect-durable-agent/domain/run-scheduling";
import {
  CommandId,
  MessageId,
  RunId,
  SequenceNumber,
  SessionId,
} from "effect-durable-agent/types/core";

/** Durable handoff accepted by the manual test authorizer. */
export const AuthorizationHandoff = Schema.Struct({
  sessionId: SessionId,
  request: RunSchedulingRequest,
});
export type AuthorizationHandoff = typeof AuthorizationHandoff.Type;

/** Test-authorizer evidence: duplicate delivery increments attempts without changing the handoff. */
export const AcceptedAuthorization = Schema.Struct({
  handoff: AuthorizationHandoff,
  deliveries: Schema.Int.check(Schema.isGreaterThan(0)),
});
export type AcceptedAuthorization = typeof AcceptedAuthorization.Type;

/** Compact shared wire contract for the local and remote verification drivers. */
export const AuthorizationSnapshot = Schema.Struct({
  lastSeq: SequenceNumber,
  request: Schema.optionalKey(RunSchedulingRequestRecord),
  runCount: Schema.Int,
  completedRunCount: Schema.Int,
  cancelledCommandIds: Schema.Array(CommandId),
  pendingMessageCount: Schema.Int,
  pendingMessageIds: Schema.Array(MessageId),
  pausedMessageCount: Schema.Int,
  recovery: Schema.Array(
    Schema.Struct({ commandId: CommandId, interruptedRunId: RunId, replacementRunId: RunId }),
  ),
});
export type AuthorizationSnapshot = typeof AuthorizationSnapshot.Type;
