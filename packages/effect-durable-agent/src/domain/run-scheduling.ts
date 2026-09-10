import * as Schema from "effect/Schema";

import {
  CommandId,
  MessageId,
  RunId,
  RunRequestId,
  SequenceNumber,
  type SessionId,
} from "../types/core";
import type { PendingCommand, ReducedState } from "./reduced-state";

/**
 * Identity of the admitted work selected by dispatch or recovery for a new run.
 * This is not a durable request identity: recovery can resolve the same command again.
 */
export interface RunSchedulingInput {
  readonly sessionId: SessionId;
  readonly commandId: CommandId;
}

/**
 * Permission to start the selected run in this session's current runtime.
 * This value is never persisted and allocates no scheduling identity.
 */
export const LocalRunResolution = Schema.TaggedStruct("Local", {});
export type LocalRunResolution = typeof LocalRunResolution.Type;

/** Intent to request external permission; resolution itself must perform no delivery. */
export const DeferredRunResolution = Schema.TaggedStruct("Deferred", {});
export type DeferredRunResolution = typeof DeferredRunResolution.Type;

/** Prompt selection result interpreted by SessionState before any run is started. */
export const RunResolution = Schema.Union([LocalRunResolution, DeferredRunResolution]);
export type RunResolution = typeof RunResolution.Type;

/** Selected work and the durable predecessor needed to authorize a recovery replacement. */
export const RunSchedulingWork = Schema.Union([
  Schema.TaggedStruct("Command", { commandId: CommandId }),
  Schema.TaggedStruct("Recovery", {
    commandId: CommandId,
    interruptedRunId: RunId,
    inputMessageIds: Schema.Array(MessageId),
  }),
]);
export type RunSchedulingWork = typeof RunSchedulingWork.Type;

/** One run-slot reservation. External schedulers must deduplicate delivery by requestId. */
export const RunSchedulingRequest = Schema.Struct({
  requestId: RunRequestId,
  work: RunSchedulingWork,
});
export type RunSchedulingRequest = typeof RunSchedulingRequest.Type;

/** Current reservation, folded solely from request and successful delivery facts. */
export const RunSchedulingRequestRecord = Schema.Struct({
  ...RunSchedulingRequest.fields,
  requestedSeq: SequenceNumber,
  deliveredSeq: Schema.optionalKey(SequenceNumber),
});
export type RunSchedulingRequestRecord = typeof RunSchedulingRequestRecord.Type;

/** A stale/duplicate permission is harmless and cannot allocate another run. */
export const RunGrantResult = Schema.Union([
  Schema.TaggedStruct("Granted", { runId: RunId }),
  Schema.TaggedStruct("Stale", {}),
]);
export type RunGrantResult = typeof RunGrantResult.Type;

/** Durable answer for reconciling an ambiguous grant delivery. Unknown never authorizes replacement work. */
export const RunRequestOutcome = Schema.Union([
  Schema.TaggedStruct("Unknown", {}),
  Schema.TaggedStruct("Waiting", {}),
  Schema.TaggedStruct("Invalidated", {}),
  Schema.TaggedStruct("Granted", {
    runId: RunId,
    status: Schema.Literals(["Running", "Completed", "Failed", "Interrupted"]),
  }),
]);
export type RunRequestOutcome = typeof RunRequestOutcome.Type;

/** Canonical records consulted by authorization policy, without an Effect-layer snapshot. */
type SchedulingState = Pick<
  ReducedState,
  "commands" | "runs" | "messages" | "commandQueues" | "runSchedulingRequest" | "resumables"
>;

/** Still-unconsumed inputs owned by a reservation, derived from the existing message records. */
export const runSchedulingInputMessageIds = (
  state: SchedulingState,
  work: RunSchedulingWork,
): ReadonlyArray<MessageId> => {
  const command = state.commands.get(work.commandId)?.command;
  const candidates =
    work._tag === "Recovery"
      ? work.inputMessageIds
      : command?._tag === "ResumePendingMessages"
        ? command.messageIds
        : Array.from(state.messages.values()).flatMap((message) =>
            (message._tag === "User" || message._tag === "Steering") &&
            message.commandId === work.commandId
              ? [message.messageId]
              : [],
          );
  return candidates.filter((messageId) => {
    const message = state.messages.get(messageId);
    return (
      (message?._tag === "User" || message?._tag === "Steering") &&
      message.consumedSeq === undefined &&
      message.cancelledSeq === undefined &&
      message.pausedByCommandId === undefined
    );
  });
};

/** Revalidate durable ownership and pending controls immediately before granting permission. */
export const canGrantRun = (state: SchedulingState, requestId: RunRequestId): boolean => {
  const request = state.runSchedulingRequest;
  if (request?.requestId !== requestId) return false;
  const command = state.commands.get(request.work.commandId);
  if (command?.command === undefined || command.terminal !== undefined) return false;
  if (Array.from(state.runs.values()).some((run) => run.terminal === undefined)) return false;
  const inputs = runSchedulingInputMessageIds(state, request.work);
  if (request.work._tag === "Command") {
    if (command.startedSeq !== undefined) return false;
    // Older admitted commands did not commit their User message until run start.
    const legacySubmit =
      command.command._tag === "SubmitMessage" &&
      !Array.from(state.messages.values()).some(
        (message) =>
          (message._tag === "User" || message._tag === "Steering") &&
          message.commandId === command.commandId,
      );
    const resumable =
      command.command._tag === "ResumeResumable"
        ? state.resumables.get(command.command.resumableId)
        : undefined;
    const resume =
      resumable?.settlement._tag === "Resolved" &&
      resumable.settlement.commandId === command.commandId;
    if (inputs.length === 0 && !legacySubmit && !resume) return false;
  } else {
    const predecessor = state.runs.get(request.work.interruptedRunId);
    if (
      command.startedSeq === undefined ||
      predecessor?.terminal === undefined ||
      !predecessor.commandIds.includes(command.commandId) ||
      (request.work.inputMessageIds.length > 0 && inputs.length === 0)
    )
      return false;
  }
  return !state.commandQueues.activeControlCommands.some((pending) => {
    if (pending.commandId === command.commandId) return false;
    switch (pending.command._tag) {
      case "StopTurn":
        return true;
      case "SubmitMessage":
        return pending.command.disposition === "interrupt";
      case "CancelPendingMessage":
        return inputs.includes(pending.command.messageId);
      default:
        return false;
    }
  });
};

/** Controls may change waiting work; later messages cannot take its reserved slot. */
export const waitingRunControl = (state: SchedulingState): PendingCommand | undefined => {
  const owner = state.runSchedulingRequest?.work.commandId;
  const controls = state.commandQueues.activeControlCommands.filter(
    (pending) =>
      pending.commandId !== owner &&
      (pending.command._tag !== "SubmitMessage" || pending.command.disposition === "interrupt"),
  );
  const priority = (pending: PendingCommand): number => {
    switch (pending.command._tag) {
      case "StopTurn":
        return 0;
      case "CancelPendingMessage":
        return 1;
      case "PromotePendingMessage":
        return 2;
      default:
        return 3;
    }
  };
  return controls.sort((a, b) => priority(a) - priority(b) || a.admittedSeq - b.admittedSeq)[0];
};

/** Atomic request changes required when one of its selected messages is cancelled. */
export interface RunSchedulingCancellationPlan {
  readonly requestId: RunRequestId;
  readonly cancelCommandId?: CommandId;
  readonly replacementWork?: RunSchedulingWork;
}

/** Preserve a recovery predecessor when cancellation leaves other selected inputs eligible. */
export const planRunSchedulingCancellation = (
  state: SchedulingState,
  messageId: MessageId,
): RunSchedulingCancellationPlan | undefined => {
  const request = state.runSchedulingRequest;
  if (request === undefined) return undefined;
  const inputs = runSchedulingInputMessageIds(state, request.work);
  if (!inputs.includes(messageId)) return undefined;
  if (request.work._tag === "Command") return { requestId: request.requestId };
  const remainingInputs = inputs.filter((id) => id !== messageId);
  return remainingInputs.length === 0
    ? { requestId: request.requestId, cancelCommandId: request.work.commandId }
    : {
        requestId: request.requestId,
        replacementWork: { ...request.work, inputMessageIds: remainingInputs },
      };
};

/** Stop/interrupt facts that invalidate authorization and pause pending work before any run. */
export const planWaitingRunInterruption = (state: SchedulingState, controlCommandId: CommandId) => {
  const request = state.runSchedulingRequest;
  if (request === undefined) return undefined;
  return {
    requestId: request.requestId,
    messageIds: [...state.commandQueues.pendingQueue, ...state.commandQueues.pendingSteers]
      .filter((message) => message.commandId !== controlCommandId)
      .map((message) => message.messageId),
    cancelCommandId: request.work.commandId,
  };
};
