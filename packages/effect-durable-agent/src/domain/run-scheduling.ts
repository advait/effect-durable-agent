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
  "commands" | "runs" | "messages" | "commandQueues" | "runSchedulingRequest"
>;

/** A work's selected messages: those a run would consume, and those a pause still holds. */
const runSchedulingInputs = (state: SchedulingState, work: RunSchedulingWork) => {
  const command = state.commands.get(work.commandId)?.command;
  const resumed: ReadonlyArray<MessageId> =
    command?._tag === "ResumePendingMessages" ? command.messageIds : [];
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
  const eligible: Array<MessageId> = [];
  const held: Array<MessageId> = [];
  for (const messageId of candidates) {
    const message = state.messages.get(messageId);
    if (
      (message?._tag !== "User" && message?._tag !== "Steering") ||
      message.consumedSeq !== undefined ||
      message.cancelledSeq !== undefined
    )
      continue;
    if (message.pausedByCommandId === undefined || resumed.includes(messageId))
      eligible.push(messageId);
    else held.push(messageId);
  }
  return { eligible, held };
};

/**
 * The messages a run for this work would consume: its selected user and steering messages that
 * are neither consumed nor cancelled. Dispatch, grant, cancellation and startup recovery all read
 * this one function, so dispatch never reserves a run that grant rejects for want of inputs,
 * cancelling an input always finds the reservation that owns it, and a replacement run selects
 * the inputs its interrupted run would have consumed.
 *
 * A paused message is an input only of the `ResumePendingMessages` that names it, for that
 * command's own work and for a recovery of it alike, because resuming paused messages is that
 * command's purpose: a host's interrupt-with-pending-message submits
 * `[StopTurn, ResumePendingMessages([m])]`, and the Stop pauses `m` before the Resume runs. Until
 * a turn consumes `m` it stays paused, so a run that dies before its first turn leaves it paused
 * for recovery. For any other work, and for any message the Resume does not name, a pause holds
 * the message until the user promotes or resumes it.
 */
export const runSchedulingInputMessageIds = (
  state: SchedulingState,
  work: RunSchedulingWork,
): ReadonlyArray<MessageId> => runSchedulingInputs(state, work).eligible;

/**
 * Whether a reservation's work can start a run now. Grant starts a run only from `Grantable`,
 * whose inputs are the ones the run consumes.
 *
 * `Blocked` clears without intervention: a non-terminal run finishes, a pending Stop, interrupt
 * or cancellation of an input resolves the reservation itself, or a promotion releases a paused
 * input. It is checked first, so a pending control always decides its reservation's fate.
 *
 * `Ineligible` never clears, because every fact behind it is absorbing: an owner command that is
 * missing, terminal, or (for ordinary work) already started; a recovery predecessor that does not
 * match; inputs that were all consumed or cancelled. The session retires such a reservation
 * rather than leave every later message waiting behind it.
 */
export const RunGrantability = Schema.Union([
  Schema.TaggedStruct("Grantable", { inputMessageIds: Schema.Array(MessageId) }),
  Schema.TaggedStruct("Blocked", {
    reason: Schema.Literals(["active-run", "pending-control", "paused-inputs"]),
  }),
  Schema.TaggedStruct("Ineligible", {
    reason: Schema.Literals([
      "owner-missing",
      "owner-terminal",
      "owner-started",
      "owner-not-started",
      "predecessor-mismatch",
      "inputs-gone",
    ]),
  }),
]);
export type RunGrantability = typeof RunGrantability.Type;

const blocked = (reason: Extract<RunGrantability, { _tag: "Blocked" }>["reason"]) =>
  ({ _tag: "Blocked", reason }) as const;
const ineligible = (reason: Extract<RunGrantability, { _tag: "Ineligible" }>["reason"]) =>
  ({ _tag: "Ineligible", reason }) as const;

/** Classify a reservation's work against durable ownership, runs and pending controls. */
export const runGrantability = (
  state: SchedulingState,
  work: RunSchedulingWork,
): RunGrantability => {
  const inputs = runSchedulingInputs(state, work);
  if (Array.from(state.runs.values()).some((run) => run.terminal === undefined))
    return blocked("active-run");
  const controlPending = state.commandQueues.activeControlCommands.some((pending) => {
    if (pending.commandId === work.commandId) return false;
    switch (pending.command._tag) {
      case "StopTurn":
        return true;
      case "SubmitMessage":
        return pending.command.disposition === "interrupt";
      case "CancelPendingMessage":
        return inputs.eligible.includes(pending.command.messageId);
      default:
        return false;
    }
  });
  if (controlPending) return blocked("pending-control");
  const command = state.commands.get(work.commandId);
  if (command?.command === undefined) return ineligible("owner-missing");
  if (command.terminal !== undefined) return ineligible("owner-terminal");
  const inputsRequired =
    work._tag === "Command"
      ? // Older admitted commands did not commit their User message until run start.
        !(
          command.command._tag === "SubmitMessage" &&
          !Array.from(state.messages.values()).some(
            (message) =>
              (message._tag === "User" || message._tag === "Steering") &&
              message.commandId === command.commandId,
          )
        )
      : work.inputMessageIds.length > 0;
  if (work._tag === "Command") {
    if (command.startedSeq !== undefined) return ineligible("owner-started");
  } else {
    if (command.startedSeq === undefined) return ineligible("owner-not-started");
    const predecessor = state.runs.get(work.interruptedRunId);
    if (predecessor === undefined || !predecessor.commandIds.includes(command.commandId))
      return ineligible("predecessor-mismatch");
  }
  if (inputsRequired && inputs.eligible.length === 0)
    return inputs.held.length > 0 ? blocked("paused-inputs") : ineligible("inputs-gone");
  return { _tag: "Grantable", inputMessageIds: inputs.eligible };
};

/** Whether a grant of `requestId` would start a run now; `runGrantability` says why not. */
export const canGrantRun = (state: SchedulingState, requestId: RunRequestId): boolean =>
  state.runSchedulingRequest?.requestId === requestId &&
  runGrantability(state, state.runSchedulingRequest.work)._tag === "Grantable";

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
