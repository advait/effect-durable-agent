import * as Schema from "effect/Schema";

import { CommandId, MessageId, RunId } from "../types/core";
import type { EDACommand } from "../types/commands";
import type { PendingCommand, ReducedState } from "./reduced-state";

/** Durable/runtime evidence for active work blocking new queued commands. */
export const DispatchActiveCommand = Schema.TaggedStruct("DispatchActiveCommand", {
  commandId: CommandId,
  runId: Schema.optionalKey(RunId),
});
export type DispatchActiveCommand = typeof DispatchActiveCommand.Type;

/** Runnable admitted command selected from the materialized command queues. */
export interface DispatchCommandCandidate {
  readonly commandId: CommandId;
  readonly command: EDACommand;
  readonly admissionTrace?: PendingCommand["admissionTrace"];
}

/** Pure scheduler decision interpreted by `SessionState` inside its serialized boundary. */
export type DispatchDecision =
  | { readonly _tag: "DispatchBlockedByActiveCommand"; readonly active: DispatchActiveCommand }
  | {
      readonly _tag: "DispatchStopCommand";
      readonly command: DispatchCommandCandidate;
      readonly active: DispatchActiveCommand;
    }
  | {
      readonly _tag: "DispatchSteerCommand";
      readonly command: DispatchCommandCandidate;
      readonly active: DispatchActiveCommand & { readonly runId: RunId };
    }
  | {
      readonly _tag: "DispatchInterruptCommand";
      readonly command: DispatchCommandCandidate;
      readonly active: DispatchActiveCommand;
    }
  | {
      readonly _tag: "DispatchCancelPendingMessage";
      readonly command: DispatchCommandCandidate;
      readonly active?: DispatchActiveCommand;
    }
  | {
      readonly _tag: "DispatchPromotePendingMessage";
      readonly command: DispatchCommandCandidate;
      readonly active?: DispatchActiveCommand;
    }
  | { readonly _tag: "DispatchNoPendingCommand" }
  | { readonly _tag: "DispatchInvariantViolation"; readonly messageIds: ReadonlyArray<MessageId> }
  | { readonly _tag: "DispatchStartCommand"; readonly command: DispatchCommandCandidate };

/** Decide the next dispatch action from the canonical reduced replay state. */
export const decideDispatch = (state: ReducedState): DispatchDecision => {
  const active = state.commandQueues.active;
  if (active !== undefined) {
    const activeDispatch = DispatchActiveCommand.make({
      commandId: active.commandId,
      ...(active.runId === undefined ? {} : { runId: active.runId }),
    });
    const control = pendingActiveControlCommand(state);
    if (control === undefined) {
      return { _tag: "DispatchBlockedByActiveCommand", active: activeDispatch };
    }
    const candidate = toCandidate(control);
    if (control.command._tag === "StopTurn") {
      return { _tag: "DispatchStopCommand", command: candidate, active: activeDispatch };
    }
    if (control.command._tag === "CancelPendingMessage") {
      return { _tag: "DispatchCancelPendingMessage", command: candidate, active: activeDispatch };
    }
    if (control.command._tag === "PromotePendingMessage") {
      return { _tag: "DispatchPromotePendingMessage", command: candidate, active: activeDispatch };
    }
    if (control.command._tag !== "SubmitMessage") {
      return { _tag: "DispatchBlockedByActiveCommand", active: activeDispatch };
    }
    if (control.command.disposition === "interrupt") {
      return { _tag: "DispatchInterruptCommand", command: candidate, active: activeDispatch };
    }
    return activeDispatch.runId === undefined
      ? { _tag: "DispatchBlockedByActiveCommand", active: activeDispatch }
      : {
          _tag: "DispatchSteerCommand",
          command: candidate,
          active: activeDispatch as DispatchActiveCommand & { readonly runId: RunId },
        };
  }

  const control = pendingActiveControlCommand(state);
  if (control?.command._tag === "CancelPendingMessage") {
    return { _tag: "DispatchCancelPendingMessage", command: toCandidate(control) };
  }
  if (control?.command._tag === "PromotePendingMessage") {
    return { _tag: "DispatchPromotePendingMessage", command: toCandidate(control) };
  }
  const stop = control?.command._tag === "StopTurn" ? control : undefined;
  if (stop !== undefined) {
    return { _tag: "DispatchStartCommand", command: toCandidate(stop) };
  }
  const next = nextIdleCommand(state);
  if (next === "orphan-pending-messages") {
    return {
      _tag: "DispatchInvariantViolation",
      messageIds: state.commandQueues.pendingSteers.map((message) => message.messageId),
    };
  }
  return next === undefined
    ? { _tag: "DispatchNoPendingCommand" }
    : { _tag: "DispatchStartCommand", command: toCandidate(next) };
};

const nextIdleCommand = (
  state: ReducedState,
): PendingCommand | "orphan-pending-messages" | undefined => {
  const pendingById = new Map(
    state.commandQueues.pendingCommands.map((command) => [command.commandId, command] as const),
  );
  for (const message of state.commandQueues.pendingSteers) {
    const origin = pendingById.get(message.commandId);
    if (origin !== undefined) return origin;
    const resume = state.commandQueues.pendingCommands.find(
      (command) =>
        command.command._tag === "ResumePendingMessages" &&
        command.command.messageIds.includes(message.messageId),
    );
    if (resume !== undefined) return resume;
  }
  if (state.commandQueues.pendingSteers.length > 0) return "orphan-pending-messages";

  const interrupt = state.commandQueues.pendingCommands.find(
    (command) =>
      command.command._tag === "SubmitMessage" && command.command.disposition === "interrupt",
  );
  if (interrupt !== undefined) return interrupt;

  for (const message of state.commandQueues.pendingQueue) {
    const origin = pendingById.get(message.commandId);
    if (origin !== undefined) return origin;
  }

  return state.commandQueues.pendingCommands.find(
    (command) =>
      command.command._tag === "ResumePendingMessages" ||
      (command.command._tag === "SubmitMessage" &&
        !state.commandQueues.pausedQueue.some(
          (message) => message.commandId === command.commandId,
        )),
  );
};

const pendingActiveControlCommand = (state: ReducedState): PendingCommand | undefined =>
  [...state.commandQueues.activeControlCommands].sort(
    (left, right) =>
      controlPriority(left.command) - controlPriority(right.command) ||
      left.admittedSeq - right.admittedSeq,
  )[0];

const controlPriority = (command: EDACommand): number => {
  switch (command._tag) {
    case "StopTurn":
      return 0;
    case "CancelPendingMessage":
      return 1;
    case "PromotePendingMessage":
      return 2;
    case "SubmitMessage":
      return 3;
    case "ResumePendingMessages":
      return 4;
  }
};

const toCandidate = (pending: PendingCommand): DispatchCommandCandidate => ({
  commandId: pending.commandId,
  command: pending.command,
  ...(pending.admissionTrace === undefined ? {} : { admissionTrace: pending.admissionTrace }),
});
