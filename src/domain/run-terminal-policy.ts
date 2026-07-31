import type { ReducedState } from "./reduced-state";
import type { MessageId } from "../types/core";

export interface RunFailurePlan {
  readonly messageIdsToResume: ReadonlyArray<MessageId>;
}

export interface UserInterruptionPlan {
  readonly messageIdsToPause: ReadonlyArray<MessageId>;
}

/** Normal live-run failure policy. This is not startup recovery. */
export const planRunFailure = (state: ReducedState): RunFailurePlan => ({
  messageIdsToResume: state.commandQueues.pendingSteers.map((message) => message.messageId),
});

/** Normal explicit Stop/Interrupt policy. This is not startup recovery. */
export const planUserInterruption = (state: ReducedState): UserInterruptionPlan => ({
  messageIdsToPause: [
    ...state.commandQueues.pendingQueue,
    ...state.commandQueues.pendingSteers,
  ].map((message) => message.messageId),
});
