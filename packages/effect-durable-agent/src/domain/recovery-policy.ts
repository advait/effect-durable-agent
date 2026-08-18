import type {
  CommandRecord,
  CompactionRecord,
  InferenceRecord,
  ReducedState,
  RunRecord,
  StopRequestRecord,
  ToolCallRecord,
  TurnRecord,
} from "./reduced-state";
import { classifyRecoverableWork } from "./reduced-state";
import type { MessageId } from "../types/core";

export interface SessionRecoveryPlan {
  readonly activeCommands: ReadonlyArray<CommandRecord>;
  readonly activeRuns: ReadonlyArray<RunRecord>;
  readonly activeTurns: ReadonlyArray<TurnRecord>;
  readonly activeInferences: ReadonlyArray<InferenceRecord>;
  readonly openToolCalls: ReadonlyArray<ToolCallRecord>;
  readonly pendingStopRequests: ReadonlyArray<StopRequestRecord>;
  readonly openCompactions: ReadonlyArray<CompactionRecord>;
  readonly messageIdsToPause: ReadonlyArray<MessageId>;
  readonly messageIdsToResume: ReadonlyArray<MessageId>;
  readonly continuation?: SessionRecoveryContinuation;
}

export interface SessionRecoveryContinuation {
  readonly command: CommandRecord;
  readonly run: RunRecord;
  readonly inputMessageIds: ReadonlyArray<MessageId>;
}

/** Pure startup-recovery policy over one canonical durable snapshot. */
export const planSessionRecovery = (state: ReducedState): SessionRecoveryPlan => {
  const recoverable = classifyRecoverableWork(state);
  const pendingCommandsById = new Set(
    state.commandQueues.pendingCommands.map((command) => command.commandId),
  );
  const resumeOwnedMessageIds = new Set(
    [...Array.from(state.commands.values()), ...state.commandQueues.pendingCommands].flatMap(
      (command) =>
        ("terminal" in command ? command.terminal === undefined : true) &&
        command.command?._tag === "ResumePendingMessages"
          ? command.command.messageIds
          : [],
    ),
  );
  const hasApplicableStop = recoverable.pendingStopRequests.some(
    (request) => request.requestedRunId !== undefined && request.requestedTurnId !== undefined,
  );
  const messageIdsToPause = hasApplicableStop
    ? [...state.commandQueues.pendingQueue, ...state.commandQueues.pendingSteers].map(
        (message) => message.messageId,
      )
    : [];
  const pausedMessageIds = new Set(messageIdsToPause);
  const stoppedRunIds = new Set(
    recoverable.pendingStopRequests.flatMap((request) =>
      request.requestedRunId === undefined ? [] : [request.requestedRunId],
    ),
  );
  const continuationCommand = hasApplicableStop
    ? undefined
    : recoverable.activeCommands.find((command) => {
        const tag = command.command?._tag;
        return (
          (tag === "SubmitMessage" || tag === "ResumePendingMessages") &&
          recoverable.activeRuns.some(
            (run) => !stoppedRunIds.has(run.runId) && run.commandIds.includes(command.commandId),
          )
        );
      });
  const continuationRun = recoverable.activeRuns.find(
    (run) =>
      continuationCommand !== undefined &&
      !stoppedRunIds.has(run.runId) &&
      run.commandIds.includes(continuationCommand.commandId),
  );
  const pendingMessageIds = new Set(
    [...state.commandQueues.pendingQueue, ...state.commandQueues.pendingSteers].map(
      (message) => message.messageId,
    ),
  );
  const sourceMessageIds =
    continuationCommand?.command?._tag === "ResumePendingMessages"
      ? continuationCommand.command.messageIds.filter((messageId) =>
          pendingMessageIds.has(messageId),
        )
      : continuationCommand === undefined
        ? []
        : [...state.commandQueues.pendingQueue, ...state.commandQueues.pendingSteers]
            .filter((message) => message.commandId === continuationCommand.commandId)
            .map((message) => message.messageId);
  const eligibleSteerIds =
    continuationRun === undefined
      ? []
      : state.commandQueues.pendingSteers
          .filter(
            (message) =>
              message.targetRunId === undefined ||
              message.targetRunId === continuationRun.runId ||
              continuationRun.commandIds.some(
                (commandId) =>
                  state.runs.get(message.targetRunId!)?.commandIds.includes(commandId) === true,
              ),
          )
          .map((message) => message.messageId);
  const continuationInputMessageIds = Array.from(
    new Set([...sourceMessageIds, ...eligibleSteerIds]),
  );
  const continuationInputIds = new Set(continuationInputMessageIds);
  const messageIdsToResume = state.commandQueues.pendingSteers
    .filter(
      (message) =>
        !pausedMessageIds.has(message.messageId) &&
        !continuationInputIds.has(message.messageId) &&
        !pendingCommandsById.has(message.commandId) &&
        !resumeOwnedMessageIds.has(message.messageId),
    )
    .map((message) => message.messageId);

  return {
    activeCommands: recoverable.activeCommands,
    activeRuns: recoverable.activeRuns,
    activeTurns: recoverable.activeTurns,
    activeInferences: recoverable.activeInferences,
    openToolCalls: recoverable.openToolCalls,
    pendingStopRequests: recoverable.pendingStopRequests,
    openCompactions: recoverable.openCompactions,
    messageIdsToPause,
    messageIdsToResume,
    ...(continuationCommand === undefined || continuationRun === undefined
      ? {}
      : {
          continuation: {
            command: continuationCommand,
            run: continuationRun,
            inputMessageIds: continuationInputMessageIds,
          },
        }),
  };
};

export const isSessionRecoveryPlanEmpty = (plan: SessionRecoveryPlan): boolean =>
  plan.activeCommands.length === 0 &&
  plan.activeRuns.length === 0 &&
  plan.activeTurns.length === 0 &&
  plan.activeInferences.length === 0 &&
  plan.openToolCalls.length === 0 &&
  plan.pendingStopRequests.length === 0 &&
  plan.openCompactions.length === 0 &&
  plan.messageIdsToPause.length === 0 &&
  plan.messageIdsToResume.length === 0 &&
  plan.continuation === undefined;
