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
import { classifyRecoverableWork, type RecoverableWork } from "./reduced-state";
import { pendingSteerings } from "./run-continuation-policy";
import { runSchedulingInputMessageIds } from "./run-scheduling";
import type { InferenceId, MessageId, RunId, TurnId } from "../types/core";
import {
  effectDurableAgentNamespace,
  type DecodedDurableEvent,
  type UsagePayload,
} from "../types/events";
import { interpretLegacyAttemptForReduction } from "../types/events/durable";

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
  /** The interrupted run still has work to answer, so a replacement run continues it. */
  readonly continuation?: SessionRecoveryContinuation;
  /** The interrupted run already committed its final answer, so recovery only closes it. */
  readonly completion?: SessionRecoveryCompletion;
}

export interface SessionRecoveryContinuation {
  readonly command: CommandRecord;
  readonly run: RunRecord;
  readonly inputMessageIds: ReadonlyArray<MessageId>;
}

/**
 * An interrupted run whose last turn's inference completed with nothing left to continue (no
 * tool feedback, no steering). Its run, command and, when still open, its turn are completed,
 * never failed or replaced, so the answer is not inferred a second time.
 */
export interface SessionRecoveryCompletion {
  readonly command: CommandRecord;
  readonly run: RunRecord;
  readonly openTurn?: { readonly turnId: TurnId; readonly usage?: UsagePayload };
}

/**
 * A run's last turn, folded from the run's own durable events. A context rebase prunes terminal
 * turn, inference and tool records from `ReducedState`, so only the log still says how an open
 * run's last turn ended.
 */
export interface RunLastTurn {
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly terminal?: "Completed" | "Failed";
  /** The turn's last inference, once it started. */
  readonly inference?: {
    readonly inferenceId: InferenceId;
    readonly completed: boolean;
    readonly usage?: UsagePayload;
  };
  /** Tool calls the turn created or rejected: either leaves feedback for another inference. */
  readonly toolDecisions: number;
}

/**
 * One step of folding `runId`'s last turn from the session's events in log order. Events are read
 * as `ReducedState` reads them: framework events only, retained attempt events as inferences, and
 * the latest inference terminal wins.
 */
export const stepRunLastTurn = (
  runId: RunId,
  last: RunLastTurn | undefined,
  entry: DecodedDurableEvent,
): RunLastTurn | undefined => {
  if (entry.namespace !== effectDurableAgentNamespace) return last;
  const event = interpretLegacyAttemptForReduction(entry);
  if (event.type === "TurnStarted") {
    return event.payload.runId === runId
      ? { runId, turnId: event.payload.turnId, toolDecisions: 0 }
      : last;
  }
  const inLastTurn = (payload: { readonly runId: RunId; readonly turnId: TurnId }) =>
    last !== undefined && payload.runId === runId && payload.turnId === last.turnId;
  switch (event.type) {
    case "InferenceStarted":
      return last !== undefined && inLastTurn(event.payload)
        ? { ...last, inference: { inferenceId: event.payload.inferenceId, completed: false } }
        : last;
    case "InferenceFailed":
      return last?.inference !== undefined &&
        inLastTurn(event.payload) &&
        event.payload.inferenceId === last.inference.inferenceId
        ? { ...last, inference: { inferenceId: last.inference.inferenceId, completed: false } }
        : last;
    case "InferenceCompleted": {
      if (
        last?.inference === undefined ||
        !inLastTurn(event.payload) ||
        event.payload.inferenceId !== last.inference.inferenceId
      ) {
        return last;
      }
      const usage = event.payload.usage;
      return {
        ...last,
        inference: {
          inferenceId: last.inference.inferenceId,
          completed: true,
          ...(usage === undefined ? {} : { usage }),
        },
      };
    }
    case "ToolCallCreated":
    case "ToolCallRejected":
      return last !== undefined && inLastTurn(event.payload)
        ? { ...last, toolDecisions: last.toolDecisions + 1 }
        : last;
    case "TurnCompleted":
      return last !== undefined && inLastTurn(event.payload)
        ? { ...last, terminal: "Completed" }
        : last;
    case "TurnFailed":
    case "TurnStopped":
      return last !== undefined && inLastTurn(event.payload)
        ? { ...last, terminal: "Failed" }
        : last;
    default:
      return last;
  }
};

/**
 * Pure startup-recovery policy over one canonical durable snapshot. `lastTurn`, folded from the
 * log for the plan's continuation run, lets the plan complete a run that already answered instead
 * of replacing it; without it the run is continued.
 */
export const planSessionRecovery = (
  state: ReducedState,
  lastTurn?: RunLastTurn,
): SessionRecoveryPlan => {
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
  const completion =
    continuationCommand === undefined || continuationRun === undefined
      ? undefined
      : planAnsweredRunCompletion(
          state,
          recoverable,
          continuationCommand,
          continuationRun,
          lastTurn,
        );
  // The inputs the interrupted run selected, including the paused messages a Resume names.
  const sourceMessageIds =
    continuationCommand === undefined
      ? []
      : runSchedulingInputMessageIds(state, {
          _tag: "Command",
          commandId: continuationCommand.commandId,
        });
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
  const continuationInputMessageIds =
    completion === undefined ? Array.from(new Set([...sourceMessageIds, ...eligibleSteerIds])) : [];
  const continuationInputIds = new Set(continuationInputMessageIds);
  const messageIdsToResume = state.commandQueues.pendingSteers
    .filter(
      (message) =>
        !pausedMessageIds.has(message.messageId) &&
        !continuationInputIds.has(message.messageId) &&
        !(
          state.runSchedulingRequest?.work._tag === "Recovery" &&
          state.runSchedulingRequest.work.inputMessageIds.includes(message.messageId)
        ) &&
        !pendingCommandsById.has(message.commandId) &&
        !resumeOwnedMessageIds.has(message.messageId),
    )
    .map((message) => message.messageId);

  return {
    activeCommands: recoverable.activeCommands.filter(
      (command) => command.commandId !== completion?.command.commandId,
    ),
    activeRuns: recoverable.activeRuns.filter((run) => run.runId !== completion?.run.runId),
    activeTurns: recoverable.activeTurns.filter(
      (turn) => turn.turnId !== completion?.openTurn?.turnId,
    ),
    activeInferences: recoverable.activeInferences,
    openToolCalls: recoverable.openToolCalls,
    pendingStopRequests: recoverable.pendingStopRequests,
    openCompactions: recoverable.openCompactions,
    messageIdsToPause,
    messageIdsToResume,
    ...(completion === undefined ? {} : { completion }),
    ...(continuationCommand === undefined ||
    continuationRun === undefined ||
    completion !== undefined
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

/**
 * `decideRunContinuation`'s `CompleteRun` over the run's durable last turn: its inference
 * completed, it decided no tool calls (no feedback to answer), and no steering is pending.
 */
const planAnsweredRunCompletion = (
  state: ReducedState,
  recoverable: RecoverableWork,
  command: CommandRecord,
  run: RunRecord,
  lastTurn: RunLastTurn | undefined,
): SessionRecoveryCompletion | undefined => {
  if (
    lastTurn?.runId !== run.runId ||
    lastTurn.terminal === "Failed" ||
    lastTurn.inference?.completed !== true ||
    lastTurn.toolDecisions > 0 ||
    pendingSteerings(state, run.runId).length > 0 ||
    recoverable.activeInferences.some((inference) => inference.runId === run.runId) ||
    recoverable.openToolCalls.some((tool) => tool.decision?.runId === run.runId)
  ) {
    return undefined;
  }
  const usage = lastTurn.inference.usage;
  return {
    command,
    run,
    ...(lastTurn.terminal === undefined
      ? { openTurn: { turnId: lastTurn.turnId, ...(usage === undefined ? {} : { usage }) } }
      : {}),
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
  plan.continuation === undefined &&
  plan.completion === undefined;
