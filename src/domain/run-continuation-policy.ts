import type { PendingSteeringMessage, PendingUserMessage } from "./command-queues";
import type { ReducedState, ToolCallRecord } from "./reduced-state";
import type { RunId, TurnId } from "../types/core";
import { FailurePayload } from "../types/events";

/** Decision for a completed turn before `SessionState` terminalizes its run. */
export type RunContinuationDecision =
  | {
      /** Continue the same run with all steering messages eligible at this boundary. */
      readonly _tag: "ContinueWithSteering";
      readonly steerings: ReadonlyArray<PendingUserMessage | PendingSteeringMessage>;
    }
  | {
      /** Continue the same run because the latest turn produced durable tool feedback. */
      readonly _tag: "ContinueWithToolFeedback";
    }
  | {
      /** Fail the run from a continuation policy terminal condition. */
      readonly _tag: "FailRun";
      readonly error: FailurePayload;
    }
  | {
      /** No steering is pending for the run, so it may be completed. */
      readonly _tag: "CompleteRun";
    };

/**
 * Decide whether a completed turn should continue its run before run terminalization.
 *
 * All eligible steering messages are selected together at the boundary.
 */
export const decideRunContinuation = (input: {
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly state: ReducedState;
  readonly maxToolRejectionCorrections?: number;
}): RunContinuationDecision => {
  const currentTurnTools = toolCallsForTurn(input.state, input.runId, input.turnId);
  const created = currentTurnTools.filter((tool) => tool.decision?._tag === "Created");
  const rejected = currentTurnTools.filter((tool) => tool.decision?._tag === "Rejected");
  const providerExecuted = created.find(
    (tool) => tool.decision?._tag === "Created" && tool.decision.providerExecuted,
  );
  if (providerExecuted !== undefined) {
    return {
      _tag: "FailRun",
      error: FailurePayload.make({
        message: `Provider-executed tool calls are unsupported: ${providerExecuted.decision?.toolName}`,
        code: "tool.unsupported_provider_executed",
        details: { toolCallId: providerExecuted.toolCallId },
      }),
    };
  }

  if (rejected.length > 0) {
    const correctionLimit = input.maxToolRejectionCorrections ?? 1;
    const rejectedTurnCount = countRejectedTurns(input.state, input.runId);
    if (rejectedTurnCount > correctionLimit) {
      return {
        _tag: "FailRun",
        error: FailurePayload.make({
          message: "tool rejection correction exhausted",
          code: "tool.rejection_correction_exhausted",
          details: {
            limit: correctionLimit,
            rejectedToolCallIds: rejected.map((tool) => tool.toolCallId),
          },
        }),
      };
    }
  }

  const toolFeedbackReady =
    rejected.length > 0 ||
    created.some((tool) => tool.decision?._tag === "Created" && tool.terminal !== undefined);
  if (toolFeedbackReady) {
    const steerings = pendingSteerings(input.state, input.runId);
    return steerings.length === 0
      ? { _tag: "ContinueWithToolFeedback" }
      : { _tag: "ContinueWithSteering", steerings };
  }

  const steerings = pendingSteerings(input.state, input.runId);
  return steerings.length === 0
    ? { _tag: "CompleteRun" }
    : { _tag: "ContinueWithSteering", steerings };
};

const pendingSteerings = (
  state: ReducedState,
  runId: RunId,
): ReadonlyArray<PendingUserMessage | PendingSteeringMessage> => {
  const current = state.commandQueues.pendingSteers.filter(
    (message) => message.targetRunId === undefined || message.targetRunId === runId,
  );
  const currentIds = new Set(current.map((message) => message.messageId));
  const legacy = (state.commandQueues.steeringByRun.get(runId) ?? []).filter(
    (message) => !currentIds.has(message.messageId),
  );
  return [...current, ...legacy].sort(
    (left, right) =>
      ("effectiveSeq" in left ? left.effectiveSeq : left.queuedSeq) -
      ("effectiveSeq" in right ? right.effectiveSeq : right.queuedSeq),
  );
};

const toolCallsForTurn = (
  state: ReducedState,
  runId: RunId,
  turnId: TurnId,
): ReadonlyArray<ToolCallRecord> =>
  Array.from(state.toolCalls.values()).filter(
    (tool) => tool.decision?.runId === runId && tool.decision.turnId === turnId,
  );

const countRejectedTurns = (state: ReducedState, runId: RunId): number => {
  const turnIds = new Set<TurnId>();
  for (const tool of state.toolCalls.values()) {
    if (tool.decision?._tag === "Rejected" && tool.decision.runId === runId) {
      turnIds.add(tool.decision.turnId);
    }
  }
  return turnIds.size;
};
