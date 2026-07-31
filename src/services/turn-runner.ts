import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Prompt from "effect/unstable/ai/Prompt";

import { assertNeverError } from "../domain/assert-never";
import { RunId, TurnId } from "../types/core";
import {
  FailurePayload,
  inferenceCompletedEventType,
  inferenceFailedEventType,
  ModelSelectionPayload,
  NonNegativeInt,
  UsagePayload,
  toolCallCreatedEventType,
  toolCallRejectedEventType,
} from "../types/events";
import type {
  ToolCallCreatedEvent,
  ToolCallFailedPayload,
  ToolCallRejectedEvent,
  InferenceCompletedEvent,
  InferenceFailedEvent,
} from "../types/events";
import {
  CommittedDurableEvent,
  EDASessionStoreError,
  hasEDASessionStoreError,
} from "./session-store";
import { EventFactory, type EventFactoryShape } from "./event-factory";
import { IdGenerator } from "./id-generator";
import { failurePayloadFromCause } from "./started-boundary-guard";
import { ToolExecutor } from "./tool-executor";
import type { ToolExecutorError } from "./tool-executor";
import { InferenceRunResult, InferenceRunner } from "./inference-runner";
import type { InferenceRunnerError } from "./inference-runner";
import type { SessionEventSink } from "./session-event-sink";
import { annotateEdaSpan, usageAttributes } from "./tracing";

/** Input identity for running one turn with one provider inference stream. */
export const RunTurnContext = Schema.Struct({
  runId: RunId,
  turnId: TurnId,
});
export type RunTurnContext = typeof RunTurnContext.Type;

/** Effectful inputs for running an already-started turn body. */
export interface RunTurnInput extends RunTurnContext {
  readonly prompt: Prompt.RawInput;
  readonly eventSink: SessionEventSink;
  readonly started: CommittedDurableEvent;
  readonly modelSelection?: ModelSelectionPayload;
  /** Maximum framework-owned tool calls this model run may execute before failing safely. */
  readonly maxToolCallsPerRun?: NonNegativeInt;
  /** Remaining rejected-tool correction turns before this turn must fail closed. */
  readonly remainingToolRejectionCorrections?: NonNegativeInt;
  /** Configured rejected-tool correction limit used in failure details. */
  readonly maxToolRejectionCorrections?: NonNegativeInt;
}

/** Turn outcome when the inference completed and no tools need handling. */
export const TurnRunCompleted = Schema.TaggedStruct("TurnRunCompleted", {
  committed: CommittedDurableEvent,
});
export type TurnRunCompleted = typeof TurnRunCompleted.Type;

/** Turn outcome when the inference failed and the turn failure was persisted. */
export const TurnRunFailed = Schema.TaggedStruct("TurnRunFailed", {
  committed: CommittedDurableEvent,
});
export type TurnRunFailed = typeof TurnRunFailed.Type;

/** Terminal outcome variants for one turn body. */
export const TurnRunOutcome = Schema.Union([TurnRunCompleted, TurnRunFailed]);
export type TurnRunOutcome = typeof TurnRunOutcome.Type;

/** Summary returned after the turn runner processes one inference stream. */
export const TurnRunResult = Schema.Struct({
  started: CommittedDurableEvent,
  inference: InferenceRunResult,
  outcome: TurnRunOutcome,
});
export type TurnRunResult = typeof TurnRunResult.Type;

/** Error surface for the turn runner's inference and tool orchestration. */
export type TurnRunnerError = EDASessionStoreError | InferenceRunnerError | ToolExecutorError;

const defaultMaxToolRejectionCorrections = NonNegativeInt.make(1);
const defaultMaxToolCallsPerRun = NonNegativeInt.make(20);
const maxToolExecutionConcurrency = 10;

/** Runtime service that owns inferences, tool execution, and turn terminalization. */
export interface TurnRunnerShape {
  /** Run one turn around a provider inference stream. */
  readonly runTurn: (input: RunTurnInput) => Effect.Effect<TurnRunResult, TurnRunnerError>;
}

/** Construct the live turn-runner implementation. */
const makeLiveTurnRunner = Effect.gen(function* () {
  const ids = yield* IdGenerator;
  const events = yield* EventFactory;
  const inferenceRunner = yield* InferenceRunner;
  const toolExecutor = yield* ToolExecutor;

  const commitCompletedTurn = (
    input: RunTurnInput,
    inferenceCompleted: CommittedInferenceCompleted,
  ) =>
    Effect.gen(function* () {
      const usage = inferenceCompleted.event.payload.usage;
      const turnCompleted = yield* events.turnCompleted({
        runId: input.runId,
        turnId: input.turnId,
        ...(usage === undefined ? {} : { usage }),
      });
      return yield* input.eventSink.appendDurable(turnCompleted);
    });

  const finishTurnAfterInference = (
    input: RunTurnInput,
    inference: InferenceRunResult,
    remainingToolCalls: NonNegativeInt,
  ) =>
    Effect.gen(function* () {
      switch (inference.terminal._tag) {
        case "InferenceRunFinished": {
          const toolEvents = inference.terminal.committed.filter(isToolDecisionBoundary);
          const created = toolEvents.filter(isToolCallCreated);
          const rejected = toolEvents.filter(isToolCallRejected);
          const frameworkCreated = yield* requireFrameworkCreatedToolCalls(created);
          if (created.length > 0) {
            if (frameworkCreated.length > remainingToolCalls) {
              yield* failUnexecutedToolCalls(
                input.eventSink,
                events,
                frameworkCreated,
                FailurePayload.make({
                  message: `maxToolCallsPerRun exceeded: requested ${frameworkCreated.length} tool call(s) with ${remainingToolCalls} remaining`,
                  code: "tool.budget_exceeded",
                  details: {
                    requested: frameworkCreated.length,
                    remaining: remainingToolCalls,
                    maxToolCallsPerRun: input.maxToolCallsPerRun ?? defaultMaxToolCallsPerRun,
                  },
                }),
              );
              const committed = yield* failTurnForToolCallBudget({
                eventSink: input.eventSink,
                events,
                requested: frameworkCreated.length,
                remaining: remainingToolCalls,
                turnIdentity: { runId: input.runId, turnId: input.turnId },
                maxToolCallsPerRun: input.maxToolCallsPerRun ?? defaultMaxToolCallsPerRun,
              });
              return TurnRunFailed.make({ committed });
            }
            yield* executeFrameworkToolCalls(input.eventSink, events, frameworkCreated);
          }
          const completed = yield* requireInferenceCompleted(inference.terminal.committed);
          if (rejected.length > 0 && rejectedToolCorrectionExhausted(input)) {
            const committed = yield* failTurnForRejectedToolCorrectionExhaustion({
              eventSink: input.eventSink,
              events,
              rejected,
              turnIdentity: { runId: input.runId, turnId: input.turnId },
              maxToolRejectionCorrections:
                input.maxToolRejectionCorrections ?? defaultMaxToolRejectionCorrections,
            });
            return TurnRunFailed.make({ committed });
          }
          const committed = yield* commitCompletedTurn(input, completed);
          return TurnRunCompleted.make({ committed });
        }
        case "InferenceRunFailed": {
          const failed = yield* requireInferenceFailed(inference.terminal.committed);
          const event = yield* events.turnFailed({
            runId: input.runId,
            turnId: input.turnId,
            error: failed.event.payload.error,
          });
          const committed = yield* input.eventSink.appendDurable(event);
          return TurnRunFailed.make({ committed });
        }
        default:
          return yield* Effect.die(assertNeverError(inference.terminal, "inference terminal"));
      }
    });

  const failUnexecutedToolCalls = (
    eventSink: SessionEventSink,
    events: EventFactoryShape,
    created: ReadonlyArray<CommittedToolCallCreated>,
    error: FailurePayload,
  ) =>
    Effect.gen(function* () {
      if (created.length === 0) {
        return;
      }
      const failed = yield* Effect.forEach(created, (entry) =>
        events.toolCallFailed({
          toolCallId: entry.event.payload.toolCallId,
          error,
          promptPart: toolFailurePromptPartFromCreated(entry, error),
        }),
      );
      yield* eventSink.appendDurableBatch(failed);
    });

  const executeFrameworkToolCalls = (
    eventSink: SessionEventSink,
    events: EventFactoryShape,
    created: ReadonlyArray<CommittedToolCallCreated>,
  ) =>
    Effect.gen(function* () {
      const open = yield* Ref.make(
        new Map(created.map((entry) => [entry.event.payload.toolCallId, entry] as const)),
      );
      const removeOpen = (entry: CommittedToolCallCreated) =>
        Ref.update(open, (current) => {
          const next = new Map(current);
          next.delete(entry.event.payload.toolCallId);
          return next;
        });
      const failRemainingOpen = (reason: string) =>
        Effect.gen(function* () {
          const remaining = Array.from((yield* Ref.get(open)).values());
          if (remaining.length === 0) {
            return;
          }
          const failed = yield* Effect.forEach(remaining, (entry) =>
            Effect.gen(function* () {
              const error = FailurePayload.make({
                message: `tool call interrupted: ${reason}`,
                code: "tool.interrupted",
                details: { reason, toolCallId: entry.event.payload.toolCallId },
              });
              return yield* events.toolCallFailed({
                toolCallId: entry.event.payload.toolCallId,
                error,
                promptPart: toolFailurePromptPartFromCreated(entry, error),
              });
            }),
          );
          yield* eventSink.appendDurableBatch(failed);
        });
      const run = Effect.forEach(
        created,
        (entry) =>
          toolExecutor
            .executeCreated({ created: entry, eventSink })
            .pipe(Effect.ensuring(removeOpen(entry))),
        { concurrency: maxToolExecutionConcurrency },
      );
      return yield* Effect.acquireUseRelease(
        Effect.succeed(open),
        () => run,
        (_open, exit) => {
          if (Exit.isSuccess(exit)) {
            return Effect.void;
          }
          if (hasEDASessionStoreError(exit.cause)) {
            return Effect.failCause(exit.cause);
          }
          return Cause.hasInterruptsOnly(exit.cause)
            ? failRemainingOpen("interrupted")
            : Effect.void;
        },
      );
    });

  return {
    runTurn: Effect.fn("agent.turn")(function* (input: RunTurnInput) {
      const turnIdentity = { runId: input.runId, turnId: input.turnId };
      const maxToolCallsPerRun = input.maxToolCallsPerRun ?? defaultMaxToolCallsPerRun;
      yield* annotateEdaSpan({
        "eda.run.id": input.runId,
        "eda.turn.id": input.turnId,
        "eda.model.provider": input.modelSelection?.provider,
        "eda.model.id": input.modelSelection?.modelId,
        "eda.tool.max_calls_per_run": maxToolCallsPerRun,
      });
      const inferencesStarted = yield* Ref.make(0);
      const body = Effect.gen(function* () {
        const inferenceId = yield* ids.makeInferenceId();
        yield* Ref.update(inferencesStarted, (count) => count + 1);
        const inference = yield* inferenceRunner.runInference({
          ...turnIdentity,
          inferenceId,
          prompt: input.prompt,
          eventSink: input.eventSink,
          modelSelection: input.modelSelection,
        });
        const outcome = yield* finishTurnAfterInference(input, inference, maxToolCallsPerRun);
        return TurnRunResult.make({
          started: input.started,
          inference,
          outcome,
        });
      });

      return yield* body.pipe(
        Effect.tap((result) =>
          Effect.gen(function* () {
            yield* annotateEdaSpan({
              "eda.turn.outcome": result.outcome._tag,
              "eda.inference.started_count": yield* Ref.get(inferencesStarted),
              "eda.provider.parts_recorded": result.inference.partsRecorded,
              ...turnUsageAttributes(result),
            });
          }),
        ),
        Effect.catchCause((cause) => {
          if (hasEDASessionStoreError(cause) || Cause.hasInterruptsOnly(cause)) {
            return Effect.failCause(cause);
          }
          return Effect.uninterruptible(
            Effect.gen(function* () {
              const terminal = yield* events.turnFailed({
                ...turnIdentity,
                error: failurePayloadFromCause(cause),
              });
              yield* input.eventSink.appendDurable(terminal);
              return yield* Effect.failCause(cause);
            }),
          );
        }),
      );
    }),
  };
});

/** Owns one turn: turn boundaries, inferences, tool handoff, and terminal turn events. */
export class TurnRunner extends Context.Service<TurnRunner, TurnRunnerShape>()(
  "@effect-durable-agent/TurnRunner",
) {
  static readonly Live = Layer.effect(TurnRunner, makeLiveTurnRunner);
}

type CommittedInferenceCompleted = CommittedDurableEvent & {
  readonly event: InferenceCompletedEvent;
};

type CommittedInferenceFailed = CommittedDurableEvent & {
  readonly event: InferenceFailedEvent;
};

type CommittedToolCallCreated = CommittedDurableEvent & {
  readonly event: ToolCallCreatedEvent;
};

type CommittedToolCallRejected = CommittedDurableEvent & {
  readonly event: ToolCallRejectedEvent;
};

const failTurnForToolCallBudget = (input: {
  readonly eventSink: SessionEventSink;
  readonly events: EventFactoryShape;
  readonly maxToolCallsPerRun: NonNegativeInt;
  readonly remaining: number;
  readonly requested: number;
  readonly turnIdentity: RunTurnContext;
}) =>
  Effect.gen(function* () {
    const event = yield* input.events.turnFailed({
      ...input.turnIdentity,
      error: FailurePayload.make({
        message: `maxToolCallsPerRun exceeded: requested ${input.requested} tool call(s) with ${input.remaining} remaining out of ${input.maxToolCallsPerRun}`,
        code: "tool.budget_exceeded",
        details: {
          requested: input.requested,
          remaining: input.remaining,
          maxToolCallsPerRun: input.maxToolCallsPerRun,
        },
      }),
    });
    return yield* input.eventSink.appendDurable(event);
  });

const failTurnForRejectedToolCorrectionExhaustion = (input: {
  readonly eventSink: SessionEventSink;
  readonly events: EventFactoryShape;
  readonly maxToolRejectionCorrections: NonNegativeInt;
  readonly rejected: ReadonlyArray<CommittedToolCallRejected>;
  readonly turnIdentity: RunTurnContext;
}) =>
  Effect.gen(function* () {
    const event = yield* input.events.turnFailed({
      ...input.turnIdentity,
      error: FailurePayload.make({
        message: "tool rejection correction exhausted",
        code: "tool.rejection_correction_exhausted",
        details: {
          limit: input.maxToolRejectionCorrections,
          rejectedToolCallIds: input.rejected.map((entry) => entry.event.payload.toolCallId),
        },
      }),
    });
    return yield* input.eventSink.appendDurable(event);
  });

const rejectedToolCorrectionExhausted = (input: RunTurnInput): boolean =>
  (input.remainingToolRejectionCorrections ?? defaultMaxToolRejectionCorrections) <= 0;

const toolFailurePromptPartFromCreated = (
  created: CommittedToolCallCreated,
  result: FailurePayload,
): ToolCallFailedPayload["promptPart"] => {
  return {
    ...Prompt.toolResultPart({
      id: created.event.payload.promptPart.id,
      name: created.event.payload.promptPart.name,
      isFailure: true,
      result,
    }),
    id: created.event.payload.promptPart.id,
    name: created.event.payload.promptPart.name,
    isFailure: true,
    result,
  };
};

const requireFrameworkCreatedToolCalls = (created: ReadonlyArray<CommittedToolCallCreated>) =>
  Effect.gen(function* () {
    const unsupported = created.find((entry) => entry.event.payload.promptPart.providerExecuted);
    if (unsupported !== undefined) {
      return yield* Effect.die(
        new Error(
          `Provider-executed tool calls are unsupported: ${unsupported.event.payload.promptPart.name}`,
        ),
      );
    }
    return created;
  });

const turnUsageAttributes = (result: TurnRunResult) =>
  result.outcome._tag === "TurnRunCompleted"
    ? usageAttributes(
        (result.outcome.committed.event.payload as { readonly usage?: UsagePayload }).usage,
      )
    : {};

const isInferenceCompleted = (
  committed: CommittedDurableEvent,
): committed is CommittedInferenceCompleted => committed.event.type === inferenceCompletedEventType;

const isInferenceFailed = (
  committed: CommittedDurableEvent,
): committed is CommittedInferenceFailed => committed.event.type === inferenceFailedEventType;

const isToolCallCreated = (
  committed: CommittedDurableEvent,
): committed is CommittedToolCallCreated => committed.event.type === toolCallCreatedEventType;

const isToolCallRejected = (
  committed: CommittedDurableEvent,
): committed is CommittedToolCallRejected => committed.event.type === toolCallRejectedEventType;

const isToolDecisionBoundary = (
  committed: CommittedDurableEvent,
): committed is CommittedToolCallCreated | CommittedToolCallRejected =>
  isToolCallCreated(committed) || isToolCallRejected(committed);

const requireInferenceCompleted = (committed: ReadonlyArray<CommittedDurableEvent>) => {
  const completed = committed.find(isInferenceCompleted);
  return completed === undefined
    ? Effect.die(new Error("InferenceRunFinished did not include InferenceCompleted"))
    : Effect.succeed(completed);
};

const requireInferenceFailed = (committed: ReadonlyArray<CommittedDurableEvent>) => {
  const failed = committed.find(isInferenceFailed);
  return failed === undefined
    ? Effect.die(new Error("InferenceRunFailed did not include InferenceFailed"))
    : Effect.succeed(failed);
};
