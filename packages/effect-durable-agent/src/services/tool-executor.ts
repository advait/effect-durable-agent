import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Prompt from "effect/unstable/ai/Prompt";

import { assertNeverError } from "../domain/assert-never";
import {
  FailurePayload,
  toolCallCreatedEventType,
  type ToolCallCompletedPayload,
  type ToolCallCreatedEvent,
  type ToolCallFailedPayload,
} from "../types/events";
import {
  CommittedDurableEvent,
  EDASessionStoreError,
  hasEDASessionStoreError,
} from "./session-store";
import { EventFactory } from "./event-factory";
import { IdGenerator } from "./id-generator";
import { SessionContext } from "./session-context";
import { makeStartedBoundaryGuard } from "./started-boundary-guard";
import { failurePayloadFromCause } from "./started-boundary-guard";
import { EDAToolRegistry } from "./tool-registry";
import type { SessionEventSink } from "./session-event-sink";
import { annotateEdaSpan } from "./tracing";

/** Committed durable event narrowed to a framework-owned ToolCallCreated boundary. */
export type CommittedToolCallCreated = CommittedDurableEvent & {
  readonly event: ToolCallCreatedEvent;
};

/** Runtime input for executing one sealed tool-call decision. */
export interface ExecuteToolCallInput {
  readonly created: CommittedDurableEvent;
  readonly eventSink: SessionEventSink;
}

/** Tool execution outcome after committing ToolCallCompleted. */
export const ToolExecutionCompleted = Schema.TaggedStruct("ToolExecutionCompleted", {
  committed: CommittedDurableEvent,
  result: Schema.Unknown,
});
export type ToolExecutionCompleted = typeof ToolExecutionCompleted.Type;

/** Tool execution outcome after committing ToolCallFailed or cancellation-as-failure. */
export const ToolExecutionFailed = Schema.TaggedStruct("ToolExecutionFailed", {
  committed: CommittedDurableEvent,
});
export type ToolExecutionFailed = typeof ToolExecutionFailed.Type;

/** Outcome for tool calls the provider already executed and EDA must not rerun. */
export const ToolExecutionSkipped = Schema.TaggedStruct("ToolExecutionSkipped", {
  reason: Schema.Literal("provider-executed"),
});
export type ToolExecutionSkipped = typeof ToolExecutionSkipped.Type;

/** Result variant for one framework tool execution attempt. */
export const ToolExecutionOutcome = Schema.Union([
  ToolExecutionCompleted,
  ToolExecutionFailed,
  ToolExecutionSkipped,
]);
export type ToolExecutionOutcome = typeof ToolExecutionOutcome.Type;

/** Execution result paired with the committed ToolCallCreated event that caused it. */
export interface ToolExecutionResult {
  readonly created: CommittedToolCallCreated;
  readonly outcome: ToolExecutionOutcome;
}

/** Failure when tool execution receives a committed event that is not ToolCallCreated. */
export class ToolExecutionEventNotCreated extends Schema.TaggedErrorClass<ToolExecutionEventNotCreated>()(
  "ToolExecutionEventNotCreated",
  { eventType: Schema.String },
) {}

/** Error surface for validating and committing framework tool execution. */
export type ToolExecutorError = ToolExecutionEventNotCreated | EDASessionStoreError;

/** Runtime service that executes sealed framework-owned tool calls. */
export interface ToolExecutorShape {
  /** Execute one sealed, framework-owned ToolCallCreated event. */
  readonly executeCreated: (
    input: ExecuteToolCallInput,
  ) => Effect.Effect<ToolExecutionResult, ToolExecutorError>;
}

/** Construct the live tool-executor implementation. */
const makeLiveToolExecutor = Effect.gen(function* () {
  const events = yield* EventFactory;
  const ids = yield* IdGenerator;
  const registry = yield* EDAToolRegistry;
  const session = yield* SessionContext;

  const failToolCall = (
    input: ExecuteToolCallInput,
    created: CommittedToolCallCreated,
    error: FailurePayload,
  ) =>
    Effect.gen(function* () {
      const event = yield* events.toolCallFailed({
        toolCallId: created.event.payload.toolCallId,
        error,
        promptPart: toolFailedPromptPart(created, error),
      });
      const committed = yield* input.eventSink.appendDurable(event);
      return ToolExecutionFailed.make({ committed });
    });

  return {
    executeCreated: Effect.fn("agent.tool")(function* (input: ExecuteToolCallInput) {
      const created = yield* requireToolCallCreated(input.created);
      yield* annotateEdaSpan({
        "eda.tool_call.id": created.event.payload.toolCallId,
        "eda.tool.name": created.event.payload.promptPart.name,
        "eda.tool.provider_executed": created.event.payload.promptPart.providerExecuted,
      });
      if (created.event.payload.promptPart.providerExecuted) {
        const outcome = ToolExecutionSkipped.make({ reason: "provider-executed" });
        yield* annotateEdaSpan({ "eda.tool.outcome": outcome._tag });
        return { created, outcome };
      }

      const startedBoundaryGuard = makeStartedBoundaryGuard(input.eventSink);
      const guarded = yield* startedBoundaryGuard({
        start: events.toolCallStarted({ toolCallId: created.event.payload.toolCallId }),
        body: () =>
          Effect.gen(function* () {
            const handled = yield* registry
              .execute(
                created.event.payload.promptPart.name,
                created.event.payload.promptPart.params,
                {
                  toolCallId: created.event.payload.toolCallId,
                  sessionId: session.sessionId,
                  makeEventId: ids.makeEventId,
                  emitDurable: input.eventSink.appendDurable,
                },
              )
              .pipe(
                Effect.map((result) => ({ _tag: "Succeeded" as const, result })),
                Effect.catchCause((cause) =>
                  hasEDASessionStoreError(cause)
                    ? Effect.failCause(cause as Cause.Cause<EDASessionStoreError>)
                    : Cause.hasInterruptsOnly(cause)
                      ? Effect.interrupt
                      : Effect.succeed({
                          _tag: "Failed" as const,
                          error: failurePayloadFromCause(cause),
                        }),
                ),
              );

            switch (handled._tag) {
              case "Succeeded": {
                const event = yield* events.toolCallCompleted({
                  toolCallId: created.event.payload.toolCallId,
                  promptPart: toolCompletedPromptPart(created, handled.result),
                });
                const committed = yield* input.eventSink.appendDurable(event);
                return ToolExecutionCompleted.make({ committed, result: handled.result });
              }
              case "Failed":
                return yield* failToolCall(input, created, handled.error);
              default:
                return yield* Effect.die(assertNeverError(handled, "tool handler outcome"));
            }
          }),
        onFailure: (error) =>
          events.toolCallFailed({
            toolCallId: created.event.payload.toolCallId,
            error,
            promptPart: toolFailedPromptPart(created, error),
          }),
        onInterrupt: Effect.gen(function* () {
          const error = FailurePayload.make({
            message: "tool call interrupted: interrupted",
            code: "tool.interrupted",
            details: { reason: "interrupted", toolCallId: created.event.payload.toolCallId },
          });
          return yield* events.toolCallFailed({
            toolCallId: created.event.payload.toolCallId,
            error,
            promptPart: toolFailedPromptPart(created, error),
          });
        }),
      });

      yield* annotateEdaSpan({ "eda.tool.outcome": guarded.value._tag });
      return { created, outcome: guarded.value };
    }),
  };
});

/** Executes sealed framework-owned tool calls and commits tool lifecycle boundaries. */
export class ToolExecutor extends Context.Service<ToolExecutor, ToolExecutorShape>()(
  "@effect-durable-agent/ToolExecutor",
) {
  static readonly Live = Layer.effect(ToolExecutor, makeLiveToolExecutor);
}

const toolCompletedPromptPart = (
  created: CommittedToolCallCreated,
  result: unknown,
): ToolCallCompletedPayload["promptPart"] => ({
  ...Prompt.toolResultPart({
    id: created.event.payload.promptPart.id,
    name: created.event.payload.promptPart.name,
    isFailure: false,
    result,
  }),
  id: created.event.payload.promptPart.id,
  name: created.event.payload.promptPart.name,
  isFailure: false,
});

const toolFailedPromptPart = (
  created: CommittedToolCallCreated,
  error: FailurePayload,
): ToolCallFailedPayload["promptPart"] => ({
  ...Prompt.toolResultPart({
    id: created.event.payload.promptPart.id,
    name: created.event.payload.promptPart.name,
    isFailure: true,
    result: error,
  }),
  id: created.event.payload.promptPart.id,
  name: created.event.payload.promptPart.name,
  isFailure: true,
  result: error,
});

const isToolCallCreated = (
  committed: CommittedDurableEvent,
): committed is CommittedToolCallCreated => committed.event.type === toolCallCreatedEventType;

const requireToolCallCreated = (committed: CommittedDurableEvent) =>
  isToolCallCreated(committed)
    ? Effect.succeed(committed)
    : new ToolExecutionEventNotCreated({ eventType: committed.event.type });
