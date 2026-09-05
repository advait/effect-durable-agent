import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type * as AiError from "effect/unstable/ai/AiError";
import { ModelResolver } from "./model-resolver";
import { makeUsagePayload } from "./provider-usage";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";

import { assertNeverError } from "../domain/assert-never";
import type {
  InferenceEmission,
  InferenceState,
  InferenceStepInput,
  EphemeralPayload,
  FinalizePayload,
  ToolValidation,
} from "../domain/inference-state";
import { initialInferenceState, step } from "../domain/inference-state";
import { InferenceId, RunId, TurnId } from "../types/core";
import type {
  AssistantPromptParts,
  EDADurableEvent,
  InferenceFailedPayload,
} from "../types/events";
import {
  FailurePayload,
  ModelSelectionPayload,
  NonNegativeInt,
  ProviderPartId,
  ToolName,
  UsagePayload,
} from "../types/events";
import {
  CommittedDurableEvent,
  EDASessionStoreError,
  hasEDASessionStoreError,
} from "./session-store";
import { EventFactory } from "./event-factory";
import { IdGenerator } from "./id-generator";
import { failurePayloadFromCause, failurePayloadFromUnknown } from "./started-boundary-guard";
import { EDAToolRegistry } from "./tool-registry";
import type { ToolParamsSchema } from "./tool-registry";
import type { SessionEventSink } from "./session-event-sink";
import { annotateEdaSpan, committedBatchAttributes, usageAttributes } from "./tracing";

/** Provider stream parts this runner explicitly handles or rejects with policy. */
export type InferenceRunnerStreamPart =
  | Response.TextStartPart
  | Response.TextDeltaPart
  | Response.TextEndPart
  | Response.ReasoningStartPart
  | Response.ReasoningDeltaPart
  | Response.ReasoningEndPart
  | Response.ToolParamsStartPart
  | Response.ToolParamsDeltaPart
  | Response.ToolParamsEndPart
  | Response.ToolCallPart<string, unknown>
  | Response.ToolResultPart<string, unknown, unknown>
  | Response.ToolApprovalRequestPart
  | Response.FilePart
  | Response.DocumentSourcePart
  | Response.UrlSourcePart
  | Response.ResponseMetadataPart
  | Response.FinishPart
  | Response.ErrorPart;

/** Input identity for running one model inference stream. */
export const RunInferenceContext = Schema.Struct({
  runId: RunId,
  turnId: TurnId,
  inferenceId: InferenceId,
});
export type RunInferenceContext = typeof RunInferenceContext.Type;

/** Effectful inputs for running one provider inference stream. */
export interface RunInferenceInput extends RunInferenceContext {
  readonly prompt: Prompt.RawInput;
  readonly eventSink: SessionEventSink;
  readonly modelSelection?: ModelSelectionPayload;
}

/** Terminal result when the inference stream reached a provider finish part. */
export const InferenceRunFinished = Schema.TaggedStruct("InferenceRunFinished", {
  committed: Schema.Array(CommittedDurableEvent),
  assistantText: Schema.String,
  reasoningText: Schema.String,
  responseParts: Schema.Array(Schema.Any),
});
export type InferenceRunFinished = typeof InferenceRunFinished.Type;

/** Terminal result when the inference stream reached or synthesized an error part. */
export const InferenceRunFailed = Schema.TaggedStruct("InferenceRunFailed", {
  committed: Schema.Array(CommittedDurableEvent),
});
export type InferenceRunFailed = typeof InferenceRunFailed.Type;

/** Terminal variants for a consumed provider inference stream. */
export const InferenceRunTerminal = Schema.Union([InferenceRunFinished, InferenceRunFailed]);
export type InferenceRunTerminal = typeof InferenceRunTerminal.Type;

/** Summary returned after one provider inference stream is consumed. */
export const InferenceRunResult = Schema.Struct({
  started: CommittedDurableEvent,
  partsRecorded: NonNegativeInt,
  terminal: InferenceRunTerminal,
});
export type InferenceRunResult = typeof InferenceRunResult.Type;

/** Failure for provider sideband parts without current EDA storage/UI semantics. */
export class UnsupportedInferencePart extends Schema.TaggedErrorClass<UnsupportedInferencePart>()(
  "UnsupportedInferencePart",
  {
    partType: Schema.String,
    reason: Schema.String,
  },
) {}

/** Error surface for model inference streaming and part policy. */
export type InferenceRunnerError = EDASessionStoreError | UnsupportedInferencePart;

/** Runtime service that owns one provider stream fold and durable finalization. */
export interface InferenceRunnerShape {
  /** Run one provider inference stream and fold provider parts into inference state. */
  readonly runInference: (
    input: RunInferenceInput,
  ) => Effect.Effect<InferenceRunResult, InferenceRunnerError>;
}

type ResponseContextEntry =
  | {
      readonly _tag: "Text";
      readonly providerPartId: string;
      readonly text: string;
    }
  | {
      readonly _tag: "Reasoning";
      readonly providerPartId: string;
      readonly text: string;
    }
  | {
      readonly _tag: "ResponsePart";
      readonly part: Response.AnyPart;
    };

interface RunInferenceState {
  readonly inference: InferenceState;
  readonly partsRecorded: number;
  readonly terminal: InferenceRunTerminal | undefined;
  readonly responseContext: ReadonlyArray<ResponseContextEntry>;
}

/** Construct the live inference-runner implementation. */
const makeLiveInferenceRunner = Effect.gen(function* () {
  const ids = yield* IdGenerator;
  const events = yield* EventFactory;
  const models = yield* ModelResolver;
  const toolRegistry = yield* EDAToolRegistry;

  const recordPart = (
    eventSink: SessionEventSink,
    state: RunInferenceState,
    part: InferenceRunnerStreamPart,
    setLatestState: (state: RunInferenceState) => Effect.Effect<void>,
  ) =>
    Effect.gen(function* () {
      if (state.inference.phase === "sealed") {
        yield* Effect.logWarning("dropping provider part after terminal inference boundary", {
          inferenceId: state.inference.inferenceId,
          partType: part.type,
        });
        return state;
      }

      if (isProviderSidebandPart(part) || isIgnoredToolResultPart(part)) {
        yield* Effect.logDebug("dropping provider sideband response part", {
          inferenceId: state.inference.inferenceId,
          partType: part.type,
        });
        return { ...state, partsRecorded: state.partsRecorded + 1 };
      }

      const input = yield* makeStepInput(state.inference, part);
      const stepped = step(state.inference, input);
      const responseContext = recordResponseContext(state.responseContext, part);
      const nextBeforeEmissions = {
        inference: stepped.next,
        partsRecorded: state.partsRecorded + 1,
        terminal: state.terminal,
        responseContext,
      } satisfies RunInferenceState;
      yield* setLatestState(nextBeforeEmissions);
      const terminal = yield* interpretEmissions(
        eventSink,
        stepped.emissions,
        stepped.next,
        responseContextToParts(responseContext),
      );
      return {
        ...nextBeforeEmissions,
        terminal: terminal ?? state.terminal,
      };
    });

  const makeStepInput = (state: InferenceState, part: InferenceRunnerStreamPart) =>
    Effect.gen(function* () {
      switch (part.type) {
        case "text-start":
        case "text-end":
        case "reasoning-start":
        case "reasoning-end":
          return {
            type: part.type,
            providerPartId: ProviderPartId.make(part.id),
          } satisfies InferenceStepInput;
        case "text-delta":
        case "reasoning-delta":
          return {
            type: part.type,
            providerPartId: ProviderPartId.make(part.id),
            delta: part.delta,
          } satisfies InferenceStepInput;
        case "tool-params-start": {
          if (part.providerExecuted) {
            return unsupportedProviderExecutedTool(part.name, part.id) satisfies InferenceStepInput;
          }
          const providerPartId = ProviderPartId.make(part.id);
          // step re-resolves draft IDs; this lookup only avoids minting an unused ID.
          const existing = state.drafts.get(providerPartId);
          const toolCallId = existing?.toolCallId ?? (yield* ids.makeToolCallId());
          return {
            type: "tool-params-start",
            providerPartId,
            toolCallId,
            toolName: ToolName.make(part.name),
            providerExecuted: part.providerExecuted,
          } satisfies InferenceStepInput;
        }
        case "tool-params-delta":
          return {
            type: "tool-params-delta",
            providerPartId: ProviderPartId.make(part.id),
            delta: part.delta,
          } satisfies InferenceStepInput;
        case "tool-params-end":
          return {
            type: "tool-params-end",
            providerPartId: ProviderPartId.make(part.id),
          } satisfies InferenceStepInput;
        case "tool-call": {
          if (part.providerExecuted) {
            return unsupportedProviderExecutedTool(part.name, part.id) satisfies InferenceStepInput;
          }
          const providerPartId = ProviderPartId.make(part.id);
          // step re-resolves draft IDs; this lookup only avoids minting an unused ID.
          const existing = state.drafts.get(providerPartId);
          const toolCallId = existing?.toolCallId ?? (yield* ids.makeToolCallId());
          return {
            type: "tool-call",
            providerPartId,
            toolCallId,
            toolName: ToolName.make(part.name),
            params: part.params,
            providerExecuted: part.providerExecuted,
            validation: yield* validateToolCall(part.name, part.params),
          } satisfies InferenceStepInput;
        }
        case "tool-result":
          return unsupportedProviderExecutedTool(part.name, part.id) satisfies InferenceStepInput;
        case "response-metadata":
          return {
            type: "response-metadata",
            responseMetadata: makeResponseMetadata(part),
          } satisfies InferenceStepInput;
        case "finish":
          return {
            type: "finish",
            finishReason: part.reason,
            usage: makeUsagePayload(part.usage),
            finishMetadata: makeFinishMetadata(part),
          } satisfies InferenceStepInput;
        case "error":
          return {
            type: "error",
            error: failurePayloadFromUnknown(part.error),
          } satisfies InferenceStepInput;
        case "tool-approval-request":
        case "file":
        case "source":
          return yield* Effect.die(
            new Error(`Provider sideband part escaped no-op handling: ${part.type}`),
          );
        default:
          return yield* Effect.die(assertNeverError(part, "inference stream part"));
      }
    });

  const validateToolCall = (toolName: string, params: unknown) =>
    Effect.gen(function* () {
      const schema = yield* toolRegistry.getParamsSchema(ToolName.make(toolName));
      if (schema === undefined) {
        return { _tag: "UnknownTool" } satisfies ToolValidation;
      }
      return validateParams(schema, params);
    });

  const interpretEmissions = (
    eventSink: SessionEventSink,
    emissions: ReadonlyArray<InferenceEmission>,
    state: InferenceState,
    responseParts: ReadonlyArray<Response.AnyPart>,
  ) =>
    Effect.gen(function* () {
      let terminal: InferenceRunTerminal | undefined;
      for (const emission of emissions) {
        switch (emission.kind) {
          case "ephemeral":
            yield* publishEphemeral(eventSink, emission.event);
            break;
          case "finalize":
            terminal = yield* commitFinalize(eventSink, emission.events, state, responseParts);
            break;
          case "fail":
            terminal = yield* commitFailure(eventSink, emission.payload, state, responseParts);
            break;
          default:
            return yield* Effect.die(assertNeverError(emission, "inference emission"));
        }
      }
      return terminal;
    });

  const publishEphemeral = (eventSink: SessionEventSink, emission: EphemeralPayload) =>
    Effect.gen(function* () {
      switch (emission.type) {
        case "TextDelta": {
          const event = yield* events.textDelta(emission.payload);
          yield* eventSink.publishEphemeral(event);
          return;
        }
        case "ReasoningDelta": {
          const event = yield* events.reasoningDelta(emission.payload);
          yield* eventSink.publishEphemeral(event);
          return;
        }
        case "ToolParamsStart": {
          const event = yield* events.toolParamsStart(emission.payload);
          yield* eventSink.publishEphemeral(event);
          return;
        }
        case "ToolParamsDelta": {
          const event = yield* events.toolParamsDelta(emission.payload);
          yield* eventSink.publishEphemeral(event);
          return;
        }
        case "ToolParamsEnd": {
          const event = yield* events.toolParamsEnd(emission.payload);
          yield* eventSink.publishEphemeral(event);
          return;
        }
        default:
          return yield* Effect.die(assertNeverError(emission, "ephemeral emission"));
      }
    });

  const commitFinalize = (
    eventSink: SessionEventSink,
    payloads: ReadonlyArray<FinalizePayload>,
    state: InferenceState,
    responseParts: ReadonlyArray<Response.AnyPart>,
  ) =>
    Effect.gen(function* () {
      const durableEvents: Array<EDADurableEvent> = [];
      const assistant = yield* assistantMessageEvent(state, responseParts);
      if (assistant !== undefined) {
        durableEvents.push(assistant);
      }
      for (const payload of payloads) {
        durableEvents.push(yield* makeFinalizeEvent(payload));
      }
      const committed = yield* eventSink.appendDurableBatch(durableEvents);
      return InferenceRunFinished.make({
        committed,
        assistantText: state.assistantText,
        reasoningText: state.reasoningText,
        responseParts,
      });
    });

  const assistantMessageEvent = (
    state: InferenceState,
    responseParts: ReadonlyArray<Response.AnyPart>,
  ) =>
    Effect.gen(function* () {
      const promptParts = nonEmptyAssistantPromptParts(
        assistantPromptPartsFromResponseParts(responseParts),
      );
      if (promptParts === undefined && !hasAssistantContent(state)) {
        return undefined;
      }
      const messageId = yield* ids.makeMessageId();
      return yield* events.assistantMessageCommitted({
        messageId,
        runId: state.runId,
        turnId: state.turnId,
        inferenceId: state.inferenceId,
        promptParts: promptParts ?? legacyAssistantPromptParts(state),
      });
    });

  const assistantPartialEvent = (
    state: InferenceState,
    reason: string,
    responseParts: ReadonlyArray<Response.AnyPart>,
  ) =>
    Effect.gen(function* () {
      if (!hasAssistantContent(state)) {
        return undefined;
      }
      const messageId = yield* ids.makeMessageId();
      return yield* events.assistantPartialCommitted({
        messageId,
        runId: state.runId,
        turnId: state.turnId,
        inferenceId: state.inferenceId,
        promptParts:
          nonEmptyAssistantPromptParts(
            partialAssistantPromptPartsFromResponseParts(responseParts),
          ) ?? legacyAssistantPromptParts(state),
        reason,
      });
    });

  const commitFailure = (
    eventSink: SessionEventSink,
    payload: InferenceFailedPayload,
    state: InferenceState,
    responseParts: ReadonlyArray<Response.AnyPart>,
  ) =>
    Effect.gen(function* () {
      const partial = yield* assistantPartialEvent(
        state,
        "inference failed before completion",
        responseParts,
      );
      const event = yield* events.inferenceFailed(payload);
      const committed = yield* eventSink.appendDurableBatch(
        partial === undefined ? [event] : [partial, event],
      );
      return InferenceRunFailed.make({ committed });
    });

  const makeFinalizeEvent = (payload: FinalizePayload): Effect.Effect<EDADurableEvent> => {
    switch (payload.type) {
      case "InferenceCompleted":
        return events.inferenceCompleted(payload.payload);
      case "ToolCallCreated":
        return events.toolCallCreated(payload.payload);
      case "ToolCallRejected":
        return events.toolCallRejected(payload.payload);
      default:
        return Effect.die(assertNeverError(payload, "finalize payload"));
    }
  };

  const recordMissingFinish = (eventSink: SessionEventSink, state: InferenceState) =>
    Effect.gen(function* () {
      const stepped = step(state, {
        type: "error",
        error: failurePayloadFromUnknown(new Error("Provider stream ended without a finish part")),
      });
      const terminal = yield* interpretEmissions(eventSink, stepped.emissions, stepped.next, []);
      return yield* requireTerminal(terminal);
    });

  return {
    runInference: Effect.fn("agent.inference")(function* (input: RunInferenceInput) {
      const languageModel = yield* models.resolve(input.modelSelection);
      const inferenceIdentity = {
        runId: input.runId,
        turnId: input.turnId,
        inferenceId: input.inferenceId,
      };
      yield* annotateEdaSpan({
        "eda.run.id": input.runId,
        "eda.turn.id": input.turnId,
        "eda.inference.id": input.inferenceId,
        "eda.model.provider": input.modelSelection?.provider,
        "eda.model.id": input.modelSelection?.modelId,
      });
      const initialState = initialRunInferenceState(inferenceIdentity);
      const stateRef = yield* Ref.make(initialState);
      const start = Effect.gen(function* () {
        const startedEvent = yield* events.inferenceStarted(inferenceIdentity);
        return yield* input.eventSink.appendDurable(startedEvent);
      });
      const body = (started: CommittedDurableEvent) =>
        Effect.gen(function* () {
          const toolkit = yield* toolRegistry.getModelToolkit();
          const providerStream = (
            languageModel.streamText({
              prompt: input.prompt,
              disableToolCallResolution: true,
              ...(toolkit === undefined ? {} : { toolkit }),
            } as never) as Stream.Stream<
              Response.StreamPart<Record<string, never>>,
              AiError.AiError
            >
          ).pipe(Stream.map((part) => part as InferenceRunnerStreamPart));
          const state = yield* providerStream.pipe(
            Stream.catchIf(isProviderStreamFailure, (error) =>
              Stream.make(makeProviderErrorPart(error)),
            ),
            Stream.runFoldEffect(
              () => initialState,
              (current, part) =>
                recordPart(input.eventSink, current, part, (next) => Ref.set(stateRef, next)).pipe(
                  Effect.tap((next) => Ref.set(stateRef, next)),
                ),
            ),
          );
          const terminal =
            state.terminal ?? (yield* recordMissingFinish(input.eventSink, state.inference));
          return {
            started,
            value: InferenceRunResult.make({
              started,
              partsRecorded: NonNegativeInt.make(state.partsRecorded),
              terminal,
            }),
          };
        });
      const release = (
        _started: CommittedDurableEvent,
        exit: Exit.Exit<
          { readonly started: CommittedDurableEvent; readonly value: InferenceRunResult },
          InferenceRunnerError
        >,
      ): Effect.Effect<void, InferenceRunnerError> => {
        if (Exit.isSuccess(exit)) {
          return Effect.void;
        }
        if (hasEDASessionStoreError(exit.cause)) {
          return Effect.failCause(exit.cause);
        }
        return Effect.uninterruptible(
          Effect.gen(function* () {
            const latest = yield* Ref.get(stateRef);
            const partial = yield* assistantPartialEvent(
              latest.inference,
              Cause.hasInterruptsOnly(exit.cause)
                ? "inference interrupted before completion"
                : "inference failed before completion",
              responseContextToParts(latest.responseContext),
            );
            const terminal = yield* events.inferenceFailed({
              ...inferenceIdentity,
              error: Cause.hasInterruptsOnly(exit.cause)
                ? FailurePayload.make({
                    message: "inference interrupted",
                    code: "inference.interrupted",
                    details: { reason: "interrupted" },
                  })
                : failurePayloadFromCause(exit.cause),
            });
            yield* input.eventSink.appendDurableBatch(
              partial === undefined ? [terminal] : [partial, terminal],
            );
          }),
        );
      };
      const guarded = yield* Effect.acquireUseRelease(start, body, release);
      yield* annotateEdaSpan({
        "eda.provider.parts_recorded": guarded.value.partsRecorded,
        "eda.inference.terminal": guarded.value.terminal._tag,
        ...inferenceUsageAttributes(guarded.value.terminal),
        ...committedBatchAttributes(guarded.value.terminal.committed),
      });
      return guarded.value;
    }),
  };
});

/** Owns provider inference streams and routes them into durable/live inference emissions. */
export class InferenceRunner extends Context.Service<InferenceRunner, InferenceRunnerShape>()(
  "@effect-durable-agent/InferenceRunner",
) {
  static readonly Live = Layer.effect(InferenceRunner, makeLiveInferenceRunner);
}

const inferenceUsageAttributes = (terminal: InferenceRunTerminal) => {
  if (terminal._tag !== "InferenceRunFinished") {
    return {};
  }
  const completed = terminal.committed.find((entry) => entry.event.type === "InferenceCompleted");
  return usageAttributes(
    (completed?.event.payload as { readonly usage?: UsagePayload } | undefined)?.usage,
  );
};

const initialRunInferenceState = (identity: RunInferenceContext): RunInferenceState => ({
  inference: initialInferenceState(identity),
  partsRecorded: 0,
  terminal: undefined,
  responseContext: [],
});

const recordResponseContext = (
  entries: ReadonlyArray<ResponseContextEntry>,
  part: InferenceRunnerStreamPart,
): ReadonlyArray<ResponseContextEntry> => {
  switch (part.type) {
    case "text-start":
      return upsertTextualResponseEntry(entries, "Text", part.id, "");
    case "text-delta":
      return upsertTextualResponseEntry(entries, "Text", part.id, part.delta);
    case "text-end":
      return entries;
    case "reasoning-start":
      return upsertTextualResponseEntry(entries, "Reasoning", part.id, "");
    case "reasoning-delta":
      return upsertTextualResponseEntry(entries, "Reasoning", part.id, part.delta);
    case "reasoning-end":
      return entries;
    case "tool-call":
      return part.providerExecuted ? entries : [...entries, { _tag: "ResponsePart", part }];
    case "tool-result":
      return entries;
    case "tool-params-start":
    case "tool-params-delta":
    case "tool-params-end":
    case "response-metadata":
    case "finish":
    case "error":
    case "tool-approval-request":
    case "file":
    case "source":
      return entries;
    default:
      throw assertNeverError(part, "response context part");
  }
};

const upsertTextualResponseEntry = (
  entries: ReadonlyArray<ResponseContextEntry>,
  tag: "Text" | "Reasoning",
  providerPartId: string,
  delta: string,
): ReadonlyArray<ResponseContextEntry> => {
  const index = entries.findIndex(
    (entry) =>
      (entry._tag === "Text" || entry._tag === "Reasoning") &&
      entry._tag === tag &&
      entry.providerPartId === providerPartId,
  );
  if (index === -1) {
    return [...entries, { _tag: tag, providerPartId, text: delta }];
  }
  const current = entries[index]!;
  if (current._tag !== tag) {
    return entries;
  }
  return entries.map((entry, entryIndex) =>
    entryIndex === index ? { ...current, text: `${current.text}${delta}` } : entry,
  );
};

const responseContextToParts = (
  entries: ReadonlyArray<ResponseContextEntry>,
): ReadonlyArray<Response.AnyPart> =>
  entries.flatMap((entry): ReadonlyArray<Response.AnyPart> => {
    switch (entry._tag) {
      case "Text":
        return entry.text.length === 0 ? [] : [Response.makePart("text", { text: entry.text })];
      case "Reasoning":
        return entry.text.length === 0
          ? []
          : [Response.makePart("reasoning", { text: entry.text })];
      case "ResponsePart":
        return [entry.part];
      default:
        throw assertNeverError(entry, "response context entry");
    }
  });

const assistantPromptPartsFromResponseParts = (
  responseParts: ReadonlyArray<Response.AnyPart>,
): ReadonlyArray<Prompt.AssistantMessagePart> => {
  const assistant = Prompt.fromResponseParts(responseParts).content.find(
    (message): message is Prompt.AssistantMessage => message.role === "assistant",
  );
  return assistant?.content ?? [];
};

const partialAssistantPromptPartsFromResponseParts = (
  responseParts: ReadonlyArray<Response.AnyPart>,
): ReadonlyArray<Prompt.AssistantMessagePart> =>
  assistantPromptPartsFromResponseParts(responseParts).filter((part) => part.type !== "tool-call");

const nonEmptyAssistantPromptParts = (
  parts: ReadonlyArray<Prompt.AssistantMessagePart>,
): AssistantPromptParts | undefined =>
  parts.length === 0 ? undefined : (parts as AssistantPromptParts);

const legacyAssistantPromptParts = (state: InferenceState): AssistantPromptParts =>
  nonEmptyAssistantPromptParts([
    ...(state.reasoningText.length === 0
      ? []
      : [Prompt.reasoningPart({ text: state.reasoningText })]),
    ...(state.assistantText.length === 0 ? [] : [Prompt.textPart({ text: state.assistantText })]),
  ]) ?? [Prompt.textPart({ text: "" })];

const isProviderStreamFailure = (_error: AiError.AiError): _error is AiError.AiError => true;

const makeProviderErrorPart = (error: unknown): InferenceRunnerStreamPart =>
  Response.makePart("error", { error }) as InferenceRunnerStreamPart;

const unsupportedProviderExecutedTool = (toolName: string, providerPartId: string) => ({
  type: "error" as const,
  error: FailurePayload.make({
    message: `Provider-executed tool calls are unsupported: ${toolName}`,
    code: "tool.unsupported_provider_executed",
    details: { providerPartId, toolName },
  }),
});

const isProviderSidebandPart = (
  part: InferenceRunnerStreamPart,
): part is
  | Response.ToolApprovalRequestPart
  | Response.FilePart
  | Response.DocumentSourcePart
  | Response.UrlSourcePart =>
  part.type === "tool-approval-request" || part.type === "file" || part.type === "source";

const isIgnoredToolResultPart = (part: InferenceRunnerStreamPart): boolean =>
  part.type === "tool-result" && (part.providerExecuted !== true || part.preliminary === true);

const hasAssistantContent = (state: InferenceState): boolean =>
  state.assistantText.length > 0 || state.reasoningText.length > 0;

const validateParams = (schema: ToolParamsSchema, params: unknown): ToolValidation => {
  try {
    return {
      _tag: "ValidToolParams",
      params: Schema.decodeUnknownSync(schema as Schema.Decoder<unknown>)(params),
    };
  } catch (error) {
    return { _tag: "InvalidToolParams", message: formatSchemaError(error) };
  }
};

const formatSchemaError = (error: unknown): string => {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
};

const requireTerminal = (terminal: InferenceRunTerminal | undefined) =>
  terminal === undefined
    ? Effect.die(new Error("Expected terminal inference result"))
    : Effect.succeed(terminal);

const makeResponseMetadata = (part: Response.ResponseMetadataPart) => ({
  id: part.id,
  modelId: part.modelId,
  timestamp: part.timestamp,
  request: part.request,
  metadata: part.metadata,
});

const makeFinishMetadata = (part: Response.FinishPart) => ({
  response: part.response,
  metadata: part.metadata,
});
