import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Prompt from "effect/ai/Prompt";
import type * as Schema from "effect/Schema";

import { contextProjectionPrompt } from "../domain/message-transcript";
import type { ContextProjection } from "../domain/context-projection";
import type { ReducedState } from "../domain/reduced-state";
import type { SessionId } from "../types/core";
import type { EDAReducerStateSnapshot } from "./reducer-registry";

/** Input for app-extensible state-to-LLM prompt projection. */
export interface EDAPromptProjectionInput {
  readonly sessionId: SessionId;
  readonly state: ReducedState;
  readonly reducerStates: EDAReducerStateSnapshot;
  readonly context?: ContextProjection;
}

/** App-derived instructions and data inserted around the durable transcript. */
export interface EDAProjectedPromptContext {
  /** Instruction messages placed after durable system context and before transcript history. */
  readonly instructions: ReadonlyArray<Prompt.SystemMessage>;
  /** Data messages placed after transcript history and before scheduler-selected user messages. */
  readonly messages: ReadonlyArray<Prompt.UserMessage>;
}

/** Host-extensible projection from durable state into provider prompt context. */
export interface EDAPromptProjectorShape {
  readonly projectContext: (
    input: EDAPromptProjectionInput,
  ) => Effect.Effect<EDAProjectedPromptContext>;
  readonly projectState: (input: EDAPromptProjectionInput) => Effect.Effect<ReducedState>;
}

/** Build a provider prompt entirely from the current durable state projection. */
export const buildEDAPrompt = (
  projector: EDAPromptProjectorShape,
  input: EDAPromptProjectionInput,
): Effect.Effect<Prompt.Prompt> =>
  Effect.gen(function* () {
    const state = yield* projector.projectState(input);
    const context = yield* projector.projectContext(input);
    const transcript = contextProjectionPrompt(state, input.context);
    const instructionBoundary = firstNonSystemMessageIndex(transcript.content);
    const instructions = [
      ...transcript.content.slice(0, instructionBoundary),
      ...context.instructions,
    ];
    const history = transcript.content.slice(instructionBoundary);
    return Prompt.fromMessages([
      ...markLast(instructions),
      ...markLast(history),
      ...context.messages,
    ]);
  });

/** Prompt option namespace EDA uses for provider-neutral request hints. */
export const EDA_PROMPT_OPTIONS_KEY = "effect-durable-agent";

/**
 * Whether a message ends a stable prefix worth caching. EDA marks the end of the instruction
 * prefix and the end of durable history, never the per-request data messages after it; provider
 * adapters translate the hint into their own cache breakpoints.
 */
export const isEDACacheBreakpoint = (message: Prompt.Message): boolean => {
  const hint: unknown = message.options[EDA_PROMPT_OPTIONS_KEY];
  return isHintRecord(hint) && hint.cacheBreakpoint === true;
};

const isHintRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const markLast = <M extends Prompt.Message>(messages: ReadonlyArray<M>): ReadonlyArray<M> =>
  messages.map((message, index) =>
    index === messages.length - 1
      ? {
          ...message,
          options: { ...message.options, [EDA_PROMPT_OPTIONS_KEY]: { cacheBreakpoint: true } },
        }
      : message,
  );

const firstNonSystemMessageIndex = (messages: ReadonlyArray<Prompt.Message>): number => {
  const index = messages.findIndex((message) => message.role !== "system");
  return index === -1 ? messages.length : index;
};

/** Service that lets apps derive LLM context independently from UI projections. */
export class EDAPromptProjector extends Context.Service<
  EDAPromptProjector,
  EDAPromptProjectorShape
>()("@effect-durable-agent/EDAPromptProjector") {
  static readonly Default = Layer.succeed(EDAPromptProjector, {
    projectContext: () => Effect.succeed({ instructions: [], messages: [] }),
    projectState: (input) => Effect.succeed(input.state),
  } satisfies EDAPromptProjectorShape);
}

/**
 * Provider-native compaction output (an Anthropic `compaction` block, an OpenAI compacted window)
 * stored on the summary prompt message. The provider that produced it replays `items` verbatim in
 * place of the message; any other provider sees the message's readable summary text.
 */
export interface EDANativeCompaction {
  readonly provider: string;
  readonly modelId: string;
  readonly items: ReadonlyArray<Schema.Json>;
}

/** Summary prompt message carrying readable text plus the provider-native compaction items. */
export const nativeCompactionSummaryMessage = (
  text: string,
  native: EDANativeCompaction,
): Prompt.UserMessage =>
  Prompt.makeMessage("user", {
    content: [Prompt.textPart({ text })],
    options: {
      [EDA_PROMPT_OPTIONS_KEY]: {
        nativeCompaction: {
          provider: native.provider,
          modelId: native.modelId,
          items: [...native.items],
        },
      },
    },
  });

/** Native compaction items stored on a prompt message, if any. */
export const getNativeCompaction = (message: Prompt.Message): EDANativeCompaction | undefined => {
  const hint: unknown = message.options[EDA_PROMPT_OPTIONS_KEY];
  if (!isHintRecord(hint)) return undefined;
  const native: unknown = hint.nativeCompaction;
  if (!isHintRecord(native)) return undefined;
  const { provider, modelId, items } = native;
  if (typeof provider !== "string" || typeof modelId !== "string" || !isJsonArray(items)) {
    return undefined;
  }
  return { provider, modelId, items };
};

const isJsonArray = (value: unknown): value is ReadonlyArray<Schema.Json> => Array.isArray(value);
