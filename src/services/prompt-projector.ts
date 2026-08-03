import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Prompt from "effect/unstable/ai/Prompt";

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
    return Prompt.fromMessages([
      ...transcript.content.slice(0, instructionBoundary),
      ...context.instructions,
      ...transcript.content.slice(instructionBoundary),
      ...context.messages,
    ]);
  });

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
