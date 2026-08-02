import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Prompt from "effect/unstable/ai/Prompt";

import { contextProjectionPrompt } from "../domain/message-transcript";
import type { ContextProjection } from "../domain/context-projection";
import type { ReducedState } from "../domain/reduced-state";
import type { UserMessageContent } from "../types/commands";
import type { CommandId, MessageId, RunId, SessionId } from "../types/core";
import type { EDAReducerStateSnapshot } from "./reducer-registry";

/** User content selected for a turn before its consuming TurnStarted event commits. */
export interface SelectedPromptUserMessage {
  readonly commandId: CommandId;
  readonly content: UserMessageContent;
  readonly messageId: MessageId;
}

/** Input for app-extensible state-to-LLM prompt projection. */
export interface EDAPromptProjectionInput {
  readonly commandId: CommandId;
  readonly runId: RunId;
  readonly sessionId: SessionId;
  readonly state: ReducedState;
  readonly reducerStates: EDAReducerStateSnapshot;
  readonly context?: ContextProjection;
  readonly selectedUserMessages: ReadonlyArray<SelectedPromptUserMessage>;
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
  readonly projectUserMessageContent: (
    input: EDAPromptProjectionInput,
    message: SelectedPromptUserMessage,
  ) => Effect.Effect<UserMessageContent>;
}

/** Build a provider prompt while preserving framework-owned selected-message cardinality/order. */
export const buildEDAPrompt = (
  projector: EDAPromptProjectorShape,
  input: EDAPromptProjectionInput,
): Effect.Effect<Prompt.Prompt> =>
  Effect.gen(function* () {
    const state = yield* projector.projectState(input);
    const context = yield* projector.projectContext(input);
    const contents = yield* Effect.forEach(
      input.selectedUserMessages.filter((message) => !isAlreadyProjectedFromState(input, message)),
      (message) => projector.projectUserMessageContent(input, message),
    );
    const transcript = contextProjectionPrompt(state, input.context);
    const instructionBoundary = firstNonSystemMessageIndex(transcript.content);
    return Prompt.fromMessages([
      ...transcript.content.slice(0, instructionBoundary),
      ...context.instructions,
      ...transcript.content.slice(instructionBoundary),
      ...context.messages,
      ...contents.map((content) => Prompt.makeMessage("user", { content })),
    ]);
  });

const firstNonSystemMessageIndex = (messages: ReadonlyArray<Prompt.Message>): number => {
  const index = messages.findIndex((message) => message.role !== "system");
  return index === -1 ? messages.length : index;
};

/** Legacy committed user messages are already visible in the durable transcript projection. */
const isAlreadyProjectedFromState = (
  input: EDAPromptProjectionInput,
  message: SelectedPromptUserMessage,
): boolean => {
  const durable = input.state.messages.get(message.messageId);
  return durable?._tag === "User" && durable.requestedDisposition === undefined;
};

/** Service that lets apps derive LLM context independently from UI projections. */
export class EDAPromptProjector extends Context.Service<
  EDAPromptProjector,
  EDAPromptProjectorShape
>()("@effect-durable-agent/EDAPromptProjector") {
  static readonly Default = Layer.succeed(EDAPromptProjector, {
    projectContext: () => Effect.succeed({ instructions: [], messages: [] }),
    projectState: (input) => Effect.succeed(input.state),
    projectUserMessageContent: (_input, message) => Effect.succeed(message.content),
  } satisfies EDAPromptProjectorShape);
}
