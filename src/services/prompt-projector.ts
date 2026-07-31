import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Prompt from "effect/unstable/ai/Prompt";

import { contextProjectionPromptWithUserContents } from "../domain/message-transcript";
import type { ContextProjection } from "../domain/context-projection";
import type { ReducedState } from "../domain/reduced-state";
import type { UserMessageContent } from "../types/commands";
import type { CommandId, MessageId, SessionId } from "../types/core";
import type { EDAReducerStateSnapshot } from "./reducer-registry";

/** User content selected for a turn before its consuming TurnStarted event commits. */
export interface SelectedPromptUserMessage {
  readonly commandId: CommandId;
  readonly content: UserMessageContent;
  readonly messageId: MessageId;
}

/** Input for app-extensible state-to-LLM prompt projection. */
export interface EDAPromptProjectionInput {
  readonly sessionId: SessionId;
  readonly state: ReducedState;
  readonly reducerStates: EDAReducerStateSnapshot;
  readonly context?: ContextProjection;
  readonly selectedUserMessages: ReadonlyArray<SelectedPromptUserMessage>;
}

/** Host-extensible projection from durable state into provider prompt context. */
export interface EDAPromptProjectorShape {
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
    const contents = yield* Effect.forEach(
      input.selectedUserMessages.filter((message) => !isAlreadyProjectedFromState(input, message)),
      (message) => projector.projectUserMessageContent(input, message),
    );
    return contextProjectionPromptWithUserContents(state, input.context, contents);
  });

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
    projectState: (input) => Effect.succeed(input.state),
    projectUserMessageContent: (_input, message) => Effect.succeed(message.content),
  } satisfies EDAPromptProjectorShape);
}
