import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as LanguageModel from "effect/ai/LanguageModel";
import type * as Prompt from "effect/ai/Prompt";
import type { ModelSelectionPayload } from "../types/events";

/** Resolves a recorded selection into an executable provider service at a model-call boundary. */
export class ModelResolver extends Context.Service<
  ModelResolver,
  {
    readonly resolve: (
      selection: ModelSelectionPayload | undefined,
    ) => Effect.Effect<LanguageModel.LanguageModel>;
    /**
     * Rewrites an EDA prompt into the selected provider's request shape just before a model call,
     * for example turning EDA cache hints into provider cache breakpoints. Defaults to identity.
     */
    readonly preparePrompt?: (
      selection: ModelSelectionPayload | undefined,
      prompt: Prompt.Prompt,
    ) => Prompt.Prompt;
  }
>()("@effect-durable-agent/ModelResolver") {
  /** Fixed provider implementation for deterministic tests and single-model hosts. */
  static readonly Fixed = Layer.effect(
    ModelResolver,
    Effect.gen(function* () {
      const model = yield* LanguageModel.LanguageModel;
      return { resolve: () => Effect.succeed(model) };
    }),
  );
}
