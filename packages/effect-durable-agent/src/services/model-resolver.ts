import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as LanguageModel from "effect/unstable/ai/LanguageModel";
import type { ModelSelectionPayload } from "../types/events";

/** Resolves a recorded selection into an executable provider service at a model-call boundary. */
export class ModelResolver extends Context.Service<
  ModelResolver,
  {
    readonly resolve: (
      selection: ModelSelectionPayload | undefined,
    ) => Effect.Effect<LanguageModel.Service>;
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
