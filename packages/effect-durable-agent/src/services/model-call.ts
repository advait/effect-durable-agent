import type * as Effect from "effect/Effect";
import type * as AiError from "effect/ai/AiError";
import type * as LanguageModel from "effect/ai/LanguageModel";
import type * as Prompt from "effect/ai/Prompt";

import type { EDAModelToolkit } from "./tool-registry";

/**
 * One non-streaming model call that offers EDA's tools without executing them, so the request's
 * tool definitions match every inference in the session (and therefore its provider cache).
 * EDA owns tool execution; with resolution disabled the handlers' requirements are never used.
 */
export const generateTextOfferingTools = (
  model: LanguageModel.LanguageModel,
  prompt: Prompt.RawInput,
  toolkit: EDAModelToolkit | undefined,
): Effect.Effect<LanguageModel.GenerateTextResponse<{}>, AiError.AiError> =>
  (toolkit === undefined
    ? model.generateText({ prompt })
    : model.generateText({
        prompt,
        toolkit,
        disableToolCallResolution: true,
      } as never)) as Effect.Effect<LanguageModel.GenerateTextResponse<{}>, AiError.AiError>;
