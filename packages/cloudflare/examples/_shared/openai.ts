import { edaRuntimeConfig, type EDASessionDurableObjectOptions } from "../../src/durable-object";
import { makeEDADurableObjectOpenAiModelLayer } from "../../src/durable-object-runtime";
import { optionalString } from "./http";

export const EXAMPLE_DEFAULT_MODEL_ID = "gpt-4.1-mini";

export interface ExampleOpenAiEnv {
  readonly EDA_OPENAI_API_URL?: string;
  readonly EDA_OPENAI_MODEL?: string;
  readonly EDA_SYSTEM_PROMPT?: string;
  readonly OPENAI_API_KEY?: string;
}

export const makeExampleOpenAiOptions = (
  env: ExampleOpenAiEnv,
): Pick<EDASessionDurableObjectOptions, "config" | "modelLayer"> => {
  const modelId = optionalString(env.EDA_OPENAI_MODEL) ?? EXAMPLE_DEFAULT_MODEL_ID;
  return {
    config: edaRuntimeConfig({
      modelId,
      provider: "openai",
      systemPrompt: optionalString(env.EDA_SYSTEM_PROMPT),
    }),
    modelLayer: makeEDADurableObjectOpenAiModelLayer({
      apiKey: optionalString(env.OPENAI_API_KEY),
      apiUrl: optionalString(env.EDA_OPENAI_API_URL),
      modelId,
    }),
  };
};
