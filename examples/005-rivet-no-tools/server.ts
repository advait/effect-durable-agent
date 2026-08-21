import { setup } from "rivetkit";

import {
  createEDASessionRivetActor,
  makeEDARivetOpenAiModelLayer,
} from "effect-durable-agent-rivet";

const apiKey = process.env.OPENAI_API_KEY;
if (apiKey === undefined) {
  throw new Error("OPENAI_API_KEY is required");
}

/** One Rivet Actor per EDA SessionId, with no application tools. */
export const edaSession = createEDASessionRivetActor({
  config: {
    modelSelection: { provider: "openai", modelId: "gpt-5-mini" },
    systemPrompt: "Answer briefly and clearly.",
  },
  modelLayer: makeEDARivetOpenAiModelLayer({
    apiKey,
    modelId: "gpt-5-mini",
  }),
});

export const registry = setup({ use: { edaSession } });

registry.start();
