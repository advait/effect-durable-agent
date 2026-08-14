import * as RivetHost from "effect-durable-agent-rivet";
import * as RivetActor from "effect-durable-agent-rivet/actor";
import * as RivetRuntime from "effect-durable-agent-rivet/runtime";
import * as RivetStorage from "effect-durable-agent-rivet/storage";
import * as RivetWebSocketProtocol from "effect-durable-agent-rivet/websocket-protocol";
import type * as Layer from "effect/Layer";
import type * as LanguageModel from "effect/unstable/ai/LanguageModel";
import { setup } from "rivetkit";

export const rivetPublicModules = [
  RivetHost,
  RivetActor,
  RivetRuntime,
  RivetStorage,
  RivetWebSocketProtocol,
];

export const makePackedRivetRegistry = (modelLayer: Layer.Layer<LanguageModel.LanguageModel>) =>
  setup({
    use: {
      edaSession: RivetHost.createEDASessionRivetActor({
        config: { modelSelection: { provider: "consumer", modelId: "consumer-model" } },
        modelLayer,
      }),
    },
  });
