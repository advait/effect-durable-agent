import * as AIResponse from "effect/unstable/ai/Response";
import * as Stream from "effect/Stream";
import { setup } from "rivetkit";
import { createClient } from "rivetkit/client";

import { makeLanguageModelLayer } from "effect-durable-agent/testkit/layers";
import { createEDASessionRivetActor } from "../../src/actor";

const usage = () =>
  new AIResponse.Usage({
    inputTokens: { total: 1, uncached: undefined, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: undefined, reasoning: undefined },
  });

const finishedStream = (text: string) =>
  Stream.make(
    AIResponse.makePart("text-delta", { id: "conformance-text", delta: text }),
    AIResponse.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
  );

const blockModel = process.env.EDA_CONFORMANCE_BLOCK_MODEL === "1";
const engineEndpoint = process.env.EDA_RIVET_ENGINE_ENDPOINT;
const authorization = process.env.EDA_RIVET_AUTHORIZATION;
const httpPort = Number(process.env.EDA_RIVET_HTTP_PORT);
if (engineEndpoint === undefined || authorization === undefined || !Number.isInteger(httpPort)) {
  throw new Error(
    "EDA_RIVET_ENGINE_ENDPOINT, EDA_RIVET_AUTHORIZATION, and an integer EDA_RIVET_HTTP_PORT are required",
  );
}

export const edaConformanceSession = createEDASessionRivetActor({
  authorize: ({ params }) => {
    if (
      typeof params !== "object" ||
      params === null ||
      !("authorization" in params) ||
      params.authorization !== authorization
    ) {
      throw new Error("Unauthorized EDA session connection");
    }
  },
  config: { modelSelection: { provider: "conformance", modelId: "fixed-pong" } },
  modelLayer: makeLanguageModelLayer(blockModel ? Stream.never : finishedStream("pong")),
});

export const registry = setup({
  use: { edaConformanceSession },
  configurePool: { url: `http://127.0.0.1:${httpPort}/api/rivet` },
  endpoint: engineEndpoint,
  serverless: { publicEndpoint: engineEndpoint },
  startEngine: false,
  httpHost: "127.0.0.1",
  httpPort,
  noWelcome: true,
  logging: { level: "error" },
});

let servingError: unknown;
const serving = registry.listen({ host: "127.0.0.1", port: httpPort }).catch((error) => {
  servingError = error;
});
const readinessActorId = "018f6bd5-2f2a-7b1e-8f1d-1f2e3d4c5b6a";
const readyDeadline = Date.now() + 60_000;
while (true) {
  if (servingError !== undefined) {
    throw servingError;
  }
  try {
    // A start request waits for RivetKit's background runner-pool upsert. Its
    // deliberately incomplete body may be rejected afterward; any response
    // other than pool-unavailable proves the engine can route actors here.
    const response = await fetch(`http://127.0.0.1:${httpPort}/api/rivet/start`, {
      body: "{}",
      headers: { "content-type": "application/json" },
      method: "POST",
    });
    if (response.status !== 503) {
      await createClient<typeof registry>(engineEndpoint)
        .edaConformanceSession.getOrCreate([readinessActorId], {
          params: { authorization },
        })
        .messages({});
      // The configuration write and the engine's routing cache converge
      // independently. Reconfirm after one cache tick before advertising ready.
      await new Promise((resolve) => setTimeout(resolve, 250));
      await createClient<typeof registry>(engineEndpoint)
        .edaConformanceSession.getOrCreate([readinessActorId], {
          params: { authorization },
        })
        .messages({});
      break;
    }
  } catch {
    // The serverless listener or its pool configuration is still starting.
  }
  if (Date.now() >= readyDeadline) {
    throw new Error("Timed out configuring the Rivet conformance runner pool");
  }
  await new Promise((resolve) => setTimeout(resolve, 100));
}
process.stdout.write("EDA_RIVET_READY\n");
await serving;
if (servingError !== undefined) {
  throw servingError;
}
