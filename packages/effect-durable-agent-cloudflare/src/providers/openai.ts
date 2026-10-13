import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { ModelResolver } from "effect-durable-agent/services/model-resolver";
import type { ModelSelectionPayload } from "effect-durable-agent/types/events";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import type * as LanguageModel from "effect/ai/LanguageModel";
import * as HttpClient from "effect/http/HttpClient";

import { makeEDAProviderHttpClient } from "./transport";

/** Provider request defaults accepted by EDA's OpenAI Responses model adapter. */
export type EDAOpenAiModelConfig = Omit<typeof OpenAiLanguageModel.Config.Service, "model">;

/** Resolves each durable selection while sharing the provider transport for the runtime lifetime. */
export const makeEDADurableObjectOpenAiModelResolverLayer = (options: {
  readonly aiGateway?: AiGateway;
  readonly apiKey?: string;
  readonly apiUrl?: string;
  readonly resolve: (selection: ModelSelectionPayload | undefined) => {
    readonly modelId: string;
    readonly config: EDAOpenAiModelConfig;
  };
}): Layer.Layer<ModelResolver> => {
  const apiKey = optionalNonEmptyString(options.apiKey);
  if (apiKey === undefined && options.aiGateway === undefined) {
    throw new Error(
      "OPENAI_API_KEY is required unless the EDA OpenAI model resolver is backed by a Cloudflare AI Gateway binding.",
    );
  }
  const client = makeEDAOpenAiClientLayer(options);
  return Layer.effect(
    ModelResolver,
    Effect.gen(function* () {
      const provider = yield* OpenAiClient.OpenAiClient;
      return {
        resolve: Effect.fn("OpenAiModelResolver.resolve")(function* (selection) {
          const resolved = options.resolve(selection);
          return yield* OpenAiLanguageModel.make({
            model: resolved.modelId,
            config: resolved.config,
          }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, provider));
        }),
      };
    }),
  ).pipe(Layer.provide(client));
};

/** OpenAI-backed Effect AI model layer for a Cloudflare Worker. */
export const makeEDADurableObjectOpenAiModelLayer = (options: {
  readonly aiGateway?: AiGateway;
  readonly apiKey?: string;
  readonly apiUrl?: string;
  readonly config?: EDAOpenAiModelConfig;
  readonly modelId: string;
}): Layer.Layer<LanguageModel.LanguageModel> => {
  const apiKey = optionalNonEmptyString(options.apiKey);
  if (apiKey === undefined && options.aiGateway === undefined) {
    throw new Error(
      "OPENAI_API_KEY is required unless the EDA OpenAI model layer is backed by a Cloudflare AI Gateway binding.",
    );
  }

  const client = makeEDAOpenAiClientLayer(options);

  return OpenAiLanguageModel.layer({
    model: options.modelId,
    ...(options.config === undefined ? {} : { config: options.config }),
  }).pipe(Layer.provide(client));
};

/** OpenAI client layer over EDA's provider transport (Cloudflare AI Gateway or `fetch`). */
export const makeEDAOpenAiClientLayer = (options: {
  readonly aiGateway?: AiGateway | undefined;
  readonly apiKey?: string | undefined;
  readonly apiUrl?: string | undefined;
}): Layer.Layer<OpenAiClient.OpenAiClient> => {
  const apiKey = optionalNonEmptyString(options.apiKey);
  if (apiKey === undefined && options.aiGateway === undefined) {
    throw new Error(
      "OPENAI_API_KEY is required unless the EDA OpenAI client is backed by a Cloudflare AI Gateway binding.",
    );
  }
  return OpenAiClient.layer({
    ...(apiKey === undefined ? {} : { apiKey: Redacted.make(apiKey) }),
    ...(options.apiUrl === undefined ? {} : { apiUrl: options.apiUrl }),
  }).pipe(
    Layer.provide(
      Layer.succeed(
        HttpClient.HttpClient,
        makeEDAProviderHttpClient({ provider: "openai", aiGateway: options.aiGateway }),
      ),
    ),
  );
};

const optionalNonEmptyString = (value: string | undefined): string | undefined => {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
};
