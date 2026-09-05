import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { ModelResolver } from "effect-durable-agent/services/model-resolver";
import type { ModelSelectionPayload } from "effect-durable-agent/types/events";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import type * as LanguageModel from "effect/unstable/ai/LanguageModel";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as HttpBody from "effect/unstable/http/HttpBody";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientError from "effect/unstable/http/HttpClientError";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";

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
  const transport =
    options.aiGateway === undefined
      ? FetchHttpClient.layer
      : Layer.succeed(HttpClient.HttpClient, makeAiGatewayHttpClient(options.aiGateway));
  const client = OpenAiClient.layer({
    ...(apiKey === undefined ? {} : { apiKey: Redacted.make(apiKey) }),
    ...(options.apiUrl === undefined ? {} : { apiUrl: options.apiUrl }),
  }).pipe(Layer.provide(transport));
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

  const httpClient =
    options.aiGateway === undefined
      ? FetchHttpClient.layer
      : Layer.succeed(HttpClient.HttpClient, makeAiGatewayHttpClient(options.aiGateway));
  const client = OpenAiClient.layer({
    ...(apiKey === undefined ? {} : { apiKey: Redacted.make(apiKey) }),
    ...(options.apiUrl === undefined ? {} : { apiUrl: options.apiUrl }),
  }).pipe(Layer.provide(httpClient));

  return OpenAiLanguageModel.layer({
    model: options.modelId,
    ...(options.config === undefined ? {} : { config: options.config }),
  }).pipe(Layer.provide(client));
};

const makeAiGatewayHttpClient = (gateway: AiGateway): HttpClient.HttpClient =>
  HttpClient.make((request, url, signal) =>
    Effect.tryPromise({
      try: async () => {
        const response = await gateway.run(
          {
            endpoint: endpointFromUrl(url),
            headers: Object.fromEntries(
              Object.entries(request.headers).filter(
                (entry): entry is [string, string] =>
                  typeof entry[1] === "string" && entry[0].toLowerCase() !== "host",
              ),
            ),
            provider: "openai",
            query: requestBody(request.body),
          },
          { signal },
        );
        return HttpClientResponse.fromWeb(request, response);
      },
      catch: (cause) =>
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({
            request,
            cause,
            description: "Cloudflare AI Gateway request failed",
          }),
        }),
    }),
  );

const endpointFromUrl = (url: URL): string => {
  const path = url.pathname.startsWith("/v1/")
    ? url.pathname.slice("/v1/".length)
    : url.pathname.replace(/^\/+/, "");
  return `${path}${url.search}`;
};

const requestBody = (body: HttpBody.HttpBody): unknown => {
  switch (body._tag) {
    case "Empty":
      return undefined;
    case "Raw":
      return body.body;
    case "Uint8Array":
      return JSON.parse(new TextDecoder().decode(body.body)) as unknown;
    default:
      throw new Error(`Unsupported OpenAI request body for Cloudflare AI Gateway: ${body._tag}`);
  }
};

const optionalNonEmptyString = (value: string | undefined): string | undefined => {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
};
