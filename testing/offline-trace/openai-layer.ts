import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";

/** Real OpenAI LanguageModel layer for offline trace runs. */
export const makeOfflineOpenAiLanguageModelLayer = (options: {
  readonly apiKey: string;
  readonly modelId: string;
  readonly apiUrl?: string;
}) => {
  const client = OpenAiClient.layer({
    apiKey: Redacted.make(options.apiKey),
    ...(options.apiUrl === undefined ? {} : { apiUrl: options.apiUrl }),
  }).pipe(Layer.provide(FetchHttpClient.layer));

  return OpenAiLanguageModel.layer({ model: options.modelId }).pipe(Layer.provide(client));
};
