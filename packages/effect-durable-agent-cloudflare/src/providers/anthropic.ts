import { AnthropicClient, AnthropicLanguageModel } from "@effect/ai-anthropic";
import { isEDACacheBreakpoint } from "effect-durable-agent/services/prompt-projector";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import type * as Schema from "effect/Schema";
import * as Prompt from "effect/ai/Prompt";
import * as HttpClient from "effect/http/HttpClient";

import { makeEDAProviderHttpClient, prepareNativeCompaction } from "./transport";

/** Anthropic request defaults for one resolved model (everything but `model`). */
export type EDAAnthropicModelConfig = Omit<typeof AnthropicLanguageModel.Config.Service, "model">;

/** Shared Anthropic transport and client options. */
export interface EDAAnthropicClientOptions {
  /** Cloudflare AI Gateway binding. With a gateway-stored key (BYOK) no `apiKey` is needed. */
  readonly aiGateway?: AiGateway | undefined;
  readonly apiKey?: string | undefined;
  readonly apiUrl?: string | undefined;
}

/** Anthropic client layer over EDA's provider transport. */
export const makeEDAAnthropicClientLayer = (
  options: EDAAnthropicClientOptions,
): Layer.Layer<AnthropicClient.AnthropicClient> => {
  const apiKey = optionalNonEmptyString(options.apiKey);
  if (apiKey === undefined && options.aiGateway === undefined) {
    throw new Error(
      "ANTHROPIC_API_KEY is required unless Anthropic traffic goes through a Cloudflare AI Gateway binding with a stored key.",
    );
  }
  return AnthropicClient.layer({
    ...(apiKey === undefined ? {} : { apiKey: Redacted.make(apiKey) }),
    ...(options.apiUrl === undefined ? {} : { apiUrl: options.apiUrl }),
  }).pipe(
    Layer.provide(
      Layer.succeed(
        HttpClient.HttpClient,
        makeEDAProviderHttpClient({ provider: "anthropic", aiGateway: options.aiGateway }),
      ),
    ),
  );
};

/**
 * Anthropic prompt preparation: EDA cache hints become `cache_control` breakpoints (end of the
 * instruction prefix and end of durable history), and Anthropic compaction summaries become the
 * transport sentinel that is replayed as the signed `compaction` block.
 */
export const prepareEDAAnthropicPrompt = (prompt: Prompt.Prompt): Prompt.Prompt =>
  prepareNativeCompaction(
    "anthropic",
    Prompt.fromMessages(
      prompt.content.map((message) =>
        isEDACacheBreakpoint(message)
          ? withMessageOptions(message, {
              anthropic: {
                ...anthropicOptions(message.options),
                cacheControl: { type: "ephemeral" },
              },
            })
          : message,
      ),
    ),
  );

const anthropicOptions = (
  options: Prompt.ProviderOptions,
): Readonly<Record<string, Schema.Json>> => {
  const current = options.anthropic;
  return isJsonObject(current) ? current : {};
};

const isJsonObject = (
  value: Schema.Json | null | undefined,
): value is Readonly<Record<string, Schema.Json>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const withMessageOptions = (
  message: Prompt.Message,
  options: Prompt.ProviderOptions,
): Prompt.Message => ({ ...message, options: { ...message.options, ...options } });

const optionalNonEmptyString = (value: string | undefined): string | undefined => {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
};
