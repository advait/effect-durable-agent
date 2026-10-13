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
  /** Beta flags sent on every request, e.g. `thinking-binding-controls-2026-08-01`. */
  readonly betas?: ReadonlyArray<string> | undefined;
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
        makeEDAProviderHttpClient({
          provider: "anthropic",
          aiGateway: options.aiGateway,
          anthropicBetas: options.betas,
        }),
      ),
    ),
  );
};

/** Beta that lets requests choose what happens to thinking bound to an edited conversation. */
export const ANTHROPIC_THINKING_BINDING_BETA = "thinking-binding-controls-2026-08-01";

/** Agent request policy for Claude 5.5-generation models. */
export interface EDAAnthropicAgentSettings {
  /** Thinking depth; Opus 5.5 cannot disable thinking, so effort is the only control. */
  readonly effort: "low" | "medium" | "high";
  /** Output budget including thinking. Defaults to 64,000. */
  readonly maxTokens?: number | undefined;
  /** `summarized` streams readable reasoning summaries. Defaults to `summarized`. */
  readonly thinkingDisplay?: "summarized" | "omitted" | undefined;
  /**
   * Explicit preserved-thinking policy (requires {@link ANTHROPIC_THINKING_BINDING_BETA}).
   * `drop_block` degrades instead of failing when replayed thinking no longer matches.
   */
  readonly prefixMismatchBehavior?: "error" | "drop_block" | undefined;
}

/** Model config for EDA agent sessions on Anthropic. */
export const anthropicAgentModelConfig = (
  settings: EDAAnthropicAgentSettings,
): EDAAnthropicModelConfig => {
  const thinking = {
    type: "adaptive" as const,
    display: settings.thinkingDisplay ?? "summarized",
    ...(settings.prefixMismatchBehavior === undefined
      ? {}
      : { block_binding: { prefix_mismatch_behavior: settings.prefixMismatchBehavior } }),
  };
  const config: EDAAnthropicModelConfig = {
    max_tokens: settings.maxTokens ?? 64_000,
    output_config: { effort: settings.effort },
    thinking,
    // EDA tool schemas are not written for strict mode's constraints.
    strictJsonSchema: false,
  };
  return config;
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
