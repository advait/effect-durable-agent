import { AnthropicClient, AnthropicLanguageModel } from "@effect/ai-anthropic";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import {
  CompactionError,
  CompactionExecutor,
  CompactionOutput,
  promptPrefixSummaryCompactionExecutor,
  type CompactionExecutorInput,
  type CompactionExecutorShape,
} from "effect-durable-agent/services/compaction";
import { generateTextOfferingTools } from "effect-durable-agent/services/model-call";
import { ModelResolver } from "effect-durable-agent/services/model-resolver";
import { nativeCompactionSummaryMessage } from "effect-durable-agent/services/prompt-projector";
import type { ModelSelectionPayload, UsagePayload } from "effect-durable-agent/types/events";
import { CompactionExecutorId } from "effect-durable-agent/domain/context-projection";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import type * as Schema from "effect/Schema";

import {
  makeEDAAnthropicClientLayer,
  prepareEDAAnthropicPrompt,
  type EDAAnthropicClientOptions,
  type EDAAnthropicModelConfig,
} from "./anthropic";
import { makeEDAOpenAiClientLayer, type EDAOpenAiModelConfig } from "./openai";
import {
  ANTHROPIC_COMPACTION_BETA,
  EDAProviderRequestOverride,
  prepareNativeCompaction,
  withBeta,
  type EDAProviderRequest,
} from "./transport";

/** Model request settings the host derives from a durable selection. */
export type EDAResolvedModel =
  | {
      readonly provider: "openai";
      readonly modelId: string;
      readonly config: EDAOpenAiModelConfig;
    }
  | {
      readonly provider: "anthropic";
      readonly modelId: string;
      readonly config: EDAAnthropicModelConfig;
    };

/** Clients for every provider a host serves; a session runs on the one its selection names. */
export interface EDAMultiProviderModelResolverOptions {
  readonly openai?: {
    readonly aiGateway?: AiGateway | undefined;
    readonly apiKey?: string | undefined;
    readonly apiUrl?: string | undefined;
  };
  readonly anthropic?: EDAAnthropicClientOptions;
  /** Host policy: validate the recorded selection and choose request settings. Throw to reject. */
  readonly resolve: (selection: ModelSelectionPayload | undefined) => EDAResolvedModel;
}

/**
 * Resolves each durable selection to the provider it names, sharing one client per provider for
 * the runtime lifetime, and prepares prompts in that provider's cache and compaction shape.
 */
export const makeEDAMultiProviderModelResolverLayer = (
  options: EDAMultiProviderModelResolverOptions,
): Layer.Layer<ModelResolver> => {
  const openAiClient =
    options.openai === undefined ? undefined : makeEDAOpenAiClientLayer(options.openai);
  const anthropicClient =
    options.anthropic === undefined ? undefined : makeEDAAnthropicClientLayer(options.anthropic);
  return Layer.effect(
    ModelResolver,
    Effect.gen(function* () {
      const openAi =
        openAiClient === undefined
          ? undefined
          : yield* OpenAiClient.OpenAiClient.pipe(Effect.provide(openAiClient));
      const anthropic =
        anthropicClient === undefined
          ? undefined
          : yield* AnthropicClient.AnthropicClient.pipe(Effect.provide(anthropicClient));
      return {
        resolve: Effect.fn("MultiProviderModelResolver.resolve")(function* (selection) {
          const resolved = options.resolve(selection);
          if (resolved.provider === "anthropic") {
            if (anthropic === undefined) {
              return yield* Effect.die(new Error("No Anthropic client is configured."));
            }
            return yield* AnthropicLanguageModel.make({
              model: resolved.modelId,
              config: resolved.config,
            }).pipe(Effect.provideService(AnthropicClient.AnthropicClient, anthropic));
          }
          if (openAi === undefined) {
            return yield* Effect.die(new Error("No OpenAI client is configured."));
          }
          return yield* OpenAiLanguageModel.make({
            model: resolved.modelId,
            config: resolved.config,
          }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, openAi));
        }),
        preparePrompt: (selection, prompt) =>
          selection?.provider === "anthropic"
            ? prepareEDAAnthropicPrompt(prompt)
            : selection?.provider === "openai"
              ? prepareNativeCompaction("openai", prompt)
              : prompt,
      };
    }),
  );
};

/** Options for provider-native compaction. */
export interface EDANativeCompactionOptions {
  /**
   * Custom summarization prompt for Anthropic (`compaction.instructions`, up to 16,384 characters).
   * OpenAI compaction takes no prompt.
   */
  readonly anthropicInstructions?: string | undefined;
  /** Output budget for the Anthropic summary call, thinking included. Defaults to 16,000. */
  readonly anthropicMaxTokens?: number | undefined;
}

/**
 * Compacts with the session provider's own endpoint: OpenAI `/responses/compact`, Anthropic
 * on-demand compaction (`compact-2026-09-04`). Each call carries the exact earlier request prompt,
 * so it reads the conversation from the provider cache, and the returned native items replace the
 * summarized history on later requests. Other providers fall back to a prompt-prefix summary.
 */
export const makeEDANativeCompactionExecutorLayer = (
  options: EDANativeCompactionOptions = {},
): Layer.Layer<CompactionExecutor> =>
  Layer.effect(
    CompactionExecutor,
    Effect.gen(function* () {
      const fallback = yield* CompactionExecutor.pipe(
        Effect.provide(promptPrefixSummaryCompactionExecutor()),
      );
      return {
        id: CompactionExecutorId.make("provider-native"),
        execute: (input) => {
          switch (input.state.modelSelection?.provider) {
            case "anthropic":
              return anthropicCompaction(input, options);
            case "openai":
              return openAiCompaction(input);
            default:
              return fallback.execute(input);
          }
        },
      } satisfies CompactionExecutorShape;
    }),
  );

interface CapturedResponse {
  readonly status: number;
  readonly body: unknown;
}

/** Run the adapter's request with an override and return the raw provider response. */
const callWithOverride = (
  input: CompactionExecutorInput,
  rewrite: (request: EDAProviderRequest) => EDAProviderRequest,
) =>
  Effect.gen(function* () {
    let captured: CapturedResponse | undefined;
    const exit = yield* generateTextOfferingTools(input.model, input.prompt, input.toolkit).pipe(
      Effect.provideService(EDAProviderRequestOverride, {
        rewrite,
        onResponse: (status, body) => {
          captured = { status, body };
        },
      }),
      Effect.exit,
    );
    if (captured === undefined) {
      return yield* new CompactionError({
        message: `compaction request failed before a provider response: ${
          Exit.isFailure(exit) ? String(exit.cause) : "no response"
        }`,
      });
    }
    if (captured.status < 200 || captured.status >= 300) {
      return yield* new CompactionError({
        message: `compaction request failed with ${captured.status}: ${providerErrorMessage(captured.body)}`,
      });
    }
    return captured.body;
  });

const anthropicCompaction = (input: CompactionExecutorInput, options: EDANativeCompactionOptions) =>
  Effect.gen(function* () {
    const body = yield* callWithOverride(input, (request) => ({
      ...request,
      headers: {
        ...withBeta(request.headers, ANTHROPIC_COMPACTION_BETA),
        accept: "text/event-stream",
      },
      body: isRecord(request.body)
        ? {
            ...request.body,
            max_tokens: options.anthropicMaxTokens ?? 16_000,
            // Streaming keeps the connection active (pings) while the summary is written.
            stream: true,
            compaction: {
              type: "summarize",
              ...(options.anthropicInstructions === undefined
                ? {}
                : { instructions: options.anthropicInstructions }),
            },
          }
        : request.body,
    })).pipe(Effect.map(anthropicMessageFromStream));
    const usage = anthropicCompactionUsage(body);
    const block = isRecord(body) && Array.isArray(body.content) ? body.content[0] : undefined;
    if (
      !isRecord(body) ||
      body.stop_reason !== "compaction" ||
      !isRecord(block) ||
      block.type !== "compaction" ||
      typeof block.content !== "string" ||
      block.content.trim().length === 0
    ) {
      return yield* new CompactionError({
        message: `anthropic compaction returned no summary (stop_reason ${
          isRecord(body) ? String(body.stop_reason) : "unknown"
        })`,
        ...(usage === undefined ? {} : { usage }),
      });
    }
    const text = block.content.trim();
    return CompactionOutput.make({
      text,
      promptMessage: nativeCompactionSummaryMessage(`Conversation summary:\n\n${text}`, {
        provider: "anthropic",
        modelId: input.state.modelSelection?.modelId ?? "unknown",
        items: [jsonValue(block)],
      }),
      executorId: CompactionExecutorId.make("anthropic-native"),
      ...(usage === undefined ? {} : { usage }),
    });
  });

const openAiCompaction = (input: CompactionExecutorInput) =>
  Effect.gen(function* () {
    const body = yield* callWithOverride(input, (request) => ({
      ...request,
      url: new URL(
        request.url.pathname.replace(/\/responses$/u, "/responses/compact"),
        request.url,
      ),
      body: isRecord(request.body)
        ? Object.fromEntries(
            Object.entries(request.body).filter(([key]) =>
              ["model", "input", "instructions", "tools"].includes(key),
            ),
          )
        : request.body,
    }));
    const usage = openAiUsage(isRecord(body) ? body.usage : undefined);
    const output = isRecord(body) && Array.isArray(body.output) ? body.output : [];
    // The host sends its own instructions; keep only conversation items from the compacted window.
    const items = output.filter(
      (item) => !(isRecord(item) && (item.role === "developer" || item.role === "system")),
    );
    if (!items.some((item) => isRecord(item) && item.type === "compaction")) {
      return yield* new CompactionError({
        message: "openai compaction returned no compaction item",
        ...(usage === undefined ? {} : { usage }),
      });
    }
    const retainedUserMessages = items.filter(
      (item) => isRecord(item) && item.role === "user",
    ).length;
    const text = `Earlier conversation compacted by OpenAI into an encrypted summary (${retainedUserMessages} user messages retained verbatim).`;
    return CompactionOutput.make({
      text,
      promptMessage: nativeCompactionSummaryMessage(text, {
        provider: "openai",
        modelId: input.state.modelSelection?.modelId ?? "unknown",
        items: items.map(jsonValue),
      }),
      executorId: CompactionExecutorId.make("openai-native"),
      ...(usage === undefined ? {} : { usage }),
    });
  });

/**
 * Rebuild a Messages API response from its SSE stream. A compaction block arrives whole in its
 * `content_block_start`; usage (with `iterations`) arrives on `message_delta`.
 */
const anthropicMessageFromStream = (body: unknown): unknown => {
  if (typeof body !== "string") return body;
  let message: Record<string, unknown> = {};
  const content: Array<unknown> = [];
  for (const line of body.split("\n")) {
    if (!line.startsWith("data: ")) continue;
    const event: unknown = JSON.parse(line.slice("data: ".length));
    if (!isRecord(event)) continue;
    if (event.type === "message_start" && isRecord(event.message)) {
      message = { ...event.message };
    } else if (event.type === "content_block_start" && typeof event.index === "number") {
      content[event.index] = event.content_block;
    } else if (event.type === "message_delta") {
      const delta = isRecord(event.delta) ? event.delta : {};
      const usage = isRecord(message.usage) ? message.usage : {};
      message = {
        ...message,
        ...delta,
        usage: { ...usage, ...(isRecord(event.usage) ? event.usage : {}) },
      };
    } else if (event.type === "error") {
      return { error: event.error };
    }
  }
  return { ...message, content: content.filter((block) => block !== undefined) };
};

/** Anthropic reports compaction cost only in `usage.iterations`; top-level fields are zero. */
const anthropicCompactionUsage = (body: unknown): UsagePayload | undefined => {
  const usage = isRecord(body) ? body.usage : undefined;
  const iterations = isRecord(usage) && Array.isArray(usage.iterations) ? usage.iterations : [];
  if (iterations.length === 0) return undefined;
  let uncached = 0;
  let cacheRead = 0;
  let cacheWrite = 0;
  let output = 0;
  for (const iteration of iterations) {
    if (!isRecord(iteration)) continue;
    uncached += count(iteration.input_tokens);
    cacheRead += count(iteration.cache_read_input_tokens);
    cacheWrite += count(iteration.cache_creation_input_tokens);
    output += count(iteration.output_tokens);
  }
  return {
    inputTokens: uncached + cacheRead + cacheWrite,
    cachedInputTokens: cacheRead,
    cacheWriteInputTokens: cacheWrite,
    outputTokens: output,
  };
};

const openAiUsage = (usage: unknown): UsagePayload | undefined => {
  if (!isRecord(usage)) return undefined;
  const details = isRecord(usage.input_tokens_details) ? usage.input_tokens_details : {};
  return {
    inputTokens: count(usage.input_tokens),
    cachedInputTokens: count(details.cached_tokens),
    cacheWriteInputTokens: count(details.cache_write_tokens),
    outputTokens: count(usage.output_tokens),
  };
};

const count = (value: unknown): number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : 0;

const providerErrorMessage = (body: unknown): string => {
  if (isRecord(body) && isRecord(body.error) && typeof body.error.message === "string") {
    return body.error.message;
  }
  return typeof body === "string" ? body.slice(0, 500) : JSON.stringify(body).slice(0, 500);
};

const jsonValue = (value: unknown): Schema.Json => {
  const parsed: Schema.Json = JSON.parse(JSON.stringify(value) ?? "null");
  return parsed;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export { prepareEDAAnthropicPrompt } from "./anthropic";
export type { EDAAnthropicModelConfig } from "./anthropic";
export type { EDAOpenAiModelConfig } from "./openai";
