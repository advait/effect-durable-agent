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
import type * as LanguageModel from "effect/ai/LanguageModel";
import type * as Prompt from "effect/ai/Prompt";

import {
  makeEDAAnthropicClientLayer,
  prepareEDAAnthropicPrompt,
  type EDAAnthropicClientOptions,
  type EDAAnthropicModelConfig,
} from "./anthropic";
import {
  EDAGoogleClient,
  makeEDAGoogleClientLayer,
  makeEDAGoogleLanguageModel,
  type EDAGoogleClientOptions,
  type EDAGoogleModelConfig,
} from "./google";
import { makeEDAOpenAiClientLayer, type EDAOpenAiModelConfig } from "./openai";
import {
  ANTHROPIC_COMPACTION_BETA,
  EDAProviderRequestOverride,
  prepareNativeCompaction,
  withBeta,
  withoutOtherProvidersState,
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
    }
  | {
      readonly provider: "google";
      readonly modelId: string;
      readonly config: EDAGoogleModelConfig;
    };

/** Clients for every provider a host serves; a session runs on the one its selection names. */
export interface EDAMultiProviderModelResolverOptions {
  readonly openai?: {
    readonly aiGateway?: AiGateway | undefined;
    readonly apiKey?: string | undefined;
    readonly apiUrl?: string | undefined;
  };
  readonly anthropic?: EDAAnthropicClientOptions;
  readonly google?: EDAGoogleClientOptions;
  /** Host policy: validate the recorded selection and choose request settings. Throw to reject. */
  readonly resolve: (selection: ModelSelectionPayload | undefined) => EDAResolvedModel;
}

type Provider = EDAResolvedModel["provider"];
type ResolvedFor<P extends Provider> = Extract<EDAResolvedModel, { readonly provider: P }>;
type ModelMaker<P extends Provider> = (
  resolved: ResolvedFor<P>,
) => Effect.Effect<LanguageModel.LanguageModel>;

/** Everything one provider contributes to resolution, prompt preparation and compaction. */
interface ProviderAdapter<P extends Provider> {
  readonly name: string;
  /**
   * Builds the provider's client layer now (throwing when the options cannot authenticate) and
   * returns the effect that starts the client and makes a model per resolved selection.
   */
  readonly connect: (
    options: NonNullable<EDAMultiProviderModelResolverOptions[P]>,
  ) => Effect.Effect<ModelMaker<P>>;
  /** Rewrites an EDA prompt into the provider's cache and compaction shape. */
  readonly preparePrompt: (prompt: Prompt.Prompt) => Prompt.Prompt;
  /** Provider-native compaction; without one, sessions compact with the prompt-prefix summary. */
  readonly compact:
    | ((
        input: CompactionExecutorInput,
        options: EDANativeCompactionOptions,
      ) => Effect.Effect<CompactionOutput, CompactionError>)
    | undefined;
}

const PROVIDERS: { readonly [P in Provider]: ProviderAdapter<P> } = {
  openai: {
    name: "OpenAI",
    connect: (options) => {
      const layer = makeEDAOpenAiClientLayer(options);
      return Effect.map(
        OpenAiClient.OpenAiClient.pipe(Effect.provide(layer)),
        (client) => (resolved) =>
          OpenAiLanguageModel.make({ model: resolved.modelId, config: resolved.config }).pipe(
            Effect.provideService(OpenAiClient.OpenAiClient, client),
          ),
      );
    },
    preparePrompt: (prompt) => prepareNativeCompaction("openai", prompt),
    compact: (input) => openAiCompaction(input),
  },
  anthropic: {
    name: "Anthropic",
    connect: (options) => {
      const layer = makeEDAAnthropicClientLayer(options);
      return Effect.map(
        AnthropicClient.AnthropicClient.pipe(Effect.provide(layer)),
        (client) => (resolved) =>
          AnthropicLanguageModel.make({ model: resolved.modelId, config: resolved.config }).pipe(
            Effect.provideService(AnthropicClient.AnthropicClient, client),
          ),
      );
    },
    preparePrompt: prepareEDAAnthropicPrompt,
    compact: (input, options) => anthropicCompaction(input, options),
  },
  google: {
    name: "Google",
    connect: (options) => {
      const layer = makeEDAGoogleClientLayer(options);
      return Effect.map(
        EDAGoogleClient.pipe(Effect.provide(layer)),
        (client) => (resolved) =>
          makeEDAGoogleLanguageModel({ modelId: resolved.modelId, config: resolved.config }).pipe(
            Effect.provideService(EDAGoogleClient, client),
          ),
      );
    },
    // Gemini caches prompt prefixes implicitly and stores no native compaction items.
    preparePrompt: (prompt) => prompt,
    // Gemini has no compaction endpoint.
    compact: undefined,
  },
};

const isProvider = (provider: string | undefined): provider is Provider =>
  provider !== undefined && Object.hasOwn(PROVIDERS, provider);

const connect = <P extends Provider>(
  provider: P,
  options: EDAMultiProviderModelResolverOptions[P] | undefined,
): Effect.Effect<ModelMaker<P> | undefined> =>
  options === undefined ? Effect.succeed(undefined) : PROVIDERS[provider].connect(options);

const makeModel = <P extends Provider>(
  makers: { readonly [Q in Provider]: ModelMaker<Q> | undefined },
  provider: P,
  resolved: ResolvedFor<P>,
): Effect.Effect<LanguageModel.LanguageModel> => {
  const make = makers[provider];
  return make === undefined
    ? Effect.die(new Error(`No ${PROVIDERS[provider].name} client is configured.`))
    : make(resolved);
};

/**
 * Resolves each durable selection to the provider it names, sharing one client per provider for
 * the runtime lifetime, and prepares prompts in that provider's cache and compaction shape,
 * without the parts that hold only another provider's state.
 */
export const makeEDAMultiProviderModelResolverLayer = (
  options: EDAMultiProviderModelResolverOptions,
): Layer.Layer<ModelResolver> => {
  const clients = {
    openai: connect("openai", options.openai),
    anthropic: connect("anthropic", options.anthropic),
    google: connect("google", options.google),
  } satisfies { readonly [P in Provider]: Effect.Effect<ModelMaker<P> | undefined> };
  return Layer.effect(
    ModelResolver,
    Effect.gen(function* () {
      const makers = yield* Effect.all(clients);
      return {
        resolve: Effect.fn("MultiProviderModelResolver.resolve")(function* (selection) {
          const resolved = options.resolve(selection);
          return yield* makeModel(makers, resolved.provider, resolved);
        }),
        preparePrompt: (selection, prompt) =>
          isProvider(selection?.provider)
            ? PROVIDERS[selection.provider].preparePrompt(
                withoutOtherProvidersState(selection.provider, prompt),
              )
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
 * summarized history on later requests. Providers without one (Google, and any selection EDA does
 * not know) summarize with `promptPrefixSummaryCompactionExecutor`, which re-sends the same
 * earlier prompt with the summary request appended.
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
          const provider = input.state.modelSelection?.provider;
          const compact = isProvider(provider) ? PROVIDERS[provider].compact : undefined;
          return compact === undefined ? fallback.execute(input) : compact(input, options);
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
export type { EDAGoogleModelConfig } from "./google";
export type { EDAOpenAiModelConfig } from "./openai";
