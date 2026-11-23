/**
 * Google Gemini API provider for EDA: a client over EDA's provider transport and an Effect AI
 * `LanguageModel` for `generateContent` and `streamGenerateContent`.
 *
 * Thought signatures are kept per part. Response text, reasoning and tool-call parts carry
 * `metadata.google.thoughtSignature`, which EDA persists on the committed prompt part and replays
 * as `options.google.thoughtSignature`; the request puts it back on the same part. Gemini asks for
 * every signature back on exactly the part that carried it, and rejects a function call replayed
 * without its signature.
 *
 * A signed part whose answer text is empty (the usual end of a streamed answer) is an empty
 * reasoning part marked `metadata.google.thought: false`: EDA keeps an empty part only as
 * reasoning, the part holds nothing but opaque thought state, and the mark replays it as the empty
 * text part Gemini sent. Other providers drop reasoning that lacks their own metadata.
 */
import * as Arr from "effect/Array";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as AiError from "effect/ai/AiError";
import * as IdGenerator from "effect/ai/IdGenerator";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Prompt from "effect/ai/Prompt";
import type * as Response from "effect/ai/Response";
import { addGenAIAnnotations } from "effect/ai/Telemetry";
import * as Tool from "effect/ai/Tool";
import * as Base64 from "effect/encoding/Base64";
import * as Sse from "effect/encoding/Sse";
import * as Headers from "effect/http/Headers";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import type * as HttpClientError from "effect/http/HttpClientError";
import * as HttpClientRequest from "effect/http/HttpClientRequest";

import {
  GoogleErrorBody,
  GoogleGenerateContentResponse,
  GoogleStreamChunk,
  type GoogleContent,
  type GoogleFunctionCallingMode,
  type GoogleFunctionDeclaration,
  type GoogleGenerateContentRequest,
  type GoogleRequestPart,
  type GoogleThinkingLevel,
  type GoogleToolConfig,
  type GoogleUsageMetadata,
} from "./google-wire";
import { makeEDAProviderHttpClient } from "./transport";

// =============================================================================
// Config
// =============================================================================

/** Gemini request defaults for one resolved model (everything but the model id). */
export interface EDAGoogleModelConfig {
  readonly generationConfig?: {
    readonly maxOutputTokens?: number | undefined;
    readonly thinkingConfig?: {
      readonly thinkingLevel?: GoogleThinkingLevel | undefined;
      readonly includeThoughts?: boolean | undefined;
    };
  };
  readonly toolConfig?: {
    /** Used when the request lets the model choose (`toolChoice: "auto"`), in place of `AUTO`. */
    readonly functionCallingConfig?: { readonly mode?: GoogleFunctionCallingMode | undefined };
  };
}

/** Agent request policy for Gemini 4 models. */
export interface EDAGoogleAgentSettings {
  /** Thinking depth. Gemini 4 cannot disable thinking, so effort is the only control. */
  readonly effort: "low" | "medium" | "high";
  /** Output budget including thinking. Defaults to 65,536, the Gemini 4 per-request maximum. */
  readonly maxOutputTokens?: number | undefined;
  /** Stream thought summaries as reasoning parts. Defaults to `true`. */
  readonly includeThoughts?: boolean | undefined;
}

/**
 * Effort to `thinkingConfig.thinkingLevel`, one to one: `low` is `low`, `medium` is `medium`,
 * `high` is `high` (Gemini's default). Gemini's `xhigh` is not offered as an effort, so the three
 * efforts mean the same on every provider. `thinkingBudget` is deprecated and returns 400 next to
 * `thinkingLevel`, so it is never sent.
 */
const THINKING_LEVEL = {
  low: "low",
  medium: "medium",
  high: "high",
} as const satisfies Record<EDAGoogleAgentSettings["effort"], GoogleThinkingLevel>;

/**
 * Model config for EDA agent sessions on Gemini 4. Effort maps to `thinkingLevel` one to one
 * (`low` → `low`, `medium` → `medium`, `high` → `high`; `xhigh` is not exposed), output defaults
 * to 65,536 tokens and thought summaries are on.
 */
export const googleAgentModelConfig = (settings: EDAGoogleAgentSettings): EDAGoogleModelConfig => ({
  generationConfig: {
    maxOutputTokens: settings.maxOutputTokens ?? 65_536,
    thinkingConfig: {
      thinkingLevel: THINKING_LEVEL[settings.effort],
      includeThoughts: settings.includeThoughts ?? true,
    },
  },
});

// =============================================================================
// Client
// =============================================================================

/** Low-level Gemini API calls, with every failure mapped to `AiError`. */
export interface EDAGoogleClientService {
  readonly generateContent: (
    model: string,
    request: GoogleGenerateContentRequest,
  ) => Effect.Effect<GoogleGenerateContentResponse, AiError.AiError>;
  /** Server-sent chunks, each a full response holding only the parts generated since the last. */
  readonly streamGenerateContent: (
    model: string,
    request: GoogleGenerateContentRequest,
  ) => Stream.Stream<GoogleGenerateContentResponse, AiError.AiError>;
}

/** Gemini API client service. */
export class EDAGoogleClient extends Context.Service<EDAGoogleClient, EDAGoogleClientService>()(
  "effect-durable-agent-cloudflare/EDAGoogleClient",
) {}

/** Shared Gemini transport and client options. */
export interface EDAGoogleClientOptions {
  /** Cloudflare AI Gateway binding; requests go to its `google-ai-studio` provider. */
  readonly aiGateway?: AiGateway | undefined;
  /** Gemini API key, sent as `x-goog-api-key`. Optional only behind a gateway that stores one. */
  readonly apiKey?: string | undefined;
  /** Defaults to `https://generativelanguage.googleapis.com`. */
  readonly apiUrl?: string | undefined;
}

const API_KEY_HEADER = "x-goog-api-key";

const withRedactedKey = Effect.updateService(
  Headers.CurrentRedactedNames,
  Arr.append(API_KEY_HEADER),
);

/** Gemini client layer over EDA's provider transport (Cloudflare AI Gateway or `fetch`). */
export const makeEDAGoogleClientLayer = (
  options: EDAGoogleClientOptions,
): Layer.Layer<EDAGoogleClient> => {
  const apiKey = optionalNonEmptyString(options.apiKey);
  if (apiKey === undefined && options.aiGateway === undefined) {
    throw new Error(
      "GEMINI_API_KEY is required unless Google traffic goes through a Cloudflare AI Gateway binding with a stored key.",
    );
  }
  const http = makeEDAProviderHttpClient({ provider: "google", aiGateway: options.aiGateway }).pipe(
    HttpClient.mapRequest((request) => {
      const based = HttpClientRequest.prependUrl(
        request,
        options.apiUrl ?? "https://generativelanguage.googleapis.com",
      );
      return apiKey === undefined
        ? based
        : HttpClientRequest.setHeader(based, API_KEY_HEADER, apiKey);
    }),
    HttpClient.filterStatusOk,
  );
  const post = (model: string, method: string, request: GoogleGenerateContentRequest) =>
    HttpClientRequest.post(`/v1beta/models/${model}:${method}`, {
      body: HttpBody.jsonUnsafe(request),
      ...(method === "streamGenerateContent" ? { urlParams: { alt: "sse" } } : {}),
    });

  return Layer.succeed(EDAGoogleClient, {
    generateContent: (model, request) =>
      http.execute(post(model, "generateContent", request)).pipe(
        Effect.flatMap((response) => response.json),
        Effect.flatMap(Schema.decodeUnknownEffect(GoogleGenerateContentResponse)),
        Effect.catchTags({
          HttpClientError: (error) => mapHttpClientError(error, "generateContent"),
          SchemaError: (error) => Effect.fail(invalidResponse(error, "generateContent")),
        }),
        withRedactedKey,
      ),
    streamGenerateContent: (model, body) => {
      const request = post(model, "streamGenerateContent", body);
      return http.execute(request).pipe(
        Effect.map((response) => response.stream),
        Stream.unwrap,
        Stream.decodeText,
        Stream.pipeThroughChannel(Sse.decodeDataSchema(GoogleStreamChunk)),
        // A failure after the response started arrives as an error event.
        Stream.mapEffect(({ data }) =>
          "error" in data
            ? Effect.fail(
                AiError.make({
                  module: "EDAGoogleClient",
                  method: "streamGenerateContent",
                  reason: statusReason({
                    status: data.error.code ?? 500,
                    body: data,
                    http: undefined,
                    request,
                  }),
                }),
              )
            : Effect.succeed(data),
        ),
        Stream.catchTags({
          Retry: (error) => Stream.die(error),
          SseError: (error) =>
            Stream.fail(
              AiError.make({
                module: "EDAGoogleClient",
                method: "streamGenerateContent",
                reason: new AiError.InvalidResponseError({ description: error.message }),
              }),
            ),
          HttpClientError: (error) =>
            Stream.fromEffect(mapHttpClientError(error, "streamGenerateContent")),
          SchemaError: (error) => Stream.fail(invalidResponse(error, "streamGenerateContent")),
        }),
        Stream.updateService(Headers.CurrentRedactedNames, Arr.append(API_KEY_HEADER)),
      );
    },
  });
};

// =============================================================================
// Errors
// =============================================================================

const invalidResponse = (error: Schema.SchemaError, method: string): AiError.AiError =>
  AiError.make({
    module: "EDAGoogleClient",
    method,
    reason: AiError.InvalidResponseError.fromSchemaError(error),
  });

const mapHttpClientError = (
  error: HttpClientError.HttpClientError,
  method: string,
): Effect.Effect<never, AiError.AiError> => {
  const reason = error.reason;
  switch (reason._tag) {
    case "TransportError":
    case "EncodeError":
    case "InvalidUrlError":
      return Effect.fail(
        AiError.make({
          module: "EDAGoogleClient",
          method,
          reason: new AiError.NetworkError({
            reason: reason._tag,
            method: reason.request.method,
            url: reason.request.url,
            description: reason.description,
            // The request (and its API key header) stays out of the error.
            cause: reason.cause,
          }),
        }),
      );
    case "StatusCodeError":
      return Effect.flatMap(
        Effect.orElseSucceed(reason.response.text, () => ""),
        (text) =>
          Effect.fail(
            AiError.make({
              module: "EDAGoogleClient",
              method,
              reason: statusReason({
                status: reason.response.status,
                body: Option.getOrUndefined(
                  Schema.decodeUnknownOption(Schema.fromJsonString(GoogleErrorBody))(text),
                ),
                http: {
                  response: {
                    status: reason.response.status,
                    headers: { ...reason.response.headers },
                  },
                  body: text,
                },
                request: reason.request,
              }),
            }),
          ),
      );
    case "DecodeError":
    case "EmptyBodyError":
      return Effect.fail(
        AiError.make({
          module: "EDAGoogleClient",
          method,
          reason: new AiError.InvalidOutputError({
            description: reason.description ?? "Failed to read the Gemini response body",
          }),
        }),
      );
  }
};

/** HTTP status to `AiError` reason, as the Anthropic adapter maps it. */
const statusReason = (params: {
  readonly status: number;
  readonly body: GoogleErrorBody | undefined;
  readonly http: typeof AiError.HttpContext.Type | undefined;
  readonly request: HttpClientRequest.HttpClientRequest;
}): AiError.AiErrorReason => {
  const { status, http } = params;
  const message = params.body?.error.message;
  const metadata = { google: { status: params.body?.error.status ?? null } };
  const description = AiError.buildErrorDescription({
    status,
    message,
    method: params.request.method,
    url: params.request.url,
    errorType: params.body?.error.status,
    body: http?.body,
  });
  const context = { metadata, ...(http === undefined ? {} : { http }) };
  switch (status) {
    case 400:
    case 404:
    case 422:
      return new AiError.InvalidRequestError({ description, ...context });
    case 401:
      return new AiError.AuthenticationError({ kind: "InvalidKey", description, ...context });
    case 403:
      return new AiError.AuthenticationError({
        kind: "InsufficientPermissions",
        description,
        ...context,
      });
    case 429:
      return new AiError.RateLimitError(context);
    default:
      return status >= 500
        ? new AiError.InternalProviderError({ description: message ?? "Server error", ...context })
        : new AiError.UnknownError({ description: message, ...context });
  }
};

const invalidInput = (method: string, description: string): AiError.AiError =>
  AiError.make({
    module: "EDAGoogleLanguageModel",
    method,
    reason: new AiError.InvalidUserInputError({ description }),
  });

// =============================================================================
// Language model
// =============================================================================

/** Gemini `LanguageModel` for one model id; requires {@link EDAGoogleClient}. */
export const makeEDAGoogleLanguageModel = (options: {
  readonly modelId: string;
  readonly config?: EDAGoogleModelConfig | undefined;
}): Effect.Effect<LanguageModel.LanguageModel, never, EDAGoogleClient> =>
  Effect.gen(function* () {
    const client = yield* EDAGoogleClient;
    const config = options.config ?? {};
    const request = (provider: LanguageModel.ProviderOptions) =>
      makeRequest(provider, config).pipe(
        Effect.tap((body) => Effect.sync(() => annotateRequest(provider, options.modelId, body))),
      );
    return yield* LanguageModel.make({
      generateText: (provider) =>
        Effect.gen(function* () {
          const body = yield* request(provider);
          const response = yield* client.generateContent(options.modelId, body);
          const converter = yield* makeConverter;
          const parts = collapseStreamParts([
            ...(yield* converter.chunk(response)),
            ...converter.end(),
          ]);
          annotateResponse(provider, parts);
          return parts;
        }),
      streamText: (provider) =>
        request(provider).pipe(
          Effect.zip(makeConverter),
          Effect.map(([body, converter]) =>
            client.streamGenerateContent(options.modelId, body).pipe(
              Stream.mapEffect(converter.chunk),
              Stream.flattenIterable,
              Stream.concat(Stream.suspend(() => Stream.fromIterable(converter.end()))),
              Stream.tap((part) => Effect.sync(() => annotateResponse(provider, [part]))),
            ),
          ),
          Stream.unwrap,
        ),
      supportsSystemMessagesInHistory: Effect.succeed(false),
    });
  });

// =============================================================================
// Request
// =============================================================================

const makeRequest = (
  provider: LanguageModel.ProviderOptions,
  config: EDAGoogleModelConfig,
): Effect.Effect<GoogleGenerateContentRequest, AiError.AiError> =>
  Effect.gen(function* () {
    const tools = yield* prepareTools(provider, config);
    const { systemInstruction, contents } = yield* prepareContents(provider.prompt);
    const format = provider.responseFormat;
    return {
      ...tools,
      ...(systemInstruction === undefined ? {} : { systemInstruction }),
      contents,
      generationConfig: {
        ...config.generationConfig,
        ...(format.type === "json"
          ? {
              responseMimeType: "application/json",
              responseJsonSchema: Tool.getJsonSchemaFromSchema(format.schema),
            }
          : {}),
      },
    } satisfies GoogleGenerateContentRequest;
  });

/**
 * Leading system messages become `systemInstruction`; the rest become `contents`, with assistant
 * messages as `model` turns and tool results as `functionResponse` parts in `user` turns. The
 * results of one step's calls share one turn, as Gemini expects for parallel calls. Every other
 * message keeps its own turn, so a request's contents always start with the previous request's
 * contents unchanged, which is the prefix Gemini's implicit cache matches.
 */
const prepareContents = (
  prompt: Prompt.Prompt,
): Effect.Effect<
  {
    readonly systemInstruction: GoogleGenerateContentRequest["systemInstruction"];
    readonly contents: ReadonlyArray<GoogleContent>;
  },
  AiError.AiError
> =>
  Effect.gen(function* () {
    const system: Array<{ readonly text: string }> = [];
    const contents: Array<GoogleContent> = [];
    let results: Array<GoogleRequestPart> | undefined;
    for (const message of prompt.content) {
      if (message.role !== "tool") results = undefined;
      switch (message.role) {
        case "system":
          if (contents.length > 0) {
            return yield* invalidInput(
              "prepareContents",
              "Gemini takes system messages only before the conversation.",
            );
          }
          system.push({ text: message.content });
          break;
        case "user":
          contents.push({ role: "user", parts: message.content.map(userPart) });
          break;
        case "assistant": {
          const parts = message.content.flatMap(modelParts);
          if (parts.length > 0) contents.push({ role: "model", parts });
          break;
        }
        case "tool":
          if (results === undefined) {
            results = [];
            contents.push({ role: "user", parts: results });
          }
          results.push(...message.content.flatMap(toolResultParts));
          break;
      }
    }
    return {
      systemInstruction: system.length === 0 ? undefined : { parts: system },
      contents,
    };
  });

const userPart = (part: Prompt.UserMessagePart): GoogleRequestPart =>
  part.type === "text" ? { text: part.text } : filePart(part);

/**
 * A file part as Gemini data. EDA stores file data as a string: bytes as a base64 `data:` URL and a
 * URL as its href. Bytes, base64 and a base64 `data:` URL go inline, the last with its payload and
 * its own media type. A URL, as a `URL` or a string with a scheme (an https or Gemini Files URI),
 * goes as `fileData.fileUri`. Base64 has no `:`, so a scheme tells a URL from base64.
 */
const filePart = (part: Prompt.FilePart): GoogleRequestPart => {
  const mimeType = part.mediaType === "image/*" ? "image/jpeg" : part.mediaType;
  if (part.data instanceof Uint8Array) {
    return { inlineData: { mimeType, data: Base64.encode(part.data) } };
  }
  const value = part.data instanceof URL ? part.data.href : part.data;
  const dataUrl = BASE64_DATA_URL.exec(value);
  if (dataUrl !== null) {
    const [, ownType = "", data = ""] = dataUrl;
    return { inlineData: { mimeType: ownType === "" ? mimeType : ownType, data } };
  }
  return URL_SCHEME.test(value)
    ? { fileData: { mimeType, fileUri: value } }
    : { inlineData: { mimeType, data: value } };
};

const BASE64_DATA_URL = /^data:([^;,]*)(?:;[^;,]*)*;base64,(.*)$/is;
const URL_SCHEME = /^[a-z][a-z\d+.-]*:/i;

/** An assistant part as the `model` part it came from, with its signature back on it. */
const modelParts = (part: Prompt.AssistantMessagePart): ReadonlyArray<GoogleRequestPart> => {
  const { thoughtSignature, thought } = googleOptions(part.options);
  const signed = thoughtSignature === undefined ? {} : { thoughtSignature };
  switch (part.type) {
    case "text":
      return part.text.length === 0 && thoughtSignature === undefined
        ? []
        : [{ text: part.text, ...signed }];
    case "reasoning":
      return part.text.length === 0 && thoughtSignature === undefined
        ? []
        : [{ text: part.text, ...(thought === false ? {} : { thought: true }), ...signed }];
    case "tool-call":
      return [{ functionCall: { name: part.name, args: part.params, id: part.id }, ...signed }];
    case "file":
      return [filePart(part)];
    case "tool-result":
    case "tool-approval-request":
      // Provider-executed results and approvals have no Gemini request form; EDA stores neither.
      return [];
  }
};

const toolResultParts = (part: Prompt.ToolMessagePart): ReadonlyArray<GoogleRequestPart> => {
  if (part.type !== "tool-result") return [];
  const output =
    part.modelOutput === undefined ? part.result : Prompt.toolModelOutputToText(part.modelOutput);
  return [
    {
      functionResponse: {
        name: part.name,
        id: part.id,
        // Gemini reads `output` as the function's result and `error` as its failure.
        response: part.isFailure ? { error: output } : { output },
      },
    },
  ];
};

/** The part's Gemini signature, and `thought: false` on reasoning that was an empty text part. */
const googleOptions = (
  options: Prompt.ProviderOptions,
): { readonly thoughtSignature?: string; readonly thought?: false } => {
  const google = options.google;
  if (!isRecord(google)) return {};
  return {
    ...(typeof google.thoughtSignature === "string"
      ? { thoughtSignature: google.thoughtSignature }
      : {}),
    ...(google.thought === false ? { thought: false } : {}),
  };
};

const prepareTools = (
  provider: LanguageModel.ProviderOptions,
  config: EDAGoogleModelConfig,
): Effect.Effect<Pick<GoogleGenerateContentRequest, "tools" | "toolConfig">, AiError.AiError> =>
  Effect.gen(function* () {
    if (provider.tools.length === 0) return {};
    const functionDeclarations: Array<GoogleFunctionDeclaration> = [];
    for (const tool of provider.tools) {
      if (Tool.isProviderDefined(tool)) {
        return yield* invalidInput(
          "prepareTools",
          `Provider-defined tool ${tool.name} is not supported on Gemini.`,
        );
      }
      const parametersJsonSchema = yield* Effect.try({
        try: () => Tool.getJsonSchema(tool),
        catch: (error) =>
          AiError.make({
            module: "EDAGoogleLanguageModel",
            method: "prepareTools",
            reason: new AiError.UnsupportedSchemaError({
              description: `Tool ${tool.name} parameters: ${String(error)}`,
            }),
          }),
      });
      const description = Tool.getDescription(tool);
      functionDeclarations.push({
        name: tool.name,
        ...(description === undefined ? {} : { description }),
        parametersJsonSchema,
      });
    }
    return {
      tools: [{ functionDeclarations }],
      toolConfig: { functionCallingConfig: functionCalling(provider.toolChoice, config) },
    };
  });

const functionCalling = (
  choice: LanguageModel.ToolChoice<string>,
  config: EDAGoogleModelConfig,
): NonNullable<GoogleToolConfig["functionCallingConfig"]> => {
  const auto = config.toolConfig?.functionCallingConfig?.mode ?? "AUTO";
  if (choice === "auto") return { mode: auto };
  if (choice === "none") return { mode: "NONE" };
  if (choice === "required") return { mode: "ANY" };
  if ("tool" in choice) return { mode: "ANY", allowedFunctionNames: [choice.tool] };
  // `oneOf` already narrowed the declared tools.
  return choice.mode === "required"
    ? { mode: "ANY", allowedFunctionNames: choice.oneOf }
    : { mode: auto };
};

// =============================================================================
// Response
// =============================================================================

/** The stream parts a Gemini response becomes. */
type StreamPart = Extract<
  Response.StreamPartEncoded,
  {
    readonly type:
      | "response-metadata"
      | "text-start"
      | "text-delta"
      | "text-end"
      | "reasoning-start"
      | "reasoning-delta"
      | "reasoning-end"
      | "tool-params-start"
      | "tool-params-delta"
      | "tool-params-end"
      | "tool-call"
      | "finish";
  }
>;

type BlockKind = "text" | "reasoning";

const blockStart = (
  kind: BlockKind,
  id: string,
  metadata: Response.ProviderMetadata = {},
): StreamPart =>
  kind === "text"
    ? { type: "text-start", id, metadata }
    : { type: "reasoning-start", id, metadata };

const blockDelta = (kind: BlockKind, id: string, delta: string): StreamPart =>
  kind === "text" ? { type: "text-delta", id, delta } : { type: "reasoning-delta", id, delta };

const blockEnd = (kind: BlockKind, id: string): StreamPart =>
  kind === "text" ? { type: "text-end", id } : { type: "reasoning-end", id };

/**
 * Converts the chunks of one response into stream parts that replay as the parts Gemini sent.
 * Unsigned text parts of one kind (thought or answer) in a row are streamed pieces of one part and
 * form one block. A signed part is a block of its own whose start carries the signature, so the
 * signature replays on exactly that part; a signed empty answer part is an empty reasoning block
 * marked `thought: false` (see the module comment). A function call closes the open block, which
 * keeps replayed parts in the order Gemini sent them. `finish` is emitted when the response ends,
 * with the last (cumulative) usage, and only if Gemini sent a finish reason.
 */
const makeConverter = Effect.gen(function* () {
  const ids = yield* IdGenerator.IdGenerator;
  let started = false;
  let open: { readonly kind: BlockKind; readonly id: string } | undefined;
  let blocks = 0;
  let toolCalls = false;
  let finishReason: string | undefined;
  let usage: GoogleUsageMetadata | undefined;

  const close = (out: Array<StreamPart>) => {
    if (open === undefined) return;
    out.push(blockEnd(open.kind, open.id));
    open = undefined;
  };

  const chunk = (response: GoogleGenerateContentResponse) =>
    Effect.gen(function* () {
      const blocked = response.promptFeedback?.blockReason;
      if (blocked !== undefined) {
        const message = response.promptFeedback?.blockReasonMessage;
        return yield* AiError.make({
          module: "EDAGoogleLanguageModel",
          method: "convertResponse",
          reason: new AiError.ContentPolicyError({
            description: `Gemini blocked the prompt (${blocked})${message === undefined ? "" : `: ${message}`}`,
            metadata: { google: { blockReason: blocked } },
          }),
        });
      }
      const out: Array<StreamPart> = [];
      if (!started) {
        started = true;
        out.push({
          type: "response-metadata",
          id: response.responseId,
          modelId: response.modelVersion,
          timestamp: DateTime.formatIso(yield* DateTime.now),
        });
      }
      usage = response.usageMetadata ?? usage;
      const candidate = response.candidates?.[0];
      const failure = finishFailure(candidate);
      if (failure !== undefined) return yield* failure;
      for (const part of candidate?.content?.parts ?? []) {
        const metadata =
          part.thoughtSignature === undefined
            ? {}
            : { metadata: { google: { thoughtSignature: part.thoughtSignature } } };
        if (part.functionCall !== undefined) {
          close(out);
          toolCalls = true;
          const id = part.functionCall.id ?? (yield* ids.generateId());
          const { name } = part.functionCall;
          const params = part.functionCall.args ?? {};
          out.push(
            { type: "tool-params-start", id, name },
            { type: "tool-params-delta", id, delta: JSON.stringify(params) },
            { type: "tool-params-end", id },
            { type: "tool-call", id, name, params, ...metadata },
          );
          continue;
        }
        if (
          part.text === undefined ||
          (part.text.length === 0 && part.thoughtSignature === undefined)
        ) {
          continue;
        }
        const kind = part.thought === true ? "reasoning" : "text";
        if (part.thoughtSignature === undefined) {
          if (open?.kind !== kind) {
            close(out);
            open = { kind, id: String(blocks++) };
            out.push(blockStart(kind, open.id));
          }
          out.push(blockDelta(kind, open.id, part.text));
          continue;
        }
        close(out);
        const id = String(blocks++);
        const emptyAnswer = kind === "text" && part.text.length === 0;
        const block = emptyAnswer ? "reasoning" : kind;
        const google = {
          thoughtSignature: part.thoughtSignature,
          ...(emptyAnswer ? { thought: false } : {}),
        };
        out.push(blockStart(block, id, { google }));
        if (part.text.length > 0) out.push(blockDelta(block, id, part.text));
        out.push(blockEnd(block, id));
      }
      finishReason = candidate?.finishReason ?? finishReason;
      return out;
    });

  const end = (): ReadonlyArray<StreamPart> => {
    if (finishReason === undefined) return [];
    const out: Array<StreamPart> = [];
    close(out);
    out.push({
      type: "finish",
      reason: resolveFinishReason(finishReason, toolCalls),
      usage: makeUsage(usage),
      metadata: { google: { finishReason, usageMetadata: usage ?? null } },
    });
    return out;
  };

  return { chunk, end };
});

/**
 * Gemini usage as Effect AI usage. Input is the prompt plus tool-use prompt tokens, of which
 * `cachedContentTokenCount` came from the implicit cache. Gemini charges no cache writes. Output
 * is the answer (`candidatesTokenCount`) plus thinking (`thoughtsTokenCount`), both billed as
 * output.
 */
const makeUsage = (usage: GoogleUsageMetadata | undefined): Response.FinishPartEncoded["usage"] => {
  if (usage === undefined) {
    return {
      inputTokens: {
        uncached: undefined,
        total: undefined,
        cacheRead: undefined,
        cacheWrite: undefined,
      },
      outputTokens: { total: undefined, text: undefined, reasoning: undefined },
    };
  }
  const input = (usage.promptTokenCount ?? 0) + (usage.toolUsePromptTokenCount ?? 0);
  const cached = usage.cachedContentTokenCount ?? 0;
  const text = usage.candidatesTokenCount ?? 0;
  const reasoning = usage.thoughtsTokenCount ?? 0;
  return {
    inputTokens: { total: input, uncached: input - cached, cacheRead: cached, cacheWrite: 0 },
    outputTokens: { total: text + reasoning, text, reasoning },
  };
};

/**
 * Gemini finish reasons that end a usable response, as the Anthropic and OpenAI adapters end
 * theirs: output limits are `length`, and safety and recitation filters are `content-filter`, as
 * Anthropic's `refusal` and OpenAI's `content_filter` are. A reason in neither table is `unknown`.
 */
const STOP_REASONS: Readonly<Record<string, Response.FinishReason>> = {
  STOP: "stop",
  MAX_TOKENS: "length",
  CONTINUATION: "length",
  SAFETY: "content-filter",
  RECITATION: "content-filter",
  LANGUAGE: "content-filter",
  BLOCKLIST: "content-filter",
  PROHIBITED_CONTENT: "content-filter",
  SPII: "content-filter",
  IMAGE_SAFETY: "content-filter",
  IMAGE_PROHIBITED_CONTENT: "content-filter",
  IMAGE_RECITATION: "content-filter",
  ESCALATION: "content-filter",
  OTHER: "other",
  IMAGE_OTHER: "other",
  NO_IMAGE: "other",
  FINISH_REASON_UNSPECIFIED: "other",
};

type FailureContext = {
  readonly description: string;
  readonly metadata: { readonly google: Readonly<Record<string, string>> };
};

/**
 * Gemini finish reasons that say the response or the request is broken. Each fails the inference
 * with an `AiError`, as OpenAI's `response.failed` does, so EDA commits no tool call from it.
 */
const FAILURE_REASONS: Readonly<
  Record<string, (context: FailureContext) => AiError.AiErrorReason>
> = {
  MALFORMED_FUNCTION_CALL: (context) => new AiError.InvalidOutputError(context),
  UNEXPECTED_TOOL_CALL: (context) => new AiError.InvalidOutputError(context),
  TOO_MANY_TOOL_CALLS: (context) => new AiError.InvalidOutputError(context),
  MALFORMED_RESPONSE: (context) => new AiError.InvalidOutputError(context),
  // The request left out a signature, which Gemini otherwise rejects with a 400.
  MISSING_THOUGHT_SIGNATURE: (context) => new AiError.InvalidRequestError(context),
  // The account is limited for Prohibited Use Policy violations; every request will stop.
  PUP_LIMITED_DISABLED: (context) => new AiError.ContentPolicyError(context),
};

/** The failure a candidate's finish reason is, keeping Gemini's `finishMessage`, if any. */
const finishFailure = (
  candidate: NonNullable<GoogleGenerateContentResponse["candidates"]>[number] | undefined,
): AiError.AiError | undefined => {
  const finishReason = candidate?.finishReason;
  const failure =
    finishReason !== undefined && Object.hasOwn(FAILURE_REASONS, finishReason)
      ? FAILURE_REASONS[finishReason]
      : undefined;
  if (finishReason === undefined || failure === undefined) return undefined;
  const finishMessage = candidate?.finishMessage;
  return AiError.make({
    module: "EDAGoogleLanguageModel",
    method: "convertResponse",
    reason: failure({
      description: `Gemini finished with ${finishReason}${finishMessage === undefined ? "" : `: ${finishMessage}`}`,
      metadata: {
        google: { finishReason, ...(finishMessage === undefined ? {} : { finishMessage }) },
      },
    }),
  });
};

/** Gemini finishes a function-call response with `STOP`; that is a `tool-calls` finish. */
const resolveFinishReason = (reason: string, toolCalls: boolean): Response.FinishReason => {
  if (reason === "STOP" && toolCalls) return "tool-calls";
  return (Object.hasOwn(STOP_REASONS, reason) ? STOP_REASONS[reason] : undefined) ?? "unknown";
};

/** Folds stream parts into the whole parts `generateText` returns. */
const collapseStreamParts = (parts: ReadonlyArray<StreamPart>): Array<Response.PartEncoded> => {
  const out: Array<Response.PartEncoded> = [];
  const blocks = new Map<string, { text: string; metadata: Response.ProviderMetadata }>();
  for (const part of parts) {
    switch (part.type) {
      case "text-start":
      case "reasoning-start":
        blocks.set(part.id, { text: "", metadata: part.metadata ?? {} });
        break;
      case "text-delta":
      case "reasoning-delta": {
        const block = blocks.get(part.id);
        if (block === undefined) break;
        block.text += part.delta;
        break;
      }
      case "text-end":
      case "reasoning-end": {
        const block = blocks.get(part.id);
        // A signed block is kept even when empty: its signature must be replayed.
        if (
          block === undefined ||
          (block.text.length === 0 && block.metadata.google === undefined)
        ) {
          break;
        }
        out.push({
          type: part.type === "text-end" ? "text" : "reasoning",
          text: block.text,
          metadata: block.metadata,
        });
        break;
      }
      case "tool-params-start":
      case "tool-params-delta":
      case "tool-params-end":
        break;
      case "response-metadata":
      case "tool-call":
      case "finish":
        out.push(part);
        break;
    }
  }
  return out;
};

// =============================================================================
// Telemetry
// =============================================================================

const annotateRequest = (
  provider: LanguageModel.ProviderOptions,
  model: string,
  request: GoogleGenerateContentRequest,
) =>
  addGenAIAnnotations(provider.span, {
    provider: { name: "gcp.gemini" },
    operation: { name: "chat" },
    request: { model, maxTokens: request.generationConfig?.maxOutputTokens },
  });

const annotateResponse = (
  provider: LanguageModel.ProviderOptions,
  parts: ReadonlyArray<StreamPart | Response.PartEncoded>,
) => {
  for (const part of parts) {
    if (part.type === "response-metadata") {
      addGenAIAnnotations(provider.span, { response: { id: part.id, model: part.modelId } });
    }
    if (part.type === "finish") {
      addGenAIAnnotations(provider.span, {
        response: { finishReasons: [part.reason] },
        usage: {
          inputTokens: part.usage.inputTokens.total,
          outputTokens: part.usage.outputTokens.total,
        },
      });
    }
  }
};

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const optionalNonEmptyString = (value: string | undefined): string | undefined => {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
};
