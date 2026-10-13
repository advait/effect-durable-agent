import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientError from "effect/http/HttpClientError";
import type * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import type * as Schema from "effect/Schema";
import * as Prompt from "effect/ai/Prompt";
import { getNativeCompaction } from "effect-durable-agent/services/prompt-projector";

/** Providers whose HTTP traffic EDA's transport understands. */
export type EDAProvider = "openai" | "anthropic";

/** Request about to leave EDA's transport, as decoded JSON. */
export interface EDAProviderRequest {
  readonly url: URL;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: unknown;
}

/**
 * One-shot rewrite an executor installs around a single model call, for provider endpoints the
 * Effect AI adapters do not model (OpenAI `/responses/compact`, Anthropic on-demand compaction).
 * The adapter still builds the request from the EDA prompt; the override rewrites it and receives
 * the raw provider response. The adapter's own handling of that response is then ignored.
 */
export interface EDAProviderRequestOverride {
  readonly rewrite: (request: EDAProviderRequest) => EDAProviderRequest;
  readonly onResponse: (status: number, body: unknown) => void;
}

/** Override in effect for the current model call, if any. */
export const EDAProviderRequestOverride = Context.Reference<EDAProviderRequestOverride | undefined>(
  "effect-durable-agent-cloudflare/EDAProviderRequestOverride",
  { defaultValue: () => undefined },
);

const NATIVE_COMPACTION_SENTINEL = "⁣eda-native-compaction:v1:";

/**
 * Replace stored native compaction messages for `provider` with a transport sentinel the provider
 * transport swaps for the native items. Other providers' native items stay as readable summaries.
 */
export const prepareNativeCompaction = (
  provider: EDAProvider,
  prompt: Prompt.Prompt,
): Prompt.Prompt =>
  Prompt.fromMessages(
    prompt.content.map((message) => {
      const native = getNativeCompaction(message);
      if (native === undefined || native.provider !== provider || message.role !== "user") {
        return message;
      }
      return Prompt.makeMessage("user", {
        content: [Prompt.textPart({ text: encodeSentinel(native.items) })],
        options: message.options,
      });
    }),
  );

const encodeSentinel = (items: ReadonlyArray<Schema.Json>): string =>
  `${NATIVE_COMPACTION_SENTINEL}${bytesToBase64(new TextEncoder().encode(JSON.stringify(items)))}`;

const decodeSentinel = (text: string): ReadonlyArray<unknown> | undefined => {
  if (!text.startsWith(NATIVE_COMPACTION_SENTINEL)) return undefined;
  const parsed: unknown = JSON.parse(
    new TextDecoder().decode(base64ToBytes(text.slice(NATIVE_COMPACTION_SENTINEL.length))),
  );
  return Array.isArray(parsed) ? parsed : undefined;
};

const bytesToBase64 = (bytes: Uint8Array): string => {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
};

const base64ToBytes = (value: string): Uint8Array =>
  Uint8Array.from(atob(value), (char) => char.charCodeAt(0));

/** Anthropic beta that lets requests carry on-demand compaction blocks. */
export const ANTHROPIC_COMPACTION_BETA = "compact-2026-09-04";

/** Rewrite provider request bodies that carry native compaction sentinels. */
export const rehydrateNativeCompaction = (
  provider: EDAProvider,
  request: EDAProviderRequest,
): EDAProviderRequest => {
  if (!isRecord(request.body)) return request;
  if (provider === "anthropic") {
    const messages = request.body.messages;
    if (!Array.isArray(messages)) return request;
    let found = false;
    const rewritten = messages.map((message: unknown) => {
      if (!isRecord(message) || !Array.isArray(message.content)) return message;
      const content = message.content.flatMap((block: unknown) => {
        if (!isRecord(block) || block.type !== "text" || typeof block.text !== "string") {
          return [block];
        }
        const items = decodeSentinel(block.text);
        if (items === undefined) return [block];
        found = true;
        // A breakpoint on the summary moves onto the block ("a breakpoint after the summary").
        return block.cache_control === undefined
          ? items
          : items.map((item, index) =>
              index === items.length - 1 && isRecord(item)
                ? { ...item, cache_control: block.cache_control }
                : item,
            );
      });
      // The block stands as an assistant message of its own when nothing else shares the message.
      const role =
        content.length > 0 &&
        content.every((block) => isRecord(block) && block.type === "compaction")
          ? "assistant"
          : message.role;
      return { ...message, role, content };
    });
    if (!found) return request;
    return {
      ...request,
      headers: withBeta(request.headers, ANTHROPIC_COMPACTION_BETA),
      body: { ...request.body, messages: rewritten },
    };
  }
  const input = request.body.input;
  if (!Array.isArray(input)) return request;
  let found = false;
  const rewritten = input.flatMap((item: unknown) => {
    const items = sentinelItems(item);
    if (items === undefined) return [item];
    found = true;
    return items;
  });
  return found ? { ...request, body: { ...request.body, input: rewritten } } : request;
};

const sentinelItems = (item: unknown): ReadonlyArray<unknown> | undefined => {
  if (!isRecord(item) || item.role !== "user") return undefined;
  const content = item.content;
  if (typeof content === "string") return decodeSentinel(content);
  if (!Array.isArray(content) || content.length !== 1) return undefined;
  const [part] = content;
  return isRecord(part) && typeof part.text === "string" ? decodeSentinel(part.text) : undefined;
};

/** Add a beta flag to an `anthropic-beta` header without duplicating existing flags. */
export const withBeta = (
  headers: Readonly<Record<string, string>>,
  beta: string,
): Readonly<Record<string, string>> => {
  const key = Object.keys(headers).find((name) => name.toLowerCase() === "anthropic-beta");
  const existing =
    key === undefined ? [] : (headers[key] ?? "").split(",").map((value) => value.trim());
  const flags = [...new Set([...existing.filter((value) => value.length > 0), beta])];
  const rest = Object.fromEntries(
    Object.entries(headers).filter(([name]) => name.toLowerCase() !== "anthropic-beta"),
  );
  return { ...rest, "anthropic-beta": flags.join(",") };
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Transport options shared by EDA's provider clients. */
export interface EDAProviderTransportOptions {
  readonly provider: EDAProvider;
  /** Cloudflare AI Gateway binding; requests go direct over `fetch` when absent. */
  readonly aiGateway?: AiGateway | undefined;
  /** Anthropic beta flags added to every Anthropic request. */
  readonly anthropicBetas?: ReadonlyArray<string> | undefined;
}

/**
 * HttpClient for provider SDK clients. It replays native compaction items, applies the current
 * request override, and sends through the Cloudflare AI Gateway binding or `fetch`.
 */
export const makeEDAProviderHttpClient = (
  options: EDAProviderTransportOptions,
): HttpClient.HttpClient =>
  HttpClient.make((request, url, signal) =>
    Effect.gen(function* () {
      const override = yield* EDAProviderRequestOverride;
      const prepared = yield* Effect.try({
        try: () => {
          const headers =
            options.provider === "anthropic"
              ? (options.anthropicBetas ?? []).reduce(withBeta, stringHeaders(request))
              : stringHeaders(request);
          const base = rehydrateNativeCompaction(options.provider, {
            url,
            headers,
            body: requestBody(request.body),
          });
          return override === undefined ? base : override.rewrite(base);
        },
        catch: (cause) => transportError(request, cause, "EDA provider request preparation failed"),
      });
      const response = yield* Effect.tryPromise({
        try: () => send(options, request, prepared, signal),
        catch: (cause) =>
          transportError(
            request,
            cause,
            options.aiGateway === undefined
              ? "Provider request failed"
              : "Cloudflare AI Gateway request failed",
          ),
      });
      if (override === undefined) {
        return HttpClientResponse.fromWeb(request, response);
      }
      // Overridden calls hand the raw body to the executor; the adapter then sees the same bytes.
      const text = yield* Effect.tryPromise({
        try: () => response.text(),
        catch: (cause) => transportError(request, cause, "Provider response read failed"),
      });
      override.onResponse(response.status, parseJson(text));
      return HttpClientResponse.fromWeb(
        request,
        new Response(text, { status: response.status, headers: response.headers }),
      );
    }),
  );

/**
 * Connection failures before any response are retried like the provider SDKs do (two retries with
 * backoff); a request that reached the provider and failed is returned as its HTTP response.
 */
const send = async (
  options: EDAProviderTransportOptions,
  request: HttpClientRequest.HttpClientRequest,
  prepared: EDAProviderRequest,
  signal: AbortSignal,
): Promise<Response> => {
  for (let attempt = 1; ; attempt++) {
    try {
      return await sendOnce(options, request, prepared, signal);
    } catch (error) {
      if (attempt >= 3 || signal.aborted) throw error;
      await new Promise((resolve) => setTimeout(resolve, 500 * 2 ** (attempt - 1)));
    }
  }
};

const sendOnce = async (
  options: EDAProviderTransportOptions,
  request: HttpClientRequest.HttpClientRequest,
  prepared: EDAProviderRequest,
  signal: AbortSignal,
): Promise<Response> => {
  const body = prepared.body === undefined ? undefined : JSON.stringify(prepared.body);
  if (options.aiGateway === undefined) {
    return fetch(prepared.url, {
      method: request.method,
      headers: prepared.headers,
      ...(body === undefined ? {} : { body }),
      signal,
    });
  }
  return options.aiGateway.run(
    {
      endpoint: gatewayEndpoint(options.provider, prepared.url),
      headers: prepared.headers,
      provider: options.provider,
      query: prepared.body,
    },
    { signal },
  );
};

/** AI Gateway endpoint for a provider URL: OpenAI paths drop `/v1/`, Anthropic keeps it. */
export const gatewayEndpoint = (provider: EDAProvider, url: URL): string => {
  const path =
    provider === "openai" && url.pathname.startsWith("/v1/")
      ? url.pathname.slice("/v1/".length)
      : url.pathname.replace(/^\/+/, "");
  return `${path}${url.search}`;
};

/** Headers that describe the original body; the transport may rewrite it, so fetch recomputes them. */
const BODY_FRAMING_HEADERS = new Set(["host", "content-length", "transfer-encoding"]);

const stringHeaders = (request: HttpClientRequest.HttpClientRequest): Record<string, string> =>
  Object.fromEntries(
    Object.entries(request.headers).filter(
      (entry): entry is [string, string] =>
        typeof entry[1] === "string" && !BODY_FRAMING_HEADERS.has(entry[0].toLowerCase()),
    ),
  );

const requestBody = (body: HttpBody.HttpBody): unknown => {
  switch (body._tag) {
    case "Empty":
      return undefined;
    case "Raw":
      return typeof body.body === "string" ? parseJson(body.body) : body.body;
    case "Uint8Array":
      return parseJson(new TextDecoder().decode(body.body));
    default:
      throw new Error(`Unsupported provider request body: ${body._tag}`);
  }
};

const parseJson = (text: string): unknown => {
  try {
    const parsed: unknown = JSON.parse(text);
    return parsed;
  } catch {
    return text;
  }
};

const transportError = (
  request: HttpClientRequest.HttpClientRequest,
  cause: unknown,
  description: string,
) =>
  new HttpClientError.HttpClientError({
    reason: new HttpClientError.TransportError({ request, cause, description }),
  });
