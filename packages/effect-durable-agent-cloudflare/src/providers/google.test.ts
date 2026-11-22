import { readFile } from "node:fs/promises";

import { ModelResolver } from "effect-durable-agent/services/model-resolver";
import { EDARuntime } from "effect-durable-agent/services/runtime";
import { makeEDAToolkit } from "effect-durable-agent/services/tool-registry";
import { makeEdaTestLayer } from "effect-durable-agent/testkit/layers";
import { SubmitMessageCommand } from "effect-durable-agent/types/commands";
import { CommandId, SequenceNumber, SessionId } from "effect-durable-agent/types/core";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as AiError from "effect/ai/AiError";
import * as Prompt from "effect/ai/Prompt";
import * as Tool from "effect/ai/Tool";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { googleAgentModelConfig, type EDAGoogleAgentSettings } from "./google";
import { makeEDAMultiProviderModelResolverLayer } from "./providers";

/** Real `streamGenerateContent` responses from gemini-4-hydrogen (2026-11-22). */
const fixture = (name: string) => readFile(new URL(`./fixtures/${name}`, import.meta.url), "utf8");

interface FixturePart {
  readonly text?: string;
  readonly thought?: boolean;
  readonly thoughtSignature?: string;
}

/** Every part of a recorded stream, in order. */
const fixtureParts = (stream: string): ReadonlyArray<FixturePart> =>
  stream
    .split("\n")
    .filter((line) => line.startsWith("data: "))
    .flatMap((line) => {
      const chunk: {
        readonly candidates: ReadonlyArray<{
          readonly content: { readonly parts: ReadonlyArray<FixturePart> };
        }>;
      } = JSON.parse(line.slice("data: ".length));
      return chunk.candidates[0]?.content.parts ?? [];
    });

const firstThought = (stream: string) =>
  fixtureParts(stream).find((part) => part.thought === true)?.text;

interface Recorded {
  readonly url: string;
  readonly headers: Record<string, string>;
  readonly body: Record<string, unknown>;
}

interface Reply {
  readonly status?: number;
  readonly body: string;
}

const stubFetch = (replies: ReadonlyArray<Reply>) => {
  const recorded: Array<Recorded> = [];
  vi.stubGlobal("fetch", async (input: URL | string, init: RequestInit) => {
    recorded.push({
      url: String(input),
      headers: Object.fromEntries(new Headers(init.headers).entries()),
      body: JSON.parse(String(init.body)),
    });
    const reply = replies[recorded.length - 1];
    if (reply === undefined) throw new Error(`unexpected request ${recorded.length}`);
    return new Response(reply.body, {
      status: reply.status ?? 200,
      headers: { "content-type": "text/event-stream" },
    });
  });
  return recorded;
};

const sse = (...chunks: ReadonlyArray<unknown>) =>
  chunks.map((chunk) => `data: ${JSON.stringify(chunk)}\r\n\r\n`).join("");

const resolverLayer = (settings: EDAGoogleAgentSettings = { effort: "high" }) =>
  makeEDAMultiProviderModelResolverLayer({
    google: { apiKey: "test-key" },
    resolve: (selection) => ({
      provider: "google",
      modelId: selection?.modelId ?? "gemini-4-hydrogen",
      config: googleAgentModelConfig(settings),
    }),
  });

const GetWeather = Tool.make("get_weather", {
  description: "Current weather for a city.",
  parameters: Schema.Struct({ city: Schema.String }),
  success: Schema.Struct({ city: Schema.String, tempC: Schema.Number }),
});

const toolkit = Effect.runSync(
  makeEDAToolkit([GetWeather], {
    get_weather: ({ city }) => Effect.succeed({ city, tempC: city === "Paris" ? 14 : 11 }),
  }),
);

/** Stream one prompt through the resolved Gemini model, as EDA's inference does. */
const streamParts = (prompt: Prompt.RawInput) =>
  Effect.gen(function* () {
    const resolver = yield* ModelResolver;
    const model = yield* resolver.resolve({ provider: "google", modelId: "gemini-4-neon" });
    return yield* model
      .streamText({ prompt, toolkit, disableToolCallResolution: true })
      .pipe(Stream.runCollect);
  }).pipe(Effect.provide(resolverLayer()), Effect.exit, Effect.runPromise);

const failureReason = (exit: Exit.Exit<unknown, unknown>) => {
  if (Exit.isSuccess(exit)) throw new Error("expected the stream to fail");
  const error = exit.cause.reasons.find((reason) => reason._tag === "Fail")?.error;
  if (!AiError.isAiError(error)) throw new Error(`expected an AiError, got ${String(error)}`);
  return error.reason;
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Gemini session through the EDA runtime", () => {
  it("replays thought signatures on the parts Gemini sent them on, and maps usage", async () => {
    const [toolCalls, answer, gatewayText] = await Promise.all([
      fixture("gemini-tool-calls.sse"),
      fixture("gemini-answer.sse"),
      fixture("gemini-gateway-text.sse"),
    ]);
    const recorded = stubFetch([{ body: toolCalls }, { body: answer }, { body: gatewayText }]);
    const layer = EDARuntime.Live({
      modelSelection: { provider: "google", modelId: "gemini-4-hydrogen" },
      systemPrompt: "You are terse. Use tools for weather.",
    }).pipe(
      Layer.provideMerge(
        makeEdaTestLayer({
          sessionId: SessionId.make(crypto.randomUUID()),
          modelResolverLayer: resolverLayer(),
          toolkit,
        }),
      ),
    );
    const events = await Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        for (const [index, text] of ["Plan a two-city picnic.", "Thanks."].entries()) {
          yield* runtime.submitAndBlock(
            new SubmitMessageCommand({
              commandId: CommandId.make(
                `018f6bd5-2f2a-7b1e-8f1b-${String(index + 1).padStart(12, "0")}`,
              ),
              disposition: "queue",
              content: [Prompt.textPart({ text })],
            }),
          );
        }
        const snapshot = yield* runtime.snapshot();
        const replay = yield* runtime.eventsAfter(SequenceNumber.make(0));
        return yield* replay.pipe(Stream.take(snapshot.state.lastSeq), Stream.runCollect);
      }),
    ).pipe(Effect.provide(layer), Effect.runPromise);

    const callSignature = fixtureParts(toolCalls).find(
      (part) => part.thoughtSignature !== undefined,
    )?.thoughtSignature;
    const answerParts = fixtureParts(answer);
    const answerSignature = answerParts.find(
      (part) => part.thoughtSignature !== undefined,
    )?.thoughtSignature;
    const answerText = answerParts
      .filter((part) => part.thought !== true)
      .map((part) => part.text ?? "")
      .join("");

    expect(recorded).toHaveLength(3);
    expect(recorded[0]?.url).toBe(
      "https://generativelanguage.googleapis.com/v1beta/models/gemini-4-hydrogen:streamGenerateContent?alt=sse",
    );
    expect(recorded[0]?.headers["x-goog-api-key"]).toBe("test-key");
    expect(recorded[0]?.body).toEqual({
      tools: [
        {
          functionDeclarations: [
            {
              name: "get_weather",
              description: "Current weather for a city.",
              parametersJsonSchema: {
                type: "object",
                properties: { city: { type: "string" } },
                required: ["city"],
                additionalProperties: true,
              },
            },
          ],
        },
      ],
      toolConfig: { functionCallingConfig: { mode: "AUTO" } },
      systemInstruction: { parts: [{ text: "You are terse. Use tools for weather." }] },
      contents: [{ role: "user", parts: [{ text: "Plan a two-city picnic." }] }],
      generationConfig: {
        maxOutputTokens: 65_536,
        thinkingConfig: { thinkingLevel: "high", includeThoughts: true },
      },
    });

    // The signed first call keeps its signature, the parallel second call has none, and both
    // results share the user turn after the calls.
    const firstTurn = [
      { role: "user", parts: [{ text: "Plan a two-city picnic." }] },
      {
        role: "model",
        parts: [
          { text: firstThought(toolCalls), thought: true },
          {
            functionCall: {
              name: "get_weather",
              args: { city: "Paris" },
              id: "call_12298579055924819135",
            },
            thoughtSignature: callSignature,
          },
          {
            functionCall: {
              name: "get_weather",
              args: { city: "London" },
              id: "call_12298579055924820790",
            },
          },
        ],
      },
      {
        role: "user",
        parts: [
          {
            functionResponse: {
              name: "get_weather",
              id: "call_12298579055924819135",
              response: { output: { city: "Paris", tempC: 14 } },
            },
          },
          {
            functionResponse: {
              name: "get_weather",
              id: "call_12298579055924820790",
              response: { output: { city: "London", tempC: 11 } },
            },
          },
        ],
      },
    ];
    expect(recorded[1]?.body.contents).toEqual(firstTurn);

    // The streamed answer replays as one text part carrying the signature that arrived on the
    // empty text part ending the stream.
    expect(recorded[2]?.body.contents).toEqual([
      ...firstTurn,
      {
        role: "model",
        parts: [
          { text: firstThought(answer), thought: true },
          { text: answerText, thoughtSignature: answerSignature },
        ],
      },
      { role: "user", parts: [{ text: "Thanks." }] },
    ]);

    const completed = Array.from(events).flatMap(({ event }) =>
      event.type === "InferenceCompleted" ? [event.payload] : [],
    );
    expect(completed.map((payload) => payload.finishReason)).toEqual([
      "tool-calls",
      "stop",
      "stop",
    ]);
    // Final chunk of the tool-call stream: prompt 129, candidates 40, thoughts 269.
    expect(completed[0]?.usage).toEqual({
      inputTokens: 129,
      cachedInputTokens: 0,
      cacheWriteInputTokens: 0,
      outputTokens: 309,
      textTokens: 40,
      reasoningTokens: 269,
    });
  });
});

describe("Gemini request settings", () => {
  it.each(["low", "medium", "high"] as const)(
    "sends effort %s as the same thinkingLevel and never a thinkingBudget",
    async (effort) => {
      const recorded = stubFetch([
        {
          body: sse({
            candidates: [{ content: { parts: [{ text: "ok" }] }, finishReason: "STOP" }],
          }),
        },
      ]);
      await Effect.gen(function* () {
        const resolver = yield* ModelResolver;
        const model = yield* resolver.resolve({ provider: "google", modelId: "gemini-4-neon" });
        return yield* model.streamText({ prompt: "hi" }).pipe(Stream.runDrain);
      }).pipe(Effect.provide(resolverLayer({ effort })), Effect.runPromise);
      expect(recorded[0]?.body.generationConfig).toEqual({
        maxOutputTokens: 65_536,
        thinkingConfig: { thinkingLevel: effort, includeThoughts: true },
      });
    },
  );
});

describe("Gemini responses", () => {
  const finish = (finishReason: string | undefined, parts: ReadonlyArray<unknown>) =>
    sse({
      candidates: [{ content: { parts }, ...(finishReason === undefined ? {} : { finishReason }) }],
      usageMetadata: {
        promptTokenCount: 5_000,
        cachedContentTokenCount: 4_100,
        candidatesTokenCount: 7,
        thoughtsTokenCount: 30,
      },
    });
  const call = { functionCall: { name: "get_weather", args: { city: "Paris" }, id: "c1" } };

  it.each([
    ["STOP", [{ text: "done" }], "stop"],
    ["STOP", [call], "tool-calls"],
    ["MAX_TOKENS", [{ text: "cut" }], "length"],
    ["SAFETY", [], "content-filter"],
    ["RECITATION", [], "content-filter"],
    ["MALFORMED_FUNCTION_CALL", [], "error"],
    ["SOME_NEW_REASON", [], "unknown"],
  ] as const)("maps finish reason %s (%j) to %s", async (finishReason, parts, expected) => {
    stubFetch([{ body: finish(finishReason, parts) }]);
    const exit = await streamParts("hi");
    const chunks = Exit.isSuccess(exit) ? Array.from(exit.value) : [];
    const end = chunks.find((part) => part.type === "finish");
    expect(end?.type === "finish" ? end.reason : undefined).toBe(expected);
    expect(end?.type === "finish" ? end.usage : undefined).toMatchObject({
      inputTokens: { total: 5_000, cacheRead: 4_100, uncached: 900, cacheWrite: 0 },
      outputTokens: { total: 37, text: 7, reasoning: 30 },
    });
  });

  it("ends without a finish part when Gemini sent no finish reason", async () => {
    stubFetch([{ body: finish(undefined, [{ text: "partial" }]) }]);
    const exit = await streamParts("hi");
    expect(Exit.isSuccess(exit) && Array.from(exit.value).map((part) => part.type)).toEqual([
      "response-metadata",
      "text-start",
      "text-delta",
    ]);
  });

  it("maps a 400 to InvalidRequestError with Gemini's message", async () => {
    // Real body for `thinkingBudget` sent next to `thinkingLevel`.
    stubFetch([
      {
        status: 400,
        body: '{\n  "error": {\n    "code": 400,\n    "message": "You can only set only one of thinking budget and thinking level.",\n    "status": "INVALID_ARGUMENT"\n  }\n}\n',
      },
    ]);
    const reason = failureReason(await streamParts("hi"));
    expect(reason._tag).toBe("InvalidRequestError");
    expect(reason.message).toContain(
      "You can only set only one of thinking budget and thinking level.",
    );
  });

  it("maps a 429 to RateLimitError", async () => {
    stubFetch([
      {
        status: 429,
        body: JSON.stringify({
          error: {
            code: 429,
            message: "Resource has been exhausted.",
            status: "RESOURCE_EXHAUSTED",
          },
        }),
      },
    ]);
    expect(failureReason(await streamParts("hi"))._tag).toBe("RateLimitError");
  });

  it("maps an error event sent after the stream started by its code", async () => {
    stubFetch([
      {
        body:
          finish(undefined, [{ text: "par" }]) +
          sse({ error: { code: 503, message: "The model is overloaded.", status: "UNAVAILABLE" } }),
      },
    ]);
    expect(failureReason(await streamParts("hi"))._tag).toBe("InternalProviderError");
  });

  it("fails a blocked prompt with ContentPolicyError", async () => {
    stubFetch([{ body: sse({ promptFeedback: { blockReason: "PROHIBITED_CONTENT" } }) }]);
    expect(failureReason(await streamParts("hi"))._tag).toBe("ContentPolicyError");
  });

  it("fails a chunk that is not a Gemini response with InvalidResponseError", async () => {
    stubFetch([{ body: sse({ candidates: "not a list" }) }]);
    expect(failureReason(await streamParts("hi"))._tag).toBe("InvalidResponseError");
  });
});
