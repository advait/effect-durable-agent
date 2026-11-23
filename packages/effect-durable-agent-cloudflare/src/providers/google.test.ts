import { readFile } from "node:fs/promises";

import { sequentialUuidV7 } from "effect-durable-agent/services/id-generator";
import { ModelResolver } from "effect-durable-agent/services/model-resolver";
import { EDARuntime } from "effect-durable-agent/services/runtime";
import { makeEDAToolkit } from "effect-durable-agent/services/tool-registry";
import { makeEdaTestLayer } from "effect-durable-agent/testkit/layers";
import { SubmitMessageCommand } from "effect-durable-agent/types/commands";
import { JsonPromptFilePart } from "effect-durable-agent/domain/reduced-state-schema";
import { CommandId, SequenceNumber, SessionId } from "effect-durable-agent/types/core";
import { DurableEventEnvelope } from "effect-durable-agent/types/events";
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

/** A stored event as a Durable Object reads it back: encoded, through JSON, and decoded. */
const reloaded = (event: DurableEventEnvelope): DurableEventEnvelope =>
  Schema.decodeUnknownSync(DurableEventEnvelope)(
    JSON.parse(JSON.stringify(Schema.encodeSync(DurableEventEnvelope)(event))),
  );

/**
 * Sends each message as one turn of a Gemini session through the EDA runtime. Every turn after the
 * first runs in a new runtime that starts from the previous one's event log, reloaded as a Durable
 * Object reloads its stored history, so each request after a turn boundary is built from storage.
 * Returns the final event log.
 */
const geminiSession = async (
  turns: ReadonlyArray<string | ReadonlyArray<Prompt.UserMessagePart>>,
  options: { readonly systemPrompt?: string } = {},
): Promise<ReadonlyArray<DurableEventEnvelope>> => {
  const sessionId = SessionId.make(crypto.randomUUID());
  let log: ReadonlyArray<DurableEventEnvelope> = [];
  for (const [index, turn] of turns.entries()) {
    const layer = EDARuntime.Live({
      modelSelection: { provider: "google", modelId: "gemini-4-hydrogen" },
      ...options,
    }).pipe(
      Layer.provideMerge(
        makeEdaTestLayer({
          sessionId,
          modelResolverLayer: resolverLayer(),
          toolkit,
          ...(index === 0
            ? {}
            : {
                seedEvents: log.map(reloaded),
                ids: Array.from({ length: 1_000 }, (_, i) => sequentialUuidV7(10_000 * index + i)),
              }),
        }),
      ),
    );
    log = await Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        yield* runtime.submitAndBlock(
          new SubmitMessageCommand({
            commandId: CommandId.make(
              `018f6bd5-2f2a-7b1e-8f1b-${String(index + 1).padStart(12, "0")}`,
            ),
            disposition: "queue",
            content: typeof turn === "string" ? [Prompt.textPart({ text: turn })] : turn,
          }),
        );
        const snapshot = yield* runtime.snapshot();
        const replay = yield* runtime.eventsAfter(SequenceNumber.make(0));
        const rows = yield* replay.pipe(Stream.take(snapshot.state.lastSeq), Stream.runCollect);
        return Array.from(rows, (row) => row.event);
      }),
    ).pipe(Effect.provide(layer), Effect.runPromise);
  }
  return log;
};

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
    const events = await geminiSession(["Plan a two-city picnic.", "Thanks."], {
      systemPrompt: "You are terse. Use tools for weather.",
    });

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

    // After a restart from the stored log, the streamed answer replays as its text and then the
    // empty text part that carried the signature, as Gemini sent them.
    expect(recorded[2]?.body.contents).toEqual([
      ...firstTurn,
      {
        role: "model",
        parts: [
          { text: firstThought(answer), thought: true },
          { text: answerText },
          { text: "", thoughtSignature: answerSignature },
        ],
      },
      { role: "user", parts: [{ text: "Thanks." }] },
    ]);

    const completed = events.flatMap((event) =>
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

describe("Gemini provider failures in a session", () => {
  it.each([
    "MALFORMED_FUNCTION_CALL",
    "UNEXPECTED_TOOL_CALL",
    "TOO_MANY_TOOL_CALLS",
    "MALFORMED_RESPONSE",
    "MISSING_THOUGHT_SIGNATURE",
    "PUP_LIMITED_DISABLED",
  ])("records %s as a failed inference and runs no streamed tool call", async (finishReason) => {
    const recorded = stubFetch([
      {
        body: sse(
          {
            candidates: [
              {
                content: {
                  role: "model",
                  parts: [
                    {
                      functionCall: { name: "get_weather", args: { city: "Paris" }, id: "c1" },
                      thoughtSignature: "U0lH",
                    },
                  ],
                },
              },
            ],
          },
          { candidates: [{ finishReason, finishMessage: "Bad call arguments." }] },
        ),
      },
    ]);
    const events = await geminiSession(["Weather in Paris?"]);
    const types = events.map((event) => event.type);
    expect(types).toContain("InferenceFailed");
    expect(types).toContain("RunFailed");
    expect(types).not.toContain("InferenceCompleted");
    expect(types).not.toContain("RunCompleted");
    expect(types).not.toContain("ToolCallCreated");
    // No tool ran, so nothing sent its result back.
    expect(recorded).toHaveLength(1);
    const failed = events.find((event) => event.type === "InferenceFailed");
    expect(failed?.type === "InferenceFailed" ? failed.payload.error.message : "").toContain(
      `Gemini finished with ${finishReason}: Bad call arguments.`,
    );
  });
});

describe("Gemini thought signatures", () => {
  interface Case {
    readonly name: string;
    readonly sent: ReadonlyArray<FixturePart>;
    /** The model turn of the next request; defaults to `sent`. */
    readonly replayed?: ReadonlyArray<FixturePart>;
  }
  it.each<Case>([
    {
      name: "two adjacent signed answer parts",
      sent: [
        { text: "First", thoughtSignature: "U0lHMQ==" },
        { text: "Second", thoughtSignature: "U0lHMg==" },
      ],
    },
    {
      name: "streamed answer pieces and the signed empty part that ends them",
      sent: [{ text: "Hel" }, { text: "lo" }, { text: "", thoughtSignature: "U0lHMw==" }],
      replayed: [{ text: "Hello" }, { text: "", thoughtSignature: "U0lHMw==" }],
    },
    {
      name: "a signed thought, and a signed piece between unsigned ones",
      sent: [
        { text: "Plan.", thought: true, thoughtSignature: "VEhPVUdIVA==" },
        { text: "A" },
        { text: "B", thoughtSignature: "U0lHNA==" },
        { text: "C" },
      ],
    },
  ])(
    "commits $name as parts of their own kind and replays them exactly after a restart",
    async ({ sent, replayed = sent }) => {
      // One SSE chunk per part, as Gemini streams them.
      const recorded = stubFetch([
        {
          body: sse(
            ...sent.map((part, index) => ({
              candidates: [
                {
                  content: { role: "model", parts: [part] },
                  ...(index === sent.length - 1 ? { finishReason: "STOP" } : {}),
                },
              ],
            })),
          ),
        },
        {
          body: sse({
            candidates: [{ content: { parts: [{ text: "ok" }] }, finishReason: "STOP" }],
          }),
        },
      ]);
      const events = await geminiSession(["hi", "again"]);
      // A thought is reasoning and everything else is answer text, the signed empty part included.
      const committed = events.find((event) => event.type === "AssistantMessageCommitted");
      expect(
        committed?.type === "AssistantMessageCommitted"
          ? committed.payload.promptParts.map((part) => ({
              type: part.type,
              text: "text" in part ? part.text : undefined,
              options: part.options,
            }))
          : [],
      ).toEqual(
        replayed.map((part) => ({
          type: part.thought === true ? "reasoning" : "text",
          text: part.text,
          options:
            part.thoughtSignature === undefined
              ? {}
              : { google: { thoughtSignature: part.thoughtSignature } },
        })),
      );
      expect(recorded[1]?.body.contents).toEqual([
        { role: "user", parts: [{ text: "hi" }] },
        { role: "model", parts: replayed },
        { role: "user", parts: [{ text: "again" }] },
      ]);
    },
  );
});

describe("Gemini history on another provider", () => {
  it("sends Anthropic and OpenAI no part or message that holds only a Gemini signature", async () => {
    const stop = (parts: ReadonlyArray<FixturePart>) =>
      sse({ candidates: [{ content: { role: "model", parts }, finishReason: "STOP" }] });
    const recorded = stubFetch([
      { body: stop([{ text: "Hello" }, { text: "", thoughtSignature: "U0lHMQ==" }]) },
      // A turn whose only output is the signed empty part.
      { body: stop([{ text: "", thoughtSignature: "U0lHMg==" }]) },
      // The other providers' requests are recorded; their answers do not matter.
      { status: 400, body: "{}" },
      { status: 400, body: "{}" },
    ]);
    const events = await geminiSession(["first", "second"]);
    const answers = events.flatMap((event) =>
      event.type === "AssistantMessageCommitted" ? [event.payload.promptParts] : [],
    );
    const user = (text: string) =>
      Prompt.makeMessage("user", { content: [Prompt.textPart({ text })] });
    const history = Prompt.fromMessages([
      user("first"),
      ...answers.slice(0, 1).map((content) => Prompt.makeMessage("assistant", { content })),
      user("second"),
      ...answers.slice(1).map((content) => Prompt.makeMessage("assistant", { content })),
      user("third"),
    ]);
    const send = (provider: "anthropic" | "openai") =>
      Effect.gen(function* () {
        const resolver = yield* ModelResolver;
        const selection = { provider, modelId: `${provider}-model` };
        const model = yield* resolver.resolve(selection);
        return yield* model.generateText({
          prompt: resolver.preparePrompt?.(selection, history) ?? history,
        });
      }).pipe(
        Effect.provide(
          makeEDAMultiProviderModelResolverLayer({
            [provider]: { apiKey: "test-key" },
            resolve: (selection) => ({
              provider,
              modelId: selection?.modelId ?? "",
              config: {},
            }),
          }),
        ),
        Effect.exit,
        Effect.runPromise,
      );
    await send("anthropic");
    await send("openai");

    expect(answers).toHaveLength(2);
    const [anthropic, openai] = recorded.slice(2).map((request) => request.body);
    // Each message as its role and texts; Anthropic joins consecutive user messages.
    const Messages = Schema.Array(
      Schema.Struct({
        role: Schema.String,
        content: Schema.Array(Schema.Struct({ text: Schema.String })),
      }),
    );
    const texts = (messages: unknown) =>
      Schema.decodeUnknownSync(Messages)(messages).map((message) => [
        message.role,
        message.content.map((part) => part.text),
      ]);
    expect(texts(anthropic?.messages)).toEqual([
      ["user", ["first"]],
      ["assistant", ["Hello"]],
      ["user", ["second", "third"]],
    ]);
    expect(texts(openai?.input)).toEqual([
      ["user", ["first"]],
      ["assistant", ["Hello"]],
      ["user", ["second"]],
      ["user", ["third"]],
    ]);
    expect(JSON.stringify([anthropic, openai])).not.toContain("U0lH");
  });
});

describe("Gemini file parts", () => {
  /** A file part as EDA holds it: commands arrive as JSON, so file data is a string. */
  const asSent = (part: Prompt.FilePart) =>
    Schema.decodeSync(JsonPromptFilePart)(Schema.encodeSync(JsonPromptFilePart)(part));

  const filesUri = "https://generativelanguage.googleapis.com/v1beta/files/abc";

  it.each([
    {
      // Bytes arrive as a base64 data URL.
      form: "bytes",
      part: Prompt.filePart({ data: new Uint8Array([104, 105]), mediaType: "text/plain" }),
      sent: { inlineData: { mimeType: "text/plain", data: "aGk=" } },
    },
    {
      // A URL arrives as its href.
      form: "a Gemini Files URL",
      part: Prompt.filePart({ data: new URL(filesUri), mediaType: "application/pdf" }),
      sent: { fileData: { mimeType: "application/pdf", fileUri: filesUri } },
    },
    {
      form: "bare base64",
      part: Prompt.filePart({ data: "iVBORw0KGgo=", mediaType: "image/png" }),
      sent: { inlineData: { mimeType: "image/png", data: "iVBORw0KGgo=" } },
    },
    {
      form: "a base64 data URL with its own media type",
      part: Prompt.filePart({ data: "data:image/webp;base64,UklGRg==", mediaType: "image/*" }),
      sent: { inlineData: { mimeType: "image/webp", data: "UklGRg==" } },
    },
    {
      form: "a percent-encoded base64 data URL",
      part: Prompt.filePart({ data: "data:text/plain;base64,aGk%3D", mediaType: "text/plain" }),
      sent: { inlineData: { mimeType: "text/plain", data: "aGk=" } },
    },
    {
      // base64("hi there")
      form: "a data URL without base64",
      part: Prompt.filePart({ data: "data:text/plain,hi%20there", mediaType: "text/plain" }),
      sent: { inlineData: { mimeType: "text/plain", data: "aGkgdGhlcmU=" } },
    },
    {
      // base64 of the UTF-8 bytes of "café"
      form: "a data URL without base64, with parameters and UTF-8 escapes",
      part: Prompt.filePart({
        data: "data:text/plain;charset=utf-8,caf%C3%A9",
        mediaType: "text/plain",
      }),
      sent: { inlineData: { mimeType: "text/plain", data: "Y2Fmw6k=" } },
    },
    {
      // An image of unknown type is sent as JPEG, as the Anthropic and OpenAI adapters send it.
      form: "bytes of an image of unknown type",
      part: Prompt.filePart({ data: new Uint8Array([255, 216, 255, 224]), mediaType: "image/*" }),
      sent: { inlineData: { mimeType: "image/jpeg", data: "/9j/4A==" } },
    },
  ])("sends $form as Gemini data, before and after a restart", async ({ part, sent }) => {
    const recorded = stubFetch(
      Array.from({ length: 2 }, () => ({
        body: sse({ candidates: [{ content: { parts: [{ text: "ok" }] }, finishReason: "STOP" }] }),
      })),
    );
    await geminiSession([[Prompt.textPart({ text: "Read this." }), asSent(part)], "again"]);
    const message = { role: "user", parts: [{ text: "Read this." }, sent] };
    // The second request rebuilds the first message from the stored log.
    expect(recorded.map((request) => request.body.contents[0])).toEqual([message, message]);
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
    ["OTHER", [], "other"],
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

  it.each([
    ["MALFORMED_FUNCTION_CALL", "InvalidOutputError"],
    ["UNEXPECTED_TOOL_CALL", "InvalidOutputError"],
    ["TOO_MANY_TOOL_CALLS", "InvalidOutputError"],
    ["MALFORMED_RESPONSE", "InvalidOutputError"],
    ["MISSING_THOUGHT_SIGNATURE", "InvalidRequestError"],
    ["PUP_LIMITED_DISABLED", "ContentPolicyError"],
  ] as const)(
    "fails finish reason %s with %s and keeps its finishMessage",
    async (finishReason, tag) => {
      stubFetch([
        {
          body: sse({
            candidates: [{ finishReason, finishMessage: "Gemini's account of the failure." }],
          }),
        },
      ]);
      const reason = failureReason(await streamParts("hi"));
      expect(reason._tag).toBe(tag);
      expect(reason.metadata).toEqual({
        google: { finishReason, finishMessage: "Gemini's account of the failure." },
      });
    },
  );

  it("returns an answer and its signed empty part as text, with no reasoning", async () => {
    stubFetch([
      {
        body: JSON.stringify({
          candidates: [
            {
              content: { parts: [{ text: "Hello" }, { text: "", thoughtSignature: "U0lH" }] },
              finishReason: "STOP",
            },
          ],
        }),
      },
    ]);
    const response = await Effect.gen(function* () {
      const resolver = yield* ModelResolver;
      const model = yield* resolver.resolve({ provider: "google", modelId: "gemini-4-neon" });
      return yield* model.generateText({ prompt: "hi" });
    }).pipe(Effect.provide(resolverLayer()), Effect.runPromise);
    expect(response.text).toBe("Hello");
    expect(response.reasoning).toEqual([]);
    // The signature stays on its own empty text part, which a replay sends back.
    expect(
      response.content.flatMap((part) =>
        part.type === "text" ? [{ text: part.text, metadata: part.metadata }] : [],
      ),
    ).toEqual([
      { text: "Hello", metadata: {} },
      { text: "", metadata: { google: { thoughtSignature: "U0lH" } } },
    ]);
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
