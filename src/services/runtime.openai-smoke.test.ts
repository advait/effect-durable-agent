import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import { describe, expect, it } from "vite-plus/test";

import { SubmitMessageCommand } from "../types/commands";
import { CommandId, SessionId } from "../types/core";
import { LiveEventBus } from "./live-event-bus";
import { EDARuntime } from "./runtime";
import { makeEdaTestLayer } from "../testkit/layers";

const shouldRunOpenAiSmoke =
  process.env.EDA_OPENAI_SMOKE === "true" && process.env.OPENAI_API_KEY !== undefined;
const describeOpenAiSmoke = shouldRunOpenAiSmoke ? describe : describe.skip;

const SESSION_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const COMMAND_ID = "018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a";
const modelId = process.env.EDA_OPENAI_MODEL ?? "gpt-4.1-mini";

const openAiLanguageModelLayer = (apiKey: string) => {
  const client = OpenAiClient.layer({
    apiKey: Redacted.make(apiKey),
    ...(process.env.OPENAI_API_URL === undefined ? {} : { apiUrl: process.env.OPENAI_API_URL }),
  }).pipe(Layer.provide(FetchHttpClient.layer));

  return OpenAiLanguageModel.layer({ model: modelId }).pipe(Layer.provide(client));
};

const makeRuntimeLayer = (apiKey: string) =>
  EDARuntime.Live({
    modelSelection: { provider: "openai", modelId },
  }).pipe(
    Layer.provideMerge(
      makeEdaTestLayer({
        sessionId: SessionId.make(SESSION_ID),
        modelLayer: openAiLanguageModelLayer(apiKey),
      }),
    ),
  );

const command = new SubmitMessageCommand({
  commandId: CommandId.make(COMMAND_ID),
  disposition: "queue",
  content: [
    Prompt.textPart({
      text: 'Reply with exactly one short sentence containing the word "pong".',
    }),
  ],
});

describeOpenAiSmoke("EDARuntime OpenAI smoke", () => {
  it("streams a real OpenAI response through the EDA runtime", async () => {
    const apiKey = process.env.OPENAI_API_KEY;
    if (apiKey === undefined) {
      throw new Error("OPENAI_API_KEY is required when EDA_OPENAI_SMOKE=true");
    }

    const program = Effect.scoped(
      Effect.gen(function* () {
        const liveBus = yield* LiveEventBus;
        const runtime = yield* EDARuntime;
        const liveStream = yield* liveBus.subscribe();
        const liveFiber = yield* liveStream.pipe(
          Stream.takeUntil((event) => event.event.type === "CommandCompleted"),
          Stream.runCollect,
          Effect.forkScoped,
        );

        yield* runtime.submit(command);
        return Array.from(yield* Fiber.join(liveFiber));
      }),
    ).pipe(Effect.provide(makeRuntimeLayer(apiKey)));

    const liveEvents = await Effect.runPromise(program);
    const eventTypes = liveEvents.map((event) => event.event.type);
    const text = liveEvents
      .filter((event) => event.event.type === "TextDelta")
      .map((event) => event.event.payload.delta)
      .join("");

    expect(eventTypes).toContain("CommandAdmitted");
    expect(eventTypes).toContain("InferenceStarted");
    expect(eventTypes).toContain("TextDelta");
    expect(eventTypes).toContain("CommandCompleted");
    expect(text.toLowerCase()).toContain("pong");
  }, 90_000);
});
