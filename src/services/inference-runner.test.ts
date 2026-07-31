import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Response from "effect/unstable/ai/Response";
import { describe, expect, it } from "vite-plus/test";

import {
  InferenceId,
  EventId,
  RunId,
  SequenceNumber,
  SessionId,
  ToolCallId,
  TurnId,
} from "../types/core";
import { ProviderPartId, UnixEpochMillis } from "../types/events";
import { EDASessionStore } from "./session-store";
import { LiveEventBus } from "./live-event-bus";
import { InferenceRunner, type InferenceRunnerStreamPart } from "./inference-runner";
import { SessionState } from "./session-state";
import { makeEdaTestLayer, testNowMs } from "../testkit/layers";

const SESSION_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const RUN_ID = "018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a";
const TURN_ID = "018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a";
const INFERENCE_ID = "018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a";
const INFERENCE_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f2a-1f2e3d4c5b6a";
const TEXT_EVENT_ID = "018f6bd5-2f2a-7b1e-8f3a-1f2e3d4c5b6a";
const INFERENCE_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f4a-1f2e3d4c5b6a";
const ASSISTANT_PARTIAL_MESSAGE_ID = "018f6bd5-2f2a-7b1e-8f4b-1f2e3d4c5b6a";
const ASSISTANT_PARTIAL_EVENT_ID = "018f6bd5-2f2a-7b1e-8f4c-1f2e3d4c5b6a";
const INFERENCE_FAILED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f5a-1f2e3d4c5b6a";
const CREATED_AT_MS = testNowMs;
const generatedId = (offset: number) =>
  `018f6bd5-2f2a-7b1e-${(0x9100 + offset).toString(16)}-1f2e3d4c5b6a`;

const EmptyParams = Schema.Struct({});

const makeTestLayer = (
  ids: ReadonlyArray<string>,
  stream: Stream.Stream<InferenceRunnerStreamPart, unknown>,
  onStreamText?: (input: {
    readonly index: number;
    readonly toolNames: ReadonlyArray<string>;
  }) => void,
) =>
  makeEdaTestLayer({
    sessionId: SessionId.make(SESSION_ID),
    ids,
    parts: stream,
    toolSchemas: new Map([["noop", EmptyParams]]),
    nowMs: CREATED_AT_MS,
    onStreamText,
  });

const inferenceInput = {
  runId: RunId.make(RUN_ID),
  turnId: TurnId.make(TURN_ID),
  inferenceId: InferenceId.make(INFERENCE_ID),
};

const usage = () =>
  new Response.Usage({
    inputTokens: {
      uncached: undefined,
      total: 10,
      cacheRead: undefined,
      cacheWrite: undefined,
    },
    outputTokens: { total: 5, text: undefined, reasoning: undefined },
  });

describe("InferenceRunner", () => {
  it("passes registered tools to the provider while retaining EDA execution ownership", async () => {
    const stream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const streamCalls: Array<{
      readonly index: number;
      readonly toolNames: ReadonlyArray<string>;
    }> = [];
    const program = Effect.gen(function* () {
      const runner = yield* InferenceRunner;
      const sessionState = yield* SessionState;
      return yield* runner.runInference({
        ...inferenceInput,
        prompt: "hello",
        eventSink: sessionState,
      });
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [INFERENCE_STARTED_EVENT_ID, INFERENCE_COMPLETED_EVENT_ID],
          stream,
          ({ index, toolNames }) => streamCalls.push({ index, toolNames }),
        ),
      ),
    );

    await Effect.runPromise(program);

    expect(streamCalls).toEqual([{ index: 0, toolNames: ["noop"] }]);
  });

  it("commits inference start and delegates provider stream parts to the inference writer", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const liveBus = yield* LiveEventBus;
        const runner = yield* InferenceRunner;
        const liveStream = yield* liveBus.subscribe();
        const liveFiber = yield* liveStream.pipe(
          Stream.take(4),
          Stream.runCollect,
          Effect.forkScoped,
        );
        const sessionState = yield* SessionState;
        const result = yield* runner.runInference({
          ...inferenceInput,
          prompt: "hello",
          eventSink: sessionState,
        });
        const liveEvents = yield* Fiber.join(liveFiber);
        return { liveEvents, result };
      }),
    ).pipe(
      Effect.provide(
        makeTestLayer(
          [
            INFERENCE_STARTED_EVENT_ID,
            TEXT_EVENT_ID,
            ASSISTANT_PARTIAL_MESSAGE_ID,
            ASSISTANT_PARTIAL_EVENT_ID,
            INFERENCE_COMPLETED_EVENT_ID,
          ],
          stream,
        ),
      ),
    );

    const { liveEvents, result } = await Effect.runPromise(program);

    expect(result).toMatchObject({
      partsRecorded: 2,
      started: {
        position: { seq: 1, subSeq: 0 },
        event: {
          eventId: EventId.make(INFERENCE_STARTED_EVENT_ID),
          type: "InferenceStarted",
          sessionId: SessionId.make(SESSION_ID),
          createdAtMs: UnixEpochMillis.make(CREATED_AT_MS),
        },
      },
      terminal: {
        _tag: "InferenceRunFinished",
        committed: [
          {
            position: { seq: 2, subSeq: 0 },
            event: {
              eventId: EventId.make(ASSISTANT_PARTIAL_EVENT_ID),
              type: "AssistantMessageCommitted",
            },
          },
          {
            position: { seq: 3, subSeq: 0 },
            event: {
              eventId: EventId.make(INFERENCE_COMPLETED_EVENT_ID),
              type: "InferenceCompleted",
            },
          },
        ],
      },
    });
    expect(Array.from(liveEvents).map((event) => event.event.type)).toEqual([
      "InferenceStarted",
      "TextDelta",
      "AssistantMessageCommitted",
      "InferenceCompleted",
    ]);
    expect(Array.from(liveEvents)[1]).toMatchObject({
      position: { seq: 1, subSeq: 1 },
      event: {
        eventId: EventId.make(TEXT_EVENT_ID),
        payload: { providerPartId: ProviderPartId.make("text-1") },
      },
    });
  });

  it("turns provider stream failures into failed inferences", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
    ).pipe(Stream.concat(Stream.fail(new Error("provider failed"))));
    const program = Effect.gen(function* () {
      const runner = yield* InferenceRunner;
      const sessionState = yield* SessionState;
      return yield* runner.runInference({
        ...inferenceInput,
        prompt: "hello",
        eventSink: sessionState,
      });
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [
            INFERENCE_STARTED_EVENT_ID,
            TEXT_EVENT_ID,
            ASSISTANT_PARTIAL_MESSAGE_ID,
            ASSISTANT_PARTIAL_EVENT_ID,
            INFERENCE_FAILED_EVENT_ID,
          ],
          stream,
        ),
      ),
    );

    const result = await Effect.runPromise(program);

    expect(result).toMatchObject({
      partsRecorded: 2,
      terminal: {
        _tag: "InferenceRunFailed",
        committed: [
          {
            position: { seq: 2, subSeq: 0 },
            event: {
              eventId: EventId.make(ASSISTANT_PARTIAL_EVENT_ID),
              type: "AssistantPartialCommitted",
              payload: { promptParts: [{ type: "text", text: "hello" }] },
            },
          },
          {
            position: { seq: 3, subSeq: 0 },
            event: {
              eventId: EventId.make(INFERENCE_FAILED_EVENT_ID),
              type: "InferenceFailed",
              payload: { error: { message: "provider failed" } },
            },
          },
        ],
      },
    });
  });

  it("records a failed inference when the stream ends without finish", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "unfinished" }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* InferenceRunner;
      const sessionState = yield* SessionState;
      return yield* runner.runInference({
        ...inferenceInput,
        prompt: "hello",
        eventSink: sessionState,
      });
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [
            INFERENCE_STARTED_EVENT_ID,
            TEXT_EVENT_ID,
            ASSISTANT_PARTIAL_MESSAGE_ID,
            ASSISTANT_PARTIAL_EVENT_ID,
            INFERENCE_FAILED_EVENT_ID,
          ],
          stream,
        ),
      ),
    );

    const result = await Effect.runPromise(program);

    expect(result).toMatchObject({
      partsRecorded: 1,
      terminal: {
        _tag: "InferenceRunFailed",
        committed: [
          {
            event: {
              type: "AssistantPartialCommitted",
              payload: { promptParts: [{ type: "text", text: "unfinished" }] },
            },
          },
          {
            event: {
              type: "InferenceFailed",
              payload: { error: { message: "Provider stream ended without a finish part" } },
            },
          },
        ],
      },
    });
  });

  it("records streamed tool params live and finalizes a valid tool call after finish", async () => {
    const ids = Array.from({ length: 9 }, (_, index) => generatedId(index));
    const frameworkToolCallId = ToolCallId.make(ids[1]!);
    const stream = Stream.make(
      Response.makePart("tool-params-start", {
        id: "tool-part-1",
        name: "noop",
        providerExecuted: false,
      }),
      Response.makePart("tool-params-delta", { id: "tool-part-1", delta: "{}" }),
      Response.makePart("tool-params-end", { id: "tool-part-1" }),
      Response.makePart("tool-call", {
        id: "tool-part-1",
        name: "noop",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const liveBus = yield* LiveEventBus;
        const runner = yield* InferenceRunner;
        const liveStream = yield* liveBus.subscribe();
        const liveFiber = yield* liveStream.pipe(
          Stream.take(7),
          Stream.runCollect,
          Effect.forkScoped,
        );
        const sessionState = yield* SessionState;
        const result = yield* runner.runInference({
          ...inferenceInput,
          prompt: "hello",
          eventSink: sessionState,
        });
        const liveEvents = yield* Fiber.join(liveFiber);
        return { liveEvents: Array.from(liveEvents), result };
      }),
    ).pipe(Effect.provide(makeTestLayer(ids, stream)));

    const { liveEvents, result } = await Effect.runPromise(program);

    expect(liveEvents.map((event) => event.event.type)).toEqual([
      "InferenceStarted",
      "ToolParamsStart",
      "ToolParamsDelta",
      "ToolParamsEnd",
      "AssistantMessageCommitted",
      "ToolCallCreated",
      "InferenceCompleted",
    ]);
    expect(liveEvents.map((event) => event.position)).toEqual([
      { seq: 1, subSeq: 0 },
      { seq: 1, subSeq: 1 },
      { seq: 1, subSeq: 2 },
      { seq: 1, subSeq: 3 },
      { seq: 2, subSeq: 0 },
      { seq: 3, subSeq: 0 },
      { seq: 4, subSeq: 0 },
    ]);
    expect(liveEvents[1]).toMatchObject({
      event: {
        eventId: EventId.make(ids[2]!),
        payload: {
          providerPartId: ProviderPartId.make("tool-part-1"),
          toolCallId: frameworkToolCallId,
          toolName: "noop",
          providerExecuted: false,
        },
      },
    });
    expect(liveEvents[2]).toMatchObject({
      event: { payload: { toolCallId: frameworkToolCallId, delta: "{}" } },
    });
    expect(liveEvents[3]).toMatchObject({
      event: { payload: { toolCallId: frameworkToolCallId } },
    });
    expect(result.terminal).toMatchObject({
      _tag: "InferenceRunFinished",
      committed: [
        { event: { type: "AssistantMessageCommitted", eventId: EventId.make(ids[6]!) } },
        {
          event: {
            type: "ToolCallCreated",
            eventId: EventId.make(ids[7]!),
            payload: {
              toolCallId: frameworkToolCallId,
              promptPart: {
                id: ProviderPartId.make("tool-part-1"),
                name: "noop",
                params: {},
                providerExecuted: false,
              },
            },
          },
        },
        { event: { type: "InferenceCompleted", eventId: EventId.make(ids[8]!) } },
      ],
    });
  });

  it("rejects invalid final tool-call params after inference completion", async () => {
    const ids = Array.from({ length: 6 }, (_, index) => generatedId(20 + index));
    const frameworkToolCallId = ToolCallId.make(ids[1]!);
    const stream = Stream.make(
      Response.makePart("tool-call", {
        id: "tool-part-invalid",
        name: "needText",
        params: { text: 123 },
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* InferenceRunner;
      const store = yield* EDASessionStore;
      const sessionState = yield* SessionState;
      const result = yield* runner.runInference({
        ...inferenceInput,
        prompt: "hello",
        eventSink: sessionState,
      });
      const committed = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { committed: Array.from(committed), result };
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          ids,
          parts: stream,
          toolSchemas: new Map([["needText", Schema.Struct({ text: Schema.String })]]),
          nowMs: CREATED_AT_MS,
        }),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);
    const eventTypes = committed.map((entry) => entry.event.type);
    const rejected = committed.find((entry) => entry.event.type === "ToolCallRejected");

    expect(eventTypes).toEqual([
      "InferenceStarted",
      "AssistantMessageCommitted",
      "ToolCallRejected",
      "InferenceCompleted",
    ]);
    expect(eventTypes).not.toContain("ToolCallCreated");
    expect(result.terminal).toMatchObject({
      _tag: "InferenceRunFinished",
      committed: [
        { event: { type: "AssistantMessageCommitted", eventId: EventId.make(ids[3]!) } },
        { event: { type: "ToolCallRejected", eventId: EventId.make(ids[4]!) } },
        { event: { type: "InferenceCompleted", eventId: EventId.make(ids[5]!) } },
      ],
    });
    expect(rejected).toMatchObject({
      event: {
        payload: {
          toolCallId: frameworkToolCallId,
          promptPart: {
            id: ProviderPartId.make("tool-part-invalid"),
            name: "needText",
            isFailure: true,
            result: {
              reason: "invalid-params",
              modelFeedback: expect.stringContaining("needText"),
            },
          },
        },
      },
    });
  });

  it("persists response metadata, finish metadata, finish reason, and usage", async () => {
    const ids = Array.from({ length: 2 }, (_, index) => generatedId(40 + index));
    const stream = Stream.make(
      Response.makePart("response-metadata", {
        id: "response-1",
        modelId: "model-1",
        timestamp: undefined,
        request: undefined,
        metadata: { provider: "metadata" },
      }),
      Response.makePart("finish", {
        reason: "length",
        usage: new Response.Usage({
          inputTokens: { uncached: 7, total: undefined, cacheRead: 2, cacheWrite: 1 },
          outputTokens: { total: undefined, text: 3, reasoning: 4 },
        }),
        response: undefined,
        metadata: { finish: "metadata" },
      }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* InferenceRunner;
      const sessionState = yield* SessionState;
      return yield* runner.runInference({
        ...inferenceInput,
        prompt: "hello",
        eventSink: sessionState,
      });
    }).pipe(Effect.provide(makeTestLayer(ids, stream)));

    const result = await Effect.runPromise(program);

    expect(result.terminal).toMatchObject({
      _tag: "InferenceRunFinished",
      committed: [
        {
          event: {
            type: "InferenceCompleted",
            payload: {
              finishReason: "length",
              responseMetadata: {
                id: "response-1",
                modelId: "model-1",
                timestamp: undefined,
                request: undefined,
                metadata: { provider: "metadata" },
              },
              finishMetadata: { response: undefined, metadata: { finish: "metadata" } },
              usage: {
                inputTokens: 10,
                cachedInputTokens: 2,
                outputTokens: 7,
                textTokens: 3,
                reasoningTokens: 4,
              },
            },
          },
        },
      ],
    });
  });

  it("publishes reasoning deltas with the same live positioning policy as text deltas", async () => {
    const ids = Array.from({ length: 5 }, (_, index) => generatedId(60 + index));
    const stream = Stream.make(
      Response.makePart("reasoning-delta", { id: "reasoning-1", delta: "thinking" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const liveBus = yield* LiveEventBus;
        const runner = yield* InferenceRunner;
        const liveStream = yield* liveBus.subscribe();
        const liveFiber = yield* liveStream.pipe(
          Stream.take(4),
          Stream.runCollect,
          Effect.forkScoped,
        );
        const sessionState = yield* SessionState;
        const result = yield* runner.runInference({
          ...inferenceInput,
          prompt: "hello",
          eventSink: sessionState,
        });
        const liveEvents = yield* Fiber.join(liveFiber);
        return { liveEvents: Array.from(liveEvents), result };
      }),
    ).pipe(Effect.provide(makeTestLayer(ids, stream)));

    const { liveEvents, result } = await Effect.runPromise(program);

    expect(liveEvents.map((event) => event.event.type)).toEqual([
      "InferenceStarted",
      "ReasoningDelta",
      "AssistantMessageCommitted",
      "InferenceCompleted",
    ]);
    expect(liveEvents[1]).toMatchObject({
      position: { seq: 1, subSeq: 1 },
      event: {
        eventId: EventId.make(ids[1]!),
        payload: { providerPartId: ProviderPartId.make("reasoning-1"), delta: "thinking" },
      },
    });
    expect(result).toMatchObject({
      partsRecorded: 2,
      terminal: { _tag: "InferenceRunFinished", reasoningText: "thinking" },
    });
  });

  it("fails provider-executed tool calls as unsupported", async () => {
    const stream = Stream.make(
      Response.makePart("tool-call", {
        id: "provider-tool-1",
        name: "noop",
        params: {},
        providerExecuted: true,
      }),
      Response.toolResultPart({
        id: "provider-tool-1",
        name: "noop",
        isFailure: false,
        result: { ok: true },
        encodedResult: { ok: true },
        providerExecuted: true,
        preliminary: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* InferenceRunner;
      const store = yield* EDASessionStore;
      const sessionState = yield* SessionState;
      const result = yield* runner.runInference({
        ...inferenceInput,
        prompt: "hello",
        eventSink: sessionState,
      });
      const committed = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { committed: Array.from(committed), result };
    }).pipe(
      Effect.provide(
        makeTestLayer([INFERENCE_STARTED_EVENT_ID, INFERENCE_FAILED_EVENT_ID], stream),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);

    expect(result).toMatchObject({
      partsRecorded: 1,
      terminal: { _tag: "InferenceRunFailed" },
    });
    expect(committed.map((entry) => entry.event.type)).toEqual([
      "InferenceStarted",
      "InferenceFailed",
    ]);
    expect(committed[1]).toMatchObject({
      event: {
        eventId: EventId.make(INFERENCE_FAILED_EVENT_ID),
        payload: {
          error: {
            message: expect.stringContaining("Provider-executed tool calls are unsupported"),
          },
        },
      },
    });
  });

  it("does not create tool lifecycle events for unsupported provider-executed tool calls", async () => {
    const stream = Stream.make(
      Response.makePart("tool-call", {
        id: "provider-tool-1",
        name: "noop",
        params: {},
        providerExecuted: true,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* InferenceRunner;
      const store = yield* EDASessionStore;
      const sessionState = yield* SessionState;
      yield* runner.runInference({ ...inferenceInput, prompt: "hello", eventSink: sessionState });
      return yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
    }).pipe(
      Effect.provide(
        makeTestLayer([INFERENCE_STARTED_EVENT_ID, INFERENCE_FAILED_EVENT_ID], stream),
      ),
    );

    const committed = Array.from(await Effect.runPromise(program));

    expect(committed.map((entry) => entry.event.type)).toEqual([
      "InferenceStarted",
      "InferenceFailed",
    ]);
    expect(committed[1]).toMatchObject({
      event: {
        eventId: EventId.make(INFERENCE_FAILED_EVENT_ID),
        payload: {
          error: {
            message: expect.stringContaining("Provider-executed tool calls are unsupported"),
          },
        },
      },
    });
  });

  it("drops provider sideband and unmatched non-provider tool-result parts instead of failing inferences", async () => {
    const stream = Stream.make(
      Response.toolResultPart({
        id: "tool-call-1",
        name: "noop",
        isFailure: false,
        result: { ok: true },
        encodedResult: { ok: true },
        providerExecuted: false,
        preliminary: false,
      }),
      Response.makePart("file", { mediaType: "text/plain", data: new Uint8Array([1, 2, 3]) }),
      Response.makePart("source", {
        sourceType: "url",
        id: "source-1",
        url: new URL("https://example.com"),
        title: "Example",
      }),
      Response.makePart("tool-approval-request", {
        approvalId: "approval-1",
        toolCallId: "tool-call-1",
      }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* InferenceRunner;
      const store = yield* EDASessionStore;
      const sessionState = yield* SessionState;
      const result = yield* runner.runInference({
        ...inferenceInput,
        prompt: "hello",
        eventSink: sessionState,
      });
      const committed = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { committed: Array.from(committed), result };
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: stream,
          toolSchemas: new Map([["noop", EmptyParams]]),
          nowMs: CREATED_AT_MS,
        }),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);

    expect(result).toMatchObject({
      partsRecorded: 5,
      terminal: { _tag: "InferenceRunFinished" },
    });
    expect(committed.map((entry) => entry.event.type)).toEqual([
      "InferenceStarted",
      "InferenceCompleted",
    ]);
  });

  it("commits assistant partial content when an in-flight inference is interrupted", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "partial answer" }),
    ).pipe(Stream.concat(Stream.never));
    const program = Effect.scoped(
      Effect.gen(function* () {
        const liveBus = yield* LiveEventBus;
        const runner = yield* InferenceRunner;
        const store = yield* EDASessionStore;
        const sessionState = yield* SessionState;
        const liveStream = yield* liveBus.subscribe();
        const textSeen = yield* liveStream.pipe(
          Stream.filter((event) => event.event.type === "TextDelta"),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );
        const fiber = yield* runner
          .runInference({ ...inferenceInput, prompt: "hello", eventSink: sessionState })
          .pipe(Effect.forkScoped);

        yield* Fiber.join(textSeen);
        yield* Fiber.interrupt(fiber);
        const committed = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
        return Array.from(committed);
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: stream,
          toolSchemas: new Map([["noop", EmptyParams]]),
          nowMs: CREATED_AT_MS,
        }),
      ),
    );

    const committed = await Effect.runPromise(program);
    const eventTypes = committed.map((entry) => entry.event.type);
    const partial = committed.find((entry) => entry.event.type === "AssistantPartialCommitted");

    expect(eventTypes).toEqual([
      "InferenceStarted",
      "AssistantPartialCommitted",
      "InferenceFailed",
    ]);
    expect(partial).toMatchObject({
      event: {
        payload: {
          promptParts: [{ type: "text", text: "partial answer" }],
          reason: expect.any(String),
        },
      },
    });
  });

  it("drops provider parts after a terminal part without committing a second terminal", async () => {
    const stream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
      Response.makePart("text-delta", { id: "text-1", delta: "too late" }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* InferenceRunner;
      const store = yield* EDASessionStore;
      const sessionState = yield* SessionState;
      const result = yield* runner.runInference({
        ...inferenceInput,
        prompt: "hello",
        eventSink: sessionState,
      });
      const committed = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { committed: Array.from(committed), result };
    }).pipe(
      Effect.provide(
        makeTestLayer([INFERENCE_STARTED_EVENT_ID, INFERENCE_COMPLETED_EVENT_ID], stream),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);

    expect(result).toMatchObject({
      partsRecorded: 1,
      terminal: { _tag: "InferenceRunFinished" },
    });
    expect(committed.map((entry) => entry.event.type)).toEqual([
      "InferenceStarted",
      "InferenceCompleted",
    ]);
    expect(committed[1]).toMatchObject({
      position: { seq: 2, subSeq: 0 },
      event: {
        eventId: EventId.make(INFERENCE_COMPLETED_EVENT_ID),
        type: "InferenceCompleted",
        createdAtMs: UnixEpochMillis.make(CREATED_AT_MS),
        payload: {
          runId: RunId.make(RUN_ID),
          turnId: TurnId.make(TURN_ID),
          inferenceId: InferenceId.make(INFERENCE_ID),
        },
      },
    });
  });
});
