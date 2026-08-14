import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";
import * as Tool from "effect/unstable/ai/Tool";
import { describe, expect, it } from "vite-plus/test";

import { EventId, RunId, SequenceNumber, SessionId, ToolCallId, TurnId } from "../types/core";
import { NonNegativeInt, ProviderPartId, UnixEpochMillis } from "../types/events";
import { EDASessionStore } from "./session-store";
import { EventFactory } from "./event-factory";
import { LiveEventBus } from "./live-event-bus";
import { sequentialUuidV7 } from "./id-generator";
import { SessionState } from "./session-state";
import { TurnRunner } from "./turn-runner";
import { makeEDAToolkit } from "./tool-registry";
import type { EDAModelToolkit } from "./tool-registry";
import { makeEdaTestLayer, testNowMs, type TestModelParts } from "../testkit/layers";

const SESSION_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const RUN_ID = "018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a";
const TURN_ID = "018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a";
const INFERENCE_ID = "018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a";
const TOOL_CALL_ID = "018f6bd5-2f2a-7b1e-8f9b-1f2e3d4c5b6a";
const TURN_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f2a-1f2e3d4c5b6a";
const INFERENCE_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f3a-1f2e3d4c5b6a";
const TEXT_EVENT_ID = "018f6bd5-2f2a-7b1e-8f4a-1f2e3d4c5b6a";
const INFERENCE_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f5a-1f2e3d4c5b6a";
const ASSISTANT_MESSAGE_ID = "018f6bd5-2f2a-7b1e-8f5c-1f2e3d4c5b6a";
const ASSISTANT_MESSAGE_EVENT_ID = "018f6bd5-2f2a-7b1e-8f5d-1f2e3d4c5b6a";
const TOOL_ASSISTANT_MESSAGE_ID = "018f6bd5-2f2a-7b1e-8f5e-1f2e3d4c5b6a";
const TOOL_ASSISTANT_MESSAGE_EVENT_ID = "018f6bd5-2f2a-7b1e-8f5f-1f2e3d4c5b6a";
const TURN_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f6a-1f2e3d4c5b6a";
const INFERENCE_FAILED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f7a-1f2e3d4c5b6a";
const TURN_FAILED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f8a-1f2e3d4c5b6a";
const TOOL_CREATED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f9a-1f2e3d4c5b6a";
const TOOL_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f9f-1f2e3d4c5b6a";
const TOOL_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f10-1f2e3d4c5b6a";
const TOOL_REJECTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f9c-1f2e3d4c5b6a";
const SECOND_INFERENCE_ID = "018f6bd5-2f2a-7b1e-bf2a-1f2e3d4c5b6a";
const SECOND_INFERENCE_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f3b-1f2e3d4c5b6a";
const SECOND_INFERENCE_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f5b-1f2e3d4c5b6a";
const SECOND_TOOL_CALL_ID = "018f6bd5-2f2a-7b1e-8f9d-1f2e3d4c5b6a";
const SECOND_TOOL_REJECTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f9e-1f2e3d4c5b6a";
const SECOND_TOOL_ASSISTANT_MESSAGE_ID = "018f6bd5-2f2a-7b1e-8f11-1f2e3d4c5b6a";
const SECOND_TOOL_ASSISTANT_MESSAGE_EVENT_ID = "018f6bd5-2f2a-7b1e-8f12-1f2e3d4c5b6a";
const CREATED_AT_MS = testNowMs;

const NoopParams = Schema.Struct({});
const LabelParams = Schema.Struct({ label: Schema.String });

const makeTestLayer = (
  ids: ReadonlyArray<string>,
  parts: TestModelParts,
  onStreamText?: (input: { readonly index: number; readonly prompt: Prompt.RawInput }) => void,
  toolkit?: EDAModelToolkit,
) =>
  makeEdaTestLayer({
    sessionId: SessionId.make(SESSION_ID),
    ids,
    parts,
    onStreamText,
    ...(toolkit === undefined ? { toolSchemas: new Map([["noop", NoopParams]]) } : { toolkit }),
    nowMs: CREATED_AT_MS,
  });

const NoopTool = Tool.make("noop", { parameters: NoopParams, success: Schema.Unknown });
const OrderedTool = Tool.make("ordered", { parameters: LabelParams, success: Schema.Unknown });

const noopToolkit = (result: unknown): EDAModelToolkit =>
  Effect.runSync(
    makeEDAToolkit([NoopTool], {
      noop: () => Effect.succeed(result),
    }),
  );

const turnInput = {
  runId: RunId.make(RUN_ID),
  turnId: TurnId.make(TURN_ID),
};

const commitTurnStarted = Effect.gen(function* () {
  const sessionState = yield* SessionState;
  const events = yield* EventFactory;
  return yield* sessionState.appendDurable(yield* events.turnStarted(turnInput));
});

const usage = () =>
  new Response.Usage({
    inputTokens: {
      uncached: undefined,
      total: 10,
      cacheRead: 3,
      cacheWrite: 4,
    },
    outputTokens: { total: 5, text: 3, reasoning: 2 },
  });

describe("TurnRunner", () => {
  it("commits turn boundaries around a completed inference without tools", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const liveBus = yield* LiveEventBus;
        const runner = yield* TurnRunner;
        const liveStream = yield* liveBus.subscribe();
        const liveFiber = yield* liveStream.pipe(
          Stream.take(6),
          Stream.runCollect,
          Effect.forkScoped,
        );
        const sessionState = yield* SessionState;
        const started = yield* commitTurnStarted;
        const result = yield* runner.runTurn({
          ...turnInput,
          prompt: "hello",
          eventSink: sessionState,
          started,
        });
        const liveEvents = yield* Fiber.join(liveFiber);
        return { liveEvents, result };
      }),
    ).pipe(
      Effect.provide(
        makeTestLayer(
          [
            TURN_STARTED_EVENT_ID,
            INFERENCE_ID,
            INFERENCE_STARTED_EVENT_ID,
            TEXT_EVENT_ID,
            INFERENCE_COMPLETED_EVENT_ID,
            ASSISTANT_MESSAGE_ID,
            ASSISTANT_MESSAGE_EVENT_ID,
            TURN_COMPLETED_EVENT_ID,
          ],
          stream,
        ),
      ),
    );

    const { liveEvents, result } = await Effect.runPromise(program);

    expect(result).toMatchObject({
      started: {
        event: {
          type: "TurnStarted",
          eventId: EventId.make(TURN_STARTED_EVENT_ID),
          sessionId: SessionId.make(SESSION_ID),
          createdAtMs: UnixEpochMillis.make(CREATED_AT_MS),
        },
      },
      inference: {
        started: {
          event: {
            type: "InferenceStarted",
            eventId: EventId.make(INFERENCE_STARTED_EVENT_ID),
            payload: { inferenceId: INFERENCE_ID },
          },
        },
      },
      outcome: {
        _tag: "TurnRunCompleted",
        committed: {
          event: {
            type: "TurnCompleted",
            eventId: EventId.make(TURN_COMPLETED_EVENT_ID),
            payload: {
              runId: RunId.make(RUN_ID),
              turnId: TurnId.make(TURN_ID),
              usage: {
                inputTokens: 10,
                cachedInputTokens: 3,
                outputTokens: 5,
                textTokens: 3,
                reasoningTokens: 2,
              },
            },
          },
        },
      },
    });
    expect(Array.from(liveEvents).map((event) => event.event.type)).toEqual([
      "TurnStarted",
      "InferenceStarted",
      "TextDelta",
      "AssistantMessageCommitted",
      "InferenceCompleted",
      "TurnCompleted",
    ]);
    expect(Array.from(liveEvents)[3]).toMatchObject({
      event: {
        type: "AssistantMessageCommitted",
        payload: { promptParts: [{ type: "text", text: "hello" }] },
      },
    });
  });

  it("commits TurnFailed from a failed inference", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
    ).pipe(Stream.concat(Stream.fail(new Error("provider failed"))));
    const program = Effect.gen(function* () {
      const runner = yield* TurnRunner;
      const sessionState = yield* SessionState;
      const started = yield* commitTurnStarted;
      return yield* runner.runTurn({
        ...turnInput,
        prompt: "hello",
        eventSink: sessionState,
        started,
      });
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [
            TURN_STARTED_EVENT_ID,
            INFERENCE_ID,
            INFERENCE_STARTED_EVENT_ID,
            TEXT_EVENT_ID,
            ASSISTANT_MESSAGE_ID,
            ASSISTANT_MESSAGE_EVENT_ID,
            INFERENCE_FAILED_EVENT_ID,
            TURN_FAILED_EVENT_ID,
          ],
          stream,
        ),
      ),
    );

    const result = await Effect.runPromise(program);

    expect(result).toMatchObject({
      outcome: {
        _tag: "TurnRunFailed",
        committed: {
          event: {
            type: "TurnFailed",
            eventId: EventId.make(TURN_FAILED_EVENT_ID),
            payload: { error: { message: "provider failed" } },
          },
        },
      },
    });
  });

  it("drops provider parts after a terminal inference and completes the turn once", async () => {
    const stream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
      Response.makePart("text-delta", { id: "text-1", delta: "too late" }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* TurnRunner;
      const store = yield* EDASessionStore;
      const sessionState = yield* SessionState;
      const started = yield* commitTurnStarted;
      const result = yield* runner.runTurn({
        ...turnInput,
        prompt: "hello",
        eventSink: sessionState,
        started,
      });
      const committed = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { committed: Array.from(committed), result };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [
            TURN_STARTED_EVENT_ID,
            INFERENCE_ID,
            INFERENCE_STARTED_EVENT_ID,
            INFERENCE_COMPLETED_EVENT_ID,
            TURN_COMPLETED_EVENT_ID,
          ],
          stream,
        ),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);

    expect(result).toMatchObject({
      inference: {
        partsRecorded: 1,
        terminal: { _tag: "InferenceRunFinished" },
      },
      outcome: { _tag: "TurnRunCompleted" },
    });
    expect(committed.map((entry) => entry.event.type)).toEqual([
      "TurnStarted",
      "InferenceStarted",
      "InferenceCompleted",
      "TurnCompleted",
    ]);
    expect(committed[3]).toMatchObject({
      event: {
        type: "TurnCompleted",
        eventId: EventId.make(TURN_COMPLETED_EVENT_ID),
        createdAtMs: UnixEpochMillis.make(CREATED_AT_MS),
        payload: {
          runId: RunId.make(RUN_ID),
          turnId: TurnId.make(TURN_ID),
        },
      },
    });
  });

  it("commits rejected tool feedback and completes the turn for runtime continuation", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const rejectedStream = Stream.make(
      Response.makePart("tool-call", {
        id: "bad-tool-1",
        name: "mystery",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const correctedStream = Stream.make(
      Response.makePart("text-delta", { id: "text-2", delta: "corrected" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* TurnRunner;
      const store = yield* EDASessionStore;
      const sessionState = yield* SessionState;
      const started = yield* commitTurnStarted;
      const result = yield* runner.runTurn({
        ...turnInput,
        prompt: "hello",
        eventSink: sessionState,
        started,
      });
      const committed = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { committed: Array.from(committed), result };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [
            TURN_STARTED_EVENT_ID,
            INFERENCE_ID,
            INFERENCE_STARTED_EVENT_ID,
            TOOL_CALL_ID,
            INFERENCE_COMPLETED_EVENT_ID,
            TOOL_REJECTED_EVENT_ID,
            TOOL_ASSISTANT_MESSAGE_ID,
            TOOL_ASSISTANT_MESSAGE_EVENT_ID,
            SECOND_INFERENCE_ID,
            SECOND_INFERENCE_STARTED_EVENT_ID,
            TEXT_EVENT_ID,
            SECOND_INFERENCE_COMPLETED_EVENT_ID,
            ASSISTANT_MESSAGE_ID,
            ASSISTANT_MESSAGE_EVENT_ID,
            TURN_COMPLETED_EVENT_ID,
          ],
          [rejectedStream, correctedStream],
          ({ prompt }) => prompts.push(prompt),
        ),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);
    const rejected = committed.find((entry) => entry.event.type === "ToolCallRejected");
    const feedback = (
      rejected?.event.payload as
        | { readonly promptPart?: { readonly result?: { readonly modelFeedback?: string } } }
        | undefined
    )?.promptPart?.result?.modelFeedback;

    expect(result).toMatchObject({
      inference: {
        started: { event: { type: "InferenceStarted", payload: { inferenceId: INFERENCE_ID } } },
      },
      outcome: { _tag: "TurnRunCompleted" },
    });
    expect(committed.map((entry) => entry.event.type)).toEqual([
      "TurnStarted",
      "InferenceStarted",
      "AssistantMessageCommitted",
      "ToolCallRejected",
      "InferenceCompleted",
      "TurnCompleted",
    ]);
    expect(rejected).toMatchObject({
      event: {
        type: "ToolCallRejected",
        payload: {
          toolCallId: ToolCallId.make(TOOL_CALL_ID),
          promptPart: {
            id: ProviderPartId.make("bad-tool-1"),
            result: { reason: "unknown-tool" },
          },
        },
      },
    });
    expect(feedback).toEqual(expect.stringContaining("Tool mystery arguments were rejected"));
    expect(prompts).toHaveLength(1);
  });

  it("does not consume a corrective stream inside the same turn after rejected-only output", async () => {
    const firstRejectedStream = Stream.make(
      Response.makePart("tool-call", {
        id: "bad-tool-1",
        name: "mystery",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const secondRejectedStream = Stream.make(
      Response.makePart("tool-call", {
        id: "bad-tool-2",
        name: "mystery",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* TurnRunner;
      const store = yield* EDASessionStore;
      const sessionState = yield* SessionState;
      const started = yield* commitTurnStarted;
      const result = yield* runner.runTurn({
        ...turnInput,
        prompt: "hello",
        eventSink: sessionState,
        started,
      });
      const committed = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { committed: Array.from(committed), result };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [
            TURN_STARTED_EVENT_ID,
            INFERENCE_ID,
            INFERENCE_STARTED_EVENT_ID,
            TOOL_CALL_ID,
            INFERENCE_COMPLETED_EVENT_ID,
            TOOL_REJECTED_EVENT_ID,
            TOOL_ASSISTANT_MESSAGE_ID,
            TOOL_ASSISTANT_MESSAGE_EVENT_ID,
            SECOND_INFERENCE_ID,
            SECOND_INFERENCE_STARTED_EVENT_ID,
            SECOND_TOOL_CALL_ID,
            SECOND_INFERENCE_COMPLETED_EVENT_ID,
            SECOND_TOOL_REJECTED_EVENT_ID,
            SECOND_TOOL_ASSISTANT_MESSAGE_ID,
            SECOND_TOOL_ASSISTANT_MESSAGE_EVENT_ID,
            TURN_FAILED_EVENT_ID,
          ],
          [firstRejectedStream, secondRejectedStream],
        ),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);

    expect(result).toMatchObject({
      inference: {
        started: { event: { type: "InferenceStarted", payload: { inferenceId: INFERENCE_ID } } },
      },
      outcome: { _tag: "TurnRunCompleted" },
    });
    expect(committed.map((entry) => entry.event.type)).toEqual([
      "TurnStarted",
      "InferenceStarted",
      "AssistantMessageCommitted",
      "ToolCallRejected",
      "InferenceCompleted",
      "TurnCompleted",
    ]);
    expect(committed.filter((entry) => entry.event.type === "ToolCallRejected")).toHaveLength(1);
  });

  it("executes valid tool calls before correcting rejected calls in a mixed batch", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const mixedStream = Stream.make(
      Response.makePart("tool-call", {
        id: "tool-call-1",
        name: "noop",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("tool-call", {
        id: "bad-tool-1",
        name: "mystery",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const correctedStream = Stream.make(
      Response.makePart("text-delta", { id: "text-2", delta: "done" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* TurnRunner;
      const store = yield* EDASessionStore;
      const sessionState = yield* SessionState;
      const started = yield* commitTurnStarted;
      const result = yield* runner.runTurn({
        ...turnInput,
        prompt: "hello",
        eventSink: sessionState,
        started,
      });
      const committed = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { committed: Array.from(committed), result };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [
            TURN_STARTED_EVENT_ID,
            INFERENCE_ID,
            INFERENCE_STARTED_EVENT_ID,
            TOOL_CALL_ID,
            SECOND_TOOL_CALL_ID,
            INFERENCE_COMPLETED_EVENT_ID,
            TOOL_CREATED_EVENT_ID,
            TOOL_REJECTED_EVENT_ID,
            TOOL_ASSISTANT_MESSAGE_ID,
            TOOL_ASSISTANT_MESSAGE_EVENT_ID,
            TOOL_STARTED_EVENT_ID,
            TOOL_COMPLETED_EVENT_ID,
            SECOND_INFERENCE_ID,
            SECOND_INFERENCE_STARTED_EVENT_ID,
            TEXT_EVENT_ID,
            SECOND_INFERENCE_COMPLETED_EVENT_ID,
            ASSISTANT_MESSAGE_ID,
            ASSISTANT_MESSAGE_EVENT_ID,
            TURN_COMPLETED_EVENT_ID,
          ],
          [mixedStream, correctedStream],
          ({ prompt }) => prompts.push(prompt),
          noopToolkit({ ok: true }),
        ),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);
    expect(result).toMatchObject({
      inference: {
        started: { event: { type: "InferenceStarted", payload: { inferenceId: INFERENCE_ID } } },
      },
      outcome: { _tag: "TurnRunCompleted" },
    });
    expect(committed.map((entry) => entry.event.type)).toEqual([
      "TurnStarted",
      "InferenceStarted",
      "AssistantMessageCommitted",
      "ToolCallCreated",
      "ToolCallRejected",
      "InferenceCompleted",
      "ToolCallStarted",
      "ToolCallCompleted",
      "TurnCompleted",
    ]);
    expect(committed.filter((entry) => entry.event.type === "ToolCallCreated")).toHaveLength(1);
    expect(committed.filter((entry) => entry.event.type === "ToolCallCompleted")).toHaveLength(1);
    expect(committed.filter((entry) => entry.event.type === "ToolCallRejected")).toHaveLength(1);
    expect(prompts).toHaveLength(1);
  });

  it("executes valid tool decisions and completes the turn for runtime continuation", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const stream = Stream.make(
      Response.makePart("tool-call", {
        id: "tool-call-1",
        name: "noop",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const continuation = Stream.make(
      Response.makePart("text-delta", { id: "text-2", delta: "done" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const liveBus = yield* LiveEventBus;
        const runner = yield* TurnRunner;
        const liveStream = yield* liveBus.subscribe();
        const liveFiber = yield* liveStream.pipe(
          Stream.take(8),
          Stream.runCollect,
          Effect.forkScoped,
        );
        const sessionState = yield* SessionState;
        const started = yield* commitTurnStarted;
        const result = yield* runner.runTurn({
          ...turnInput,
          prompt: "hello",
          eventSink: sessionState,
          started,
        });
        const liveEvents = yield* Fiber.join(liveFiber);
        return { liveEvents, result };
      }),
    ).pipe(
      Effect.provide(
        makeTestLayer(
          [
            TURN_STARTED_EVENT_ID,
            INFERENCE_ID,
            INFERENCE_STARTED_EVENT_ID,
            TOOL_CALL_ID,
            INFERENCE_COMPLETED_EVENT_ID,
            TOOL_CREATED_EVENT_ID,
            TOOL_ASSISTANT_MESSAGE_ID,
            TOOL_ASSISTANT_MESSAGE_EVENT_ID,
            TOOL_STARTED_EVENT_ID,
            TOOL_COMPLETED_EVENT_ID,
            SECOND_INFERENCE_ID,
            SECOND_INFERENCE_STARTED_EVENT_ID,
            TEXT_EVENT_ID,
            SECOND_INFERENCE_COMPLETED_EVENT_ID,
            ASSISTANT_MESSAGE_ID,
            ASSISTANT_MESSAGE_EVENT_ID,
            TURN_COMPLETED_EVENT_ID,
          ],
          [stream, continuation],
          ({ prompt }) => prompts.push(prompt),
          noopToolkit({ ok: true }),
        ),
      ),
    );

    const { liveEvents, result } = await Effect.runPromise(program);

    expect(result).toMatchObject({
      inference: {
        started: { event: { type: "InferenceStarted", payload: { inferenceId: INFERENCE_ID } } },
      },
      outcome: { _tag: "TurnRunCompleted" },
    });
    expect(Array.from(liveEvents).map((event) => event.event.type)).toEqual([
      "TurnStarted",
      "InferenceStarted",
      "AssistantMessageCommitted",
      "ToolCallCreated",
      "InferenceCompleted",
      "ToolCallStarted",
      "ToolCallCompleted",
      "TurnCompleted",
    ]);
    expect(prompts).toHaveLength(1);
  });

  it("executes one tool-producing turn and leaves additional streams for later turns", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const firstTool = Stream.make(
      Response.makePart("tool-call", {
        id: "tool-call-1",
        name: "noop",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const secondTool = Stream.make(
      Response.makePart("tool-call", {
        id: "tool-call-2",
        name: "noop",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const final = Stream.make(
      Response.makePart("text-delta", { id: "text-final", delta: "done" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* TurnRunner;
      const store = yield* EDASessionStore;
      const sessionState = yield* SessionState;
      const started = yield* commitTurnStarted;
      const result = yield* runner.runTurn({
        ...turnInput,
        prompt: "hello",
        eventSink: sessionState,
        started,
        maxToolCallsPerRun: NonNegativeInt.make(2),
      });
      const committed = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { committed: Array.from(committed), result };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          Array.from({ length: 60 }, (_, index) => sequentialUuidV7(index + 1)),
          [firstTool, secondTool, final],
          ({ prompt }) => prompts.push(prompt),
          noopToolkit({ ok: true }),
        ),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);

    expect(result.outcome).toMatchObject({ _tag: "TurnRunCompleted" });
    expect(committed.filter((entry) => entry.event.type === "ToolCallCreated")).toHaveLength(1);
    expect(committed.filter((entry) => entry.event.type === "ToolCallCompleted")).toHaveLength(1);
    expect(prompts).toHaveLength(1);
  });

  it("fails safely when maxToolCallsPerRun is exhausted", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const firstTool = Stream.make(
      Response.makePart("tool-call", {
        id: "tool-call-1",
        name: "noop",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("tool-call", {
        id: "tool-call-2",
        name: "noop",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* TurnRunner;
      const store = yield* EDASessionStore;
      const sessionState = yield* SessionState;
      const started = yield* commitTurnStarted;
      const result = yield* runner.runTurn({
        ...turnInput,
        prompt: "hello",
        eventSink: sessionState,
        started,
        maxToolCallsPerRun: NonNegativeInt.make(1),
      });
      const committed = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { committed: Array.from(committed), result };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          Array.from({ length: 60 }, (_, index) => sequentialUuidV7(index + 1)),
          [firstTool],
          ({ prompt }) => prompts.push(prompt),
          noopToolkit({ ok: true }),
        ),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);
    const turnFailed = committed.find((entry) => entry.event.type === "TurnFailed");

    expect(result.outcome).toMatchObject({ _tag: "TurnRunFailed" });
    expect(committed.filter((entry) => entry.event.type === "ToolCallCompleted")).toHaveLength(0);
    expect(committed.filter((entry) => entry.event.type === "ToolCallFailed")).toHaveLength(2);
    expect(committed.find((entry) => entry.event.type === "ToolCallFailed")).toMatchObject({
      event: {
        payload: {
          promptPart: {
            id: "tool-call-1",
            isFailure: true,
            result: { message: expect.stringContaining("maxToolCallsPerRun exceeded") },
          },
        },
      },
    });
    expect(turnFailed).toMatchObject({
      event: {
        payload: {
          error: { message: expect.stringContaining("maxToolCallsPerRun exceeded") },
        },
      },
    });
    expect(prompts).toHaveLength(1);
  });

  it("persists and echoes assistant preamble parts before tool continuations", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "I will check." }),
      Response.makePart("reasoning-delta", { id: "reasoning-1", delta: "I need a tool." }),
      Response.makePart("tool-call", {
        id: "tool-call-1",
        name: "noop",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const continuation = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const runner = yield* TurnRunner;
        const store = yield* EDASessionStore;
        const sessionState = yield* SessionState;
        const started = yield* commitTurnStarted;
        yield* runner.runTurn({
          ...turnInput,
          prompt: "hello",
          eventSink: sessionState,
          started,
        });
        const committed = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
        return Array.from(committed);
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: [stream, continuation],
          onStreamText: ({ prompt }) => prompts.push(prompt),
          toolSchemas: new Map([["noop", NoopParams]]),
          nowMs: CREATED_AT_MS,
          toolkit: noopToolkit({ ok: true }),
        }),
      ),
    );

    const committed = await Effect.runPromise(program);
    const eventTypes = committed.map((entry) => entry.event.type);
    const assistantMessages = committed.filter(
      (entry) => entry.event.type === "AssistantMessageCommitted",
    );

    expect(assistantMessages).toHaveLength(1);
    expect(assistantMessages[0]?.event.payload).toMatchObject({
      promptParts: [
        { type: "text", text: "I will check." },
        { type: "reasoning", text: "I need a tool." },
        { type: "tool-call", id: "tool-call-1", name: "noop", params: {} },
      ],
    });
    expect(eventTypes.indexOf("AssistantMessageCommitted")).toBeLessThan(
      eventTypes.indexOf("ToolCallCreated"),
    );
    expect(eventTypes.indexOf("AssistantMessageCommitted")).toBeLessThan(
      eventTypes.indexOf("InferenceCompleted"),
    );
    expect(eventTypes.indexOf("ToolCallCreated")).toBeLessThan(
      eventTypes.indexOf("InferenceCompleted"),
    );
    expect(prompts).toHaveLength(1);
  });

  it("fails provider-executed tool calls without rerunning or projecting tools", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "I already checked." }),
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
        result: { provider: "ok" },
        encodedResult: { provider: "ok" },
        providerExecuted: true,
        preliminary: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const runner = yield* TurnRunner;
      const store = yield* EDASessionStore;
      const sessionState = yield* SessionState;
      const started = yield* commitTurnStarted;
      const result = yield* runner.runTurn({
        ...turnInput,
        prompt: "hello",
        eventSink: sessionState,
        started,
      });
      const committed = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { committed: Array.from(committed), result };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [
            TURN_STARTED_EVENT_ID,
            INFERENCE_ID,
            INFERENCE_STARTED_EVENT_ID,
            TEXT_EVENT_ID,
            TOOL_CALL_ID,
            INFERENCE_COMPLETED_EVENT_ID,
            TOOL_CREATED_EVENT_ID,
            TOOL_COMPLETED_EVENT_ID,
            ASSISTANT_MESSAGE_ID,
            ASSISTANT_MESSAGE_EVENT_ID,
            SECOND_INFERENCE_ID,
            SECOND_INFERENCE_STARTED_EVENT_ID,
            SECOND_INFERENCE_COMPLETED_EVENT_ID,
            TURN_COMPLETED_EVENT_ID,
          ],
          [stream],
          ({ prompt }) => prompts.push(prompt),
          noopToolkit({ shouldNotRun: true }),
        ),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);
    const eventTypes = committed.map((entry) => entry.event.type);

    expect(result.outcome).toMatchObject({ _tag: "TurnRunFailed" });
    expect(eventTypes).toEqual([
      "TurnStarted",
      "InferenceStarted",
      "AssistantPartialCommitted",
      "InferenceFailed",
      "TurnFailed",
    ]);
    expect(eventTypes).not.toContain("ToolCallCreated");
    expect(eventTypes).not.toContain("ToolCallStarted");
    expect(eventTypes).not.toContain("ToolCallCompleted");
    expect(committed.find((entry) => entry.event.type === "InferenceFailed")).toMatchObject({
      event: {
        payload: {
          error: {
            message: expect.stringContaining("Provider-executed tool calls are unsupported"),
          },
        },
      },
    });
    expect(prompts).toHaveLength(1);
  });

  it("runs tool calls concurrently while preserving model order in the continuation prompt", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const slowStarted = await Effect.runPromise(Deferred.make<void>());
    const fastStarted = await Effect.runPromise(Deferred.make<void>());
    const slowRelease = await Effect.runPromise(Deferred.make<void>());
    const fastRelease = await Effect.runPromise(Deferred.make<void>());
    const stream = Stream.make(
      Response.makePart("tool-call", {
        id: "tool-call-slow",
        name: "ordered",
        params: { label: "slow" },
        providerExecuted: false,
      }),
      Response.makePart("tool-call", {
        id: "tool-call-fast",
        name: "ordered",
        params: { label: "fast" },
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const continuation = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const toolkit = Effect.runSync(
      makeEDAToolkit([OrderedTool], {
        ordered: (params) =>
          Effect.gen(function* () {
            const label = (params as { readonly label: "slow" | "fast" }).label;
            if (label === "slow") {
              yield* Deferred.succeed(slowStarted, undefined);
              yield* Deferred.await(slowRelease);
              return { label };
            }
            yield* Deferred.succeed(fastStarted, undefined);
            yield* Deferred.await(fastRelease);
            return { label };
          }),
      }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const liveBus = yield* LiveEventBus;
        const runner = yield* TurnRunner;
        const durableStore = yield* EDASessionStore;
        const sessionState = yield* SessionState;
        const started = yield* commitTurnStarted;
        const liveStream = yield* liveBus.subscribe();
        const firstCompletionFiber = yield* liveStream.pipe(
          Stream.filter((event) => event.event.type === "ToolCallCompleted"),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );
        const runFiber = yield* runner
          .runTurn({ ...turnInput, prompt: "hello", eventSink: sessionState, started })
          .pipe(Effect.forkScoped);

        yield* Deferred.await(slowStarted);
        yield* Deferred.await(fastStarted);
        yield* Deferred.succeed(fastRelease, undefined);
        const firstCompletion = Array.from(yield* Fiber.join(firstCompletionFiber))[0];
        yield* Deferred.succeed(slowRelease, undefined);
        const result = yield* Fiber.join(runFiber);
        const committed = yield* durableStore.eventsAfter(SequenceNumber.make(0)).pipe(
          Stream.runCollect,
          Effect.map((events) => Array.from(events)),
        );
        return { committed, firstCompletion, result };
      }),
    ).pipe(
      Effect.provide(
        makeTestLayer(
          Array.from({ length: 30 }, (_, index) => sequentialUuidV7(index + 1)),
          [stream, continuation],
          ({ prompt }) => prompts.push(prompt),
          toolkit,
        ),
      ),
    );

    const { committed, firstCompletion, result } = await Effect.runPromise(program);
    const completedResults = committed.flatMap((entry) =>
      entry.event.type === "ToolCallCompleted"
        ? [(entry.event.payload.promptPart.result as { readonly label: string }).label]
        : [],
    );
    const assistantMessage = committed.find(
      (entry) => entry.event.type === "AssistantMessageCommitted",
    );

    expect(firstCompletion).toMatchObject({
      event: { type: "ToolCallCompleted", payload: { promptPart: { result: { label: "fast" } } } },
    });
    expect(completedResults).toEqual(["fast", "slow"]);
    expect(result.outcome).toMatchObject({ _tag: "TurnRunCompleted" });
    expect(prompts).toHaveLength(1);
    const promptJson = JSON.stringify(assistantMessage?.event.payload);
    expect(promptJson.indexOf("tool-call-slow")).toBeLessThan(promptJson.indexOf("tool-call-fast"));
    expect(promptJson.indexOf('"label":"slow"')).toBeLessThan(promptJson.indexOf('"label":"fast"'));
  });
});
