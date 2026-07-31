import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";
import { describe, expect, it } from "vite-plus/test";

import { SubmitMessageCommand } from "../types/commands";
import { CommandId, EventId, SessionId, SequenceNumber } from "../types/core";
import {
  DurableEventEnvelope,
  EventNamespace,
  EventType,
  UnixEpochMillis,
  schemaV1,
  assistantMessageCommittedEventType,
  textDeltaEventType,
} from "../types/events";
import { EDARuntime, type EDARuntimeShape } from "./runtime";
import { sequentialUuidV7 } from "./id-generator";
import { EDAKeepAlive } from "./keep-alive";
import { getEDAReducerState, type EDAReducer } from "./reducer-registry";
import type { EDASink } from "./sink-registry";
import { EDASinkName, SinkCheckpointStore } from "./sink-checkpoint-store";
import { makeEdaTestLayer } from "../testkit/layers";
import { makeEdaExportingTracer, type EDAExportedSpan } from "./tracing";

const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const SECOND_COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1c-1f2e3d4c5b6a");
const EXAMPLE_INBOX = EventNamespace.make("example.inbox");

const modelSelection = { provider: "test", modelId: "test-model" };

const command = (commandId: CommandId, text: string) =>
  new SubmitMessageCommand({
    commandId,
    disposition: "queue",
    content: [Prompt.textPart({ text })],
  });

const finishStream = Stream.make(
  Response.makePart("text-delta", { id: "text-1", delta: "assistant reply" }),
  Response.makePart("finish", {
    reason: "stop",
    usage: new Response.Usage({
      inputTokens: { uncached: undefined, total: 1, cacheRead: undefined, cacheWrite: undefined },
      outputTokens: { total: 1, text: undefined, reasoning: undefined },
    }),
    response: undefined,
  }),
);

const appEvent = (input: {
  readonly id: number;
  readonly type: string;
  readonly payload: unknown;
}) =>
  DurableEventEnvelope.make({
    namespace: EXAMPLE_INBOX,
    type: EventType.make(input.type),
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(sequentialUuidV7(input.id)),
    sessionId: SESSION_ID,
    createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
    payload: input.payload,
  });

const replayAll = (runtime: EDARuntimeShape) =>
  Effect.gen(function* () {
    const snapshot = yield* runtime.snapshot();
    const replay = yield* runtime.eventsAfter(SequenceNumber.make(0));
    return Array.from(yield* replay.pipe(Stream.take(snapshot.state.lastSeq), Stream.runCollect));
  });

const waitUntil = (label: string, predicate: () => Effect.Effect<boolean>): Effect.Effect<void> =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 100; attempt++) {
      if (yield* predicate()) {
        return;
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(new Error(`Timed out waiting for ${label}`));
  });

const waitUntilSync = (label: string, predicate: () => boolean): Effect.Effect<void> =>
  waitUntil(label, () => Effect.sync(predicate));

const waitUntilWithSleep = (label: string, predicate: () => boolean): Effect.Effect<void> =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 250; attempt++) {
      if (predicate()) {
        return;
      }
      yield* Effect.sleep("25 millis");
    }
    return yield* Effect.die(new Error(`Timed out waiting for ${label}`));
  });

describe("EDASinkRegistry", () => {
  it("submits commands and app durable events in one ordered batch", async () => {
    const inbound = appEvent({
      id: 9_001,
      type: "ExternalMessageReceived",
      payload: { commandId: COMMAND_ID, externalMessageId: "message-1" },
    });

    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        const committed = yield* runtime.submit([command(COMMAND_ID, "hello from inbox"), inbound]);
        yield* runtime.blockOnCommand(COMMAND_ID, committed[0]!.position.seq);
        const events = yield* replayAll(runtime);
        return { committed, events };
      }),
    ).pipe(
      Effect.provide(
        EDARuntime.Live({ modelSelection }).pipe(
          Layer.provideMerge(
            makeEdaTestLayer({
              sessionId: SESSION_ID,
              parts: finishStream,
            }),
          ),
        ),
      ),
    );

    const { committed, events } = await Effect.runPromise(program);

    expect(committed.map((entry) => entry.event.type)).toEqual([
      "CommandAdmitted",
      "UserMessageSubmitted",
      "ExternalMessageReceived",
    ]);
    expect(events.map((entry) => entry.event.type).slice(0, 3)).toEqual([
      "CommandAdmitted",
      "UserMessageSubmitted",
      "ExternalMessageReceived",
    ]);
  });

  it("exposes custom app reducers for command-adjacent app events", async () => {
    type InboundReducerState = {
      readonly externalMessageByCommandId: ReadonlyMap<string, string>;
    };
    const inboundReducer: EDAReducer<InboundReducerState> = {
      name: "example.inbox",
      initial: { externalMessageByCommandId: new Map() },
      stateSchema: Schema.Struct({
        externalMessageByCommandId: Schema.ReadonlyMap(Schema.String, Schema.String),
      }),
      reduce: (state, event) => {
        if (event.event.type !== EventType.make("ExternalMessageReceived")) {
          return state;
        }
        const payload = event.event.payload as {
          readonly commandId: string;
          readonly externalMessageId: string;
        };
        return {
          externalMessageByCommandId: new Map(state.externalMessageByCommandId).set(
            payload.commandId,
            payload.externalMessageId,
          ),
        };
      },
    };
    const observed: Array<string | undefined> = [];
    const sink: EDASink = {
      name: "test.reducer-reader",
      durable: {
        interests: ["TurnCompleted"],
        process: (batch) =>
          Effect.sync(() => {
            observed.push(
              getEDAReducerState(
                batch.reducerStates,
                inboundReducer,
              ).externalMessageByCommandId.get(COMMAND_ID),
            );
          }),
      },
    };
    const inbound = appEvent({
      id: 9_003,
      type: "ExternalMessageReceived",
      payload: { commandId: COMMAND_ID, externalMessageId: "message-3" },
    });

    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        const committed = yield* runtime.submit([command(COMMAND_ID, "hello"), inbound]);
        yield* runtime.blockOnCommand(COMMAND_ID, committed[0]!.position.seq);
        yield* waitUntilSync("reducer sink observation", () => observed.length > 0);
      }),
    ).pipe(
      Effect.provide(
        EDARuntime.Live({ modelSelection }).pipe(
          Layer.provideMerge(
            makeEdaTestLayer({
              sessionId: SESSION_ID,
              parts: finishStream,
              reducers: [inboundReducer],
              sinks: [sink],
            }),
          ),
        ),
      ),
    );

    await Effect.runPromise(program);

    expect(observed).toEqual(["message-3"]);
  });

  it("does not block command completion on slow durable sinks", async () => {
    let activeLeases = 0;
    let activeDuringSink = 0;
    let sinkStarted = false;
    const sink: EDASink = {
      name: "test.slow-final-reply-sink",
      durable: {
        interests: ["AssistantMessageCommitted"],
        process: () =>
          Effect.gen(function* () {
            sinkStarted = true;
            return yield* Effect.never;
          }),
      },
    };

    const keepAliveLayer = Layer.succeed(EDAKeepAlive, {
      withActiveWork: (_label, effect) =>
        Effect.acquireUseRelease(
          Effect.sync(() => {
            activeLeases += 1;
          }),
          () => effect,
          () =>
            Effect.sync(() => {
              activeLeases -= 1;
            }),
        ),
    });

    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        const terminal = yield* runtime.submitAndBlock(command(COMMAND_ID, "hello"));
        yield* waitUntilSync("slow sink startup", () => sinkStarted && activeLeases > 0);
        activeDuringSink = activeLeases;
        return terminal;
      }),
    ).pipe(
      Effect.provide(
        EDARuntime.Live({ modelSelection }).pipe(
          Layer.provideMerge(
            makeEdaTestLayer({
              sessionId: SESSION_ID,
              keepAliveLayer,
              parts: finishStream,
              sinks: [sink],
            }),
          ),
        ),
      ),
    );

    const terminal = await Effect.runPromise(program);

    expect(terminal.event.type).toBe("CommandCompleted");
    expect(activeDuringSink).toBe(1);
    expect(activeLeases).toBe(0);
  });

  it("serializes durable and ephemeral callbacks for mixed sinks", async () => {
    const observed: string[] = [];
    const sink: EDASink = {
      name: "test.mixed-live-slack-like-sink",
      durable: {
        interests: [assistantMessageCommittedEventType],
        process: () =>
          Effect.sync(() => {
            observed.push("durable-final");
          }),
      },
      ephemeral: {
        interests: [textDeltaEventType],
        process: () =>
          Effect.gen(function* () {
            observed.push("ephemeral-start");
            yield* Effect.sleep("50 millis");
            observed.push("ephemeral-end");
          }),
      },
    };

    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        yield* runtime.submitAndBlock(command(COMMAND_ID, "hello"));
        yield* waitUntilWithSleep("mixed sink durable final", () =>
          observed.includes("durable-final"),
        );
      }),
    ).pipe(
      Effect.provide(
        EDARuntime.Live({ modelSelection }).pipe(
          Layer.provideMerge(
            makeEdaTestLayer({
              sessionId: SESSION_ID,
              clock: "live",
              parts: finishStream,
              sinks: [sink],
            }),
          ),
        ),
      ),
    );

    await Effect.runPromise(program);

    expect(observed).toEqual(["ephemeral-start", "ephemeral-end", "durable-final"]);
  });

  it("processes a filtered durable sink on TurnCompleted and advances through uninterested events", async () => {
    const processed: Array<ReadonlyArray<string>> = [];
    const sink: EDASink = {
      name: "test.turn-summary-sink",
      durable: {
        interests: ["TurnCompleted"],
        process: (batch) =>
          Effect.sync(() => {
            processed.push(batch.events.map((entry) => entry.event.type));
          }),
      },
    };

    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        yield* runtime.submitAndBlock(command(COMMAND_ID, "hello"));
        yield* waitUntilSync("turn summary sink", () => processed.length > 0);
      }),
    ).pipe(
      Effect.provide(
        EDARuntime.Live({ modelSelection }).pipe(
          Layer.provideMerge(
            makeEdaTestLayer({
              sessionId: SESSION_ID,
              parts: finishStream,
              sinks: [sink],
            }),
          ),
        ),
      ),
    );

    await Effect.runPromise(program);

    expect(processed).toEqual([["TurnCompleted"]]);
  });

  it("advances through uninterested events without emitting an empty drain span", async () => {
    const processed: Array<ReadonlyArray<string>> = [];
    const spans: Array<EDAExportedSpan> = [];
    const sink: EDASink = {
      name: "test.external-marker-sink",
      durable: {
        interests: ["ExternalMarker"],
        process: (batch) =>
          Effect.sync(() => {
            processed.push(batch.allEvents.map((entry) => entry.event.type));
          }),
      },
    };

    const LayerWithTracing = Layer.merge(
      makeEdaTestLayer({
        sessionId: SESSION_ID,
        clock: "live",
        parts: finishStream,
        sinks: [sink],
      }),
      Layer.succeed(
        Tracer.Tracer,
        makeEdaExportingTracer((span) => spans.push(span)),
      ),
    );
    const RuntimeLayer = EDARuntime.Live({ modelSelection }).pipe(
      Layer.provideMerge(LayerWithTracing),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        const checkpointStore = yield* SinkCheckpointStore;
        yield* runtime.submitAndBlock(command(COMMAND_ID, "hello"));
        const afterFirst = yield* runtime.snapshot();
        yield* waitUntil("uninterested cursor advance", () =>
          checkpointStore
            .load(EDASinkName.make(sink.name))
            .pipe(Effect.map((checkpoint) => checkpoint.afterSeq >= afterFirst.state.lastSeq)),
        );
        const drainSpansBeforeInterestedEvent = spans.filter(
          (span) => span.name === "agent.sink.drain",
        ).length;
        yield* runtime.submit([
          appEvent({ id: 9_004, type: "ExternalMarker", payload: { marker: "ready" } }),
        ]);
        yield* waitUntilSync("external marker sink", () => processed.length > 0);
        return drainSpansBeforeInterestedEvent;
      }),
    ).pipe(Effect.provide(RuntimeLayer));

    const drainSpansBeforeInterestedEvent = await Effect.runPromise(program);

    expect(drainSpansBeforeInterestedEvent).toBe(0);
    expect(processed).toEqual([["ExternalMarker"]]);
  });

  it("retries bubbled durable sink failures without advancing the cursor first", async () => {
    let calls = 0;
    const throughSeqs: Array<SequenceNumber> = [];
    const sink: EDASink = {
      name: "test.retrying-marker-sink",
      durable: {
        interests: ["ExternalMarker"],
        process: (batch) =>
          Effect.gen(function* () {
            calls += 1;
            throughSeqs.push(batch.throughSeq);
            if (calls === 1) {
              return yield* Effect.fail(new Error("transient sink failure"));
            }
          }),
      },
    };

    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        const checkpointStore = yield* SinkCheckpointStore;
        const committed = yield* runtime.submit([
          appEvent({ id: 9_005, type: "ExternalMarker", payload: { marker: "retry" } }),
        ]);
        const markerSeq = committed[0]!.position.seq;
        yield* waitUntilSync("first failed sink attempt", () => calls >= 1);
        const cursorAfterFailure = (yield* checkpointStore.load(EDASinkName.make(sink.name)))
          .afterSeq;
        yield* waitUntilWithSleep("successful sink retry", () => calls >= 2);
        yield* waitUntil("cursor after successful retry", () =>
          checkpointStore
            .load(EDASinkName.make(sink.name))
            .pipe(Effect.map((checkpoint) => checkpoint.afterSeq >= markerSeq)),
        );
        const cursorAfterSuccess = (yield* checkpointStore.load(EDASinkName.make(sink.name)))
          .afterSeq;
        return { cursorAfterFailure, cursorAfterSuccess, markerSeq };
      }),
    ).pipe(
      Effect.provide(
        EDARuntime.Live({ modelSelection }).pipe(
          Layer.provideMerge(
            makeEdaTestLayer({
              sessionId: SESSION_ID,
              clock: "live",
              sinks: [sink],
            }),
          ),
        ),
      ),
    );

    const result = await Effect.runPromise(program);

    expect(calls).toBe(2);
    expect(result.cursorAfterFailure).toBe(0);
    expect(result.cursorAfterSuccess).toBe(result.markerSeq);
    expect(throughSeqs).toEqual([result.markerSeq, result.markerSeq]);
  });

  it("persists typed sink state independently from cursor advancement", async () => {
    const CounterCheckpoint = Schema.Struct({ count: Schema.Number });
    const observed: Array<number> = [];
    const sink: EDASink = {
      name: "test.checkpoint-state",
      durable: {
        interests: ["ExternalMarker"],
        process: (_batch, ctx) =>
          Effect.gen(function* () {
            const current = yield* ctx.checkpoint.get(CounterCheckpoint, { count: 0 });
            observed.push(current.count);
            yield* ctx.checkpoint.save(CounterCheckpoint, { count: current.count + 1 });
          }),
      },
    };

    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        const checkpointStore = yield* SinkCheckpointStore;
        const first = yield* runtime.submit([
          appEvent({ id: 9_006, type: "ExternalMarker", payload: { marker: "first" } }),
        ]);
        const firstSeq = first.at(0)?.position.seq;
        if (firstSeq === undefined) {
          return yield* Effect.die(new Error("missing first committed marker"));
        }
        yield* waitUntil("first checkpoint state", () =>
          checkpointStore
            .load(EDASinkName.make(sink.name))
            .pipe(Effect.map((checkpoint) => checkpoint.afterSeq >= firstSeq)),
        );
        const second = yield* runtime.submit([
          appEvent({ id: 9_007, type: "ExternalMarker", payload: { marker: "second" } }),
        ]);
        const secondSeq = second.at(0)?.position.seq;
        if (secondSeq === undefined) {
          return yield* Effect.die(new Error("missing second committed marker"));
        }
        yield* waitUntil("second checkpoint state", () =>
          checkpointStore
            .load(EDASinkName.make(sink.name))
            .pipe(Effect.map((checkpoint) => checkpoint.afterSeq >= secondSeq)),
        );
        return yield* checkpointStore.load(EDASinkName.make(sink.name));
      }),
    ).pipe(
      Effect.provide(
        EDARuntime.Live({ modelSelection }).pipe(
          Layer.provideMerge(
            makeEdaTestLayer({
              sessionId: SESSION_ID,
              sinks: [sink],
            }),
          ),
        ),
      ),
    );

    const checkpoint = await Effect.runPromise(program);

    expect(observed).toEqual([0, 1]);
    expect(checkpoint.afterSeq).toBeGreaterThan(0);
    expect(checkpoint.payload).toMatchObject({
      formatVersion: 1,
      state: { count: 2 },
    });
  });

  it("commits sink-staged durable events before advancing the sink checkpoint", async () => {
    let deliveredEventId = 9_002;
    const sink: EDASink = {
      name: "test.final-reply-sink",
      durable: {
        interests: ["AssistantMessageCommitted"],
        process: (batch, ctx) =>
          Effect.gen(function* () {
            for (const event of batch.events) {
              const payload = event.event.payload as { readonly messageId: string };
              yield* ctx.stageDurable(
                appEvent({
                  id: deliveredEventId++,
                  type: "ExternalReplyDelivered",
                  payload: { assistantMessageId: payload.messageId, externalReplyId: "reply-1" },
                }),
              );
            }
          }),
      },
    };

    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        yield* runtime.submitAndBlock(command(COMMAND_ID, "hello"));
        yield* waitUntil("first staged delivery", () =>
          replayAll(runtime).pipe(
            Effect.map((events) =>
              events.some((entry) => entry.event.type === "ExternalReplyDelivered"),
            ),
          ),
        );
        const events = yield* replayAll(runtime);
        yield* runtime.submitAndBlock(command(SECOND_COMMAND_ID, "second"));
        yield* waitUntil("second staged delivery", () =>
          replayAll(runtime).pipe(
            Effect.map(
              (events) =>
                events.filter((entry) => entry.event.type === "ExternalReplyDelivered").length >= 2,
            ),
          ),
        );
        const afterSecond = yield* replayAll(runtime);
        return { events, afterSecond };
      }),
    ).pipe(
      Effect.provide(
        EDARuntime.Live({ modelSelection }).pipe(
          Layer.provideMerge(
            makeEdaTestLayer({
              sessionId: SESSION_ID,
              parts: [finishStream, finishStream],
              sinks: [sink],
            }),
          ),
        ),
      ),
    );

    const { events, afterSecond } = await Effect.runPromise(program);
    const delivered = events.filter((entry) => entry.event.type === "ExternalReplyDelivered");
    const deliveredAfterSecond = afterSecond.filter(
      (entry) => entry.event.type === "ExternalReplyDelivered",
    );

    expect(delivered).toHaveLength(1);
    expect(deliveredAfterSecond).toHaveLength(2);
  });
});
