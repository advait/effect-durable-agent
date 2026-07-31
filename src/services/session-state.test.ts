import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";
import { describe, expect, it } from "vite-plus/test";

import {
  frameworkReducedStateReducerName,
  frameworkReducedStateReducerSchemaVersion,
  pendingCommands,
} from "../domain/reduced-state";
import { StopTurnCommand, SubmitMessageCommand } from "../types/commands";
import { CommandId, EventId, RunId, SequenceNumber, SessionId, TurnId } from "../types/core";
import {
  CommandAdmittedEvent,
  EventType,
  EphemeralEventEnvelope,
  PositionedEvent,
  RecoveryCompletedEvent,
  TurnCompletedEvent,
  UnixEpochMillis,
  commandAdmittedEventType,
  effectDurableAgentNamespace,
  recoveryCompletedEventType,
  schemaV1,
  turnCompletedEventType,
} from "../types/events";
import { EDASessionStore, EDASessionStoreError, type EDASessionStoreShape } from "./session-store";
import { LiveEventBus } from "./live-event-bus";
import { SessionState } from "./session-state";
import { makeEdaExportingTracer, type EDAExportedSpan } from "./tracing";
import { makeEdaTestLayer } from "../testkit/layers";

const SESSION_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const EVENT_ID = "018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a";
const EPHEMERAL_EVENT_ID = "018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a";
const TURN_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f1c-1f2e3d4c5b6a";
const COMMAND_ID = "018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a";
const RUN_ID = "018f6bd5-2f2a-7b1e-8f1d-1f2e3d4c5b6a";
const REPLACEMENT_RUN_ID = "018f6bd5-2f2a-7b1e-8f1f-1f2e3d4c5b6a";
const TURN_ID = "018f6bd5-2f2a-7b1e-8f1e-1f2e3d4c5b6a";

const TestLayer = makeEdaTestLayer({ sessionId: SessionId.make(SESSION_ID) });

const durableEvent = CommandAdmittedEvent.make({
  namespace: effectDurableAgentNamespace,
  type: commandAdmittedEventType,
  schemaVersion: schemaV1,
  durability: "durable",
  eventId: EventId.make(EVENT_ID),
  sessionId: SessionId.make(SESSION_ID),
  createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
  payload: {
    command: new StopTurnCommand({ commandId: CommandId.make(COMMAND_ID) }),
  },
});

const submitCommand = new SubmitMessageCommand({
  commandId: CommandId.make(COMMAND_ID),
  disposition: "queue",
  content: [Prompt.textPart({ text: "hello" })],
});

const legacySubmitAdmittedEvent = CommandAdmittedEvent.make({
  namespace: effectDurableAgentNamespace,
  type: commandAdmittedEventType,
  schemaVersion: schemaV1,
  durability: "durable",
  eventId: EventId.make("018f6bd5-2f2a-7b1e-9f1b-1f2e3d4c5b6a"),
  sessionId: SessionId.make(SESSION_ID),
  createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
  payload: { command: submitCommand },
});

const usage = () =>
  new Response.Usage({
    inputTokens: { total: 1, uncached: undefined, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: undefined, reasoning: undefined },
  });

const finishedStream = Stream.make(
  Response.makePart("text-delta", { id: "text-1", delta: "pong" }),
  Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
);

const modelSelection = { provider: "test", modelId: "test-model" };

const ephemeralEvent = EphemeralEventEnvelope.make({
  namespace: effectDurableAgentNamespace,
  type: EventType.make("TestEphemeral"),
  schemaVersion: schemaV1,
  durability: "ephemeral",
  eventId: EventId.make(EPHEMERAL_EVENT_ID),
  sessionId: SessionId.make(SESSION_ID),
  createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
  payload: { status: "thinking" },
});

const turnCompletedEvent = TurnCompletedEvent.make({
  namespace: effectDurableAgentNamespace,
  type: turnCompletedEventType,
  schemaVersion: schemaV1,
  durability: "durable",
  eventId: EventId.make(TURN_COMPLETED_EVENT_ID),
  sessionId: SessionId.make(SESSION_ID),
  createdAtMs: UnixEpochMillis.make(1_715_000_000_001),
  payload: {
    runId: RunId.make(RUN_ID),
    turnId: TurnId.make(TURN_ID),
  },
});

const recoveryCompletedEvent = RecoveryCompletedEvent.make({
  namespace: effectDurableAgentNamespace,
  type: recoveryCompletedEventType,
  schemaVersion: schemaV1,
  durability: "durable",
  eventId: EventId.make("018f6bd5-2f2a-7b1e-9f1d-1f2e3d4c5b6a"),
  sessionId: SessionId.make(SESSION_ID),
  createdAtMs: UnixEpochMillis.make(1_715_000_000_002),
  payload: {
    trigger: "runtime-restart",
    continuation: {
      commandId: CommandId.make(COMMAND_ID),
      interruptedRunId: RunId.make(RUN_ID),
      replacementRunId: RunId.make(REPLACEMENT_RUN_ID),
    },
  },
});

const collectCommitted = (store: EDASessionStoreShape) =>
  store.eventsAfter(SequenceNumber.make(0)).pipe(
    Stream.runCollect,
    Effect.map((events) => Array.from(events)),
  );

const waitForCommittedEventType = (store: EDASessionStoreShape, type: string) =>
  Effect.gen(function* () {
    let lastCommitted: ReadonlyArray<{ readonly event: { readonly type: string } }> = [];
    for (let attempt = 0; attempt < 1_000; attempt += 1) {
      const committed = yield* collectCommitted(store);
      lastCommitted = committed;
      if (committed.some((entry) => entry.event.type === type)) {
        return committed;
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(
      new Error(
        `Timed out waiting for ${type}; saw ${lastCommitted
          .map((entry) => entry.event.type)
          .join(", ")}`,
      ),
    );
  });

const waitForFrameworkCheckpoint = (store: EDASessionStoreShape, throughSeq: SequenceNumber) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const checkpoint = yield* store.loadReducerCheckpoint(frameworkReducedStateReducerName);
      if (checkpoint?.throughSeq === throughSeq) {
        return checkpoint;
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(new Error("Timed out waiting for framework checkpoint"));
  });

describe("SessionState", () => {
  it("starts a legacy admitted submit without migrating persisted session state", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const LegacySessionLayer = makeEdaTestLayer({
      sessionId: SessionId.make(SESSION_ID),
      seedEvents: [legacySubmitAdmittedEvent],
      parts: finishedStream,
      onStreamText: ({ prompt }) => prompts.push(prompt),
    });
    const program = Effect.scoped(
      Effect.gen(function* () {
        const sessionState = yield* SessionState;
        const store = yield* EDASessionStore;
        yield* sessionState.start({ modelSelection });
        yield* sessionState.drainReadyWork({ modelSelection });
        return yield* waitForCommittedEventType(store, "InferenceStarted");
      }),
    ).pipe(Effect.provide(LegacySessionLayer));

    const committed = await Effect.runPromise(program);
    const userMessage = committed.find((entry) => entry.event.type === "UserMessageCommitted");
    const turnStarted = committed.find((entry) => entry.event.type === "TurnStarted");

    expect(Prompt.make(prompts[0] ?? "").content).toMatchObject([
      { role: "user", content: [{ type: "text", text: "hello" }] },
    ]);
    expect(turnStarted).toMatchObject({
      event: { payload: { inputMessageIds: [userMessage?.event.payload.messageId] } },
    });
  });

  it("folds durable appends into the authoritative snapshot before returning", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const sessionState = yield* SessionState;
        const liveBus = yield* LiveEventBus;
        const store = yield* EDASessionStore;
        const liveStream = yield* liveBus.subscribe();
        const liveFiber = yield* liveStream.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );

        const committed = yield* sessionState.appendDurable(durableEvent);
        const snapshot = yield* sessionState.snapshot();
        const replay = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
        const liveEvents = yield* Fiber.join(liveFiber);

        return { committed, liveEvents, replay: Array.from(replay), snapshot };
      }),
    ).pipe(Effect.provide(TestLayer));

    const result = await Effect.runPromise(program);

    expect(result.snapshot.lastSeq).toBe(SequenceNumber.make(1));
    expect(pendingCommands(result.snapshot).map((command) => command.commandId)).toEqual([
      CommandId.make(COMMAND_ID),
    ]);
    expect(result.replay).toEqual([result.committed]);
    expect(result.liveEvents).toEqual([PositionedEvent.make(result.committed)]);
  });

  it("does not checkpoint ordinary durable appends", async () => {
    let checkpointWrites = 0;
    const LayerWithCheckpointSpy = makeEdaTestLayer({
      sessionId: SessionId.make(SESSION_ID),
      wrapStore: (inner) => ({
        ...inner,
        saveReducerCheckpoints: (checkpoints) =>
          Effect.sync(() => {
            checkpointWrites += checkpoints.length;
          }).pipe(Effect.andThen(inner.saveReducerCheckpoints(checkpoints))),
      }),
    });

    const program = Effect.scoped(
      Effect.gen(function* () {
        const sessionState = yield* SessionState;
        yield* sessionState.appendDurable(durableEvent);
        yield* Effect.yieldNow;
        return checkpointWrites;
      }),
    ).pipe(Effect.provide(LayerWithCheckpointSpy));

    const result = await Effect.runPromise(program);

    expect(result).toBe(0);
  });

  it("queues turn-boundary checkpoints without blocking durable append", async () => {
    const saveStarted = Effect.runSync(Deferred.make<void>());
    const releaseSave = Effect.runSync(Deferred.make<void>());
    const LayerWithSlowCheckpoint = makeEdaTestLayer({
      clock: "live",
      parts: finishedStream,
      sessionId: SessionId.make(SESSION_ID),
      wrapStore: (inner) => ({
        ...inner,
        saveReducerCheckpoints: (checkpoints) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(saveStarted, undefined);
            yield* Deferred.await(releaseSave);
            return yield* inner.saveReducerCheckpoints(checkpoints);
          }),
      }),
    });

    const program = Effect.scoped(
      Effect.gen(function* () {
        const sessionState = yield* SessionState;
        const store = yield* EDASessionStore;
        yield* sessionState.start({ modelSelection });
        yield* sessionState.admitCommand(submitCommand);
        const committed = yield* waitForCommittedEventType(store, "CommandCompleted");
        const turnCompleted = committed.find(
          (entry) => entry.event.type === turnCompletedEventType,
        );
        if (turnCompleted === undefined) {
          return yield* Effect.die(new Error("missing TurnCompleted event"));
        }
        const checkpointBeforeRelease = yield* store.loadReducerCheckpoint(
          frameworkReducedStateReducerName,
        );
        yield* Deferred.await(saveStarted).pipe(Effect.timeout("1 second"));
        yield* Deferred.succeed(releaseSave, undefined);
        const checkpoint = yield* waitForFrameworkCheckpoint(store, turnCompleted.position.seq);
        return { checkpoint, checkpointBeforeRelease, turnCompleted };
      }),
    ).pipe(Effect.provide(LayerWithSlowCheckpoint));

    const result = await Effect.runPromise(program);

    expect(result.checkpointBeforeRelease).toBeUndefined();
    expect(result.checkpoint.throughSeq).toBe(result.turnCompleted.position.seq);
  });

  it("exports the persisted run root with turn and inference children", async () => {
    const spans: Array<EDAExportedSpan> = [];
    const LayerWithTracing = Layer.merge(
      makeEdaTestLayer({
        clock: "live",
        parts: finishedStream,
        sessionId: SessionId.make(SESSION_ID),
      }),
      Layer.succeed(
        Tracer.Tracer,
        makeEdaExportingTracer((span) => spans.push(span)),
      ),
    );

    const program = Effect.scoped(
      Effect.gen(function* () {
        const sessionState = yield* SessionState;
        const store = yield* EDASessionStore;
        yield* sessionState.start({ modelSelection });
        yield* sessionState.admitCommand(submitCommand);
        const committed = yield* waitForCommittedEventType(store, "CommandCompleted");
        for (
          let attempt = 0;
          attempt < 100 && !spans.some((span) => span.name === "agent.run");
          attempt += 1
        ) {
          yield* Effect.yieldNow;
        }
        return committed;
      }),
    ).pipe(Effect.provide(LayerWithTracing));

    const committed = await Effect.runPromise(program);
    const runStarted = committed.find((entry) => entry.event.type === "RunStarted");
    const run = spans.find((span) => span.name === "agent.run");
    const turn = spans.find((span) => span.name === "agent.turn");
    const inference = spans.find((span) => span.name === "agent.inference");

    expect(runStarted).toBeDefined();
    expect(run).toMatchObject({
      traceId: runStarted?.event.payload.trace.root.traceId,
      spanId: runStarted?.event.payload.trace.root.spanId,
    });
    expect(turn?.parentSpanId).toBe(run?.spanId);
    expect(inference?.parentSpanId).toBe(turn?.spanId);
    expect(run?.links).toHaveLength(1);
    expect(run?.startedAtUnixNano).toMatch(/^[1-9][0-9]*$/);
    expect(run?.endedAtUnixNano).toMatch(/^[1-9][0-9]*$/);
  });

  it("closes the run trace when durable startup fails before execution owns it", async () => {
    const runStartedAppendAttempted = Effect.runSync(Deferred.make<void>());
    const spans: Array<EDAExportedSpan> = [];
    const LayerWithFailingRunStart = Layer.merge(
      makeEdaTestLayer({
        clock: "live",
        parts: finishedStream,
        sessionId: SessionId.make(SESSION_ID),
        wrapStore: (inner) => ({
          ...inner,
          append: (batch) =>
            batch.entries.some((entry) => entry.event.type === "RunStarted")
              ? Effect.gen(function* () {
                  yield* Deferred.succeed(runStartedAppendAttempted, undefined);
                  return yield* new EDASessionStoreError({
                    message: "injected run-start failure",
                  });
                })
              : inner.append(batch),
        }),
      }),
      Layer.succeed(
        Tracer.Tracer,
        makeEdaExportingTracer((span) => spans.push(span)),
      ),
    );

    const closedBeforeRuntimeShutdown = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sessionState = yield* SessionState;
          yield* sessionState.start({ modelSelection });
          yield* sessionState.admitCommand(submitCommand);
          yield* Deferred.await(runStartedAppendAttempted).pipe(Effect.timeout("1 second"));
          for (
            let attempt = 0;
            attempt < 100 && !spans.some((span) => span.name === "agent.run");
            attempt += 1
          ) {
            yield* Effect.yieldNow;
          }
          return spans.some((span) => span.name === "agent.run" && span.statusCode === "ERROR");
        }),
      ).pipe(Effect.provide(LayerWithFailingRunStart)),
    );

    expect(closedBeforeRuntimeShutdown).toBe(true);
  });

  it("checkpoints after startup recovery replays past the previous checkpoint", async () => {
    const LayerWithSeededTail = makeEdaTestLayer({
      sessionId: SessionId.make(SESSION_ID),
      seedEvents: [turnCompletedEvent],
    });

    const program = Effect.scoped(
      Effect.gen(function* () {
        const sessionState = yield* SessionState;
        const store = yield* EDASessionStore;
        yield* sessionState.start({ modelSelection });
        return yield* waitForFrameworkCheckpoint(store, SequenceNumber.make(1));
      }),
    ).pipe(Effect.provide(LayerWithSeededTail));

    const checkpoint = await Effect.runPromise(program);

    expect(checkpoint.throughSeq).toBe(SequenceNumber.make(1));
  });

  it("ignores stale framework checkpoint versions and replays from the event log", async () => {
    const replayCursors: Array<SequenceNumber> = [];
    const LayerWithStaleCheckpoint = makeEdaTestLayer({
      sessionId: SessionId.make(SESSION_ID),
      seedEvents: [durableEvent],
      wrapStore: (inner) => ({
        ...inner,
        eventsAfter: (afterSeq) => {
          replayCursors.push(afterSeq);
          return inner.eventsAfter(afterSeq);
        },
        loadReducerCheckpoint: (name) =>
          name === frameworkReducedStateReducerName
            ? Effect.succeed({
                name,
                schemaVersion: frameworkReducedStateReducerSchemaVersion - 1,
                throughSeq: SequenceNumber.make(1),
                payload: {
                  lastSeq: SequenceNumber.make(1),
                },
                updatedAtMs: 1_715_000_000_001,
              })
            : inner.loadReducerCheckpoint(name),
      }),
    });

    const program = Effect.scoped(
      Effect.gen(function* () {
        const sessionState = yield* SessionState;
        return yield* sessionState.snapshot();
      }),
    ).pipe(Effect.provide(LayerWithStaleCheckpoint));

    const snapshot = await Effect.runPromise(program);

    expect(replayCursors[0]).toBe(SequenceNumber.make(0));
    expect(snapshot.lastSeq).toBe(SequenceNumber.make(1));
    expect(pendingCommands(snapshot).map((entry) => entry.commandId)).toEqual([
      CommandId.make(COMMAND_ID),
    ]);
  });

  it("replays recovery continuations hidden by a version 3 rollback checkpoint", async () => {
    const LayerWithRollbackCheckpoint = makeEdaTestLayer({
      sessionId: SessionId.make(SESSION_ID),
      seedEvents: [recoveryCompletedEvent],
      wrapStore: (inner) => ({
        ...inner,
        loadReducerCheckpoint: (name) =>
          name === frameworkReducedStateReducerName
            ? Effect.succeed({
                name,
                schemaVersion: 3,
                throughSeq: SequenceNumber.make(1),
                payload: { lastSeq: SequenceNumber.make(1) },
                updatedAtMs: 1_715_000_000_003,
              })
            : inner.loadReducerCheckpoint(name),
      }),
    });

    const snapshot = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sessionState = yield* SessionState;
          return yield* sessionState.snapshot();
        }),
      ).pipe(Effect.provide(LayerWithRollbackCheckpoint)),
    );

    expect(Array.from(snapshot.recoveryContinuations.values())).toEqual([
      {
        commandId: CommandId.make(COMMAND_ID),
        interruptedRunId: RunId.make(RUN_ID),
        replacementRunId: RunId.make(REPLACEMENT_RUN_ID),
        seq: SequenceNumber.make(1),
      },
    ]);
  });

  it("finishes fold and publish when interrupted after durable commit", async () => {
    const committedSignal = Effect.runSync(Deferred.make<void>());
    const releaseCommit = Effect.runSync(Deferred.make<void>());
    const LayerWithSlowCommit = makeEdaTestLayer({
      sessionId: SessionId.make(SESSION_ID),
      wrapStore: (inner) => ({
        ...inner,
        append: (batch) =>
          Effect.gen(function* () {
            const committed = yield* inner.append(batch);
            yield* Deferred.succeed(committedSignal, undefined);
            yield* Deferred.await(releaseCommit);
            return committed;
          }),
      }),
    });

    const program = Effect.scoped(
      Effect.gen(function* () {
        const sessionState = yield* SessionState;
        const liveBus = yield* LiveEventBus;
        const liveStream = yield* liveBus.subscribe();
        const liveFiber = yield* liveStream.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );
        const appendFiber = yield* sessionState.appendDurable(durableEvent).pipe(Effect.forkScoped);

        yield* Deferred.await(committedSignal);
        const interrupter = yield* Fiber.interrupt(appendFiber).pipe(Effect.forkScoped);
        yield* Deferred.succeed(releaseCommit, undefined);
        yield* Fiber.await(appendFiber);
        yield* Fiber.await(interrupter);

        const snapshot = yield* sessionState.snapshot();
        const liveEvents = yield* Fiber.join(liveFiber);
        return { liveEvents, snapshot };
      }),
    ).pipe(Effect.provide(LayerWithSlowCommit));

    const result = await Effect.runPromise(program);

    expect(result.snapshot.lastSeq).toBe(SequenceNumber.make(1));
    expect(pendingCommands(result.snapshot).map((command) => command.commandId)).toEqual([
      CommandId.make(COMMAND_ID),
    ]);
    expect(Array.from(result.liveEvents).map((event) => event.event.eventId)).toEqual([
      EventId.make(EVENT_ID),
    ]);
  });

  it("anchors ephemerals to the authoritative snapshot head", async () => {
    const program = Effect.gen(function* () {
      const sessionState = yield* SessionState;
      yield* sessionState.appendDurable(durableEvent);
      return yield* sessionState.publishEphemeral(ephemeralEvent);
    }).pipe(Effect.provide(TestLayer));

    const positioned = await Effect.runPromise(program);

    expect(positioned.position).toEqual({ seq: 1, subSeq: 1 });
  });
});
