import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Tool from "effect/unstable/ai/Tool";
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
import type { EDADurableEvent } from "../types/events";
import { EDASessionStore, EDASessionStoreError } from "./session-store";
import type { EDASessionStoreShape } from "./session-store";
import { EventFactory } from "./event-factory";
import { ToolExecutor } from "./tool-executor";
import { SessionState } from "./session-state";
import { makeEDAToolkit, type EDAModelToolkit } from "./tool-registry";
import { makeEdaTestLayer, testNowMs } from "../testkit/layers";

const SESSION_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const RUN_ID = "018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a";
const TURN_ID = "018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a";
const INFERENCE_ID = "018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a";
const TOOL_CALL_ID = "018f6bd5-2f2a-7b1e-8f9b-1f2e3d4c5b6a";
const TOOL_CREATED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f9a-1f2e3d4c5b6a";
const TOOL_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f9c-1f2e3d4c5b6a";
const TOOL_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f9d-1f2e3d4c5b6a";
const TOOL_FAILED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f9e-1f2e3d4c5b6a";
const TOOL_CANCELLED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f9f-1f2e3d4c5b6a";
const TURN_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f8f-1f2e3d4c5b6a";
const CREATED_AT_MS = testNowMs;

const NoopParams = Schema.Struct({});

const NoopTool = Tool.make("noop", { parameters: NoopParams, success: Schema.Unknown });

const makeTestLayer = (
  ids: ReadonlyArray<string>,
  toolkit: EDAModelToolkit,
  wrapStore?: (inner: EDASessionStoreShape) => EDASessionStoreShape,
) =>
  makeEdaTestLayer({
    sessionId: SessionId.make(SESSION_ID),
    ids,
    toolkit,
    nowMs: CREATED_AT_MS,
    wrapStore,
  });

const noopToolkit = (handler: () => Effect.Effect<unknown, unknown>): EDAModelToolkit =>
  Effect.runSync(
    makeEDAToolkit([NoopTool], {
      noop: handler,
    }),
  );

const commitCreated = (input?: { readonly providerExecuted?: boolean }) =>
  Effect.gen(function* () {
    const events = yield* EventFactory;
    const sessionState = yield* SessionState;
    const event = yield* events.toolCallCreated({
      runId: RunId.make(RUN_ID),
      turnId: TurnId.make(TURN_ID),
      inferenceId: InferenceId.make(INFERENCE_ID),
      toolCallId: ToolCallId.make(TOOL_CALL_ID),
      promptPart: Prompt.toolCallPart({
        id: "tool-call-1",
        name: "noop",
        params: {},
        providerExecuted: input?.providerExecuted ?? false,
      }),
    });
    return yield* sessionState.appendDurable(event);
  });

const collectCommitted = (store: EDASessionStore) =>
  store.eventsAfter(SequenceNumber.make(0)).pipe(
    Stream.runCollect,
    Effect.map((events) => Array.from(events)),
  );

describe("ToolExecutor", () => {
  it("commits ToolCallStarted and ToolCallCompleted for a successful handler", async () => {
    const toolkit = noopToolkit(() => Effect.succeed({ ok: true }));
    const program = Effect.gen(function* () {
      const executor = yield* ToolExecutor;
      const store = yield* EDASessionStore;
      const created = yield* commitCreated();
      const sessionState = yield* SessionState;
      const result = yield* executor.executeCreated({ created, eventSink: sessionState });
      const committed = yield* collectCommitted(store);
      return { committed, result };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [TOOL_CREATED_EVENT_ID, TOOL_STARTED_EVENT_ID, TOOL_COMPLETED_EVENT_ID],
          toolkit,
        ),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);

    expect(result.outcome).toMatchObject({
      _tag: "ToolExecutionCompleted",
      committed: { event: { type: "ToolCallCompleted" } },
      result: { ok: true },
    });
    expect(committed.map((entry) => entry.event.type)).toEqual([
      "ToolCallCreated",
      "ToolCallStarted",
      "ToolCallCompleted",
    ]);
    expect(committed[1]).toMatchObject({
      event: {
        type: "ToolCallStarted",
        eventId: EventId.make(TOOL_STARTED_EVENT_ID),
        payload: { toolCallId: ToolCallId.make(TOOL_CALL_ID) },
      },
    });
    expect(committed[2]).toMatchObject({
      event: {
        type: "ToolCallCompleted",
        eventId: EventId.make(TOOL_COMPLETED_EVENT_ID),
        payload: {
          toolCallId: ToolCallId.make(TOOL_CALL_ID),
          promptPart: { id: "tool-call-1", name: "noop", isFailure: false, result: { ok: true } },
        },
      },
    });
  });

  it("rejects committed events that are not ToolCallCreated", async () => {
    const toolkit = noopToolkit(() => Effect.die(new Error("should not execute")));
    const program = Effect.gen(function* () {
      const executor = yield* ToolExecutor;
      const store = yield* EDASessionStore;
      const events = yield* EventFactory;
      const sessionState = yield* SessionState;
      const started = yield* sessionState.appendDurable(
        yield* events.turnStarted({ runId: RunId.make(RUN_ID), turnId: TurnId.make(TURN_ID) }),
      );
      const exit = yield* Effect.exit(
        executor.executeCreated({ created: started, eventSink: sessionState }),
      );
      const committed = yield* collectCommitted(store);
      return { committed, exit };
    }).pipe(Effect.provide(makeTestLayer([TURN_STARTED_EVENT_ID], toolkit)));

    const { committed, exit } = await Effect.runPromise(program);

    expectFailure(exit, "ToolExecutionEventNotCreated");
    expect(committed.map((entry) => entry.event.type)).toEqual(["TurnStarted"]);
  });

  it("commits ToolCallFailed when the handler fails", async () => {
    const toolkit = noopToolkit(() => Effect.fail(new Error("boom")));
    const program = Effect.gen(function* () {
      const executor = yield* ToolExecutor;
      const store = yield* EDASessionStore;
      const created = yield* commitCreated();
      const sessionState = yield* SessionState;
      const result = yield* executor.executeCreated({ created, eventSink: sessionState });
      const committed = yield* collectCommitted(store);
      return { committed, result };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [TOOL_CREATED_EVENT_ID, TOOL_STARTED_EVENT_ID, TOOL_FAILED_EVENT_ID],
          toolkit,
        ),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);

    expect(result.outcome).toMatchObject({
      _tag: "ToolExecutionFailed",
      committed: { event: { type: "ToolCallFailed" } },
    });
    expect(committed.map((entry) => entry.event.type)).toEqual([
      "ToolCallCreated",
      "ToolCallStarted",
      "ToolCallFailed",
    ]);
    expect(committed[2]).toMatchObject({
      event: {
        type: "ToolCallFailed",
        eventId: EventId.make(TOOL_FAILED_EVENT_ID),
        payload: {
          toolCallId: ToolCallId.make(TOOL_CALL_ID),
          promptPart: {
            id: "tool-call-1",
            name: "noop",
            isFailure: true,
            result: { message: "boom" },
          },
        },
      },
    });
  });

  it("maps handler defects to ToolCallFailed instead of escaping defects", async () => {
    const toolkit = noopToolkit(() => Effect.die(new Error("defect boom")));
    const program = Effect.gen(function* () {
      const executor = yield* ToolExecutor;
      const store = yield* EDASessionStore;
      const created = yield* commitCreated();
      const sessionState = yield* SessionState;
      const result = yield* executor.executeCreated({ created, eventSink: sessionState });
      const committed = yield* collectCommitted(store);
      return { committed, result };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [TOOL_CREATED_EVENT_ID, TOOL_STARTED_EVENT_ID, TOOL_FAILED_EVENT_ID],
          toolkit,
        ),
      ),
    );

    const { committed, result } = await Effect.runPromise(program);

    expect(result.outcome).toMatchObject({ _tag: "ToolExecutionFailed" });
    expect(committed.map((entry) => entry.event.type)).toEqual([
      "ToolCallCreated",
      "ToolCallStarted",
      "ToolCallFailed",
    ]);
    expect(committed[2]).toMatchObject({
      event: {
        type: "ToolCallFailed",
        payload: {
          promptPart: {
            id: "tool-call-1",
            name: "noop",
            isFailure: true,
            result: { message: "defect boom" },
          },
        },
      },
    });
  });

  it("commits ToolCallFailed when execution is interrupted", async () => {
    const handlerStarted = await Effect.runPromise(Deferred.make<void>());
    const releaseHandler = await Effect.runPromise(Deferred.make<void>());
    const toolkit = noopToolkit(() =>
      Effect.gen(function* () {
        yield* Deferred.succeed(handlerStarted, undefined);
        yield* Deferred.await(releaseHandler);
        return { ok: true };
      }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const executor = yield* ToolExecutor;
        const store = yield* EDASessionStore;
        const created = yield* commitCreated();
        const sessionState = yield* SessionState;
        const fiber = yield* executor
          .executeCreated({ created, eventSink: sessionState })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(handlerStarted);
        yield* Fiber.interrupt(fiber);
        const committed = yield* collectCommitted(store);
        return committed;
      }),
    ).pipe(
      Effect.provide(
        makeTestLayer(
          [TOOL_CREATED_EVENT_ID, TOOL_STARTED_EVENT_ID, TOOL_CANCELLED_EVENT_ID],
          toolkit,
        ),
      ),
    );

    const committed = await Effect.runPromise(program);

    expect(committed.map((entry) => entry.event.type)).toEqual([
      "ToolCallCreated",
      "ToolCallStarted",
      "ToolCallFailed",
    ]);
    expect(committed[2]).toMatchObject({
      event: {
        type: "ToolCallFailed",
        eventId: EventId.make(TOOL_CANCELLED_EVENT_ID),
        payload: {
          toolCallId: ToolCallId.make(TOOL_CALL_ID),
          promptPart: {
            id: "tool-call-1",
            name: "noop",
            isFailure: true,
            result: { message: "tool call interrupted: interrupted" },
          },
        },
      },
    });
  });

  it("does not run the handler when ToolCallStarted cannot be committed", async () => {
    let handlerCalls = 0;
    const toolkit = noopToolkit(() =>
      Effect.sync(() => {
        handlerCalls += 1;
        return { ok: true };
      }),
    );
    const wrapStore = failWhen(
      (event) => event.type === "ToolCallStarted",
      "injected tool start failure",
    );
    const program = Effect.gen(function* () {
      const executor = yield* ToolExecutor;
      const store = yield* EDASessionStore;
      const created = yield* commitCreated();
      const sessionState = yield* SessionState;
      const exit = yield* Effect.exit(
        executor.executeCreated({ created, eventSink: sessionState }),
      );
      const committed = yield* collectCommitted(store);
      return { committed, exit };
    }).pipe(
      Effect.provide(
        makeTestLayer([TOOL_CREATED_EVENT_ID, TOOL_STARTED_EVENT_ID], toolkit, wrapStore),
      ),
    );

    const { committed, exit } = await Effect.runPromise(program);

    expectFailure(exit, "injected tool start failure");
    expect(handlerCalls).toBe(0);
    expect(committed.map((entry) => entry.event.type)).toEqual(["ToolCallCreated"]);
  });

  it("propagates terminal durable-write failures without committing a fallback terminal", async () => {
    const toolkit = noopToolkit(() => Effect.succeed({ ok: true }));
    const wrapStore = failWhen(
      (event) => event.type === "ToolCallCompleted",
      "injected tool completion failure",
    );
    const program = Effect.gen(function* () {
      const executor = yield* ToolExecutor;
      const store = yield* EDASessionStore;
      const created = yield* commitCreated();
      const sessionState = yield* SessionState;
      const exit = yield* Effect.exit(
        executor.executeCreated({ created, eventSink: sessionState }),
      );
      const committed = yield* collectCommitted(store);
      return { committed, exit };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [TOOL_CREATED_EVENT_ID, TOOL_STARTED_EVENT_ID, TOOL_COMPLETED_EVENT_ID],
          toolkit,
          wrapStore,
        ),
      ),
    );

    const { committed, exit } = await Effect.runPromise(program);

    expectFailure(exit, "injected tool completion failure");
    expect(committed.map((entry) => entry.event.type)).toEqual([
      "ToolCallCreated",
      "ToolCallStarted",
    ]);
  });

  it("skips provider-executed tool calls without committing framework execution boundaries", async () => {
    const toolkit = noopToolkit(() => Effect.die(new Error("should not execute")));
    const program = Effect.gen(function* () {
      const executor = yield* ToolExecutor;
      const store = yield* EDASessionStore;
      const created = yield* commitCreated({ providerExecuted: true });
      const sessionState = yield* SessionState;
      const result = yield* executor.executeCreated({ created, eventSink: sessionState });
      const committed = yield* collectCommitted(store);
      return { committed, result };
    }).pipe(Effect.provide(makeTestLayer([TOOL_CREATED_EVENT_ID], toolkit)));

    const { committed, result } = await Effect.runPromise(program);

    expect(result.outcome).toMatchObject({
      _tag: "ToolExecutionSkipped",
      reason: "provider-executed",
    });
    expect(committed.map((entry) => entry.event.type)).toEqual(["ToolCallCreated"]);
  });
});

const failWhen =
  (
    predicate: (event: EDADurableEvent) => boolean,
    message: string,
  ): ((inner: EDASessionStoreShape) => EDASessionStoreShape) =>
  (inner) => ({
    ...inner,
    append: (batch) =>
      batch.entries.some((entry) => predicate(entry.event))
        ? Effect.fail(new EDASessionStoreError({ message }))
        : inner.append(batch),
  });

const expectFailure = <A, E>(exit: Exit.Exit<A, E>, message: string) => {
  expect(Exit.isFailure(exit)).toBe(true);
  expect(Exit.isFailure(exit) ? Cause.pretty(exit.cause) : "").toContain(message);
};
