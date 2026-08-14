import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Response from "effect/unstable/ai/Response";
import { describe, expect, it } from "vite-plus/test";

import type { SessionStateShape } from "./session-state";
import type { CommittedDurableEvent } from "./session-store";
import {
  CommandId,
  EDASessionStore,
  SessionId,
  SessionState,
  makeEdaTestLayer,
  SESSION_ID,
  NoopParams,
  command,
  interruptCommand,
  modelSelection,
  steerCommand,
  stopTurnCommand,
  usage,
  collectCommitted,
  hasEventType,
  waitForCommitted,
} from "./session-state-control-testkit";

const drainUntilCommitted = (
  dispatcher: SessionStateShape,
  store: { readonly eventsAfter: Parameters<typeof collectCommitted>[0]["eventsAfter"] },
  predicate: (entries: ReadonlyArray<CommittedDurableEvent>) => boolean,
) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const drain = yield* dispatcher.drainReadyWork({ modelSelection });
      const committed = yield* collectCommitted(store as Parameters<typeof collectCommitted>[0]);
      if (predicate(committed)) {
        return { drain, committed };
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(new Error("Timed out waiting for committed scheduler state"));
  });

const countEvents = (entries: ReadonlyArray<CommittedDurableEvent>, type: string): number =>
  entries.filter((entry) => entry.event.type === type).length;

const eventTypes = (entries: ReadonlyArray<CommittedDurableEvent>): ReadonlyArray<string> =>
  entries.map((entry) => entry.event.type);

const indexOfType = (entries: ReadonlyArray<CommittedDurableEvent>, type: string): number =>
  entries.findIndex((entry) => entry.event.type === type);

const indexOfCommandEvent = (
  entries: ReadonlyArray<CommittedDurableEvent>,
  type: string,
  commandId: CommandId,
): number =>
  entries.findIndex(
    (entry) =>
      entry.event.type === type &&
      "commandId" in entry.event.payload &&
      entry.event.payload.commandId === commandId,
  );

describe("SessionState control loop - completed turn controls", () => {
  it("consumes a late steer after TurnCompleted before run terminalization", async () => {
    const finishContinuation = await Effect.runPromise(Deferred.make<void>());
    const firstStream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "first answer" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const continuationStream = Stream.make(
      Response.makePart("text-delta", { id: "text-2", delta: "continuation" }),
    ).pipe(
      Stream.concat(
        Stream.fromEffect(
          Deferred.await(finishContinuation).pipe(
            Effect.as(
              Response.makePart("finish", {
                reason: "stop",
                usage: usage(),
                response: undefined,
              }),
            ),
          ),
        ),
      ),
    );

    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const store = yield* EDASessionStore;

      yield* dispatcher.admitCommand(command);
      yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(store, hasEventType("TurnCompleted"));
      yield* dispatcher.admitCommand(steerCommand);
      const continuation = yield* drainUntilCommitted(
        dispatcher,
        store,
        (entries) => countEvents(entries, "TurnStarted") >= 2,
      );
      const beforeContinuationFinish = yield* collectCommitted(store);
      yield* Deferred.succeed(finishContinuation, undefined);
      yield* waitForCommitted(store, (entries) => countEvents(entries, "TurnCompleted") >= 2);
      const final = yield* drainUntilCommitted(
        dispatcher,
        store,
        (entries) => countEvents(entries, "RunCompleted") === 1,
      );
      return { beforeContinuationFinish, continuation, final };
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: [firstStream, continuationStream],
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const { beforeContinuationFinish, continuation, final } = await Effect.runPromise(program);
    const turnStarted = continuation.committed.filter(
      (entry) => entry.event.type === "TurnStarted",
    );
    const secondTurnStarted = turnStarted[1];

    expect(continuation.drain.processed).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          outcome: expect.objectContaining({ _tag: "SessionCommandCompleted" }),
        }),
        expect.objectContaining({ _tag: "SessionForkedCommand", commandId: command.commandId }),
      ]),
    );
    expect(continuation.committed.map((entry) => entry.event.type)).toEqual(
      expect.arrayContaining(["UserMessageSubmitted", "CommandCompleted", "TurnStarted"]),
    );
    expect(secondTurnStarted).toMatchObject({
      event: { payload: { inputMessageIds: [expect.any(String)] } },
    });
    expect(countEvents(beforeContinuationFinish, "RunCompleted")).toBe(0);
    expect(indexOfType(final.committed, "RunCompleted")).toBeGreaterThan(
      indexOfType(final.committed, "TurnCompleted"),
    );
  });

  it("interrupts a completed-turn run before terminalization and starts replacement work", async () => {
    const finishReplacement = await Effect.runPromise(Deferred.make<void>());
    const firstStream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "first answer" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const replacementStream = Stream.make(
      Response.makePart("text-delta", { id: "text-2", delta: "replacement" }),
    ).pipe(
      Stream.concat(
        Stream.fromEffect(
          Deferred.await(finishReplacement).pipe(
            Effect.as(
              Response.makePart("finish", {
                reason: "stop",
                usage: usage(),
                response: undefined,
              }),
            ),
          ),
        ),
      ),
    );

    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const store = yield* EDASessionStore;

      yield* dispatcher.admitCommand(command);
      yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(store, hasEventType("TurnCompleted"));
      yield* dispatcher.admitCommand(interruptCommand);
      const replacement = yield* drainUntilCommitted(
        dispatcher,
        store,
        (entries) =>
          indexOfCommandEvent(entries, "CommandStarted", interruptCommand.commandId) >= 0 &&
          countEvents(entries, "RunFailed") === 1,
      );
      yield* Deferred.succeed(finishReplacement, undefined);
      return replacement.committed;
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: [firstStream, replacementStream],
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const committed = await Effect.runPromise(program);
    const originalCancelled = indexOfCommandEvent(committed, "CommandCancelled", command.commandId);
    const replacementStarted = indexOfCommandEvent(
      committed,
      "CommandStarted",
      interruptCommand.commandId,
    );

    expect(eventTypes(committed)).toEqual(
      expect.arrayContaining(["RunFailed", "CommandCancelled", "CommandStarted", "RunStarted"]),
    );
    expect(indexOfType(committed, "RunCompleted")).toBe(-1);
    expect(originalCancelled).toBeGreaterThan(indexOfType(committed, "RunFailed"));
    expect(replacementStarted).toBeGreaterThan(originalCancelled);
  });

  it("applies a late StopTurn to the completed-turn run before run completion", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "first answer" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );

    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const store = yield* EDASessionStore;

      yield* dispatcher.admitCommand(command);
      yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(store, hasEventType("TurnCompleted"));
      yield* dispatcher.admitCommand(stopTurnCommand);
      const stopped = yield* drainUntilCommitted(
        dispatcher,
        store,
        hasEventType("StopTurnApplied"),
      );
      return stopped.committed;
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: stream,
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const committed = await Effect.runPromise(program);
    const stopStarted = indexOfCommandEvent(committed, "CommandStarted", stopTurnCommand.commandId);
    const activeCancelled = indexOfCommandEvent(committed, "CommandCancelled", command.commandId);
    const stopCompleted = indexOfCommandEvent(
      committed,
      "CommandCompleted",
      stopTurnCommand.commandId,
    );

    expect(eventTypes(committed)).toEqual(
      expect.arrayContaining([
        "StopTurnRequested",
        "RunFailed",
        "CommandCancelled",
        "StopTurnApplied",
        "CommandCompleted",
      ]),
    );
    expect(indexOfType(committed, "RunCompleted")).toBe(-1);
    expect(stopStarted).toBeGreaterThan(indexOfType(committed, "TurnCompleted"));
    expect(indexOfType(committed, "StopTurnRequested")).toBeGreaterThan(stopStarted);
    expect(activeCancelled).toBeGreaterThan(indexOfType(committed, "RunFailed"));
    expect(indexOfType(committed, "StopTurnApplied")).toBeGreaterThan(activeCancelled);
    expect(stopCompleted).toBeGreaterThan(indexOfType(committed, "StopTurnApplied"));
  });
});
