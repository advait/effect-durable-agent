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

describe("SessionState control loop - active control priority", () => {
  it("processes running active-control commands in durable FIFO order", async () => {
    const finishActive = await Effect.runPromise(Deferred.make<void>());
    const finishReplacement = await Effect.runPromise(Deferred.make<void>());
    const activeStream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "active" }),
    ).pipe(
      Stream.concat(
        Stream.fromEffect(
          Deferred.await(finishActive).pipe(
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
      yield* waitForCommitted(store, hasEventType("InferenceStarted"));
      yield* dispatcher.admitCommand(steerCommand);
      yield* dispatcher.admitCommand(interruptCommand);

      const controlDrain = yield* drainUntilCommitted(
        dispatcher,
        store,
        (entries) =>
          indexOfCommandEvent(entries, "CommandCompleted", steerCommand.commandId) >= 0 &&
          indexOfCommandEvent(entries, "CommandStarted", interruptCommand.commandId) >= 0 &&
          indexOfType(entries, "RunFailed") >= 0,
      );
      yield* Deferred.succeed(finishActive, undefined);
      yield* Deferred.succeed(finishReplacement, undefined);
      return { controlDrain };
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: [activeStream, replacementStream],
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const { controlDrain } = await Effect.runPromise(program);
    const committed = controlDrain.committed;
    const steerStarted = indexOfCommandEvent(committed, "CommandStarted", steerCommand.commandId);
    const steerCompleted = indexOfCommandEvent(
      committed,
      "CommandCompleted",
      steerCommand.commandId,
    );
    const interruptStarted = indexOfCommandEvent(
      committed,
      "CommandStarted",
      interruptCommand.commandId,
    );
    const runInterrupted = indexOfType(committed, "RunFailed");

    expect(controlDrain.drain.processed).toEqual([
      expect.objectContaining({
        outcome: expect.objectContaining({ _tag: "SessionCommandCompleted" }),
      }),
      expect.objectContaining({
        _tag: "SessionForkedCommand",
        commandId: interruptCommand.commandId,
      }),
    ]);
    expect(steerStarted).toBeGreaterThan(indexOfType(committed, "InferenceStarted"));
    expect(steerCompleted).toBeGreaterThan(steerStarted);
    expect(interruptStarted).toBeGreaterThan(steerCompleted);
    expect(runInterrupted).toBeGreaterThan(steerCompleted);
  });
});
