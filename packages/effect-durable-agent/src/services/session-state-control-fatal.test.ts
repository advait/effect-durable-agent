import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vite-plus/test";

import type { EDADurableEvent } from "../types/events";
import type { EDASessionStoreShape } from "./session-store";
import {
  CommandId,
  EDASessionStore,
  EDASessionStoreError,
  EventFactory,
  SessionId,
  SessionState,
  makeEdaTestLayer,
  SESSION_ID,
  COMMAND_ID,
  SECOND_COMMAND_ID,
  NoopParams,
  modelSelection,
  command,
  secondCommand,
  thirdCommand,
  collectCommitted,
} from "./session-state-control-testkit";

const injectedStartFailure = "injected start lifecycle commit failure";
const injectedRecoveryFailure = "injected recovery commit failure";

describe("SessionState fatal durable-write handling", () => {
  it("remembers a failed start lifecycle append as fatal and does not commit later lifecycle claims", async () => {
    let failuresRemaining = 1;
    const wrapStore = failOnceWhen(
      (events) => events.some((event) => event.type === "RunStarted"),
      injectedStartFailure,
      () => failuresRemaining-- > 0,
    );

    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const durableStore = yield* EDASessionStore;

      yield* dispatcher.admitCommand(command);
      const firstDrainExit = yield* Effect.exit(dispatcher.drainReadyWork({ modelSelection }));
      const laterDrainExit = yield* Effect.exit(dispatcher.drainReadyWork({ modelSelection }));
      const laterAdmitExit = yield* Effect.exit(dispatcher.admitCommand(secondCommand));
      const committed = yield* collectCommitted(durableStore);
      return { firstDrainExit, laterDrainExit, laterAdmitExit, committed };
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: Stream.never,
          toolSchemas: new Map([["noop", NoopParams]]),
          wrapStore,
        }),
      ),
    );

    const result = await Effect.runPromise(program);
    const eventTypes = result.committed.map((entry) => entry.event.type);

    expect(failuresRemaining).toBe(0);
    expectFailure(result.firstDrainExit, injectedStartFailure);
    expectFailure(result.laterDrainExit, injectedStartFailure);
    expectFailure(result.laterAdmitExit, injectedStartFailure);
    expect(eventTypes).toEqual(["CommandAdmitted", "UserMessageSubmitted"]);
    expect(result.committed[0]).toMatchObject({
      event: { payload: { command: { commandId: CommandId.make(COMMAND_ID) } } },
    });
    expect(eventTypes).not.toContain("CommandStarted");
    expect(eventTypes).not.toContain("RunStarted");
    expect(
      result.committed.some(
        (entry) =>
          entry.event.type === "CommandAdmitted" &&
          entry.event.payload.command.commandId === CommandId.make(SECOND_COMMAND_ID),
      ),
    ).toBe(false);
  });

  it("remembers a failed recovery append as fatal and does not start queued work", async () => {
    let failuresRemaining = 1;
    const wrapStore = failOnceWhen(
      (events) => events.some((event) => event.type === "CommandCancelled"),
      injectedRecoveryFailure,
      () => failuresRemaining-- > 0,
    );

    const program = Effect.gen(function* () {
      const events = yield* EventFactory;
      const dispatcher = yield* SessionState;
      const durableStore = yield* EDASessionStore;
      const commandId = CommandId.make(COMMAND_ID);

      yield* dispatcher.appendDurableBatch([
        yield* events.commandAdmitted({ command }),
        yield* events.commandStarted({ commandId }),
        yield* events.commandAdmitted({ command: secondCommand }),
      ]);
      const firstRecoveryExit = yield* Effect.exit(dispatcher.start({ modelSelection }));
      const laterRecoveryExit = yield* Effect.exit(dispatcher.start({ modelSelection }));
      const laterAdmitExit = yield* Effect.exit(dispatcher.admitCommand(thirdCommand));
      const committed = yield* collectCommitted(durableStore);
      return { firstRecoveryExit, laterRecoveryExit, laterAdmitExit, committed };
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: Stream.never,
          toolSchemas: new Map([["noop", NoopParams]]),
          wrapStore,
        }),
      ),
    );

    const result = await Effect.runPromise(program);
    const eventTypes = result.committed.map((entry) => entry.event.type);
    const secondCommandStarted = result.committed.some(
      (entry) =>
        entry.event.type === "CommandStarted" &&
        entry.event.payload.commandId === CommandId.make(SECOND_COMMAND_ID),
    );

    expect(failuresRemaining).toBe(0);
    expectFailure(result.firstRecoveryExit, injectedRecoveryFailure);
    expectFailure(result.laterRecoveryExit, injectedRecoveryFailure);
    expectFailure(result.laterAdmitExit, injectedRecoveryFailure);
    expect(eventTypes).toEqual(["CommandAdmitted", "CommandStarted", "CommandAdmitted"]);
    expect(eventTypes).not.toContain("CommandCancelled");
    expect(eventTypes).not.toContain("RunStarted");
    expect(secondCommandStarted).toBe(false);
    expect(
      result.committed.some(
        (entry) =>
          entry.event.type === "CommandAdmitted" &&
          entry.event.payload.command.commandId === thirdCommand.commandId,
      ),
    ).toBe(false);
  });
});

const failOnceWhen =
  (
    predicate: (events: ReadonlyArray<EDADurableEvent>) => boolean,
    message: string,
    shouldFail: () => boolean,
  ): ((inner: EDASessionStoreShape) => EDASessionStoreShape) =>
  (inner) => ({
    ...inner,
    append: (batch) => {
      const events = batch.entries.map((entry) => entry.event);
      return predicate(events) && shouldFail()
        ? Effect.fail(new EDASessionStoreError({ message }))
        : inner.append(batch);
    },
  });

const expectFailure = <A, E>(exit: Exit.Exit<A, E>, message: string) => {
  expect(Exit.isFailure(exit)).toBe(true);
  expect(Exit.isFailure(exit) ? Cause.pretty(exit.cause) : "").toContain(message);
};
