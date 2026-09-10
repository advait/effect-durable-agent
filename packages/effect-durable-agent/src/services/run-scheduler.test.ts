import { assert, makeMethods } from "@effect/vitest";
import { describe, it } from "vite-plus/test";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Response from "effect/unstable/ai/Response";

import { ResumePendingMessagesCommand } from "../types/commands";
import { CommandId, MessageId, RunId, SessionId, TurnId } from "../types/core";
import { makeEdaTestLayer, makeLanguageModelLayer } from "../testkit/layers";
import { EDARuntime } from "./runtime";
import { makeEDARuntimeLayer } from "./runtime-layer";
import { ModelResolver } from "./model-resolver";
import { EDASessionStore } from "./session-store";
import { SinkCheckpointStore } from "./sink-checkpoint-store";
import { SessionState } from "./session-state";
import { EventFactory } from "./event-factory";
import { LocalRunResolution, RunScheduler, type RunSchedulingInput } from "./run-scheduler";
import {
  SESSION_ID,
  USER_MESSAGE_ID,
  RUN_ID,
  TURN_ID,
  command,
  secondCommand,
  steerCommand,
  interruptCommand,
  stopTurnCommand,
  modelSelection,
  usage,
  collectCommitted,
  waitForCommitted,
  hasCommandCompleted,
  hasEventType,
} from "./session-state-control-testkit";

const sessionId = SessionId.make(SESSION_ID);
const finished = Stream.make(
  Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
);
const recordingScheduler = (calls: Array<RunSchedulingInput>) =>
  Layer.succeed(RunScheduler, {
    resolve: (input) =>
      Effect.sync(() => {
        calls.push(input);
        return LocalRunResolution.make({});
      }),
  });

describe("RunScheduler", () => {
  makeMethods(it).effect("uses the supplied scheduler through the public runtime layer", () => {
    const calls: Array<RunSchedulingInput> = [];
    return Effect.gen(function* () {
      const runtime = yield* EDARuntime;
      yield* runtime.submitAndBlock(command);
      yield* runtime.submitAndBlock(secondCommand);
      assert.deepStrictEqual(calls, [
        { sessionId, commandId: command.commandId },
        { sessionId, commandId: secondCommand.commandId },
      ]);
    }).pipe(
      Effect.provide(
        makeEDARuntimeLayer({
          config: { modelSelection },
          sessionId,
          sessionStoreLayer: EDASessionStore.InMemory(sessionId),
          sinkCheckpointStoreLayer: SinkCheckpointStore.InMemory,
          modelResolverLayer: ModelResolver.Fixed.pipe(
            Layer.provide(makeLanguageModelLayer(finished)),
          ),
          runSchedulerLayer: recordingScheduler(calls),
        }),
      ),
    );
  });

  makeMethods(it).effect("schedules queued followups but not steering or continuation turns", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const calls: Array<RunSchedulingInput> = [];
      yield* Effect.gen(function* () {
        const state = yield* SessionState;
        const store = yield* EDASessionStore;
        yield* state.start({ modelSelection });
        yield* state.admitCommand(command);
        yield* waitForCommitted(store, hasEventType("InferenceStarted"));
        yield* state.admitCommand(secondCommand);
        yield* state.admitCommand(steerCommand);
        yield* waitForCommitted(store, hasCommandCompleted(steerCommand.commandId));
        assert.deepStrictEqual(calls, [{ sessionId, commandId: command.commandId }]);
        yield* Deferred.succeed(release, undefined);
        const committed = yield* waitForCommitted(
          store,
          hasCommandCompleted(secondCommand.commandId),
        );
        assert.deepStrictEqual(calls, [
          { sessionId, commandId: command.commandId },
          { sessionId, commandId: secondCommand.commandId },
        ]);
        assert.strictEqual(committed.filter(({ event }) => event.type === "RunStarted").length, 2);
        // The pending steer forces a second turn in the first run.
        assert.strictEqual(committed.filter(({ event }) => event.type === "TurnStarted").length, 3);
      }).pipe(
        Effect.provide(
          makeEdaTestLayer({
            sessionId,
            runSchedulerLayer: recordingScheduler(calls),
            parts: [
              Stream.unwrap(Deferred.await(release).pipe(Effect.as(finished))),
              finished,
              finished,
            ],
          }),
        ),
      );
    }),
  );

  makeMethods(it).effect("schedules an interrupt replacement but not stop controls", () => {
    const calls: Array<RunSchedulingInput> = [];
    return Effect.gen(function* () {
      const state = yield* SessionState;
      const store = yield* EDASessionStore;
      yield* state.start({ modelSelection });
      yield* state.admitCommand(command);
      yield* waitForCommitted(store, hasEventType("InferenceStarted"));
      yield* state.admitCommand(interruptCommand);
      yield* waitForCommitted(
        store,
        (events) => events.filter(({ event }) => event.type === "InferenceStarted").length === 2,
      );
      yield* state.admitCommand(stopTurnCommand);
      yield* waitForCommitted(store, hasCommandCompleted(stopTurnCommand.commandId));
      assert.deepStrictEqual(calls, [
        { sessionId, commandId: command.commandId },
        { sessionId, commandId: interruptCommand.commandId },
      ]);
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId,
          parts: Stream.never,
          runSchedulerLayer: recordingScheduler(calls),
        }),
      ),
    );
  });

  makeMethods(it).effect("resolves recovery once before creating the replacement run", () => {
    const calls: Array<RunSchedulingInput> = [];
    return Effect.gen(function* () {
      const state = yield* SessionState;
      const events = yield* EventFactory;
      const store = yield* EDASessionStore;
      const runId = RunId.make(RUN_ID);
      yield* state.appendDurableBatch([
        yield* events.commandAdmitted({ command }),
        yield* events.commandStarted({ commandId: command.commandId }),
        yield* events.userMessageCommitted({
          commandId: command.commandId,
          messageId: MessageId.make(USER_MESSAGE_ID),
          content: command.content,
        }),
        yield* events.runStarted({ runId, commandIds: [command.commandId], modelSelection }),
        yield* events.turnStarted({ runId, turnId: TurnId.make(TURN_ID) }),
      ]);
      assert.isEmpty(calls);
      yield* state.start({ modelSelection });
      const committed = yield* waitForCommitted(store, hasCommandCompleted(command.commandId));
      yield* state.start({ modelSelection });
      assert.deepStrictEqual(calls, [{ sessionId, commandId: command.commandId }]);
      assert.strictEqual(committed.filter(({ event }) => event.type === "RunStarted").length, 2);
      assert.strictEqual(
        committed.filter(({ event }) => event.type === "RecoveryCompleted").length,
        1,
      );
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId,
          parts: finished,
          runSchedulerLayer: recordingScheduler(calls),
        }),
      ),
    );
  });

  makeMethods(it).effect("resolves pending-message resumption and skips an empty resume", () => {
    const calls: Array<RunSchedulingInput> = [];
    return Effect.gen(function* () {
      const state = yield* SessionState;
      const events = yield* EventFactory;
      const store = yield* EDASessionStore;
      const messageId = MessageId.make(USER_MESSAGE_ID);
      yield* state.appendDurableBatch([
        yield* events.commandAdmitted({ command }),
        yield* events.commandStarted({ commandId: command.commandId }),
        yield* events.runStarted({
          runId: RunId.make(RUN_ID),
          commandIds: [command.commandId],
          modelSelection,
        }),
        yield* events.steeringMessageQueued({
          commandId: command.commandId,
          messageId,
          runId: RunId.make(RUN_ID),
          content: command.content,
        }),
        yield* events.runCompleted({ runId: RunId.make(RUN_ID) }),
        yield* events.commandCompleted({ commandId: command.commandId }),
      ]);
      yield* state.start({ modelSelection });
      const committed = yield* waitForCommitted(
        store,
        (entries) => entries.filter(({ event }) => event.type === "RunCompleted").length === 2,
      );
      const resumed = committed.find(
        ({ event }) =>
          event.type === "CommandAdmitted" &&
          event.payload.command._tag === "ResumePendingMessages",
      );
      assert.isDefined(resumed);
      assert.lengthOf(calls, 1);
      assert.notStrictEqual(calls[0]?.commandId, command.commandId);
      const emptyResume = new ResumePendingMessagesCommand({
        commandId: CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c9999"),
        messageIds: [messageId],
      });
      yield* state.admitCommand(emptyResume);
      yield* waitForCommitted(store, (events) =>
        events.some(
          ({ event }) =>
            event.type === "CommandCancelled" && event.payload.commandId === emptyResume.commandId,
        ),
      );
      assert.lengthOf(calls, 1);
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId,
          parts: finished,
          runSchedulerLayer: recordingScheduler(calls),
        }),
      ),
    );
  });

  makeMethods(it).effect("does not commit a run when resolution defects", () =>
    Effect.gen(function* () {
      const state = yield* SessionState;
      const store = yield* EDASessionStore;
      yield* state.admitCommand(command);
      const outcome = yield* Effect.exit(state.drainReadyWork({ modelSelection }));
      assert.strictEqual(outcome._tag, "Failure");
      const committed = yield* collectCommitted(store);
      assert.deepStrictEqual(
        committed.map(({ event }) => event.type),
        ["CommandAdmitted", "UserMessageSubmitted"],
      );
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId,
          runSchedulerLayer: Layer.succeed(RunScheduler, {
            resolve: () => Effect.die("scheduler defect"),
          }),
        }),
      ),
    ),
  );
});
