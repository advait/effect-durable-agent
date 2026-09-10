import { assert, makeMethods } from "@effect/vitest";
import { describe, it } from "vite-plus/test";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Response from "effect/unstable/ai/Response";

import { makeEdaTestLayer, type EdaTestLayerOptions } from "../testkit/layers";
import { RunSchedulingRequestRecord, type RunSchedulingRequest } from "../domain/run-scheduling";
import {
  decodeReducedStateCheckpoint,
  encodeReducedStateCheckpoint,
  frameworkReducedStateReducerName,
  reduceCommittedEvents,
} from "../domain/reduced-state";
import { isSessionRecoveryPlanEmpty, planSessionRecovery } from "../domain/recovery-policy";
import {
  CancelPendingMessageCommand,
  EDACommand,
  GrantRunCommand,
  PromotePendingMessageCommand,
  SubmitMessageCommand,
} from "../types/commands";
import { CommandId, SessionId } from "../types/core";
import { type DurableEventEnvelope } from "../types/events";
import { EDASessionStore, EDASessionStoreError, type EDASessionStoreShape } from "./session-store";
import { EDASessionQuery } from "./session-query";
import { SessionState } from "./session-state";
import {
  RunScheduler,
  RunSchedulingDeliveryError,
  type RunSchedulingDelivery,
} from "./run-scheduler";
import { RunSchedulingWakeup } from "./run-scheduling-wakeup";
import { sequentialUuidV7 } from "./id-generator";
import {
  SESSION_ID,
  command,
  secondCommand,
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
const methods = makeMethods(it);

/** Replace only the external authorizer and host wakeup, retaining the real session engine. */
const fixture = (
  options: {
    readonly deliver?: RunSchedulingDelivery;
    readonly seedEvents?: ReadonlyArray<DurableEventEnvelope>;
    readonly wrapStore?: EdaTestLayerOptions["wrapStore"];
    readonly parts?: EdaTestLayerOptions["parts"];
  } = {},
) => {
  const delivered: Array<RunSchedulingRequest> = [];
  let wakeupPending = false;
  const layer = makeEdaTestLayer({
    sessionId,
    clock: "live",
    parts: options.parts ?? finished,
    seedEvents: options.seedEvents,
    ids:
      options.seedEvents === undefined
        ? undefined
        : Array.from({ length: 500 }, (_, i) =>
            sequentialUuidV7(10_000 + (options.seedEvents?.length ?? 0) * 1_000 + i),
          ),
    wrapStore: options.wrapStore,
    runSchedulerLayer: RunScheduler.Deferred((input) =>
      Effect.gen(function* () {
        delivered.push(input.request);
        if (options.deliver !== undefined) yield* options.deliver(input);
      }),
    ),
    runSchedulingWakeupLayer: Layer.succeed(RunSchedulingWakeup, {
      setPending: (pending) =>
        Effect.sync(() => {
          wakeupPending = pending;
        }),
    }),
  });
  return { layer, delivered, wakeupPending: () => wakeupPending };
};

const waitingRequest = Effect.gen(function* () {
  const state = yield* SessionState;
  const store = yield* EDASessionStore;
  yield* waitForCommitted(store, hasEventType("RunSchedulingRequested"));
  return Schema.decodeUnknownSync(RunSchedulingRequestRecord)(
    (yield* state.snapshot()).runSchedulingRequest,
  );
});

const settleDelivery = (store: EDASessionStoreShape) =>
  waitForCommitted(store, hasEventType("RunSchedulingDelivered"));

describe("durable run authorization", () => {
  it("excludes grants from ordinary user command ingress", () => {
    assert.throws(() =>
      Schema.decodeUnknownSync(EDACommand)({ _tag: "GrantRun", requestId: sequentialUuidV7(1) }),
    );
  });

  methods.effect("persists before delivery and parks without starting a command or run", () => {
    const test = fixture();
    return Effect.gen(function* () {
      const state = yield* SessionState;
      const store = yield* EDASessionStore;
      yield* state.start({ modelSelection });
      yield* state.admitCommand(command);
      const request = yield* waitingRequest;
      yield* settleDelivery(store);
      yield* state.retryRunSchedulingDelivery();
      const snapshot = yield* state.snapshot();
      assert.strictEqual(test.delivered[0]?.requestId, request.requestId);
      assert.isDefined(snapshot.runSchedulingRequest?.deliveredSeq);
      assert.isUndefined(snapshot.commands.get(command.commandId)?.startedSeq);
      assert.strictEqual(snapshot.runs.size, 0);
      assert.isFalse(test.wakeupPending());
      const drained = yield* state.drainReadyWork({ modelSelection });
      assert.strictEqual(drained.stop.reason, "awaiting-run-authorization");
      assert.deepStrictEqual(
        (yield* collectCommitted(store)).map((item) => item.event.type),
        [
          "CommandAdmitted",
          "UserMessageSubmitted",
          "RunSchedulingRequested",
          "RunSchedulingDelivered",
        ],
      );
    }).pipe(Effect.provide(test.layer));
  });

  methods.effect(
    "consumes one grant atomically, ignores duplicates, and reserves FIFO work separately",
    () => {
      const batches: Array<ReadonlyArray<string>> = [];
      const test = fixture({
        wrapStore: (inner) => ({
          ...inner,
          append: (batch) =>
            Effect.gen(function* () {
              batches.push(batch.entries.map((item) => item.event.type));
              return yield* inner.append(batch);
            }),
        }),
      });
      return Effect.gen(function* () {
        const state = yield* SessionState;
        const store = yield* EDASessionStore;
        yield* state.start({ modelSelection });
        yield* state.admitCommand(command);
        const first = yield* waitingRequest;
        yield* state.admitCommand(secondCommand);
        assert.strictEqual(
          (yield* state.snapshot()).runSchedulingRequest?.requestId,
          first.requestId,
        );
        const grant = new GrantRunCommand({ requestId: first.requestId });
        const result = yield* state.grantRun(grant, { modelSelection });
        assert.strictEqual(result._tag, "Granted");
        assert.deepStrictEqual(yield* state.grantRun(grant, { modelSelection }), { _tag: "Stale" });
        yield* waitForCommitted(store, hasCommandCompleted(command.commandId));
        yield* waitForCommitted(
          store,
          (entries) =>
            entries.filter((item) => item.event.type === "RunSchedulingRequested").length === 2,
        );
        const next = Schema.decodeUnknownSync(RunSchedulingRequestRecord)(
          (yield* state.snapshot()).runSchedulingRequest,
        );
        assert.notStrictEqual(next.requestId, first.requestId);
        assert.strictEqual(next.work.commandId, secondCommand.commandId);
        assert.strictEqual((yield* state.snapshot()).runs.size, 1);
        yield* state.grantRun(new GrantRunCommand({ requestId: next.requestId }), {
          modelSelection,
        });
        const committed = yield* waitForCommitted(
          store,
          hasCommandCompleted(secondCommand.commandId),
        );
        assert.strictEqual(committed.filter((item) => item.event.type === "RunStarted").length, 2);
        assert.isTrue(
          batches
            .filter((batch) => batch.includes("RunSchedulingGranted"))
            .every((batch) => batch.includes("RunStarted")),
        );
      }).pipe(Effect.provide(test.layer));
    },
  );

  for (const failure of ["typed", "defect"] as const) {
    methods.effect(
      `does not acknowledge ${failure} delivery failures and retries the same identity`,
      () => {
        let calls = 0;
        const test = fixture({
          deliver: () =>
            Effect.suspend(() => {
              calls += 1;
              if (calls > 1) return Effect.void;
              return failure === "typed"
                ? Effect.fail(new RunSchedulingDeliveryError({ message: "authorizer unavailable" }))
                : Effect.die("delivery defect");
            }),
        });
        return Effect.gen(function* () {
          const state = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* state.admitCommand(command);
          yield* state.drainReadyWork({ modelSelection });
          const request = yield* waitingRequest;
          for (let i = 0; i < 10; i += 1) yield* Effect.yieldNow;
          assert.isUndefined((yield* state.snapshot()).runSchedulingRequest?.deliveredSeq);
          assert.isTrue(test.wakeupPending());
          yield* state.retryRunSchedulingDelivery();
          yield* settleDelivery(store);
          assert.deepStrictEqual(
            test.delivered.map((item) => item.requestId),
            [request.requestId, request.requestId],
          );
          assert.isFalse(test.wakeupPending());
        }).pipe(Effect.provide(test.layer));
      },
    );
  }

  methods.effect(
    "processes stop while outbound delivery is blocked and rejects its late grant",
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const test = fixture({
          deliver: () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }),
        });
        yield* Effect.gen(function* () {
          const state = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* state.start({ modelSelection });
          yield* state.admitCommand(command);
          const request = yield* waitingRequest;
          yield* Deferred.await(entered);
          yield* state.admitCommand(secondCommand);
          yield* state.admitCommand(stopTurnCommand);
          yield* waitForCommitted(store, hasCommandCompleted(stopTurnCommand.commandId));
          yield* Deferred.succeed(release, undefined);
          const result = yield* state.grantRun(
            new GrantRunCommand({ requestId: request.requestId }),
            { modelSelection },
          );
          assert.deepStrictEqual(result, { _tag: "Stale" });
          assert.deepStrictEqual(
            yield* (yield* EDASessionQuery).runRequestOutcome(request.requestId),
            { _tag: "Invalidated" },
          );
          const snapshot = yield* state.snapshot();
          assert.isUndefined(snapshot.runSchedulingRequest);
          assert.strictEqual(snapshot.runs.size, 0);
          assert.lengthOf(snapshot.commandQueues.pausedQueue, 2);
          assert.strictEqual(snapshot.commands.get(command.commandId)?.terminal?._tag, "Cancelled");
          assert.isFalse(test.wakeupPending());
        }).pipe(Effect.provide(test.layer));
      }),
  );

  methods.effect("an interrupt supersedes a waiting reservation and needs its own grant", () => {
    const test = fixture();
    return Effect.gen(function* () {
      const state = yield* SessionState;
      const store = yield* EDASessionStore;
      yield* state.start({ modelSelection });
      yield* state.admitCommand(command);
      const first = yield* waitingRequest;
      yield* state.admitCommand(interruptCommand);
      yield* waitForCommitted(
        store,
        (entries) =>
          entries.filter((item) => item.event.type === "RunSchedulingRequested").length === 2,
      );
      const next = Schema.decodeUnknownSync(RunSchedulingRequestRecord)(
        (yield* state.snapshot()).runSchedulingRequest,
      );
      assert.strictEqual(next.work.commandId, interruptCommand.commandId);
      assert.deepStrictEqual(
        yield* state.grantRun(new GrantRunCommand({ requestId: first.requestId }), {
          modelSelection,
        }),
        { _tag: "Stale" },
      );
      yield* state.grantRun(new GrantRunCommand({ requestId: next.requestId }), { modelSelection });
      yield* waitForCommitted(store, hasCommandCompleted(interruptCommand.commandId));
      assert.strictEqual((yield* state.snapshot()).runs.size, 1);
      assert.lengthOf((yield* state.snapshot()).commandQueues.pausedQueue, 1);
      assert.strictEqual(
        (yield* state.snapshot()).commands.get(command.commandId)?.terminal?._tag,
        "Cancelled",
      );
    }).pipe(Effect.provide(test.layer));
  });

  for (const interruption of ["stop", "interrupt"] as const) {
    methods.effect(`promotion after ${interruption} has a fresh owner across replay`, () =>
      Effect.gen(function* () {
        const test = fixture();
        const before = yield* Effect.gen(function* () {
          const state = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* state.start({ modelSelection });
          yield* state.admitCommand(command);
          const original = yield* waitingRequest;
          yield* state.admitCommand(interruption === "stop" ? stopTurnCommand : interruptCommand);
          if (interruption === "stop") {
            yield* waitForCommitted(store, hasCommandCompleted(stopTurnCommand.commandId));
          } else {
            yield* waitForCommitted(
              store,
              (entries) =>
                entries.filter((item) => item.event.type === "RunSchedulingRequested").length === 2,
            );
            const replacement = Schema.decodeUnknownSync(RunSchedulingRequestRecord)(
              (yield* state.snapshot()).runSchedulingRequest,
            );
            yield* state.grantRun(new GrantRunCommand({ requestId: replacement.requestId }), {
              modelSelection,
            });
            yield* waitForCommitted(store, hasCommandCompleted(interruptCommand.commandId));
          }
          const paused = (yield* state.snapshot()).commandQueues.pausedQueue[0];
          assert.isDefined(paused);
          if (paused === undefined) return yield* Effect.die("Missing paused message");
          const promotion = new PromotePendingMessageCommand({
            commandId: CommandId.make(sequentialUuidV7(9_100)),
            messageId: paused.messageId,
          });
          yield* state.admitCommand(promotion);
          yield* state.drainReadyWork({ modelSelection });
          const committed = yield* collectCommitted(store);
          const completedIndex = committed.findIndex(
            (item) =>
              item.event.type === "CommandCompleted" &&
              item.event.payload.commandId === promotion.commandId,
          );
          assert.isAtLeast(completedIndex, 0);
          return {
            original,
            messageId: paused.messageId,
            events: committed.slice(0, completedIndex + 1).map((item) => item.event),
          };
        }).pipe(Effect.provide(test.layer));
        const replayed = fixture({ seedEvents: before.events });
        yield* Effect.gen(function* () {
          const state = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* state.start({ modelSelection });
          yield* state.drainReadyWork({ modelSelection });
          const request = Schema.decodeUnknownSync(RunSchedulingRequestRecord)(
            (yield* state.snapshot()).runSchedulingRequest,
          );
          assert.notStrictEqual(request.work.commandId, command.commandId);
          assert.strictEqual(
            (yield* state.snapshot()).commands.get(request.work.commandId)?.command?._tag,
            "ResumePendingMessages",
          );
          assert.deepStrictEqual(
            yield* state.grantRun(new GrantRunCommand({ requestId: before.original.requestId }), {
              modelSelection,
            }),
            { _tag: "Stale" },
          );
          yield* state.grantRun(new GrantRunCommand({ requestId: request.requestId }), {
            modelSelection,
          });
          yield* waitForCommitted(store, hasCommandCompleted(request.work.commandId));
          const snapshot = yield* state.snapshot();
          assert.strictEqual(snapshot.commands.get(command.commandId)?.terminal?._tag, "Cancelled");
          assert.isDefined(snapshot.messages.get(before.messageId)?.consumedSeq);
          assert.lengthOf(snapshot.commandQueues.pendingSteers, 0);
        }).pipe(Effect.provide(replayed.layer));
      }),
    );
  }

  methods.effect("cancellation revokes the request and cannot revive its terminal command", () => {
    const test = fixture();
    return Effect.gen(function* () {
      const state = yield* SessionState;
      const store = yield* EDASessionStore;
      yield* state.start({ modelSelection });
      yield* state.admitCommand(command);
      const request = yield* waitingRequest;
      const pending = (yield* state.snapshot()).commandQueues.pendingQueue[0];
      assert.isDefined(pending);
      if (pending === undefined) return;
      const cancel = new CancelPendingMessageCommand({
        commandId: CommandId.make(sequentialUuidV7(9_001)),
        messageId: pending.messageId,
        reason: "user-cancel",
      });
      yield* state.admitCommand(cancel);
      yield* waitForCommitted(store, hasCommandCompleted(cancel.commandId));
      assert.deepStrictEqual(
        yield* state.grantRun(new GrantRunCommand({ requestId: request.requestId }), {
          modelSelection,
        }),
        { _tag: "Stale" },
      );
      assert.isUndefined((yield* state.snapshot()).runSchedulingRequest);
      assert.strictEqual((yield* state.snapshot()).runs.size, 0);
      assert.lengthOf((yield* state.snapshot()).commandQueues.pendingCommands, 0);
    }).pipe(Effect.provide(test.layer));
  });

  for (const delivered of [false, true]) {
    methods.effect(
      `restart preserves ${delivered ? "acknowledged" : "undelivered"} request identity`,
      () =>
        Effect.gen(function* () {
          const original = fixture({
            deliver: () =>
              delivered
                ? Effect.void
                : Effect.fail(new RunSchedulingDeliveryError({ message: "offline" })),
          });
          const before = yield* Effect.gen(function* () {
            const state = yield* SessionState;
            const store = yield* EDASessionStore;
            yield* state.start({ modelSelection });
            yield* state.admitCommand(command);
            const request = yield* waitingRequest;
            if (delivered) yield* settleDelivery(store);
            return { request, events: (yield* collectCommitted(store)).map((item) => item.event) };
          }).pipe(Effect.provide(original.layer));
          const restarted = fixture({ seedEvents: before.events });
          yield* Effect.gen(function* () {
            const state = yield* SessionState;
            const store = yield* EDASessionStore;
            yield* state.start({ modelSelection });
            yield* settleDelivery(store);
            assert.strictEqual(
              (yield* state.snapshot()).runSchedulingRequest?.requestId,
              before.request.requestId,
            );
            assert.lengthOf(restarted.delivered, delivered ? 0 : 1);
            assert.strictEqual(
              (yield* collectCommitted(store)).filter(
                (item) => item.event.type === "RunSchedulingRequested",
              ).length,
              1,
            );
            yield* state.grantRun(new GrantRunCommand({ requestId: before.request.requestId }), {
              modelSelection,
            });
            yield* waitForCommitted(store, hasCommandCompleted(command.commandId));
          }).pipe(Effect.provide(restarted.layer));
        }),
    );
  }

  methods.effect(
    "accepts a callback before delivery returns without deadlocking or acknowledging stale work",
    () => {
      let grant: RunSchedulingDelivery | undefined;
      const test = fixture({
        deliver: (input) =>
          Effect.suspend(() => {
            if (grant === undefined) return Effect.die("grant callback not installed");
            return grant(input);
          }),
      });
      return Effect.gen(function* () {
        const state = yield* SessionState;
        const store = yield* EDASessionStore;
        grant = (input) =>
          state
            .grantRun(new GrantRunCommand({ requestId: input.request.requestId }), {
              modelSelection,
            })
            .pipe(Effect.orDie, Effect.asVoid);
        yield* state.start({ modelSelection });
        yield* state.admitCommand(command);
        const committed = yield* waitForCommitted(store, hasCommandCompleted(command.commandId));
        assert.lengthOf(test.delivered, 1);
        assert.strictEqual(
          committed.filter((item) => item.event.type === "RunSchedulingGranted").length,
          1,
        );
        assert.isFalse(committed.some((item) => item.event.type === "RunSchedulingDelivered"));
        assert.isUndefined((yield* state.snapshot()).runSchedulingRequest);
      }).pipe(Effect.provide(test.layer));
    },
  );

  methods.effect(
    "a failed grant append commits neither permission nor run and restart reuses the request",
    () =>
      Effect.gen(function* () {
        const failing = fixture({
          wrapStore: (inner) => ({
            ...inner,
            append: (batch) =>
              batch.entries.some((item) => item.event.type === "RunSchedulingGranted")
                ? Effect.fail(new EDASessionStoreError({ message: "crash before grant commit" }))
                : inner.append(batch),
          }),
        });
        const before = yield* Effect.gen(function* () {
          const state = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* state.start({ modelSelection });
          yield* state.admitCommand(command);
          const request = yield* waitingRequest;
          yield* settleDelivery(store);
          const result = yield* Effect.exit(
            state.grantRun(new GrantRunCommand({ requestId: request.requestId }), {
              modelSelection,
            }),
          );
          assert.strictEqual(result._tag, "Failure");
          const entries = yield* collectCommitted(store);
          assert.isFalse(
            entries.some(
              (item) =>
                item.event.type === "RunSchedulingGranted" || item.event.type === "RunStarted",
            ),
          );
          return { request, events: entries.map((item) => item.event) };
        }).pipe(Effect.provide(failing.layer));
        const restarted = fixture({ seedEvents: before.events });
        yield* Effect.gen(function* () {
          const state = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* state.start({ modelSelection });
          assert.strictEqual(
            (yield* state.snapshot()).runSchedulingRequest?.requestId,
            before.request.requestId,
          );
          yield* state.grantRun(new GrantRunCommand({ requestId: before.request.requestId }), {
            modelSelection,
          });
          yield* waitForCommitted(store, hasCommandCompleted(command.commandId));
        }).pipe(Effect.provide(restarted.layer));
      }),
  );

  methods.effect(
    "a crash after RunStarted requires a new recovery grant, preserved through another restart",
    () =>
      Effect.gen(function* () {
        const crashing = fixture({
          wrapStore: (inner) => ({
            ...inner,
            append: (batch) =>
              batch.entries.some((item) => item.event.type === "TurnStarted")
                ? Effect.fail(new EDASessionStoreError({ message: "crash before first turn" }))
                : inner.append(batch),
          }),
        });
        const before = yield* Effect.gen(function* () {
          const state = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* state.start({ modelSelection });
          yield* state.admitCommand(command);
          const request = yield* waitingRequest;
          yield* settleDelivery(store);
          const result = yield* Effect.exit(
            state.grantRun(new GrantRunCommand({ requestId: request.requestId }), {
              modelSelection,
            }),
          );
          assert.strictEqual(result._tag, "Failure");
          const snapshot = yield* state.snapshot();
          assert.isUndefined(snapshot.runSchedulingRequest);
          assert.strictEqual(snapshot.runs.size, 1);
          return { request, events: (yield* collectCommitted(store)).map((item) => item.event) };
        }).pipe(Effect.provide(crashing.layer));
        const recoveryBatches: Array<ReadonlyArray<string>> = [];
        const restarting = fixture({
          seedEvents: before.events,
          wrapStore: (inner) => ({
            ...inner,
            append: (batch) =>
              Effect.gen(function* () {
                recoveryBatches.push(batch.entries.map((item) => item.event.type));
                return yield* inner.append(batch);
              }),
          }),
        });
        const waiting = yield* Effect.gen(function* () {
          const state = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* state.start({ modelSelection });
          yield* settleDelivery(store);
          const snapshot = yield* state.snapshot();
          const request = Schema.decodeUnknownSync(RunSchedulingRequestRecord)(
            snapshot.runSchedulingRequest,
          );
          assert.notStrictEqual(request.requestId, before.request.requestId);
          assert.strictEqual(request.work._tag, "Recovery");
          assert.isTrue(isSessionRecoveryPlanEmpty(planSessionRecovery(snapshot)));
          assert.isTrue(
            recoveryBatches.some(
              (batch) =>
                batch.includes("RunFailed") &&
                batch.includes("RunSchedulingRequested") &&
                batch.includes("RecoveryCompleted"),
            ),
          );
          assert.deepStrictEqual(
            yield* state.grantRun(new GrantRunCommand({ requestId: before.request.requestId }), {
              modelSelection,
            }),
            { _tag: "Stale" },
          );
          const committed = yield* collectCommitted(store);
          assert.deepStrictEqual(
            decodeReducedStateCheckpoint(encodeReducedStateCheckpoint(snapshot), committed),
            snapshot,
          );
          return { request, events: committed.map((item) => item.event) };
        }).pipe(Effect.provide(restarting.layer));
        const resumed = fixture({ seedEvents: waiting.events });
        yield* Effect.gen(function* () {
          const state = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* state.start({ modelSelection });
          assert.strictEqual(
            (yield* state.snapshot()).runSchedulingRequest?.requestId,
            waiting.request.requestId,
          );
          yield* state.grantRun(new GrantRunCommand({ requestId: waiting.request.requestId }), {
            modelSelection,
          });
          const committed = yield* waitForCommitted(store, hasCommandCompleted(command.commandId));
          const snapshot = yield* state.snapshot();
          assert.strictEqual(snapshot.runs.size, 2);
          assert.strictEqual(snapshot.recoveryContinuations.size, 1);
          assert.strictEqual(
            committed.filter((item) => item.event.type === "RunSchedulingRequested").length,
            2,
          );
          assert.isUndefined(snapshot.runSchedulingRequest);
        }).pipe(Effect.provide(resumed.layer));
      }),
  );

  for (const cancelLast of [false, true]) {
    methods.effect(
      `recovery cancellation ${cancelLast ? "removes the last input" : "retains another input"} across restart`,
      () =>
        Effect.gen(function* () {
          const original = fixture({ parts: Stream.never });
          const interrupted = yield* Effect.gen(function* () {
            const state = yield* SessionState;
            const store = yield* EDASessionStore;
            yield* state.start({ modelSelection });
            yield* state.admitCommand(command);
            const request = yield* waitingRequest;
            const granted = yield* state.grantRun(
              new GrantRunCommand({ requestId: request.requestId }),
              { modelSelection },
            );
            if (granted._tag !== "Granted") return yield* Effect.die("Expected initial grant");
            for (const index of [0, 1]) {
              const steer = new SubmitMessageCommand({
                commandId: CommandId.make(sequentialUuidV7(9_200 + index)),
                content: command.content,
                disposition: "steer",
              });
              yield* state.admitCommand(steer);
              yield* waitForCommitted(store, hasCommandCompleted(steer.commandId));
            }
            return {
              runId: granted.runId,
              events: (yield* collectCommitted(store)).map((item) => item.event),
            };
          }).pipe(Effect.provide(original.layer));
          const recovering = fixture({ seedEvents: interrupted.events });
          const afterCancel = yield* Effect.gen(function* () {
            const state = yield* SessionState;
            const store = yield* EDASessionStore;
            yield* state.start({ modelSelection });
            const first = Schema.decodeUnknownSync(RunSchedulingRequestRecord)(
              (yield* state.snapshot()).runSchedulingRequest,
            );
            if (first.work._tag !== "Recovery")
              return yield* Effect.die("Expected recovery request");
            assert.lengthOf(first.work.inputMessageIds, 2);
            const [firstMessageId, secondMessageId] = first.work.inputMessageIds;
            if (firstMessageId === undefined || secondMessageId === undefined)
              return yield* Effect.die("Expected two inputs");
            const cancel = new CancelPendingMessageCommand({
              commandId: CommandId.make(sequentialUuidV7(9_300)),
              messageId: firstMessageId,
              reason: "user-cancel",
            });
            yield* state.admitCommand(cancel);
            yield* waitForCommitted(store, hasCommandCompleted(cancel.commandId));
            const replacement = Schema.decodeUnknownSync(RunSchedulingRequestRecord)(
              (yield* state.snapshot()).runSchedulingRequest,
            );
            assert.notStrictEqual(replacement.requestId, first.requestId);
            assert.deepStrictEqual(replacement.work, {
              _tag: "Recovery",
              commandId: command.commandId,
              interruptedRunId: interrupted.runId,
              inputMessageIds: [secondMessageId],
            });
            assert.deepStrictEqual(
              yield* state.grantRun(new GrantRunCommand({ requestId: first.requestId }), {
                modelSelection,
              }),
              { _tag: "Stale" },
            );
            if (cancelLast) {
              const last = new CancelPendingMessageCommand({
                commandId: CommandId.make(sequentialUuidV7(9_301)),
                messageId: secondMessageId,
                reason: "user-cancel",
              });
              yield* state.admitCommand(last);
              yield* waitForCommitted(store, hasCommandCompleted(last.commandId));
              assert.isUndefined((yield* state.snapshot()).runSchedulingRequest);
            }
            return {
              request: replacement,
              events: (yield* collectCommitted(store)).map((item) => item.event),
            };
          }).pipe(Effect.provide(recovering.layer));
          const restarted = fixture({ seedEvents: afterCancel.events });
          yield* Effect.gen(function* () {
            const state = yield* SessionState;
            const store = yield* EDASessionStore;
            yield* state.start({ modelSelection });
            const result = yield* state.grantRun(
              new GrantRunCommand({ requestId: afterCancel.request.requestId }),
              { modelSelection },
            );
            if (cancelLast) {
              assert.deepStrictEqual(result, { _tag: "Stale" });
              const snapshot = yield* state.snapshot();
              assert.isUndefined(snapshot.runSchedulingRequest);
              assert.strictEqual(
                snapshot.commands.get(command.commandId)?.terminal?._tag,
                "Cancelled",
              );
              assert.strictEqual(snapshot.runs.size, 1);
            } else {
              assert.strictEqual(result._tag, "Granted");
              yield* waitForCommitted(store, hasCommandCompleted(command.commandId));
              const snapshot = yield* state.snapshot();
              assert.strictEqual(snapshot.runs.size, 2);
              assert.strictEqual(snapshot.recoveryContinuations.size, 1);
            }
          }).pipe(Effect.provide(restarted.layer));
        }),
    );
  }

  methods.effect(
    "rebuilds the deployed schema-6 checkpoint from retained history before deferring",
    () =>
      Effect.gen(function* () {
        const baseline = fixture();
        const prefix = yield* Effect.gen(function* () {
          const state = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* state.admitCommand(command);
          return yield* collectCommitted(store);
        }).pipe(Effect.provide(baseline.layer));
        const original = reduceCommittedEvents(prefix);
        const oldCheckpoint = {
          name: frameworkReducedStateReducerName,
          schemaVersion: 6,
          throughSeq: original.lastSeq,
          payload: encodeReducedStateCheckpoint(original),
          updatedAtMs: 0,
        };
        const test = fixture({
          seedEvents: prefix.map((item) => item.event),
          wrapStore: (inner) => ({
            ...inner,
            loadReducerCheckpoint: (name) =>
              name === frameworkReducedStateReducerName
                ? Effect.succeed(oldCheckpoint)
                : inner.loadReducerCheckpoint(name),
          }),
        });
        yield* Effect.gen(function* () {
          const state = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* state.start({ modelSelection });
          const request = yield* waitingRequest;
          yield* state.grantRun(new GrantRunCommand({ requestId: request.requestId }), {
            modelSelection,
          });
          yield* waitForCommitted(store, hasCommandCompleted(command.commandId));
          assert.strictEqual((yield* state.snapshot()).runs.size, 1);
          const retained = (yield* collectCommitted(store)).slice(0, prefix.length);
          assert.deepStrictEqual(retained, prefix);
        }).pipe(Effect.provide(test.layer));
      }),
  );
});
