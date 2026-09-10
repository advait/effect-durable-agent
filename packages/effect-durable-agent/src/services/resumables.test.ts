import { assert, makeMethods } from "@effect/vitest";
import { describe, it } from "vite-plus/test";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";

import { decideDispatch } from "../domain/dispatch-policy";
import {
  encodeReducedStateCheckpoint,
  decodeReducedStateCheckpoint,
} from "../domain/reduced-state";
import { ResumableHandle } from "../domain/resumables";
import { GrantRunCommand, ResumeResumableCommand } from "../types/commands";
import { ResumableId, SessionId } from "../types/core";
import { DurableEventEnvelope, UnixEpochMillis } from "../types/events";
import { makeEdaTestLayer } from "../testkit/layers";
import { EDAToolRegistry, type EDAToolExecutionContext } from "./tool-registry";
import { SessionState } from "./session-state";
import { EDASessionStore, EDASessionStoreError, type CommittedDurableEvent } from "./session-store";
import { RunScheduler } from "./run-scheduler";
import { RunSchedulingWakeup } from "./run-scheduling-wakeup";
import { sequentialUuidV7 } from "./id-generator";
import {
  SESSION_ID,
  command,
  secondCommand,
  stopTurnCommand,
  interruptCommand,
  modelSelection,
  usage,
  collectCommitted,
  waitForCommitted,
  hasCommandCompleted,
} from "./session-state-control-testkit";

const sessionId = SessionId.make(SESSION_ID);
const finished = Stream.make(
  Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
);
const spawn = Stream.make(
  Response.makePart("tool-call", {
    id: "external-work",
    name: "spawn",
    params: {},
    providerExecuted: false,
  }),
  Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
);
const registry = (execute: (context: EDAToolExecutionContext) => Effect.Effect<unknown, unknown>) =>
  EDAToolRegistry.FromShape({
    getParamsSchema: () => Effect.succeed(Schema.Struct({})),
    getModelToolkit: () => Effect.succeed(undefined),
    execute: (_name, _params, context) => execute(context),
  });

const open = (context: EDAToolExecutionContext) =>
  context.openResumable({ kind: "test.child", title: "Waiting for child" }, (handle) =>
    Effect.gen(function* () {
      return [
        DurableEventEnvelope.make({
          namespace: "test",
          type: "ChildRequested",
          schemaVersion: 1,
          durability: "durable",
          sessionId,
          eventId: yield* context.makeEventId(),
          createdAtMs: UnixEpochMillis.make(yield* Clock.currentTimeMillis),
          payload: handle,
        }),
      ];
    }),
  );

const resumeAdmissions = (entries: ReadonlyArray<CommittedDurableEvent>) =>
  entries.filter((entry) => {
    if (entry.event.type !== "CommandAdmitted") return false;
    const payload = entry.event.payload;
    return (
      typeof payload === "object" &&
      payload !== null &&
      "command" in payload &&
      Schema.is(ResumeResumableCommand)(payload.command)
    );
  });

describe("durable resumables", () => {
  makeMethods(it).effect(
    "recovers every durable boundary from open through result delivery without duplicating a continuation",
    () =>
      Effect.gen(function* () {
        const prefixes: Array<ReadonlyArray<DurableEventEnvelope>> = [];
        let committed: Array<DurableEventEnvelope> = [];
        const golden = yield* Effect.scoped(
          Effect.gen(function* () {
            const state = yield* SessionState;
            const store = yield* EDASessionStore;
            yield* state.start({ modelSelection });
            yield* state.admitCommand(command);
            yield* waitForCommitted(store, hasCommandCompleted(command.commandId));
            const [handle] = (yield* state.snapshot()).resumables.values();
            if (handle === undefined) return yield* Effect.die("missing handle");
            const resolved = yield* state.resolveResumable({
              resumableId: handle.resumableId,
              result: "REPLAY_RESULT",
            });
            if (resolved._tag !== "Resolved") return yield* Effect.die("missing resolution");
            yield* waitForCommitted(store, hasCommandCompleted(resolved.commandId));
            return { handle, checkpoint: encodeReducedStateCheckpoint(yield* state.snapshot()) };
          }).pipe(
            Effect.provide(
              makeEdaTestLayer({
                sessionId,
                parts: [spawn, finished],
                toolRegistryLayer: registry(open),
                wrapStore: (inner) => ({
                  ...inner,
                  append: (input) =>
                    Effect.gen(function* () {
                      const result = yield* inner.append(input);
                      committed = [...committed, ...result.map((entry) => entry.event)];
                      if (committed.some((event) => event.type === "ResumableOpened"))
                        prefixes.push([...committed]);
                      return result;
                    }),
                }),
              }),
            ),
          ),
        );
        assert.isAbove(prefixes.length, 5);
        for (const seed of prefixes) {
          const prompts: Array<Prompt.RawInput> = [];
          yield* Effect.scoped(
            Effect.gen(function* () {
              const state = yield* SessionState;
              const store = yield* EDASessionStore;
              yield* state.start({ modelSelection });
              if (!seed.some((event) => event.type === "ResumableResolved")) {
                assert.strictEqual(
                  decideDispatch(yield* state.snapshot())._tag,
                  "DispatchWaitingOnResumables",
                );
                assert.lengthOf(prompts, 0);
              }
              const resolved = yield* state.resolveResumable({
                resumableId: golden.handle.resumableId,
                result: "REPLAY_RESULT",
              });
              if (resolved._tag !== "Resolved")
                return yield* Effect.die("missing replay resolution");
              yield* state.resolveResumable({
                resumableId: golden.handle.resumableId,
                result: "DUPLICATE",
              });
              const all = yield* waitForCommitted(store, hasCommandCompleted(resolved.commandId));
              assert.lengthOf(resumeAdmissions(all), 1);
              assert.lengthOf(
                all.filter((entry) => entry.event.type === "ResumableResolved"),
                1,
              );
              assert.lengthOf(
                all.filter((entry) => entry.event.type === "ChildRequested"),
                1,
              );
              assert.isAtMost(prompts.length, 1);
            }).pipe(
              Effect.provide(
                makeEdaTestLayer({
                  sessionId,
                  seedEvents: seed,
                  parts: finished,
                  ids: Array.from({ length: 256 }, (_, index) => sequentialUuidV7(10_000 + index)),
                  onStreamText: ({ prompt }) => prompts.push(prompt),
                }),
              ),
            ),
          );
        }
        // Version 7 is deployed: discard its derived cache and replay the retained event log.
        const { resumables: _newField, ...oldCheckpoint } = golden.checkpoint;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const state = yield* SessionState;
            yield* state.start({ modelSelection });
            assert.strictEqual(
              (yield* state.snapshot()).resumables.get(golden.handle.resumableId)?.settlement._tag,
              "Resolved",
            );
          }).pipe(
            Effect.provide(
              makeEdaTestLayer({
                sessionId,
                seedEvents: committed,
                wrapStore: (inner) => ({
                  ...inner,
                  loadReducerCheckpoint: (name) =>
                    Effect.succeed({
                      name,
                      schemaVersion: 7,
                      throughSeq: golden.checkpoint.lastSeq,
                      payload: oldCheckpoint,
                      updatedAtMs: 0,
                    }),
                }),
              }),
            ),
          ),
        );
      }),
  );
  makeMethods(it).effect(
    "ends the parent run, holds queued messages, and resumes through fresh admission once",
    () => {
      const prompts: Array<Prompt.RawInput> = [];
      const batches: Array<ReadonlyArray<string>> = [];
      return Effect.gen(function* () {
        const state = yield* SessionState;
        const store = yield* EDASessionStore;
        yield* state.start({ modelSelection });
        yield* state.admitCommand(command);
        yield* waitForCommitted(store, hasCommandCompleted(command.commandId));
        const waiting = yield* state.snapshot();
        assert.strictEqual(waiting.commandQueues.active, undefined);
        assert.strictEqual(decideDispatch(waiting)._tag, "DispatchWaitingOnResumables");
        assert.lengthOf(prompts, 1);
        assert.isTrue(
          batches.some(
            (batch) => batch.includes("ResumableOpened") && batch.includes("ChildRequested"),
          ),
        );
        yield* state.admitCommand(secondCommand);
        yield* state.drainReadyWork({ modelSelection });
        assert.lengthOf(prompts, 1);
        const [handle] = waiting.resumables.values();
        assert.isDefined(handle);
        if (handle === undefined) return yield* Effect.die("missing handle");
        const resolved = yield* state.resolveResumable({
          resumableId: handle.resumableId,
          result: "CHILD_RESULT",
        });
        assert.strictEqual(resolved._tag, "Resolved");
        assert.deepStrictEqual(
          yield* state.resolveResumable({ resumableId: handle.resumableId, result: "DUPLICATE" }),
          resolved,
        );
        yield* waitForCommitted(store, hasCommandCompleted(secondCommand.commandId));
        const all = yield* collectCommitted(store);
        assert.lengthOf(resumeAdmissions(all), 1);
        assert.lengthOf(
          all.filter((entry) => entry.event.type === "RunStarted"),
          3,
        );
        assert.lengthOf(prompts, 3);
        assert.include(JSON.stringify(prompts[1]), "CHILD_RESULT");
        assert.notInclude(JSON.stringify(prompts[1]), "DUPLICATE");
        assert.isTrue(
          batches.some(
            (batch) => batch.includes("ResumableResolved") && batch.includes("CommandAdmitted"),
          ),
        );
        const checkpoint = encodeReducedStateCheckpoint(yield* state.snapshot());
        assert.deepStrictEqual(
          decodeReducedStateCheckpoint(checkpoint, all).resumables,
          (yield* state.snapshot()).resumables,
        );
      }).pipe(
        Effect.provide(
          makeEdaTestLayer({
            sessionId,
            parts: [spawn, finished, finished],
            toolRegistryLayer: registry(open),
            onStreamText: ({ prompt }) => prompts.push(prompt),
            wrapStore: (inner) => ({
              ...inner,
              append: (input) => {
                batches.push(input.entries.map((entry) => entry.event.type));
                return inner.append(input);
              },
            }),
          }),
        ),
      );
    },
  );

  makeMethods(it).effect(
    "an early result still ends the opening run before starting a continuation",
    () =>
      Effect.gen(function* () {
        const opened = yield* Deferred.make<ResumableHandle>();
        const release = yield* Deferred.make<void>();
        const prompts: Array<Prompt.RawInput> = [];
        yield* Effect.gen(function* () {
          const state = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* state.start({ modelSelection });
          yield* state.admitCommand(command);
          const handle = yield* Deferred.await(opened);
          const resolution = yield* state.resolveResumable({
            resumableId: handle.resumableId,
            result: "EARLY",
          });
          assert.strictEqual(resolution._tag, "Resolved");
          assert.lengthOf(prompts, 1);
          yield* Deferred.succeed(release, undefined);
          const all = yield* waitForCommitted(
            store,
            (entries) =>
              entries.filter((entry) => entry.event.type === "RunCompleted").length === 2,
          );
          assert.lengthOf(prompts, 2);
          assert.lengthOf(resumeAdmissions(all), 1);
          const ended = all.findIndex((entry) => entry.event.type === "RunCompleted");
          const starts = all.flatMap((entry, index) =>
            entry.event.type === "RunStarted" ? [index] : [],
          );
          assert.isAbove(starts[1] ?? -1, ended);
        }).pipe(
          Effect.provide(
            makeEdaTestLayer({
              sessionId,
              parts: [spawn, finished],
              onStreamText: ({ prompt }) => prompts.push(prompt),
              toolRegistryLayer: registry((context) =>
                Effect.gen(function* () {
                  const handle = yield* open(context);
                  assert.deepStrictEqual(yield* open(context), handle);
                  yield* Deferred.succeed(opened, handle);
                  yield* Deferred.await(release);
                  return handle;
                }),
              ),
            }),
          ),
        );
      }),
  );

  for (const control of [stopTurnCommand, interruptCommand]) {
    for (const alreadyResolved of [false, true]) {
      makeMethods(it).effect(
        `${control._tag} fences ${alreadyResolved ? "queued" : "late"} resolution`,
        () =>
          Effect.gen(function* () {
            const state = yield* SessionState;
            const store = yield* EDASessionStore;
            yield* state.start({ modelSelection });
            yield* state.admitCommand(command);
            yield* waitForCommitted(store, hasCommandCompleted(command.commandId));
            const [handle] = (yield* state.snapshot()).resumables.values();
            if (handle === undefined) return yield* Effect.die("missing handle");
            if (alreadyResolved)
              yield* state.resolveResumable({ resumableId: handle.resumableId, result: "RESULT" });
            yield* state.admitCommand(control);
            assert.deepStrictEqual(
              yield* state.resolveResumable({ resumableId: handle.resumableId, result: "LATE" }),
              { _tag: "Cancelled" },
            );
            yield* waitForCommitted(store, (entries) =>
              entries.some(
                (entry) =>
                  (entry.event.type === "CommandCompleted" ||
                    entry.event.type === "CommandCancelled") &&
                  typeof entry.event.payload === "object" &&
                  entry.event.payload !== null &&
                  "commandId" in entry.event.payload &&
                  entry.event.payload.commandId === control.commandId,
              ),
            );
            assert.strictEqual(
              (yield* state.snapshot()).resumables.get(handle.resumableId)?.settlement._tag,
              "Cancelled",
            );
          }).pipe(
            Effect.provide(
              makeEdaTestLayer({
                sessionId,
                parts: [spawn, finished, finished],
                toolRegistryLayer: registry(open),
              }),
            ),
          ),
      );
    }
  }

  makeMethods(it).effect(
    "the continuation waits for an external grant and rejects stale grants after stop",
    () => {
      const delivered: Array<string> = [];
      return Effect.gen(function* () {
        const state = yield* SessionState;
        const store = yield* EDASessionStore;
        yield* state.start({ modelSelection });
        yield* state.admitCommand(command);
        yield* waitForCommitted(store, (entries) =>
          entries.some((entry) => entry.event.type === "RunSchedulingRequested"),
        );
        const first = (yield* state.snapshot()).runSchedulingRequest;
        if (first === undefined) return yield* Effect.die("missing first reservation");
        yield* state.grantRun(new GrantRunCommand({ requestId: first.requestId }), {
          modelSelection,
        });
        yield* waitForCommitted(store, hasCommandCompleted(command.commandId));
        const [handle] = (yield* state.snapshot()).resumables.values();
        if (handle === undefined) return yield* Effect.die("missing handle");
        yield* state.resolveResumable({ resumableId: handle.resumableId, result: "READY" });
        yield* waitForCommitted(
          store,
          (entries) =>
            entries.filter((entry) => entry.event.type === "RunSchedulingRequested").length === 2,
        );
        const second = (yield* state.snapshot()).runSchedulingRequest;
        if (second === undefined) return yield* Effect.die("missing continuation reservation");
        assert.notStrictEqual(second.requestId, first.requestId);
        assert.strictEqual((yield* state.snapshot()).runs.size, 1);
        yield* state.admitCommand(stopTurnCommand);
        assert.deepStrictEqual(
          yield* state.grantRun(new GrantRunCommand({ requestId: second.requestId }), {
            modelSelection,
          }),
          { _tag: "Stale" },
        );
        yield* waitForCommitted(store, hasCommandCompleted(stopTurnCommand.commandId));
        assert.strictEqual((yield* state.snapshot()).runs.size, 1);
      }).pipe(
        Effect.provide(
          makeEdaTestLayer({
            sessionId,
            parts: spawn,
            toolRegistryLayer: registry(open),
            runSchedulerLayer: RunScheduler.Deferred((input) =>
              Effect.sync(() => {
                delivered.push(input.request.requestId);
              }),
            ),
            runSchedulingWakeupLayer: Layer.succeed(RunSchedulingWakeup, {
              setPending: () => Effect.void,
            }),
          }),
        ),
      );
    },
  );

  makeMethods(it).effect(
    "ordinary command ingress cannot manufacture a resumable continuation",
    () =>
      Effect.gen(function* () {
        const state = yield* SessionState;
        const outcome = yield* Effect.result(
          state.admitCommand(
            new ResumeResumableCommand({ resumableId: ResumableId.make(SESSION_ID) }),
          ),
        );
        assert.strictEqual(outcome._tag, "Failure");
        assert.strictEqual((yield* state.snapshot()).lastSeq, 0);
      }).pipe(Effect.provide(makeEdaTestLayer({ sessionId }))),
  );

  makeMethods(it).effect(
    "a rejected open commit leaves neither wait nor external launch intent",
    () =>
      Effect.gen(function* () {
        const state = yield* SessionState;
        const store = yield* EDASessionStore;
        yield* state.start({ modelSelection });
        yield* state.admitCommand(command);
        yield* waitForCommitted(store, (entries) =>
          entries.some((entry) => entry.event.type === "ToolCallStarted"),
        );
        yield* Effect.yieldNow;
        const all = yield* collectCommitted(store);
        assert.isFalse(
          all.some(
            (entry) =>
              entry.event.type === "ResumableOpened" || entry.event.type === "ChildRequested",
          ),
        );
      }).pipe(
        Effect.provide(
          makeEdaTestLayer({
            sessionId,
            parts: spawn,
            toolRegistryLayer: registry(open),
            wrapStore: (inner) => ({
              ...inner,
              append: (input) =>
                input.entries.some((entry) => entry.event.type === "ResumableOpened")
                  ? Effect.fail(
                      new EDASessionStoreError({ message: "injected atomic write failure" }),
                    )
                  : inner.append(input),
            }),
          }),
        ),
      ),
  );
});
