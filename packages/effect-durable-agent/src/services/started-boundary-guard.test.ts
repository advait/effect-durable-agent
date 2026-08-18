import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  InferenceId,
  RunId,
  SequenceNumber,
  SessionId,
  TurnId,
  durablePosition,
} from "../types/core";
import { EDADurableEvent, FailurePayload } from "../types/events";
import { CommittedDurableEvent, EDASessionStoreError } from "./session-store";
import { EventFactory } from "./event-factory";
import { makeStartedBoundaryGuard, type DurableBoundaryAppender } from "./started-boundary-guard";
import { makeEdaTestLayer } from "../testkit/layers";

const SESSION_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const RUN_ID = "018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a";
const TURN_ID = "018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a";
const INFERENCE_ID = "018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a";

const inferencePayload = {
  runId: RunId.make(RUN_ID),
  turnId: TurnId.make(TURN_ID),
  inferenceId: InferenceId.make(INFERENCE_ID),
};

const interruptedPayload = {
  ...inferencePayload,
  error: FailurePayload.make({ message: "interrupted" }),
};

const TestLayer = makeEdaTestLayer({ sessionId: SessionId.make(SESSION_ID) });

class BodyFailure extends Schema.TaggedErrorClass<BodyFailure>()("BodyFailure", {
  message: Schema.String,
}) {}

const makeRecordingAppender = (options?: { readonly failTerminalCommits?: boolean }) =>
  Effect.gen(function* () {
    const committed = yield* Ref.make<ReadonlyArray<EDADurableEvent>>([]);
    let seq = 0;

    const appendDurable = (event: EDADurableEvent) =>
      Effect.gen(function* () {
        const isFirst = (yield* Ref.get(committed)).length === 0;
        if (options?.failTerminalCommits === true && !isFirst) {
          return yield* new EDASessionStoreError({ message: "terminal commit refused" });
        }

        seq += 1;
        yield* Ref.update(committed, (all) => [...all, event]);
        return CommittedDurableEvent.make({
          position: durablePosition(SequenceNumber.make(seq)),
          event,
        });
      });

    const appender: DurableBoundaryAppender = { appendDurable };

    return { appender, committed };
  });

describe("makeStartedBoundaryGuard", () => {
  it("passes through success and commits no terminal event", async () => {
    const program = Effect.gen(function* () {
      const { appender, committed } = yield* makeRecordingAppender();
      const events = yield* EventFactory;
      const started = yield* events.inferenceStarted(inferencePayload);
      const interrupted = yield* events.inferenceFailed(interruptedPayload);
      const guard = makeStartedBoundaryGuard(appender);

      const result = yield* guard({
        start: Effect.succeed(started),
        body: () => Effect.succeed("ok"),
        onFailure: (error) => events.inferenceFailed({ ...inferencePayload, error }),
        onInterrupt: Effect.succeed(interrupted),
      });
      const recorded = yield* Ref.get(committed);

      return { recorded, result, started };
    }).pipe(Effect.provide(TestLayer));

    const { recorded, result, started } = await Effect.runPromise(program);

    expect(result.value).toBe("ok");
    expect(result.started.event).toEqual(started);
    expect(recorded).toEqual([started]);
  });

  it("commits a failed terminal event for typed failures and preserves the original error", async () => {
    const program = Effect.gen(function* () {
      const { appender, committed } = yield* makeRecordingAppender();
      const events = yield* EventFactory;
      const started = yield* events.inferenceStarted(inferencePayload);
      const expectedFailed = yield* events.inferenceFailed({
        ...inferencePayload,
        error: FailurePayload.make({ message: "expected" }),
      });
      const interrupted = yield* events.inferenceFailed(interruptedPayload);
      const guard = makeStartedBoundaryGuard(appender);

      const exit = yield* Effect.exit(
        guard({
          start: Effect.succeed(started),
          body: () => Effect.fail(new BodyFailure({ message: "body failed" })),
          onFailure: (error) => events.inferenceFailed({ ...inferencePayload, error }),
          onInterrupt: Effect.succeed(interrupted),
        }),
      );
      const recorded = yield* Ref.get(committed);

      return { expectedFailed, exit, recorded, started };
    }).pipe(Effect.provide(TestLayer));

    const { expectedFailed, exit, recorded, started } = await Effect.runPromise(program);
    const error = Exit.isFailure(exit)
      ? Option.getOrUndefined(Cause.findErrorOption(exit.cause))
      : undefined;

    expect(error).toMatchObject({ _tag: "BodyFailure", message: "body failed" });
    expect(recorded).toHaveLength(2);
    expect(recorded[0]).toEqual(started);
    expect(recorded[1]!.type).toBe(expectedFailed.type);
    expect(recorded[1]).toMatchObject({ payload: { error: { message: "body failed" } } });
  });

  it("propagates durable store failures without committing a derived failure terminal", async () => {
    const program = Effect.gen(function* () {
      const { appender, committed } = yield* makeRecordingAppender();
      const events = yield* EventFactory;
      const started = yield* events.inferenceStarted(inferencePayload);
      const interrupted = yield* events.inferenceFailed(interruptedPayload);
      const guard = makeStartedBoundaryGuard(appender);

      const exit = yield* Effect.exit(
        guard({
          start: Effect.succeed(started),
          body: () => new EDASessionStoreError({ message: "event log refused write" }),
          onFailure: (error) => events.inferenceFailed({ ...inferencePayload, error }),
          onInterrupt: Effect.succeed(interrupted),
        }),
      );
      const recorded = yield* Ref.get(committed);

      return { exit, recorded, started };
    }).pipe(Effect.provide(TestLayer));

    const { exit, recorded, started } = await Effect.runPromise(program);
    const error = Exit.isFailure(exit)
      ? Option.getOrUndefined(Cause.findErrorOption(exit.cause))
      : undefined;

    expect(error).toMatchObject({ _tag: "EDASessionStoreError" });
    expect(recorded).toEqual([started]);
  });

  it("commits a failed terminal event for defects and preserves the defect", async () => {
    const program = Effect.gen(function* () {
      const { appender, committed } = yield* makeRecordingAppender();
      const events = yield* EventFactory;
      const started = yield* events.inferenceStarted(inferencePayload);
      const expectedFailed = yield* events.inferenceFailed({
        ...inferencePayload,
        error: FailurePayload.make({ message: "expected" }),
      });
      const interrupted = yield* events.inferenceFailed(interruptedPayload);
      const guard = makeStartedBoundaryGuard(appender);

      const exit = yield* Effect.exit(
        guard({
          start: Effect.succeed(started),
          body: () => Effect.die(new Error("boom")),
          onFailure: (error) => events.inferenceFailed({ ...inferencePayload, error }),
          onInterrupt: Effect.succeed(interrupted),
        }),
      );
      const recorded = yield* Ref.get(committed);

      return { expectedFailed, exit, recorded, started };
    }).pipe(Effect.provide(TestLayer));

    const { expectedFailed, exit, recorded, started } = await Effect.runPromise(program);

    expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
    expect(recorded).toHaveLength(2);
    expect(recorded[0]).toEqual(started);
    expect(recorded[1]!.type).toBe(expectedFailed.type);
    expect(recorded[1]).toMatchObject({ payload: { error: { message: "boom" } } });
  });

  it("commits the interrupt terminal event when the body is interrupted", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const { appender, committed } = yield* makeRecordingAppender();
        const bodyStarted = yield* Deferred.make<void>();
        const gate = yield* Deferred.make<void>();
        const events = yield* EventFactory;
        const started = yield* events.inferenceStarted(inferencePayload);
        const interrupted = yield* events.inferenceFailed(interruptedPayload);
        const guard = makeStartedBoundaryGuard(appender);

        const fiber = yield* guard({
          start: Effect.succeed(started),
          body: () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(bodyStarted, undefined);
              return yield* Deferred.await(gate);
            }),
          onFailure: (error) => events.inferenceFailed({ ...inferencePayload, error }),
          onInterrupt: Effect.succeed(interrupted),
        }).pipe(Effect.forkScoped);

        yield* Deferred.await(bodyStarted);
        yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);
        const recorded = yield* Ref.get(committed);

        return { exit, interrupted, recorded, started };
      }),
    ).pipe(Effect.provide(TestLayer));

    const { exit, interrupted, recorded, started } = await Effect.runPromise(program);

    expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
    expect(recorded).toEqual([started, interrupted]);
  });

  it("propagates terminal commit failures as fatal active-runtime failures", async () => {
    const program = Effect.gen(function* () {
      const { appender, committed } = yield* makeRecordingAppender({ failTerminalCommits: true });
      const events = yield* EventFactory;
      const started = yield* events.inferenceStarted(inferencePayload);
      const interrupted = yield* events.inferenceFailed(interruptedPayload);
      const guard = makeStartedBoundaryGuard(appender);

      const exit = yield* Effect.exit(
        guard({
          start: Effect.succeed(started),
          body: () => Effect.fail(new BodyFailure({ message: "body failed" })),
          onFailure: (error) => events.inferenceFailed({ ...inferencePayload, error }),
          onInterrupt: Effect.succeed(interrupted),
        }),
      );
      const recorded = yield* Ref.get(committed);

      return { exit, recorded, started };
    }).pipe(Effect.provide(TestLayer));

    const { exit, recorded, started } = await Effect.runPromise(program);

    expect(Exit.isFailure(exit)).toBe(true);
    expect(Exit.isFailure(exit) ? Cause.pretty(exit.cause) : "").toContain(
      "terminal commit refused",
    );
    expect(recorded).toEqual([started]);
  });
});
