import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import { FailurePayload } from "../types/events";
import type { EDADurableEvent } from "../types/events";
import {
  EDASessionStoreError,
  hasEDASessionStoreError,
  type CommittedDurableEvent,
} from "./session-store";

/** One guarded started-boundary execution: the started event plus the body's value. */
export interface StartedBoundaryGuardResult<A> {
  readonly started: CommittedDurableEvent;
  readonly value: A;
}

/** Parameters for guarding one started lifecycle body with abnormal-exit terminals. */
export interface StartedBoundaryGuardSpec<A, E, R> {
  /** Construct the durable started boundary committed before the body runs. */
  readonly start: Effect.Effect<EDADurableEvent>;
  /** Body owning the happy path, including its own domain terminal commits. */
  readonly body: (started: CommittedDurableEvent) => Effect.Effect<A, E, R>;
  /** Construct the terminal committed when the body fails or dies. */
  readonly onFailure: (failure: FailurePayload) => Effect.Effect<EDADurableEvent>;
  /** Construct the terminal committed when the body is interrupted. */
  readonly onInterrupt: Effect.Effect<EDADurableEvent>;
}

/**
 * Guard a durable `*Started` boundary with an abnormal-exit terminal boundary.
 *
 * The body owns normal terminal commits (completed/stopped variants carry
 * domain data this guard cannot know). The guard only interprets escaped exits:
 * typed failure or defect commits the matching `*Failed`; interruption commits
 * the matching interrupted/stopped/cancelled terminal. Effect owns the bracket
 * mechanics through `acquireUseRelease`; EDA owns the Exit -> event mapping.
 */
/** Minimal durable append capability needed by the started-boundary guard. */
export interface DurableBoundaryAppender {
  readonly appendDurable: (
    event: EDADurableEvent,
  ) => Effect.Effect<CommittedDurableEvent, EDASessionStoreError>;
}

/** Build a guard bound to the session's durable append boundary. */
export const makeStartedBoundaryGuard =
  (state: DurableBoundaryAppender) =>
  <A, E, R>(
    spec: StartedBoundaryGuardSpec<A, E, R>,
  ): Effect.Effect<StartedBoundaryGuardResult<A>, E | EDASessionStoreError, R> => {
    const start = Effect.gen(function* () {
      const startedEvent = yield* spec.start;
      return yield* state.appendDurable(startedEvent);
    });

    const body = (started: CommittedDurableEvent) =>
      Effect.gen(function* () {
        const value = yield* spec.body(started);
        return { started, value };
      });

    const release = (
      _started: CommittedDurableEvent,
      exit: Exit.Exit<StartedBoundaryGuardResult<A>, E>,
    ): Effect.Effect<void, E | EDASessionStoreError> => {
      if (Exit.isSuccess(exit)) {
        return Effect.void;
      }

      if (hasEDASessionStoreError(exit.cause)) {
        return Effect.failCause(exit.cause);
      }

      const terminal = Cause.hasInterruptsOnly(exit.cause)
        ? spec.onInterrupt
        : spec.onFailure(failurePayloadFromCause(exit.cause));

      const commitTerminal = Effect.gen(function* () {
        const terminalEvent = yield* terminal;
        yield* state.appendDurable(terminalEvent);
      });

      return commitTerminal;
    };

    return Effect.acquireUseRelease(start, body, release);
  };

/** Callable guard type produced by `makeStartedBoundaryGuard`. */
export type StartedBoundaryGuard = ReturnType<typeof makeStartedBoundaryGuard>;

/** Flatten an unknown thrown/streamed error into the durable failure payload. */
export const failurePayloadFromUnknown = (error: unknown): FailurePayload => {
  if (error instanceof Error) {
    // TaggedErrorClass instances often have an empty message; the tag/name is
    // then the only durable identification of the failure.
    return FailurePayload.make({
      message: error.message !== "" ? error.message : error.name,
      details: { name: error.name, stack: error.stack },
    });
  }
  if (typeof error === "string") {
    return FailurePayload.make({ message: error });
  }
  return FailurePayload.make({ message: "Unknown error", details: error });
};

/** Flatten a started-boundary exit cause into the durable failure payload. */
export const failurePayloadFromCause = <E>(cause: Cause.Cause<E>): FailurePayload =>
  failurePayloadFromUnknown(Cause.squash(cause));
