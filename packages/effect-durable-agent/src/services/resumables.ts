import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  OpenResumable,
  ResumableHandle,
  ResumableOpenError,
  maxOpenResumables,
  type ResolveResumable,
  type ResumableCancelled,
  type ResumableResolution,
} from "../domain/resumables";
import type { ReducedState } from "../domain/reduced-state";
import { ResumeResumableCommand } from "../types/commands";
import { ResumableId, type SessionId } from "../types/core";
import { effectDurableAgentNamespace, type DurableEventEnvelope } from "../types/events";
import type { EventFactoryShape } from "./event-factory";
import type { IdGeneratorShape } from "./id-generator";
import type { SessionEventSink } from "./session-event-sink";
import type { CommittedDurableEvent, EDASessionStoreError } from "./session-store";

/** Canonical session append authority; no operation may bypass the supplied serialization gate. */
interface ResumableSession {
  readonly sessionId: SessionId;
  readonly events: EventFactoryShape;
  readonly ids: IdGeneratorShape;
  readonly snapshot: () => Effect.Effect<ReducedState>;
  readonly withinGate: <A, E>(
    effect: Effect.Effect<A, E>,
  ) => Effect.Effect<A, E | EDASessionStoreError>;
  readonly commit: (
    events: ReadonlyArray<DurableEventEnvelope>,
  ) => Effect.Effect<ReadonlyArray<CommittedDurableEvent>, EDASessionStoreError>;
  readonly wake: () => Effect.Effect<unknown>;
}

/** Trusted continuation settlement, separate from ordinary user command submission. */
export interface ResumableOperations {
  readonly openToolResumable: SessionEventSink["openToolResumable"];
  readonly resolveResumable: (
    input: ResolveResumable,
    appEvents?: ReadonlyArray<DurableEventEnvelope>,
  ) => Effect.Effect<ResumableResolution, EDASessionStoreError>;
  readonly cancelResumable: (
    input: ResumableCancelled,
  ) => Effect.Effect<ResumableResolution, EDASessionStoreError>;
  /** Build cancellation facts inside an already held append gate, for stop/interrupt admission. */
  readonly cancelPendingResumables: (
    state: ReducedState,
    reason: string,
  ) => Effect.Effect<ReadonlyArray<DurableEventEnvelope>>;
}

/** Owns atomic open/settle/cancel handoffs while SessionState remains the sole durable writer. */
export const makeResumableOperations = (session: ResumableSession): ResumableOperations => {
  const { events, ids } = session;
  const checkAppEvents = (appEvents: ReadonlyArray<DurableEventEnvelope>) =>
    Effect.gen(function* () {
      if (
        appEvents.some(
          (event) =>
            event.sessionId !== session.sessionId ||
            event.namespace === effectDurableAgentNamespace,
        )
      )
        return yield* Effect.die(
          new Error("Resumable handoffs accept only same-session extension events"),
        );
    });

  const cancellationEvents = Effect.fn(function* (state: ReducedState, input: ResumableCancelled) {
    const record = state.resumables.get(input.resumableId);
    if (record === undefined || record.settlement._tag === "Cancelled") return [];
    const batch = [yield* events.resumableCancelled(input)];
    const command =
      record.settlement._tag === "Resolved"
        ? state.commands.get(record.settlement.commandId)
        : undefined;
    const cancelled: Array<DurableEventEnvelope> = batch;
    if (command !== undefined && command.startedSeq === undefined && command.terminal === undefined)
      cancelled.push(
        yield* events.commandCancelled({ commandId: command.commandId, reason: input.reason }),
      );
    return cancelled;
  });

  return {
    openToolResumable: Effect.fn("agent.resumable.open")((toolCallId, input, requestEvents) =>
      session.withinGate(
        Effect.gen(function* () {
          const current = yield* session.snapshot();
          const resumableId = Schema.decodeUnknownSync(ResumableId)(toolCallId);
          const handle = ResumableHandle.make({ status: "waiting", resumableId });
          if (current.resumables.has(resumableId)) return handle;
          const tool = current.toolCalls.get(toolCallId);
          const decision = tool?.decision;
          if (
            decision?._tag !== "Created" ||
            tool?.terminal !== undefined ||
            current.runs.get(decision.runId) === undefined ||
            current.runs.get(decision.runId)?.terminal !== undefined
          )
            return yield* new ResumableOpenError({
              message: "A resumable requires an active tool call",
            });
          if (
            current.commandQueues.activeControlCommands.some(
              (pending) =>
                pending.command._tag === "StopTurn" ||
                (pending.command._tag === "SubmitMessage" &&
                  pending.command.disposition === "interrupt"),
            )
          )
            return yield* new ResumableOpenError({
              message: "The tool's run is being interrupted",
            });
          if (
            Array.from(current.resumables.values()).filter(
              (record) => record.settlement._tag === "Open",
            ).length >= maxOpenResumables
          )
            return yield* new ResumableOpenError({ message: "Too many outstanding resumables" });
          const parsed = yield* Schema.decodeUnknownEffect(OpenResumable)(input).pipe(
            Effect.mapError(
              () => new ResumableOpenError({ message: "Invalid resumable kind or title" }),
            ),
          );
          const appEvents = yield* requestEvents(handle);
          yield* checkAppEvents(appEvents);
          yield* session.commit([
            yield* events.resumableOpened({
              ...parsed,
              resumableId,
              runId: decision.runId,
              toolCallId,
            }),
            ...appEvents,
          ]);
          return handle;
        }),
      ),
    ),
    resolveResumable: Effect.fn("agent.resumable.resolve")(function* (input, appEvents = []) {
      const outcome = yield* session.withinGate(
        Effect.gen(function* (): Effect.fn.Return<ResumableResolution, EDASessionStoreError> {
          const current = yield* session.snapshot();
          const record = current.resumables.get(input.resumableId);
          if (record === undefined) return { _tag: "Missing" };
          if (record.settlement._tag === "Cancelled") return { _tag: "Cancelled" };
          if (record.settlement._tag === "Resolved")
            return { _tag: "Resolved", commandId: record.settlement.commandId };
          yield* checkAppEvents(appEvents);
          const commandId = yield* ids.makeCommandId();
          yield* session.commit([
            ...appEvents,
            yield* events.resumableResolved({ ...input, commandId }),
            yield* events.commandAdmitted({
              command: new ResumeResumableCommand({ commandId, resumableId: input.resumableId }),
            }),
          ]);
          return { _tag: "Resolved", commandId };
        }),
      );
      yield* session.wake();
      return outcome;
    }),
    cancelResumable: Effect.fn("agent.resumable.cancel")(function* (input) {
      const outcome = yield* session.withinGate(
        Effect.gen(function* (): Effect.fn.Return<ResumableResolution, EDASessionStoreError> {
          const current = yield* session.snapshot();
          if (!current.resumables.has(input.resumableId)) return { _tag: "Missing" };
          yield* session.commit(yield* cancellationEvents(current, input));
          return { _tag: "Cancelled" };
        }),
      );
      yield* session.wake();
      return outcome;
    }),
    cancelPendingResumables: Effect.fn(function* (state, reason) {
      const pending = Array.from(state.resumables.values()).filter(
        (record) =>
          record.settlement._tag === "Open" ||
          (record.settlement._tag === "Resolved" &&
            state.commands.get(record.settlement.commandId)?.terminal === undefined),
      );
      const batches = yield* Effect.forEach(pending, (record) =>
        cancellationEvents(state, { resumableId: record.resumableId, reason }),
      );
      return batches.flat();
    }),
  };
};
