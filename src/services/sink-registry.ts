import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";

import {
  foldReducedState,
  reduceCommittedEvents,
  type ReducedState,
} from "../domain/reduced-state";
import { EDAReducerRegistry, type EDAReducerStateSnapshot } from "./reducer-registry";
import { EventId, SessionId, SequenceNumber, durablePosition } from "../types/core";
import type {
  DurableEventEnvelope,
  EphemeralEventEnvelope,
  EventType,
  PositionedEvent,
} from "../types/events";
import type { EDASessionStoreShape } from "./session-store";
import { CommittedDurableEvent, EDASessionStore, EDASessionStoreError } from "./session-store";
import { EventFactory } from "./event-factory";
import type { EventFactoryShape } from "./event-factory";
import { IdGenerator } from "./id-generator";
import { EDAKeepAlive } from "./keep-alive";
import { LiveEventBus } from "./live-event-bus";
import { SessionContext } from "./session-context";
import {
  EDASinkName,
  SinkCheckpointStore,
  type SinkCheckpointStoreShape,
  type StoredSinkCheckpoint,
} from "./sink-checkpoint-store";
import { annotateEdaSpan } from "./tracing";

/** Cursor-window input delivered to one durable sink drain. */
export interface EDASinkDurableBatch {
  /** All durable events replayed for this cursor window, including uninterested events. */
  readonly allEvents: ReadonlyArray<CommittedDurableEvent>;
  /** Events matching the sink's durable interests. */
  readonly events: ReadonlyArray<CommittedDurableEvent>;
  /** Authoritative reduced state folded exactly through `throughSeq`. */
  readonly stateAfter: ReducedState;
  /** App-specific durable reducer states folded exactly through `throughSeq`. */
  readonly reducerStates: EDAReducerStateSnapshot;
  /** Durable sequence through which the sink may advance after success. */
  readonly throughSeq: SequenceNumber;
}

/** Typed, sink-owned durable state stored independently from cursor advancement. */
export interface EDASinkCheckpoint {
  /** Decode current durable state, or return `initial` for a new or legacy cursor row. */
  readonly get: <State>(
    schema: Schema.Codec<State, unknown, never, never>,
    initial: State,
  ) => Effect.Effect<State, EDASessionStoreError>;
  /** Validate, encode, and durably save state without advancing the sink cursor. */
  readonly save: <State>(
    schema: Schema.Codec<State, unknown, never, never>,
    state: State,
  ) => Effect.Effect<void, EDASessionStoreError>;
}

/** Capabilities exposed to sink processors without granting raw store or bus access. */
export interface EDASinkContext {
  readonly sessionId: SessionId;
  /** Sink-owned checkpoint state, serialized with cursor commits and background writes. */
  readonly checkpoint: EDASinkCheckpoint;
  /** Build a framework durable event if the sink needs to emit one. */
  readonly events: EventFactoryShape;
  /** Mint an app durable event id for staged custom events. */
  readonly makeEventId: () => Effect.Effect<EventId>;
  /** Publish a best-effort live-only event. */
  readonly emitEphemeral: (
    event: EphemeralEventEnvelope,
  ) => Effect.Effect<PositionedEvent, EDASessionStoreError>;
  /** Stage durable events; the runner commits them after the sink succeeds. */
  readonly stageDurable: (event: DurableEventEnvelope) => Effect.Effect<void>;
  /** Fork background sink work into the session scope. */
  readonly forkScoped: (effect: Effect.Effect<unknown, unknown>) => Effect.Effect<void>;
}

/** At-least-once sink definition backed by a durable checkpoint. */
export interface EDADurableSinkDefinition {
  readonly interests?: ReadonlyArray<EventType | string>;
  readonly batchSize?: number;
  readonly process: (
    batch: EDASinkDurableBatch,
    ctx: EDASinkContext,
  ) => Effect.Effect<void, unknown>;
}

/** Best-effort live-only sink definition. */
export interface EDAEphemeralSinkDefinition {
  readonly interests?: ReadonlyArray<EventType | string>;
  readonly process: (event: PositionedEvent, ctx: EDASinkContext) => Effect.Effect<void, unknown>;
}

/**
 * Named app integration hook with optional durable and ephemeral filters.
 *
 * Every sink runs on one serialized position-ordered lane. Durable callbacks are
 * retried before the sink checkpoint advances; ephemeral callbacks are best-effort
 * and are only processed after the durable prefix at their anchor sequence has
 * been projected.
 */
export interface EDASink {
  readonly name: string;
  readonly durable?: EDADurableSinkDefinition;
  readonly ephemeral?: EDAEphemeralSinkDefinition;
}

/** Convenience constructor preserving a sink's literal name/type information. */
export const EDASink = {
  make: <const Sink extends EDASink>(sink: Sink): Sink => sink,
};

/** Session-owned capabilities needed to start all registered sink runners. */
export interface EDASinkRunnerStartInput {
  readonly appendDurableBatch: (
    events: ReadonlyArray<DurableEventEnvelope>,
  ) => Effect.Effect<ReadonlyArray<CommittedDurableEvent>, EDASessionStoreError>;
  readonly initialHead: SequenceNumber;
  readonly publishEphemeral: (
    event: EphemeralEventEnvelope,
  ) => Effect.Effect<PositionedEvent, EDASessionStoreError>;
  readonly scope: Scope.Scope;
}

/** Registry service coordinating durable checkpoints and live sink delivery. */
export interface EDASinkRegistryShape {
  readonly sinks: ReadonlyArray<EDASink>;
  /** Start long-lived sink runners. Safe to call once during session-state acquisition. */
  readonly startSinkRunners: (
    input: EDASinkRunnerStartInput,
  ) => Effect.Effect<void, EDASessionStoreError>;
  /** Coalesced non-blocking nudge that the durable log head advanced. */
  readonly notifyDurableHeadAdvanced: (head: SequenceNumber) => Effect.Effect<void>;
  /** Best-effort non-blocking delivery to ephemeral sink workers. */
  readonly publishEphemeralToSinks: (
    event: PositionedEvent,
    publishEphemeral: (
      event: EphemeralEventEnvelope,
    ) => Effect.Effect<PositionedEvent, EDASessionStoreError>,
  ) => Effect.Effect<void>;
}

/** Registry and runner for app-provided EDA sinks. */
export class EDASinkRegistry extends Context.Service<EDASinkRegistry, EDASinkRegistryShape>()(
  "@effect-durable-agent/EDASinkRegistry",
) {
  static readonly Empty = Layer.succeed(EDASinkRegistry, {
    sinks: [],
    startSinkRunners: () => Effect.void,
    notifyDurableHeadAdvanced: () => Effect.void,
    publishEphemeralToSinks: () => Effect.void,
  } satisfies EDASinkRegistryShape);

  static readonly Live = (sinks: ReadonlyArray<EDASink>) =>
    Layer.effect(EDASinkRegistry, makeSinkRegistry(sinks));
}

const defaultBatchSize = 100;
const maxDurableSinkRetryDelay = Duration.seconds(5);
const sinkCheckpointFormatVersion = 1;
const durableSinkRetrySchedule = Schedule.exponential("100 millis").pipe(
  Schedule.jittered,
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.min(duration, maxDurableSinkRetryDelay)),
  ),
);

interface DurableRunnerState {
  readonly cursor: SequenceNumber;
  readonly reduced: ReducedState;
  readonly reducerStates: EDAReducerStateSnapshot;
}

interface SerializedSinkState {
  readonly formatVersion: typeof sinkCheckpointFormatVersion;
  readonly state: unknown;
  readonly updatedAtMs: number;
}

const makeSinkRegistry = (sinks: ReadonlyArray<EDASink>) =>
  Effect.gen(function* () {
    const session = yield* SessionContext;
    const store = yield* EDASessionStore;
    const liveBus = yield* LiveEventBus;
    const checkpointStore = yield* SinkCheckpointStore;
    const eventFactory = yield* EventFactory;
    const ids = yield* IdGenerator;
    const reducerRegistry = yield* EDAReducerRegistry;
    const keepAlive = yield* EDAKeepAlive;
    const started = yield* Ref.make(false);

    const baseContext = (
      staged: Array<DurableEventEnvelope>,
      publishEphemeral: (
        event: EphemeralEventEnvelope,
      ) => Effect.Effect<PositionedEvent, EDASessionStoreError>,
      scope: Scope.Scope,
      checkpoint: EDASinkCheckpoint,
    ): EDASinkContext => ({
      sessionId: session.sessionId,
      checkpoint,
      events: eventFactory,
      makeEventId: ids.makeEventId,
      emitEphemeral: publishEphemeral,
      stageDurable: (event) => Effect.sync(() => staged.push(event)),
      forkScoped: (effect) => effect.pipe(Effect.forkIn(scope), Effect.asVoid),
    });

    const makeSinkRunner = (sink: EDASink, input: EDASinkRunnerStartInput) =>
      Effect.gen(function* () {
        const sinkName = EDASinkName.make(sink.name);
        const storedCheckpoint = yield* checkpointStore.load(sinkName);
        const checkpointRef = yield* SynchronizedRef.make(storedCheckpoint);
        const checkpoint = makeSinkCheckpoint(checkpointStore, sinkName, checkpointRef);
        const cursor = storedCheckpoint.afterSeq;
        const prefix = yield* readDurablePrefixThrough(store, cursor);
        const state = yield* Ref.make<DurableRunnerState>({
          cursor,
          reduced: reduceCommittedEvents(prefix),
          reducerStates: reducerRegistry.reduce(reducerRegistry.initial, prefix),
        });
        const live = yield* liveBus.subscribeQueue().pipe(Scope.provide(input.scope));

        const processDurableBatch = Effect.fnUntraced(function* (
          allEvents: ReadonlyArray<CommittedDurableEvent>,
        ) {
          if (allEvents.length === 0) {
            return;
          }

          const current = yield* Ref.get(state);
          const fresh = allEvents.filter((entry) => entry.position.seq > current.cursor);
          if (fresh.length === 0) {
            return;
          }

          const durable = sink.durable;
          const throughSeq = fresh.at(-1)!.position.seq;
          const nextReduced = foldReducedState(current.reduced, fresh);
          const nextReducerStates = reducerRegistry.reduce(current.reducerStates, fresh);
          const eventsForSink =
            durable === undefined ? [] : filterInterested(fresh, durable.interests);

          if (durable !== undefined && eventsForSink.length > 0) {
            yield* Effect.gen(function* () {
              const staged: Array<DurableEventEnvelope> = [];
              yield* durable.process(
                {
                  allEvents: fresh,
                  events: eventsForSink,
                  stateAfter: nextReduced,
                  reducerStates: nextReducerStates,
                  throughSeq,
                },
                baseContext(staged, input.publishEphemeral, input.scope, checkpoint),
              );
              if (staged.length > 0) {
                yield* annotateEdaSpan({ "eda.sink.staged_events": staged.length });
                yield* input.appendDurableBatch(staged);
              }
            }).pipe(
              Effect.withSpan("agent.sink.drain", {
                attributes: {
                  "eda.sink.name": sink.name,
                  "eda.sink.cursor.before": current.cursor,
                  "eda.sink.events.read": fresh.length,
                  "eda.sink.events.interested": eventsForSink.length,
                  "eda.sink.cursor.after": throughSeq,
                },
              }),
            );
          }

          yield* commitSinkCheckpoint(checkpointStore, sinkName, checkpointRef, throughSeq);
          yield* Ref.set(state, {
            cursor: throughSeq,
            reduced: nextReduced,
            reducerStates: nextReducerStates,
          });
        });

        const processDurableBatchWithRetry = (events: ReadonlyArray<CommittedDurableEvent>) =>
          keepAlive.withActiveWork(
            `sink:${sink.name}`,
            processDurableBatch(events).pipe(
              Effect.tapError((error) =>
                Effect.logWarning("EDA sink durable projection failed; retrying", {
                  error: formatSinkError(error),
                  sink: sink.name,
                }),
              ),
              Effect.retry(durableSinkRetrySchedule),
            ),
          );

        const drainDurablesThrough = (targetHead: SequenceNumber) =>
          Effect.gen(function* () {
            while (true) {
              const current = yield* Ref.get(state);
              if (current.cursor >= targetHead) {
                return;
              }
              const allEvents = yield* readCursorBatchThrough(
                store,
                current.cursor,
                targetHead,
                sink.durable?.batchSize,
              );
              if (allEvents.length === 0) {
                return;
              }
              yield* processDurableBatchWithRetry(allEvents);
            }
          });

        const processLiveDurable = (event: PositionedEvent) =>
          Effect.gen(function* () {
            const current = yield* Ref.get(state);
            if (event.position.seq <= current.cursor) {
              return;
            }
            if (event.position.seq > current.cursor + 1) {
              yield* drainDurablesThrough(SequenceNumber.make(event.position.seq - 1));
            }
            const afterCatchup = yield* Ref.get(state);
            if (event.position.seq > afterCatchup.cursor + 1) {
              yield* Effect.logWarning("EDA sink runner could not catch up before live durable", {
                cursor: afterCatchup.cursor,
                eventSeq: event.position.seq,
                sink: sink.name,
              });
              return;
            }
            yield* processDurableBatchWithRetry([committedDurableFromPositioned(event)]);
          });

        const processLiveEphemeral = (event: PositionedEvent) =>
          Effect.gen(function* () {
            const before = yield* Ref.get(state);
            if (event.position.seq < before.cursor) {
              return;
            }
            if (event.position.seq > before.cursor) {
              yield* drainDurablesThrough(event.position.seq);
            }
            const after = yield* Ref.get(state);
            if (event.position.seq !== after.cursor) {
              return;
            }
            if (
              sink.ephemeral === undefined ||
              !matchesInterest(event.event.type, sink.ephemeral.interests)
            ) {
              return;
            }
            yield* sink.ephemeral
              .process(event, baseContext([], input.publishEphemeral, input.scope, checkpoint))
              .pipe(
                Effect.catchCause((cause) =>
                  Effect.logError("EDA sink ephemeral projection failed", {
                    cause: String(cause),
                    sink: sink.name,
                  }),
                ),
              );
          });

        const processLiveEvent = (event: PositionedEvent) =>
          event.event.durability === "durable"
            ? processLiveDurable(event)
            : processLiveEphemeral(event);

        const runLoop = Effect.gen(function* () {
          yield* drainDurablesThrough(input.initialHead);
          yield* Effect.forever(
            Effect.gen(function* () {
              const event = yield* PubSub.take(live);
              yield* processLiveEvent(event);
            }),
          );
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logError("EDA sink runner failed", {
              cause: Cause.pretty(cause),
              sink: sink.name,
            }),
          ),
        );

        yield* runLoop.pipe(Effect.forkIn(input.scope));
      });

    return {
      sinks,
      startSinkRunners: Effect.fnUntraced(function* (input: EDASinkRunnerStartInput) {
        const wasStarted = yield* Ref.getAndSet(started, true);
        if (wasStarted) {
          return;
        }
        yield* Effect.forEach(sinks, (sink) => makeSinkRunner(sink, input), {
          discard: true,
          concurrency: "unbounded",
        });
      }),
      notifyDurableHeadAdvanced: () => Effect.void,
      publishEphemeralToSinks: () => Effect.void,
    } satisfies EDASinkRegistryShape;
  });

const readDurablePrefixThrough = (store: EDASessionStoreShape, throughSeq: SequenceNumber) => {
  if (throughSeq <= SequenceNumber.make(0)) {
    return Effect.succeed([]);
  }
  return store.eventsAfter(SequenceNumber.make(0)).pipe(
    Stream.takeWhile((event) => event.position.seq <= throughSeq),
    Stream.runCollect,
    Effect.map((events) => Array.from(events)),
  );
};

const makeSinkCheckpoint = (
  store: SinkCheckpointStoreShape,
  sinkName: EDASinkName,
  checkpointRef: SynchronizedRef.SynchronizedRef<StoredSinkCheckpoint>,
): EDASinkCheckpoint => ({
  get: (schema, initial) =>
    SynchronizedRef.get(checkpointRef).pipe(
      Effect.flatMap((checkpoint) => {
        const serialized = serializedSinkState(checkpoint.payload);
        return serialized === undefined
          ? Effect.succeed(initial)
          : decodeSinkState(schema, serialized.state);
      }),
    ),
  save: (schema, state) =>
    encodeSinkState(schema, state).pipe(
      Effect.flatMap((encoded) => {
        const payload: SerializedSinkState = {
          formatVersion: sinkCheckpointFormatVersion,
          state: encoded,
          updatedAtMs: Date.now(),
        };
        return SynchronizedRef.modifyEffect(checkpointRef, (checkpoint) =>
          store.saveState(sinkName, payload).pipe(
            Effect.as([
              undefined,
              {
                afterSeq: checkpoint.afterSeq,
                payload,
              },
            ] as const),
          ),
        );
      }),
    ),
});

const commitSinkCheckpoint = (
  store: SinkCheckpointStoreShape,
  sinkName: EDASinkName,
  checkpointRef: SynchronizedRef.SynchronizedRef<StoredSinkCheckpoint>,
  afterSeq: SequenceNumber,
): Effect.Effect<void, EDASessionStoreError> =>
  SynchronizedRef.modifyEffect(checkpointRef, (checkpoint) =>
    store.commit(sinkName, afterSeq, checkpoint.payload).pipe(
      Effect.as([
        undefined,
        {
          afterSeq,
          payload: checkpoint.payload,
        },
      ] as const),
    ),
  );

const serializedSinkState = (payload: unknown): SerializedSinkState | undefined => {
  if (
    typeof payload !== "object" ||
    payload === null ||
    !("formatVersion" in payload) ||
    payload.formatVersion !== sinkCheckpointFormatVersion ||
    !("state" in payload) ||
    !("updatedAtMs" in payload) ||
    typeof payload.updatedAtMs !== "number"
  ) {
    return undefined;
  }
  return {
    formatVersion: sinkCheckpointFormatVersion,
    state: payload.state,
    updatedAtMs: payload.updatedAtMs,
  };
};

const decodeSinkState = <State>(
  schema: Schema.Codec<State, unknown, never, never>,
  encoded: unknown,
): Effect.Effect<State, EDASessionStoreError> =>
  Effect.try({
    try: () => Schema.decodeUnknownSync(schema)(encoded),
    catch: (cause) => sinkCheckpointCodecError("decoding", cause),
  });

const encodeSinkState = <State>(
  schema: Schema.Codec<State, unknown, never, never>,
  state: State,
): Effect.Effect<unknown, EDASessionStoreError> =>
  Effect.try({
    try: () => Schema.encodeUnknownSync(schema)(state),
    catch: (cause) => sinkCheckpointCodecError("encoding", cause),
  });

const sinkCheckpointCodecError = (operation: string, cause: unknown) =>
  new EDASessionStoreError({
    message:
      cause instanceof Error
        ? `${operation} sink checkpoint: ${cause.message}`
        : `${operation} sink checkpoint: ${String(cause)}`,
  });

const readCursorBatchThrough = (
  store: EDASessionStoreShape,
  afterSeq: SequenceNumber,
  throughSeq: SequenceNumber,
  batchSize = defaultBatchSize,
) =>
  store.eventsAfter(afterSeq).pipe(
    Stream.takeWhile((event) => event.position.seq <= throughSeq),
    Stream.take(batchSize),
    Stream.runCollect,
    Effect.map((events) => Array.from(events)),
  );

const committedDurableFromPositioned = (event: PositionedEvent): CommittedDurableEvent =>
  CommittedDurableEvent.make({
    event: event.event as DurableEventEnvelope,
    position: durablePosition(event.position.seq),
  });

const filterInterested = (
  events: ReadonlyArray<CommittedDurableEvent>,
  interests: ReadonlyArray<EventType | string> | undefined,
): ReadonlyArray<CommittedDurableEvent> =>
  events.filter((event) => matchesInterest(event.event.type, interests));

const matchesInterest = (
  type: EventType | string,
  interests: ReadonlyArray<EventType | string> | undefined,
): boolean => interests === undefined || interests.some((interest) => interest === type);

const formatSinkError = (error: unknown): string => {
  if (Cause.isUnknownError(error)) {
    return `${error.message ?? error._tag}: ${formatSinkError(error.cause)}`;
  }
  return error instanceof Error ? error.message : String(error);
};
