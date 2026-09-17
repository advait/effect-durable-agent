import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";

import { foldReducedState, initialReducedState, type ReducedState } from "../domain/reduced-state";
import { EDAReducerRegistry, type EDAReducerStateSnapshot } from "./reducer-registry";
import { EventId, SessionId, SequenceNumber } from "../types/core";
import type {
  DurableEventEnvelope,
  EphemeralEventEnvelope,
  EventType,
  PositionedEvent,
} from "../types/events";
import type { EDASessionStoreShape } from "./session-store";
import { type CommittedDurableEvent, EDASessionStore, EDASessionStoreError } from "./session-store";
import { EventFactory } from "./event-factory";
import type { EventFactoryShape } from "./event-factory";
import { IdGenerator } from "./id-generator";
import { EDAKeepAlive } from "./keep-alive";
import { makeSinkInbox } from "./sink-inbox";
import { SessionContext } from "./session-context";
import {
  EDASinkName,
  SinkCheckpointStore,
  type SinkCheckpointStoreShape,
  type StoredSinkCheckpoint,
} from "./sink-checkpoint-store";
import { annotateEdaSpan } from "./tracing";

/** Cursor-window input delivered to one durable sink drain. */
export interface EDASinkRawDurableBatch {
  /** All durable events replayed for this cursor window, including uninterested events. */
  readonly allEvents: ReadonlyArray<CommittedDurableEvent>;
  /** Events matching the sink's durable interests. */
  readonly events: ReadonlyArray<CommittedDurableEvent>;
  /** Durable sequence through which the sink may advance after success. */
  readonly throughSeq: SequenceNumber;
}

/** Cursor window with framework and app projections, enabled by default. */
export interface EDASinkDurableBatch extends EDASinkRawDurableBatch {
  /** Authoritative reduced state folded exactly through `throughSeq`. */
  readonly stateAfter: ReducedState;
  /** App-specific durable reducer states folded exactly through `throughSeq`. */
  readonly reducerStates: EDAReducerStateSnapshot;
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

/** `"*"` and omission select every event; an array selects exact event types. */
export type EDASinkInterests = "*" | ReadonlyArray<EventType | string>;

/** Delivery policy shared by raw and projected durable consumers. */
interface EDADurableSinkOptions {
  readonly interests?: EDASinkInterests;
  readonly batchSize?: number;
}

/** Default durable consumer with framework and app projections at each cursor window. */
export interface EDAProjectedDurableSinkDefinition extends EDADurableSinkOptions {
  readonly process: (batch: EDASinkDurableBatch, ctx: EDASinkContext) => Effect.Effect<void, never>;
}

/** Raw consumer that never hydrates, folds, or retains a session projection. */
export interface EDARawDurableSinkDefinition extends EDADurableSinkOptions {
  readonly process: (
    batch: EDASinkRawDurableBatch,
    ctx: EDASinkContext,
  ) => Effect.Effect<void, never>;
}

/** Durable sink definition backed by a checkpoint and an app-owned delivery policy. */
export type EDADurableSinkDefinition = EDAProjectedDurableSinkDefinition;

/** Best-effort live-only sink definition. */
export interface EDAEphemeralSinkDefinition {
  readonly interests?: EDASinkInterests;
  readonly process: (event: PositionedEvent, ctx: EDASinkContext) => Effect.Effect<void, never>;
}

/**
 * Named app integration hook with optional durable and ephemeral filters.
 *
 * Every sink runs on one serialized position-ordered lane. Sinks own retry and
 * terminal failure handling, so callbacks must have an infallible typed error
 * channel. An unexpected defect is logged and skipped before the durable checkpoint
 * advances. Ephemeral callbacks are best-effort and are only processed after the
 * durable prefix at their anchor sequence has been projected.
 */
interface EDASinkOptions {
  readonly name: string;
  readonly ephemeral?: EDAEphemeralSinkDefinition;
}

/** Existing consumers receive framework and app projections unless explicitly opted out. */
export interface EDAProjectedSink extends EDASinkOptions {
  readonly state?: "projected";
  readonly durable?: EDAProjectedDurableSinkDefinition;
}

/** Raw consumers retain only their cursor and sink-owned checkpoint payload. */
export interface EDARawSink extends EDASinkOptions {
  readonly state: "none";
  readonly durable?: EDARawDurableSinkDefinition;
}

/** The outer discriminant preserves contextual callback typing for inline default sinks. */
export type EDASink = EDAProjectedSink | EDARawSink;

/** Projected and raw overloads preserve contextual callback inference and literal names. */
function makeSink<const Sink extends EDAProjectedSink>(sink: Sink): Sink;
function makeSink<const Sink extends EDARawSink>(sink: Sink): Sink;
function makeSink<const Sink extends EDASink>(sink: Sink): Sink;
function makeSink(sink: EDASink): EDASink {
  return sink;
}

/** Convenience constructor preserving a sink's literal name/type information. */
export const EDASink = { make: makeSink };

/** Session-owned capabilities needed to start all registered sink runners. */
export interface EDASinkRunnerStartInput {
  readonly appendDurableBatch: (
    events: ReadonlyArray<DurableEventEnvelope>,
  ) => Effect.Effect<ReadonlyArray<CommittedDurableEvent>, EDASessionStoreError>;
  /** Immutable framework and app projections at one common durable head. */
  readonly initialProjection: {
    readonly reduced: ReducedState;
    readonly reducerStates: EDAReducerStateSnapshot;
  };
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
  readonly publishEphemeralToSinks: (event: PositionedEvent) => Effect.Effect<void>;
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
const sinkCheckpointFormatVersion = 1;

interface SinkProjection {
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
    const checkpointStore = yield* SinkCheckpointStore;
    const eventFactory = yield* EventFactory;
    const ids = yield* IdGenerator;
    const reducerRegistry = yield* EDAReducerRegistry;
    const keepAlive = yield* EDAKeepAlive;
    const started = yield* Ref.make(false);
    const hydrationGate = yield* Semaphore.make(1);
    const deliveries: Array<{
      readonly sink: EDASink;
      readonly inbox: Effect.Success<ReturnType<typeof makeSinkInbox>>;
    }> = [];

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

    const makeSinkRunner = Effect.fnUntraced(function* (
      sink: EDASink,
      { initialProjection, appendDurableBatch, publishEphemeral, scope }: EDASinkRunnerStartInput,
    ) {
      // Long-lived fibers capture the head and capabilities, not the initial projection.
      const initialHead = initialProjection.reduced.lastSeq;
      const sinkName = EDASinkName.make(sink.name);
      const storedCheckpoint = yield* checkpointStore.load(sinkName);
      const checkpointRef = yield* SynchronizedRef.make(storedCheckpoint);
      const checkpoint = makeSinkCheckpoint(checkpointStore, sinkName, checkpointRef);
      const cursor = storedCheckpoint.afterSeq;
      const cursorRef = yield* Ref.make(cursor);
      if (cursor > initialHead) {
        return yield* new EDASessionStoreError({
          message: `Sink ${sink.name} cursor exceeds session head`,
        });
      }
      // Keep only an exact projection seed; lagging workers reconstruct in their own scope.
      const state = yield* Ref.make<SinkProjection | undefined>(
        sink.state === "none"
          ? undefined
          : cursor === initialHead
            ? {
                reduced: initialProjection.reduced,
                reducerStates: initialProjection.reducerStates,
              }
            : cursor === 0
              ? { reduced: initialReducedState, reducerStates: reducerRegistry.initial }
              : undefined,
      );
      const inbox = yield* makeSinkInbox(initialHead).pipe(Scope.provide(scope));
      deliveries.push({ sink, inbox });
      const ensureProjection = Effect.fnUntraced(function* () {
        const existing = yield* Ref.get(state);
        if (existing !== undefined) return existing;
        const hydrated = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(
          Stream.takeWhile((event) => event.position.seq <= cursor),
          Stream.chunks,
          Stream.runFold(
            (): SinkProjection => ({
              reduced: initialReducedState,
              reducerStates: reducerRegistry.initial,
            }),
            (current, page): SinkProjection => ({
              reduced: foldReducedState(current.reduced, page),
              reducerStates: reducerRegistry.reduce(current.reducerStates, page),
            }),
          ),
          Effect.withSpan("agent.sink.hydrate", {
            attributes: {
              "eda.sink.name": sink.name,
              "eda.session.id": session.sessionId,
              "eda.seq.through": cursor,
            },
          }),
          hydrationGate.withPermits(1),
        );
        if (hydrated.reduced.lastSeq !== cursor) {
          return yield* new EDASessionStoreError({
            message: `Sink ${sink.name} could not reconstruct its cursor`,
          });
        }
        yield* Ref.set(state, hydrated);
        return hydrated;
      });

      const processDurableBatch = Effect.fnUntraced(function* (
        allEvents: ReadonlyArray<CommittedDurableEvent>,
      ) {
        if (allEvents.length === 0) {
          return;
        }

        const currentCursor = yield* Ref.get(cursorRef);
        const fresh = allEvents.filter((entry) => entry.position.seq > currentCursor);
        if (fresh.length === 0) {
          return;
        }

        const durable = sink.durable;
        const last = fresh.at(-1);
        if (last === undefined) return;
        const throughSeq = last.position.seq;
        const nextProjection =
          sink.state === "none"
            ? undefined
            : yield* Effect.gen(function* () {
                const current = yield* ensureProjection();
                return {
                  reduced: foldReducedState(current.reduced, fresh),
                  reducerStates: reducerRegistry.reduce(current.reducerStates, fresh),
                };
              });
        const eventsForSink =
          durable === undefined ? [] : filterInterested(fresh, durable.interests);

        if (durable !== undefined && eventsForSink.length > 0) {
          yield* Effect.gen(function* () {
            const staged: Array<DurableEventEnvelope> = [];
            const rawBatch: EDASinkRawDurableBatch = {
              allEvents: fresh,
              events: eventsForSink,
              throughSeq,
            };
            const context = baseContext(staged, publishEphemeral, scope, checkpoint);
            const delivery = Effect.gen(function* () {
              if (sink.durable === undefined) return;
              if (sink.state === "none") return yield* sink.durable.process(rawBatch, context);
              // The projected branch owns both construction and delivery of these fields.
              if (nextProjection === undefined)
                return yield* Effect.die("Projected sink missing its projection");
              return yield* sink.durable.process(
                {
                  ...rawBatch,
                  stateAfter: nextProjection.reduced,
                  reducerStates: nextProjection.reducerStates,
                },
                context,
              );
            });
            const completed = yield* delivery.pipe(
              Effect.as(true),
              Effect.catchCause((cause) =>
                Cause.hasInterruptsOnly(cause)
                  ? Effect.failCause(cause)
                  : Effect.logError("EDA durable sink violated its infallible contract", {
                      cause: Cause.pretty(cause),
                      sink: sink.name,
                    }).pipe(Effect.as(false)),
              ),
            );
            if (completed && staged.length > 0) {
              yield* annotateEdaSpan({ "eda.sink.staged_events": staged.length });
              yield* appendDurableBatch(staged);
            }
          }).pipe(
            Effect.withSpan("agent.sink.drain", {
              attributes: {
                "eda.sink.name": sink.name,
                "eda.sink.cursor.before": currentCursor,
                "eda.sink.events.read": fresh.length,
                "eda.sink.events.interested": eventsForSink.length,
                "eda.sink.cursor.after": throughSeq,
              },
            }),
          );
        }

        yield* commitSinkCheckpoint(checkpointStore, sinkName, checkpointRef, throughSeq);
        yield* Ref.set(cursorRef, throughSeq);
        yield* Ref.set(state, nextProjection);
      });

      const drainDurablesThrough = Effect.fnUntraced(function* (targetHead: SequenceNumber) {
        const currentCursor = yield* Ref.get(cursorRef);
        if (currentCursor >= targetHead) return;
        const allEvents = yield* readCursorBatchThrough(
          store,
          currentCursor,
          targetHead,
          sink.durable?.batchSize,
        );
        if (allEvents.length === 0) {
          return yield* new EDASessionStoreError({
            message: `Sink ${sink.name} could not read its durable backlog`,
          });
        }
        yield* processDurableBatch(allEvents);
      });

      const processEphemeral = Effect.fnUntraced(function* (event: PositionedEvent) {
        if (sink.ephemeral === undefined) return;
        if (sink.state !== "none") yield* ensureProjection();
        yield* sink.ephemeral
          .process(event, baseContext([], publishEphemeral, scope, checkpoint))
          .pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.failCause(cause)
                : Effect.logError("EDA sink ephemeral projection failed", {
                    cause: Cause.pretty(cause),
                    sink: sink.name,
                  }),
            ),
          );
      });

      const runLoop = Effect.forever(
        Effect.gen(function* () {
          const currentCursor = yield* Ref.get(cursorRef);
          const work = yield* inbox.poll(currentCursor);
          if (work === undefined) {
            yield* inbox.awaitWork;
            return;
          }
          yield* keepAlive.withActiveWork(
            `sink:${sink.name}`,
            work._tag === "Durable"
              ? drainDurablesThrough(work.throughSeq)
              : processEphemeral(work.event),
          );
          yield* Effect.yieldNow;
        }),
      ).pipe(
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.failCause(cause)
            : Effect.logError("EDA sink runner failed", {
                cause: Cause.pretty(cause),
                sink: sink.name,
              }),
        ),
      );

      return {
        runLoop,
        initialization:
          sink.state === "none"
            ? "raw"
            : cursor === initialHead
              ? "reused"
              : cursor > 0
                ? "deferred"
                : "empty",
      };
    });

    return {
      sinks,
      startSinkRunners: Effect.fnUntraced(function* (input: EDASinkRunnerStartInput) {
        const wasStarted = yield* Ref.getAndSet(started, true);
        if (wasStarted) {
          return;
        }
        const runners = yield* Effect.gen(function* () {
          const runners = yield* Effect.forEach(sinks, (sink) => makeSinkRunner(sink, input), {
            concurrency: 1,
          });
          yield* Effect.annotateCurrentSpan({
            "eda.sinks.raw": runners.filter((runner) => runner.initialization === "raw").length,
            "eda.sinks.reused": runners.filter((runner) => runner.initialization === "reused")
              .length,
            "eda.sinks.deferred": runners.filter((runner) => runner.initialization === "deferred")
              .length,
            "eda.sinks.empty": runners.filter((runner) => runner.initialization === "empty").length,
          });
          return runners;
        }).pipe(
          Effect.withSpan("agent.sinks.initialize", {
            attributes: {
              sessionId: session.sessionId,
              "eda.session.id": session.sessionId,
              "eda.seq.head": input.initialProjection.reduced.lastSeq,
              "eda.sinks.count": sinks.length,
            },
          }),
        );
        // Catch-up can append events. Register every inbox before starting any delivery fiber.
        yield* Effect.forEach(
          runners,
          (runner) => runner.runLoop.pipe(Effect.forkIn(input.scope)),
          {
            discard: true,
          },
        );
      }),
      notifyDurableHeadAdvanced: (head) =>
        Effect.forEach(deliveries, ({ inbox }) => inbox.notify(head), { discard: true }),
      publishEphemeralToSinks: (event) =>
        Effect.forEach(
          deliveries,
          ({ sink, inbox }) => {
            if (
              sink.ephemeral === undefined ||
              !matchesInterest(event.event.type, sink.ephemeral.interests)
            )
              return Effect.void;
            return Effect.gen(function* () {
              const accepted = yield* inbox.offerEphemeral(event);
              if (!accepted) {
                yield* Effect.logWarning("EDA sink ephemeral buffer full", { sink: sink.name });
              }
            }).pipe(
              Effect.catchTag("SinkEphemeralEncodingError", (error) =>
                Effect.logWarning("EDA sink ephemeral serialization failed", {
                  cause: error.cause,
                  sink: sink.name,
                }),
              ),
            );
          },
          { discard: true },
        ),
    } satisfies EDASinkRegistryShape;
  });

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

const filterInterested = (
  events: ReadonlyArray<CommittedDurableEvent>,
  interests: EDASinkInterests | undefined,
): ReadonlyArray<CommittedDurableEvent> =>
  events.filter((event) => matchesInterest(event.event.type, interests));

const matchesInterest = (
  type: EventType | string,
  interests: EDASinkInterests | undefined,
): boolean =>
  interests === undefined || interests === "*" || interests.some((interest) => interest === type);
