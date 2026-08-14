import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { durableMessageTranscript } from "../domain/message-transcript";
import type { DurableTranscriptMessage } from "../domain/message-transcript";
import type { ReducedState } from "../domain/reduced-state";
import type { EDAReducerStateSnapshot } from "./reducer-registry";
import { SequenceNumber } from "../types/core";
import { PositionedEvent } from "../types/events";
import { EDASessionStore, EDASessionStoreError } from "./session-store";
import { LiveEventBus } from "./live-event-bus";
import { SessionState } from "./session-state";
import { annotateEdaSpan } from "./tracing";

/** Authoritative live snapshot for one EDA session plus its derived durable transcript. */
export interface EDASessionSnapshot {
  readonly state: ReducedState;
  readonly reducerStates: EDAReducerStateSnapshot;
  readonly messages: ReadonlyArray<DurableTranscriptMessage>;
}

/** Read-only query facade over authoritative live state and reconnect-safe event streams. */
export interface EDASessionQueryShape {
  /** Read the authoritative live state and derive its durable transcript. */
  readonly snapshot: () => Effect.Effect<EDASessionSnapshot, EDASessionStoreError>;
  /** Read durable user/assistant messages in committed sequence order from the live snapshot. */
  readonly messages: () => Effect.Effect<
    ReadonlyArray<DurableTranscriptMessage>,
    EDASessionStoreError
  >;
  /** Backfill durable events after `afterSeq`, then follow live durable/ephemeral events. */
  readonly eventsAfter: (
    afterSeq: SequenceNumber,
  ) => Effect.Effect<
    Stream.Stream<PositionedEvent, EDASessionStoreError>,
    EDASessionStoreError,
    Scope.Scope
  >;
}

/** Read-only session query facade backed by authoritative SessionState plus durable backfill. */
export class EDASessionQuery extends Context.Service<EDASessionQuery, EDASessionQueryShape>()(
  "@effect-durable-agent/EDASessionQuery",
) {
  static readonly Live = Layer.effect(
    EDASessionQuery,
    Effect.gen(function* () {
      const store = yield* EDASessionStore;
      const liveBus = yield* LiveEventBus;
      const sessionState = yield* SessionState;

      const snapshot = Effect.gen(function* () {
        const data = yield* sessionState.snapshotData();
        return {
          state: data.reduced,
          reducerStates: data.reducerStates,
          messages: durableMessageTranscript(data.reduced),
        } satisfies EDASessionSnapshot;
      });

      return {
        snapshot: () => snapshot,
        messages: () => snapshot.pipe(Effect.map((snapshot) => snapshot.messages)),
        eventsAfter: (afterSeq: SequenceNumber) =>
          Effect.gen(function* () {
            const live = yield* liveBus.subscribeQueue();
            const replayHead = (yield* sessionState.snapshot()).lastSeq;
            // TODO(backpressure): this runCollect drains the store stream eagerly and buffers the
            // whole durable reconnect prefix, defeating host-level paged replay/backpressure for
            // slow clients. Stream this prefix into the live follow stream instead.
            const replay = yield* store.eventsAfter(afterSeq).pipe(
              Stream.filter((entry) => entry.position.seq <= replayHead),
              Stream.map((entry) =>
                PositionedEvent.make({ position: entry.position, event: entry.event }),
              ),
              Stream.runCollect,
              Effect.map((events) => Array.from(events)),
            );
            const activeTurnReplay = yield* liveBus.activeTurnReplay();
            const pendingLive = (yield* PubSub.takeUpTo(live, Number.POSITIVE_INFINITY)).filter(
              (event) => shouldEmitLiveEvent(event, replayHead),
            );
            const reconnectPrefix: ReadonlyArray<PositionedEvent> = Array.from(
              uniquePositionedEvents([...replay, ...activeTurnReplay.events, ...pendingLive]),
            ).sort(comparePositionedEvents);
            yield* annotateEdaSpan({
              "eda.seq.after": afterSeq,
              "eda.seq.head": replayHead,
              "eda.replay.events": replay.length,
              "eda.live.active_turn_events": activeTurnReplay.events.length,
              "eda.live.active_turn_overflowed": activeTurnReplay.overflowed,
              "eda.live.buffered_events": pendingLive.length,
            });
            const follow: Stream.Stream<PositionedEvent> = Stream.fromEffectRepeat(
              PubSub.take(live),
            ).pipe(Stream.filter((event) => shouldEmitLiveEvent(event, replayHead)));
            return Stream.fromIterable(reconnectPrefix).pipe(Stream.concat(follow));
          }).pipe(
            Effect.withSpan("agent.events.follow", {
              attributes: { "eda.seq.after": afterSeq },
            }),
          ),
      };
    }),
  );
}

const shouldEmitLiveEvent = (event: PositionedEvent, replayHead: SequenceNumber): boolean =>
  event.event.durability === "ephemeral" || event.position.seq > replayHead;

const comparePositionedEvents = (left: PositionedEvent, right: PositionedEvent): number => {
  const seq = left.position.seq - right.position.seq;
  return seq === 0 ? left.position.subSeq - right.position.subSeq : seq;
};

const uniquePositionedEvents = (
  events: ReadonlyArray<PositionedEvent>,
): ReadonlyArray<PositionedEvent> => {
  const seen = new Set<string>();
  const output: PositionedEvent[] = [];
  for (const event of events) {
    const key = `${event.position.seq}:${event.position.subSeq}:${event.event.eventId}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    output.push(event);
  }
  return output;
};
