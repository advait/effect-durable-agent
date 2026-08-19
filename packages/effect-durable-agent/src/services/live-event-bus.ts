import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { TurnId } from "../types/core";
import {
  PositionedEvent,
  turnCompletedEventType,
  turnFailedEventType,
  turnStartedEventType,
  turnStoppedEventType,
} from "../types/events";

/** Conservative raw-delta cap for one open turn's live-only reconnect replay. */
export const activeTurnEphemeralReplayCapacity = 4_096;

/** Live-only replay buffer for the currently open turn. */
export interface ActiveTurnEphemeralReplay {
  readonly turnId?: TurnId;
  readonly events: ReadonlyArray<PositionedEvent>;
  readonly overflowed: boolean;
}

/**
 * Push-delivery listener invoked inline as each event is published.
 *
 * Listeners let hibernation-capable hosts fan events out to accepted
 * WebSockets during the already-active turn instead of parking a resident
 * subscriber fiber on the pub/sub. Listeners must not fail; defects are
 * logged and never interrupt the publishing turn.
 */
export type LiveDeliveryListener = (event: PositionedEvent) => Effect.Effect<void>;

/** In-memory live fanout plus active-turn replay source for reconnect streams. */
export interface LiveEventBusShape {
  readonly publish: (event: PositionedEvent) => Effect.Effect<boolean>;
  readonly subscribe: () => Effect.Effect<Stream.Stream<PositionedEvent>, never, Scope.Scope>;
  readonly subscribeQueue: () => Effect.Effect<
    PubSub.Subscription<PositionedEvent>,
    never,
    Scope.Scope
  >;
  /** Snapshot raw live-only events retained for the currently open turn. */
  readonly activeTurnReplay: () => Effect.Effect<ActiveTurnEphemeralReplay>;
  /** Register one push-delivery listener invoked inline on every published event. */
  readonly registerDeliveryListener: (listener: LiveDeliveryListener) => Effect.Effect<void>;
}

/** Unbounded in-memory fanout plus bounded active-turn ephemeral replay. */
export class LiveEventBus extends Context.Service<LiveEventBus, LiveEventBusShape>()(
  "@effect-durable-agent/LiveEventBus",
) {
  static readonly Live = Layer.effect(
    LiveEventBus,
    Effect.gen(function* () {
      const pubsub = yield* Effect.acquireRelease(
        PubSub.unbounded<PositionedEvent>(),
        PubSub.shutdown,
      );
      const activeTurnReplay = yield* Ref.make<ActiveTurnEphemeralReplay>({
        events: [],
        overflowed: false,
      });
      const listeners = yield* Ref.make<ReadonlyArray<LiveDeliveryListener>>([]);
      return makeLiveBus(pubsub, activeTurnReplay, listeners);
    }),
  );
}

const makeLiveBus = (
  pubsub: PubSub.PubSub<PositionedEvent>,
  activeTurnReplay: Ref.Ref<ActiveTurnEphemeralReplay>,
  listeners: Ref.Ref<ReadonlyArray<LiveDeliveryListener>>,
): LiveEventBusShape => ({
  publish: (event) =>
    updateActiveTurnReplay(activeTurnReplay, event).pipe(
      Effect.andThen(notifyDeliveryListeners(listeners, event)),
      Effect.andThen(PubSub.publish(pubsub, event)),
    ),
  subscribeQueue: () => PubSub.subscribe(pubsub),
  subscribe: () =>
    Effect.map(PubSub.subscribe(pubsub), (subscription) =>
      Stream.fromEffectRepeat(PubSub.take(subscription)),
    ),
  activeTurnReplay: () => Ref.get(activeTurnReplay),
  registerDeliveryListener: (listener) =>
    Ref.update(listeners, (current) => [...current, listener]),
});

const notifyDeliveryListeners = (
  listeners: Ref.Ref<ReadonlyArray<LiveDeliveryListener>>,
  event: PositionedEvent,
): Effect.Effect<void> =>
  Ref.get(listeners).pipe(
    Effect.flatMap((current) =>
      Effect.forEach(
        current,
        (listener) =>
          listener(event).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("EDA live delivery listener defected", cause),
            ),
          ),
        { discard: true },
      ),
    ),
  );

const updateActiveTurnReplay = (
  ref: Ref.Ref<ActiveTurnEphemeralReplay>,
  event: PositionedEvent,
): Effect.Effect<void> => Ref.update(ref, (state) => nextActiveTurnReplay(state, event));

const nextActiveTurnReplay = (
  state: ActiveTurnEphemeralReplay,
  event: PositionedEvent,
): ActiveTurnEphemeralReplay => {
  if (event.event.type === turnStartedEventType) {
    return {
      turnId: (event.event.payload as { readonly turnId: TurnId }).turnId,
      events: [],
      overflowed: false,
    };
  }

  if (
    event.event.type === turnCompletedEventType ||
    event.event.type === turnFailedEventType ||
    event.event.type === turnStoppedEventType
  ) {
    const turnId = (event.event.payload as { readonly turnId?: TurnId }).turnId;
    return turnId === undefined || state.turnId === undefined || turnId === state.turnId
      ? { events: [], overflowed: false }
      : state;
  }

  if (event.event.durability !== "ephemeral" || state.turnId === undefined) {
    return state;
  }

  const events = [...state.events, event];
  if (events.length <= activeTurnEphemeralReplayCapacity) {
    return { ...state, events };
  }
  return {
    ...state,
    events: events.slice(events.length - activeTurnEphemeralReplayCapacity),
    overflowed: true,
  };
};
