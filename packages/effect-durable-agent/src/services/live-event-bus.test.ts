import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vite-plus/test";

import { StopTurnCommand } from "../types/commands";
import {
  CommandId,
  EventId,
  Position,
  RunId,
  SequenceNumber,
  SessionId,
  SubSequenceNumber,
  TurnId,
  durablePosition,
} from "../types/core";
import {
  CommandAdmittedEvent,
  EventType,
  commandAdmittedEventType,
  effectDurableAgentNamespace,
  PositionedEvent,
  ProviderPartId,
  schemaV1,
  turnCompletedEventType,
  turnStartedEventType,
  UnixEpochMillis,
} from "../types/events";
import { LiveEventBus } from "./live-event-bus";
import { SessionEventObserver } from "./session-event-observer";

const SESSION_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const EVENT_IDS = [
  "018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a",
  "018f6bd5-2f2a-7b1e-9f1b-1f2e3d4c5b6a",
  "018f6bd5-2f2a-7b1e-9f1c-1f2e3d4c5b6a",
] as const;
const COMMAND_ID = "018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a";
const RUN_ID = RunId.make("018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a");
const TURN_ID = TurnId.make("018f6bd5-2f2a-7b1e-af1b-1f2e3d4c5b6a");

const eventIdAt = (index: number) =>
  EventId.make(`018f6bd5-2f2a-7b1e-${(0x9000 + index).toString(16)}-1f2e3d4c5b6a`);

const positionedEventAt = (seq: number, eventId = EVENT_IDS[seq - 1] ?? EVENT_IDS[0]) =>
  PositionedEvent.make({
    position: durablePosition(SequenceNumber.make(seq)),
    event: CommandAdmittedEvent.make({
      namespace: effectDurableAgentNamespace,
      type: commandAdmittedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(eventId),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000 + seq),
      payload: {
        command: new StopTurnCommand({ commandId: CommandId.make(COMMAND_ID) }),
      },
    }),
  });

const positionedTurnStarted = (seq: number) =>
  PositionedEvent.make({
    position: durablePosition(SequenceNumber.make(seq)),
    event: {
      namespace: effectDurableAgentNamespace,
      type: turnStartedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: eventIdAt(seq),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000 + seq),
      payload: { runId: RUN_ID, turnId: TURN_ID },
    },
  });

const positionedTurnCompleted = (seq: number) =>
  PositionedEvent.make({
    position: durablePosition(SequenceNumber.make(seq)),
    event: {
      namespace: effectDurableAgentNamespace,
      type: turnCompletedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: eventIdAt(seq),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000 + seq),
      payload: { runId: RUN_ID, turnId: TURN_ID },
    },
  });

const positionedTextDelta = (seq: number, subSeq: number, index: number) =>
  PositionedEvent.make({
    position: Position.make({
      seq: SequenceNumber.make(seq),
      subSeq: SubSequenceNumber.make(subSeq),
    }),
    event: {
      namespace: effectDurableAgentNamespace,
      type: EventType.make("TextDelta"),
      schemaVersion: schemaV1,
      durability: "ephemeral",
      eventId: eventIdAt(index),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000 + index),
      payload: { providerPartId: ProviderPartId.make(`text-${index}`), delta: `delta-${index}` },
    },
  });

const positionedEvent = positionedEventAt(1);

describe("LiveEventBus", () => {
  it("broadcasts positioned events to active subscribers", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* LiveEventBus;
        const firstStream = yield* bus.subscribe();
        const secondStream = yield* bus.subscribe();
        const first = yield* firstStream.pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped);
        const second = yield* secondStream.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );

        const accepted = yield* bus.publish(positionedEvent);
        const firstEvents = yield* Fiber.join(first);
        const secondEvents = yield* Fiber.join(second);

        return { accepted, firstEvents, secondEvents };
      }),
    ).pipe(Effect.provide(LiveEventBus.Noop));

    const result = await Effect.runPromise(program);

    expect(result.accepted).toBe(true);
    expect(result.firstEvents).toEqual([positionedEvent]);
    expect(result.secondEvents).toEqual([positionedEvent]);
    expect(Schema.is(PositionedEvent)(result.firstEvents[0])).toBe(true);
  });

  it("preserves publish order within each subscriber", async () => {
    const events = [positionedEventAt(1), positionedEventAt(2), positionedEventAt(3)];
    const program = Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* LiveEventBus;
        const stream = yield* bus.subscribe();
        const collectedFiber = yield* stream.pipe(
          Stream.take(events.length),
          Stream.runCollect,
          Effect.forkScoped,
        );

        for (const event of events) {
          yield* bus.publish(event);
        }

        return yield* Fiber.join(collectedFiber);
      }),
    ).pipe(Effect.provide(LiveEventBus.Noop));

    const received = await Effect.runPromise(program);

    expect(received).toEqual(events);
    expect(received.map((event) => event.position.seq)).toEqual([1, 2, 3]);
  });

  it("notifies the injected host observer in publish order", async () => {
    const observed: PositionedEvent[] = [];
    const events = [positionedEventAt(1), positionedEventAt(2), positionedEventAt(3)];
    const busLayer = LiveEventBus.Live.pipe(
      Layer.provide(
        SessionEventObserver.FromHandler((event) =>
          Effect.sync(() => {
            observed.push(event);
          }),
        ),
      ),
    );

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const bus = yield* LiveEventBus;
          for (const event of events) yield* bus.publish(event);
        }),
      ).pipe(Effect.provide(busLayer)),
    );

    expect(observed).toEqual(events);
  });

  it("keeps subscriber backlogs isolated from each other", async () => {
    const events = [positionedEventAt(1), positionedEventAt(2)];
    const program = Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* LiveEventBus;
        const slowQueue = yield* bus.subscribeQueue();
        const fastStream = yield* bus.subscribe();
        const fastFiber = yield* fastStream.pipe(
          Stream.take(events.length),
          Stream.runCollect,
          Effect.forkScoped,
        );

        for (const event of events) {
          yield* bus.publish(event);
        }

        const fastEvents = yield* Fiber.join(fastFiber);
        const slowFirst = yield* PubSub.take(slowQueue);
        const slowSecond = yield* PubSub.take(slowQueue);
        return { fastEvents, slowEvents: [slowFirst, slowSecond] };
      }),
    ).pipe(Effect.provide(LiveEventBus.Noop));

    const result = await Effect.runPromise(program);

    expect(result.fastEvents).toEqual(events);
    expect(result.slowEvents).toEqual(events);
  });

  it("does not replay events published before subscription", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* LiveEventBus;
        yield* bus.publish(positionedEventAt(1));
        const subscription = yield* bus.subscribeQueue();
        const beforeLivePublish = yield* PubSub.takeUpTo(subscription, 1);
        const liveEvent = positionedEventAt(2);
        yield* bus.publish(liveEvent);
        const afterLivePublish = yield* PubSub.take(subscription);
        return { beforeLivePublish, afterLivePublish, liveEvent };
      }),
    ).pipe(Effect.provide(LiveEventBus.Noop));

    const result = await Effect.runPromise(program);

    expect(result.beforeLivePublish).toEqual([]);
    expect(result.afterLivePublish).toEqual(result.liveEvent);
  });

  it("continues publishing after an interrupted subscriber take", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* LiveEventBus;
        const subscription = yield* bus.subscribeQueue();
        const pendingTake = yield* PubSub.take(subscription).pipe(Effect.forkScoped);

        yield* Effect.yieldNow;
        yield* Fiber.interrupt(pendingTake);

        return yield* bus.publish(positionedEvent);
      }),
    ).pipe(Effect.provide(LiveEventBus.Noop));

    await expect(Effect.runPromise(program)).resolves.toBe(true);
  });

  it("buffers active-turn ephemerals until the turn terminal checkpoint", async () => {
    const firstDelta = positionedTextDelta(1, 1, 20);
    const secondDelta = positionedTextDelta(1, 2, 21);
    const program = Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* LiveEventBus;
        yield* bus.publish(positionedTurnStarted(1));
        yield* bus.publish(firstDelta);
        yield* bus.publish(secondDelta);
        const active = yield* bus.activeTurnReplay();
        yield* bus.publish(positionedTurnCompleted(2));
        const completed = yield* bus.activeTurnReplay();
        return { active, completed };
      }),
    ).pipe(Effect.provide(LiveEventBus.Noop));

    const result = await Effect.runPromise(program);

    expect(result.active.turnId).toBe(TURN_ID);
    expect(result.active.events).toEqual([firstDelta, secondDelta]);
    expect(result.active.overflowed).toBe(false);
    expect(result.completed).toEqual({ events: [], overflowed: false });
  });
});
