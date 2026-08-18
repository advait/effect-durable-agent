import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
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
import type { EDADurableEvent, EDAEphemeralEvent } from "../types/events";
import {
  EventType,
  PositionedEvent,
  ProviderPartId,
  UnixEpochMillis,
  effectDurableAgentNamespace,
  schemaV1,
} from "../types/events";
import { LiveEventBus } from "./live-event-bus";
import { EDASessionQuery } from "./session-query";
import { SessionState, type SessionStateShape } from "./session-state";
import { makeEdaTestLayer, testNowMs } from "../testkit/layers";

const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const SECOND_COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1c-1f2e3d4c5b6a");
const RUN_ID = RunId.make("018f6bd5-2f2a-7b1e-8f1d-1f2e3d4c5b6a");
const TURN_ID = TurnId.make("018f6bd5-2f2a-7b1e-9f1d-1f2e3d4c5b6a");

const command = new StopTurnCommand({ commandId: COMMAND_ID });
const secondCommand = new StopTurnCommand({ commandId: SECOND_COMMAND_ID });

const makeTestLayer = () => makeEdaTestLayer({ sessionId: SESSION_ID });

const eventId = (index: number) =>
  EventId.make(`018f6bd5-2f2a-7b1e-${(0x9000 + index).toString(16)}-1f2e3d4c5b6a`);

const durableCommandEvent = (index: number): EDADurableEvent =>
  ({
    namespace: effectDurableAgentNamespace,
    type: EventType.make("CommandAdmitted"),
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: eventId(index),
    sessionId: SESSION_ID,
    createdAtMs: UnixEpochMillis.make(testNowMs + index),
    payload: { command: index % 2 === 0 ? secondCommand : command },
  }) as EDADurableEvent;

const ephemeralTextDelta = (index: number): EDAEphemeralEvent =>
  ({
    namespace: effectDurableAgentNamespace,
    type: EventType.make("TextDelta"),
    schemaVersion: schemaV1,
    durability: "ephemeral",
    eventId: eventId(index),
    sessionId: SESSION_ID,
    createdAtMs: UnixEpochMillis.make(testNowMs + index),
    payload: {
      providerPartId: ProviderPartId.make(`text-${index}`),
      delta: `live-${index}`,
    },
  }) as EDAEphemeralEvent;

const positionedEphemeral = (event: EDAEphemeralEvent, seq: number, subSeq: number) =>
  PositionedEvent.make({
    position: Position.make({
      seq: SequenceNumber.make(seq),
      subSeq: SubSequenceNumber.make(subSeq),
    }),
    event,
  });

const positionedTurnStarted = (seq: number) =>
  PositionedEvent.make({
    position: durablePosition(SequenceNumber.make(seq)),
    event: {
      namespace: effectDurableAgentNamespace,
      type: EventType.make("TurnStarted"),
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: eventId(100 + seq),
      sessionId: SESSION_ID,
      createdAtMs: UnixEpochMillis.make(testNowMs + seq),
      payload: { runId: RUN_ID, turnId: TURN_ID },
    },
  });

const appendDurable = (session: SessionStateShape, event: EDADurableEvent) =>
  session.appendDurable(event);

const commitDurables = (session: SessionStateShape, count: number) =>
  Effect.forEach(
    Array.from({ length: count }, (_, index) => durableCommandEvent(index + 1)),
    (event) => appendDurable(session, event),
  );

describe("EDASessionQuery", () => {
  it("replays only durable events with seq after the resume point up to the replay head", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const session = yield* SessionState;
        const query = yield* EDASessionQuery;
        yield* commitDurables(session, 4);

        const stream = yield* query.eventsAfter(SequenceNumber.make(1));
        return yield* stream.pipe(Stream.take(3), Stream.runCollect);
      }),
    ).pipe(Effect.provide(makeTestLayer()));

    const events = Array.from(await Effect.runPromise(program));

    expect(events.map((event) => event.position.seq)).toEqual([
      SequenceNumber.make(2),
      SequenceNumber.make(3),
      SequenceNumber.make(4),
    ]);
    expect(events.map((event) => event.event.durability)).toEqual([
      "durable",
      "durable",
      "durable",
    ]);
  });

  it("drops buffered live durable duplicates at or below the replay head", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const session = yield* SessionState;
        const liveBus = yield* LiveEventBus;
        const query = yield* EDASessionQuery;
        const committed = yield* commitDurables(session, 3);

        const stream = yield* query.eventsAfter(SequenceNumber.make(0));
        const collected = yield* stream.pipe(Stream.take(4), Stream.runCollect, Effect.forkScoped);
        yield* liveBus.publish(PositionedEvent.make(committed[1]!));
        const newCommitted = yield* appendDurable(session, durableCommandEvent(4));
        yield* liveBus.publish(PositionedEvent.make(newCommitted));
        return yield* Fiber.join(collected);
      }),
    ).pipe(Effect.provide(makeTestLayer()));

    const events = Array.from(await Effect.runPromise(program));

    expect(events.map((event) => event.position.seq)).toEqual([
      SequenceNumber.make(1),
      SequenceNumber.make(2),
      SequenceNumber.make(3),
      SequenceNumber.make(4),
    ]);
    expect(events.filter((event) => event.event.eventId === eventId(2))).toHaveLength(1);
  });

  it("retains buffered live-only ephemeral events at or below the replay head", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const session = yield* SessionState;
        const liveBus = yield* LiveEventBus;
        const query = yield* EDASessionQuery;
        yield* commitDurables(session, 3);

        const stream = yield* query.eventsAfter(SequenceNumber.make(0));
        yield* liveBus.publish(positionedEphemeral(ephemeralTextDelta(20), 3, 1));
        return yield* stream.pipe(Stream.take(4), Stream.runCollect);
      }),
    ).pipe(Effect.provide(makeTestLayer()));

    const events = Array.from(await Effect.runPromise(program));

    expect(events.map((event) => event.position)).toEqual([
      durablePosition(SequenceNumber.make(1)),
      durablePosition(SequenceNumber.make(2)),
      durablePosition(SequenceNumber.make(3)),
      Position.make({ seq: SequenceNumber.make(3), subSeq: SubSequenceNumber.make(1) }),
    ]);
    expect(events.at(-1)).toMatchObject({
      event: { durability: "ephemeral", payload: { delta: "live-20" } },
    });
  });

  it("follows new live durable events after replay completes", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const session = yield* SessionState;
        const liveBus = yield* LiveEventBus;
        const query = yield* EDASessionQuery;
        yield* commitDurables(session, 2);

        const stream = yield* query.eventsAfter(SequenceNumber.make(0));
        const collected = yield* stream.pipe(Stream.take(3), Stream.runCollect, Effect.forkScoped);
        const newCommitted = yield* appendDurable(session, durableCommandEvent(3));
        yield* liveBus.publish(PositionedEvent.make(newCommitted));
        return yield* Fiber.join(collected);
      }),
    ).pipe(Effect.provide(makeTestLayer()));

    const events = Array.from(await Effect.runPromise(program));

    expect(events.map((event) => event.position.seq)).toEqual([
      SequenceNumber.make(1),
      SequenceNumber.make(2),
      SequenceNumber.make(3),
    ]);
    expect(events.at(-1)).toMatchObject({
      event: { durability: "durable", eventId: eventId(3) },
    });
  });

  it("replays active-turn buffered ephemerals even when the durable resume seq is current", async () => {
    const liveDelta = positionedEphemeral(ephemeralTextDelta(40), 3, 1);
    const program = Effect.scoped(
      Effect.gen(function* () {
        const session = yield* SessionState;
        const liveBus = yield* LiveEventBus;
        const query = yield* EDASessionQuery;
        yield* commitDurables(session, 3);
        yield* liveBus.publish(positionedTurnStarted(3));
        yield* liveBus.publish(liveDelta);

        const stream = yield* query.eventsAfter(SequenceNumber.make(3));
        return yield* stream.pipe(Stream.take(1), Stream.runCollect);
      }),
    ).pipe(Effect.provide(makeTestLayer()));

    const events = Array.from(await Effect.runPromise(program));

    expect(events).toEqual([liveDelta]);
  });
});
