import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { EDARuntime } from "./runtime";
import {
  runWebSocketSubscriber,
  type EDAWebSocketSubscriberTransport,
} from "./websocket-subscriber";
import { CommandIdempotencyKey, StopTurnCommand, SubmitMessageCommand } from "../types/commands";
import { CommandId, EventId, SequenceNumber, SessionId, durablePosition } from "../types/core";
import {
  CommandAdmittedEvent,
  PositionedEvent,
  UnixEpochMillis,
  commandAdmittedEventType,
  effectDurableAgentNamespace,
  schemaV1,
} from "../types/events";
import {
  EDAWebSocketAckFrame,
  EDAWebSocketServerFrame,
  type EDAWebSocketClientFrame,
  SubscriberId,
  defaultEDAWebSocketFlowControl,
} from "../host/websocket-protocol";

const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const SUBSCRIBER_ID = SubscriberId.make("subscriber-test");

const eventId = (index: number) =>
  EventId.make(`018f6bd5-2f2a-7b1e-${(0x9000 + index).toString(16)}-1f2e3d4c5b6a`);

const positionedEventAt = (seq: number) =>
  PositionedEvent.make({
    position: durablePosition(SequenceNumber.make(seq)),
    event: CommandAdmittedEvent.make({
      namespace: effectDurableAgentNamespace,
      type: commandAdmittedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: eventId(seq),
      sessionId: SESSION_ID,
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000 + seq),
      payload: {
        command: new StopTurnCommand({ commandId: COMMAND_ID }),
      },
    }),
  });

const positionedSubmitMessageEventAt = (input: { readonly seq: number; readonly text: string }) =>
  PositionedEvent.make({
    position: durablePosition(SequenceNumber.make(input.seq)),
    event: CommandAdmittedEvent.make({
      namespace: effectDurableAgentNamespace,
      type: commandAdmittedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: eventId(input.seq),
      sessionId: SESSION_ID,
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000 + input.seq),
      payload: {
        command: new SubmitMessageCommand({
          commandId: COMMAND_ID,
          disposition: "queue",
          idempotencyKey: CommandIdempotencyKey.make(`large-${input.seq}`),
          content: [Prompt.textPart({ text: input.text })],
        }),
      },
    }),
  });

const makeRuntimeLayer = (events: ReadonlyArray<PositionedEvent>) =>
  Layer.succeed(EDARuntime, {
    submit: (() => Effect.die(new Error("unused submit"))) as never,
    blockOnCommand: () => Effect.die(new Error("unused blockOnCommand")),
    submitAndBlock: () => Effect.die(new Error("unused submitAndBlock")),
    snapshot: () => Effect.die(new Error("unused snapshot")),
    messages: () => Effect.die(new Error("unused messages")),
    eventsAfter: () =>
      Effect.succeed(Stream.fromIterable(events).pipe(Stream.concat(Stream.never))),
  });

const makeTransport = () =>
  Effect.gen(function* () {
    const incoming = yield* Queue.unbounded<EDAWebSocketClientFrame>();
    const sent = yield* Queue.unbounded<EDAWebSocketServerFrame>();
    const closed = yield* Deferred.make<void>();
    const ackedOnce = yield* Deferred.make<SequenceNumber>();
    const acked: SequenceNumber[] = [];
    const transport: EDAWebSocketSubscriberTransport = {
      incoming,
      closed,
      send: (frame) => Queue.offer(sent, frame).pipe(Effect.asVoid),
      persistAck: (seq) =>
        Effect.sync(() => void acked.push(seq)).pipe(
          Effect.andThen(Deferred.succeed(ackedOnce, seq).pipe(Effect.ignore)),
        ),
    };
    return { acked, ackedOnce, closed, incoming, sent, transport };
  });

describe("runWebSocketSubscriber", () => {
  it("sends event frames and advances the durable ACK cursor", async () => {
    const event = positionedEventAt(1);
    const program = Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* makeTransport();
        const fiber = yield* runWebSocketSubscriber({
          subscriberId: SUBSCRIBER_ID,
          resumeSeq: SequenceNumber.make(0),
          policy: defaultEDAWebSocketFlowControl,
          transport: transport.transport,
        }).pipe(Effect.forkScoped);

        const hello = yield* Queue.take(transport.sent);
        const frame = yield* Queue.take(transport.sent);
        if (frame._tag !== "events") {
          throw new Error("Expected events frame");
        }
        yield* Queue.offer(
          transport.incoming,
          EDAWebSocketAckFrame.make({
            _tag: "ack",
            frameId: frame.frameId,
            durableThroughSeq: SequenceNumber.make(1),
          }),
        );
        yield* Deferred.await(transport.ackedOnce);
        yield* Deferred.succeed(transport.closed, undefined);
        yield* Fiber.await(fiber);
        return { acked: transport.acked, frame, hello };
      }),
    ).pipe(Effect.provide(makeRuntimeLayer([event])));

    const result = await Effect.runPromise(program);

    expect(result.hello._tag).toBe("hello");
    expect(result.hello).toMatchObject({
      flowControl: { heartbeatIntervalMs: 10_000 },
    });
    expect(result.frame).toMatchObject({ _tag: "events", events: [event] });
    expect(result.acked).toEqual([SequenceNumber.make(1)]);
  });

  it("sends large durable event frames under the default bounded frame limit", async () => {
    const event = positionedSubmitMessageEventAt({ seq: 1, text: "x".repeat(80_000) });
    const program = Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* makeTransport();
        const fiber = yield* runWebSocketSubscriber({
          subscriberId: SUBSCRIBER_ID,
          resumeSeq: SequenceNumber.make(0),
          policy: defaultEDAWebSocketFlowControl,
          transport: transport.transport,
        }).pipe(Effect.forkScoped);

        yield* Queue.take(transport.sent);
        const frame = yield* Queue.take(transport.sent);
        yield* Deferred.succeed(transport.closed, undefined);
        yield* Fiber.await(fiber);
        return frame;
      }),
    ).pipe(Effect.provide(makeRuntimeLayer([event])));

    const frame = await Effect.runPromise(program);

    expect(frame).toMatchObject({ _tag: "events", events: [event] });
  });

  it("sends idle heartbeats without requiring ACK", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* makeTransport();
        const fiber = yield* runWebSocketSubscriber({
          subscriberId: SUBSCRIBER_ID,
          resumeSeq: SequenceNumber.make(7),
          policy: {
            ...defaultEDAWebSocketFlowControl,
            ackTimeoutMs: 10,
            heartbeatIntervalMs: 1,
          },
          transport: transport.transport,
        }).pipe(Effect.forkScoped);

        yield* Queue.take(transport.sent);
        const heartbeat = yield* Queue.take(transport.sent);
        yield* Effect.sleep("25 millis");
        yield* Deferred.succeed(transport.closed, undefined);
        const exit = yield* Fiber.await(fiber);
        return { acked: transport.acked, exit, heartbeat };
      }),
    ).pipe(Effect.provide(makeRuntimeLayer([])));

    const result = await Effect.runPromise(program);

    expect(result.heartbeat).toMatchObject({
      _tag: "heartbeat",
      durableThroughSeq: SequenceNumber.make(7),
    });
    expect(result.acked).toEqual([]);
    expect(Exit.isSuccess(result.exit)).toBe(true);
  });

  it("fails with SubscriberLagged when the bounded subscriber queue overflows", async () => {
    const events = [positionedEventAt(1), positionedEventAt(2), positionedEventAt(3)];
    const program = Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* makeTransport();
        return yield* runWebSocketSubscriber({
          subscriberId: SUBSCRIBER_ID,
          resumeSeq: SequenceNumber.make(0),
          policy: {
            ...defaultEDAWebSocketFlowControl,
            maxInFlightFrames: 1,
            subscriberBufferCapacityEvents: 1,
            ackTimeoutMs: 60_000,
          },
          transport: transport.transport,
        }).pipe(Effect.exit);
      }),
    ).pipe(Effect.provide(makeRuntimeLayer(events)));

    const exit = await Effect.runPromise(program);

    expect(Exit.isFailure(exit)).toBe(true);
    const failure = Exit.isFailure(exit) ? String(exit.cause) : "";
    expect(failure).toContain("SubscriberLagged");
  });
});
