import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { EDARuntime } from "./runtime";
import { EDASessionStoreError } from "./session-store";
import { SequenceNumber } from "../types/core";
import { UnixEpochMillis, type PositionedEvent } from "../types/events";
import {
  EDA_WEB_SOCKET_PROTOCOL_VERSION,
  EDAWebSocketAckFrame,
  EDAWebSocketEventsFrame,
  EDAWebSocketFlowControl,
  EDAWebSocketHeartbeatFrame,
  EDAWebSocketHelloFrame,
  FrameId,
  SubscriberId,
  type EDAWebSocketClientFrame,
  type EDAWebSocketServerFrame,
} from "../host/websocket-protocol";

/** Slow subscriber reasons that intentionally close only the lagging socket. */
export const SubscriberLagReason = Schema.Literals(["buffer-overflow", "ack-timeout"]);
export type SubscriberLagReason = typeof SubscriberLagReason.Type;

/** The subscriber could not keep up with the bounded live-delivery policy. */
export class SubscriberLagged extends Schema.TaggedErrorClass<SubscriberLagged>()(
  "SubscriberLagged",
  {
    subscriberId: SubscriberId,
    lastAckedSeq: SequenceNumber,
    reason: SubscriberLagReason,
  },
) {}

/** The client sent a malformed or impossible protocol frame. */
export class SubscriberProtocolError extends Schema.TaggedErrorClass<SubscriberProtocolError>()(
  "SubscriberProtocolError",
  {
    subscriberId: SubscriberId,
    message: Schema.String,
  },
) {}

/** The host WebSocket boundary failed while sending a frame. */
export class SubscriberSendFailed extends Schema.TaggedErrorClass<SubscriberSendFailed>()(
  "SubscriberSendFailed",
  {
    subscriberId: SubscriberId,
    cause: Schema.Defect(),
  },
) {}

/** Expected error surface for one WebSocket subscriber workflow. */
export type EDAWebSocketSubscriberError =
  | SubscriberLagged
  | SubscriberProtocolError
  | SubscriberSendFailed
  | EDASessionStoreError;

/** Transport boundary implemented by a host WebSocket adapter or tests. */
export interface EDAWebSocketSubscriberTransport {
  readonly send: (frame: EDAWebSocketServerFrame) => Effect.Effect<void, SubscriberSendFailed>;
  readonly incoming: Queue.Queue<EDAWebSocketClientFrame>;
  readonly closed: Deferred.Deferred<void>;
  readonly persistAck: (seq: SequenceNumber) => Effect.Effect<void>;
}

/** Configuration for one scoped ACK-window subscriber workflow. */
export interface EDAWebSocketSubscriberInput {
  readonly subscriberId: SubscriberId;
  readonly resumeSeq: SequenceNumber;
  readonly policy: EDAWebSocketFlowControl;
  readonly transport: EDAWebSocketSubscriberTransport;
}

interface SentFrame {
  readonly frameId: FrameId;
  readonly durableThroughSeq: SequenceNumber;
  readonly sentAtMs: number;
}

/** Run one scoped, ACK-gated live-event WebSocket subscriber. */
export const runWebSocketSubscriber = (
  input: EDAWebSocketSubscriberInput,
): Effect.Effect<void, EDAWebSocketSubscriberError, EDARuntime | Scope.Scope> =>
  Effect.gen(function* () {
    const eda = yield* EDARuntime;
    const outbound = yield* Queue.dropping<PositionedEvent>(
      input.policy.subscriberBufferCapacityEvents,
    );
    const windowSignal = yield* Queue.sliding<void>(1);
    const timeoutFailure = yield* Deferred.make<never, SubscriberLagged>();
    const lastAckedSeq = yield* Ref.make(input.resumeSeq);
    const lastAckedFrameId = yield* Ref.make(0);
    const lastSentFrameId = yield* Ref.make(0);
    const lastSentDurableThroughSeq = yield* Ref.make(input.resumeSeq);
    const nextFrameId = yield* Ref.make(FrameId.make(1));
    const inFlight = yield* Ref.make<ReadonlyArray<SentFrame>>([]);

    yield* input.transport.send(
      EDAWebSocketHelloFrame.make({
        _tag: "hello",
        protocolVersion: EDA_WEB_SOCKET_PROTOCOL_VERSION,
        subscriberId: input.subscriberId,
        resumeSeq: input.resumeSeq,
        flowControl: input.policy,
      }),
    );

    const events = yield* eda.eventsAfter(input.resumeSeq);
    // TODO(backpressure): this pump drains the replay/live stream into an intermediate queue ahead
    // of the ACK window. That bounded queue protects memory, but it still breaks end-to-end
    // backpressure from slow WebSocket clients to host-level paged durable replay.
    const pump = events.pipe(
      Stream.interruptWhen(Deferred.await(input.transport.closed)),
      Stream.runForEach((event) =>
        Queue.offer(outbound, event).pipe(
          Effect.flatMap((accepted) =>
            accepted
              ? Effect.void
              : Effect.gen(function* () {
                  const seq = yield* Ref.get(lastAckedSeq);
                  return yield* new SubscriberLagged({
                    subscriberId: input.subscriberId,
                    lastAckedSeq: seq,
                    reason: "buffer-overflow",
                  });
                }),
          ),
        ),
      ),
    );

    const sender = sendLoop({
      inFlight,
      input,
      lastAckedSeq,
      lastSentDurableThroughSeq,
      lastSentFrameId,
      nextFrameId,
      outbound,
      timeoutFailure,
      windowSignal,
    });
    const ackLoop = receiveAcks({
      inFlight,
      input,
      lastAckedFrameId,
      lastAckedSeq,
      lastSentFrameId,
      windowSignal,
    });

    yield* Effect.raceFirst(
      Deferred.await(input.transport.closed),
      Effect.raceFirst(
        Deferred.await(timeoutFailure),
        Effect.raceFirst(pump, Effect.raceFirst(sender, ackLoop)),
      ),
    );
  });

const sendLoop = (input: {
  readonly inFlight: Ref.Ref<ReadonlyArray<SentFrame>>;
  readonly input: EDAWebSocketSubscriberInput;
  readonly lastAckedSeq: Ref.Ref<SequenceNumber>;
  readonly lastSentDurableThroughSeq: Ref.Ref<SequenceNumber>;
  readonly lastSentFrameId: Ref.Ref<number>;
  readonly nextFrameId: Ref.Ref<FrameId>;
  readonly outbound: Queue.Queue<PositionedEvent>;
  readonly timeoutFailure: Deferred.Deferred<never, SubscriberLagged>;
  readonly windowSignal: Queue.Queue<void>;
}): Effect.Effect<
  void,
  SubscriberLagged | SubscriberProtocolError | SubscriberSendFailed,
  Scope.Scope
> =>
  Effect.forever(
    Effect.gen(function* () {
      yield* waitForWindow(
        input.inFlight,
        input.windowSignal,
        input.input.policy.maxInFlightFrames,
      );
      const next = yield* takeNextOutboundOrHeartbeat(
        input.outbound,
        input.input.policy.heartbeatIntervalMs,
      );
      if (next._tag === "heartbeat") {
        yield* sendHeartbeat(input.input, input.lastSentDurableThroughSeq);
        return;
      }

      const event = next.event;
      const frameId = yield* Ref.get(input.nextFrameId);
      yield* Ref.set(input.nextFrameId, FrameId.make(frameId + 1));

      const previousSentDurableSeq = yield* Ref.get(input.lastSentDurableThroughSeq);
      const durableThroughSeq = durableThroughForFrame([event], previousSentDurableSeq);
      const frame = EDAWebSocketEventsFrame.make({
        _tag: "events",
        frameId,
        events: [event],
        durableThroughSeq,
      });
      const encodedBytes = encodedFrameBytes(frame);
      if (encodedBytes > input.input.policy.maxFrameBytes) {
        return yield* new SubscriberProtocolError({
          subscriberId: input.input.subscriberId,
          message: `Encoded WebSocket frame exceeded maxFrameBytes (${encodedBytes} > ${input.input.policy.maxFrameBytes})`,
        });
      }

      const sentAtMs = yield* Clock.currentTimeMillis;
      const sent = { frameId, durableThroughSeq, sentAtMs } satisfies SentFrame;
      // Mark the frame in-flight before the synchronous host send so an immediate
      // client ACK cannot race ahead of our bookkeeping.
      yield* Ref.update(input.inFlight, (frames) => [...frames, sent]);
      yield* Ref.set(input.lastSentFrameId, frameId);
      yield* Ref.set(input.lastSentDurableThroughSeq, durableThroughSeq);
      yield* input.input.transport.send(frame);
      yield* startAckTimeout(
        input.input,
        input.inFlight,
        input.lastAckedSeq,
        input.timeoutFailure,
        sent,
      );
    }),
  );

const takeNextOutboundOrHeartbeat = (
  outbound: Queue.Queue<PositionedEvent>,
  heartbeatIntervalMs: number,
): Effect.Effect<
  { readonly _tag: "event"; readonly event: PositionedEvent } | { readonly _tag: "heartbeat" }
> =>
  Effect.raceFirst(
    Queue.take(outbound).pipe(Effect.map((event) => ({ _tag: "event", event }) as const)),
    Effect.sleep(`${heartbeatIntervalMs} millis`).pipe(Effect.as({ _tag: "heartbeat" } as const)),
  );

const sendHeartbeat = (
  input: EDAWebSocketSubscriberInput,
  lastSentDurableThroughSeq: Ref.Ref<SequenceNumber>,
): Effect.Effect<void, SubscriberSendFailed> =>
  Effect.gen(function* () {
    const durableThroughSeq = yield* Ref.get(lastSentDurableThroughSeq);
    const serverTimeMs = yield* Clock.currentTimeMillis;
    yield* input.transport.send(
      EDAWebSocketHeartbeatFrame.make({
        _tag: "heartbeat",
        serverTimeMs: UnixEpochMillis.make(serverTimeMs),
        durableThroughSeq,
      }),
    );
  });

const receiveAcks = (input: {
  readonly inFlight: Ref.Ref<ReadonlyArray<SentFrame>>;
  readonly input: EDAWebSocketSubscriberInput;
  readonly lastAckedFrameId: Ref.Ref<number>;
  readonly lastAckedSeq: Ref.Ref<SequenceNumber>;
  readonly lastSentFrameId: Ref.Ref<number>;
  readonly windowSignal: Queue.Queue<void>;
}): Effect.Effect<void, SubscriberProtocolError> =>
  Effect.forever(
    Effect.gen(function* () {
      const frame = yield* Queue.take(input.input.transport.incoming);
      if (frame._tag !== "ack") {
        return yield* new SubscriberProtocolError({
          subscriberId: input.input.subscriberId,
          message: `Unsupported client frame ${
            (
              frame as {
                readonly _tag?: string;
              }
            )._tag ?? "unknown"
          }`,
        });
      }
      yield* applyAck(input, frame as EDAWebSocketAckFrame);
    }),
  );

const applyAck = (
  input: Parameters<typeof receiveAcks>[0],
  ack: EDAWebSocketAckFrame,
): Effect.Effect<void, SubscriberProtocolError> =>
  Effect.gen(function* () {
    const lastAckedFrame = yield* Ref.get(input.lastAckedFrameId);
    const lastSentFrame = yield* Ref.get(input.lastSentFrameId);
    const currentSeq = yield* Ref.get(input.lastAckedSeq);

    if (ack.frameId <= lastAckedFrame) {
      if (ack.durableThroughSeq > currentSeq) {
        return yield* new SubscriberProtocolError({
          subscriberId: input.input.subscriberId,
          message: "Duplicate ACK attempted to advance durable seq",
        });
      }
      return;
    }

    if (ack.frameId > lastSentFrame) {
      return yield* new SubscriberProtocolError({
        subscriberId: input.input.subscriberId,
        message: "ACK referenced an unsent frame",
      });
    }

    if (ack.durableThroughSeq < currentSeq) {
      return yield* new SubscriberProtocolError({
        subscriberId: input.input.subscriberId,
        message: "ACK durable seq moved backwards",
      });
    }

    const frames = yield* Ref.get(input.inFlight);
    const acknowledged = frames.filter((frame) => frame.frameId <= ack.frameId);
    if (acknowledged.length === 0) {
      return yield* new SubscriberProtocolError({
        subscriberId: input.input.subscriberId,
        message: "ACK referenced no in-flight frames",
      });
    }
    const allowedDurableSeq = SequenceNumber.make(
      acknowledged.reduce<number>(
        (max, frame) => Math.max(max, frame.durableThroughSeq),
        currentSeq,
      ),
    );
    if (ack.durableThroughSeq > allowedDurableSeq) {
      return yield* new SubscriberProtocolError({
        subscriberId: input.input.subscriberId,
        message: "ACK durable seq exceeded sent frame boundary",
      });
    }

    yield* Ref.update(input.inFlight, (frames) =>
      frames.filter((frame) => frame.frameId > ack.frameId),
    );
    yield* Ref.set(input.lastAckedFrameId, ack.frameId);
    yield* Ref.set(input.lastAckedSeq, ack.durableThroughSeq);
    yield* input.input.transport.persistAck(ack.durableThroughSeq);
    yield* Queue.offer(input.windowSignal, undefined);
  });

const waitForWindow = (
  inFlight: Ref.Ref<ReadonlyArray<SentFrame>>,
  windowSignal: Queue.Queue<void>,
  maxInFlightFrames: number,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    while (true) {
      const frames = yield* Ref.get(inFlight);
      if (frames.length < maxInFlightFrames) {
        return;
      }
      yield* Queue.take(windowSignal);
    }
  });

const startAckTimeout = (
  input: EDAWebSocketSubscriberInput,
  inFlight: Ref.Ref<ReadonlyArray<SentFrame>>,
  lastAckedSeq: Ref.Ref<SequenceNumber>,
  timeoutFailure: Deferred.Deferred<never, SubscriberLagged>,
  sent: SentFrame,
): Effect.Effect<void, never, Scope.Scope> =>
  Effect.gen(function* () {
    yield* Effect.sleep(`${input.policy.ackTimeoutMs} millis`);
    const stillInFlight = (yield* Ref.get(inFlight)).some(
      (frame) => frame.frameId === sent.frameId,
    );
    if (!stillInFlight) {
      return;
    }
    const seq = yield* Ref.get(lastAckedSeq);
    yield* Deferred.fail(
      timeoutFailure,
      new SubscriberLagged({
        subscriberId: input.subscriberId,
        lastAckedSeq: seq,
        reason: "ack-timeout",
      }),
    );
  }).pipe(Effect.forkScoped, Effect.asVoid);

const durableThroughForFrame = (
  events: ReadonlyArray<PositionedEvent>,
  previousSentDurableSeq: SequenceNumber,
): SequenceNumber =>
  SequenceNumber.make(
    events.reduce<number>(
      (max, event) =>
        event.event.durability === "durable" ? Math.max(max, event.position.seq) : max,
      previousSentDurableSeq,
    ),
  );

const encodedFrameBytes = (frame: EDAWebSocketServerFrame): number =>
  new TextEncoder().encode(JSON.stringify(frame)).byteLength;
