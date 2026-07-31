import * as Schema from "effect/Schema";

import { Position } from "../types/core";
import { EDADurableEvent, EDAEphemeralEvent, type EventEnvelope } from "../types/events";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/** Current EDA live-event WebSocket wire protocol version. */
export const EDA_WEB_SOCKET_WIRE_PROTOCOL_VERSION = 1 as const;

/** Flow-control policy announced by the server in the hello frame. */
export const EDAWebSocketWireFlowControl = Schema.Struct({
  ackTimeoutMs: PositiveInt,
  heartbeatIntervalMs: PositiveInt,
  maxFrameBytes: PositiveInt,
  maxFrameEvents: PositiveInt,
  maxInFlightFrames: PositiveInt,
  subscriberBufferCapacityEvents: PositiveInt,
});
export type EDAWebSocketWireFlowControl = typeof EDAWebSocketWireFlowControl.Type;

export const EDAWebSocketWireHelloFrame = Schema.Struct({
  _tag: Schema.Literal("hello"),
  protocolVersion: Schema.Literal(EDA_WEB_SOCKET_WIRE_PROTOCOL_VERSION),
  subscriberId: Schema.NonEmptyString,
  resumeSeq: NonNegativeInt,
  flowControl: EDAWebSocketWireFlowControl,
});
export type EDAWebSocketWireHelloFrame = typeof EDAWebSocketWireHelloFrame.Type;

export const EDAWebSocketWireHeartbeatFrame = Schema.Struct({
  _tag: Schema.Literal("heartbeat"),
  serverTimeMs: NonNegativeInt,
  durableThroughSeq: NonNegativeInt,
});
export type EDAWebSocketWireHeartbeatFrame = typeof EDAWebSocketWireHeartbeatFrame.Type;

export const EDAWebSocketWireLaggedFrame = Schema.Struct({
  _tag: Schema.Literal("lagged"),
  resumeSeq: NonNegativeInt,
  reason: Schema.Literals(["buffer-overflow", "ack-timeout"]),
});
export type EDAWebSocketWireLaggedFrame = typeof EDAWebSocketWireLaggedFrame.Type;

export const EDAWebSocketWireErrorFrame = Schema.Struct({
  _tag: Schema.Literal("error"),
  message: Schema.String,
});
export type EDAWebSocketWireErrorFrame = typeof EDAWebSocketWireErrorFrame.Type;

export const EDAWebSocketWireAckFrame = Schema.Struct({
  _tag: Schema.Literal("ack"),
  frameId: PositiveInt,
  durableThroughSeq: NonNegativeInt,
});
export type EDAWebSocketWireAckFrame = typeof EDAWebSocketWireAckFrame.Type;

export const EDAWebSocketWireClientFrame = Schema.Union([EDAWebSocketWireAckFrame]);
export type EDAWebSocketWireClientFrame = typeof EDAWebSocketWireClientFrame.Type;

/** Minimal encoder surface consumed by an EDA host. */
export interface EDAWebSocketServerFrameEncoder {
  readonly encodeServerFrame: (frame: unknown) => string;
}

/** Exhaustive decision table for a concrete WebSocket event union. */
export type EDAWebSocketEventConsumerPolicy<
  Event extends { readonly type: string },
  Disposition,
> = Readonly<Record<Event["type"], Disposition>>;

/** Complete framework-owned event union present on every EDA WebSocket. */
export const EDAFrameworkWebSocketEvent = Schema.Union([EDADurableEvent, EDAEphemeralEvent]);
export type EDAFrameworkWebSocketEvent = typeof EDAFrameworkWebSocketEvent.Type;

const makeEDAWebSocketWireProtocolFromEventSchema = <
  EventSchema extends Schema.Codec<unknown, unknown, never, never>,
>(
  event: EventSchema,
) => {
  const DomainPositionedEvent = Schema.Struct({
    position: Position,
    event,
  });
  const DomainEventsFrame = Schema.Struct({
    _tag: Schema.Literal("events"),
    frameId: PositiveInt,
    events: Schema.NonEmptyArray(DomainPositionedEvent),
    durableThroughSeq: NonNegativeInt,
  });
  const DomainServerFrame = Schema.Union([
    EDAWebSocketWireHelloFrame,
    DomainEventsFrame,
    EDAWebSocketWireHeartbeatFrame,
    EDAWebSocketWireLaggedFrame,
    EDAWebSocketWireErrorFrame,
  ]);
  return {
    event: Schema.toEncoded(event),
    positionedEvent: Schema.toEncoded(DomainPositionedEvent),
    eventsFrame: Schema.toEncoded(DomainEventsFrame),
    serverFrame: Schema.toEncoded(DomainServerFrame),
    clientFrame: EDAWebSocketWireClientFrame,
    encodeServerFrame: (frame: unknown): string =>
      JSON.stringify(Schema.encodeUnknownSync(DomainServerFrame)(frame)),
  };
};

/** Strict framework-only protocol used when an app registers no custom events. */
export const edaFrameworkWebSocketWireProtocol = makeEDAWebSocketWireProtocolFromEventSchema(
  EDAFrameworkWebSocketEvent,
);

/**
 * Bind the generic EDA WebSocket transport to one app's custom event schema.
 *
 * Framework durable and ephemeral events are included automatically. Future EDA
 * apps register only their custom events, then share the returned schemas
 * between their server host and browser consumer.
 */
export const makeEDAWebSocketWireProtocol = <
  AppEventSchema extends Schema.Codec<EventEnvelope, unknown, never, never>,
>(options: {
  readonly appEvents: AppEventSchema;
}) =>
  makeEDAWebSocketWireProtocolFromEventSchema(
    Schema.Union([EDAFrameworkWebSocketEvent, options.appEvents]),
  );
