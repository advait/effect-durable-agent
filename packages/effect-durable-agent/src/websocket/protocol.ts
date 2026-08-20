import * as Schema from "effect/Schema";

import { Position } from "../types/core";
import { EDADurableEvent, EDAEphemeralEvent, EventEnvelope } from "../types/events";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/** Current EDA live-event WebSocket wire protocol version. */
export const EDA_WEB_SOCKET_WIRE_PROTOCOL_VERSION = 1 as const;

/** Flow-control policy announced by the server in the hello frame. */
export const EDAWebSocketWireFlowControl = Schema.Struct({
  ackTimeoutMs: PositiveInt,
  /** Deprecated: servers no longer send heartbeat frames. Kept so deployed clients can decode hello. */
  heartbeatIntervalMs: PositiveInt,
  maxFrameBytes: PositiveInt,
  maxFrameEvents: PositiveInt,
  maxInFlightFrames: PositiveInt,
  /** Suggested client-originated ping cadence; absent on servers that still send heartbeats. */
  pingIntervalMs: Schema.optionalKey(PositiveInt),
  subscriberBufferCapacityEvents: PositiveInt,
});
export type EDAWebSocketWireFlowControl = typeof EDAWebSocketWireFlowControl.Type;

/**
 * Canonical client liveness ping message.
 *
 * The exact string matters: hibernation-capable hosts register it with
 * WebSocket auto-response so the runtime answers without waking the object.
 */
export const EDA_WEB_SOCKET_PING_MESSAGE = '{"_tag":"ping"}';

/** Canonical server pong message paired with {@link EDA_WEB_SOCKET_PING_MESSAGE}. */
export const EDA_WEB_SOCKET_PONG_MESSAGE = '{"_tag":"pong"}';

/** Client liveness ping; hosts answer with a pong and never ACK-track it. */
export const EDAWebSocketWirePingFrame = Schema.Struct({
  _tag: Schema.Literal("ping"),
});
export type EDAWebSocketWirePingFrame = typeof EDAWebSocketWirePingFrame.Type;

/** Server liveness pong; clients may ignore it and must not ACK it. */
export const EDAWebSocketWirePongFrame = Schema.Struct({
  _tag: Schema.Literal("pong"),
});
export type EDAWebSocketWirePongFrame = typeof EDAWebSocketWirePongFrame.Type;

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

export const EDAWebSocketWireClientFrame = Schema.Union([
  EDAWebSocketWireAckFrame,
  EDAWebSocketWirePingFrame,
]);
export type EDAWebSocketWireClientFrame = typeof EDAWebSocketWireClientFrame.Type;

/** Exhaustive decision table for a concrete WebSocket event union. */
export type EDAWebSocketEventConsumerPolicy<
  Event extends { readonly type: string },
  Disposition,
> = Readonly<Record<Event["type"], Disposition>>;

/** Complete framework-owned event union present on every EDA WebSocket. */
export const EDAFrameworkWebSocketEvent = Schema.Union([EDADurableEvent, EDAEphemeralEvent]);
export type EDAFrameworkWebSocketEvent = typeof EDAFrameworkWebSocketEvent.Type;

const makeEDAWebSocketDomainSchemas = <
  EventSchema extends Schema.Codec<EventEnvelope, unknown, never, never>,
>(
  event: EventSchema,
) => {
  const positionedEvent = Schema.Struct({
    position: Position,
    event,
  });
  const eventsFrame = Schema.Struct({
    _tag: Schema.Literal("events"),
    frameId: PositiveInt,
    events: Schema.NonEmptyArray(positionedEvent),
    durableThroughSeq: NonNegativeInt,
  });
  const serverFrame = Schema.Union([
    EDAWebSocketWireHelloFrame,
    eventsFrame,
    EDAWebSocketWireHeartbeatFrame,
    EDAWebSocketWirePongFrame,
    EDAWebSocketWireLaggedFrame,
    EDAWebSocketWireErrorFrame,
  ]);
  return { event, positionedEvent, eventsFrame, serverFrame };
};

type EDAWebSocketDomainSchemas<
  EventSchema extends Schema.Codec<EventEnvelope, unknown, never, never>,
> = ReturnType<typeof makeEDAWebSocketDomainSchemas<EventSchema>>;

const EDAWebSocketHostDomain = makeEDAWebSocketDomainSchemas(EventEnvelope);

/** Domain frame shape accepted by an app-bound encoder at the EDA host boundary. */
export type EDAWebSocketServerFrameInput = typeof EDAWebSocketHostDomain.serverFrame.Type;

/** Minimal typed encoder surface consumed by an EDA host. */
export interface EDAWebSocketServerFrameEncoder {
  readonly encodeServerFrame: (frame: EDAWebSocketServerFrameInput) => string;
}

/** App-bound domain schemas, wire schemas, and encoders for protocol version 1. */
export interface EDAWebSocketWireProtocol<
  EventSchema extends Schema.Codec<EventEnvelope, unknown, never, never>,
> {
  /** Exact app-bound schemas whose `Type` values are used inside the server. */
  readonly domain: EDAWebSocketDomainSchemas<EventSchema>;
  /** Exact app-bound schemas whose `Type` values are the serialized JSON representation. */
  readonly wire: {
    readonly event: Schema.toEncoded<EventSchema>;
    readonly positionedEvent: Schema.toEncoded<
      EDAWebSocketDomainSchemas<EventSchema>["positionedEvent"]
    >;
    readonly eventsFrame: Schema.toEncoded<EDAWebSocketDomainSchemas<EventSchema>["eventsFrame"]>;
    readonly serverFrame: Schema.toEncoded<EDAWebSocketDomainSchemas<EventSchema>["serverFrame"]>;
    readonly clientFrame: typeof EDAWebSocketWireClientFrame;
  };
  /** Serialize one exact app-bound domain frame to a JSON text message. */
  readonly encodeServerFrame: (
    frame: Schema.Schema.Type<EDAWebSocketDomainSchemas<EventSchema>["serverFrame"]>,
  ) => string;
  /** Validate broad host frames against the app event union before serialization. */
  readonly host: EDAWebSocketServerFrameEncoder;
}

const makeEDAWebSocketWireProtocolFromEventSchema = <
  EventSchema extends Schema.Codec<EventEnvelope, unknown, never, never>,
>(
  event: EventSchema,
): EDAWebSocketWireProtocol<EventSchema> => {
  const domain = makeEDAWebSocketDomainSchemas(event);
  const encodeDomainServerFrame = Schema.encodeSync(domain.serverFrame);
  const encodeUnknownDomainServerFrame = Schema.encodeUnknownSync(domain.serverFrame);
  const encodeServerFrame = (frame: typeof domain.serverFrame.Type): string =>
    JSON.stringify(encodeDomainServerFrame(frame));
  const host: EDAWebSocketServerFrameEncoder = {
    encodeServerFrame: (frame) => JSON.stringify(encodeUnknownDomainServerFrame(frame)),
  };

  return {
    domain,
    wire: {
      event: Schema.toEncoded(domain.event),
      positionedEvent: Schema.toEncoded(domain.positionedEvent),
      eventsFrame: Schema.toEncoded(domain.eventsFrame),
      serverFrame: Schema.toEncoded(domain.serverFrame),
      clientFrame: EDAWebSocketWireClientFrame,
    },
    encodeServerFrame,
    host,
  };
};

/** Strict framework-only protocol used when an app registers no custom events. */
export const edaFrameworkWebSocketWireProtocol: EDAWebSocketWireProtocol<
  typeof EDAFrameworkWebSocketEvent
> = makeEDAWebSocketWireProtocolFromEventSchema(EDAFrameworkWebSocketEvent);

/**
 * Bind the generic EDA WebSocket transport to one app's custom event schema.
 *
 * Framework durable and ephemeral events are included automatically. Future EDA
 * apps register only their custom events. Servers use the returned domain schemas
 * and typed encoder; external consumers decode with the wire schemas. Pass the
 * returned `host` adapter to the selected host package.
 */
export const makeEDAWebSocketWireProtocol = <
  AppEventSchema extends Schema.Codec<EventEnvelope, unknown, never, never>,
>(options: {
  readonly appEvents: AppEventSchema;
}): EDAWebSocketWireProtocol<
  Schema.Union<readonly [typeof EDAFrameworkWebSocketEvent, AppEventSchema]>
> =>
  makeEDAWebSocketWireProtocolFromEventSchema(
    Schema.Union([EDAFrameworkWebSocketEvent, options.appEvents]),
  );
