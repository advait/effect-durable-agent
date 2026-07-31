import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  EDA_WEB_SOCKET_WIRE_PROTOCOL_VERSION,
  EDAWebSocketWireAckFrame,
  EDAWebSocketWireClientFrame,
  EDAWebSocketWireErrorFrame,
  EDAWebSocketWireFlowControl,
  EDAWebSocketWireHeartbeatFrame,
  EDAWebSocketWireHelloFrame,
  EDAWebSocketWireLaggedFrame,
  edaFrameworkWebSocketWireProtocol,
  type EDAWebSocketServerFrameEncoder,
} from "./websocket-wire";

import { SequenceNumber, SessionId } from "../types/core";
import { PositionedEvent, UnixEpochMillis } from "../types/events";
import { EDATraceMetadata } from "../types/tracing";

/** Current EDA live-event WebSocket protocol version. */
export const EDA_WEB_SOCKET_PROTOCOL_VERSION = EDA_WEB_SOCKET_WIRE_PROTOCOL_VERSION;

/** WebSocket close code used when a subscriber cannot keep up with live delivery. */
export const EDA_WS_CLOSE_LAGGED = 4008;

/** WebSocket close code used for normal host-driven shutdown. */
export const EDA_WS_CLOSE_GOING_AWAY = 1001;

/** WebSocket close code used for malformed client protocol messages. */
export const EDA_WS_CLOSE_PROTOCOL_ERROR = 1002;

/** WebSocket close code used when the host cannot send a frame. */
export const EDA_WS_CLOSE_SEND_FAILED = 1011;

/** WebSocket close code used for unsupported binary messages. */
export const EDA_WS_CLOSE_UNSUPPORTED_DATA = 1003;

/** Live subscriber identity persisted in WebSocket attachments. */
export const SubscriberId = Schema.NonEmptyString.pipe(Schema.brand("SubscriberId"));
export type SubscriberId = typeof SubscriberId.Type;

/** Per-connection monotonically increasing server frame id. */
export const FrameId = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)).pipe(
  Schema.brand("FrameId"),
);
export type FrameId = typeof FrameId.Type;

/** Flow-control policy announced by the server in the hello frame. */
export const EDAWebSocketFlowControl = EDAWebSocketWireFlowControl;
export type EDAWebSocketFlowControl = typeof EDAWebSocketFlowControl.Type;

/** Default bounded policy sized for fast model deltas and large durable tool/message events. */
export const defaultEDAWebSocketFlowControl: EDAWebSocketFlowControl = {
  ackTimeoutMs: 30_000,
  heartbeatIntervalMs: 10_000,
  maxFrameBytes: 256 * 1024,
  maxFrameEvents: 1,
  maxInFlightFrames: 16,
  subscriberBufferCapacityEvents: 2_048,
};

/** First server frame; announces protocol and flow-control policy. */
export const EDAWebSocketHelloFrame = Schema.Struct({
  ...EDAWebSocketWireHelloFrame.fields,
  subscriberId: SubscriberId,
  resumeSeq: SequenceNumber,
});
export type EDAWebSocketHelloFrame = typeof EDAWebSocketHelloFrame.Type;

/** Ordered positioned events delivered to the client. */
export const EDAWebSocketEventsFrame = Schema.Struct({
  _tag: Schema.Literal("events"),
  frameId: FrameId,
  events: Schema.NonEmptyArray(PositionedEvent),
  durableThroughSeq: SequenceNumber,
});
export type EDAWebSocketEventsFrame = typeof EDAWebSocketEventsFrame.Type;

/** Idle server keepalive frame that clients may ignore and must not ACK. */
export const EDAWebSocketHeartbeatFrame = Schema.Struct({
  ...EDAWebSocketWireHeartbeatFrame.fields,
  serverTimeMs: UnixEpochMillis,
  durableThroughSeq: SequenceNumber,
});
export type EDAWebSocketHeartbeatFrame = typeof EDAWebSocketHeartbeatFrame.Type;

/** Optional final frame sent before a lag-close. */
export const EDAWebSocketLaggedFrame = Schema.Struct({
  ...EDAWebSocketWireLaggedFrame.fields,
  resumeSeq: SequenceNumber,
});
export type EDAWebSocketLaggedFrame = typeof EDAWebSocketLaggedFrame.Type;

/** Optional final frame sent before a protocol/internal close. */
export const EDAWebSocketErrorFrame = EDAWebSocketWireErrorFrame;
export type EDAWebSocketErrorFrame = typeof EDAWebSocketErrorFrame.Type;

/** Server-to-client application frame union. */
export const EDAWebSocketServerFrame = Schema.Union([
  EDAWebSocketHelloFrame,
  EDAWebSocketEventsFrame,
  EDAWebSocketHeartbeatFrame,
  EDAWebSocketLaggedFrame,
  EDAWebSocketErrorFrame,
]);
export type EDAWebSocketServerFrame = typeof EDAWebSocketServerFrame.Type;

/** Client acknowledgement after applying a server frame. */
export const EDAWebSocketAckFrame = Schema.Struct({
  ...EDAWebSocketWireAckFrame.fields,
  frameId: FrameId,
  durableThroughSeq: SequenceNumber,
});
export type EDAWebSocketAckFrame = typeof EDAWebSocketAckFrame.Type;

/** Client-to-server application frame union. */
export const EDAWebSocketClientFrame = Schema.Union([EDAWebSocketAckFrame]);
export type EDAWebSocketClientFrame = typeof EDAWebSocketClientFrame.Type;

/** Small attachment persisted by Cloudflare for hibernating WebSockets. */
export const EDAWebSocketAttachment = Schema.Struct({
  kind: Schema.Literal("eda-events-v1"),
  sessionId: SessionId,
  subscriberId: SubscriberId,
  lastAckedSeq: SequenceNumber,
  trace: EDATraceMetadata,
});
export type EDAWebSocketAttachment = typeof EDAWebSocketAttachment.Type;

/** Serialize a server frame for a text WebSocket message. */
export const encodeEDAWebSocketServerFrame = (
  frame: EDAWebSocketServerFrame,
  protocol?: EDAWebSocketServerFrameEncoder,
): string => (protocol ?? edaFrameworkWebSocketWireProtocol.host).encodeServerFrame(frame);

/** Decode an unknown attachment value from Cloudflare WebSocket hibernation storage. */
export const decodeEDAWebSocketAttachment = (
  input: unknown,
): Effect.Effect<EDAWebSocketAttachment, unknown> =>
  Schema.decodeUnknownEffect(EDAWebSocketAttachment)(input);

/** Decode an inbound text WebSocket message at the host boundary. */
export const decodeEDAWebSocketClientMessage = (
  input: string,
): Effect.Effect<EDAWebSocketClientFrame, unknown> =>
  Effect.try({
    try: () => JSON.parse(input) as unknown,
    catch: (error) => error,
  }).pipe(
    Effect.flatMap((json) => Schema.decodeUnknownEffect(EDAWebSocketWireClientFrame)(json)),
    Effect.flatMap((frame) => Schema.decodeUnknownEffect(EDAWebSocketClientFrame)(frame)),
  );

/** Short close reason that fits the WebSocket protocol reason limit. */
export const laggedCloseReason = (seq: SequenceNumber): string => `lag;seq=${seq}`;
