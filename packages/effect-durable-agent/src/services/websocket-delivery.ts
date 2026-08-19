import * as Schema from "effect/Schema";

import { SequenceNumber } from "../types/core";
import type { PositionedEvent } from "../types/events";
import {
  EDA_WEB_SOCKET_PROTOCOL_VERSION,
  EDA_WS_CLOSE_LAGGED,
  EDA_WS_CLOSE_PROTOCOL_ERROR,
  EDAWebSocketEventsFrame,
  EDAWebSocketErrorFrame,
  EDAWebSocketHelloFrame,
  EDAWebSocketLaggedFrame,
  FrameId,
  SubscriberId,
  laggedCloseReason,
  type EDAWebSocketAckFrame,
  type EDAWebSocketFlowControl,
  type EDAWebSocketServerFrame,
} from "../host/websocket-protocol";

/** Slow subscriber reasons that intentionally close only the lagging socket. */
export const SubscriberLagReason = Schema.Literals(["buffer-overflow", "ack-timeout"]);
export type SubscriberLagReason = typeof SubscriberLagReason.Type;

/** One sent-but-unacknowledged events frame. */
export interface InFlightFrame {
  readonly frameId: FrameId;
  readonly durableThroughSeq: SequenceNumber;
  readonly sentAtMs: number;
}

/**
 * In-memory delivery state for one accepted event WebSocket.
 *
 * This state is deliberately disposable: hibernation-capable hosts persist
 * only the acknowledged durable cursor in the socket attachment and rebuild
 * a cold state after isolate eviction. Nothing here owns a fiber, a timer,
 * or a stream; all progress is driven by explicit host calls.
 */
export interface EDAWebSocketDeliveryState {
  readonly subscriberId: SubscriberId;
  readonly policy: EDAWebSocketFlowControl;
  /**
   * True when this state was rebuilt from a persisted attachment after the
   * in-memory frame bookkeeping was lost. Frame-id validation is relaxed for
   * the remainder of the connection because pre-eviction frame ids are
   * unknowable; durable cursors stay strictly monotonic either way.
   */
  readonly coldRestored: boolean;
  readonly lastAckedSeq: SequenceNumber;
  readonly lastAckedFrameId: number;
  readonly lastSentFrameId: number;
  readonly nextFrameId: number;
  readonly sentDurableThroughSeq: SequenceNumber;
  readonly inFlight: ReadonlyArray<InFlightFrame>;
  readonly pendingEphemeral: ReadonlyArray<PositionedEvent>;
}

/** Close instruction for the host WebSocket boundary. */
export interface DeliveryClose {
  readonly code: number;
  readonly reason: string;
}

/** Result of one pure delivery transition. */
export interface EDAWebSocketDeliveryResult {
  readonly state: EDAWebSocketDeliveryState;
  /** Frames to send in order, including any final lagged/error frame before `close`. */
  readonly frames: ReadonlyArray<EDAWebSocketServerFrame>;
  /** When set, persist this acknowledged durable cursor to the socket attachment. */
  readonly persistSeq?: SequenceNumber;
  /** When set, send `frames` then close the socket and drop its state. */
  readonly close?: DeliveryClose;
  /** When set, the host should read a durable slice after this seq and call `onDurableEvents`. */
  readonly wantsCatchUpAfterSeq?: SequenceNumber;
}

/** Inputs for one fresh or cold-restored delivery state. */
export interface MakeDeliveryStateInput {
  readonly subscriberId: SubscriberId;
  readonly resumeSeq: SequenceNumber;
  readonly policy: EDAWebSocketFlowControl;
  readonly coldRestored?: boolean;
}

/** Build the delivery state for a new connection or a post-eviction restore. */
export const makeWebSocketDeliveryState = (
  input: MakeDeliveryStateInput,
): EDAWebSocketDeliveryState => ({
  subscriberId: input.subscriberId,
  policy: input.policy,
  coldRestored: input.coldRestored ?? false,
  lastAckedSeq: input.resumeSeq,
  lastAckedFrameId: 0,
  lastSentFrameId: 0,
  nextFrameId: 1,
  sentDurableThroughSeq: input.resumeSeq,
  inFlight: [],
  pendingEphemeral: [],
});

/** First frame for a freshly accepted connection. Cold restores never re-send hello. */
export const deliveryHelloFrame = (state: EDAWebSocketDeliveryState): EDAWebSocketHelloFrame =>
  EDAWebSocketHelloFrame.make({
    _tag: "hello",
    protocolVersion: EDA_WEB_SOCKET_PROTOCOL_VERSION,
    subscriberId: state.subscriberId,
    resumeSeq: state.lastAckedSeq,
    flowControl: state.policy,
  });

/**
 * Deliver committed durable events read from the store.
 *
 * The store is the only buffer for durable events: frames are emitted while
 * the ACK window has room and the remainder is requested again via
 * `wantsCatchUpAfterSeq` once ACKs reopen the window.
 */
export const onDurableEvents = (
  state: EDAWebSocketDeliveryState,
  events: ReadonlyArray<PositionedEvent>,
  head: SequenceNumber,
  nowMs: number,
): EDAWebSocketDeliveryResult => {
  const stale = staleAckResult(state, nowMs);
  if (stale !== undefined) {
    return stale;
  }

  let next = state;
  const frames: EDAWebSocketServerFrame[] = [];
  let batch: PositionedEvent[] = [];

  const flushBatch = (): EDAWebSocketDeliveryResult | undefined => {
    if (batch.length === 0) {
      return undefined;
    }
    const sent = sendEventsFrame(next, batch, nowMs);
    if (sent.close !== undefined) {
      return { ...sent, frames: [...frames, ...sent.frames] };
    }
    next = sent.state;
    frames.push(...sent.frames);
    batch = [];
    return undefined;
  };

  for (const event of events) {
    if (event.event.durability === "durable" && event.position.seq <= next.sentDurableThroughSeq) {
      continue;
    }
    if (batch.length === 0 && windowCapacity(next) <= 0) {
      break;
    }
    batch.push(event);
    if (batch.length >= next.policy.maxFrameEvents) {
      const closed = flushBatch();
      if (closed !== undefined) {
        return closed;
      }
    }
  }
  const closed = flushBatch();
  if (closed !== undefined) {
    return closed;
  }

  return {
    state: next,
    frames,
    ...(windowCapacity(next) > 0 && next.sentDurableThroughSeq < head
      ? { wantsCatchUpAfterSeq: next.sentDurableThroughSeq }
      : {}),
  };
};

/**
 * Deliver one live ephemeral event.
 *
 * Ephemeral events have no durable backing, so a bounded in-memory buffer
 * holds them while the ACK window is full; overflow lag-closes only this
 * subscriber, mirroring the previous streaming behavior.
 */
export const onEphemeralEvent = (
  state: EDAWebSocketDeliveryState,
  event: PositionedEvent,
  nowMs: number,
): EDAWebSocketDeliveryResult => {
  const stale = staleAckResult(state, nowMs);
  if (stale !== undefined) {
    return stale;
  }

  if (windowCapacity(state) > 0 && state.pendingEphemeral.length === 0) {
    return sendEventsFrame(state, [event], nowMs);
  }

  const pendingEphemeral = [...state.pendingEphemeral, event];
  if (pendingEphemeral.length > state.policy.subscriberBufferCapacityEvents) {
    return laggedResult(state, "buffer-overflow");
  }
  return { state: { ...state, pendingEphemeral }, frames: [] };
};

/** Apply one client ACK, reopen the window, and drain buffered ephemeral events. */
export const onClientAck = (
  state: EDAWebSocketDeliveryState,
  ack: EDAWebSocketAckFrame,
  nowMs: number,
): EDAWebSocketDeliveryResult => {
  const validated = state.coldRestored ? applyColdAck(state, ack) : applyWarmAck(state, ack);
  if (validated._tag === "close") {
    return validated.result;
  }
  if (validated._tag === "ignore") {
    return { state, frames: [] };
  }

  let next = validated.state;
  const frames: EDAWebSocketServerFrame[] = [];
  while (next.pendingEphemeral.length > 0 && windowCapacity(next) > 0) {
    const [head, ...rest] = next.pendingEphemeral;
    const sent = sendEventsFrame({ ...next, pendingEphemeral: rest }, [head!], nowMs);
    if (sent.close !== undefined) {
      return {
        ...sent,
        frames: [...frames, ...sent.frames],
        persistSeq: validated.persistSeq,
      };
    }
    next = sent.state;
    frames.push(...sent.frames);
  }

  const stale = staleAckResult(next, nowMs);
  if (stale !== undefined) {
    return { ...stale, frames: [...frames, ...stale.frames], persistSeq: validated.persistSeq };
  }

  return {
    state: next,
    frames,
    persistSeq: validated.persistSeq,
    ...(windowCapacity(next) > 0 && next.pendingEphemeral.length === 0
      ? { wantsCatchUpAfterSeq: next.sentDurableThroughSeq }
      : {}),
  };
};

type AckValidation =
  | { readonly _tag: "ignore" }
  | { readonly _tag: "close"; readonly result: EDAWebSocketDeliveryResult }
  | {
      readonly _tag: "apply";
      readonly state: EDAWebSocketDeliveryState;
      readonly persistSeq: SequenceNumber;
    };

const applyWarmAck = (
  state: EDAWebSocketDeliveryState,
  ack: EDAWebSocketAckFrame,
): AckValidation => {
  if (ack.frameId <= state.lastAckedFrameId) {
    return ack.durableThroughSeq > state.lastAckedSeq
      ? protocolClose(state, "Duplicate ACK attempted to advance durable seq")
      : { _tag: "ignore" };
  }
  if (ack.frameId > state.lastSentFrameId) {
    return protocolClose(state, "ACK referenced an unsent frame");
  }
  if (ack.durableThroughSeq < state.lastAckedSeq) {
    return protocolClose(state, "ACK durable seq moved backwards");
  }
  const acknowledged = state.inFlight.filter((frame) => frame.frameId <= ack.frameId);
  if (acknowledged.length === 0) {
    return protocolClose(state, "ACK referenced no in-flight frames");
  }
  const allowedDurableSeq = SequenceNumber.make(
    acknowledged.reduce<number>(
      (max, frame) => Math.max(max, frame.durableThroughSeq),
      state.lastAckedSeq,
    ),
  );
  if (ack.durableThroughSeq > allowedDurableSeq) {
    return protocolClose(state, "ACK durable seq exceeded sent frame boundary");
  }
  return {
    _tag: "apply",
    persistSeq: ack.durableThroughSeq,
    state: {
      ...state,
      inFlight: state.inFlight.filter((frame) => frame.frameId > ack.frameId),
      lastAckedFrameId: ack.frameId,
      lastAckedSeq: ack.durableThroughSeq,
    },
  };
};

const applyColdAck = (
  state: EDAWebSocketDeliveryState,
  ack: EDAWebSocketAckFrame,
): AckValidation => {
  // Pre-eviction frame ids are unknowable, so only durable-cursor monotonicity
  // is enforced; stale re-ACKs are ignored instead of protocol-closed.
  if (ack.durableThroughSeq < state.lastAckedSeq) {
    return { _tag: "ignore" };
  }
  const lastAckedSeq = SequenceNumber.make(Math.max(state.lastAckedSeq, ack.durableThroughSeq));
  return {
    _tag: "apply",
    persistSeq: lastAckedSeq,
    state: {
      ...state,
      inFlight: state.inFlight.filter((frame) => frame.frameId > ack.frameId),
      lastAckedFrameId: Math.max(state.lastAckedFrameId, ack.frameId),
      lastAckedSeq,
      // A late ACK can prove delivery beyond what this cold state has sent;
      // never re-send durable events the client has already acknowledged.
      sentDurableThroughSeq: SequenceNumber.make(
        Math.max(state.sentDurableThroughSeq, lastAckedSeq),
      ),
    },
  };
};

const windowCapacity = (state: EDAWebSocketDeliveryState): number =>
  state.policy.maxInFlightFrames - state.inFlight.length;

const sendEventsFrame = (
  state: EDAWebSocketDeliveryState,
  events: ReadonlyArray<PositionedEvent>,
  nowMs: number,
): EDAWebSocketDeliveryResult => {
  const frameId = FrameId.make(state.nextFrameId);
  const durableThroughSeq = SequenceNumber.make(
    events.reduce<number>(
      (max, event) =>
        event.event.durability === "durable" ? Math.max(max, event.position.seq) : max,
      state.sentDurableThroughSeq,
    ),
  );
  const frame = EDAWebSocketEventsFrame.make({
    _tag: "events",
    frameId,
    events: events as [PositionedEvent, ...PositionedEvent[]],
    durableThroughSeq,
  });
  const encodedBytes = new TextEncoder().encode(JSON.stringify(frame)).byteLength;
  if (encodedBytes > state.policy.maxFrameBytes) {
    const close = protocolClose(
      state,
      `Encoded WebSocket frame exceeded maxFrameBytes (${encodedBytes} > ${state.policy.maxFrameBytes})`,
    );
    return close.result;
  }
  return {
    state: {
      ...state,
      inFlight: [...state.inFlight, { frameId, durableThroughSeq, sentAtMs: nowMs }],
      lastSentFrameId: frameId,
      nextFrameId: frameId + 1,
      sentDurableThroughSeq: durableThroughSeq,
    },
    frames: [frame],
  };
};

/** Lazily enforce the ACK deadline: no resident timer, checked on each transition. */
const staleAckResult = (
  state: EDAWebSocketDeliveryState,
  nowMs: number,
): EDAWebSocketDeliveryResult | undefined => {
  const overdue = state.inFlight.some(
    (frame) => frame.sentAtMs + state.policy.ackTimeoutMs <= nowMs,
  );
  return overdue ? laggedResult(state, "ack-timeout") : undefined;
};

const laggedResult = (
  state: EDAWebSocketDeliveryState,
  reason: SubscriberLagReason,
): EDAWebSocketDeliveryResult => ({
  state,
  frames: [
    EDAWebSocketLaggedFrame.make({
      _tag: "lagged",
      resumeSeq: state.lastAckedSeq,
      reason,
    }),
  ],
  close: { code: EDA_WS_CLOSE_LAGGED, reason: laggedCloseReason(state.lastAckedSeq) },
});

const protocolClose = (
  state: EDAWebSocketDeliveryState,
  message: string,
): { readonly _tag: "close"; readonly result: EDAWebSocketDeliveryResult } => ({
  _tag: "close",
  result: {
    state,
    frames: [EDAWebSocketErrorFrame.make({ _tag: "error", message: message.slice(0, 120) })],
    close: { code: EDA_WS_CLOSE_PROTOCOL_ERROR, reason: "protocol" },
  },
});
