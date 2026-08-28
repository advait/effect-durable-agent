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
} from "./messages";

/** Slow subscriber reasons that intentionally close only the lagging socket. */
export const SubscriberLagReason = Schema.Literals(["buffer-overflow", "ack-timeout"]);
export type SubscriberLagReason = typeof SubscriberLagReason.Type;

/** One sent-but-unacknowledged events frame. */
export const EDAWebSocketInFlightFrame = Schema.Struct({
  frameId: FrameId,
  durableThroughSeq: SequenceNumber,
  sentAtMs: Schema.Number,
});
export type EDAWebSocketInFlightFrame = typeof EDAWebSocketInFlightFrame.Type;

/** Compact consecutive host-suppressed receipts blocked by a visible ACK. */
export const EDAWebSocketSuppressedRange = Schema.Struct({
  fromFrameId: FrameId,
  throughFrameId: FrameId,
  durableThroughSeq: SequenceNumber,
});
export type EDAWebSocketSuppressedRange = typeof EDAWebSocketSuppressedRange.Type;

/** Complete delivery bookkeeping persisted in the WebSocket attachment. */
export const EDAWebSocketDeliveryCheckpoint = Schema.Struct({
  lastAckedSeq: SequenceNumber,
  lastAckedFrameId: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  lastSentFrameId: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  nextFrameId: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  sentDurableThroughSeq: SequenceNumber,
  inFlight: Schema.Array(EDAWebSocketInFlightFrame),
  /** Host-projected receipts waiting behind an earlier client-visible ACK. */
  suppressed: Schema.optionalKey(Schema.Array(EDAWebSocketSuppressedRange)),
});
export type EDAWebSocketDeliveryCheckpoint = typeof EDAWebSocketDeliveryCheckpoint.Type;

/** In-memory delivery state for one accepted event WebSocket. */
export interface EDAWebSocketDeliveryState extends EDAWebSocketDeliveryCheckpoint {
  readonly subscriberId: SubscriberId;
  readonly policy: EDAWebSocketFlowControl;
  /** Ephemeral events are live-only and intentionally absent from the attachment. */
  readonly pendingEphemeral: ReadonlyArray<PositionedEvent>;
  readonly suppressed: ReadonlyArray<EDAWebSocketSuppressedRange>;
}

/** Explicit instructions emitted by the pure delivery state machine. */
export type EDAWebSocketDeliveryAction =
  | { readonly _tag: "Persist" }
  | { readonly _tag: "Send"; readonly frames: ReadonlyArray<EDAWebSocketServerFrame> }
  | { readonly _tag: "ReadEventPage"; readonly afterSeq: SequenceNumber }
  | { readonly _tag: "Close"; readonly code: number; readonly reason: string };

/** Result of one pure delivery transition. */
export interface EDAWebSocketDeliveryResult {
  readonly state: EDAWebSocketDeliveryState;
  readonly actions: ReadonlyArray<EDAWebSocketDeliveryAction>;
}

export interface MakeDeliveryStateInput {
  readonly subscriberId: SubscriberId;
  readonly resumeSeq: SequenceNumber;
  readonly policy: EDAWebSocketFlowControl;
}

/** Build delivery state for a newly accepted connection. */
export const makeWebSocketDeliveryState = (
  input: MakeDeliveryStateInput,
): EDAWebSocketDeliveryState => ({
  subscriberId: input.subscriberId,
  policy: input.policy,
  lastAckedSeq: input.resumeSeq,
  lastAckedFrameId: 0,
  lastSentFrameId: 0,
  nextFrameId: 1,
  sentDurableThroughSeq: input.resumeSeq,
  inFlight: [],
  suppressed: [],
  pendingEphemeral: [],
});

/** Rebuild exact ACK bookkeeping from a hibernating WebSocket attachment. */
export const restoreWebSocketDeliveryState = (input: {
  readonly subscriberId: SubscriberId;
  readonly policy: EDAWebSocketFlowControl;
  readonly checkpoint: EDAWebSocketDeliveryCheckpoint;
}): EDAWebSocketDeliveryState => ({
  subscriberId: input.subscriberId,
  policy: input.policy,
  ...input.checkpoint,
  pendingEphemeral: [],
  suppressed: input.checkpoint.suppressed ?? [],
});

/** Project in-memory delivery state into the attachment-safe checkpoint. */
export const checkpointWebSocketDeliveryState = (
  state: EDAWebSocketDeliveryState,
): EDAWebSocketDeliveryCheckpoint => ({
  lastAckedSeq: state.lastAckedSeq,
  lastAckedFrameId: state.lastAckedFrameId,
  lastSentFrameId: state.lastSentFrameId,
  nextFrameId: state.nextFrameId,
  sentDurableThroughSeq: state.sentDurableThroughSeq,
  inFlight: state.inFlight,
  ...(state.suppressed.length === 0 ? {} : { suppressed: state.suppressed }),
});

/** First frame for a freshly accepted connection. Restores never re-send hello. */
export const deliveryHelloFrame = (state: EDAWebSocketDeliveryState): EDAWebSocketHelloFrame =>
  EDAWebSocketHelloFrame.make({
    _tag: "hello",
    protocolVersion: EDA_WEB_SOCKET_PROTOCOL_VERSION,
    subscriberId: state.subscriberId,
    resumeSeq: state.lastAckedSeq,
    flowControl: state.policy,
  });

/** Deliver committed durable events read from the store. */
export const onDurableEvents = (
  state: EDAWebSocketDeliveryState,
  events: ReadonlyArray<PositionedEvent>,
  head: SequenceNumber,
  nowMs: number,
): EDAWebSocketDeliveryResult => {
  const stale = staleAckResult(state, nowMs);
  if (stale !== undefined) return stale;

  let next = state;
  const frames: EDAWebSocketServerFrame[] = [];
  let batch: PositionedEvent[] = [];

  const flushBatch = (): void => {
    if (batch.length === 0) return;
    const sent = sendEventsFrame(next, batch, nowMs);
    next = sent.state;
    frames.push(sent.frame);
    batch = [];
  };

  for (const event of events) {
    if (event.event.durability === "durable" && event.position.seq <= next.sentDurableThroughSeq) {
      continue;
    }
    if (batch.length === 0 && windowCapacity(next) <= 0) break;
    batch.push(event);
    if (batch.length >= next.policy.maxFrameEvents) flushBatch();
  }
  flushBatch();

  return sentResult(
    next,
    frames,
    windowCapacity(next) > 0 && next.sentDurableThroughSeq < head
      ? next.sentDurableThroughSeq
      : undefined,
  );
};

/** Deliver one live ephemeral event, buffering only while this isolate is active. */
export const onEphemeralEvent = (
  state: EDAWebSocketDeliveryState,
  event: PositionedEvent,
  nowMs: number,
): EDAWebSocketDeliveryResult => {
  const stale = staleAckResult(state, nowMs);
  if (stale !== undefined) return stale;

  if (windowCapacity(state) > 0 && state.pendingEphemeral.length === 0) {
    const sent = sendEventsFrame(state, [event], nowMs);
    return sentResult(sent.state, [sent.frame]);
  }

  const pendingEphemeral = [...state.pendingEphemeral, event];
  if (pendingEphemeral.length > state.policy.subscriberBufferCapacityEvents) {
    return laggedResult(state, "buffer-overflow");
  }
  return { state: { ...state, pendingEphemeral }, actions: [] };
};

/** Apply one client ACK, reopen the window, and drain buffered ephemeral events. */
export const onClientAck = (
  state: EDAWebSocketDeliveryState,
  ack: EDAWebSocketAckFrame,
  nowMs: number,
): EDAWebSocketDeliveryResult => {
  const validated = applyAck(state, ack);
  if (validated._tag === "close") return validated.result;
  if (validated._tag === "ignore") return { state, actions: [] };

  return reopenDeliveryWindow(validated.state, nowMs);
};

/**
 * Apply one exact host-suppressed receipt without cumulatively ACKing any
 * earlier client-visible frame.
 */
export const onHostSuppressedFrame = (
  state: EDAWebSocketDeliveryState,
  frame: Pick<EDAWebSocketEventsFrame, "durableThroughSeq" | "frameId">,
  nowMs: number,
): EDAWebSocketDeliveryResult => {
  const receipt = state.inFlight.find((candidate) => candidate.frameId === frame.frameId);
  if (receipt === undefined || receipt.durableThroughSeq !== frame.durableThroughSeq) {
    return protocolClose(state, "Host suppressed an unknown frame receipt").result;
  }
  const next = advanceSuppressedReceipts({
    ...state,
    inFlight: state.inFlight.filter((candidate) => candidate.frameId !== frame.frameId),
    suppressed: insertSuppressedReceipt(state.suppressed, receipt),
  });
  return reopenDeliveryWindow(next, nowMs);
};

const reopenDeliveryWindow = (
  state: EDAWebSocketDeliveryState,
  nowMs: number,
): EDAWebSocketDeliveryResult => {
  let next = state;
  const frames: EDAWebSocketServerFrame[] = [];
  while (next.pendingEphemeral.length > 0 && windowCapacity(next) > 0) {
    const [head, ...rest] = next.pendingEphemeral;
    if (head === undefined) break;
    const sent = sendEventsFrame({ ...next, pendingEphemeral: rest }, [head], nowMs);
    next = sent.state;
    frames.push(sent.frame);
  }

  const stale = staleAckResult(next, nowMs);
  if (stale !== undefined) return stale;

  return sentResult(
    next,
    frames,
    windowCapacity(next) > 0 && next.pendingEphemeral.length === 0
      ? next.sentDurableThroughSeq
      : undefined,
    true,
  );
};

type AckValidation =
  | { readonly _tag: "ignore" }
  | { readonly _tag: "close"; readonly result: EDAWebSocketDeliveryResult }
  | { readonly _tag: "apply"; readonly state: EDAWebSocketDeliveryState };

const applyAck = (state: EDAWebSocketDeliveryState, ack: EDAWebSocketAckFrame): AckValidation => {
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
  const acknowledgedVisible = state.inFlight.filter((frame) => frame.frameId <= ack.frameId);
  if (acknowledgedVisible.length === 0) {
    return protocolClose(state, "ACK referenced no in-flight frames");
  }
  if (
    state.suppressed.some(
      (range) => range.fromFrameId <= ack.frameId && ack.frameId <= range.throughFrameId,
    )
  ) {
    return protocolClose(state, "ACK referenced a host-suppressed frame");
  }
  const acknowledgedSuppressed = state.suppressed.filter(
    (range) => range.throughFrameId <= ack.frameId,
  );
  const acknowledged = [...acknowledgedVisible, ...acknowledgedSuppressed];
  const allowedDurableSeq = SequenceNumber.make(
    acknowledged.reduce<number>(
      (max, receipt) => Math.max(max, receipt.durableThroughSeq),
      state.lastAckedSeq,
    ),
  );
  if (ack.durableThroughSeq > allowedDurableSeq) {
    return protocolClose(state, "ACK durable seq exceeded sent frame boundary");
  }
  return {
    _tag: "apply",
    state: advanceSuppressedReceipts({
      ...state,
      inFlight: state.inFlight.filter((frame) => frame.frameId > ack.frameId),
      lastAckedFrameId: ack.frameId,
      lastAckedSeq: ack.durableThroughSeq,
      suppressed: state.suppressed.filter((range) => range.throughFrameId > ack.frameId),
    }),
  };
};

const advanceSuppressedReceipts = (state: EDAWebSocketDeliveryState): EDAWebSocketDeliveryState => {
  let lastAckedFrameId = state.lastAckedFrameId;
  let lastAckedSeq = state.lastAckedSeq;
  let consumed = 0;
  for (const range of state.suppressed) {
    if (range.fromFrameId !== lastAckedFrameId + 1) break;
    lastAckedFrameId = range.throughFrameId;
    lastAckedSeq = SequenceNumber.make(Math.max(lastAckedSeq, range.durableThroughSeq));
    consumed += 1;
  }
  return consumed === 0
    ? state
    : {
        ...state,
        lastAckedFrameId,
        lastAckedSeq,
        suppressed: state.suppressed.slice(consumed),
      };
};

const insertSuppressedReceipt = (
  ranges: ReadonlyArray<EDAWebSocketSuppressedRange>,
  receipt: EDAWebSocketInFlightFrame,
): ReadonlyArray<EDAWebSocketSuppressedRange> => {
  const inserted = [
    ...ranges,
    EDAWebSocketSuppressedRange.make({
      fromFrameId: receipt.frameId,
      throughFrameId: receipt.frameId,
      durableThroughSeq: receipt.durableThroughSeq,
    }),
  ].sort((left, right) => left.fromFrameId - right.fromFrameId);
  const compacted: EDAWebSocketSuppressedRange[] = [];
  for (const range of inserted) {
    const previous = compacted.at(-1);
    if (previous === undefined || range.fromFrameId > previous.throughFrameId + 1) {
      compacted.push(range);
      continue;
    }
    compacted[compacted.length - 1] = EDAWebSocketSuppressedRange.make({
      fromFrameId: previous.fromFrameId,
      throughFrameId: FrameId.make(Math.max(previous.throughFrameId, range.throughFrameId)),
      durableThroughSeq: SequenceNumber.make(
        Math.max(previous.durableThroughSeq, range.durableThroughSeq),
      ),
    });
  }
  return compacted;
};

const windowCapacity = (state: EDAWebSocketDeliveryState): number =>
  state.policy.maxInFlightFrames - state.inFlight.length;

const sendEventsFrame = (
  state: EDAWebSocketDeliveryState,
  events: ReadonlyArray<PositionedEvent>,
  nowMs: number,
): { readonly state: EDAWebSocketDeliveryState; readonly frame: EDAWebSocketEventsFrame } => {
  const [first, ...rest] = events;
  if (first === undefined) throw new Error("Cannot send an empty events frame");
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
    events: [first, ...rest],
    durableThroughSeq,
  });
  return {
    state: {
      ...state,
      inFlight: [...state.inFlight, { frameId, durableThroughSeq, sentAtMs: nowMs }],
      lastSentFrameId: frameId,
      nextFrameId: frameId + 1,
      sentDurableThroughSeq: durableThroughSeq,
    },
    frame,
  };
};

const sentResult = (
  state: EDAWebSocketDeliveryState,
  frames: ReadonlyArray<EDAWebSocketServerFrame>,
  catchUpAfterSeq?: SequenceNumber,
  stateChanged = frames.length > 0,
): EDAWebSocketDeliveryResult => ({
  state,
  actions: [
    ...(stateChanged ? ([{ _tag: "Persist" }] as const) : []),
    ...(frames.length > 0 ? ([{ _tag: "Send", frames }] as const) : []),
    ...(catchUpAfterSeq === undefined
      ? []
      : ([{ _tag: "ReadEventPage", afterSeq: catchUpAfterSeq }] as const)),
  ],
});

/** Lazily enforce the ACK deadline: no resident timer, checked on each transition. */
const staleAckResult = (
  state: EDAWebSocketDeliveryState,
  nowMs: number,
): EDAWebSocketDeliveryResult | undefined =>
  state.inFlight.some((frame) => frame.sentAtMs + state.policy.ackTimeoutMs <= nowMs)
    ? laggedResult(state, "ack-timeout")
    : undefined;

const laggedResult = (
  state: EDAWebSocketDeliveryState,
  reason: SubscriberLagReason,
): EDAWebSocketDeliveryResult => ({
  state,
  actions: [
    {
      _tag: "Send",
      frames: [
        EDAWebSocketLaggedFrame.make({
          _tag: "lagged",
          resumeSeq: state.lastAckedSeq,
          reason,
        }),
      ],
    },
    { _tag: "Close", code: EDA_WS_CLOSE_LAGGED, reason: laggedCloseReason(state.lastAckedSeq) },
  ],
});

const protocolClose = (
  state: EDAWebSocketDeliveryState,
  message: string,
): { readonly _tag: "close"; readonly result: EDAWebSocketDeliveryResult } => ({
  _tag: "close",
  result: {
    state,
    actions: [
      {
        _tag: "Send",
        frames: [EDAWebSocketErrorFrame.make({ _tag: "error", message: message.slice(0, 120) })],
      },
      { _tag: "Close", code: EDA_WS_CLOSE_PROTOCOL_ERROR, reason: "protocol" },
    ],
  },
});
