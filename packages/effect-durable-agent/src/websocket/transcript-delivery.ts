import * as Schema from "effect/Schema";

import { SequenceNumber } from "../types/core";
import { TranscriptSubscriptionBase, type TranscriptUpdate } from "../transcript/subscription";
import type { EDAWebSocketFlowControl } from "./messages";
import type { EDAWebSocketWireAckFrame } from "./protocol";
import {
  EDATranscriptRequestFrame,
  edaTranscriptChunkFrameText,
  edaTranscriptUpdateFrameText,
  splitEDATranscriptUpdate,
  utf8ByteLength,
} from "./transcript";

const PositiveInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/** A chunked update whose frames are not all sent yet; its chunk data is volatile. */
export const EDATranscriptTransfer = Schema.Struct({
  updateSeq: PositiveInt,
  throughSeq: SequenceNumber,
  count: PositiveInt,
  nextIndex: NonNegativeInt,
});
export type EDATranscriptTransfer = typeof EDATranscriptTransfer.Type;

/**
 * Everything a transcript socket needs to resume after hibernation, persisted in its attachment
 * before every send and after every applied ACK.
 *
 * Transport receipts (`inFlight`, `lastAckedFrameId`) bound frames in flight exactly as on an
 * event socket. The applied cut (`appliedUpdateSeq`, `appliedThroughSeq`) is the last update the
 * client applied completely. `subscription` is the base the next update is planned against, and
 * `requests` are client requests not yet answered. A persisted `transfer` means chunk data was
 * staged in memory; a socket restored with one cannot finish it and is closed.
 */
export const EDATranscriptDeliveryCheckpoint = Schema.Struct({
  lastAckedFrameId: NonNegativeInt,
  nextFrameId: PositiveInt,
  inFlight: Schema.Array(Schema.Struct({ frameId: PositiveInt, sentAtMs: Schema.Number })),
  nextUpdateSeq: PositiveInt,
  completedUpdateSeq: NonNegativeInt,
  completedThroughSeq: SequenceNumber,
  appliedUpdateSeq: NonNegativeInt,
  appliedThroughSeq: SequenceNumber,
  transfer: Schema.optionalKey(EDATranscriptTransfer),
  subscription: Schema.optionalKey(TranscriptSubscriptionBase),
  requests: Schema.Array(EDATranscriptRequestFrame),
});
export type EDATranscriptDeliveryCheckpoint = typeof EDATranscriptDeliveryCheckpoint.Type;

/** Bounds on one transcript socket beyond the shared flow-control policy. */
export interface EDATranscriptDeliveryLimits {
  /** Largest encoded update staged for one transfer; a larger one closes the socket. */
  readonly maxStagedBytes: number;
  /** Unanswered client requests held at once; more is a protocol violation. */
  readonly maxPendingRequests: number;
}

export const defaultEDATranscriptDeliveryLimits: EDATranscriptDeliveryLimits = {
  maxStagedBytes: 8 * 1_048_576,
  maxPendingRequests: 4,
};

/** In-memory state of one transcript socket. */
export interface EDATranscriptDeliveryState extends EDATranscriptDeliveryCheckpoint {
  readonly policy: EDAWebSocketFlowControl;
  /** Chunk data of `transfer` not yet sent, in order. Never persisted. */
  readonly staged: ReadonlyArray<string>;
}

/** Delivery state for a newly accepted transcript socket. */
export const makeTranscriptDeliveryState = (
  policy: EDAWebSocketFlowControl,
): EDATranscriptDeliveryState => ({
  policy,
  lastAckedFrameId: 0,
  nextFrameId: 1,
  inFlight: [],
  nextUpdateSeq: 1,
  completedUpdateSeq: 0,
  completedThroughSeq: SequenceNumber.make(0),
  appliedUpdateSeq: 0,
  appliedThroughSeq: SequenceNumber.make(0),
  requests: [],
  staged: [],
});

/**
 * Restore a hibernated transcript socket. A socket that was mid-transfer lost its staged chunks
 * with the isolate and cannot resume: the caller closes it and the client resubscribes.
 */
export const restoreTranscriptDeliveryState = (
  policy: EDAWebSocketFlowControl,
  checkpoint: EDATranscriptDeliveryCheckpoint,
):
  | { readonly _tag: "Restored"; readonly state: EDATranscriptDeliveryState }
  | { readonly _tag: "TransferLost" } =>
  checkpoint.transfer === undefined
    ? { _tag: "Restored", state: { ...checkpoint, policy, staged: [] } }
    : { _tag: "TransferLost" };

/** The attachment-safe part of the state. */
export const checkpointTranscriptDelivery = (
  state: EDATranscriptDeliveryState,
): EDATranscriptDeliveryCheckpoint => ({
  lastAckedFrameId: state.lastAckedFrameId,
  nextFrameId: state.nextFrameId,
  inFlight: state.inFlight,
  nextUpdateSeq: state.nextUpdateSeq,
  completedUpdateSeq: state.completedUpdateSeq,
  completedThroughSeq: state.completedThroughSeq,
  appliedUpdateSeq: state.appliedUpdateSeq,
  appliedThroughSeq: state.appliedThroughSeq,
  ...(state.transfer === undefined ? {} : { transfer: state.transfer }),
  ...(state.subscription === undefined ? {} : { subscription: state.subscription }),
  requests: state.requests,
});

/** Frames that may be sent before the client's next receipt. */
export const transcriptDeliveryCapacity = (state: EDATranscriptDeliveryState): number =>
  state.policy.maxInFlightFrames - state.inFlight.length;

/** Lazily enforced ACK deadline: no resident timer, checked on each transition. */
export const transcriptAckTimedOut = (state: EDATranscriptDeliveryState, nowMs: number): boolean =>
  state.inFlight.some((frame) => frame.sentAtMs + state.policy.ackTimeoutMs <= nowMs);

/** Result of a transition that may emit frames. */
export interface EDATranscriptSendResult {
  readonly state: EDATranscriptDeliveryState;
  /** Encoded frames to send in order, after persisting `state`. */
  readonly frames: ReadonlyArray<string>;
}

/**
 * Start sending one planned update, captured from its own snapshot.
 *
 * `subscription` becomes the base for the next update immediately: nothing else is sent until
 * this update's last frame is, so the client can only apply later updates on top of it. An
 * update within the frame limit is one frame; a larger one is chunked and as many chunks as
 * flow control allows are sent now, the rest staged.
 */
export const startTranscriptUpdate = (
  state: EDATranscriptDeliveryState,
  input: {
    readonly update: TranscriptUpdate;
    readonly encodedUpdate: string;
    readonly subscription: TranscriptSubscriptionBase;
  },
  limits: EDATranscriptDeliveryLimits,
  nowMs: number,
):
  | ({ readonly _tag: "Sending" } & EDATranscriptSendResult)
  | { readonly _tag: "TooLarge"; readonly bytes: number } => {
  if (state.transfer !== undefined) {
    throw new Error("A transcript update cannot start while another is being transferred");
  }
  if (input.update.updateSeq !== state.nextUpdateSeq) {
    throw new Error("A transcript update must carry the socket's next update seq");
  }
  const updateSeq = input.update.updateSeq;
  const advanced: EDATranscriptDeliveryState = {
    ...state,
    nextUpdateSeq: updateSeq + 1,
    subscription: input.subscription,
  };
  const single = edaTranscriptUpdateFrameText(state.nextFrameId, input.encodedUpdate);
  if (utf8ByteLength(single) <= state.policy.maxFrameBytes) {
    return {
      _tag: "Sending",
      ...sendFrames(
        {
          ...advanced,
          completedUpdateSeq: updateSeq,
          completedThroughSeq: input.update.throughSeq,
        },
        [single],
        nowMs,
      ),
    };
  }
  const bytes = utf8ByteLength(input.encodedUpdate);
  if (bytes > limits.maxStagedBytes) return { _tag: "TooLarge", bytes };
  const chunks = splitEDATranscriptUpdate(input.encodedUpdate, state.policy.maxFrameBytes);
  return {
    _tag: "Sending",
    ...pumpTranscriptTransfer(
      {
        ...advanced,
        transfer: {
          updateSeq,
          throughSeq: input.update.throughSeq,
          count: chunks.length,
          nextIndex: 0,
        },
        staged: chunks,
      },
      nowMs,
    ),
  };
};

/** Send staged chunks of the transfer in progress while flow control allows. */
export const pumpTranscriptTransfer = (
  state: EDATranscriptDeliveryState,
  nowMs: number,
): EDATranscriptSendResult => {
  let next = state;
  const frames: string[] = [];
  while (next.transfer !== undefined && transcriptDeliveryCapacity(next) > 0) {
    const transfer: EDATranscriptTransfer = next.transfer;
    const [data, ...rest] = next.staged;
    if (data === undefined) break;
    const text = edaTranscriptChunkFrameText({
      frameId: next.nextFrameId,
      updateSeq: transfer.updateSeq,
      index: transfer.nextIndex,
      count: transfer.count,
      data,
    });
    const nextIndex = transfer.nextIndex + 1;
    const done = nextIndex === transfer.count;
    const { transfer: _sent, ...withoutTransfer } = next;
    const sent = sendFrames(
      done
        ? {
            ...withoutTransfer,
            staged: [],
            completedUpdateSeq: transfer.updateSeq,
            completedThroughSeq: transfer.throughSeq,
          }
        : { ...next, staged: rest, transfer: { ...transfer, nextIndex } },
      [text],
      nowMs,
    );
    next = sent.state;
    frames.push(...sent.frames);
  }
  return { state: next, frames };
};

const sendFrames = (
  state: EDATranscriptDeliveryState,
  texts: ReadonlyArray<string>,
  nowMs: number,
): EDATranscriptSendResult => {
  // Frame texts embed `state.nextFrameId` onwards in order.
  const inFlight = texts.map((_, index) => ({
    frameId: state.nextFrameId + index,
    sentAtMs: nowMs,
  }));
  return {
    state: {
      ...state,
      inFlight: [...state.inFlight, ...inFlight],
      nextFrameId: state.nextFrameId + texts.length,
    },
    frames: texts,
  };
};

/** Outcome of one client ACK on a transcript socket. */
export type EDATranscriptAckResult =
  | { readonly _tag: "Applied"; readonly state: EDATranscriptDeliveryState }
  | { readonly _tag: "Ignored" }
  | { readonly _tag: "ProtocolError"; readonly message: string };

/**
 * Apply one ACK. The receipt frees flow-control capacity cumulatively; the applied cut may only
 * name an update whose frames were all sent, and never moves backwards.
 */
export const onTranscriptAck = (
  state: EDATranscriptDeliveryState,
  ack: EDAWebSocketWireAckFrame,
): EDATranscriptAckResult => {
  const appliedUpdateSeq = ack.appliedUpdateSeq ?? state.appliedUpdateSeq;
  const advancesApplied =
    appliedUpdateSeq > state.appliedUpdateSeq || ack.durableThroughSeq > state.appliedThroughSeq;
  if (ack.frameId <= state.lastAckedFrameId) {
    return advancesApplied
      ? { _tag: "ProtocolError", message: "Duplicate ACK attempted to advance the applied cut" }
      : { _tag: "Ignored" };
  }
  if (ack.frameId >= state.nextFrameId) {
    return { _tag: "ProtocolError", message: "ACK referenced an unsent frame" };
  }
  if (
    appliedUpdateSeq < state.appliedUpdateSeq ||
    ack.durableThroughSeq < state.appliedThroughSeq
  ) {
    return { _tag: "ProtocolError", message: "ACK applied cut moved backwards" };
  }
  if (
    appliedUpdateSeq > state.completedUpdateSeq ||
    ack.durableThroughSeq > state.completedThroughSeq
  ) {
    return { _tag: "ProtocolError", message: "ACK applied an incomplete update" };
  }
  return {
    _tag: "Applied",
    state: {
      ...state,
      lastAckedFrameId: ack.frameId,
      inFlight: state.inFlight.filter((frame) => frame.frameId > ack.frameId),
      appliedUpdateSeq,
      appliedThroughSeq: SequenceNumber.make(ack.durableThroughSeq),
    },
  };
};

/** Queue one client request, or report that the client exceeded its request bound. */
export const enqueueTranscriptRequest = (
  state: EDATranscriptDeliveryState,
  request: EDATranscriptRequestFrame,
  limits: EDATranscriptDeliveryLimits,
): EDATranscriptDeliveryState | undefined =>
  state.requests.length >= limits.maxPendingRequests
    ? undefined
    : { ...state, requests: [...state.requests, request] };
