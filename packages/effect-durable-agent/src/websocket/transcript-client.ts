import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import { SequenceNumber } from "../types/core";
import {
  TranscriptOrder,
  type TranscriptLogIncarnation,
  type TranscriptOrderKey,
  type TranscriptRecordView,
  type TranscriptVersion,
} from "../transcript/records";
import {
  TranscriptUpdate,
  transcriptCoverageOf,
  type TranscriptCoverage,
} from "../transcript/subscription";
import type {
  TranscriptActiveContent,
  TranscriptReadiness,
  TranscriptWindow,
} from "../transcript/window";
import type { EDAWebSocketWireAckFrame } from "./protocol";
import {
  EDATranscriptChunkFrame,
  EDATranscriptUpdateFrame,
  negotiateEDATranscript,
  type EDATranscriptNegotiationResult,
  type EDATranscriptPageFrame,
  type EDATranscriptRetainFrame,
  type EDATranscriptSubscribeFrame,
} from "./transcript";

/**
 * The window a client holds: always exactly one complete cut. Top-level records are in
 * transcript order; dependencies are keyed by id and ordered as the server listed them.
 */
export interface EDATranscriptClientWindow {
  readonly incarnation: TranscriptLogIncarnation;
  readonly version: TranscriptVersion;
  readonly throughSeq: SequenceNumber;
  readonly coverage: TranscriptCoverage | undefined;
  readonly hasOlder: boolean;
  readonly hasNewer: boolean;
  readonly readiness: TranscriptReadiness;
  readonly records: ReadonlyArray<TranscriptRecordView>;
  readonly dependencies: ReadonlyArray<TranscriptRecordView>;
  readonly activeContent: ReadonlyArray<TranscriptActiveContent>;
}

/**
 * Reference client state for one transcript socket, independent of any UI framework.
 *
 * Hosts feed every server message to `applyEDATranscriptServerMessage`, send the ACK it returns,
 * and render `window`. Chunked updates are assembled here and applied only when complete, so the
 * rendered window never shows a partial update.
 */
export interface EDATranscriptClientState {
  readonly window: EDATranscriptClientWindow | undefined;
  /** Highest frame id received on the current socket. */
  readonly lastFrameId: number;
  /** The last update applied on the current socket. */
  readonly appliedUpdateSeq: number;
  readonly assembling:
    | { readonly updateSeq: number; readonly count: number; readonly data: ReadonlyArray<string> }
    | undefined;
}

/** A client that has rendered nothing yet. */
export const emptyEDATranscriptClientState: EDATranscriptClientState = {
  window: undefined,
  lastFrameId: 0,
  appliedUpdateSeq: 0,
  assembling: undefined,
};

/** Seed a client from the window its page was server-rendered with. */
export const edaTranscriptClientFromWindow = (
  window: TranscriptWindow,
): EDATranscriptClientState => ({
  ...emptyEDATranscriptClientState,
  window: {
    incarnation: window.incarnation,
    version: window.version,
    throughSeq: window.throughSeq,
    coverage: transcriptCoverageOf(window),
    hasOlder: window.hasOlder,
    hasNewer: window.hasNewer,
    readiness: window.readiness,
    records: window.records,
    dependencies: window.dependencies,
    activeContent: window.activeContent ?? [],
  },
});

/**
 * Prepare for a new socket: per-socket counters restart and any partial transfer is discarded.
 * The applied window is kept; the next `subscribe` asks for changes since it.
 */
export const edaTranscriptClientReconnected = (
  state: EDATranscriptClientState,
): EDATranscriptClientState => ({ ...emptyEDATranscriptClientState, window: state.window });

/** What one server message did. */
export type EDATranscriptClientOutcome =
  | { readonly _tag: "Hello"; readonly negotiation: EDATranscriptNegotiationResult }
  | { readonly _tag: "Applied"; readonly updateSeq: number; readonly requestId?: string }
  | { readonly _tag: "Partial" }
  | { readonly _tag: "Ignored" }
  /** The stream is unusable (a gap, a bad frame, a protocol close): reconnect and resubscribe. */
  | { readonly _tag: "Resubscribe"; readonly reason: string };

export interface EDATranscriptClientStep {
  readonly state: EDATranscriptClientState;
  readonly outcome: EDATranscriptClientOutcome;
  /** Send this ACK when present: a transport receipt plus the applied cut. */
  readonly ack?: EDAWebSocketWireAckFrame;
}

const decodeUpdateFrame = Schema.decodeUnknownExit(EDATranscriptUpdateFrame);
const decodeChunkFrame = Schema.decodeUnknownExit(EDATranscriptChunkFrame);
const decodeUpdate = Schema.decodeUnknownExit(TranscriptUpdate);

const peek = (value: unknown): { readonly tag?: string; readonly frameId?: number } => {
  if (typeof value !== "object" || value === null) return {};
  const tag = "_tag" in value && typeof value._tag === "string" ? value._tag : undefined;
  const frameId =
    "frameId" in value && typeof value.frameId === "number" && Number.isInteger(value.frameId)
      ? value.frameId
      : undefined;
  return {
    ...(tag === undefined ? {} : { tag }),
    ...(frameId === undefined ? {} : { frameId }),
  };
};

/** Apply one server text message. Frames must arrive in frame-id order without gaps. */
export const applyEDATranscriptServerMessage = (
  state: EDATranscriptClientState,
  message: string,
): EDATranscriptClientStep => {
  let json: unknown;
  try {
    json = JSON.parse(message);
  } catch {
    return resubscribe(state, "malformed server message");
  }
  const { tag, frameId } = peek(json);
  switch (tag) {
    case "hello":
      return { state, outcome: { _tag: "Hello", negotiation: negotiateEDATranscript(json) } };
    case "pong":
      return { state, outcome: { _tag: "Ignored" } };
    case "lagged":
    case "error":
      return resubscribe(state, `server sent ${tag}`);
  }
  if (frameId === undefined) return { state, outcome: { _tag: "Ignored" } };
  if (frameId !== state.lastFrameId + 1) return resubscribe(state, "frame gap");
  const received: EDATranscriptClientState = { ...state, lastFrameId: frameId };

  if (tag === "transcript") {
    const decoded = decodeUpdateFrame(json);
    if (Exit.isFailure(decoded)) return resubscribe(state, "undecodable update");
    if (received.assembling !== undefined)
      return resubscribe(state, "update interrupted a transfer");
    return applyUpdate(received, decoded.value.update);
  }
  if (tag === "transcript-chunk") {
    const decoded = decodeChunkFrame(json);
    if (Exit.isFailure(decoded)) return resubscribe(state, "undecodable chunk");
    const chunk = decoded.value;
    const assembling = received.assembling ?? {
      updateSeq: chunk.updateSeq,
      count: chunk.count,
      data: [],
    };
    if (
      assembling.updateSeq !== chunk.updateSeq ||
      assembling.count !== chunk.count ||
      assembling.data.length !== chunk.index
    ) {
      return resubscribe(state, "interrupted chunked transfer");
    }
    const data = [...assembling.data, chunk.data];
    if (data.length < chunk.count) {
      const next = { ...received, assembling: { ...assembling, data } };
      return { state: next, outcome: { _tag: "Partial" }, ack: ackOf(next) };
    }
    let updateJson: unknown;
    try {
      updateJson = JSON.parse(data.join(""));
    } catch {
      return resubscribe(state, "malformed chunked update");
    }
    const update = decodeUpdate(updateJson);
    if (Exit.isFailure(update) || update.value.updateSeq !== chunk.updateSeq) {
      return resubscribe(state, "undecodable chunked update");
    }
    return applyUpdate({ ...received, assembling: undefined }, update.value);
  }
  // A frame kind from a newer server: acknowledge its receipt and ignore it.
  return { state: received, outcome: { _tag: "Ignored" }, ack: ackOf(received) };
};

const resubscribe = (state: EDATranscriptClientState, reason: string): EDATranscriptClientStep => ({
  state: { ...state, assembling: undefined },
  outcome: { _tag: "Resubscribe", reason },
});

const ackOf = (state: EDATranscriptClientState): EDAWebSocketWireAckFrame => ({
  _tag: "ack",
  frameId: state.lastFrameId,
  durableThroughSeq: state.appliedUpdateSeq === 0 ? 0 : (state.window?.throughSeq ?? 0),
  appliedUpdateSeq: state.appliedUpdateSeq,
});

const orderKeyOf = (record: TranscriptRecordView): TranscriptOrderKey | undefined =>
  "position" in record && record.position !== undefined
    ? { ...record.position, recordId: record.id }
    : undefined;

const covered = (
  coverage: TranscriptCoverage | undefined,
  record: TranscriptRecordView,
): boolean => {
  const key = orderKeyOf(record);
  return (
    coverage !== undefined &&
    key !== undefined &&
    TranscriptOrder(coverage.from, key) <= 0 &&
    TranscriptOrder(key, coverage.to) <= 0
  );
};

const applyUpdate = (
  state: EDATranscriptClientState,
  update: TranscriptUpdate,
): EDATranscriptClientStep => {
  if (update.updateSeq !== state.appliedUpdateSeq + 1) return resubscribe(state, "update gap");
  const previous = state.window;
  if (
    update.mode === "patch" &&
    (previous === undefined ||
      previous.incarnation !== update.incarnation ||
      previous.version !== update.version)
  ) {
    return resubscribe(state, "patch without its base");
  }
  const base = update.mode === "patch" ? previous : undefined;

  const topLevel = new Map((base?.records ?? []).map((record) => [record.id, record]));
  for (const record of update.records) topLevel.set(record.id, record);
  const records = [...topLevel.values()]
    .filter((record) => covered(update.coverage, record))
    .sort((left, right) => {
      const leftKey = orderKeyOf(left);
      const rightKey = orderKeyOf(right);
      return leftKey === undefined || rightKey === undefined
        ? 0
        : TranscriptOrder(leftKey, rightKey);
    });

  const dependencyMap = new Map((base?.dependencies ?? []).map((record) => [record.id, record]));
  for (const record of update.dependencies) dependencyMap.set(record.id, record);
  const dependencies = update.dependencyIds.flatMap((id) => {
    const record = dependencyMap.get(id);
    return record === undefined ? [] : [record];
  });
  if (dependencies.length !== update.dependencyIds.length) {
    return resubscribe(state, "patch referenced a dependency the client does not hold");
  }

  const window: EDATranscriptClientWindow = {
    incarnation: update.incarnation,
    version: update.version,
    throughSeq: update.throughSeq,
    coverage: update.coverage,
    hasOlder: update.hasOlder,
    hasNewer: update.hasNewer,
    readiness: update.readiness,
    records,
    dependencies,
    activeContent:
      update.readiness._tag !== "Ready" ? [] : (update.activeContent ?? base?.activeContent ?? []),
  };
  const next: EDATranscriptClientState = {
    ...state,
    window,
    appliedUpdateSeq: update.updateSeq,
    assembling: undefined,
  };
  return {
    state: next,
    outcome: {
      _tag: "Applied",
      updateSeq: update.updateSeq,
      ...(update.requestId === undefined ? {} : { requestId: update.requestId }),
    },
    ack: ackOf(next),
  };
};

/**
 * The request a client sends first on every socket: changes since the window it holds over the
 * span it holds, or the latest `count` records when it holds none.
 */
export const edaTranscriptSubscribeRequest = (
  state: EDATranscriptClientState,
  options?: { readonly count?: number; readonly requestId?: string },
): EDATranscriptSubscribeFrame => {
  const window = state.window;
  const extra = {
    ...(options?.count === undefined ? {} : { count: options.count }),
    ...(options?.requestId === undefined ? {} : { requestId: options.requestId }),
  };
  return window?.coverage === undefined
    ? { _tag: "subscribe", ...extra }
    : {
        _tag: "subscribe",
        range: window.coverage,
        tail: !window.hasNewer,
        since: window.throughSeq,
        ...extra,
      };
};

/** Page from the held coverage's edge, or `undefined` when there is nothing to page from. */
export const edaTranscriptPageRequest = (
  state: EDATranscriptClientState,
  direction: "before" | "after",
  count: number,
  requestId?: string,
): EDATranscriptPageFrame | undefined => {
  const coverage = state.window?.coverage;
  if (coverage === undefined) return undefined;
  return {
    _tag: "page",
    direction,
    cursor: direction === "before" ? coverage.from : coverage.to,
    count,
    ...(requestId === undefined ? {} : { requestId }),
  };
};

/**
 * Keep only the top-level records from `fromIndex` to `toIndex` (inclusive) of the held window;
 * the server confirms with an update whose coverage is the narrowed span.
 */
export const edaTranscriptRetainRequest = (
  state: EDATranscriptClientState,
  fromIndex: number,
  toIndex: number,
  requestId?: string,
): EDATranscriptRetainFrame | undefined => {
  const window = state.window;
  const from = window?.records[fromIndex];
  const to = window?.records[toIndex];
  const fromKey = from === undefined ? undefined : orderKeyOf(from);
  const toKey = to === undefined ? undefined : orderKeyOf(to);
  if (window === undefined || fromKey === undefined || toKey === undefined) return undefined;
  const scope = { incarnation: window.incarnation, version: window.version };
  return {
    _tag: "retain",
    from: { ...scope, ...fromKey, anchorSeq: SequenceNumber.make(fromKey.anchorSeq) },
    to: { ...scope, ...toKey, anchorSeq: SequenceNumber.make(toKey.anchorSeq) },
    ...(requestId === undefined ? {} : { requestId }),
  };
};
