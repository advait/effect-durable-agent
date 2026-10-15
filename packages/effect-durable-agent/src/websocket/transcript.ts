import * as Schema from "effect/Schema";

import { SequenceNumber } from "../types/core";
import { TranscriptCursor } from "../transcript/records";
import {
  TranscriptCoverage,
  TranscriptRequestId,
  TranscriptUpdate,
} from "../transcript/subscription";
import {
  EDAWebSocketWireAckFrame,
  EDAWebSocketWireErrorFrame,
  EDAWebSocketWireHelloFrame,
  EDAWebSocketWireLaggedFrame,
  EDAWebSocketWirePingFrame,
  EDAWebSocketWirePongFrame,
} from "./protocol";

const PositiveInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

// ---------------------------------------------------------------------------------------------
// Negotiation
// ---------------------------------------------------------------------------------------------

/**
 * Transcript protocol major. A change of meaning that an older client would misread bumps it;
 * a client that sees another major performs one bounded reload.
 */
export const EDA_TRANSCRIPT_PROTOCOL_MAJOR = 1;

/** Additive wire revision within a major; older clients drop fields they do not know. */
export const EDA_TRANSCRIPT_WIRE_VERSION = 1;

/**
 * The small, stable envelope a client decodes before any version-specific schema, from the
 * socket hello and from the host's SSR data. Its shape never changes; everything else may.
 */
export const EDATranscriptNegotiation = Schema.Struct({
  protocolMajor: PositiveInt,
  wireVersion: PositiveInt,
  /** Optional features this server speaks; clients ignore names they do not know. */
  capabilities: Schema.Array(Schema.String),
});
export type EDATranscriptNegotiation = typeof EDATranscriptNegotiation.Type;

/** What this EDA release speaks. Hosts embed it in SSR data next to the transcript window. */
export const edaTranscriptNegotiation: EDATranscriptNegotiation = {
  protocolMajor: EDA_TRANSCRIPT_PROTOCOL_MAJOR,
  wireVersion: EDA_TRANSCRIPT_WIRE_VERSION,
  capabilities: ["window", "page", "retain", "chunked-updates", "active-content"],
};

/** Any value carrying the envelope under `negotiation`: a hello frame or SSR data. */
const NegotiationCarrier = Schema.Struct({ negotiation: EDATranscriptNegotiation });

/** How a client should treat a server's negotiation envelope. */
export type EDATranscriptNegotiationResult =
  | { readonly _tag: "Compatible"; readonly remote: EDATranscriptNegotiation }
  /** The server speaks another major: reload once, preserving the viewport and draft. */
  | { readonly _tag: "MajorMismatch"; readonly remote: EDATranscriptNegotiation }
  /** No envelope could be decoded: an incompatible or non-transcript peer; also reload. */
  | { readonly _tag: "Undecodable" };

const decodeNegotiationCarrier = Schema.decodeUnknownOption(NegotiationCarrier);

/**
 * Decode only the negotiation envelope from an untrusted hello frame or SSR payload and compare
 * it with this client's major. Run before decoding anything version-specific.
 */
export const negotiateEDATranscript = (
  carrier: unknown,
  localMajor: number = EDA_TRANSCRIPT_PROTOCOL_MAJOR,
): EDATranscriptNegotiationResult => {
  const decoded = decodeNegotiationCarrier(carrier);
  if (decoded._tag === "None") return { _tag: "Undecodable" };
  const remote = decoded.value.negotiation;
  return remote.protocolMajor === localMajor
    ? { _tag: "Compatible", remote }
    : { _tag: "MajorMismatch", remote };
};

// ---------------------------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------------------------

/** WebSocket close code: the server lost volatile transfer state; reconnect and resubscribe. */
export const EDA_WS_CLOSE_TRANSCRIPT_RESUBSCRIBE = 4009;

/** First frame on a transcript socket: the event-socket hello plus the negotiation envelope. */
export const EDATranscriptHelloFrame = Schema.Struct({
  ...EDAWebSocketWireHelloFrame.fields,
  negotiation: EDATranscriptNegotiation,
});
export type EDATranscriptHelloFrame = typeof EDATranscriptHelloFrame.Type;

/** A transcript update that fits one frame. */
export const EDATranscriptUpdateFrame = Schema.Struct({
  _tag: Schema.Literal("transcript"),
  frameId: PositiveInt,
  update: TranscriptUpdate,
});
export type EDATranscriptUpdateFrame = typeof EDATranscriptUpdateFrame.Type;

/**
 * One slice of an update too large for a frame. `data` slices concatenated in `index` order are
 * the JSON of one `TranscriptUpdate`; the client applies it only after slice `count - 1` (the
 * completion marker) and discards a transfer that is interrupted.
 */
export const EDATranscriptChunkFrame = Schema.Struct({
  _tag: Schema.Literal("transcript-chunk"),
  frameId: PositiveInt,
  updateSeq: PositiveInt,
  index: NonNegativeInt,
  count: PositiveInt,
  data: Schema.String,
}).check(Schema.makeFilter((chunk) => chunk.index < chunk.count || "chunk index out of range"));
export type EDATranscriptChunkFrame = typeof EDATranscriptChunkFrame.Type;

/** Every server frame a transcript socket may carry. */
export const EDATranscriptServerFrame = Schema.Union([
  EDATranscriptHelloFrame,
  EDATranscriptUpdateFrame,
  EDATranscriptChunkFrame,
  EDAWebSocketWirePongFrame,
  EDAWebSocketWireLaggedFrame,
  EDAWebSocketWireErrorFrame,
]);
export type EDATranscriptServerFrame = typeof EDATranscriptServerFrame.Type;

/**
 * Start or restart the subscription. After SSR, `range` is what the page rendered, `tail` its
 * `!hasNewer`, and `since` its `throughSeq`; a reconnecting client sends what it last applied.
 * Without `range` the server replies with the latest `count` records.
 */
export const EDATranscriptSubscribeFrame = Schema.Struct({
  _tag: Schema.Literal("subscribe"),
  requestId: Schema.optionalKey(TranscriptRequestId),
  range: Schema.optionalKey(TranscriptCoverage),
  tail: Schema.optionalKey(Schema.Boolean),
  since: Schema.optionalKey(SequenceNumber),
  count: Schema.optionalKey(PositiveInt),
});
export type EDATranscriptSubscribeFrame = typeof EDATranscriptSubscribeFrame.Type;

/** Extend the coverage from its edge: `cursor` is the coverage's `from` (before) or `to` (after). */
export const EDATranscriptPageFrame = Schema.Struct({
  _tag: Schema.Literal("page"),
  requestId: Schema.optionalKey(TranscriptRequestId),
  direction: Schema.Literals(["before", "after"]),
  cursor: TranscriptCursor,
  count: PositiveInt,
});
export type EDATranscriptPageFrame = typeof EDATranscriptPageFrame.Type;

/** Narrow the coverage to a span inside it, so the server stops sending changes outside it. */
export const EDATranscriptRetainFrame = Schema.Struct({
  _tag: Schema.Literal("retain"),
  requestId: Schema.optionalKey(TranscriptRequestId),
  from: TranscriptCursor,
  to: TranscriptCursor,
});
export type EDATranscriptRetainFrame = typeof EDATranscriptRetainFrame.Type;

/** A client request; each is answered by exactly one update, in arrival order. */
export const EDATranscriptRequestFrame = Schema.Union([
  EDATranscriptSubscribeFrame,
  EDATranscriptPageFrame,
  EDATranscriptRetainFrame,
]);
export type EDATranscriptRequestFrame = typeof EDATranscriptRequestFrame.Type;

/** Every client frame a transcript socket accepts. */
export const EDATranscriptClientFrame = Schema.Union([
  EDAWebSocketWireAckFrame,
  EDAWebSocketWirePingFrame,
  EDATranscriptSubscribeFrame,
  EDATranscriptPageFrame,
  EDATranscriptRetainFrame,
]);
export type EDATranscriptClientFrame = typeof EDATranscriptClientFrame.Type;

// ---------------------------------------------------------------------------------------------
// Encoding and chunking
// ---------------------------------------------------------------------------------------------

const encodeUpdate = Schema.encodeSync(TranscriptUpdate);
const encodeServerFrame = Schema.encodeSync(EDATranscriptServerFrame);

/** Serialize one update once; frames embed this text, chunks slice it. */
export const encodeEDATranscriptUpdate = (update: TranscriptUpdate): string =>
  JSON.stringify(encodeUpdate(update));

/** Serialize a non-update server frame. */
export const encodeEDATranscriptServerFrame = (frame: EDATranscriptServerFrame): string =>
  JSON.stringify(encodeServerFrame(frame));

/** The single-frame form of an encoded update. */
export const edaTranscriptUpdateFrameText = (frameId: number, encodedUpdate: string): string =>
  `{"_tag":"transcript","frameId":${frameId},"update":${encodedUpdate}}`;

/** One chunk frame of an encoded update. */
export const edaTranscriptChunkFrameText = (input: {
  readonly frameId: number;
  readonly updateSeq: number;
  readonly index: number;
  readonly count: number;
  readonly data: string;
}): string =>
  `{"_tag":"transcript-chunk","frameId":${input.frameId},"updateSeq":${input.updateSeq},` +
  `"index":${input.index},"count":${input.count},"data":${JSON.stringify(input.data)}}`;

const textEncoder = new TextEncoder();

/** UTF-8 byte length of a string. */
export const utf8ByteLength = (text: string): number => textEncoder.encode(text).byteLength;

/** Bytes a chunk frame spends besides its data, with generous room for large numbers. */
const chunkFrameOverheadBytes = 160;

/** Control characters JSON writes as a two-byte escape (`\b`, `\t`, `\n`, `\f`, `\r`). */
const shortEscapes: ReadonlySet<number> = new Set([0x08, 0x09, 0x0a, 0x0c, 0x0d]);

/** UTF-8 bytes one UTF-16 code unit costs inside a JSON string literal. */
const jsonStringUnitBytes = (code: number): number => {
  if (code === 0x22 || code === 0x5c) return 2;
  if (code < 0x20) return shortEscapes.has(code) ? 2 : 6;
  if (code < 0x80) return 1;
  if (code < 0x800) return 2;
  // A lone surrogate half is written as a `\uXXXX` escape; pairs are costed by the caller.
  if (code >= 0xd800 && code <= 0xdfff) return 6;
  return 3;
};

/**
 * Split an encoded update into chunk data that each fit a frame of `maxFrameBytes`, never
 * splitting a surrogate pair.
 */
export const splitEDATranscriptUpdate = (
  encodedUpdate: string,
  maxFrameBytes: number,
): ReadonlyArray<string> => {
  const budget = Math.max(16, maxFrameBytes - chunkFrameOverheadBytes);
  const chunks: string[] = [];
  let start = 0;
  let bytes = 0;
  for (let index = 0; index < encodedUpdate.length; index += 1) {
    const code = encodedUpdate.charCodeAt(index);
    const pair =
      code >= 0xd800 && code <= 0xdbff && index + 1 < encodedUpdate.length
        ? encodedUpdate.charCodeAt(index + 1)
        : undefined;
    const isPair = pair !== undefined && pair >= 0xdc00 && pair <= 0xdfff;
    const cost = isPair ? 4 : jsonStringUnitBytes(code);
    if (bytes + cost > budget && index > start) {
      chunks.push(encodedUpdate.slice(start, index));
      start = index;
      bytes = 0;
    }
    bytes += cost;
    if (isPair) index += 1;
  }
  if (start < encodedUpdate.length || chunks.length === 0) chunks.push(encodedUpdate.slice(start));
  return chunks;
};
