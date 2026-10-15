import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  EDATranscriptDeliveryCheckpoint,
  EDAWebSocketDeliveryCheckpoint,
} from "effect-durable-agent/websocket";
import { SubscriberId } from "effect-durable-agent/websocket";
import { SessionId } from "effect-durable-agent/types/core";
import { EDATraceMetadata } from "effect-durable-agent/types/tracing";
import { EDAWebSocketProjectionId } from "./projection";

/** Versioned state required to resume strict delivery after Durable Object hibernation. */
export const EDAWebSocketAttachment = Schema.Struct({
  kind: Schema.Literal("eda-events-v2"),
  sessionId: SessionId,
  subscriberId: SubscriberId,
  trace: EDATraceMetadata,
  delivery: EDAWebSocketDeliveryCheckpoint,
  projection: Schema.optionalKey(
    Schema.Struct({
      id: EDAWebSocketProjectionId,
      state: Schema.Unknown,
    }),
  ),
});
export type EDAWebSocketAttachment = typeof EDAWebSocketAttachment.Type;

/**
 * State required to resume a transcript-window socket after hibernation: the transport receipts,
 * the applied cut, the subscription base and unanswered requests. Staged chunk data is never
 * persisted; a checkpoint that names a transfer cannot resume it.
 */
export const EDATranscriptWebSocketAttachment = Schema.Struct({
  kind: Schema.Literal("eda-transcript-v1"),
  sessionId: SessionId,
  subscriberId: SubscriberId,
  trace: EDATraceMetadata,
  delivery: EDATranscriptDeliveryCheckpoint,
});
export type EDATranscriptWebSocketAttachment = typeof EDATranscriptWebSocketAttachment.Type;

/** Either socket kind an EDA Durable Object accepts. */
const EDAAnyWebSocketAttachment = Schema.Union([
  EDAWebSocketAttachment,
  EDATranscriptWebSocketAttachment,
]);

export type DecodedWebSocketAttachment =
  | { readonly _tag: "Missing" }
  | { readonly _tag: "Malformed" }
  | {
      readonly _tag: "Decoded";
      readonly attachment: EDAWebSocketAttachment | EDATranscriptWebSocketAttachment;
    };

/** Decode and classify an attachment read from Cloudflare's structured-clone boundary. */
export const decodeWebSocketAttachment = async (
  input: unknown,
): Promise<DecodedWebSocketAttachment> => {
  if (input === null || input === undefined) return { _tag: "Missing" };
  try {
    return {
      _tag: "Decoded",
      attachment: await Effect.runPromise(
        Schema.decodeUnknownEffect(EDAAnyWebSocketAttachment)(input),
      ),
    };
  } catch {
    return { _tag: "Malformed" };
  }
};

/** Encode before serialization so schema transformations cannot leak into attachments. */
export const encodeWebSocketAttachment = (attachment: EDAWebSocketAttachment): unknown =>
  Schema.encodeSync(EDAWebSocketAttachment)(attachment);

/** Encode a transcript socket attachment for Cloudflare's structured-clone boundary. */
export const encodeTranscriptWebSocketAttachment = (
  attachment: EDATranscriptWebSocketAttachment,
): unknown => Schema.encodeSync(EDATranscriptWebSocketAttachment)(attachment);
