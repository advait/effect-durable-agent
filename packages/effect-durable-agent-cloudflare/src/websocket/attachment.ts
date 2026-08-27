import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { EDAWebSocketDeliveryCheckpoint } from "effect-durable-agent/websocket";
import { SubscriberId } from "effect-durable-agent/websocket";
import { SessionId } from "effect-durable-agent/types/core";
import { EDATraceMetadata } from "effect-durable-agent/types/tracing";

/** Versioned state required to resume strict delivery after Durable Object hibernation. */
export const EDAWebSocketAttachment = Schema.Struct({
  kind: Schema.Literal("eda-events-v2"),
  sessionId: SessionId,
  subscriberId: SubscriberId,
  trace: EDATraceMetadata,
  delivery: EDAWebSocketDeliveryCheckpoint,
});
export type EDAWebSocketAttachment = typeof EDAWebSocketAttachment.Type;

export type DecodedWebSocketAttachment =
  | { readonly _tag: "Missing" }
  | { readonly _tag: "Malformed" }
  | { readonly _tag: "Decoded"; readonly attachment: EDAWebSocketAttachment };

/** Decode and classify an attachment read from Cloudflare's structured-clone boundary. */
export const decodeWebSocketAttachment = async (
  input: unknown,
): Promise<DecodedWebSocketAttachment> => {
  if (input === null || input === undefined) return { _tag: "Missing" };
  try {
    return {
      _tag: "Decoded",
      attachment: await Effect.runPromise(
        Schema.decodeUnknownEffect(EDAWebSocketAttachment)(input),
      ),
    };
  } catch {
    return { _tag: "Malformed" };
  }
};

/** Encode before serialization so schema transformations cannot leak into attachments. */
export const encodeWebSocketAttachment = (attachment: EDAWebSocketAttachment): unknown =>
  Schema.encodeSync(EDAWebSocketAttachment)(attachment);
