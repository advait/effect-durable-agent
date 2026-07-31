import * as Schema from "effect/Schema";

import {
  DurableEventEnvelope,
  EDADurableEvent,
  effectDurableAgentNamespace,
} from "../types/events";

/** Encode one durable event into a structured-clone-safe Durable Object RPC value. */
export const encodeEdaRpcDurableEvent = (event: DurableEventEnvelope): DurableEventEnvelope => {
  const encoded =
    event.namespace === effectDurableAgentNamespace
      ? Schema.encodeSync(EDADurableEvent)(Schema.decodeUnknownSync(EDADurableEvent)(event))
      : Schema.encodeSync(DurableEventEnvelope)(event);
  return Schema.decodeUnknownSync(DurableEventEnvelope)(encoded);
};
