import * as Schema from "effect/Schema";

import {
  DurableEventEnvelope,
  EphemeralEventEnvelope,
  EventType,
  effectDurableAgentNamespace,
  schemaV1,
} from "./envelope";

/** Build a built-in durable event schema from its fixed event type and payload schema. */
export const durableEventSchema = <const Type extends EventType, Payload extends Schema.Top>(
  type: Type,
  payload: Payload,
) =>
  Schema.Struct({
    ...DurableEventEnvelope.fields,
    namespace: Schema.Literal(effectDurableAgentNamespace),
    type: Schema.Literal(type),
    schemaVersion: Schema.Literal(schemaV1),
    payload,
  });

/** Build a built-in ephemeral event schema from its fixed event type and payload schema. */
export const ephemeralEventSchema = <const Type extends EventType, Payload extends Schema.Top>(
  type: Type,
  payload: Payload,
) =>
  Schema.Struct({
    ...EphemeralEventEnvelope.fields,
    namespace: Schema.Literal(effectDurableAgentNamespace),
    type: Schema.Literal(type),
    schemaVersion: Schema.Literal(schemaV1),
    payload,
  });
