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

const isPlainRecord = (value: unknown): value is Readonly<Record<string, unknown>> => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

/**
 * Re-attach input properties that a decode dropped. Effect 4 removed
 * `onExcessProperty: "preserve"`; durable facts written by other framework or application
 * versions must still re-encode at persistence and protocol boundaries without losing fields.
 * Decoded values win for known properties; class instances and other non-plain values are kept.
 */
export const retainExcessProperties = <A>(input: unknown, decoded: A): A => {
  if (Array.isArray(input) && Array.isArray(decoded) && input.length === decoded.length) {
    return Object.assign(
      [],
      decoded,
      decoded.map((item, index) => retainExcessProperties(input[index], item)),
    );
  }
  if (!isPlainRecord(input) || !isPlainRecord(decoded)) return decoded;
  const merged: Record<string, unknown> = {};
  for (const key of Object.keys(input)) {
    merged[key] = Object.hasOwn(decoded, key)
      ? retainExcessProperties(input[key], decoded[key])
      : input[key];
  }
  return Object.assign({}, decoded, merged);
};

/** Decode with Effect's default excess-property handling, then retain unknown input fields. */
export const decodeRetainingExcess =
  <S extends Schema.Decoder<unknown>>(schema: S) =>
  (input: unknown): S["Type"] =>
    retainExcessProperties(input, Schema.decodeUnknownSync(schema)(input));
