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

/** Own data property, including a literal `__proto__` key from JSON, without touching prototypes. */
const setOwn = (target: Record<string, unknown>, key: string, value: unknown): void => {
  Object.defineProperty(target, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
};

/** Whether a decode dropped any input key, so the exact encoded comparison is needed. */
const droppedKeys = (input: unknown, decoded: unknown): boolean => {
  if (Array.isArray(input) && Array.isArray(decoded)) {
    return input.some((item, index) => droppedKeys(item, decoded[index]));
  }
  if (!isPlainRecord(input) || !isPlainRecord(decoded)) return false;
  return Object.keys(input).some(
    (key) => !Object.hasOwn(decoded, key) || droppedKeys(input[key], decoded[key]),
  );
};

/**
 * Re-attach input properties the schema does not model. A key is unknown only when the
 * schema's own encoding lacks it, so keys consumed by renaming transformations stay consumed.
 * Decoded values win for known properties; class instances and other non-plain values are kept.
 */
const retainUnknown = <A>(input: unknown, encoded: unknown, decoded: A): A => {
  if (
    Array.isArray(input) &&
    Array.isArray(encoded) &&
    Array.isArray(decoded) &&
    input.length === decoded.length
  ) {
    return Object.assign(
      [],
      decoded,
      decoded.map((item, index) => retainUnknown(input[index], encoded[index], item)),
    );
  }
  if (!isPlainRecord(input) || !isPlainRecord(encoded) || !isPlainRecord(decoded)) return decoded;
  const unknown: Record<string, unknown> = {};
  const known: Record<string, unknown> = {};
  for (const key of Object.keys(input)) {
    if (!Object.hasOwn(encoded, key)) setOwn(unknown, key, input[key]);
    else if (Object.hasOwn(decoded, key))
      setOwn(known, key, retainUnknown(input[key], encoded[key], decoded[key]));
  }
  return Object.assign(unknown, decoded, known);
};

/**
 * Effect 4 removed `onExcessProperty: "preserve"`. Durable facts written by other framework or
 * application versions must still re-encode at persistence and protocol boundaries without losing
 * fields, so decode with the default policy and re-attach what the schema does not model. The
 * extra encode runs only when the decode dropped a key.
 */
export const decodeRetainingExcess = <S extends Schema.Codec<unknown, unknown>>(schema: S) => {
  const decode = Schema.decodeUnknownSync(schema);
  const encode = Schema.encodeUnknownSync(schema);
  return (input: unknown): S["Type"] => {
    const decoded = decode(input);
    return droppedKeys(input, decoded) ? retainUnknown(input, encode(decoded), decoded) : decoded;
  };
};
