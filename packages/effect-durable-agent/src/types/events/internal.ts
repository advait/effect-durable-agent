import * as Exit from "effect/Exit";
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

/** Whether a decode dropped any input key. Values returned by reference were not reshaped. */
const droppedKeys = (input: unknown, decoded: unknown): boolean => {
  if (input === decoded) return false;
  if (Array.isArray(input) && Array.isArray(decoded)) {
    return input.some((item, index) => droppedKeys(item, decoded[index]));
  }
  if (!isPlainRecord(input) || !isPlainRecord(decoded)) return false;
  return Object.keys(input).some(
    (key) => !Object.hasOwn(decoded, key) || droppedKeys(input[key], decoded[key]),
  );
};

/** The schema's encoding of this subtree when it has the input's container shape. */
const sameShape = (input: unknown, encoded: unknown): unknown =>
  (Array.isArray(input) && Array.isArray(encoded)) ||
  (isPlainRecord(input) && isPlainRecord(encoded))
    ? encoded
    : undefined;

/**
 * Re-attach input properties the schema does not model. A key is unknown when the schema's own
 * encoding lacks it, so keys consumed by renaming transformations stay consumed. Where no encoding
 * of the same shape is available, such as a union member that encodes to a string, a key is
 * unknown when the decoded value lacks it. Only subtrees that dropped a key are copied.
 */
const retainUnknown = <A>(input: unknown, encoded: unknown, decoded: A): A => {
  if (!droppedKeys(input, decoded)) return decoded;
  const known = sameShape(input, encoded);
  if (Array.isArray(input) && Array.isArray(decoded) && input.length === decoded.length) {
    const items: ReadonlyArray<unknown> = Array.isArray(known) ? known : [];
    return Object.assign(
      [],
      decoded,
      decoded.map((item, index) => retainUnknown(input[index], items[index], item)),
    );
  }
  if (!isPlainRecord(input) || !isPlainRecord(decoded)) return decoded;
  const modelled: Readonly<Record<string, unknown>> = isPlainRecord(known) ? known : decoded;
  const unknown: Record<string, unknown> = {};
  const retained: Record<string, unknown> = {};
  for (const key of Object.keys(input)) {
    if (!Object.hasOwn(modelled, key)) setOwn(unknown, key, input[key]);
    else if (Object.hasOwn(decoded, key))
      setOwn(retained, key, retainUnknown(input[key], modelled[key], decoded[key]));
  }
  return Object.assign(unknown, decoded, retained);
};

/**
 * Effect 4 removed `onExcessProperty: "preserve"`. Durable facts written by other framework or
 * application versions must still re-encode at persistence and protocol boundaries without losing
 * fields, so decode with the default policy and re-attach what the schema does not model. The
 * extra encode runs only when the decode dropped a key; decode-only schemas use decoded keys.
 */
export const decodeRetainingExcess = <S extends Schema.Codec<unknown, unknown>>(schema: S) => {
  const decode = Schema.decodeUnknownSync(schema);
  const encode = Schema.encodeUnknownExit(schema);
  return (input: unknown): S["Type"] => {
    const decoded = decode(input);
    if (!droppedKeys(input, decoded)) return decoded;
    const encoded = encode(decoded);
    return retainUnknown(input, Exit.isSuccess(encoded) ? encoded.value : undefined, decoded);
  };
};
