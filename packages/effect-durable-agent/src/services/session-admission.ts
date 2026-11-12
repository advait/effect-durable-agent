import * as Context from "effect/Context";
import * as Schema from "effect/Schema";
import type * as SchemaAST from "effect/SchemaAST";

import { MessageId } from "../types/core";
import {
  type DurableEventEnvelope,
  effectDurableAgentNamespace,
  maxAppEventBytes,
} from "../types/events/envelope";
import { decodeDurableEventSync } from "../types/events/durable";
import { decodeRetainingExcess } from "../types/events/internal";

/**
 * Admission rules every app-written durable event passes before it reaches the store, shared by
 * `submit`, decider drafts, tool `emitDurable` and sink `stageDurable`. A refusal here is not a
 * store error, so it never makes the live runtime fatal. Internal: not an exported subpath.
 */

export class SessionCommandAdmissionConflict extends Schema.TaggedError<SessionCommandAdmissionConflict>()(
  "SessionCommandAdmissionConflict",
  {
    code: Schema.Literals([
      "queue_paused",
      "paused_queue_changed",
      "event_too_large",
      "framework_event",
      "invalid_event",
      "payload_not_json",
    ]),
    message: Schema.String,
    pausedMessageIds: Schema.Array(MessageId),
  },
) {}

/** Why an app event is refused. */
export interface AppEventRefusal {
  readonly code: "event_too_large" | "framework_event" | "invalid_event" | "payload_not_json";
  readonly message: string;
}

/**
 * Framework event types app code may still write through `submit`, decider drafts, `emitDurable`
 * and `stageDurable`: a session seed (`SessionConfigured`, `SystemMessageCommitted`) and imported
 * history. Every other `effect-durable-agent` event is the framework's alone; a forged
 * `DeciderAccepted` would make a later decision replay a result nobody decided.
 */
export const appWritableFrameworkEventTypes: ReadonlySet<string> = new Set([
  "SessionConfigured",
  "SystemMessageCommitted",
  "AssistantMessageImported",
]);

/** App event schemas are a union of structs with literal namespaces; throw for any other shape. */
export const eventSchemaNamespaces = (ast: SchemaAST.AST): ReadonlyArray<string> => {
  if (ast._tag === "Union") return ast.types.flatMap(eventSchemaNamespaces);
  if (ast._tag === "Objects") {
    const namespace = ast.propertySignatures.find((field) => field.name === "namespace")?.type;
    if (namespace?._tag === "Literal" && typeof namespace.literal === "string")
      return [namespace.literal];
  }
  throw new Error("App event schemas must be a union of structs with literal namespaces");
};

/**
 * Whether `value` survives `JSON.parse(JSON.stringify(value))` unchanged: plain objects, arrays,
 * strings, finite numbers, booleans and null. An object property whose value is `undefined`
 * passes (it reads back as absent); anything with a prototype (`Redacted`, `Option`, `Date`, a
 * class instance), `undefined` in an array, a non-finite number, a bigint, a function, a symbol
 * or a cycle does not.
 */
export const jsonNativeViolation = (value: unknown): string | undefined =>
  jsonNativeAt(value, "payload", new Set());

const jsonNativeAt = (value: unknown, path: string, seen: Set<object>): string | undefined => {
  if (value === null || typeof value === "string" || typeof value === "boolean") return undefined;
  if (typeof value === "number") {
    return Number.isFinite(value) ? undefined : `${path} is the non-finite number ${value}`;
  }
  if (typeof value !== "object") return `${path} is a ${typeof value}`;
  if (seen.has(value)) return `${path} is a cycle`;
  seen.add(value);
  try {
    if (Array.isArray(value)) {
      for (let index = 0; index < value.length; index += 1) {
        if (!(index in value) || value[index] === undefined) {
          return `${path}[${index}] is undefined`;
        }
        const violation = jsonNativeAt(value[index], `${path}[${index}]`, seen);
        if (violation !== undefined) return violation;
      }
      return undefined;
    }
    const prototype: unknown = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      const name =
        typeof prototype === "object" &&
        prototype !== null &&
        "constructor" in prototype &&
        typeof prototype.constructor === "function"
          ? prototype.constructor.name
          : "unknown";
      return `${path} is not a plain object (prototype ${name || "anonymous"})`;
    }
    if (Object.getOwnPropertySymbols(value).length > 0) return `${path} has symbol keys`;
    for (const [key, entry] of Object.entries(value)) {
      if (entry === undefined) continue;
      const violation = jsonNativeAt(entry, `${path}.${key}`, seen);
      if (violation !== undefined) return violation;
    }
    return undefined;
  } finally {
    seen.delete(value);
  }
};

const jsonEncoder = new TextEncoder();

/** UTF-8 size of a JSON-native value's JSON text. */
export const jsonBytes = (value: unknown): number => {
  const json: string | undefined = JSON.stringify(value);
  return json === undefined ? 0 : jsonEncoder.encode(json).byteLength;
};

/** The registered application event union, if the runtime has one. */
export type AppEventCodec = Schema.Codec<DurableEventEnvelope, unknown, never, never>;

/** The runtime's registered `appEvents` union; absent for a framework-only runtime. */
export const RegisteredAppEvents = Context.Reference<AppEventCodec | undefined>(
  "effect-durable-agent/RegisteredAppEvents",
  { defaultValue: () => undefined },
);

/** Checks one app-written durable event against the runtime's admission rules. */
export interface AppEventAdmission {
  readonly refuse: (event: DurableEventEnvelope) => AppEventRefusal | undefined;
}

/** Build the checker for a runtime, with its registered `appEvents` codec if any. */
export const makeAppEventAdmission = (appEvents: AppEventCodec | undefined): AppEventAdmission => {
  const namespaces = new Set(appEvents === undefined ? [] : eventSchemaNamespaces(appEvents.ast));
  const decode = appEvents === undefined ? undefined : decodeRetainingExcess(appEvents);
  const describe = (event: DurableEventEnvelope) =>
    `Event ${event.namespace}/${event.type} ${event.eventId}`;
  return {
    refuse: (event) => {
      if (
        event.namespace === effectDurableAgentNamespace &&
        !appWritableFrameworkEventTypes.has(event.type)
      ) {
        return {
          code: "framework_event",
          message: `${describe(event)} is a framework lifecycle event; app code may write only ${Array.from(appWritableFrameworkEventTypes).join(", ")}`,
        };
      }
      if (event.namespace === effectDurableAgentNamespace) {
        // The store decodes framework events with their own schema and would fail a malformed one
        // as a store error, so an allowlisted type is checked against that schema here.
        try {
          decodeDurableEventSync(event);
        } catch (cause) {
          return {
            code: "invalid_event",
            message: `${describe(event)} does not match its framework event schema: ${String(cause)}`,
          };
        }
      }
      const notJson = jsonNativeViolation(event.payload);
      if (notJson !== undefined) {
        return {
          code: "payload_not_json",
          message: `${describe(event)} payload is not JSON-native: ${notJson}. Encode it before writing; the store keeps JSON only.`,
        };
      }
      const bytes = jsonBytes(event.payload);
      if (bytes > maxAppEventBytes) {
        return {
          code: "event_too_large",
          message: `${describe(event)} payload is ${bytes} bytes; the limit is ${maxAppEventBytes} bytes`,
        };
      }
      if (decode !== undefined && namespaces.has(event.namespace)) {
        try {
          decode(event);
        } catch (cause) {
          return {
            code: "invalid_event",
            message: `${describe(event)} does not match the registered appEvents schema: ${String(cause)}`,
          };
        }
      }
      return undefined;
    },
  };
};

/** The admission conflict a refused `submit` item fails with. */
export const appEventConflict = (refusal: AppEventRefusal): SessionCommandAdmissionConflict =>
  new SessionCommandAdmissionConflict({
    code: refusal.code,
    message: refusal.message,
    pausedMessageIds: [],
  });
