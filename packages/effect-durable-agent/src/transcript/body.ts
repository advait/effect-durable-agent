import * as Schema from "effect/Schema";

import {
  assistantMessageCommittedEventType,
  assistantMessageImportedEventType,
  assistantPartialCommittedEventType,
  toolCallCompletedEventType,
  toolCallCreatedEventType,
} from "../types/events/durable";
import { ProviderPartId, ToolName, effectDurableAgentNamespace } from "../types/events/envelope";
import type { TranscriptFieldPath } from "./records";

const bodyEncoder = new TextEncoder();

/**
 * A body value normalized to the JSON a persisted log holds: what the interpreter inlines and what
 * a reference resolves to, so the two can never disagree. A value with no JSON text (`undefined`,
 * a function) is `null`; a value with no JSON form at all (a bigint, a cycle) is `undefined`.
 */
export const normalizeTranscriptBodyValue = (
  value: unknown,
): { readonly value: Schema.Json; readonly byteLength: number } | undefined => {
  let json: string | undefined;
  try {
    json = JSON.stringify(value);
  } catch {
    return undefined;
  }
  const normalized: unknown = json === undefined ? null : JSON.parse(json);
  if (!Schema.is(Schema.Json)(normalized)) return undefined;
  return {
    value: normalized,
    byteLength: json === undefined ? 4 : bodyEncoder.encode(json).length,
  };
};

/** The committed-event fields that decide where its bodies are. */
export interface TranscriptBodyEvent {
  readonly namespace: string;
  readonly type: string;
  readonly payload: unknown;
}

const isRecord = (value: unknown): value is object =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** An own property, or `undefined`: never an inherited name such as `toString`. */
const own = (value: unknown, key: string): unknown =>
  isRecord(value) && Object.prototype.hasOwnProperty.call(value, key)
    ? Reflect.get(value, key)
    : undefined;

const assistantEventTypes: ReadonlySet<string> = new Set([
  assistantMessageCommittedEventType,
  assistantPartialCommittedEventType,
  assistantMessageImportedEventType,
]);

/**
 * The value at a body site of a framework event, or `undefined` when `path` names no body.
 *
 * The interpreter creates bodies at exactly three sites: tool parameters
 * (`ToolCallCreated` `promptPart.params`), tool results (`ToolCallCompleted` `promptPart.result`)
 * and provider-executed tool results inside assistant output (`promptParts[i].result` of a
 * well-formed `tool-result` part). Nothing else in the log is a body, so a reference can never
 * reach a system prompt, a command input or an app event.
 */
const bodySiteValue = (
  event: TranscriptBodyEvent,
  path: TranscriptFieldPath,
): { readonly value: unknown } | undefined => {
  if (event.namespace !== effectDurableAgentNamespace) return undefined;
  const [first, second, third, ...rest] = path;
  if (rest.length > 0) return undefined;
  if (event.type === toolCallCreatedEventType || event.type === toolCallCompletedEventType) {
    const field = event.type === toolCallCreatedEventType ? "params" : "result";
    const promptPart = own(event.payload, "promptPart");
    return first === "promptPart" && second === field && third === undefined && isRecord(promptPart)
      ? { value: own(promptPart, field) }
      : undefined;
  }
  if (!assistantEventTypes.has(event.type)) return undefined;
  if (first !== "promptParts" || typeof second !== "number" || third !== "result") return undefined;
  const parts = own(event.payload, "promptParts");
  const part: unknown = Array.isArray(parts) ? parts[second] : undefined;
  return own(part, "type") === "tool-result" &&
    Schema.is(ProviderPartId)(own(part, "id")) &&
    Schema.is(ToolName)(own(part, "name"))
    ? { value: own(part, "result") }
    : undefined;
};

/** A resolved body: the JSON the interpreter inlined there, or why there is none. */
export type ResolvedTranscriptBody =
  | { readonly _tag: "Found"; readonly value: Schema.Json }
  | { readonly _tag: "Missing"; readonly message: string };

/**
 * Resolve a body reference against the committed event it names: the read side of the
 * interpreter's body rule. Resolving through the log keeps a `Ref` valid after projection rows are
 * rebuilt or deleted; any path that is not a body site resolves to nothing.
 */
export const resolveTranscriptBody = (
  event: TranscriptBodyEvent,
  path: TranscriptFieldPath,
): ResolvedTranscriptBody => {
  const site = bodySiteValue(event, path);
  if (site === undefined) {
    return { _tag: "Missing", message: "the path names no transcript body" };
  }
  const normalized = normalizeTranscriptBodyValue(site.value);
  return normalized === undefined
    ? { _tag: "Missing", message: "the body has no JSON form" }
    : { _tag: "Found", value: normalized.value };
};
