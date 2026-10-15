import * as Schema from "effect/Schema";

import { CommandIdempotencyKey } from "../types/commands";
import { CommandId, type SequenceNumber } from "../types/core";
import type { DurableEventEnvelope } from "../types/events/envelope";
import type { TranscriptBatch } from "./interpreter";
import {
  transcriptMappingBounds,
  type TranscriptDefinition,
  type TranscriptMapping,
  type TranscriptMappingOperation,
} from "./mapping";
import { TranscriptProjectionError } from "./plan";
import {
  TranscriptDecorationEntryKey,
  transcriptDecorationBufferLimit,
  transcriptRecordId,
  type TranscriptBufferedDecoration,
  type TranscriptClassification,
  type TranscriptDecoration,
  type TranscriptDecorationBufferRecord,
  type TranscriptDecorations,
  type TranscriptFieldPath,
  type TranscriptUserRecord,
} from "./records";
import { canonicalJson } from "./version";

/**
 * Executes host mappings inside the projection. Every failure is local: a missing or oversize
 * value skips only that operation, and an unresolvable target skips only that mapping. Nothing
 * here can reject an event or touch a record the mapping does not target.
 */

interface MappingIndex {
  readonly byEvent: ReadonlyMap<string, ReadonlyArray<TranscriptMapping>>;
  readonly byName: ReadonlyMap<string, TranscriptMapping>;
}

const indexes = new WeakMap<TranscriptDefinition, MappingIndex>();

const eventKey = (namespace: string, type: string, schemaVersion: number) =>
  `${namespace}\u0000${type}\u0000${schemaVersion}`;

const mappingIndex = (definition: TranscriptDefinition): MappingIndex => {
  const cached = indexes.get(definition);
  if (cached !== undefined) return cached;
  const byEvent = new Map<string, Array<TranscriptMapping>>();
  const byName = new Map<string, TranscriptMapping>();
  for (const mapping of definition.mappings.mappings) {
    const key = eventKey(mapping.event.namespace, mapping.event.type, mapping.event.schemaVersion);
    byEvent.set(key, [...(byEvent.get(key) ?? []), mapping]);
    byName.set(mapping.name, mapping);
  }
  const index = { byEvent, byName };
  indexes.set(definition, index);
  return index;
};

/** A validated value for one operation of one mapping, ready to apply or to park. */
type PreparedOperation = Omit<TranscriptBufferedDecoration, "seq">;

/** Apply every mapping that names this app event. Unmapped events are ignored. */
export const applyForeignEvent = (
  batch: TranscriptBatch,
  envelope: DurableEventEnvelope,
  seq: SequenceNumber,
): void => {
  const mappings = mappingIndex(batch.definition).byEvent.get(
    eventKey(envelope.namespace, envelope.type, envelope.schemaVersion),
  );
  for (const mapping of mappings ?? []) {
    applyMapping(batch, mapping, envelope.payload, seq);
  }
};

const applyMapping = (
  batch: TranscriptBatch,
  mapping: TranscriptMapping,
  payload: unknown,
  seq: SequenceNumber,
) => {
  const prepared = mapping.operations.flatMap((operation, index) =>
    prepareOperation(mapping.name, operation, index, payload),
  );
  if (prepared.length === 0) return;
  const target = mapping.target;
  switch (target._tag) {
    case "Session": {
      const session = batch.session(seq);
      const applied = prepared.reduce<Decorated>(
        (decorated, operation) => applyPrepared(batch.definition, decorated, operation),
        { decorations: session.decorations },
      );
      batch.put({ ...session, updatedSeq: seq, decorations: applied.decorations });
      return;
    }
    case "UserByCommandId": {
      const commandId = extract(payload, target.path);
      if (!Schema.is(CommandId)(commandId)) return;
      const user = batch.user(transcriptRecordId.user(commandId));
      if (user === undefined) park(batch, seq, "command", commandId, prepared);
      else batch.put(decorateUser(batch.definition, { ...user, updatedSeq: seq }, prepared));
      return;
    }
    case "UserByIdempotencyKey": {
      const key = extract(payload, target.path);
      if (!Schema.is(CommandIdempotencyKey)(key)) return;
      const userId = batch.alias("idempotencyKey", key);
      const user = userId === undefined ? undefined : batch.user(userId);
      if (user === undefined) park(batch, seq, "idempotencyKey", key, prepared);
      else batch.put(decorateUser(batch.definition, { ...user, updatedSeq: seq }, prepared));
      return;
    }
  }
};

const prepareOperation = (
  mapping: string,
  operation: TranscriptMappingOperation,
  index: number,
  payload: unknown,
): ReadonlyArray<PreparedOperation> => {
  switch (operation._tag) {
    case "Classify":
      return [{ mapping, operation: index, value: null }];
    case "Copy":
    case "FirstWins": {
      const value = boundedJson(extract(payload, operation.from));
      return value === undefined ? [] : [{ mapping, operation: index, value }];
    }
    case "ReplaceListEntry": {
      const value = boundedJson(extract(payload, operation.from));
      const entryKey = listEntryKey(operation.key, payload);
      return value === undefined || entryKey === undefined
        ? []
        : [{ mapping, operation: index, value, entryKey }];
    }
  }
};

const listEntryKey = (
  key: Extract<TranscriptMappingOperation, { readonly _tag: "ReplaceListEntry" }>["key"],
  payload: unknown,
): TranscriptDecorationEntryKey | undefined => {
  const value = extract(payload, key.path);
  switch (key._tag) {
    case "Id":
      return typeof value === "string" &&
        value.length > 0 &&
        Schema.is(TranscriptDecorationEntryKey)(value)
        ? value
        : undefined;
    case "Ordinal":
      return typeof value === "number" && Number.isSafeInteger(value) && value >= 0
        ? value
        : undefined;
  }
};

const park = (
  batch: TranscriptBatch,
  seq: SequenceNumber,
  association: "command" | "idempotencyKey",
  key: string,
  prepared: ReadonlyArray<PreparedOperation>,
) => {
  const id = transcriptRecordId.decorationBuffer(association, key);
  const existing = batch.get(id);
  if (existing !== undefined && existing.kind !== "decorationBuffer") {
    throw new TranscriptProjectionError({
      message: `record ${id} is ${existing.kind}, expected decorationBuffer`,
    });
  }
  const buffer: TranscriptDecorationBufferRecord = {
    kind: "decorationBuffer",
    id,
    updatedSeq: seq,
    // Bounded in count and bytes: an orphaned key (its command never arrives) cannot grow
    // without limit.
    operations: withinBudget([
      ...(existing?.operations ?? []),
      ...prepared.map((operation) => ({ ...operation, seq })),
    ]),
  };
  batch.put(buffer);
};

/**
 * Apply decorations parked for a new user record, in arrival order, then the host's user
 * fallbacks for decorations that are still absent.
 */
export const replayBufferedDecorations = (
  batch: TranscriptBatch,
  seq: SequenceNumber,
  user: TranscriptUserRecord,
): TranscriptUserRecord => {
  const buffered = [
    transcriptRecordId.decorationBuffer("command", user.commandId),
    ...(user.idempotencyKey === undefined
      ? []
      : [transcriptRecordId.decorationBuffer("idempotencyKey", user.idempotencyKey)]),
  ].flatMap((id) => {
    const buffer = batch.get(id);
    if (buffer?.kind !== "decorationBuffer" || buffer.operations.length === 0) return [];
    // Drained once applied: the user record now holds these decorations.
    batch.put({ ...buffer, updatedSeq: seq, operations: [] });
    return buffer.operations;
  });
  buffered.sort((left, right) => left.seq - right.seq);
  const decorated = decorateUser(batch.definition, user, buffered);

  // Fallbacks describe ordinary submissions; a host-classified record (such as a subagent
  // notice) keeps only the decorations its own mappings give it.
  const session = batch.get(transcriptRecordId.session);
  if (session?.kind !== "sessionContext" || decorated.classification !== undefined) {
    return decorated;
  }
  let decorations = decorated.decorations;
  for (const fallback of batch.definition.mappings.userFallbacks) {
    const source = session.decorations[fallback.fromSession];
    if (decorations[fallback.to] === undefined && source?._tag === "Value") {
      decorations = { ...decorations, [fallback.to]: source };
    }
  }
  return decorations === decorated.decorations
    ? decorated
    : { ...decorated, updatedSeq: seq, decorations };
};

interface Decorated {
  readonly decorations: TranscriptDecorations;
  readonly classification?: TranscriptClassification;
}

const decorateUser = (
  definition: TranscriptDefinition,
  user: TranscriptUserRecord,
  operations: ReadonlyArray<PreparedOperation>,
): TranscriptUserRecord => {
  const decorated = operations.reduce<Decorated>(
    (current, operation) => applyPrepared(definition, current, operation),
    {
      decorations: user.decorations,
      ...(user.classification === undefined ? {} : { classification: user.classification }),
    },
  );
  return {
    ...user,
    decorations: decorated.decorations,
    ...(decorated.classification === undefined ? {} : { classification: decorated.classification }),
  };
};

/** The longest prefix of parked operations within the buffer's count and byte bounds. */
const withinBudget = (
  operations: ReadonlyArray<TranscriptBufferedDecoration>,
): ReadonlyArray<TranscriptBufferedDecoration> => {
  const kept: Array<TranscriptBufferedDecoration> = [];
  let bytes = 2;
  for (const operation of operations.slice(0, transcriptDecorationBufferLimit)) {
    bytes += jsonBytes(operation) + 1;
    if (bytes > transcriptMappingBounds.maxDecorationBytes) break;
    kept.push(operation);
  }
  return kept;
};

/** Apply one operation unless the record's decorations would exceed their byte budget. */
const applyPrepared = (
  definition: TranscriptDefinition,
  target: Decorated,
  prepared: PreparedOperation,
): Decorated => {
  const next = applyOperation(definition, target, prepared);
  return next === target ||
    jsonBytes(next.decorations) <= transcriptMappingBounds.maxDecorationBytes
    ? next
    : target;
};

const jsonBytes = (value: unknown): number => encoder.encode(canonicalJson(value)).length;

const applyOperation = (
  definition: TranscriptDefinition,
  target: Decorated,
  prepared: PreparedOperation,
): Decorated => {
  const operation = mappingIndex(definition).byName.get(prepared.mapping)?.operations[
    prepared.operation
  ];
  if (operation === undefined) return target;
  switch (operation._tag) {
    case "Classify":
      return { ...target, classification: operation.classification };
    case "Copy":
      return withDecoration(target, operation.to, { _tag: "Value", value: prepared.value });
    case "FirstWins":
      return target.decorations[operation.to] === undefined
        ? withDecoration(target, operation.to, { _tag: "Value", value: prepared.value })
        : target;
    case "ReplaceListEntry": {
      if (prepared.entryKey === undefined) return target;
      const list = replaceListEntry(
        target.decorations[operation.to],
        prepared.entryKey,
        prepared.value,
        operation.key._tag === "Ordinal",
      );
      return list === undefined ? target : withDecoration(target, operation.to, list);
    }
  }
};

const withDecoration = (
  target: Decorated,
  key: string,
  decoration: TranscriptDecoration,
): Decorated => ({ ...target, decorations: { ...target.decorations, [key]: decoration } });

const replaceListEntry = (
  existing: TranscriptDecoration | undefined,
  key: TranscriptDecorationEntryKey,
  value: Schema.Json,
  ordered: boolean,
): TranscriptDecoration | undefined => {
  const entries = existing?._tag === "List" ? existing.entries : [];
  const index = entries.findIndex((entry) => entry.key === key);
  if (index >= 0) {
    return {
      _tag: "List",
      entries: entries.map((entry, position) => (position === index ? { key, value } : entry)),
    };
  }
  if (entries.length >= transcriptMappingBounds.maxListEntries) return undefined;
  const next = [...entries, { key, value }];
  if (ordered) next.sort((left, right) => Number(left.key) - Number(right.key));
  return { _tag: "List", entries: next };
};

/** Read a payload value by path through own properties only. */
const extract = (value: unknown, path: TranscriptFieldPath): unknown =>
  path.reduce<unknown>((current, key) => {
    if (typeof key === "number") return Array.isArray(current) ? current[key] : undefined;
    if (current === null || typeof current !== "object" || Array.isArray(current)) {
      return undefined;
    }
    return Object.getOwnPropertyDescriptor(current, key)?.value;
  }, value);

const encoder = new TextEncoder();

/**
 * A payload value as the JSON a persisted log holds (so a live projection and a rebuild agree),
 * within the value bound; otherwise nothing.
 */
const boundedJson = (value: unknown): Schema.Json | undefined => {
  if (value === undefined) return undefined;
  try {
    const json = JSON.stringify(value);
    if (json === undefined) return undefined;
    const normalized: unknown = JSON.parse(json);
    return Schema.is(Schema.Json)(normalized) &&
      encoder.encode(canonicalJson(normalized)).length <= transcriptMappingBounds.maxValueBytes
      ? normalized
      : undefined;
  } catch {
    return undefined;
  }
};
