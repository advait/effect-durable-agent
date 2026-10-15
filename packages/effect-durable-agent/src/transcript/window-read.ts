import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import { SequenceNumber } from "../types/core";
import { NonNegativeInt } from "../types/events/envelope";
import type { TranscriptAliasNamespace } from "./plan";
import {
  TranscriptOrder,
  TranscriptRecordFromEnvelope,
  isPendingTranscriptUserRecord,
  transcriptRecordId,
  type AssistantRecordId,
  type TranscriptAssistantPart,
  type TranscriptAssistantRecord,
  type TranscriptBody,
  type TranscriptCompactionRecord,
  type TranscriptCursor,
  type TranscriptLogIncarnation,
  type TranscriptOrderKey,
  type TranscriptRecord,
  type TranscriptRecordId,
  type TranscriptToolRecord,
  type TranscriptUserRecord,
  type TranscriptVersion,
} from "./records";
import { TranscriptCursorMismatch } from "./store";
import type {
  TranscriptReadiness,
  TranscriptVersionMeta,
  TranscriptWindow,
  TranscriptWindowLimits,
  TranscriptWindowSelector,
} from "./window";

/** A positioned record that counts toward a window's record limit. */
export type TranscriptTopLevelRecord =
  | TranscriptUserRecord
  | TranscriptAssistantRecord
  | TranscriptCompactionRecord;

/**
 * Synchronous indexed reads of the served version at one cut, as a store provides them.
 *
 * Window assembly (paging, dependency collection, byte budgeting and body references) is shared
 * by every store through `readTranscriptWindowFrom`; a store only supplies these reads.
 */
export interface TranscriptWindowSource {
  readonly incarnation: TranscriptLogIncarnation;
  /** Maintenance state of the served (active) version; its cursor is the window's `throughSeq`. */
  readonly meta: TranscriptVersionMeta;
  /** False when this release no longer carries the served version (a missed session). */
  readonly carried: boolean;
  /** Top-level records before `key` (or before the end), newest first. */
  readonly topLevelBefore: (
    key: TranscriptOrderKey | undefined,
    limit: number,
    inclusive: boolean,
  ) => ReadonlyArray<TranscriptTopLevelRecord>;
  /** Top-level records after `key` (or after the start), oldest first. */
  readonly topLevelAfter: (
    key: TranscriptOrderKey | undefined,
    limit: number,
    inclusive: boolean,
  ) => ReadonlyArray<TranscriptTopLevelRecord>;
  readonly record: (id: TranscriptRecordId) => TranscriptRecord | undefined;
  readonly alias: (
    namespace: TranscriptAliasNamespace,
    key: string,
  ) => TranscriptRecordId | undefined;
  /** Tool records of one assistant (the typed dependency lookup). */
  readonly toolsOf: (assistantId: AssistantRecordId) => ReadonlyArray<TranscriptToolRecord>;
  /** Every user record still waiting for the agent (the pending-membership index). */
  readonly pendingUsers: () => ReadonlyArray<TranscriptUserRecord>;
}

/** Readiness of a served version: why its cut may be behind the log. */
export const transcriptReadiness = (
  meta: TranscriptVersionMeta,
  carried: boolean,
): TranscriptReadiness => {
  if (!carried) return { _tag: "UpdatingHistory" };
  switch (meta.status._tag) {
    case "Ok":
      return { _tag: "Ready" };
    case "Failed":
      return {
        _tag: "Degraded",
        failedAtSeq: meta.status.failedAtSeq,
        error: meta.status.error,
      };
  }
};

/** The empty window served while no version is servable yet. */
export const buildingTranscriptWindow = (
  incarnation: TranscriptLogIncarnation,
  version: TranscriptVersion,
): TranscriptWindow => ({
  incarnation,
  version,
  throughSeq: SequenceNumber.make(0),
  records: [],
  dependencies: [],
  hasOlder: false,
  hasNewer: false,
  readiness: { _tag: "Building" },
});

const orderKey = (record: TranscriptTopLevelRecord): TranscriptOrderKey => ({
  anchorSeq: record.position?.anchorSeq ?? 0,
  ordinal: record.position?.ordinal ?? 0,
  recordId: record.id,
});

const cursorKey = (cursor: TranscriptCursor): TranscriptOrderKey => ({
  anchorSeq: cursor.anchorSeq,
  ordinal: cursor.ordinal,
  recordId: cursor.recordId,
});

/**
 * Assemble one window from a source at a single cut.
 *
 * The response never contains anything newer than the served version's cursor: a frozen (failed
 * or no longer carried) version's rows, pending users and session context all stop at its last
 * good cut, which is the degraded-read rule. Bodies above the inline limit become references;
 * if the response still exceeds the byte budget, records are dropped from the far end of the
 * selection, keeping at least one so paging always progresses.
 */
export const readTranscriptWindowFrom = (
  source: TranscriptWindowSource,
  selector: TranscriptWindowSelector,
  limits: TranscriptWindowLimits,
): Result.Result<TranscriptWindow, TranscriptCursorMismatch> => {
  const version = source.meta.version;
  for (const cursor of selectorCursors(selector)) {
    if (cursor.incarnation !== source.incarnation || cursor.version !== version) {
      return Result.fail(
        new TranscriptCursorMismatch({
          message: `cursor for ${cursor.incarnation}/${cursor.version} read against ${source.incarnation}/${version}`,
        }),
      );
    }
  }

  const selected = selectTopLevel(source, selector, limits.maxRecords);
  // Latest and Page(before) keep the newest records when trimming; the others keep the oldest.
  const keepNewest =
    selector._tag === "Latest" || (selector._tag === "Page" && selector.direction === "before");
  let top = selected.map((record) => withBodyRefs(record, limits.maxInlineBodyBytes));
  let dependencies = collectDependencies(source, top, limits.maxInlineBodyBytes);
  while (top.length > 1 && windowBytes(top, dependencies) > limits.maxBytes) {
    top = keepNewest ? top.slice(1) : top.slice(0, -1);
    dependencies = collectDependencies(source, top, limits.maxInlineBodyBytes);
  }

  const first = top[0];
  const last = top[top.length - 1];
  const [hasOlder, hasNewer] =
    first === undefined || last === undefined
      ? emptySelectionNeighbours(source, selector)
      : [
          source.topLevelBefore(orderKey(first), 1, false).length > 0,
          source.topLevelAfter(orderKey(last), 1, false).length > 0,
        ];

  return Result.succeed({
    incarnation: source.incarnation,
    version,
    throughSeq: source.meta.cursor,
    records: top,
    dependencies,
    hasOlder,
    hasNewer,
    readiness: transcriptReadiness(source.meta, source.carried),
  });
};

const selectorCursors = (selector: TranscriptWindowSelector): ReadonlyArray<TranscriptCursor> => {
  switch (selector._tag) {
    case "Latest":
      return [];
    case "Page":
      return [selector.cursor];
    case "Range":
      return [selector.from, selector.to];
  }
};

const selectTopLevel = (
  source: TranscriptWindowSource,
  selector: TranscriptWindowSelector,
  maxRecords: number,
): ReadonlyArray<TranscriptTopLevelRecord> => {
  switch (selector._tag) {
    case "Latest":
      return [
        ...source.topLevelBefore(undefined, Math.min(selector.count, maxRecords), false),
      ].reverse();
    case "Page": {
      const count = Math.min(selector.count, maxRecords);
      return selector.direction === "before"
        ? [...source.topLevelBefore(cursorKey(selector.cursor), count, false)].reverse()
        : source.topLevelAfter(cursorKey(selector.cursor), count, false);
    }
    case "Range": {
      const to = cursorKey(selector.to);
      return source
        .topLevelAfter(cursorKey(selector.from), maxRecords, true)
        .filter((record) => TranscriptOrder(orderKey(record), to) <= 0);
    }
  }
};

const emptySelectionNeighbours = (
  source: TranscriptWindowSource,
  selector: TranscriptWindowSelector,
): readonly [boolean, boolean] => {
  switch (selector._tag) {
    case "Latest":
      return [false, false];
    case "Page": {
      const key = cursorKey(selector.cursor);
      return selector.direction === "before"
        ? [false, source.topLevelAfter(key, 1, true).length > 0]
        : [source.topLevelBefore(key, 1, true).length > 0, false];
    }
    case "Range":
      return [
        source.topLevelBefore(cursorKey(selector.from), 1, false).length > 0,
        source.topLevelAfter(cursorKey(selector.to), 1, false).length > 0,
      ];
  }
};

/** Session context, then each record's run group and tools in part order, then pending users. */
const collectDependencies = (
  source: TranscriptWindowSource,
  top: ReadonlyArray<TranscriptTopLevelRecord>,
  maxInlineBodyBytes: number,
): ReadonlyArray<TranscriptRecord> => {
  const dependencies = new Map<string, TranscriptRecord>();
  const add = (record: TranscriptRecord | undefined) => {
    if (record !== undefined && !dependencies.has(record.id)) {
      dependencies.set(record.id, withBodyRefs(record, maxInlineBodyBytes));
    }
  };
  add(source.record(transcriptRecordId.session));
  for (const record of top) {
    switch (record.kind) {
      case "assistant": {
        if (record.runGroupId !== undefined) add(source.record(record.runGroupId));
        const partOrder = new Map(
          record.parts.flatMap((part, index) =>
            part._tag === "ToolCall" && part.toolId !== undefined ? [[part.toolId, index]] : [],
          ),
        );
        const tools = [...source.toolsOf(record.id)].sort(
          (left, right) =>
            (partOrder.get(left.id) ?? Number.MAX_SAFE_INTEGER) -
              (partOrder.get(right.id) ?? Number.MAX_SAFE_INTEGER) ||
            (left.id < right.id ? -1 : left.id > right.id ? 1 : 0),
        );
        tools.forEach(add);
        break;
      }
      case "user": {
        const runId = record.state._tag === "Consumed" ? record.state.runId : undefined;
        const groupId = runId === undefined ? undefined : source.alias("runId", runId);
        if (groupId !== undefined) add(source.record(groupId));
        break;
      }
      case "compaction":
        break;
    }
  }
  [...source.pendingUsers()]
    .filter(isPendingTranscriptUserRecord)
    .sort(
      (left, right) =>
        left.submittedSeq - right.submittedSeq ||
        (left.id < right.id ? -1 : left.id > right.id ? 1 : 0),
    )
    .forEach(add);
  return Array.from(dependencies.values());
};

const encoder = new TextEncoder();
const encodeRecord = Schema.encodeSync(TranscriptRecordFromEnvelope);

const jsonBytes = (value: unknown): number => {
  const json = JSON.stringify(value);
  return json === undefined ? 0 : encoder.encode(json).length;
};

const windowBytes = (
  top: ReadonlyArray<TranscriptRecord>,
  dependencies: ReadonlyArray<TranscriptRecord>,
): number =>
  [...top, ...dependencies].reduce((total, record) => total + jsonBytes(encodeRecord(record)), 0);

const bodyRef = (body: TranscriptBody, maxInlineBodyBytes: number): TranscriptBody => {
  if (body._tag === "Ref") return body;
  const byteLength = jsonBytes(body.value);
  return byteLength <= maxInlineBodyBytes
    ? body
    : { _tag: "Ref", source: body.source, byteLength: NonNegativeInt.make(byteLength) };
};

/** Replace inline bodies above the limit with references to their immutable log source. */
const withBodyRefs = <R extends TranscriptRecord>(record: R, maxInlineBodyBytes: number): R => {
  switch (record.kind) {
    case "tool": {
      const tool: TranscriptToolRecord = {
        ...record,
        ...(record.params === undefined
          ? {}
          : { params: bodyRef(record.params, maxInlineBodyBytes) }),
        lifecycle:
          record.lifecycle._tag === "Completed"
            ? { _tag: "Completed", result: bodyRef(record.lifecycle.result, maxInlineBodyBytes) }
            : record.lifecycle,
      };
      return { ...record, ...tool };
    }
    case "assistant": {
      const parts = record.parts.map((part): TranscriptAssistantPart =>
        part._tag === "ToolResult"
          ? { ...part, result: bodyRef(part.result, maxInlineBodyBytes) }
          : part,
      );
      return { ...record, parts };
    }
    default:
      return record;
  }
};
