import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";

import { SequenceNumber } from "../types/core";
import { NonNegativeInt } from "../types/events/envelope";
import {
  InferenceAssistantRecordId,
  TranscriptAssistantPart,
  TranscriptCursor,
  TranscriptLogIncarnation,
  TranscriptOrder,
  TranscriptRecordViewFromEnvelope,
  isPendingTranscriptUserRecord,
  isTopLevelTranscriptRecord,
  TranscriptVersion,
  type TranscriptOrderKey,
  type TranscriptRecordView,
} from "./records";

const PositiveInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));

const orderKeyOfCursor = (cursor: TranscriptCursor): TranscriptOrderKey => ({
  anchorSeq: cursor.anchorSeq,
  ordinal: cursor.ordinal,
  recordId: cursor.recordId,
});

/**
 * Which slice of history to read.
 *
 * `Latest` serves first paint (SSR, CLIs); `Page` walks either direction from a cursor;
 * `Range` re-reads exactly the span a client retains, inclusive of both ends, at one new cut.
 */
export const TranscriptWindowSelector = Schema.Union([
  Schema.TaggedStruct("Latest", { count: PositiveInt }),
  Schema.TaggedStruct("Page", {
    direction: Schema.Literals(["before", "after"]),
    cursor: TranscriptCursor,
    count: PositiveInt,
  }),
  Schema.TaggedStruct("Range", { from: TranscriptCursor, to: TranscriptCursor }).check(
    Schema.makeFilter(
      (range) =>
        (range.from.incarnation === range.to.incarnation &&
          range.from.version === range.to.version) ||
        "a range is scoped to one incarnation and version",
    ),
    Schema.makeFilter(
      (range) =>
        TranscriptOrder(orderKeyOfCursor(range.from), orderKeyOfCursor(range.to)) <= 0 ||
        "a range starts at or before its end",
    ),
  ),
]);
export type TranscriptWindowSelector = typeof TranscriptWindowSelector.Type;

/**
 * Bounds on one window. `maxRecords` caps top-level records (a selector's `count` is further
 * capped by it); `maxBytes` caps the whole response, dependencies included; bodies above
 * `maxInlineBodyBytes` are sent as references.
 */
export const TranscriptWindowLimits = Schema.Struct({
  maxRecords: PositiveInt,
  maxBytes: PositiveInt,
  maxInlineBodyBytes: NonNegativeInt,
});
export type TranscriptWindowLimits = typeof TranscriptWindowLimits.Type;

/** Default bounds: the first window holds 100 top-level records (design decision 4). */
export const defaultTranscriptWindowLimits: TranscriptWindowLimits = {
  maxRecords: 100,
  maxBytes: 1_048_576,
  maxInlineBodyBytes: 16_384,
};

/**
 * Whether the window is the current interpretation of the log, and if not, why its cut is
 * behind.
 *
 * Every state other than `Ready` is a degraded read and obeys one rule: the response contains
 * nothing newer than `throughSeq` — no newer pending users, session context or live content.
 */
const KnownReadiness = Schema.TaggedUnion({
  /** The active version is healthy and projected through the log head at read time. */
  Ready: {},
  /** The active version failed while projecting `failedAtSeq`; this is its last good cut. */
  Degraded: { failedAtSeq: SequenceNumber, error: Schema.String },
  /**
   * The served cut is consistent but behind the log: either the active version is no longer
   * carried by this release and history is being rebuilt under the current definition (clients
   * receive one replacement when it activates), or the active version is catching up to the head
   * (a session re-enabled after it was disabled; clients receive a re-read when it gets there).
   */
  UpdatingHistory: {},
  /** No version is servable yet; the window is empty. */
  Building: {},
  /**
   * A stored row of the served version cannot be decoded by this release. Nothing is served
   * until a rebuild finishes, rather than a wrong view; the window is empty.
   */
  Unreadable: { error: Schema.String },
  /** A readiness this reader does not know; treated as degraded. */
  Unknown: { readinessType: Schema.String },
});

const knownReadinessTags: ReadonlySet<string> = new Set([
  "Ready",
  "Degraded",
  "UpdatingHistory",
  "Building",
  "Unreadable",
  "Unknown",
]);

const UnrecognizedReadiness = Schema.Struct({
  _tag: Schema.String.check(
    Schema.makeFilter(
      (tag) => !knownReadinessTags.has(tag) || "a known readiness must decode as that readiness",
    ),
  ),
}).pipe(
  Schema.decodeTo(Schema.toType(KnownReadiness.cases.Unknown), {
    decode: SchemaGetter.transform((readiness) => ({
      _tag: "Unknown" as const,
      readinessType: readiness._tag,
    })),
    encode: SchemaGetter.transform((readiness) => ({ _tag: readiness.readinessType })),
  }),
);

/**
 * Whether the window is the current interpretation of the log. A readiness this reader does not
 * know decodes as `Unknown` and is treated as degraded: it carries no live content. A malformed
 * readiness with a known tag is a decode failure.
 */
export const TranscriptReadiness = Schema.Union([KnownReadiness, UnrecognizedReadiness]);
export type TranscriptReadiness = typeof TranscriptReadiness.Type;

/**
 * One live part of the inference streaming now.
 *
 * `id` is stable for the life of the part (`text:<providerPartId>`, `reasoning:<providerPartId>`,
 * `tool:<toolCallId>`), so a client keys a streaming part by it and replaces it in place as it
 * grows. `paramsText` is the tool-call parameter text streamed so far; it is speculative until
 * the call is sealed into a tool record.
 */
export const TranscriptActivePart = Schema.Struct({
  id: Schema.NonEmptyString,
  part: TranscriptAssistantPart,
  paramsText: Schema.optionalKey(Schema.String),
}).check(
  Schema.makeFilter(
    (active) =>
      (active.part._tag === "Text"
        ? active.id.startsWith("text:")
        : active.part._tag === "Reasoning"
          ? active.id.startsWith("reasoning:")
          : active.part._tag === "ToolCall"
            ? active.id.startsWith("tool:")
            : true) || "a live part's id names its kind",
  ),
  Schema.makeFilter(
    (active) =>
      active.paramsText === undefined ||
      active.part._tag === "ToolCall" ||
      "only a tool call streams parameter text",
  ),
);
export type TranscriptActivePart = typeof TranscriptActivePart.Type;

/**
 * Accumulated parts of the inference streaming now, captured at the same publication boundary as
 * the window. Owned by the live runtime; storage-level reads never include it.
 */
export const TranscriptActiveContent = Schema.Struct({
  assistantId: InferenceAssistantRecordId,
  parts: Schema.Array(TranscriptActivePart),
});
export type TranscriptActiveContent = typeof TranscriptActiveContent.Type;

const recordUpdatedSeq = (record: TranscriptRecordView): number => record.updatedSeq;

const recordPosition = (record: TranscriptRecordView) =>
  "position" in record ? record.position : undefined;

/** Positioned history, which is served only as a top-level record and never as a dependency. */
const isHistoryRecord = (record: TranscriptRecordView): boolean =>
  record.kind === "unknown" ? record.position !== undefined : isTopLevelTranscriptRecord(record);

/**
 * One complete, consistent read of a transcript version.
 *
 * `records` are top-level records in transcript order; `dependencies` are the tools and run
 * groups they reference, the session context, and every pending user record. Every record is
 * projected through `throughSeq` and none is newer, which is what makes a cut complete. A client
 * replaces, never merges, what a window covers.
 */
export const TranscriptWindow = Schema.Struct({
  incarnation: TranscriptLogIncarnation,
  version: TranscriptVersion,
  throughSeq: SequenceNumber,
  records: Schema.Array(TranscriptRecordViewFromEnvelope),
  dependencies: Schema.Array(TranscriptRecordViewFromEnvelope),
  activeContent: Schema.optionalKey(Schema.Array(TranscriptActiveContent)),
  hasOlder: Schema.Boolean,
  hasNewer: Schema.Boolean,
  readiness: TranscriptReadiness,
}).check(
  Schema.makeFilter(
    (window) =>
      [...window.records, ...window.dependencies].every(
        (record) => recordUpdatedSeq(record) <= window.throughSeq,
      ) || "every record must be projected through throughSeq and none may be newer",
  ),
  Schema.makeFilter(
    (window) =>
      window.readiness._tag === "Ready" ||
      window.activeContent === undefined ||
      "a degraded read never attaches live content",
  ),
  Schema.makeFilter(
    (window) =>
      window.readiness._tag !== "Degraded" ||
      window.readiness.failedAtSeq > window.throughSeq ||
      "a degraded cut ends before the seq that failed",
  ),
  Schema.makeFilter(
    (window) =>
      [...window.records, ...window.dependencies].every(
        (record) =>
          record.kind !== "decorationBuffer" &&
          (record.kind !== "unknown" || record.originalKind !== "decorationBuffer"),
      ) || "decoration buffers are never served",
  ),
  Schema.makeFilter((window) => {
    const ids = [...window.records, ...window.dependencies].map((record) => record.id);
    return new Set(ids).size === ids.length || "a record is served at most once";
  }),
  Schema.makeFilter(
    (window) =>
      window.records.every((record, index) => {
        const position = recordPosition(record);
        if (position === undefined || !isHistoryRecord(record)) return false;
        const previous = index === 0 ? undefined : window.records[index - 1];
        const previousPosition = previous === undefined ? undefined : recordPosition(previous);
        return (
          previous === undefined ||
          previousPosition === undefined ||
          TranscriptOrder(
            { ...previousPosition, recordId: previous.id },
            { ...position, recordId: record.id },
          ) < 0
        );
      }) || "records are positioned history records in transcript order",
  ),
  Schema.makeFilter(
    (window) =>
      window.dependencies.every((record) => !isHistoryRecord(record)) ||
      "dependencies never contain history records",
  ),
  Schema.makeFilter(
    (window) =>
      window.dependencies.every(
        (record) => record.kind !== "user" || isPendingTranscriptUserRecord(record),
      ) || "the only user dependencies are pending users; cancelled users are never served",
  ),
  Schema.makeFilter(
    (window) =>
      (window.readiness._tag !== "Building" && window.readiness._tag !== "Unreadable") ||
      (window.records.length === 0 &&
        window.dependencies.length === 0 &&
        !window.hasOlder &&
        !window.hasNewer) ||
      "a building or unreadable window is empty",
  ),
);
export type TranscriptWindow = typeof TranscriptWindow.Type;

/** Paging cursor for one top-level record of a window. */
export const transcriptCursorOf = (
  window: Pick<TranscriptWindow, "incarnation" | "version">,
  record: TranscriptRecordView,
): TranscriptCursor | undefined => {
  const position = "position" in record ? record.position : undefined;
  return position === undefined
    ? undefined
    : {
        incarnation: window.incarnation,
        version: window.version,
        anchorSeq: position.anchorSeq,
        ordinal: position.ordinal,
        recordId: record.id,
      };
};

/** Health of one stored transcript version. A failed version is frozen at its last good cursor. */
export const TranscriptVersionStatus = Schema.TaggedUnion({
  Ok: {},
  Failed: { failedAtSeq: SequenceNumber, error: Schema.String },
});
export type TranscriptVersionStatus = typeof TranscriptVersionStatus.Type;

/**
 * Per-version maintenance state, one per version present in a session's storage.
 *
 * `cursor` is the last fully projected seq, committed together with that version's rows. At most
 * one version is `active` (served) and at most one is `building` (catching up from the log).
 */
export const TranscriptVersionMeta = Schema.Struct({
  version: TranscriptVersion,
  role: Schema.Literals(["active", "building"]),
  cursor: SequenceNumber,
  status: TranscriptVersionStatus,
}).check(
  Schema.makeFilter(
    (meta) =>
      meta.status._tag !== "Failed" ||
      meta.status.failedAtSeq > meta.cursor ||
      "a failed version is frozen before the seq that failed",
  ),
);
export type TranscriptVersionMeta = typeof TranscriptVersionMeta.Type;
