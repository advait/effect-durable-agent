import * as Schema from "effect/Schema";

import { SequenceNumber } from "../types/core";
import { NonNegativeInt } from "../types/events/envelope";
import {
  InferenceAssistantRecordId,
  TranscriptAssistantPart,
  TranscriptCursor,
  TranscriptLogIncarnation,
  TranscriptRecordViewFromEnvelope,
  TranscriptVersion,
  type TranscriptRecordView,
} from "./records";

const PositiveInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));

/**
 * Which slice of history to read.
 *
 * `Latest` serves first paint (SSR, CLIs); `Page` walks either direction from a cursor;
 * `Range` re-reads exactly the span a client retains, inclusive of both ends, at one new cut.
 */
export const TranscriptWindowSelector = Schema.TaggedUnion({
  Latest: { count: PositiveInt },
  Page: {
    direction: Schema.Literals(["before", "after"]),
    cursor: TranscriptCursor,
    count: PositiveInt,
  },
  Range: { from: TranscriptCursor, to: TranscriptCursor },
});
export type TranscriptWindowSelector = typeof TranscriptWindowSelector.Type;

/**
 * Bounds on one window. `maxRecords` caps top-level records; `maxBytes` caps the whole response,
 * dependencies included; bodies above `maxInlineBodyBytes` are sent as references.
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
export const TranscriptReadiness = Schema.TaggedUnion({
  /** The active version is healthy and projected through the log head at read time. */
  Ready: {},
  /** The active version failed while projecting `failedAtSeq`; this is its last good cut. */
  Degraded: { failedAtSeq: SequenceNumber, error: Schema.String },
  /**
   * The active version is no longer carried by this release; history is being rebuilt under
   * the current definition, and clients receive one replacement when it activates.
   */
  UpdatingHistory: {},
  /** No version is servable yet; the window is empty. */
  Building: {},
});
export type TranscriptReadiness = typeof TranscriptReadiness.Type;

/**
 * Accumulated parts of the inference streaming now, captured at the same publication boundary as
 * the window. Owned by the live runtime; storage-level reads never include it.
 */
export const TranscriptActiveContent = Schema.Struct({
  assistantId: InferenceAssistantRecordId,
  parts: Schema.Array(TranscriptAssistantPart),
});
export type TranscriptActiveContent = typeof TranscriptActiveContent.Type;

const recordUpdatedSeq = (record: TranscriptRecordView): number => record.updatedSeq;

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
});
export type TranscriptVersionMeta = typeof TranscriptVersionMeta.Type;
