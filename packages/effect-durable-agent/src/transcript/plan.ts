import * as Schema from "effect/Schema";

import type { SequenceNumber } from "../types/core";
import type { CommittedDurableEvent } from "../services/session-store";
import type { TranscriptDefinition } from "./mapping";
import type { TranscriptRecord, TranscriptRecordId } from "./records";

/**
 * Secondary keys that resolve to a record through an indexed join, never an in-memory map.
 *
 * - `messageId`: every message id a user submission has carried, including replacement ids.
 * - `idempotencyKey`: a submitting command's caller-owned retry key.
 * - `runId`: every run of a run group, so continuations resolve to the root's group.
 * - `runCommand`: commands owned by a run group; a run that reuses them continues the group.
 * - `toolPart`: `<inferenceId>/<providerPartId>`, linking an assistant tool-call part to its tool.
 */
export const TranscriptAliasNamespace = Schema.Literals([
  "messageId",
  "idempotencyKey",
  "runId",
  "runCommand",
  "toolPart",
]);
export type TranscriptAliasNamespace = typeof TranscriptAliasNamespace.Type;

/** One alias row: `(namespace, key)` resolves to `recordId` within one version. */
export interface TranscriptAlias {
  readonly namespace: TranscriptAliasNamespace;
  readonly key: string;
  readonly recordId: TranscriptRecordId;
}

/**
 * Synchronous indexed reads of one transcript version, as committed before the batch.
 *
 * Adapters back this with indexed storage reads so the interpreter can run inside a synchronous
 * transaction (Cloudflare `storage.transactionSync`). Reads see no uncommitted plan; the
 * interpreter overlays its own in-batch changes.
 */
export interface TranscriptLookup {
  /** Last seq already projected into this version. Events at or below it are skipped. */
  readonly cursor: SequenceNumber;
  readonly record: (id: TranscriptRecordId) => TranscriptRecord | undefined;
  readonly alias: (
    namespace: TranscriptAliasNamespace,
    key: string,
  ) => TranscriptRecordId | undefined;
  /** Next free ordinal among records positioned at `anchorSeq`; 0 when none are. */
  readonly nextOrdinal: (anchorSeq: SequenceNumber) => number;
}

/**
 * Deterministic changes for one committed batch: the final state of every record the batch
 * touched, in first-touch order, the aliases it introduced, and the new cursor.
 *
 * There are no deletions. Aliases are inserts only: an alias never moves to another record.
 * Applying the plan and advancing the cursor must commit atomically with each other.
 */
export interface TranscriptChangePlan {
  readonly records: ReadonlyArray<TranscriptRecord>;
  readonly aliases: ReadonlyArray<TranscriptAlias>;
  readonly throughSeq: SequenceNumber;
}

/**
 * The interpretation boundary: a pure, synchronous function from a lookup, committed events and
 * a definition to a change plan.
 *
 * It throws `TranscriptProjectionError` when the stored projection is inconsistent with the log.
 * The adapter treats any throw as a recoverable projection failure of that version only.
 */
export type PlanTranscriptBatch = (
  lookup: TranscriptLookup,
  committed: ReadonlyArray<CommittedDurableEvent>,
  definition: TranscriptDefinition,
) => TranscriptChangePlan;

/** The stored projection cannot be advanced from the given lookup and batch. */
export class TranscriptProjectionError extends Schema.TaggedError<TranscriptProjectionError>()(
  "TranscriptProjectionError",
  { message: Schema.String },
) {}
