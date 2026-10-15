import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { IdGenerator } from "effect-durable-agent/services/id-generator";
import { SequenceNumber } from "effect-durable-agent/types/core";
import {
  TranscriptAliasNamespace,
  TranscriptLogIncarnation,
  TranscriptRecordFromEnvelope,
  TranscriptVersion,
  isPendingTranscriptUserRecord,
  isTopLevelTranscriptRecord,
  type TranscriptChangePlan,
  type TranscriptLookup,
  type TranscriptOrderKey,
  type TranscriptRecord,
  TranscriptRecordId,
  type TranscriptTopLevelRecord,
  type TranscriptToolRecord,
  type TranscriptUserRecord,
  type TranscriptVersionMeta,
  type TranscriptVersionStatus,
  type TranscriptWindowSource,
} from "effect-durable-agent/transcript";
import { stringifyJsonForColumn } from "./durable-object-json";
import type { DurableObjectSqlStorage } from "./durable-object-storage";

/**
 * SQL layout of the transcript projection (design §5.1), created by the store's forward migration.
 *
 * Rows are keyed by a **slot**: one stored instance of one transcript version. A version is built
 * in a fresh slot, so a retired slot that is still being deleted in bounded slices never collides
 * with a later slot of the same version. At most one slot is `active` and at most one is
 * `building`; `retired` slots are garbage awaiting cleanup and are never read.
 *
 * `_eda_log_incarnation` holds the identity of this physical event log (§8); `_eda_alarm_wakeups`
 * holds each alarm owner's durable wakeup (see `durable-object-alarms.ts`).
 */
export const transcriptTableStatements: ReadonlyArray<string> = [
  `CREATE TABLE IF NOT EXISTS _eda_transcript_meta (
    slot INTEGER PRIMARY KEY AUTOINCREMENT,
    version TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('active', 'building', 'retired')),
    cursor INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ok', 'failed')),
    failed_at_seq INTEGER,
    error TEXT
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS _eda_transcript_meta_role_idx
    ON _eda_transcript_meta(role) WHERE role IN ('active', 'building')`,
  `CREATE TABLE IF NOT EXISTS _eda_transcript_records (
    slot INTEGER NOT NULL,
    id TEXT NOT NULL,
    kind TEXT NOT NULL,
    anchor_seq INTEGER,
    ordinal INTEGER,
    top_level INTEGER NOT NULL,
    pending INTEGER NOT NULL,
    assistant_id TEXT,
    updated_seq INTEGER NOT NULL,
    envelope_json TEXT NOT NULL,
    PRIMARY KEY (slot, id)
  )`,
  // Transcript order over top-level records, for keyset paging in either direction.
  `CREATE INDEX IF NOT EXISTS _eda_transcript_records_order_idx
    ON _eda_transcript_records(slot, top_level, anchor_seq, ordinal, id)`,
  // Ordinal allocation among every positioned record of one anchor event.
  `CREATE INDEX IF NOT EXISTS _eda_transcript_records_anchor_idx
    ON _eda_transcript_records(slot, anchor_seq, ordinal) WHERE anchor_seq IS NOT NULL`,
  // Changes since a cut, for delivery that replaces what changed after `throughSeq`.
  `CREATE INDEX IF NOT EXISTS _eda_transcript_records_updated_idx
    ON _eda_transcript_records(slot, updated_seq)`,
  `CREATE INDEX IF NOT EXISTS _eda_transcript_records_pending_idx
    ON _eda_transcript_records(slot, id) WHERE pending = 1`,
  // The typed dependency lookup: tools of one assistant.
  `CREATE INDEX IF NOT EXISTS _eda_transcript_records_tool_idx
    ON _eda_transcript_records(slot, assistant_id) WHERE assistant_id IS NOT NULL`,
  `CREATE TABLE IF NOT EXISTS _eda_transcript_aliases (
    slot INTEGER NOT NULL,
    namespace TEXT NOT NULL,
    key TEXT NOT NULL,
    record_id TEXT NOT NULL,
    PRIMARY KEY (slot, namespace, key)
  )`,
  `CREATE TABLE IF NOT EXISTS _eda_log_incarnation (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    incarnation TEXT NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS _eda_alarm_wakeups (
    owner TEXT PRIMARY KEY,
    at_ms INTEGER NOT NULL
  )`,
];

/** Role of a stored slot. Only `active` and `building` are visible through the port. */
export type TranscriptSlotRole = "active" | "building" | "retired";

/** One `_eda_transcript_meta` row, decoded. */
export interface TranscriptSlot {
  readonly slot: number;
  readonly version: TranscriptVersion;
  readonly role: TranscriptSlotRole;
  readonly cursor: SequenceNumber;
  readonly status: TranscriptVersionStatus;
}

interface TranscriptMetaRow {
  readonly slot: number;
  readonly version: string;
  readonly role: string;
  readonly cursor: number;
  readonly status: string;
  readonly failed_at_seq: number | null;
  readonly error: string | null;
}

const decodeVersion = Schema.decodeUnknownSync(TranscriptVersion);

const decodeSlot = (row: TranscriptMetaRow): TranscriptSlot => {
  if (row.role !== "active" && row.role !== "building" && row.role !== "retired") {
    throw new Error(`transcript slot ${row.slot} has unknown role ${row.role}`);
  }
  if (row.status === "failed" && (row.failed_at_seq === null || row.error === null)) {
    throw new Error(`failed transcript slot ${row.slot} has no failure seq or error`);
  }
  return {
    slot: row.slot,
    version: decodeVersion(row.version),
    role: row.role,
    cursor: SequenceNumber.make(row.cursor),
    status:
      row.status === "failed"
        ? {
            _tag: "Failed",
            failedAtSeq: SequenceNumber.make(row.failed_at_seq ?? 0),
            error: row.error ?? "",
          }
        : { _tag: "Ok" },
  };
};

/** Every stored slot, in creation order. */
export const readTranscriptSlots = (sql: DurableObjectSqlStorage): ReadonlyArray<TranscriptSlot> =>
  sql
    .exec<TranscriptMetaRow>(
      "SELECT slot, version, role, cursor, status, failed_at_seq, error FROM _eda_transcript_meta ORDER BY slot",
    )
    .toArray()
    .map(decodeSlot);

/** The port's view of a slot; retired slots have none. */
export const transcriptVersionMetaOf = (slot: TranscriptSlot): TranscriptVersionMeta | undefined =>
  slot.role === "retired"
    ? undefined
    : { version: slot.version, role: slot.role, cursor: slot.cursor, status: slot.status };

export const insertTranscriptSlot = (
  sql: DurableObjectSqlStorage,
  version: TranscriptVersion,
  role: "active" | "building",
): void => {
  sql.exec(
    "INSERT INTO _eda_transcript_meta (version, role, cursor, status) VALUES (?, ?, 0, 'ok')",
    version,
    role,
  );
};

export const setTranscriptSlotRole = (
  sql: DurableObjectSqlStorage,
  slot: number,
  role: TranscriptSlotRole,
): void => {
  sql.exec("UPDATE _eda_transcript_meta SET role = ? WHERE slot = ?", role, slot);
};

/** Freeze a slot at its unchanged cursor. A failed slot is never written again. */
export const markTranscriptSlotFailed = (
  sql: DurableObjectSqlStorage,
  slot: number,
  failedAtSeq: SequenceNumber,
  error: string,
): void => {
  sql.exec(
    "UPDATE _eda_transcript_meta SET status = 'failed', failed_at_seq = ?, error = ? WHERE slot = ?",
    failedAtSeq,
    error,
    slot,
  );
};

/**
 * Delete up to `limit` rows of a retired slot, records first, and the slot itself once empty.
 * Returns the number of rows deleted.
 */
export const deleteRetiredTranscriptRows = (
  sql: DurableObjectSqlStorage,
  slot: number,
  limit: number,
): number => {
  const records = sql
    .exec<{ readonly deleted: number }>(
      `DELETE FROM _eda_transcript_records WHERE rowid IN (
        SELECT rowid FROM _eda_transcript_records WHERE slot = ? LIMIT ?
      ) RETURNING 1 AS deleted`,
      slot,
      limit,
    )
    .toArray().length;
  const aliases =
    records >= limit
      ? 0
      : sql
          .exec<{ readonly deleted: number }>(
            `DELETE FROM _eda_transcript_aliases WHERE rowid IN (
              SELECT rowid FROM _eda_transcript_aliases WHERE slot = ? LIMIT ?
            ) RETURNING 1 AS deleted`,
            slot,
            limit - records,
          )
          .toArray().length;
  if (records + aliases < limit) {
    sql.exec("DELETE FROM _eda_transcript_meta WHERE slot = ? AND role = 'retired'", slot);
  }
  return records + aliases;
};

// ---------------------------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------------------------

interface TranscriptRecordRow {
  readonly envelope_json: string;
}

const encodeEnvelope = Schema.encodeSync(TranscriptRecordFromEnvelope);
const decodeEnvelope = Schema.decodeUnknownSync(TranscriptRecordFromEnvelope);

/** A stored record row this release cannot decode. */
export class TranscriptRowDecodeError extends Error {
  override readonly name = "TranscriptRowDecodeError";
}

/**
 * Decode one stored row. A row this release cannot decode throws; maintenance treats that as a
 * projection failure and reads as an unreadable window, never as a wrong view.
 */
const decodeRecordRow = (row: TranscriptRecordRow): TranscriptRecord => {
  try {
    return decodeEnvelope(JSON.parse(row.envelope_json));
  } catch (cause) {
    throw new TranscriptRowDecodeError(
      `undecodable transcript row: ${cause instanceof Error ? cause.message : String(cause)}`,
      { cause },
    );
  }
};

const positionOf = (record: TranscriptRecord) =>
  "position" in record ? record.position : undefined;

/**
 * A change plan already encoded into rows: every envelope serialized and size-checked. Encoding
 * is where a plan can still fail recoverably, so it happens before any write.
 */
export interface EncodedTranscriptPlan {
  readonly records: ReadonlyArray<ReadonlyArray<string | number | null>>;
  readonly aliases: ReadonlyArray<TranscriptChangePlan["aliases"][number]>;
  readonly throughSeq: SequenceNumber;
}

export const encodeTranscriptPlan = (plan: TranscriptChangePlan): EncodedTranscriptPlan => ({
  records: plan.records.map((record) => {
    const position = positionOf(record);
    return [
      record.id,
      record.kind,
      position?.anchorSeq ?? null,
      position?.ordinal ?? null,
      isTopLevelTranscriptRecord(record) ? 1 : 0,
      isPendingTranscriptUserRecord(record) ? 1 : 0,
      record.kind === "tool" ? record.assistantId : null,
      record.updatedSeq,
      stringifyJsonForColumn(encodeEnvelope(record), "_eda_transcript_records.envelope_json"),
    ];
  }),
  aliases: plan.aliases,
  throughSeq: plan.throughSeq,
});

/** Write an encoded plan to a slot and advance its cursor; the caller owns the transaction. */
export const writeTranscriptPlan = (
  sql: DurableObjectSqlStorage,
  slot: number,
  plan: EncodedTranscriptPlan,
): void => {
  for (const row of plan.records) {
    sql.exec(
      `INSERT INTO _eda_transcript_records (
        slot, id, kind, anchor_seq, ordinal, top_level, pending, assistant_id, updated_seq, envelope_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(slot, id) DO UPDATE SET
        kind = excluded.kind,
        anchor_seq = excluded.anchor_seq,
        ordinal = excluded.ordinal,
        top_level = excluded.top_level,
        pending = excluded.pending,
        assistant_id = excluded.assistant_id,
        updated_seq = excluded.updated_seq,
        envelope_json = excluded.envelope_json`,
      slot,
      ...row,
    );
  }
  for (const alias of plan.aliases) {
    // Aliases are inserts only: an alias never moves to another record.
    sql.exec(
      `INSERT INTO _eda_transcript_aliases (slot, namespace, key, record_id) VALUES (?, ?, ?, ?)
      ON CONFLICT(slot, namespace, key) DO NOTHING`,
      slot,
      alias.namespace,
      alias.key,
      alias.recordId,
    );
  }
  sql.exec("UPDATE _eda_transcript_meta SET cursor = ? WHERE slot = ?", plan.throughSeq, slot);
};

const readRecord = (
  sql: DurableObjectSqlStorage,
  slot: number,
  id: string,
): TranscriptRecord | undefined => {
  const row = sql
    .exec<TranscriptRecordRow>(
      "SELECT envelope_json FROM _eda_transcript_records WHERE slot = ? AND id = ?",
      slot,
      id,
    )
    .toArray()[0];
  return row === undefined ? undefined : decodeRecordRow(row);
};

const decodeRecordId = Schema.decodeUnknownSync(TranscriptRecordId);

const readAlias = (
  sql: DurableObjectSqlStorage,
  slot: number,
  namespace: TranscriptAliasNamespace,
  key: string,
): TranscriptRecordId | undefined => {
  const row = sql
    .exec<{ readonly record_id: string }>(
      "SELECT record_id FROM _eda_transcript_aliases WHERE slot = ? AND namespace = ? AND key = ?",
      slot,
      namespace,
      key,
    )
    .toArray()[0];
  return row === undefined ? undefined : decodeRecordId(row.record_id);
};

/** Indexed synchronous reads of one slot as committed so far in this transaction. */
export const makeTranscriptLookup = (
  sql: DurableObjectSqlStorage,
  slot: TranscriptSlot,
): TranscriptLookup => ({
  cursor: slot.cursor,
  record: (id) => readRecord(sql, slot.slot, id),
  alias: (namespace, key) => readAlias(sql, slot.slot, namespace, key),
  nextOrdinal: (anchorSeq) =>
    sql
      .exec<{ readonly next: number | null }>(
        `SELECT MAX(ordinal) + 1 AS next FROM _eda_transcript_records
        WHERE slot = ? AND anchor_seq = ?`,
        slot.slot,
        anchorSeq,
      )
      .one().next ?? 0,
});

const topLevel = (record: TranscriptRecord): TranscriptTopLevelRecord => {
  if (!isTopLevelTranscriptRecord(record)) {
    throw new TranscriptRowDecodeError(
      `transcript record ${record.id} is indexed as top-level but is not`,
    );
  }
  return record;
};

/**
 * Window reads over one served slot. Every method is a synchronous indexed query, so a window
 * assembled from them inside one synchronous call is one cut. Record ids are ASCII, so SQLite's
 * binary text order equals `TranscriptOrder`'s code-unit order.
 */
export const makeTranscriptWindowSource = (
  sql: DurableObjectSqlStorage,
  incarnation: TranscriptLogIncarnation,
  slot: TranscriptSlot,
  carried: boolean,
): TranscriptWindowSource => {
  const meta = transcriptVersionMetaOf(slot);
  if (meta === undefined) throw new Error(`retired transcript slot ${slot.slot} is not servable`);
  const topLevelPage = (
    key: TranscriptOrderKey | undefined,
    limit: number,
    inclusive: boolean,
    direction: "before" | "after",
  ): ReadonlyArray<TranscriptTopLevelRecord> => {
    const comparison = direction === "before" ? (inclusive ? "<=" : "<") : inclusive ? ">=" : ">";
    const order = direction === "before" ? "DESC" : "ASC";
    const keyClause =
      key === undefined ? "" : `AND (anchor_seq, ordinal, id) ${comparison} (?, ?, ?)`;
    const keyBindings = key === undefined ? [] : [key.anchorSeq, key.ordinal, key.recordId];
    return sql
      .exec<TranscriptRecordRow>(
        `SELECT envelope_json FROM _eda_transcript_records
        WHERE slot = ? AND top_level = 1 ${keyClause}
        ORDER BY anchor_seq ${order}, ordinal ${order}, id ${order}
        LIMIT ?`,
        slot.slot,
        ...keyBindings,
        limit,
      )
      .toArray()
      .map((row) => topLevel(decodeRecordRow(row)));
  };
  return {
    incarnation,
    meta,
    carried,
    topLevelBefore: (key, limit, inclusive) => topLevelPage(key, limit, inclusive, "before"),
    topLevelAfter: (key, limit, inclusive) => topLevelPage(key, limit, inclusive, "after"),
    record: (id) => readRecord(sql, slot.slot, id),
    alias: (namespace, key) => readAlias(sql, slot.slot, namespace, key),
    toolsOf: (assistantId) =>
      sql
        .exec<TranscriptRecordRow>(
          "SELECT envelope_json FROM _eda_transcript_records WHERE slot = ? AND assistant_id = ?",
          slot.slot,
          assistantId,
        )
        .toArray()
        .flatMap((row): ReadonlyArray<TranscriptToolRecord> => {
          const record = decodeRecordRow(row);
          return record.kind === "tool" ? [record] : [];
        }),
    pendingUsers: () =>
      sql
        .exec<TranscriptRecordRow>(
          "SELECT envelope_json FROM _eda_transcript_records WHERE slot = ? AND pending = 1",
          slot.slot,
        )
        .toArray()
        .flatMap((row): ReadonlyArray<TranscriptUserRecord> => {
          const record = decodeRecordRow(row);
          return record.kind === "user" ? [record] : [];
        }),
  };
};

// ---------------------------------------------------------------------------------------------
// Log incarnation
// ---------------------------------------------------------------------------------------------

/**
 * A host's existing synchronous key/value incarnation to adopt (Gia: `gia.log_incarnation` in the
 * Durable Object's `ctx.storage.kv`). The adapter adopts the host's value when one exists and
 * otherwise writes the value it mints there too, so host and adapter always name the same log.
 */
export interface AdoptedLogIncarnation {
  readonly key: string;
  readonly kv: {
    readonly get: (key: string) => unknown;
    readonly put: (key: string, value: string) => void;
  };
}

const mintUuidV7 = (): string =>
  Effect.runSync(
    Effect.gen(function* () {
      const ids = yield* IdGenerator;
      return yield* ids.makeCorrelationId();
    }).pipe(Effect.provide(IdGenerator.Live)),
  );

/**
 * Read this log's incarnation, minting one on first need. The caller runs it inside a
 * `transactionSync`, so read-or-mint is atomic; storage wipes delete the row and so mint anew.
 */
export const readOrMintLogIncarnation = (
  sql: DurableObjectSqlStorage,
  adopt: AdoptedLogIncarnation | undefined,
): TranscriptLogIncarnation => {
  const row = sql
    .exec<{ readonly incarnation: string }>(
      "SELECT incarnation FROM _eda_log_incarnation WHERE id = 1",
    )
    .toArray()[0];
  if (row !== undefined) return TranscriptLogIncarnation.make(row.incarnation);
  const adopted = adopt?.kv.get(adopt.key);
  const incarnation = typeof adopted === "string" && adopted.length > 0 ? adopted : mintUuidV7();
  if (adopt !== undefined && adopted !== incarnation) adopt.kv.put(adopt.key, incarnation);
  sql.exec("INSERT INTO _eda_log_incarnation (id, incarnation) VALUES (1, ?)", incarnation);
  return TranscriptLogIncarnation.make(incarnation);
};
