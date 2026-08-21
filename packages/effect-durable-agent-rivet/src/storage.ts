import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { CompactionSummaryArtifact } from "effect-durable-agent/domain/context-projection";
import { SequenceNumber, SessionId, durablePosition } from "effect-durable-agent/types/core";
import {
  DurableEventEnvelope,
  commandAdmittedEventType,
  contextRebasedEventType,
  decodeUnknownEDADurableEventSync,
  effectDurableAgentNamespace,
  summaryCreatedEventType,
} from "effect-durable-agent/types/events";
import {
  CommittedDurableEvent,
  EDASessionStore,
  EDASessionStoreError,
  type DurableAppendBatch,
  type DurableAppendEntry,
  type EDAReducerCheckpoint,
  type EDASessionStoreShape,
  type FindCommandAdmissionInput,
  type SaveReducerCheckpointInput,
} from "effect-durable-agent/services/session-store";
import {
  SinkCheckpointStore,
  type EDASinkName,
  type SinkCheckpointStoreShape,
} from "effect-durable-agent/services/sink-checkpoint-store";
import { annotateEdaSpan, committedBatchAttributes } from "effect-durable-agent/services/tracing";

/** The actor-local async SQLite surface used by the Rivet adapter. */
export interface RivetSqlStorage {
  readonly execute: <Row extends Record<string, unknown> = Record<string, unknown>>(
    query: string,
    ...args: ReadonlyArray<unknown>
  ) => Promise<Array<Row>>;
  readonly transaction: <A>(
    callback: (transaction: RivetSqlStorage) => Promise<A> | A,
    options?: { readonly timeout?: number },
  ) => Promise<A>;
}

/** Options for constructing the session-scoped Rivet event store. */
export interface RivetSessionStoreOptions {
  readonly sessionId: SessionId;
  readonly storage: RivetSqlStorage;
}

interface EventRow {
  readonly [key: string]: unknown;
  readonly seq: number;
  readonly event_json: string;
}

interface AdmissionRow {
  readonly [key: string]: unknown;
  readonly admitted_seq: number;
}

interface SummaryRow {
  readonly [key: string]: unknown;
  readonly payload_json: string;
}

interface ReducerCheckpointRow {
  readonly [key: string]: unknown;
  readonly reducer_name: string;
  readonly schema_version: number;
  readonly through_seq: number;
  readonly payload_json: string;
  readonly updated_at_ms: number;
}

interface SinkCheckpointRow {
  readonly [key: string]: unknown;
  readonly after_seq: number;
  readonly payload_json: string;
}

/** Rivet actor-local SQLite implementation of EDA's semantic event-store port. */
export class RivetSessionStore {
  /** Create all framework-owned tables. Safe to call on every actor wake. */
  static readonly migrate = (storage: RivetSqlStorage) => migrateRivetStorage(storage);

  /** Build one session-scoped store after applying idempotent migrations. */
  static readonly make = ({
    sessionId,
    storage,
  }: RivetSessionStoreOptions): Effect.Effect<EDASessionStoreShape, EDASessionStoreError> =>
    Effect.gen(function* () {
      yield* RivetSessionStore.migrate(storage);
      return makeRivetSessionStore(sessionId, storage);
    });

  /** Layer constructor used by the Rivet Actor runtime graph. */
  static readonly layer = (options: RivetSessionStoreOptions) =>
    Layer.effect(EDASessionStore, RivetSessionStore.make(options));
}

/** Rivet actor-local SQLite implementation of EDA's durable sink cursor port. */
export class RivetSinkCheckpointStore {
  static readonly migrate = (storage: RivetSqlStorage) => migrateRivetStorage(storage);

  static readonly make = (
    storage: RivetSqlStorage,
  ): Effect.Effect<SinkCheckpointStoreShape, EDASessionStoreError> =>
    Effect.gen(function* () {
      yield* RivetSinkCheckpointStore.migrate(storage);
      return makeRivetSinkCheckpointStore(storage);
    });

  static readonly layer = (storage: RivetSqlStorage) =>
    Layer.effect(SinkCheckpointStore, RivetSinkCheckpointStore.make(storage));
}

const migrateRivetStorage = (storage: RivetSqlStorage): Effect.Effect<void, EDASessionStoreError> =>
  tryStorage("migrating Rivet EDA storage", async () => {
    await storage.transaction(async (transaction) => {
      await transaction.execute(`
        CREATE TABLE IF NOT EXISTS _eda_schema_migrations (
          version INTEGER PRIMARY KEY,
          applied_at_ms INTEGER NOT NULL
        )
      `);
      await transaction.execute(`
        CREATE TABLE IF NOT EXISTS _eda_event_log (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          event_id TEXT NOT NULL UNIQUE,
          namespace TEXT NOT NULL,
          type TEXT NOT NULL,
          event_json TEXT NOT NULL
        )
      `);
      await transaction.execute(`
        CREATE INDEX IF NOT EXISTS _eda_event_log_namespace_type_seq_idx
          ON _eda_event_log(namespace, type, seq)
      `);
      await transaction.execute(`
        CREATE TABLE IF NOT EXISTS _eda_command_admissions (
          command_id TEXT PRIMARY KEY,
          idempotency_key TEXT,
          admitted_seq INTEGER NOT NULL UNIQUE
        )
      `);
      await transaction.execute(`
        CREATE UNIQUE INDEX IF NOT EXISTS _eda_command_admissions_idempotency_idx
          ON _eda_command_admissions(idempotency_key)
          WHERE idempotency_key IS NOT NULL
      `);
      await transaction.execute(`
        CREATE TABLE IF NOT EXISTS _eda_summary_artifacts (
          summary_id TEXT PRIMARY KEY,
          created_seq INTEGER NOT NULL UNIQUE,
          payload_json TEXT NOT NULL
        )
      `);
      await transaction.execute(`
        CREATE TABLE IF NOT EXISTS _eda_reducer_checkpoints (
          reducer_name TEXT PRIMARY KEY,
          schema_version INTEGER NOT NULL,
          through_seq INTEGER NOT NULL,
          payload_json TEXT NOT NULL,
          updated_at_ms INTEGER NOT NULL
        )
      `);
      await transaction.execute(`
        CREATE TABLE IF NOT EXISTS _eda_sink_cursors (
          sink_name TEXT PRIMARY KEY,
          after_seq INTEGER NOT NULL,
          payload_json TEXT NOT NULL
        )
      `);
      await transaction.execute(
        `INSERT OR IGNORE INTO _eda_schema_migrations (version, applied_at_ms) VALUES (?, ?)`,
        1,
        Date.now(),
      );
    });
  });

const makeRivetSessionStore = (
  sessionId: SessionId,
  storage: RivetSqlStorage,
): EDASessionStoreShape => {
  const append = (batch: DurableAppendBatch) =>
    tryStorage("appending Rivet EDA event batch", () =>
      storage.transaction(async (transaction) => {
        const committed: Array<CommittedDurableEvent> = [];
        for (const entry of batch.entries) {
          committed.push(await insertOrRead(transaction, sessionId, entry));
        }
        return committed;
      }),
    ).pipe(Effect.tap((committed) => annotateEdaSpan(committedBatchAttributes(committed))));

  return {
    append,
    eventsAfter: (afterSeq) =>
      Stream.unwrap(
        tryStorage("replaying Rivet EDA events", async () => {
          const rows = await storage.execute<EventRow>(
            `SELECT seq, event_json FROM _eda_event_log WHERE seq > ? ORDER BY seq ASC`,
            afterSeq,
          );
          return Stream.fromIterable(rows.map((row) => rowToCommitted(sessionId, row)));
        }),
      ),
    loadCommittedEventsBySeq: (seqs) =>
      tryStorage("loading Rivet EDA events by sequence", async () => {
        const unique = Array.from(new Set(seqs.map(Number))).sort((left, right) => left - right);
        if (unique.length === 0) {
          return [];
        }
        const placeholders = unique.map(() => "?").join(", ");
        const rows = await storage.execute<EventRow>(
          `SELECT seq, event_json FROM _eda_event_log WHERE seq IN (${placeholders}) ORDER BY seq ASC`,
          ...unique,
        );
        return rows.map((row) => rowToCommitted(sessionId, row));
      }),
    findCommandAdmission: (input) =>
      tryStorage("finding Rivet EDA command admission", async () => {
        const row = await findAdmission(storage, input);
        if (row === undefined) {
          return undefined;
        }
        const events = await storage.execute<EventRow>(
          `SELECT seq, event_json FROM _eda_event_log WHERE seq = ?`,
          row.admitted_seq,
        );
        const event = events[0];
        if (event === undefined) {
          throw new Error(`Admission points to missing event sequence ${row.admitted_seq}`);
        }
        return rowToCommitted(sessionId, event);
      }),
    loadSummaryArtifact: (summaryId) =>
      tryStorage("loading Rivet EDA summary", async () => {
        const rows = await storage.execute<SummaryRow>(
          `SELECT payload_json FROM _eda_summary_artifacts WHERE summary_id = ?`,
          summaryId,
        );
        const row = rows[0];
        return row === undefined
          ? undefined
          : Schema.decodeUnknownSync(CompactionSummaryArtifact)(JSON.parse(row.payload_json));
      }),
    loadReducerCheckpoint: (name) =>
      tryStorage(`loading Rivet EDA reducer checkpoint ${name}`, async () => {
        const rows = await storage.execute<ReducerCheckpointRow>(
          `
            SELECT reducer_name, schema_version, through_seq, payload_json, updated_at_ms
            FROM _eda_reducer_checkpoints
            WHERE reducer_name = ?
          `,
          name,
        );
        return rows[0] === undefined ? undefined : rowToReducerCheckpoint(rows[0]);
      }),
    saveReducerCheckpoint: (checkpoint) =>
      tryStorage(`saving Rivet EDA reducer checkpoint ${checkpoint.name}`, () =>
        writeReducerCheckpoint(storage, checkpoint),
      ),
    saveReducerCheckpoints: (checkpoints) =>
      tryStorage("saving Rivet EDA reducer checkpoints", () =>
        storage.transaction(async (transaction) => {
          for (const checkpoint of checkpoints) {
            await writeReducerCheckpoint(transaction, checkpoint);
          }
        }),
      ),
  };
};

const insertOrRead = async (
  storage: RivetSqlStorage,
  sessionId: SessionId,
  entry: DurableAppendEntry,
): Promise<CommittedDurableEvent> => {
  const { event } = entry;
  if (event.sessionId !== sessionId) {
    throw new Error(`RivetSessionStore is scoped to ${sessionId}; received ${event.sessionId}`);
  }

  const existing = await storage.execute<EventRow>(
    `SELECT seq, event_json FROM _eda_event_log WHERE event_id = ?`,
    event.eventId,
  );
  if (existing[0] !== undefined) {
    return rowToCommitted(sessionId, existing[0]);
  }

  const encoded = Schema.encodeSync(DurableEventEnvelope)(event);
  await storage.execute(
    `
      INSERT INTO _eda_event_log (event_id, namespace, type, event_json)
      VALUES (?, ?, ?, ?)
    `,
    event.eventId,
    event.namespace,
    event.type,
    stringifyJson(encoded, "_eda_event_log.event_json"),
  );
  const inserted = await storage.execute<EventRow>(
    `SELECT seq, event_json FROM _eda_event_log WHERE event_id = ?`,
    event.eventId,
  );
  const row = inserted[0];
  if (row === undefined) {
    throw new Error(`Inserted event ${event.eventId} could not be read back`);
  }
  const seq = SequenceNumber.make(row.seq);
  await writeEventProjections(storage, event, seq);
  return rowToCommitted(sessionId, row);
};

const writeEventProjections = async (
  storage: RivetSqlStorage,
  event: DurableEventEnvelope,
  seq: SequenceNumber,
): Promise<void> => {
  if (event.namespace !== effectDurableAgentNamespace) {
    return;
  }

  if (event.type === commandAdmittedEventType) {
    const decoded = decodeUnknownEDADurableEventSync(event);
    if (decoded.type !== commandAdmittedEventType) {
      throw new Error("Command admission failed to decode");
    }
    await storage.execute(
      `
        INSERT INTO _eda_command_admissions (command_id, idempotency_key, admitted_seq)
        VALUES (?, ?, ?)
      `,
      decoded.payload.command.commandId,
      decoded.payload.command.idempotencyKey ?? null,
      seq,
    );
  }

  if (event.type === summaryCreatedEventType) {
    const decoded = decodeUnknownEDADurableEventSync(event);
    if (decoded.type !== summaryCreatedEventType) {
      throw new Error("Summary creation failed to decode");
    }
    const summary = Schema.decodeUnknownSync(CompactionSummaryArtifact)(decoded.payload.summary);
    if (summary.summaryId !== decoded.payload.summaryId) {
      throw new Error("SummaryCreated summaryId does not match summary payload");
    }
    if (summary.compactionId !== decoded.payload.compactionId) {
      throw new Error("SummaryCreated compactionId does not match summary payload");
    }
    await storage.execute(
      `
        INSERT INTO _eda_summary_artifacts (summary_id, created_seq, payload_json)
        VALUES (?, ?, ?)
        ON CONFLICT(summary_id) DO UPDATE SET
          created_seq = excluded.created_seq,
          payload_json = excluded.payload_json
      `,
      summary.summaryId,
      seq,
      stringifyJson(Schema.encodeSync(CompactionSummaryArtifact)(summary), "summary artifact"),
    );
  }

  if (event.type === contextRebasedEventType) {
    const decoded = decodeUnknownEDADurableEventSync(event);
    if (decoded.type !== contextRebasedEventType) {
      throw new Error("Context rebase failed to decode");
    }
    const rows = await storage.execute<SummaryRow>(
      `SELECT payload_json FROM _eda_summary_artifacts WHERE summary_id = ?`,
      decoded.payload.summaryId,
    );
    const row = rows[0];
    if (row === undefined) {
      throw new Error(`ContextRebased references unknown summaryId ${decoded.payload.summaryId}`);
    }
    const summary = Schema.decodeUnknownSync(CompactionSummaryArtifact)(
      JSON.parse(row.payload_json),
    );
    if (
      summary.compactionId !== decoded.payload.compactionId ||
      summary.retainedFromContextSeq !== decoded.payload.retainedFromContextSeq
    ) {
      throw new Error("ContextRebased does not match its summary artifact");
    }
  }
};

const findAdmission = async (
  storage: RivetSqlStorage,
  input: FindCommandAdmissionInput,
): Promise<AdmissionRow | undefined> => {
  if (input.idempotencyKey !== undefined) {
    const rows = await storage.execute<AdmissionRow>(
      `SELECT admitted_seq FROM _eda_command_admissions WHERE idempotency_key = ?`,
      input.idempotencyKey,
    );
    if (rows[0] !== undefined) {
      return rows[0];
    }
  }
  if (input.commandId === undefined) {
    return undefined;
  }
  const rows = await storage.execute<AdmissionRow>(
    `SELECT admitted_seq FROM _eda_command_admissions WHERE command_id = ?`,
    input.commandId,
  );
  return rows[0];
};

const rowToCommitted = (sessionId: SessionId, row: EventRow): CommittedDurableEvent => {
  const envelope = Schema.decodeUnknownSync(DurableEventEnvelope)(JSON.parse(row.event_json));
  if (envelope.sessionId !== sessionId) {
    throw new Error(`Stored event belongs to ${envelope.sessionId}; expected ${sessionId}`);
  }
  const event =
    envelope.namespace === effectDurableAgentNamespace
      ? decodeUnknownEDADurableEventSync(envelope)
      : envelope;
  return CommittedDurableEvent.make({
    position: durablePosition(SequenceNumber.make(row.seq)),
    event,
  });
};

const rowToReducerCheckpoint = (row: ReducerCheckpointRow): EDAReducerCheckpoint => ({
  name: row.reducer_name,
  schemaVersion: row.schema_version,
  throughSeq: SequenceNumber.make(row.through_seq),
  payload: JSON.parse(row.payload_json),
  updatedAtMs: row.updated_at_ms,
});

const writeReducerCheckpoint = async (
  storage: RivetSqlStorage,
  checkpoint: SaveReducerCheckpointInput,
): Promise<void> => {
  await storage.execute(
    `
      INSERT INTO _eda_reducer_checkpoints (
        reducer_name, schema_version, through_seq, payload_json, updated_at_ms
      ) VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(reducer_name) DO UPDATE SET
        schema_version = excluded.schema_version,
        through_seq = excluded.through_seq,
        payload_json = excluded.payload_json,
        updated_at_ms = excluded.updated_at_ms
    `,
    checkpoint.name,
    checkpoint.schemaVersion,
    checkpoint.throughSeq,
    stringifyJson(checkpoint.payload, "reducer checkpoint"),
    checkpoint.updatedAtMs,
  );
};

const makeRivetSinkCheckpointStore = (storage: RivetSqlStorage): SinkCheckpointStoreShape => ({
  load: (sinkName) =>
    tryStorage("loading Rivet EDA sink checkpoint", async () => {
      const rows = await storage.execute<SinkCheckpointRow>(
        `SELECT after_seq, payload_json FROM _eda_sink_cursors WHERE sink_name = ?`,
        sinkName,
      );
      const row = rows[0];
      return row === undefined
        ? { afterSeq: SequenceNumber.make(0), payload: undefined }
        : {
            afterSeq: SequenceNumber.make(row.after_seq),
            payload: JSON.parse(row.payload_json),
          };
    }),
  saveState: (sinkName: EDASinkName, payload: unknown) =>
    tryStorage("saving Rivet EDA sink checkpoint state", async () => {
      await storage.execute(
        `
          INSERT INTO _eda_sink_cursors (sink_name, after_seq, payload_json)
          VALUES (?, 0, ?)
          ON CONFLICT(sink_name) DO UPDATE SET payload_json = excluded.payload_json
        `,
        sinkName,
        stringifyJson(payload, "sink checkpoint state"),
      );
    }),
  commit: (sinkName: EDASinkName, afterSeq: SequenceNumber, payload: unknown | undefined) =>
    tryStorage("committing Rivet EDA sink checkpoint", async () => {
      await storage.execute(
        `
          INSERT INTO _eda_sink_cursors (sink_name, after_seq, payload_json)
          VALUES (?, ?, ?)
          ON CONFLICT(sink_name) DO UPDATE SET
            after_seq = excluded.after_seq,
            payload_json = excluded.payload_json
        `,
        sinkName,
        afterSeq,
        stringifyJson(payload ?? { updatedAtMs: Date.now() }, "sink checkpoint"),
      );
    }),
});

/** Remove all EDA-owned rows while preserving the warm Rivet actor instance. */
export const clearRivetSessionStorage = (
  storage: RivetSqlStorage,
): Effect.Effect<void, EDASessionStoreError> =>
  tryStorage("clearing Rivet EDA session storage", () =>
    storage.transaction(async (transaction) => {
      await transaction.execute(`DELETE FROM _eda_sink_cursors`);
      await transaction.execute(`DELETE FROM _eda_reducer_checkpoints`);
      await transaction.execute(`DELETE FROM _eda_summary_artifacts`);
      await transaction.execute(`DELETE FROM _eda_command_admissions`);
      await transaction.execute(`DELETE FROM _eda_event_log`);
      await transaction.execute(`DELETE FROM sqlite_sequence WHERE name = '_eda_event_log'`);
    }),
  );

const tryStorage = <A>(
  operation: string,
  evaluate: () => Promise<A>,
): Effect.Effect<A, EDASessionStoreError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) =>
      new EDASessionStoreError({
        message:
          cause instanceof Error
            ? `${operation}: ${cause.message}`
            : `${operation}: ${String(cause)}`,
      }),
  });

const stringifyJson = (value: unknown, column: string): string => {
  const encoded = JSON.stringify(value);
  if (encoded === undefined) {
    throw new Error(`${column} cannot encode undefined`);
  }
  return encoded;
};
