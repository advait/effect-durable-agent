import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { SequenceNumber, SessionId, SummaryId, durablePosition } from "../types/core";
import { CompactionSummaryArtifact } from "../domain/context-projection";
import {
  DurableEventEnvelope,
  assistantMessageCommittedEventType,
  assistantMessageImportedEventType,
  assistantPartialCommittedEventType,
  commandAdmittedEventType,
  commandCancelledEventType,
  commandCompletedEventType,
  commandFailedEventType,
  commandStartedEventType,
  contextRebasedEventType,
  decodeUnknownEDADurableEventSync,
  effectDurableAgentNamespace,
  steeringMessageQueuedEventType,
  summaryCreatedEventType,
  systemMessageCommittedEventType,
  userMessageCommittedEventType,
} from "../types/events";
import {
  CommittedDurableEvent,
  DurableAppendBatch,
  DurableAppendEntry,
  EDASessionStore,
  EDASessionStoreError,
  type EDAReducerCheckpoint,
  type EDASessionStoreShape,
  type SaveReducerCheckpointInput,
  durableObjectSerializedJsonHardCapBytes,
} from "../services/session-store";
import { annotateEdaSpan, committedBatchAttributes } from "../services/tracing";
import type {
  DurableObjectSessionStorage,
  DurableObjectSessionStoreOptions,
  DurableObjectSqlStorage,
} from "./durable-object-storage";

interface DurableObjectEventRow {
  readonly seq: number;
  readonly event_id: string;
  readonly namespace: string;
  readonly type: string;
  readonly schema_version: number;
  readonly created_at_ms: number;
  readonly trace_json: string;
  readonly fact_json: string;
}

interface DurableObjectEventHeadRow {
  readonly head: number | null;
}

interface DurableObjectEventPageCursor {
  readonly afterSeq: SequenceNumber;
  readonly throughSeq: SequenceNumber | undefined;
}

interface DurableObjectMigrationRow {
  readonly version: number | null;
}

interface DurableObjectMessagePayloadRow {
  readonly context_seq: number;
  readonly payload_json: string;
}

interface DurableObjectSummaryPayloadRow {
  readonly payload_json: string;
}

interface DurableObjectCommandLookupRow {
  readonly admitted_seq: number;
}

interface DurableObjectCommandInputPayloadRow {
  readonly payload_json: string;
}

interface DurableObjectReducerCheckpointRow {
  readonly reducer_name: string;
  readonly schema_version: number;
  readonly through_seq: number;
  readonly payload_json: string;
  readonly updated_at_ms: number;
}

/** Single squashed schema version for the current pre-release DO store. */
const durableObjectSessionStoreSchemaVersion = 1;
const jsonEncoder = new TextEncoder();

/**
 * Bound one synchronous DO SQLite event-log read/decode to a small CPU and memory slice.
 *
 * A single logical event can already approach the per-row JSON hard cap, and logical decode may
 * touch sidecar rows. Sixteen rows keeps worst-case page memory materially below isolate limits
 * while still amortizing SQL cursor setup for normal small framework events. Revisit with page
 * duration/byte telemetry before increasing.
 */
const durableObjectEventReadPageRows = 16;

/**
 * Durable Object SQLite-backed implementation of the semantic `EDASessionStore`.
 *
 * This adapter is the only place EDA framework storage code touches DO SQL. It
 * translates domain append intent into synchronous SQLite writes while keeping
 * `SessionState` and runners SQL-agnostic.
 */
export class DurableObjectSessionStore {
  /** Run the single squashed framework migration for the current pre-release schema. */
  static readonly migrate = (storage: DurableObjectSessionStorage) =>
    migrateDurableObjectSessionStore(storage);

  /** Build a session-scoped store after migrations have completed. */
  static readonly make = ({
    sessionId,
    storage,
  }: DurableObjectSessionStoreOptions): Effect.Effect<EDASessionStoreShape, EDASessionStoreError> =>
    Effect.gen(function* () {
      yield* DurableObjectSessionStore.migrate(storage);
      return makeDurableObjectSessionStore(sessionId, storage);
    });

  /** Layer constructor used by the Durable Object host runtime graph. */
  static readonly layer = (options: DurableObjectSessionStoreOptions) =>
    Layer.effect(EDASessionStore, DurableObjectSessionStore.make(options));
}

/**
 * Create all framework-owned tables in one idempotent startup step.
 *
 * The project is still pre-release, so this remains a squashed version-1 schema
 * rather than a chain of compatibility migrations.
 */
const migrateDurableObjectSessionStore = (storage: DurableObjectSessionStorage) =>
  Effect.try({
    try: () =>
      storage.transactionSync(() => {
        const { sql } = storage;
        sql.exec(`
          CREATE TABLE IF NOT EXISTS _eda_schema_migrations (
            id INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT (datetime('now'))
          )
        `);

        const currentVersion =
          sql
            .exec<DurableObjectMigrationRow>(
              "SELECT COALESCE(MAX(id), 0) AS version FROM _eda_schema_migrations",
            )
            .one().version ?? 0;

        if (currentVersion < durableObjectSessionStoreSchemaVersion) {
          sql.exec(`
            CREATE TABLE IF NOT EXISTS _eda_event_log (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              event_id TEXT NOT NULL UNIQUE,
              namespace TEXT NOT NULL,
              type TEXT NOT NULL,
              schema_version INTEGER NOT NULL,
              created_at_ms INTEGER NOT NULL,
              trace_json TEXT NOT NULL,
              fact_json TEXT NOT NULL
            )
          `);
          sql.exec(`
            CREATE INDEX IF NOT EXISTS _eda_event_log_namespace_type_seq_idx
              ON _eda_event_log(namespace, type, seq)
          `);

          sql.exec(`
            CREATE TABLE IF NOT EXISTS _eda_command_state (
              command_id TEXT PRIMARY KEY,
              admitted_seq INTEGER NOT NULL,
              status TEXT NOT NULL,
              idempotency_key TEXT,
              payload_json TEXT NOT NULL
            )
          `);
          sql.exec(`
            CREATE TABLE IF NOT EXISTS _eda_command_inputs (
              command_id TEXT PRIMARY KEY,
              admitted_seq INTEGER NOT NULL UNIQUE,
              payload_json TEXT NOT NULL
            )
          `);
          sql.exec(`
            CREATE INDEX IF NOT EXISTS _eda_command_state_status_seq_idx
              ON _eda_command_state(status, admitted_seq)
          `);
          sql.exec(`
            CREATE UNIQUE INDEX IF NOT EXISTS _eda_command_state_idempotency_idx
              ON _eda_command_state(idempotency_key) WHERE idempotency_key IS NOT NULL
          `);
          sql.exec(`
            CREATE TABLE IF NOT EXISTS _eda_context_messages (
              message_id TEXT PRIMARY KEY,
              context_seq INTEGER NOT NULL UNIQUE,
              payload_json TEXT NOT NULL
            )
          `);
          sql.exec(`
            CREATE INDEX IF NOT EXISTS _eda_context_messages_context_seq_idx
              ON _eda_context_messages(context_seq)
          `);
          sql.exec(`
            CREATE TABLE IF NOT EXISTS _eda_context_summaries (
              summary_id TEXT PRIMARY KEY,
              created_seq INTEGER NOT NULL UNIQUE,
              payload_json TEXT NOT NULL
            )
          `);
          sql.exec(`
            CREATE INDEX IF NOT EXISTS _eda_context_summaries_created_seq_idx
              ON _eda_context_summaries(created_seq)
          `);
          sql.exec(`
            CREATE TABLE IF NOT EXISTS _eda_reducer_checkpoints (
              reducer_name TEXT PRIMARY KEY,
              schema_version INTEGER NOT NULL,
              through_seq INTEGER NOT NULL,
              payload_json TEXT NOT NULL,
              updated_at_ms INTEGER NOT NULL
            )
          `);
          sql.exec(`
            CREATE TABLE IF NOT EXISTS _eda_sink_cursors (
              sink_name TEXT PRIMARY KEY,
              after_seq INTEGER NOT NULL,
              payload_json TEXT NOT NULL
            )
          `);
          sql.exec(
            "INSERT INTO _eda_schema_migrations (id) VALUES (?)",
            durableObjectSessionStoreSchemaVersion,
          );
        }
      }),
    catch: (error) => durableStoreError("migrating DurableObjectSessionStore", error),
  });

/**
 * Interpret the semantic store API with native DO SQLite.
 *
 * All writes that affect durable ordering happen inside `transactionSync`; replay
 * and lookup reads stay synchronous SQL wrapped in Effect boundaries.
 */
const makeDurableObjectSessionStore = (
  sessionId: SessionId,
  storage: DurableObjectSessionStorage,
): EDASessionStoreShape => {
  const { sql } = storage;
  const ensureSession = (event: DurableEventEnvelope) => {
    if (event.sessionId !== sessionId) {
      throw new Error(
        `DurableObjectSessionStore is scoped to session ${sessionId}; received ${event.sessionId}`,
      );
    }
  };

  /**
   * Insert one append entry or return its existing idempotent event position.
   *
   * The event row is the sequence allocator. Artifact/projection writes happen
   * only for newly inserted events, preventing duplicate appends from advancing
   * body rows or metadata.
   */
  const insertOrRead = (entry: DurableAppendEntry): CommittedDurableEvent => {
    const { event } = entry;
    ensureSession(event);
    const existing = readCommittedByEventIdOptional(sql, sessionId, event.eventId);
    if (existing !== undefined) {
      return existing;
    }

    const encoded = encodeEventLogRow(event);
    sql.exec(
      `
        INSERT INTO _eda_event_log (
          event_id,
          namespace,
          type,
          schema_version,
          created_at_ms,
          trace_json,
          fact_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `,
      encoded.eventId,
      encoded.namespace,
      encoded.type,
      encoded.schemaVersion,
      encoded.createdAtMs,
      encoded.traceJson,
      encoded.factJson,
    );
    const row = readEventLogRowByEventId(sql, event.eventId);
    applySynchronousProjections(sql, entry, SequenceNumber.make(row.seq), encoded.factJson);
    return rowToCommittedDurableEvent(sql, row, sessionId);
  };

  /** Execute an entire append batch as one synchronous SQLite transaction. */
  const append = (batch: DurableAppendBatch) =>
    Effect.try({
      try: () => storage.transactionSync(() => batch.entries.map(insertOrRead)),
      catch: (error) => durableStoreError("appending DurableObjectSessionStore batch", error),
    }).pipe(Effect.tap((committed) => annotateEdaSpan(committedBatchAttributes(committed))));

  return {
    append,
    eventsAfter: (afterSeq) => makeCommittedEventStream(sql, sessionId, afterSeq),
    loadCommittedEventsBySeq: (seqs) =>
      Effect.try({
        try: () =>
          uniqueSequenceNumbers(seqs)
            .sort((left, right) => left - right)
            .map((seq) => readCommittedBySeq(sql, sessionId, seq)),
        catch: (error) =>
          durableStoreError("loading DurableObjectSessionStore events by seq", error),
      }),
    findCommandAdmission: (input) =>
      Effect.try({
        try: () => {
          const row = findCommandAdmissionRow(sql, input.commandId, input.idempotencyKey);
          return row === undefined
            ? undefined
            : readCommittedBySeq(sql, sessionId, row.admitted_seq);
        },
        catch: (error) => durableStoreError("finding command admission", error),
      }),
    loadSummaryArtifact: (summaryId) =>
      Effect.try({
        try: () => readSummaryPayloadOptional(sql, summaryId),
        catch: (error) => durableStoreError("loading DurableObjectSessionStore summary", error),
      }),
    loadReducerCheckpoint: (name) =>
      Effect.try({
        try: () => readReducerCheckpoint(sql, name),
        catch: (error) =>
          durableStoreError(`loading DurableObjectSessionStore reducer checkpoint ${name}`, error),
      }),
    saveReducerCheckpoint: (checkpoint) =>
      Effect.try({
        try: () => writeReducerCheckpoint(sql, checkpoint),
        catch: (error) =>
          durableStoreError(
            `saving DurableObjectSessionStore reducer checkpoint ${checkpoint.name}`,
            error,
          ),
      }),
    saveReducerCheckpoints: (checkpoints) =>
      Effect.try({
        try: () =>
          storage.transactionSync(() =>
            checkpoints.forEach((checkpoint) => writeReducerCheckpoint(sql, checkpoint)),
          ),
        catch: (error) =>
          durableStoreError("saving DurableObjectSessionStore reducer checkpoints", error),
      }),
  };
};

const makeCommittedEventStream = (
  sql: DurableObjectSqlStorage,
  sessionId: SessionId,
  afterSeq: SequenceNumber,
): Stream.Stream<CommittedDurableEvent, EDASessionStoreError> =>
  Stream.fromPull(
    Effect.sync(() => {
      let cursor: DurableObjectEventPageCursor = { afterSeq, throughSeq: undefined };
      let done = false;
      return Effect.gen(function* () {
        if (done) {
          return yield* Cause.done();
        }
        // DO SQLite is synchronous; yield between bounded pages so eager consumers do not
        // monopolize the isolate with one long replay/decode loop.
        yield* Effect.yieldNow;
        const page = yield* readCommittedEventPage(sql, sessionId, cursor);
        if (page.events.length === 0) {
          done = true;
          return yield* Cause.done();
        }
        if (Option.isSome(page.next)) {
          cursor = page.next.value;
        } else {
          done = true;
        }
        return page.events as readonly [CommittedDurableEvent, ...Array<CommittedDurableEvent>];
      });
    }),
  );

const readCommittedEventPage = (
  sql: DurableObjectSqlStorage,
  sessionId: SessionId,
  cursor: DurableObjectEventPageCursor,
): Effect.Effect<
  {
    readonly events: ReadonlyArray<CommittedDurableEvent>;
    readonly next: Option.Option<DurableObjectEventPageCursor>;
  },
  EDASessionStoreError
> =>
  Effect.try({
    try: () => {
      const throughSeq = cursor.throughSeq ?? readEventLogHead(sql);
      if (cursor.afterSeq >= throughSeq) {
        return { events: [], next: Option.none() };
      }

      const events = sql
        .exec<DurableObjectEventRow>(
          `
            SELECT seq, event_id, namespace, type, schema_version, created_at_ms, trace_json, fact_json
            FROM _eda_event_log
            WHERE seq > ? AND seq <= ?
            ORDER BY seq ASC
            LIMIT ?
          `,
          cursor.afterSeq,
          throughSeq,
          durableObjectEventReadPageRows,
        )
        .toArray()
        .map((row) => rowToCommittedDurableEvent(sql, row, sessionId));
      const lastSeq = events.at(-1)?.position.seq ?? cursor.afterSeq;
      return {
        events,
        next:
          lastSeq < throughSeq
            ? Option.some({ afterSeq: lastSeq, throughSeq } satisfies DurableObjectEventPageCursor)
            : Option.none(),
      };
    },
    catch: (error) => durableStoreError("replaying DurableObjectSessionStore events", error),
  });

const readEventLogHead = (sql: DurableObjectSqlStorage): SequenceNumber =>
  SequenceNumber.make(
    sql
      .exec<DurableObjectEventHeadRow>(
        `
          SELECT COALESCE(MAX(seq), 0) AS head
          FROM _eda_event_log
        `,
      )
      .one().head ?? 0,
  );

const uniqueSequenceNumbers = (seqs: ReadonlyArray<SequenceNumber>): Array<SequenceNumber> =>
  Array.from(new Set(seqs.map((seq) => Number(seq)))).map((seq) => SequenceNumber.make(seq));

const readEventLogRowByEventId = (
  sql: DurableObjectSqlStorage,
  eventId: DurableEventEnvelope["eventId"],
): DurableObjectEventRow =>
  sql
    .exec<DurableObjectEventRow>(
      `
        SELECT seq, event_id, namespace, type, schema_version, created_at_ms, trace_json, fact_json
        FROM _eda_event_log
        WHERE event_id = ?
      `,
      eventId,
    )
    .one();

const readCommittedByEventIdOptional = (
  sql: DurableObjectSqlStorage,
  sessionId: SessionId,
  eventId: DurableEventEnvelope["eventId"],
): CommittedDurableEvent | undefined => {
  const row = sql
    .exec<DurableObjectEventRow>(
      `
        SELECT seq, event_id, namespace, type, schema_version, created_at_ms, trace_json, fact_json
        FROM _eda_event_log
        WHERE event_id = ?
      `,
      eventId,
    )
    .toArray()[0];
  return row === undefined ? undefined : rowToCommittedDurableEvent(sql, row, sessionId);
};

const readReducerCheckpoint = (
  sql: DurableObjectSqlStorage,
  name: string,
): EDAReducerCheckpoint | undefined => {
  const row = sql
    .exec<DurableObjectReducerCheckpointRow>(
      `
        SELECT reducer_name, schema_version, through_seq, payload_json, updated_at_ms
        FROM _eda_reducer_checkpoints
        WHERE reducer_name = ?
      `,
      name,
    )
    .toArray()[0];
  return row === undefined
    ? undefined
    : {
        name: row.reducer_name,
        schemaVersion: row.schema_version,
        throughSeq: SequenceNumber.make(row.through_seq),
        payload: JSON.parse(row.payload_json),
        updatedAtMs: row.updated_at_ms,
      };
};

const writeReducerCheckpoint = (
  sql: DurableObjectSqlStorage,
  checkpoint: SaveReducerCheckpointInput,
): void => {
  sql.exec(
    `
      INSERT INTO _eda_reducer_checkpoints (
        reducer_name,
        schema_version,
        through_seq,
        payload_json,
        updated_at_ms
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
    stringifyJsonForColumn(checkpoint.payload, "_eda_reducer_checkpoints.payload_json"),
    checkpoint.updatedAtMs,
  );
};

const readMessagePayloadByContextSeq = (
  sql: DurableObjectSqlStorage,
  contextSeq: number,
): DurableObjectMessagePayloadRow => {
  const message = sql
    .exec<DurableObjectMessagePayloadRow>(
      `
        SELECT context_seq, payload_json
        FROM _eda_context_messages
        WHERE context_seq = ?
      `,
      contextSeq,
    )
    .toArray()[0];
  if (message === undefined) {
    throw new Error(`Context message at seq ${contextSeq} missing from _eda_context_messages`);
  }
  return message;
};

const readCommittedBySeq = (
  sql: DurableObjectSqlStorage,
  sessionId: SessionId,
  seq: number,
): CommittedDurableEvent => {
  const row = sql
    .exec<DurableObjectEventRow>(
      `
        SELECT seq, event_id, namespace, type, schema_version, created_at_ms, trace_json, fact_json
        FROM _eda_event_log
        WHERE seq = ?
      `,
      seq,
    )
    .one();
  return rowToCommittedDurableEvent(sql, row, sessionId);
};

const rowToCommittedDurableEvent = (
  sql: DurableObjectSqlStorage,
  row: DurableObjectEventRow,
  sessionId: SessionId,
): CommittedDurableEvent =>
  CommittedDurableEvent.make({
    position: durablePosition(SequenceNumber.make(row.seq)),
    event: decodeDurableEvent({
      namespace: row.namespace,
      type: row.type,
      schemaVersion: row.schema_version,
      durability: "durable",
      eventId: row.event_id,
      sessionId,
      createdAtMs: row.created_at_ms,
      trace: JSON.parse(row.trace_json),
      payload: readLogicalPayload(sql, row),
    }),
  });

const decodeDurableEvent = (input: unknown): DurableEventEnvelope => {
  const event = Schema.decodeUnknownSync(DurableEventEnvelope)(input);
  return event.namespace === effectDurableAgentNamespace
    ? decodeUnknownEDADurableEventSync(event)
    : event;
};

interface EncodedEventLogRow {
  readonly eventId: string;
  readonly namespace: string;
  readonly type: string;
  readonly schemaVersion: number;
  readonly createdAtMs: number;
  readonly traceJson: string;
  readonly factJson: string;
}

const encodeEventLogRow = (event: DurableEventEnvelope): EncodedEventLogRow => {
  const encoded = Schema.encodeSync(DurableEventEnvelope)(event);
  return {
    eventId: encoded.eventId,
    namespace: encoded.namespace,
    type: encoded.type,
    schemaVersion: encoded.schemaVersion,
    createdAtMs: encoded.createdAtMs,
    traceJson: stringifyJsonForColumn(encoded.trace, "_eda_event_log.trace_json"),
    factJson: stringifyJsonForColumn(
      eventLogFactPayload(encoded.namespace, encoded.type, encoded.payload),
      "_eda_event_log.fact_json",
    ),
  };
};

const readLogicalPayload = (sql: DurableObjectSqlStorage, row: DurableObjectEventRow): unknown => {
  if (row.namespace !== effectDurableAgentNamespace) {
    return JSON.parse(row.fact_json);
  }
  if (row.type === commandAdmittedEventType) {
    const commandId = readCommandIdFromFact(row);
    const command = readCommandInputPayload(sql, commandId);
    return { command };
  }
  if (contextMessageEventTypes.has(row.type)) {
    return JSON.parse(readMessagePayloadByContextSeq(sql, row.seq).payload_json);
  }
  if (row.type === summaryCreatedEventType) {
    return {
      ...(JSON.parse(row.fact_json) as Record<string, unknown>),
      summary: readSummaryPayloadFromCreatedSeq(sql, row.seq),
    };
  }
  return JSON.parse(row.fact_json);
};

const eventLogFactPayload = (namespace: string, type: string, payload: unknown): unknown => {
  if (
    namespace !== effectDurableAgentNamespace ||
    typeof payload !== "object" ||
    payload === null
  ) {
    return payload;
  }

  const record = payload as Record<string, unknown>;
  switch (type) {
    case commandAdmittedEventType:
      return { command: commandFact(record.command) };
    case systemMessageCommittedEventType:
      return pick(record, ["messageId"]);
    case userMessageCommittedEventType:
      return pick(record, ["commandId", "messageId"]);
    case steeringMessageQueuedEventType:
      return pick(record, ["commandId", "messageId", "runId"]);
    case assistantMessageCommittedEventType:
    case assistantMessageImportedEventType:
      return pick(record, ["messageId", "runId", "turnId", "inferenceId"]);
    case assistantPartialCommittedEventType:
      return pick(record, ["messageId", "runId", "turnId", "inferenceId", "reason"]);
    case summaryCreatedEventType:
      return pick(record, ["compactionId", "summaryId", "sourceFromSeq", "sourceToSeq"]);
    default:
      return payload;
  }
};

const commandFact = (command: unknown): unknown => {
  if (typeof command !== "object" || command === null) {
    return command;
  }
  const record = command as Record<string, unknown>;
  return record._tag === "SubmitMessage"
    ? pick(record, ["_tag", "commandId", "disposition", "idempotencyKey"])
    : command;
};

const pick = (
  record: Record<string, unknown>,
  keys: ReadonlyArray<string>,
): Record<string, unknown> => {
  const out: Record<string, unknown> = {};
  for (const key of keys) {
    if (record[key] !== undefined) {
      out[key] = record[key];
    }
  }
  return out;
};

const readCommandIdFromFact = (row: DurableObjectEventRow): string => {
  const commandId = (
    JSON.parse(row.fact_json) as { readonly command?: { readonly commandId?: unknown } }
  ).command?.commandId;
  if (typeof commandId !== "string") {
    throw new Error(`CommandAdmitted fact at seq ${row.seq} missing command.commandId`);
  }
  return commandId;
};

const readCommandInputPayload = (sql: DurableObjectSqlStorage, commandId: string): unknown => {
  const row = sql
    .exec<DurableObjectCommandInputPayloadRow>(
      `
        SELECT payload_json
        FROM _eda_command_inputs
        WHERE command_id = ?
      `,
      commandId,
    )
    .toArray()[0];
  if (row === undefined) {
    throw new Error(`Command input ${commandId} missing from _eda_command_inputs`);
  }
  return JSON.parse(row.payload_json);
};

const readSummaryPayloadOptional = (
  sql: DurableObjectSqlStorage,
  summaryId: SummaryId,
): CompactionSummaryArtifact | undefined => {
  const row = sql
    .exec<DurableObjectSummaryPayloadRow>(
      `
        SELECT payload_json
        FROM _eda_context_summaries
        WHERE summary_id = ?
      `,
      summaryId,
    )
    .toArray()[0];
  return row === undefined
    ? undefined
    : Schema.decodeUnknownSync(CompactionSummaryArtifact)(JSON.parse(row.payload_json));
};

const readSummaryPayloadFromCreatedSeq = (
  sql: DurableObjectSqlStorage,
  createdSeq: number,
): unknown => {
  const row = sql
    .exec<DurableObjectSummaryPayloadRow>(
      `
        SELECT payload_json
        FROM _eda_context_summaries
        WHERE created_seq = ?
      `,
      createdSeq,
    )
    .toArray()[0];
  if (row === undefined) {
    throw new Error(`SummaryCreated at seq ${createdSeq} missing from _eda_context_summaries`);
  }
  return JSON.parse(row.payload_json);
};

/**
 * Maintain bounded storage projections in the same transaction as the event.
 *
 * These rows are durability aids, not a second source of lifecycle truth: event
 * order still comes from `_eda_event_log.seq`, while projections make command
 * lookup and body-sidecar hydration bounded.
 */
const applySynchronousProjections = (
  sql: DurableObjectSqlStorage,
  entry: DurableAppendEntry,
  seq: SequenceNumber,
  factJson: string,
): void => {
  const { event } = entry;
  if (event.namespace !== effectDurableAgentNamespace) {
    return;
  }

  writeCommandProjection(sql, event, seq, factJson);
  writeCommandInputProjection(sql, event, seq);
  writeMessageProjection(sql, event, seq);
  writeSummaryProjection(sql, event, seq);
  writeContextRebaseProjection(sql, event, seq);
};

const writeCommandProjection = (
  sql: DurableObjectSqlStorage,
  event: DurableEventEnvelope,
  seq: SequenceNumber,
  factJson: string,
): void => {
  const payload = event.payload as any;
  switch (event.type) {
    case commandAdmittedEventType: {
      const command = payload.command;
      const commandId = command?.commandId;
      if (commandId === undefined) {
        throw new Error("CommandAdmitted missing command.commandId");
      }
      sql.exec(
        `
          INSERT INTO _eda_command_state (
            command_id,
            admitted_seq,
            status,
            idempotency_key,
            payload_json
          ) VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(command_id) DO UPDATE SET
            admitted_seq = excluded.admitted_seq,
            status = excluded.status,
            idempotency_key = excluded.idempotency_key,
            payload_json = excluded.payload_json
        `,
        commandId,
        seq,
        "admitted",
        command.idempotencyKey ?? null,
        factJson,
      );
      return;
    }
    case commandStartedEventType:
      writeCommandStatus(sql, payload.commandId, seq, "started", factJson);
      return;
    case commandCompletedEventType:
      writeCommandStatus(sql, payload.commandId, seq, "completed", factJson);
      return;
    case commandFailedEventType:
      writeCommandStatus(sql, payload.commandId, seq, "failed", factJson);
      return;
    case commandCancelledEventType:
      writeCommandStatus(sql, payload.commandId, seq, "cancelled", factJson);
      return;
    default:
      return;
  }
};

const writeCommandStatus = (
  sql: DurableObjectSqlStorage,
  commandId: unknown,
  seq: SequenceNumber,
  status: string,
  payloadJson: string,
): void => {
  if (commandId === undefined) {
    throw new Error(`${status} command event missing commandId`);
  }
  sql.exec(
    `
      INSERT INTO _eda_command_state (
        command_id,
        admitted_seq,
        status,
        idempotency_key,
        payload_json
      ) VALUES (?, ?, ?, NULL, ?)
      ON CONFLICT(command_id) DO UPDATE SET
        status = excluded.status,
        payload_json = excluded.payload_json
    `,
    commandId,
    seq,
    status,
    payloadJson,
  );
};

const writeCommandInputProjection = (
  sql: DurableObjectSqlStorage,
  event: DurableEventEnvelope,
  seq: SequenceNumber,
): void => {
  if (event.type !== commandAdmittedEventType) {
    return;
  }
  const encoded = Schema.encodeSync(DurableEventEnvelope)(event);
  const command = (encoded.payload as { readonly command?: { readonly commandId?: unknown } })
    .command;
  const commandId = command?.commandId;
  if (commandId === undefined) {
    throw new Error("CommandAdmitted missing command.commandId");
  }
  sql.exec(
    `
      INSERT INTO _eda_command_inputs (
        command_id,
        admitted_seq,
        payload_json
      ) VALUES (?, ?, ?)
      ON CONFLICT(command_id) DO UPDATE SET
        admitted_seq = excluded.admitted_seq,
        payload_json = excluded.payload_json
    `,
    commandId,
    seq,
    stringifyJsonForColumn(command, "_eda_command_inputs.payload_json"),
  );
};

const contextMessageEventTypes = new Set<string>([
  systemMessageCommittedEventType,
  userMessageCommittedEventType,
  steeringMessageQueuedEventType,
  assistantMessageCommittedEventType,
  assistantMessageImportedEventType,
  assistantPartialCommittedEventType,
]);

/** Persist model-visible message bodies with the seq that exposed them to context. */
const writeMessageProjection = (
  sql: DurableObjectSqlStorage,
  event: DurableEventEnvelope,
  seq: SequenceNumber,
): void => {
  if (!contextMessageEventTypes.has(event.type)) {
    return;
  }

  const encoded = Schema.encodeSync(DurableEventEnvelope)(event);
  const messageId = (encoded.payload as { readonly messageId?: unknown }).messageId;
  if (messageId === undefined) {
    throw new Error(`${event.type} missing messageId`);
  }

  sql.exec(
    `
      INSERT INTO _eda_context_messages (
        message_id,
        context_seq,
        payload_json
      ) VALUES (?, ?, ?)
      ON CONFLICT(message_id) DO UPDATE SET
        context_seq = excluded.context_seq,
        payload_json = excluded.payload_json
    `,
    messageId,
    seq,
    stringifyJsonForColumn(encoded.payload, "_eda_context_messages.payload_json"),
  );
};

/**
 * Persist a cumulative summary artifact without making it the active context.
 *
 * `ContextRebased` selects summaries in the framework reduced-state checkpoint;
 * failed compactions can leave orphaned summaries without changing future context.
 */
const writeSummaryProjection = (
  sql: DurableObjectSqlStorage,
  event: DurableEventEnvelope,
  seq: SequenceNumber,
): void => {
  if (event.type !== summaryCreatedEventType) {
    return;
  }

  const payload = event.payload as {
    readonly compactionId?: unknown;
    readonly summaryId?: unknown;
    readonly summary?: unknown;
  };
  const summaryId = payload.summaryId;
  if (summaryId === undefined) {
    throw new Error("SummaryCreated missing summaryId");
  }
  if (payload.summary === undefined) {
    throw new Error("SummaryCreated requires summary payload");
  }

  const summary = Schema.decodeUnknownSync(CompactionSummaryArtifact)(payload.summary);
  if (summary.summaryId !== summaryId) {
    throw new Error("SummaryCreated summaryId does not match summary payload");
  }
  if (summary.compactionId !== payload.compactionId) {
    throw new Error("SummaryCreated compactionId does not match summary payload");
  }

  sql.exec(
    `
      INSERT INTO _eda_context_summaries (
        summary_id,
        created_seq,
        payload_json
      ) VALUES (?, ?, ?)
      ON CONFLICT(summary_id) DO UPDATE SET
        created_seq = excluded.created_seq,
        payload_json = excluded.payload_json
    `,
    summaryId,
    seq,
    stringifyJsonForColumn(summary, "_eda_context_summaries.payload_json"),
  );
};

/** Validate that `ContextRebased` points at a committed summary sidecar. */
const writeContextRebaseProjection = (
  sql: DurableObjectSqlStorage,
  event: DurableEventEnvelope,
  _seq: SequenceNumber,
): void => {
  if (event.type !== contextRebasedEventType) {
    return;
  }

  const payload = event.payload as {
    readonly compactionId?: unknown;
    readonly retainedFromContextSeq?: unknown;
    readonly summaryId?: unknown;
  };
  if (payload.summaryId === undefined) {
    throw new Error("ContextRebased missing summaryId");
  }
  const summary = readSummaryPayloadOptional(sql, SummaryId.make(String(payload.summaryId)));
  if (summary === undefined) {
    throw new Error(`ContextRebased references unknown summaryId ${String(payload.summaryId)}`);
  }
  if (summary.compactionId !== payload.compactionId) {
    throw new Error("ContextRebased summary compactionId does not match event payload");
  }
  if (summary.retainedFromContextSeq !== payload.retainedFromContextSeq) {
    throw new Error("ContextRebased retained cursor does not match summary payload");
  }
};

/**
 * Lookup prior command admission through `_eda_command_state` instead of genesis replay.
 *
 * Idempotency key wins over explicit command id so caller-owned retry keys remain
 * the stable dedupe surface for external ingress.
 */
const findCommandAdmissionRow = (
  sql: DurableObjectSqlStorage,
  commandId: unknown,
  idempotencyKey: unknown,
): DurableObjectCommandLookupRow | undefined => {
  if (idempotencyKey !== undefined) {
    const row = sql
      .exec<DurableObjectCommandLookupRow>(
        `
          SELECT admitted_seq
          FROM _eda_command_state
          WHERE idempotency_key = ?
        `,
        idempotencyKey,
      )
      .toArray()[0];
    if (row !== undefined) {
      return row;
    }
  }

  if (commandId === undefined) {
    return undefined;
  }
  return sql
    .exec<DurableObjectCommandLookupRow>(
      `
        SELECT admitted_seq
        FROM _eda_command_state
        WHERE command_id = ?
      `,
      commandId,
    )
    .toArray()[0];
};

/** Serialize and size-check one JSON column before it reaches DO SQLite. */
const stringifyJsonForColumn = (value: unknown, column: string): string => {
  const json = JSON.stringify(value);
  if (json === undefined) {
    throw new Error(`${column} value is not JSON serializable`);
  }
  const bytes = jsonEncoder.encode(json).byteLength;
  if (bytes > durableObjectSerializedJsonHardCapBytes) {
    throw new Error(
      `${column} serialized JSON is ${bytes} bytes; hard cap is ${durableObjectSerializedJsonHardCapBytes} bytes`,
    );
  }
  return json;
};

const durableStoreError = (operation: string, cause: unknown) =>
  new EDASessionStoreError({
    message:
      cause instanceof Error ? `${operation}: ${cause.message}` : `${operation}: ${String(cause)}`,
  });
