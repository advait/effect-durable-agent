import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { SequenceNumber } from "effect-durable-agent/types/core";
import { EDASessionStoreError } from "effect-durable-agent/services/session-store";
import {
  SinkCheckpointStore,
  type EDASinkName,
  type SinkCheckpointStoreShape,
} from "effect-durable-agent/services/sink-checkpoint-store";
import type { DurableObjectSessionStorage } from "./durable-object-storage";

/**
 * Durable Object SQLite adapter for EDA sink checkpoints.
 *
 * Checkpoint SQL stays in the host layer for the same reason session storage SQL
 * does: framework sink code should depend on checkpoint semantics, not table access.
 */
export class DurableObjectSinkCheckpointStore {
  /** Ensure the fixed `_eda_sink_cursors` table exists during host startup. */
  static readonly migrate = (storage: DurableObjectSessionStorage) =>
    migrateSinkCheckpointStore(storage);

  /** Build the semantic checkpoint store over native DO SQLite. */
  static readonly make = (
    storage: DurableObjectSessionStorage,
  ): Effect.Effect<SinkCheckpointStoreShape, EDASessionStoreError> =>
    makeDurableObjectSinkCheckpointStore(storage);

  static readonly layer = (storage: DurableObjectSessionStorage) =>
    Layer.effect(SinkCheckpointStore, DurableObjectSinkCheckpointStore.make(storage));
}

/** Create the sink checkpoint table inside the current EDA storage schema. */
const migrateSinkCheckpointStore = (
  storage: DurableObjectSessionStorage,
): Effect.Effect<void, EDASessionStoreError> =>
  Effect.try({
    try: () => {
      storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS _eda_sink_cursors (
          sink_name TEXT PRIMARY KEY,
          after_seq INTEGER NOT NULL,
          payload_json TEXT NOT NULL
        )
      `);
    },
    catch: (error) => sinkCheckpointError("migrating SinkCheckpointStore", error),
  });

/** Interpret checkpoint load/save/commit with idempotent upserts. */
const makeDurableObjectSinkCheckpointStore = (
  storage: DurableObjectSessionStorage,
): Effect.Effect<SinkCheckpointStoreShape, EDASessionStoreError> =>
  Effect.gen(function* () {
    yield* DurableObjectSinkCheckpointStore.migrate(storage);
    const { sql } = storage;
    return {
      load: (sinkName: EDASinkName) =>
        Effect.try({
          try: () => {
            const rows = sql
              .exec<{ after_seq: number; payload_json: string }>(
                `SELECT after_seq, payload_json FROM _eda_sink_cursors WHERE sink_name = ?`,
                sinkName,
              )
              .toArray();
            const row = rows[0];
            return row === undefined
              ? { afterSeq: SequenceNumber.make(0), payload: undefined }
              : {
                  afterSeq: SequenceNumber.make(row.after_seq),
                  payload: JSON.parse(row.payload_json),
                };
          },
          catch: (error) => sinkCheckpointError("reading sink checkpoint", error),
        }),
      saveState: (sinkName: EDASinkName, payload: unknown) =>
        Effect.try({
          try: () => {
            sql.exec(
              `
                INSERT INTO _eda_sink_cursors (sink_name, after_seq, payload_json)
                VALUES (?, 0, ?)
                ON CONFLICT(sink_name) DO UPDATE SET
                  payload_json = excluded.payload_json
              `,
              sinkName,
              JSON.stringify(payload),
            );
          },
          catch: (error) => sinkCheckpointError("saving sink checkpoint state", error),
        }),
      commit: (sinkName: EDASinkName, afterSeq: SequenceNumber, payload: unknown | undefined) =>
        Effect.try({
          try: () => {
            sql.exec(
              `
                INSERT INTO _eda_sink_cursors (sink_name, after_seq, payload_json)
                VALUES (?, ?, ?)
                ON CONFLICT(sink_name) DO UPDATE SET
                  after_seq = excluded.after_seq,
                  payload_json = excluded.payload_json
              `,
              sinkName,
              afterSeq,
              JSON.stringify(payload ?? { updatedAtMs: Date.now() }),
            );
          },
          catch: (error) => sinkCheckpointError("committing sink checkpoint", error),
        }),
    } satisfies SinkCheckpointStoreShape;
  });

const sinkCheckpointError = (operation: string, cause: unknown) =>
  new EDASessionStoreError({
    message:
      cause instanceof Error ? `${operation}: ${cause.message}` : `${operation}: ${String(cause)}`,
  });
