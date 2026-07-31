import * as Effect from "effect/Effect";
import { describe, expect, it } from "vite-plus/test";

import { EDASinkName } from "../services/sink-checkpoint-store";
import { SequenceNumber } from "../types/core";
import type { DurableObjectSessionStorage, DurableObjectSqlCursor } from "./durable-object-storage";
import { DurableObjectSinkCheckpointStore } from "./durable-object-sink-checkpoints";

interface SinkCheckpointRow {
  readonly after_seq: number;
  readonly payload_json: string;
}

class FakeSinkCheckpointStorage implements DurableObjectSessionStorage {
  readonly sql = {
    exec: <Row = Record<string, unknown>>(query: string, ...bindings: ReadonlyArray<unknown>) =>
      this.exec<Row>(query, ...bindings),
  };
  private readonly rows = new Map<string, SinkCheckpointRow>();

  transactionSync<A>(closure: () => A): A {
    return closure();
  }

  private exec<Row = Record<string, unknown>>(
    query: string,
    ...bindings: ReadonlyArray<unknown>
  ): DurableObjectSqlCursor<Row> {
    const normalized = query.replace(/\s+/g, " ").trim().toUpperCase();
    if (normalized.startsWith("CREATE TABLE IF NOT EXISTS _EDA_SINK_CURSORS")) {
      return cursor<Row>([]);
    }
    if (normalized.startsWith("SELECT AFTER_SEQ, PAYLOAD_JSON FROM _EDA_SINK_CURSORS")) {
      const row = this.rows.get(String(bindings[0]));
      return cursor<Row>(row === undefined ? [] : [row as Row]);
    }
    if (
      normalized.startsWith("INSERT INTO _EDA_SINK_CURSORS") &&
      normalized.includes("VALUES (?, 0, ?)")
    ) {
      const sinkName = String(bindings[0]);
      const current = this.rows.get(sinkName);
      this.rows.set(sinkName, {
        after_seq: current?.after_seq ?? 0,
        payload_json: String(bindings[1]),
      });
      return cursor<Row>([]);
    }
    if (normalized.startsWith("INSERT INTO _EDA_SINK_CURSORS")) {
      this.rows.set(String(bindings[0]), {
        after_seq: Number(bindings[1]),
        payload_json: String(bindings[2]),
      });
      return cursor<Row>([]);
    }
    throw new Error(`Unsupported fake SQL query: ${query}`);
  }
}

const cursor = <Row>(items: Array<Row>): DurableObjectSqlCursor<Row> => ({
  one: () => {
    const row = items[0];
    if (row === undefined) {
      throw new Error("Expected one SQL row");
    }
    return row;
  },
  toArray: () => items,
});

describe("DurableObjectSinkCheckpointStore", () => {
  it("updates sink state without changing the committed cursor", async () => {
    const storage = new FakeSinkCheckpointStorage();
    const store = await Effect.runPromise(DurableObjectSinkCheckpointStore.make(storage));
    const sinkName = EDASinkName.make("test.slack");

    await Effect.runPromise(store.commit(sinkName, SequenceNumber.make(7), { activeStream: null }));
    await Effect.runPromise(
      store.saveState(sinkName, { activeStream: { streamTs: "1700000000.000001" } }),
    );

    await expect(Effect.runPromise(store.load(sinkName))).resolves.toEqual({
      afterSeq: 7,
      payload: { activeStream: { streamTs: "1700000000.000001" } },
    });
  });

  it("loads legacy cursor payloads without rewriting them", async () => {
    const storage = new FakeSinkCheckpointStorage();
    const store = await Effect.runPromise(DurableObjectSinkCheckpointStore.make(storage));
    const sinkName = EDASinkName.make("test.legacy");

    await Effect.runPromise(
      store.commit(sinkName, SequenceNumber.make(12), { updatedAtMs: 1_715_000_000_000 }),
    );

    await expect(Effect.runPromise(store.load(sinkName))).resolves.toEqual({
      afterSeq: 12,
      payload: { updatedAtMs: 1_715_000_000_000 },
    });
  });
});
