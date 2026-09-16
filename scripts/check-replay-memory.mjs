/** Run with node --max-old-space-size=128 --expose-gc --import tsx scripts/check-replay-memory.mjs. */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { pathToFileURL } from "node:url";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Prompt from "effect/unstable/ai/Prompt";
import { DurableObjectSessionStore } from "../packages/effect-durable-agent-cloudflare/src/durable-object-store.ts";
import { DurableObjectSinkCheckpointStore } from "../packages/effect-durable-agent-cloudflare/src/durable-object-sink-checkpoints.ts";

// Override only the core source to compare the same persisted fixture against the old implementation.
const source = process.env.EDA_REPLAY_SOURCE;
const load = (path) =>
  import(
    source === undefined
      ? new URL(`../packages/effect-durable-agent/src/${path}.ts`, import.meta.url).href
      : pathToFileURL(join(source, `${path}.ts`)).href
  );
const { EDASinkRegistry } = await load("services/sink-registry");
const { EDASessionStore } = await load("services/session-store");
const { SinkCheckpointStore, EDASinkName } = await load("services/sink-checkpoint-store");
const { LiveEventBus } = await load("services/live-event-bus");
const { EventFactory } = await load("services/event-factory");
const { IdGenerator } = await load("services/id-generator");
const { SessionContext } = await load("services/session-context");
const { EDAReducerRegistry, EDAReducer } = await load("services/reducer-registry");
const { EDAKeepAlive } = await load("services/keep-alive");
const { SessionId, SequenceNumber, EventId } = await load("types/core");
const { DurableEventEnvelope, EventType, UnixEpochMillis, schemaV1, effectDurableAgentNamespace } =
  await load("types/events");
const {
  initialReducedState,
  foldReducedState,
  encodeReducedStateCheckpoint,
  frameworkReducedStateReducerName,
  frameworkReducedStateReducerSchemaVersion,
} = await load("domain/reduced-state");
const phase = process.argv[4] ?? "sinks";
assert(["sinks", "checkpointed", "lagging", "state"].includes(phase));
const count = Number(process.argv[2] ?? 4500);
const payloadBytes = Number(process.argv[3] ?? 32768);
assert(Number.isSafeInteger(count) && count > 0 && count % 5 === 0);
assert(Number.isSafeInteger(payloadBytes) && payloadBytes > 0);
assert.equal(typeof global.gc, "function", "Pass --expose-gc");
const directory = mkdtempSync(join(tmpdir(), "eda-replay-memory-"));
const sqlite = new DatabaseSync(join(directory, "session.sqlite"));
const storage = {
  sql: {
    exec(query, ...bindings) {
      const statement = sqlite.prepare(query);
      const rows =
        statement.columns().length > 0
          ? statement.all(...bindings)
          : (statement.run(...bindings), []);
      return {
        toArray: () => rows,
        one: () => {
          assert.equal(rows.length, 1);
          return rows[0];
        },
      };
    },
  },
  transactionSync(closure) {
    sqlite.exec("BEGIN");
    try {
      const result = closure();
      sqlite.exec("COMMIT");
      return result;
    } catch (error) {
      sqlite.exec("ROLLBACK");
      throw error;
    }
  },
};
const uuid = (n) => `018f6bd5-2f2a-7b1e-8f1a-${n.toString(16).padStart(12, "0")}`;
const sessionId = SessionId.make(uuid(0));
const text = "x".repeat(payloadBytes);
const makeEvent = (seq) => {
  const cycle = Math.floor((seq - 1) / 5) + 1;
  const offset = (seq - 1) % 5;
  const payload = [
    () => ({ commandId: uuid(900000), messageId: uuid(seq), content: [Prompt.textPart({ text })] }),
    () => ({
      messageId: uuid(seq),
      runId: uuid(900001),
      turnId: uuid(900002),
      inferenceId: uuid(900003),
      promptParts: [Prompt.textPart({ text })],
    }),
    () => ({
      compactionId: uuid(100000 + cycle),
      summaryId: uuid(200000 + cycle),
      sourceFromSeq: seq - 2,
      sourceToSeq: seq - 1,
      summary: {
        compactionId: uuid(100000 + cycle),
        summaryId: uuid(200000 + cycle),
        sourceFromSeq: seq - 2,
        sourceToSeq: seq - 1,
        retainedFromContextSeq: seq + 1,
        text: "summary",
        promptMessage: Prompt.makeMessage("user", {
          content: [Prompt.textPart({ text: "summary" })],
        }),
        policyId: "memory",
      },
    }),
    () => ({
      compactionId: uuid(100000 + cycle),
      summaryId: uuid(200000 + cycle),
      contextVersion: cycle,
      retainedFromContextSeq: seq,
    }),
    () => ({ compactionId: uuid(100000 + cycle) }),
  ][offset]();
  return DurableEventEnvelope.make({
    namespace: effectDurableAgentNamespace,
    type: EventType.make(
      [
        "UserMessageCommitted",
        "AssistantMessageCommitted",
        "SummaryCreated",
        "ContextRebased",
        "CompactionCompleted",
      ][offset],
    ),
    schemaVersion: schemaV1,
    durability: "durable",
    sessionId,
    eventId: EventId.make(uuid(seq)),
    createdAtMs: UnixEpochMillis.make(1715000000000),
    payload,
  });
};
const unused = () => Effect.die(new Error("Unexpected delivery or mutation of caught-up session"));
let peakHeap = 0;
let pages = 0;
let deliveredBatches = 0;
const sample = () => {
  peakHeap = Math.max(peakHeap, process.memoryUsage().heapUsed);
};
try {
  const store = await Effect.runPromise(DurableObjectSessionStore.make({ storage, sessionId }));
  const checkpoints = await Effect.runPromise(DurableObjectSinkCheckpointStore.make(storage));
  // Production serialization and sidecars, written in bounded batches to a disk-backed database.
  await Effect.runPromise(
    Effect.gen(function* () {
      let reduced = initialReducedState;
      for (let first = 1; first <= count; first += 16) {
        const committed = yield* store.append({
          entries: Array.from({ length: Math.min(16, count - first + 1) }, (_, i) => ({
            event: makeEvent(first + i),
          })),
        });
        if (phase === "checkpointed") reduced = foldReducedState(reduced, committed);
      }
      if (phase === "checkpointed")
        yield* store.saveReducerCheckpoints([
          {
            name: frameworkReducedStateReducerName,
            schemaVersion: frameworkReducedStateReducerSchemaVersion,
            throughSeq: SequenceNumber.make(count),
            payload: encodeReducedStateCheckpoint(reduced),
            updatedAtMs: 1715000000000,
          },
          {
            name: "memory.count",
            schemaVersion: 1,
            throughSeq: SequenceNumber.make(count),
            payload: count,
            updatedAtMs: 1715000000000,
          },
        ]);
      for (let i = 0; i < 7; i++)
        yield* checkpoints.commit(
          EDASinkName.make(`memory.${i}`),
          SequenceNumber.make(phase === "lagging" ? count - 5 : count),
          undefined,
        );
    }),
  );
  const originalExec = storage.sql.exec;
  storage.sql.exec = (...args) => {
    const result = originalExec(...args);
    pages++;
    sample();
    return result;
  };
  const session = SessionContext.Live(sessionId);
  const ids = IdGenerator.Live;
  const counter = EDAReducer.make({
    name: "memory.count",
    initial: 0,
    stateSchema: Schema.Number,
    reduce: (n) => n + 1,
  });
  const registry = EDASinkRegistry.Live(
    Array.from({ length: 7 }, (_, i) => ({
      name: `memory.${i}`,
      durable: {
        process: () => {
          deliveredBatches++;
          return phase === "lagging" ? Effect.void : unused();
        },
      },
    })),
  ).pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(EDASessionStore, store),
        Layer.succeed(SinkCheckpointStore, checkpoints),
        LiveEventBus.Noop,
        session,
        ids,
        EventFactory.Live.pipe(Layer.provide(Layer.merge(session, ids))),
        EDAReducerRegistry.Live([counter]),
        EDAKeepAlive.Noop,
      ),
    ),
  );
  global.gc();
  const baselineHeap = process.memoryUsage().heapUsed;
  const started = performance.now();
  let hydrationMs = 0;
  let sinkElapsedMs = 0;
  let sinkSqliteStatements = 0;
  let sinkBaselineHeapMiB = 0;
  let sinkPeakHeapMiB = 0;
  let hydrationPeakHeap = 0;
  if (phase !== "state") {
    const { SessionState } = await load("services/session-state");
    const { makeEdaTestLayer } = await load("testkit/layers");
    // Both revisions pay for the same session projection before sink initialization.
    const initialProjection = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const state = yield* SessionState;
          return yield* state.snapshotData();
        }),
      ).pipe(
        Effect.provide(
          makeEdaTestLayer({ sessionId, wrapStore: () => store, reducers: [counter] }),
        ),
      ),
    );
    assert.equal(initialProjection.reduced.lastSeq, count);
    assert.equal(initialProjection.reducerStates.get(counter.name), count);
    hydrationMs = performance.now() - started;
    hydrationPeakHeap = peakHeap;
    global.gc();
    sinkBaselineHeapMiB = process.memoryUsage().heapUsed / 1048576;
    peakHeap = process.memoryUsage().heapUsed;
    const sinkStarted = performance.now();
    const statementsBeforeSinks = pages;
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sinks = yield* EDASinkRegistry;
          const scope = yield* Effect.scope;
          yield* sinks.startSinkRunners({
            // Kept only in this JS probe so alpha.11 can run the identical fixture.
            initialHead: SequenceNumber.make(count),
            initialProjection,
            scope,
            appendDurableBatch: unused,
            publishEphemeral: unused,
          });
          for (let attempts = 0; phase === "lagging" && attempts < 2000; attempts++) {
            const cursors = yield* Effect.forEach(
              Array.from({ length: 7 }, (_, i) => i),
              (i) => checkpoints.load(EDASinkName.make(`memory.${i}`)),
            );
            if (cursors.every((checkpoint) => checkpoint.afterSeq === count)) break;
            yield* Effect.yieldNow;
          }
          sample();
          for (let i = 0; i < 7; i++)
            assert.equal(
              (yield* checkpoints.load(EDASinkName.make(`memory.${i}`))).afterSeq,
              count,
            );
        }),
      ).pipe(Effect.provide(registry)),
    );
    assert.equal(deliveredBatches > 0, phase === "lagging");
    sinkElapsedMs = performance.now() - sinkStarted;
    sinkSqliteStatements = pages - statementsBeforeSinks;
    sinkPeakHeapMiB = peakHeap / 1048576;
    peakHeap = Math.max(peakHeap, hydrationPeakHeap);
  } else {
    const { SessionState } = await load("services/session-state");
    const { makeEdaTestLayer } = await load("testkit/layers");
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const state = yield* SessionState;
          const snapshot = yield* state.snapshotData();
          assert.equal(snapshot.reduced.lastSeq, count);
          assert.equal(snapshot.reducerStates.get(counter.name), count);
          assert.equal(snapshot.reduced.messages.size, 0);
          sample();
        }),
      ).pipe(
        Effect.provide(
          makeEdaTestLayer({ sessionId, wrapStore: () => store, reducers: [counter] }),
        ),
      ),
    );
  }
  console.log(
    JSON.stringify({
      phase,
      count,
      payloadBytes,
      sinks: phase === "state" ? 0 : 7,
      deliveredBatches,
      hydrationMs,
      sinkElapsedMs,
      sinkSqliteStatements,
      sinkBaselineHeapMiB,
      sinkPeakHeapMiB,
      sqliteStatements: pages,
      baselineHeapMiB: baselineHeap / 1048576,
      peakHeapMiB: peakHeap / 1048576,
      elapsedMs: performance.now() - started,
    }),
  );
} finally {
  sqlite.close();
  rmSync(directory, { recursive: true, force: true });
}
