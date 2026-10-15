import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type { CommittedDurableEvent } from "effect-durable-agent/services/session-store";
import {
  TestLog,
  giaLikeTestMappings,
  normalizedProjection,
  projectBatches,
  testDefinition,
  testSessionId,
  transcriptScenarios,
} from "effect-durable-agent/testkit/transcript";
import {
  TranscriptRecordFromEnvelope,
  defaultTranscriptWindowLimits,
  type TranscriptDefinition,
  type TranscriptMaintenanceConfig,
  type TranscriptWindow,
} from "effect-durable-agent/transcript";

import {
  DurableObjectSessionStore,
  DurableObjectTranscriptStore,
  type DurableObjectSessionStorage,
  type DurableObjectTranscriptOptions,
  type TranscriptAlert,
} from "../../packages/effect-durable-agent-cloudflare/dist/index.js";

/**
 * Storage-level transcript scenarios run inside a real Durable Object (workerd and celld), where
 * nested `transactionSync` rollback is the platform's, not a fake's (design §13 gate 3). Each
 * step runs in one object and reports the checks that failed; the shared suite expects none.
 */

const plain = testDefinition();
const gia = testDefinition(giaLikeTestMappings);
const latest = { _tag: "Latest", count: 100 } as const;

/** Projection statements of an append: lookups, record and alias writes, the cursor update. */
const isProjectionStatement = (query: string) =>
  /_eda_transcript_(records|aliases)|SET cursor/.test(query);
const isWrite = (query: string) => /^\s*(INSERT|UPDATE|DELETE)/i.test(query);

/** Real storage, instrumented to count and fail projection statements. */
class ProbeStorage implements DurableObjectSessionStorage {
  depth = 0;
  /** Projection statements run since the last reset, in order. */
  projectionStatements: Array<string> = [];
  /** Statements run inside a nested transaction since the last reset. */
  innerStatements = 0;
  fault: ((query: string) => void) | undefined;

  constructor(private readonly storage: DurableObjectStorage) {}

  readonly sql: DurableObjectSessionStorage["sql"] = {
    exec: (query, ...bindings) => {
      if (this.depth >= 2) this.innerStatements += 1;
      if (isProjectionStatement(query)) {
        this.projectionStatements.push(query);
        this.fault?.(query);
      }
      return toSessionStorage(this.storage).sql.exec(query, ...bindings);
    },
  };

  transactionSync = <A>(closure: () => A): A => {
    this.depth += 1;
    try {
      return this.storage.transactionSync(closure);
    } finally {
      this.depth -= 1;
    }
  };
}

/** Rows are trusted to have the shape each adapter query selects. */
const isRow = <Row>(_row: unknown): _row is Row => true;

/** The adapter's storage subset over the platform storage, without a type assertion. */
const toSessionStorage = (storage: DurableObjectStorage): DurableObjectSessionStorage => ({
  sql: {
    exec: <Row>(query: string, ...bindings: ReadonlyArray<unknown>) => {
      const cursor = storage.sql.exec(query, ...bindings);
      const rows = cursor.toArray();
      return {
        one: () => {
          const [row, ...rest] = rows;
          if (row === undefined || rest.length > 0 || !isRow<Row>(row)) {
            throw new Error(`expected one row, got ${rows.length}`);
          }
          return row;
        },
        toArray: () => {
          const typed: Array<Row> = [];
          for (const row of rows) if (isRow<Row>(row)) typed.push(row);
          return typed;
        },
      };
    },
  },
  transactionSync: (closure) => storage.transactionSync(closure),
});

interface Harness {
  readonly storage: ProbeStorage;
  readonly alerts: Array<TranscriptAlert>;
  readonly activations: Array<string>;
  readonly service: DurableObjectTranscriptStore;
  readonly append: (events: ReadonlyArray<CommittedDurableEvent>) => void;
}

const harness = (
  storage: ProbeStorage,
  definitions: TranscriptMaintenanceConfig | undefined,
  sliceLimits?: DurableObjectTranscriptOptions["sliceLimits"],
): Harness => {
  const alerts: Array<TranscriptAlert> = [];
  const activations: Array<string> = [];
  const transcript: DurableObjectTranscriptOptions = {
    definitions: definitions ?? { current: plain },
    observer: {
      onAlert: (alert) => alerts.push(alert),
      onActivated: ({ version }) => activations.push(version),
    },
    ...(sliceLimits === undefined ? {} : { sliceLimits }),
  };
  const store = Effect.runSync(
    DurableObjectSessionStore.make({
      sessionId: testSessionId,
      storage,
      ...(definitions === undefined ? {} : { transcript }),
    }),
  );
  return {
    storage,
    alerts,
    activations,
    service: new DurableObjectTranscriptStore({ sessionId: testSessionId, storage, transcript }),
    append: (events) =>
      Effect.runSync(
        store.append({
          entries: events.map(({ event }) => ({
            event: event.namespace === "__foreign" ? event.envelope : event,
          })),
        }),
      ),
  };
};

const conversation = (turns: number): TestLog => {
  const log = new TestLog().configure();
  for (let turn = 1; turn <= turns; turn += 1) {
    log.submit({ command: turn, message: turn, text: `ping ${turn}` });
    log.startRun({ run: turn, commands: [turn] });
    log.assistantTurn({
      run: turn,
      turn,
      inference: turn,
      message: 1_000 + turn,
      text: `pong ${turn}`,
      inputs: [turn],
      ...(turn % 2 === 0 ? { tools: [turn] } : {}),
    });
    log.runCompleted(turn, [turn]);
  }
  return log;
};

const count = (storage: ProbeStorage, query: string): number =>
  storage.sql.exec<{ readonly count: number }>(query).one().count;

const storedProjection = (storage: ProbeStorage, role = "active") => {
  const meta = storage.sql
    .exec<{
      readonly slot: number;
      readonly version: string;
      readonly cursor: number;
      readonly status: string;
    }>("SELECT slot, version, cursor, status FROM _eda_transcript_meta WHERE role = ?", role)
    .toArray()[0];
  if (meta === undefined) return undefined;
  return {
    meta: { version: meta.version, role, cursor: meta.cursor, status: { _tag: "Ok" } },
    failed: meta.status === "failed",
    records: storage.sql
      .exec<{ readonly envelope_json: string }>(
        "SELECT envelope_json FROM _eda_transcript_records WHERE slot = ? ORDER BY id",
        meta.slot,
      )
      .toArray()
      .map(({ envelope_json }) => ({
        ...Schema.encodeSync(TranscriptRecordFromEnvelope)(
          Schema.decodeUnknownSync(TranscriptRecordFromEnvelope)(JSON.parse(envelope_json)),
        ),
        updatedSeq: 0,
      })),
    aliases: storage.sql
      .exec<{ readonly namespace: string; readonly key: string; readonly record_id: string }>(
        "SELECT namespace, key, record_id FROM _eda_transcript_aliases WHERE slot = ?",
        meta.slot,
      )
      .toArray()
      .map(({ namespace, key, record_id }) => [`${namespace}\u0000${key}`, record_id])
      .sort(([left = ""], [right = ""]) => (left < right ? -1 : left > right ? 1 : 0)),
  };
};

const reference = (
  definition: TranscriptDefinition,
  events: ReadonlyArray<CommittedDurableEvent>,
) => {
  const projection = normalizedProjection(projectBatches(definition, [events]));
  return { ...projection, meta: { ...projection.meta, role: "active" }, failed: false };
};

const windowOf = (service: DurableObjectTranscriptStore): TranscriptWindow => {
  const window = service.readWindow(latest, defaultTranscriptWindowLimits);
  if (Result.isFailure(window)) throw window.failure;
  return window.success;
};

const drain = (service: DurableObjectTranscriptStore): number => {
  let slices = 1;
  while (service.runSlice()) {
    slices += 1;
    if (slices > 1_000) throw new Error("builder did not converge");
  }
  return slices;
};

class Checks {
  readonly failures: Array<string> = [];
  equal(name: string, actual: unknown, expected: unknown): void {
    const left = canonical(actual);
    const right = canonical(expected);
    if (left !== right)
      this.failures.push(`${name}: expected ${right.slice(0, 400)}, got ${left.slice(0, 400)}`);
  }
}

const canonical = (value: unknown): string =>
  JSON.stringify(value, (_key, entry: unknown) =>
    entry !== null && typeof entry === "object" && !Array.isArray(entry)
      ? Object.fromEntries(Object.entries(entry).sort(([left], [right]) => (left < right ? -1 : 1)))
      : entry,
  ) ?? "undefined";

/** The result of one probe step. */
export interface TranscriptProbeResult {
  readonly failures: ReadonlyArray<string>;
  readonly info: Record<string, number>;
}

/** A realistic 10k-event log (user turns, assistant text, every other turn a tool). */
const perfLog = (): ReadonlyArray<CommittedDurableEvent> => {
  const log = new TestLog().configure();
  for (let turn = 1; log.events.length < 10_000; turn += 1) {
    log.submit({ command: turn, message: turn, text: `ping ${turn} `.repeat(20) });
    log.startRun({ run: turn, commands: [turn] });
    log.assistantTurn({
      run: turn,
      turn,
      inference: turn,
      message: 100_000 + turn,
      text: `pong ${turn} `.repeat(40),
      inputs: [turn],
      ...(turn % 2 === 0 ? { tools: [turn] } : {}),
    });
    log.runCompleted(turn, [turn]);
  }
  return log.events.slice(0, 10_000);
};

const faultLog = conversation(3);
const faultPrefix = faultLog.events.slice(0, 10);
const faultBatch = faultLog.events.slice(10, 20);
const faultLater = faultLog.events.slice(20);

/**
 * Run one named probe step against this object's storage.
 *
 * Steps: `oracle-count`, `oracle:<index>`, `fault-count`, `fault:<n>`, `build-start`,
 * `build-resume`, `activate`, `discard-and-repair`, `missed-session`, `init`, and the
 * measurement steps `perf-append:on|off` (10k events in two-event appends) and `perf-build`
 * (build the `perf-append:off` object's log), which the caller times from outside because
 * Workers clocks do not advance during synchronous execution.
 */
export const runTranscriptProbe = (
  durableStorage: DurableObjectStorage,
  step: string,
): TranscriptProbeResult => {
  const storage = new ProbeStorage(durableStorage);
  const checks = new Checks();
  const info: Record<string, number> = {};
  const [name, argument] = step.split(":");

  switch (name) {
    case "oracle-count":
      info.scenarios = transcriptScenarios().length;
      break;
    case "oracle": {
      const scenario = transcriptScenarios()[Number(argument)];
      if (scenario === undefined) throw new Error(`no scenario ${argument}`);
      const probe = harness(storage, { current: gia });
      for (const event of scenario.events) probe.append([event]);
      checks.equal(`${scenario.name} alerts`, probe.alerts, []);
      checks.equal(
        `${scenario.name} projection`,
        storedProjection(storage),
        reference(gia, scenario.events),
      );
      break;
    }
    case "fault-count": {
      const probe = harness(storage, { current: plain });
      probe.append(faultPrefix);
      storage.projectionStatements = [];
      storage.innerStatements = 0;
      probe.append(faultBatch);
      info.projectionStatements = storage.projectionStatements.length;
      // Zero where the platform does not nest transactions (celld).
      info.innerStatements = storage.innerStatements;
      break;
    }
    case "fault": {
      const failAt = Number(argument);
      const probe = harness(storage, { current: plain });
      probe.append(faultPrefix);
      const lastGood = storedProjection(storage);
      const lastGoodWindow = windowOf(probe.service);
      storage.projectionStatements = [];
      let failed = "";
      storage.fault = (query) => {
        if (storage.projectionStatements.length === failAt) {
          failed = query;
          throw new Error(`injected failure ${failAt}`);
        }
      };
      let appendFailed = false;
      try {
        probe.append(faultBatch);
      } catch {
        appendFailed = true;
      }
      storage.fault = undefined;
      info.writeFault = isWrite(failed) ? 1 : 0;
      if (appendFailed) {
        // Without nested transactions a failing write is a storage failure: the whole append
        // rolls back atomically, events included, and nothing is published.
        info.appendFailed = 1;
        checks.equal(
          "append rolled back",
          count(storage, "SELECT COUNT(*) AS count FROM _eda_event_log"),
          10,
        );
        checks.equal("projection untouched", storedProjection(storage), lastGood);
        checks.equal("nothing published", probe.alerts, []);
        probe.append(faultBatch);
        probe.append(faultLater);
        checks.equal(
          "maintained after retry",
          storedProjection(storage),
          reference(plain, faultLog.events),
        );
        break;
      }
      checks.equal(
        "events committed",
        count(storage, "SELECT COUNT(*) AS count FROM _eda_event_log"),
        20,
      );
      const frozen = storedProjection(storage);
      checks.equal("projection rolled back", { ...frozen, failed: false }, lastGood);
      checks.equal("failure recorded", frozen?.failed, true);
      checks.equal(
        "alert published",
        probe.alerts.map((alert) => alert._tag),
        ["ProjectionFailed"],
      );
      probe.append(faultLater);
      checks.equal(
        "later events committed",
        count(storage, "SELECT COUNT(*) AS count FROM _eda_event_log"),
        faultLog.events.length,
      );
      checks.equal("frozen permanently", storedProjection(storage), frozen);
      drain(probe.service);
      checks.equal("builder leaves it frozen", storedProjection(storage), frozen);
      const degraded = windowOf(probe.service);
      checks.equal("degraded readiness", degraded.readiness._tag, "Degraded");
      checks.equal(
        "last good cut",
        { ...degraded, readiness: lastGoodWindow.readiness },
        lastGoodWindow,
      );
      break;
    }
    case "build-start": {
      const log = conversation(6);
      harness(storage, undefined).append(log.events);
      const probe = harness(storage, { current: gia }, { maxEvents: 5, pageEvents: 5 });
      checks.equal("building readiness", windowOf(probe.service).readiness._tag, "Building");
      probe.service.runSlice();
      info.cursor = count(
        storage,
        "SELECT cursor AS count FROM _eda_transcript_meta WHERE role = 'building'",
      );
      break;
    }
    case "build-resume": {
      const log = conversation(6);
      const probe = harness(storage, { current: gia }, { maxEvents: 5, pageEvents: 5 });
      info.cursorAtResume = count(
        storage,
        "SELECT cursor AS count FROM _eda_transcript_meta WHERE role = 'building'",
      );
      info.slices = drain(probe.service);
      checks.equal("built projection", storedProjection(storage), reference(gia, log.events));
      checks.equal("activated", probe.activations, [gia.version]);
      checks.equal("ready", windowOf(probe.service).readiness._tag, "Ready");
      break;
    }
    case "activate": {
      const log = conversation(12);
      harness(storage, { current: plain }).append(log.events.slice(0, 40));
      const probe = harness(
        storage,
        { current: gia, previous: plain },
        { maxEvents: 8, pageEvents: 4, maxDeletedRows: 5 },
      );
      let appended = 40;
      while (probe.activations.length === 0) {
        checks.equal(
          "active serves while building",
          windowOf(probe.service).readiness._tag,
          "Ready",
        );
        if (appended < log.events.length) {
          probe.append(log.events.slice(appended, appended + 3));
          appended += 3;
        }
        probe.service.runSlice();
      }
      info.appendedBeforeActivation = appended;
      probe.append(log.events.slice(appended));
      checks.equal("activated projection", storedProjection(storage), reference(gia, log.events));
      info.cleanupSlices = drain(probe.service);
      checks.equal(
        "old version deleted",
        count(storage, "SELECT COUNT(*) AS count FROM _eda_transcript_meta"),
        1,
      );
      checks.equal("converged", probe.service.status().converged, true);
      break;
    }
    case "discard-and-repair": {
      const log = conversation(6);
      harness(storage, { current: plain }).append(log.events);
      const release1 = harness(
        storage,
        { current: gia, previous: plain },
        { maxEvents: 4, pageEvents: 4 },
      );
      release1.service.runSlice();
      storage.projectionStatements = [];
      storage.fault = (query) => {
        const reads = storage.projectionStatements.filter((statement) => !isWrite(statement));
        if (!isWrite(query) && reads.length === 3) throw new Error("injected build failure");
      };
      release1.service.runSlice();
      storage.fault = undefined;
      checks.equal(
        "build failed",
        release1.alerts.map((alert) => alert._tag),
        ["ProjectionFailed"],
      );
      const failed = storedProjection(storage, "building");
      checks.equal("failed build not pending", release1.service.hasPendingWork(), false);
      release1.service.runSlice();
      checks.equal("failed build never retried", storedProjection(storage, "building"), failed);
      const repairDefinition = testDefinition({
        ...giaLikeTestMappings,
        userFallbacks: [{ to: "author", fromSession: "owner" }],
      });
      const repair = harness(
        storage,
        { current: repairDefinition, previous: gia },
        { maxDeletedRows: 3 },
      );
      drain(repair.service);
      checks.equal(
        "repaired projection",
        storedProjection(storage),
        reference(repairDefinition, log.events),
      );
      checks.equal(
        "one version left",
        count(storage, "SELECT COUNT(*) AS count FROM _eda_transcript_meta"),
        1,
      );
      break;
    }
    case "missed-session": {
      const log = conversation(6);
      harness(storage, { current: plain }).append(log.events.slice(0, 30));
      const probe = harness(storage, { current: gia });
      probe.append(log.events.slice(30));
      checks.equal(
        "alert",
        probe.alerts.map((alert) => alert._tag),
        ["MissedSession"],
      );
      const frozen = windowOf(probe.service);
      checks.equal("updating history", frozen.readiness._tag, "UpdatingHistory");
      checks.equal("frozen cut", frozen.throughSeq, 30);
      drain(probe.service);
      checks.equal("rebuilt", storedProjection(storage), reference(gia, log.events));
      break;
    }
    case "init": {
      const probe = harness(storage, { current: plain });
      const window = windowOf(probe.service);
      checks.equal(
        "brand-new log is ready at seq 0",
        [window.readiness._tag, window.throughSeq],
        ["Ready", 0],
      );
      const log = conversation(2);
      probe.append(log.events);
      checks.equal("maintained", storedProjection(storage), reference(plain, log.events));
      break;
    }
    case "perf-append": {
      const events = perfLog();
      const probe = harness(storage, argument === "on" ? { current: gia } : undefined);
      for (let index = 0; index < events.length; index += 2) {
        probe.append(events.slice(index, index + 2));
      }
      info.appends = events.length / 2;
      break;
    }
    case "perf-build": {
      const probe = harness(storage, { current: gia });
      info.slices = drain(probe.service);
      checks.equal("built", probe.service.status().converged, true);
      break;
    }
    default:
      throw new Error(`unknown transcript probe step ${step}`);
  }
  return { failures: checks.failures, info };
};
