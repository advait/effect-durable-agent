import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  EDASessionStore,
  type CommittedDurableEvent,
  type EDASessionStoreShape,
} from "effect-durable-agent/services/session-store";
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
  EDATranscriptStore,
  TranscriptRecordFromEnvelope,
  defaultTranscriptWindowLimits,
  type TranscriptDefinition,
  type TranscriptMaintenanceConfig,
  type TranscriptWindow,
} from "effect-durable-agent/transcript";
import { SessionId } from "effect-durable-agent/types/core";
import { DurableObjectAlarmScheduler } from "./durable-object-alarms";
import { DurableObjectKeepAlive } from "./durable-object-keepalive";
import { DurableObjectSessionStore } from "./durable-object-store";
import { DurableObjectTranscriptStore } from "./durable-object-transcript";
import type {
  DurableObjectTranscriptOptions,
  TranscriptAlert,
} from "./durable-object-transcript-maintenance";
import { SqliteDurableObjectStorage } from "./testing/sqlite-durable-object-storage";

const plain = testDefinition();
const gia = testDefinition(giaLikeTestMappings);
const latest = { _tag: "Latest", count: 100 } as const;

interface Harness {
  readonly storage: SqliteDurableObjectStorage;
  readonly alerts: Array<{ readonly alert: TranscriptAlert; readonly depth: number }>;
  readonly activations: Array<string>;
  readonly transcript: DurableObjectTranscriptOptions;
  readonly store: EDASessionStoreShape;
  readonly service: DurableObjectTranscriptStore;
  /** Append events through the session store, as the runtime does. */
  readonly append: (events: ReadonlyArray<CommittedDurableEvent>) => void;
  /** The same storage under another release's definitions (a new isolate after a deploy). */
  readonly release: (
    definitions: TranscriptMaintenanceConfig | undefined,
    sliceLimits?: DurableObjectTranscriptOptions["sliceLimits"],
  ) => Harness;
}

const makeHarness = (
  definitions: TranscriptMaintenanceConfig | undefined,
  sliceLimits: DurableObjectTranscriptOptions["sliceLimits"] = undefined,
  storage = new SqliteDurableObjectStorage(),
): Harness => {
  const alerts: Harness["alerts"] = [];
  const activations: Array<string> = [];
  const transcript: DurableObjectTranscriptOptions = {
    definitions: definitions ?? { current: plain },
    observer: {
      onAlert: (alert) => alerts.push({ alert, depth: storage.depth }),
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
    transcript,
    store,
    service: new DurableObjectTranscriptStore({ sessionId: testSessionId, storage, transcript }),
    append: (events) => {
      Effect.runSync(
        store.append({ entries: events.map(({ event }) => ({ event: envelope(event) })) }),
      );
    },
    release: (next, limits) => makeHarness(next, limits, storage),
  };
};

const envelope = (event: CommittedDurableEvent["event"]) =>
  event.namespace === "__foreign" ? event.envelope : event;

const runToCompletion = (service: DurableObjectTranscriptStore, maxSlices = 1_000): number => {
  let slices = 0;
  while (service.runSlice()) {
    slices += 1;
    if (slices > maxSlices) throw new Error("builder did not converge");
  }
  return slices + 1;
};

/** The stored projection of the active (or given) slot, in `normalizedProjection` form. */
const storedProjection = (storage: SqliteDurableObjectStorage, role = "active") => {
  const [meta] = storage.rows<{
    slot: number;
    version: string;
    cursor: number;
    status: string;
    failed_at_seq: number | null;
    error: string | null;
  }>("SELECT * FROM _eda_transcript_meta WHERE role = ?", role);
  if (meta === undefined) throw new Error(`no ${role} slot`);
  return {
    meta: {
      version: meta.version,
      role,
      cursor: meta.cursor,
      status:
        meta.status === "ok"
          ? { _tag: "Ok" }
          : { _tag: "Failed", failedAtSeq: meta.failed_at_seq, error: meta.error },
    },
    records: storage
      .rows<{ envelope_json: string }>(
        "SELECT envelope_json FROM _eda_transcript_records WHERE slot = ? ORDER BY id",
        meta.slot,
      )
      .map(({ envelope_json }) => ({
        ...Schema.encodeSync(TranscriptRecordFromEnvelope)(
          Schema.decodeUnknownSync(TranscriptRecordFromEnvelope)(JSON.parse(envelope_json)),
        ),
        updatedSeq: 0,
      })),
    aliases: storage
      .rows<{ namespace: string; key: string; record_id: string }>(
        "SELECT namespace, key, record_id FROM _eda_transcript_aliases WHERE slot = ?",
        meta.slot,
      )
      .map(({ namespace, key, record_id }) => [`${namespace}\u0000${key}`, record_id] as const)
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0)),
  };
};

const referenceProjection = (
  definition: TranscriptDefinition,
  events: ReadonlyArray<CommittedDurableEvent>,
) => {
  const reference = normalizedProjection(projectBatches(definition, [events]));
  return { ...reference, meta: { ...reference.meta, role: "active" } };
};

const windowOf = (service: DurableObjectTranscriptStore): TranscriptWindow => {
  const window = service.readWindow(latest, defaultTranscriptWindowLimits);
  if (Result.isFailure(window)) throw window.failure;
  return window.success;
};

const referenceWindow = (
  definition: TranscriptDefinition,
  events: ReadonlyArray<CommittedDurableEvent>,
): TranscriptWindow =>
  Effect.runSync(
    Effect.gen(function* () {
      const transcript = yield* EDATranscriptStore;
      return yield* transcript.readTranscriptWindow(latest, defaultTranscriptWindowLimits);
    }).pipe(
      Effect.provide(
        EDASessionStore.InMemorySeeded(
          testSessionId,
          events.map(({ event }) => envelope(event)),
          { transcript: { current: definition } },
        ),
      ),
    ),
  );

const withoutIncarnation = ({ incarnation: _incarnation, ...window }: TranscriptWindow) => window;

const eventCount = (storage: SqliteDurableObjectStorage): number | undefined =>
  storage.rows<{ count: number }>("SELECT COUNT(*) AS count FROM _eda_event_log")[0]?.count;

const transcriptRowCount = (storage: SqliteDurableObjectStorage): number | undefined =>
  storage.rows<{ count: number }>(
    "SELECT (SELECT COUNT(*) FROM _eda_transcript_records) + (SELECT COUNT(*) FROM _eda_transcript_aliases) AS count",
  )[0]?.count;

/** A small conversation: two user turns, one with a tool call. */
const conversation = (turns: number, from = 1): TestLog => {
  const log = new TestLog().configure();
  for (let turn = from; turn < from + turns; turn += 1) {
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

/** Deterministic batch partition of a log. */
const partition = <A>(items: ReadonlyArray<A>, seed: number): Array<Array<A>> => {
  const batches: Array<Array<A>> = [];
  let state = seed;
  let index = 0;
  while (index < items.length) {
    state = (state * 1_103_515_245 + 12_345) % 2_147_483_648;
    const size = 1 + (state % 5);
    batches.push(items.slice(index, index + size));
    index += size;
  }
  return batches;
};

describe("Durable Object transcript maintenance", () => {
  describe("oracle: the SQL projection equals the reference projection", () => {
    for (const scenario of transcriptScenarios()) {
      for (const [label, batches] of [
        ["one event per batch", scenario.events.map((event) => [event])],
        ["one batch", [scenario.events]],
        ["seeded partition", partition(scenario.events, scenario.name.length)],
      ] as const) {
        it(`${scenario.name}, ${label}`, () => {
          const harness = makeHarness({ current: gia });
          for (const batch of batches) harness.append(batch);
          expect(storedProjection(harness.storage)).toEqual(
            referenceProjection(gia, scenario.events),
          );
          expect(withoutIncarnation(windowOf(harness.service))).toEqual(
            withoutIncarnation(referenceWindow(gia, scenario.events)),
          );
        });
      }

      it(`${scenario.name}, built from the log in slices`, () => {
        const legacy = makeHarness(undefined);
        legacy.append(scenario.events);
        const harness = legacy.release({ current: gia }, { maxEvents: 3, pageEvents: 2 });
        expect(windowOf(harness.service).readiness._tag).toBe("Building");
        runToCompletion(harness.service);
        expect(storedProjection(harness.storage)).toEqual(
          referenceProjection(gia, scenario.events),
        );
      });
    }
  });

  it("initializes a brand-new log at seq 0 before its first read or append", () => {
    const harness = makeHarness({ current: plain });
    const window = windowOf(harness.service);
    expect(window.readiness).toEqual({ _tag: "Ready" });
    expect(window.throughSeq).toBe(0);
    expect(harness.service.readMeta()).toEqual([
      { version: plain.version, role: "active", cursor: 0, status: { _tag: "Ok" } },
    ]);
    expect(harness.service.hasPendingWork()).toBe(false);

    const log = conversation(2);
    harness.append(log.events);
    expect(storedProjection(harness.storage)).toEqual(referenceProjection(plain, log.events));
  });

  it("maintains nothing for idempotent duplicates and keeps the append atomic", () => {
    const harness = makeHarness({ current: plain });
    const log = conversation(1);
    harness.append(log.events);
    const before = storedProjection(harness.storage);
    harness.append(log.events);
    expect(storedProjection(harness.storage)).toEqual(before);

    const [first, second] = conversation(2).events.slice(log.events.length);
    if (first === undefined || second === undefined) throw new Error("conversation too short");
    const otherSession = {
      ...envelope(first.event),
      sessionId: SessionId.make("018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a"),
    };
    expect(() =>
      Effect.runSync(
        harness.store.append({
          entries: [{ event: envelope(second.event) }, { event: otherSession }],
        }),
      ),
    ).toThrow();
    expect(eventCount(harness.storage)).toBe(log.events.length);
    expect(storedProjection(harness.storage)).toEqual(before);
  });

  describe("projection failure freezes only that version and never fails the append", () => {
    const log = conversation(3);
    const prefix = log.events.slice(0, 10);
    const batch = log.events.slice(10, 20);
    const later = log.events.slice(20);

    // Count the statements maintenance runs inside its inner transaction for this batch, then
    // fail each of them in turn: every write, and every lookup of a later batch entry.
    const probe = makeHarness({ current: plain });
    probe.append(prefix);
    let innerStatements = 0;
    probe.storage.faults = () => {
      if (probe.storage.depth >= 2) innerStatements += 1;
    };
    probe.append(batch);
    expect(innerStatements).toBeGreaterThan(10);

    for (let failAt = 1; failAt <= innerStatements; failAt += 1) {
      it(`at inner statement ${failAt} of ${innerStatements}`, () => {
        const harness = makeHarness({ current: plain });
        harness.append(prefix);
        const lastGood = storedProjection(harness.storage);
        const lastGoodWindow = windowOf(harness.service);
        let seen = 0;
        harness.storage.faults = (query) => {
          if (harness.storage.depth >= 2 && ++seen === failAt) {
            throw new Error(`injected failure at ${query.trim().split(/\s+/, 3).join(" ")}`);
          }
        };
        harness.append(batch);
        harness.storage.faults = undefined;

        // Events committed; the projection rolled back to its last good cut; failure recorded.
        expect(eventCount(harness.storage)).toBe(prefix.length + batch.length);
        const frozen = storedProjection(harness.storage);
        expect({ ...frozen, meta: { ...frozen.meta, status: { _tag: "Ok" } } }).toEqual(lastGood);
        expect(frozen.meta.status).toMatchObject({
          _tag: "Failed",
          failedAtSeq: prefix.length + 1,
        });
        // Published once, after the outer transaction committed.
        expect(harness.alerts).toHaveLength(1);
        expect(harness.alerts[0]).toMatchObject({ depth: 0, alert: { _tag: "ProjectionFailed" } });

        // Frozen permanently: later appends commit events but never touch the version.
        harness.append(later);
        expect(eventCount(harness.storage)).toBe(log.events.length);
        expect(storedProjection(harness.storage)).toEqual(frozen);
        expect(harness.service.hasPendingWork()).toBe(false);
        expect(runToCompletion(harness.service)).toBe(1);
        expect(storedProjection(harness.storage)).toEqual(frozen);

        // The degraded read serves the last good cut and nothing newer.
        const degraded = windowOf(harness.service);
        expect(degraded.readiness).toMatchObject({
          _tag: "Degraded",
          failedAtSeq: prefix.length + 1,
        });
        expect({ ...degraded, readiness: lastGoodWindow.readiness }).toEqual(lastGoodWindow);
      });
    }
  });

  it("builds a new definition beside the active one, activates at the head under concurrent appends, and deletes the old rows", () => {
    const log = conversation(12);
    const firstRelease = makeHarness({ current: plain });
    firstRelease.append(log.events.slice(0, 40));

    const harness = firstRelease.release(
      { current: gia, previous: plain },
      { maxEvents: 8, pageEvents: 4, maxDeletedRows: 5 },
    );
    expect(harness.service.readMeta().map(({ role, version }) => [role, version])).toEqual([
      ["active", plain.version],
      ["building", gia.version],
    ]);

    // Appends keep landing between slices: the active version stays maintained in the append
    // and serves, and the build chases the head.
    let appended = 40;
    while (harness.activations.length === 0) {
      expect(windowOf(harness.service).readiness).toEqual({ _tag: "Ready" });
      if (appended < log.events.length) {
        harness.append(log.events.slice(appended, appended + 3));
        appended += 3;
        expect(storedProjection(harness.storage).meta.cursor).toBe(
          Math.min(appended, log.events.length),
        );
      }
      harness.service.runSlice();
    }
    expect(harness.activations).toEqual([gia.version]);
    expect(appended).toBeLessThan(log.events.length);

    // Activated exactly at the head; later appends maintain the new active version.
    harness.append(log.events.slice(appended));
    expect(storedProjection(harness.storage)).toEqual(referenceProjection(gia, log.events));
    expect(harness.service.hasPendingWork()).toBe(true);

    // The old version is deleted in bounded slices, then nothing is pending.
    const slices = runToCompletion(harness.service);
    expect(slices).toBeGreaterThan(2);
    expect(harness.service.readMeta()).toEqual([
      { version: gia.version, role: "active", cursor: log.events.length, status: { _tag: "Ok" } },
    ]);
    expect(harness.storage.rows("SELECT * FROM _eda_transcript_meta")).toHaveLength(1);
    expect(harness.service.status()).toMatchObject({ converged: true, pending: false });
  });

  it("resumes a build from its committed cursor after eviction", () => {
    const log = conversation(6);
    const legacy = makeHarness(undefined);
    legacy.append(log.events);

    const limits = { maxEvents: 5, pageEvents: 5 };
    let instance = legacy.release({ current: gia }, limits);
    const cursors: Array<number> = [];
    while (instance.service.runSlice()) {
      cursors.push(
        instance.storage.rows<{ cursor: number }>(
          "SELECT cursor FROM _eda_transcript_meta WHERE role = 'building'",
        )[0]?.cursor ?? -1,
      );
      // A fresh object instance over the same storage: no in-memory builder state survives.
      instance = instance.release({ current: gia }, limits);
    }
    expect(cursors.slice(0, 3)).toEqual([5, 10, 15]);
    expect(storedProjection(instance.storage)).toEqual(referenceProjection(gia, log.events));
  });

  it("discards a persisted build that is not current and never retries a failed build", () => {
    const log = conversation(6);
    const base = makeHarness({ current: plain });
    base.append(log.events);
    const repairDefinition = testDefinition({
      ...giaLikeTestMappings,
      userFallbacks: [{ to: "author", fromSession: "owner" }],
    });

    // Release 1 starts building `gia`, then fails it partway.
    const release1 = base.release(
      { current: gia, previous: plain },
      { maxEvents: 4, pageEvents: 4 },
    );
    release1.service.runSlice();
    let builds = 0;
    release1.storage.faults = () => {
      if (release1.storage.depth >= 2 && ++builds === 3) throw new Error("injected build failure");
    };
    release1.service.runSlice();
    release1.storage.faults = undefined;
    expect(release1.alerts.map(({ alert }) => alert)).toMatchObject([
      { _tag: "ProjectionFailed", role: "building", version: gia.version },
    ]);
    const failed = storedProjection(release1.storage, "building");
    expect(release1.service.hasPendingWork()).toBe(false);
    release1.service.runSlice();
    expect(storedProjection(release1.storage, "building")).toEqual(failed);
    expect(windowOf(release1.service).readiness).toEqual({ _tag: "Ready" });

    // The repair release discards the failed build and rebuilds its own definition from the log.
    const repair = base.release(
      { current: repairDefinition, previous: gia },
      { maxDeletedRows: 3 },
    );
    expect(repair.service.readMeta().map(({ role, version }) => [role, version])).toEqual([
      ["active", plain.version],
      ["building", repairDefinition.version],
    ]);
    runToCompletion(repair.service);
    expect(storedProjection(repair.storage)).toEqual(
      referenceProjection(repairDefinition, log.events),
    );
    expect(repair.storage.rows("SELECT * FROM _eda_transcript_meta")).toHaveLength(1);
    expect(transcriptRowCount(repair.storage)).toBe(
      repair.storage.rows("SELECT * FROM _eda_transcript_records").length +
        repair.storage.rows("SELECT * FROM _eda_transcript_aliases").length,
    );
  });

  it("serves a missed session's frozen cut as updating history, builds current at once and alerts", () => {
    const log = conversation(6);
    const old = makeHarness({ current: plain });
    old.append(log.events.slice(0, 30));
    const lastGood = windowOf(old.service);

    // This release carries neither the active version as current nor as previous.
    const harness = old.release({ current: gia });
    expect(harness.service.hasPendingWork()).toBe(true);
    harness.append(log.events.slice(30));
    expect(harness.alerts.map(({ alert }) => alert)).toEqual([
      { _tag: "MissedSession", activeVersion: plain.version, currentVersion: gia.version },
    ]);
    const frozen = windowOf(harness.service);
    expect(frozen.readiness).toEqual({ _tag: "UpdatingHistory" });
    expect(frozen.throughSeq).toBe(30);
    expect({ ...frozen, readiness: lastGood.readiness }).toEqual(lastGood);

    runToCompletion(harness.service);
    expect(harness.activations).toEqual([gia.version]);
    expect(windowOf(harness.service).readiness).toEqual({ _tag: "Ready" });
    expect(storedProjection(harness.storage)).toEqual(referenceProjection(gia, log.events));
  });

  it("serves an undecodable stored row as an unreadable empty window", () => {
    const harness = makeHarness({ current: plain });
    harness.append(conversation(1).events);
    harness.storage.rows(
      'UPDATE _eda_transcript_records SET envelope_json = \'{"format":1,"kind":"user","id":"x","updatedSeq":1,"body":{}}\' WHERE kind = \'user\'',
    );
    const window = windowOf(harness.service);
    expect(window.readiness._tag).toBe("Unreadable");
    expect(window.records).toEqual([]);
  });

  it("owns the log incarnation, adopting a host's value or sharing the one it mints", () => {
    const harness = makeHarness({ current: plain });
    const minted = windowOf(harness.service).incarnation;
    expect(minted).toMatch(/^[0-9a-f-]{36}$/);
    expect(harness.service.status().incarnation).toBe(minted);

    const kv = new Map<string, unknown>([["gia.log_incarnation", "host-incarnation"]]);
    const kvStorage = {
      get: (key: string) => kv.get(key),
      put: (key: string, value: string) => kv.set(key, value),
    };
    const storage = new SqliteDurableObjectStorage();
    Effect.runSync(DurableObjectSessionStore.migrate(storage));
    const adopting = new DurableObjectTranscriptStore({
      sessionId: testSessionId,
      storage,
      transcript: {
        definitions: { current: plain },
        adoptIncarnation: { key: "gia.log_incarnation", kv: kvStorage },
      },
    });
    expect(windowOf(adopting).incarnation).toBe("host-incarnation");
    kv.set("gia.log_incarnation", "changed-later");
    expect(windowOf(adopting).incarnation).toBe("host-incarnation");

    kv.clear();
    const fresh = new SqliteDurableObjectStorage();
    Effect.runSync(DurableObjectSessionStore.migrate(fresh));
    const minting = new DurableObjectTranscriptStore({
      sessionId: testSessionId,
      storage: fresh,
      transcript: {
        definitions: { current: plain },
        adoptIncarnation: { key: "gia.log_incarnation", kv: kvStorage },
      },
    });
    const own = windowOf(minting).incarnation;
    expect(kv.get("gia.log_incarnation")).toBe(own);
    expect(own).not.toBe(minted);
  });
});

describe("DurableObjectAlarmScheduler", () => {
  const scheduler = () => {
    const storage = new SqliteDurableObjectStorage();
    Effect.runSync(DurableObjectSessionStore.migrate(storage));
    return { storage, alarms: new DurableObjectAlarmScheduler(storage) };
  };

  it("never lets the idle keep-alive delete the builder's wakeup", async () => {
    const { storage, alarms } = scheduler();
    const keepAlive = new DurableObjectKeepAlive(alarms.owner("runtime"), undefined, {
      intervalMs: 5_000,
      now: () => 1_000,
    });
    await alarms.request("transcript", 500);
    const lease = await keepAlive.acquire();
    expect(storage.alarm).toBe(500);
    await lease.release();
    expect(storage.alarm).toBe(500);

    expect(alarms.takeDue(600)).toEqual(new Set(["transcript"]));
    await alarms.sync();
    expect(storage.alarm).toBeNull();
  });

  it("keeps the earliest wakeup of every owner and reports which owners an alarm fired for", async () => {
    const { storage, alarms } = scheduler();
    await alarms.request("runtime", 2_000);
    await alarms.request("transcript", 3_000);
    expect(storage.alarm).toBe(2_000);
    await alarms.request("transcript", 4_000);
    expect(alarms.owner("transcript").getAlarm()).toBe(3_000);

    expect(alarms.takeDue(2_000)).toEqual(new Set(["runtime"]));
    await alarms.sync();
    expect(storage.alarm).toBe(3_000);
    // An alarm that fires early still belongs to the earliest owner.
    expect(alarms.takeDue(1_000)).toEqual(new Set(["transcript"]));
    await alarms.owner("runtime").deleteAlarm();
    expect(storage.alarm).toBeNull();
  });
});
