import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vite-plus/test";

import { EDASessionStore } from "../services/session-store";
import { SequenceNumber } from "../types/core";
import type { DurableEventEnvelope } from "../types/events/envelope";
import {
  convergeTranscripts,
  emptyTranscriptVersion,
  inMemoryTranscriptWindowSource,
  initialTranscriptState,
  type InMemoryTranscriptState,
} from "./in-memory";
import {
  TranscriptLogIncarnation,
  TranscriptOrder,
  isTopLevelTranscriptRecord,
  transcriptStoredInlineBodyLimit,
  type TranscriptCursor,
  type TranscriptRecordView,
} from "./records";
import { EDATranscriptStore } from "./store";
import { giaLikeTestMappings, transcriptScenarios } from "./transcript-scenarios";
import { TestLog, ids, projectBatches, testDefinition, testSessionId } from "./transcript-testkit";
import {
  TranscriptWindow,
  defaultTranscriptWindowLimits,
  transcriptCursorOf,
  type TranscriptWindowSelector,
} from "./window";
import { inactiveTranscriptWindow, readTranscriptWindowFrom } from "./window-read";

const gia = testDefinition(giaLikeTestMappings);
const plain = testDefinition();
const incarnation = TranscriptLogIncarnation.make("test");

const scenarioLog = (name: string) => {
  const found = transcriptScenarios().find((scenario) => scenario.name === name);
  if (found === undefined) throw new Error(`unknown scenario ${name}`);
  return found.events;
};

/** Run against an in-memory store, appending the envelopes in the given batches first. */
const withStore = <A>(
  batches: ReadonlyArray<ReadonlyArray<DurableEventEnvelope>>,
  use: Effect.Effect<A, unknown, EDASessionStore | EDATranscriptStore>,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* EDASessionStore;
      for (const batch of batches) {
        yield* store.append({ entries: batch.map((event) => ({ event })) });
      }
      return yield* use;
    }).pipe(
      Effect.provide(EDASessionStore.InMemory(testSessionId, { transcript: { current: gia } })),
    ),
  );

const readWindow = (selector: TranscriptWindowSelector, limits = defaultTranscriptWindowLimits) =>
  Effect.gen(function* () {
    const transcript = yield* EDATranscriptStore;
    return yield* transcript.readTranscriptWindow(selector, limits);
  });

/** Every window a store returns satisfies the wire contract, degraded-read rule included. */
const roundTrip = (window: TranscriptWindow) =>
  Schema.decodeUnknownSync(TranscriptWindow)(Schema.encodeSync(TranscriptWindow)(window));

const orderKey = (record: {
  readonly id: string;
  readonly position?: { readonly anchorSeq: number; readonly ordinal: number };
}) => ({
  anchorSeq: record.position?.anchorSeq ?? 0,
  ordinal: record.position?.ordinal ?? 0,
  recordId: record.id,
});

const ids_ = (records: ReadonlyArray<TranscriptRecordView>) => records.map((record) => record.id);

const chunk = <A>(items: ReadonlyArray<A>, size: number): ReadonlyArray<ReadonlyArray<A>> =>
  Array.from({ length: Math.ceil(items.length / size) }, (_, index) =>
    items.slice(index * size, index * size + size),
  );

describe("in-memory transcript maintenance", () => {
  it("maintains the transcript inside every append and serves it at one cut", async () => {
    const decorated = scenarioLog("host-decorations");
    const envelopes = decorated.map((entry) =>
      entry.event.namespace === "__foreign" ? entry.event.envelope : entry.event,
    );
    const window = await withStore(chunk(envelopes, 4), readWindow({ _tag: "Latest", count: 100 }));
    const expected = Array.from(projectBatches(gia, [decorated]).records.values())
      .filter(isTopLevelTranscriptRecord)
      .sort((left, right) => TranscriptOrder(orderKey(left), orderKey(right)));
    expect(roundTrip(window).readiness).toEqual({ _tag: "Ready" });
    expect(window.throughSeq).toBe(decorated.length);
    expect(window.records).toEqual(expected);
    expect(window.dependencies.map((record) => record.kind)).toEqual([
      "sessionContext",
      "runGroup",
    ]);
  });

  it("freezes only the failed version and still commits the events", async () => {
    const log = new TestLog().configure();
    log.submit({ command: 1, message: 1, text: "hi" });
    log.startRun({ run: 1, commands: [1] });
    log.turn({ run: 1, turn: 1, inputs: [1] });
    log.inferenceStarted({ run: 1, turn: 1, inference: 1 });
    const good = [...log.durableEnvelopes];
    // A body with no JSON form cannot be projected: a recoverable projection failure.
    log.append("ToolCallCreated", {
      runId: ids.run(1),
      turnId: ids.turn(1),
      inferenceId: ids.inference(1),
      toolCallId: ids.tool(1),
      promptPart: {
        type: "tool-call",
        id: "call_1",
        name: "binary",
        params: { count: 1n },
        providerExecuted: false,
      },
    });
    log.submit({ command: 2, message: 2, text: "after the failure" });
    const all = log.durableEnvelopes;
    const result = await withStore(
      [good, all.slice(good.length, good.length + 1), all.slice(good.length + 1)],
      Effect.gen(function* () {
        const store = yield* EDASessionStore;
        const transcript = yield* EDATranscriptStore;
        const committed = yield* Stream.runCollect(store.eventsAfter(SequenceNumber.make(0)));
        return {
          committed: committed.length,
          meta: yield* transcript.readTranscriptMeta,
          window: yield* transcript.readTranscriptWindow(
            { _tag: "Latest", count: 10 },
            defaultTranscriptWindowLimits,
          ),
        };
      }),
    );
    expect(result.committed).toBe(all.length);
    expect(result.meta).toEqual([
      expect.objectContaining({
        role: "active",
        cursor: good.length,
        status: {
          _tag: "Failed",
          failedAtSeq: good.length + 1,
          error: expect.stringContaining("not JSON"),
        },
      }),
    ]);
    const window = roundTrip(result.window);
    expect(window).toMatchObject({
      throughSeq: good.length,
      readiness: { _tag: "Degraded", failedAtSeq: good.length + 1 },
    });
    // Nothing newer than the cut: the later submission is not served as pending.
    expect(window.dependencies.filter((record) => record.kind === "user")).toEqual([]);
  });

  it("reads a first build that froze as failed as unreadable, naming the failure", async () => {
    const log = new TestLog().configure();
    log.submit({ command: 1, message: 1, text: "hi" });
    log.startRun({ run: 1, commands: [1] });
    log.turn({ run: 1, turn: 1, inputs: [1] });
    log.inferenceStarted({ run: 1, turn: 1, inference: 1 });
    // A body with no JSON form cannot be projected, so building `current` from the seeded log
    // fails and no version ever becomes active. The build projects the seeded log as one batch,
    // so it freezes at that batch's first seq.
    const failedAtSeq = 1;
    log.append("ToolCallCreated", {
      runId: ids.run(1),
      turnId: ids.turn(1),
      inferenceId: ids.inference(1),
      toolCallId: ids.tool(1),
      promptPart: {
        type: "tool-call",
        id: "call_1",
        name: "binary",
        params: { count: 1n },
        providerExecuted: false,
      },
    });
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const transcript = yield* EDATranscriptStore;
        return {
          meta: yield* transcript.readTranscriptMeta,
          window: yield* transcript.readTranscriptWindow(
            { _tag: "Latest", count: 10 },
            defaultTranscriptWindowLimits,
          ),
        };
      }).pipe(
        Effect.provide(
          EDASessionStore.InMemorySeeded(testSessionId, log.durableEnvelopes, {
            transcript: { current: gia },
          }),
        ),
      ),
    );
    const [build] = result.meta;
    expect(build).toMatchObject({
      role: "building",
      version: gia.version,
      status: { _tag: "Failed", failedAtSeq },
    });
    if (build?.status._tag !== "Failed") throw new Error("expected the build to freeze");
    expect(roundTrip(result.window)).toMatchObject({
      version: gia.version,
      throughSeq: 0,
      records: [],
      dependencies: [],
      readiness: {
        _tag: "Unreadable",
        error: `transcript version ${gia.version} failed to build at seq ${failedAtSeq}: ${build.status.error}`,
      },
    });
  });

  it("stores large bodies as log references", () => {
    const log = new TestLog().configure();
    log.submit({ command: 1, message: 1, text: "hi" });
    log.startRun({ run: 1, commands: [1] });
    log.turn({ run: 1, turn: 1, inputs: [1] });
    log.inferenceStarted({ run: 1, turn: 1, inference: 1 });
    log.toolCreated({ run: 1, turn: 1, inference: 1, tool: 1, providerId: "call_1", name: "big" });
    log.toolCompleted({
      tool: 1,
      providerId: "call_1",
      name: "big",
      result: "x".repeat(transcriptStoredInlineBodyLimit + 1),
    });
    const tool = Array.from(projectBatches(plain, [log.events]).records.values()).find(
      (record) => record.kind === "tool",
    );
    expect(tool).toMatchObject({
      params: { _tag: "Inline" },
      lifecycle: {
        _tag: "Completed",
        result: { _tag: "Ref", source: { seq: log.events.length, path: ["promptPart", "result"] } },
      },
    });
  });
});

describe("transcript convergence", () => {
  const events = scenarioLog("queue-run-tools");
  const builtWith = (definition: typeof gia, role: "active" | "building") => {
    const built = projectBatches(definition, [events]);
    return { ...built, meta: { ...built.meta, role } };
  };

  it("initializes a brand-new log at seq 0 and an existing log by building from it", () => {
    expect(
      initialTranscriptState(incarnation, { current: plain }, []).versions.map(
        (version) => version.meta,
      ),
    ).toEqual([{ version: plain.version, role: "active", cursor: 0, status: { _tag: "Ok" } }]);
    const seeded = initialTranscriptState(incarnation, { current: plain }, events);
    expect(seeded.versions.map((version) => [version.meta.role, version.meta.cursor])).toEqual([
      ["active", events.length],
    ]);
  });

  it("serves a carried previous version, rebuilds current from the log and activates it", () => {
    const state: InMemoryTranscriptState = {
      incarnation,
      config: { current: gia, previous: plain },
      versions: [
        builtWith(plain, "active"),
        {
          ...emptyTranscriptVersion(
            testDefinition({ mappings: [], userFallbacks: [] }).version,
            "building",
          ),
        },
      ],
    };
    const source = inMemoryTranscriptWindowSource(state);
    expect(source?.carried).toBe(true);
    const converged = convergeTranscripts(state, events);
    expect(
      converged.versions.map((version) => [
        version.meta.version,
        version.meta.role,
        version.meta.cursor,
      ]),
    ).toEqual([[gia.version, "active", events.length]]);
  });

  it("serves a missed session's version as updating history until the rebuild activates", () => {
    const state: InMemoryTranscriptState = {
      incarnation,
      config: { current: gia },
      versions: [builtWith(plain, "active")],
    };
    const source = inMemoryTranscriptWindowSource(state);
    if (source === undefined) throw new Error("expected an active version");
    const window = readTranscriptWindowFrom(
      source,
      { _tag: "Latest", count: 100 },
      defaultTranscriptWindowLimits,
    );
    expect(Result.isSuccess(window) && roundTrip(window.success).readiness).toEqual({
      _tag: "UpdatingHistory",
    });
    expect(
      convergeTranscripts(state, events).versions.map((version) => version.meta.version),
    ).toEqual([gia.version]);
  });

  it("keeps the active version when the current build fails", () => {
    const failed = {
      ...builtWith(gia, "building"),
      meta: {
        ...builtWith(gia, "building").meta,
        cursor: SequenceNumber.make(3),
        status: { _tag: "Failed" as const, failedAtSeq: SequenceNumber.make(4), error: "boom" },
      },
    };
    const state: InMemoryTranscriptState = {
      incarnation,
      config: { current: gia, previous: plain },
      versions: [builtWith(plain, "active"), failed],
    };
    const converged = convergeTranscripts(state, events);
    expect(
      converged.versions.map((version) => [version.meta.role, version.meta.status._tag]),
    ).toEqual([
      ["active", "Ok"],
      ["building", "Failed"],
    ]);
    expect(inMemoryTranscriptWindowSource(converged)?.meta.version).toBe(plain.version);
  });
});

describe("the window served while no version is active", () => {
  const failed = {
    _tag: "Failed" as const,
    failedAtSeq: SequenceNumber.make(4),
    error: "boom",
  };

  it("is building with no build, a healthy build, or a failed build of another version", () => {
    for (const build of [
      undefined,
      { version: gia.version, status: { _tag: "Ok" as const } },
      { version: plain.version, status: failed },
    ]) {
      expect(
        roundTrip(inactiveTranscriptWindow(incarnation, gia.version, build)).readiness,
      ).toEqual({ _tag: "Building" });
    }
  });

  it("is unreadable, naming the failure, once the build of the current version has failed", () => {
    const window = roundTrip(
      inactiveTranscriptWindow(incarnation, gia.version, { version: gia.version, status: failed }),
    );
    expect(window).toEqual({
      incarnation,
      version: gia.version,
      throughSeq: 0,
      records: [],
      dependencies: [],
      hasOlder: false,
      hasNewer: false,
      readiness: {
        _tag: "Unreadable",
        error: `transcript version ${gia.version} failed to build at seq 4: boom`,
      },
    });
  });
});

describe("transcript window reads", () => {
  const events = scenarioLog("mid-run-compaction");
  const state = initialTranscriptState(incarnation, { current: plain }, events);
  const source = inMemoryTranscriptWindowSource(state);
  if (source === undefined) throw new Error("expected an active version");
  const read = (selector: TranscriptWindowSelector, limits = defaultTranscriptWindowLimits) => {
    const window = readTranscriptWindowFrom(source, selector, limits);
    if (Result.isFailure(window)) throw window.failure;
    return roundTrip(window.success);
  };
  const all = read({ _tag: "Latest", count: 100 });
  const cursor = (window: TranscriptWindow, index: number): TranscriptCursor => {
    const record = window.records[index < 0 ? window.records.length + index : index];
    const found = record === undefined ? undefined : transcriptCursorOf(window, record);
    if (found === undefined) throw new Error("no cursor");
    return found;
  };

  it("pages backwards and forwards over the same order", () => {
    expect(all.records.length).toBe(6);
    expect([all.hasOlder, all.hasNewer]).toEqual([false, false]);
    const latest = read({ _tag: "Latest", count: 2 });
    expect(ids_(latest.records)).toEqual(ids_(all.records).slice(-2));
    expect([latest.hasOlder, latest.hasNewer]).toEqual([true, false]);
    const before = read({ _tag: "Page", direction: "before", cursor: cursor(latest, 0), count: 3 });
    expect(ids_(before.records)).toEqual(ids_(all.records).slice(1, 4));
    expect([before.hasOlder, before.hasNewer]).toEqual([true, true]);
    const after = read({ _tag: "Page", direction: "after", cursor: cursor(before, 0), count: 10 });
    expect(ids_(after.records)).toEqual(ids_(all.records).slice(2));
    const range = read({ _tag: "Range", from: cursor(before, 0), to: cursor(before, -1) });
    expect(ids_(range.records)).toEqual(ids_(before.records));
    expect(
      read({ _tag: "Latest", count: 100 }, { ...defaultTranscriptWindowLimits, maxRecords: 2 })
        .records.length,
    ).toBe(2);
  });

  it("serves each record's dependencies once", () => {
    const deps = all.dependencies.map((record) => record.kind);
    expect(deps).toEqual(["sessionContext", "runGroup", "tool", "runGroup"]);
  });

  it("trims to the byte budget from the far end and references large bodies", () => {
    const tight = read(
      { _tag: "Latest", count: 100 },
      { maxRecords: 100, maxBytes: 1, maxInlineBodyBytes: 0 },
    );
    expect(ids_(tight.records)).toEqual(ids_(all.records).slice(-1));
    const tools = read(
      { _tag: "Latest", count: 100 },
      { ...defaultTranscriptWindowLimits, maxInlineBodyBytes: 1 },
    ).dependencies.filter((record) => record.kind === "tool");
    expect(tools).toEqual([
      expect.objectContaining({ params: expect.objectContaining({ _tag: "Ref" }) }),
    ]);
  });

  it("rejects a cursor from another incarnation or version", () => {
    const foreign = { ...cursor(all, 0), version: gia.version };
    const window = readTranscriptWindowFrom(
      source,
      { _tag: "Page", direction: "after", cursor: foreign, count: 1 },
      defaultTranscriptWindowLimits,
    );
    expect(Result.isFailure(window) && window.failure._tag).toBe("TranscriptCursorMismatch");
  });

  it("serves the run group of an interrupt-path user whose run failed before inference", async () => {
    const log = new TestLog().configure();
    log.admitOnly({ command: 1, text: "stop and do this" });
    log.append("CommandStarted", { commandId: ids.command(1) });
    log.commitUser({ command: 1, message: 1, text: "stop and do this" });
    log.runStarted({ run: 1, commands: [1] });
    log.turn({ run: 1, turn: 1, inputs: [1] });
    log.append("RunFailed", { runId: ids.run(1), error: { message: "provider down" } });
    const window = await withStore(
      [log.durableEnvelopes],
      readWindow({ _tag: "Latest", count: 10 }),
    );
    expect(window.records.map((record) => record.kind)).toEqual(["user"]);
    expect(window.dependencies).toContainEqual(
      expect.objectContaining({
        kind: "runGroup",
        runs: [
          expect.objectContaining({
            outcome: { _tag: "Failed", error: { message: "provider down" } },
          }),
        ],
      }),
    );
  });

  it("serves pending users as dependencies, never as history", async () => {
    const log = new TestLog().configure();
    log.submit({ command: 1, message: 1, text: "waiting" });
    log.submit({ command: 2, message: 2, text: "also waiting" });
    const window = await withStore(
      [log.durableEnvelopes],
      readWindow({ _tag: "Latest", count: 10 }),
    );
    expect(window.records).toEqual([]);
    expect(window.dependencies.map((record) => record.kind === "user" && record.commandId)).toEqual(
      [false, ids.command(1), ids.command(2)],
    );
  });

  it("reports a store failure as a typed cursor mismatch", async () => {
    const exit = await Effect.runPromise(
      Effect.exit(
        readWindow({
          _tag: "Page",
          direction: "after",
          cursor: { ...cursor(all, 0), incarnation: TranscriptLogIncarnation.make("old") },
          count: 1,
        }).pipe(Effect.provide(EDASessionStore.InMemory(testSessionId))),
      ),
    );
    expect(Exit.isFailure(exit)).toBe(true);
  });
});
