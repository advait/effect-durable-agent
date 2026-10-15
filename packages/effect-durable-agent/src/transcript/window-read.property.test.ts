import * as fc from "fast-check";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import type { CommittedDurableEvent } from "../services/session-store";
import { NonNegativeInt } from "../types/events/envelope";
import { inMemoryTranscriptWindowSource, initialTranscriptState } from "./in-memory";
import {
  TranscriptLogIncarnation,
  TranscriptRecordFromEnvelope,
  isPendingTranscriptUserRecord,
  transcriptRecordId,
  transcriptStoredInlineBodyLimit,
  type TranscriptAssistantPart,
  type TranscriptBody,
  type TranscriptRecord,
  type TranscriptToolRecord,
  type TranscriptTopLevelRecord,
} from "./records";
import { giaLikeTestMappings } from "./transcript-scenarios";
import { TestLog, generatedTestLog, testDefinition } from "./transcript-testkit";
import {
  transcriptCursorOf,
  type TranscriptWindow,
  type TranscriptWindowLimits,
  type TranscriptWindowSelector,
} from "./window";
import { readTranscriptWindowFrom, type TranscriptWindowSource } from "./window-read";

const gia = testDefinition(giaLikeTestMappings);
const incarnation = TranscriptLogIncarnation.make("test");

const sourceOf = (events: ReadonlyArray<CommittedDurableEvent>): TranscriptWindowSource => {
  const source = inMemoryTranscriptWindowSource(
    initialTranscriptState(incarnation, { current: gia }, events),
  );
  if (source === undefined) throw new Error("expected an active version");
  return source;
};

const read = (
  source: TranscriptWindowSource,
  selector: TranscriptWindowSelector,
  limits: TranscriptWindowLimits,
): TranscriptWindow => {
  const window = readTranscriptWindowFrom(source, selector, limits);
  if (Result.isFailure(window)) throw window.failure;
  return window.success;
};

// The trim as alpha.25 shipped it, kept verbatim as the oracle: after every dropped record it
// re-collects the dependencies and re-measures the whole window.
const encoder = new TextEncoder();
const encodeRecord = Schema.encodeSync(TranscriptRecordFromEnvelope);
const jsonBytes = (value: unknown): number => {
  const json = JSON.stringify(value);
  return json === undefined ? 0 : encoder.encode(json).length;
};
const windowBytes = (
  top: ReadonlyArray<TranscriptRecord>,
  dependencies: ReadonlyArray<TranscriptRecord>,
): number =>
  [...top, ...dependencies].reduce((total, record) => total + jsonBytes(encodeRecord(record)), 0);
const bodyRef = (body: TranscriptBody, maxInlineBodyBytes: number): TranscriptBody => {
  if (body._tag === "Ref") return body;
  const byteLength = jsonBytes(body.value);
  return byteLength <= maxInlineBodyBytes
    ? body
    : { _tag: "Ref", source: body.source, byteLength: NonNegativeInt.make(byteLength) };
};
const withBodyRefs = <R extends TranscriptRecord>(record: R, maxInlineBodyBytes: number): R => {
  switch (record.kind) {
    case "tool": {
      const tool: TranscriptToolRecord = {
        ...record,
        ...(record.params === undefined
          ? {}
          : { params: bodyRef(record.params, maxInlineBodyBytes) }),
        lifecycle:
          record.lifecycle._tag === "Completed"
            ? { _tag: "Completed", result: bodyRef(record.lifecycle.result, maxInlineBodyBytes) }
            : record.lifecycle,
      };
      return { ...record, ...tool };
    }
    case "assistant": {
      const parts = record.parts.map((part): TranscriptAssistantPart =>
        part._tag === "ToolResult"
          ? { ...part, result: bodyRef(part.result, maxInlineBodyBytes) }
          : part,
      );
      return { ...record, parts };
    }
    default:
      return record;
  }
};
const collectDependencies = (
  source: TranscriptWindowSource,
  top: ReadonlyArray<TranscriptTopLevelRecord>,
  maxInlineBodyBytes: number,
): ReadonlyArray<TranscriptRecord> => {
  const dependencies = new Map<string, TranscriptRecord>();
  const add = (record: TranscriptRecord | undefined) => {
    if (record !== undefined && !dependencies.has(record.id)) {
      dependencies.set(record.id, withBodyRefs(record, maxInlineBodyBytes));
    }
  };
  add(source.record(transcriptRecordId.session));
  for (const record of top) {
    switch (record.kind) {
      case "assistant": {
        if (record.runGroupId !== undefined) add(source.record(record.runGroupId));
        const partOrder = new Map(
          record.parts.flatMap((part, index) =>
            part._tag === "ToolCall" && part.toolId !== undefined ? [[part.toolId, index]] : [],
          ),
        );
        const tools = [...source.toolsOf(record.id)].sort(
          (left, right) =>
            (partOrder.get(left.id) ?? Number.MAX_SAFE_INTEGER) -
              (partOrder.get(right.id) ?? Number.MAX_SAFE_INTEGER) ||
            (left.id < right.id ? -1 : left.id > right.id ? 1 : 0),
        );
        tools.forEach(add);
        break;
      }
      case "user": {
        const runId = record.state._tag === "Consumed" ? record.state.runId : undefined;
        const groupId =
          (runId === undefined ? undefined : source.alias("runId", runId)) ??
          source.alias("runCommand", record.commandId);
        if (groupId !== undefined) add(source.record(groupId));
        break;
      }
      case "compaction":
        break;
    }
  }
  [...source.pendingUsers()]
    .filter(isPendingTranscriptUserRecord)
    .sort(
      (left, right) =>
        left.submittedSeq - right.submittedSeq ||
        (left.id < right.id ? -1 : left.id > right.id ? 1 : 0),
    )
    .forEach(add);
  return Array.from(dependencies.values());
};

/** The alpha.25 window for `selector`: the untrimmed selection, trimmed one record at a time. */
const quadraticWindow = (
  source: TranscriptWindowSource,
  selector: TranscriptWindowSelector,
  limits: TranscriptWindowLimits,
) => {
  // Selection and body references are unchanged; only the trim is under test.
  const selected = read(source, selector, { ...limits, maxBytes: Number.MAX_SAFE_INTEGER });
  const keepNewest =
    selector._tag === "Latest" || (selector._tag === "Page" && selector.direction === "before");
  let top: ReadonlyArray<TranscriptTopLevelRecord> = selected.records.filter(
    (record): record is TranscriptTopLevelRecord => record.kind !== "unknown",
  );
  let dependencies = collectDependencies(source, top, limits.maxInlineBodyBytes);
  while (top.length > 1 && windowBytes(top, dependencies) > limits.maxBytes) {
    top = keepNewest ? top.slice(1) : top.slice(0, -1);
    dependencies = collectDependencies(source, top, limits.maxInlineBodyBytes);
  }
  return { records: top, dependencies };
};

/** A selector of each kind, anchored on records of the whole history. */
const selectorArbitrary = (whole: TranscriptWindow): fc.Arbitrary<TranscriptWindowSelector> => {
  const count = fc.integer({ min: 1, max: 40 });
  const latest = count.map((n): TranscriptWindowSelector => ({ _tag: "Latest", count: n }));
  const cursors = whole.records.flatMap((record) => {
    const cursor = transcriptCursorOf(whole, record);
    return cursor === undefined ? [] : [cursor];
  });
  if (cursors.length === 0) return latest;
  const cursor = fc.constantFrom(...cursors);
  return fc.oneof(
    latest,
    fc
      .tuple(fc.constantFrom("before" as const, "after" as const), cursor, count)
      .map(([direction, at, n]): TranscriptWindowSelector => ({
        _tag: "Page",
        direction,
        cursor: at,
        count: n,
      })),
    fc
      .tuple(cursor, cursor)
      .map(([a, b]): TranscriptWindowSelector =>
        cursors.indexOf(a) <= cursors.indexOf(b)
          ? { _tag: "Range", from: a, to: b }
          : { _tag: "Range", from: b, to: a },
      ),
  );
};

const totalBytes = (window: TranscriptWindow) =>
  windowBytes(
    window.records.filter((record) => record.kind !== "unknown"),
    window.dependencies.filter((record) => record.kind !== "unknown"),
  );

/** A log of `turns` runs whose tools return bodies of the given sizes. */
const toolBodyLog = (bodies: ReadonlyArray<ReadonlyArray<number>>) => {
  const log = new TestLog().configure();
  let tool = 1;
  bodies.forEach((sizes, index) => {
    const run = index + 1;
    const turn = { run, turn: 1, inference: run, message: 1_000 + run };
    const calls = sizes.map((size) => {
      const id = tool++;
      return { tool: id, providerId: `call_${id}`, name: "runBash", result: "x".repeat(size) };
    });
    log.submit({ command: run, message: run, text: `q${run}`, idempotencyKey: `web:${run}` });
    log.startRun({ run, commands: [run] });
    log.turn({ ...turn, inputs: [run] });
    log.inferenceStarted(turn);
    log.assistant({ ...turn, text: "running", toolCalls: calls });
    for (const call of calls) log.toolCreated({ ...turn, ...call });
    log.inferenceCompleted(turn);
    for (const call of calls) log.toolStarted(call.tool).toolCompleted(call);
    log.turnCompleted(turn);
    log.runCompleted(run, [run]);
  });
  return log;
};

/**
 * Compare both trims at every budget that matters for this selection: the size of each window
 * the trim may stop at, and one byte either side of it.
 */
const expectSameTrim = (
  source: TranscriptWindowSource,
  selector: TranscriptWindowSelector,
  limits: Omit<TranscriptWindowLimits, "maxBytes">,
) => {
  const selected = quadraticWindow(source, selector, {
    ...limits,
    maxBytes: Number.MAX_SAFE_INTEGER,
  }).records;
  const keepNewest =
    selector._tag === "Latest" || (selector._tag === "Page" && selector.direction === "before");
  const budgets = new Set([1]);
  for (let kept = 1; kept <= selected.length; kept++) {
    const top = keepNewest ? selected.slice(-kept) : selected.slice(0, kept);
    const bytes = windowBytes(top, collectDependencies(source, top, limits.maxInlineBodyBytes));
    budgets
      .add(bytes - 1)
      .add(bytes)
      .add(bytes + 1);
  }
  for (const maxBytes of budgets) {
    if (maxBytes < 1) continue;
    const window = read(source, selector, { ...limits, maxBytes });
    const oracle = quadraticWindow(source, selector, { ...limits, maxBytes });
    expect(window.records).toEqual(oracle.records);
    expect(window.dependencies).toEqual(oracle.dependencies);
  }
};

const otherLimitsArbitrary = fc.record({
  maxRecords: fc.integer({ min: 1, max: 40 }),
  maxInlineBodyBytes: fc.integer({ min: 0, max: 2_000 }),
});

describe("window byte budget", () => {
  it("trims exactly as re-measuring after every drop, for generated sessions", () => {
    fc.assert(
      fc.property(
        fc.array(fc.nat({ max: 10_000 }), { minLength: 1, maxLength: 80 }).chain((steps) => {
          const source = sourceOf(generatedTestLog(steps).log.events);
          const whole = read(
            source,
            { _tag: "Latest", count: 10_000 },
            {
              maxRecords: 10_000,
              maxBytes: Number.MAX_SAFE_INTEGER,
              maxInlineBodyBytes: Number.MAX_SAFE_INTEGER,
            },
          );
          return fc.tuple(fc.constant(source), selectorArbitrary(whole), otherLimitsArbitrary);
        }),
        ([source, selector, limits]) => expectSameTrim(source, selector, limits),
      ),
      { numRuns: 200 },
    );
  });

  it("trims exactly as re-measuring after every drop, for tool bodies around the inline limit", () => {
    const size = fc.oneof(
      fc.integer({ min: 0, max: 2_000 }),
      fc.integer({ min: 16_000, max: transcriptStoredInlineBodyLimit + 1_000 }),
    );
    fc.assert(
      fc.property(
        fc
          .array(fc.array(size, { maxLength: 4 }), { minLength: 1, maxLength: 12 })
          .chain((bodies) => {
            const source = sourceOf(toolBodyLog(bodies).events);
            const whole = read(
              source,
              { _tag: "Latest", count: 10_000 },
              {
                maxRecords: 10_000,
                maxBytes: Number.MAX_SAFE_INTEGER,
                maxInlineBodyBytes: Number.MAX_SAFE_INTEGER,
              },
            );
            return fc.tuple(
              fc.constant(source),
              selectorArbitrary(whole),
              fc.record({
                maxRecords: fc.integer({ min: 1, max: 40 }),
                maxInlineBodyBytes: fc.constantFrom(0, 16_384, transcriptStoredInlineBodyLimit),
              }),
            );
          }),
        ([source, selector, limits]) => expectSameTrim(source, selector, limits),
      ),
      { numRuns: 50 },
    );
  });

  it("trims a large selection of large inline bodies in linear time", () => {
    // 100 runs of 15 tools near 64 KiB: alpha.25's trim re-measured the window after each of
    // the ~98 drops (about 6 s); measuring each record once takes a small fraction of that.
    const source = sourceOf(
      toolBodyLog(Array.from({ length: 100 }, () => Array(15).fill(63 * 1024))).events,
    );
    const limits = {
      maxRecords: 100,
      maxBytes: 1_048_576,
      maxInlineBodyBytes: transcriptStoredInlineBodyLimit,
    };
    const started = performance.now();
    const window = read(source, { _tag: "Latest", count: 100 }, limits);
    const elapsed = performance.now() - started;
    expect(window.records.length).toBeGreaterThanOrEqual(1);
    expect(window.hasOlder).toBe(true);
    expect(totalBytes(window)).toBeLessThanOrEqual(limits.maxBytes + 15 * 70 * 1024);
    expect(elapsed).toBeLessThan(2_000);
  });
});
