import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as fc from "fast-check";
import { describe, expect, it } from "vite-plus/test";

import type { CommittedDurableEvent } from "../services/session-store";
import { emptyTranscriptVersion, inMemoryTranscriptWindowSource } from "../transcript/in-memory";
import { advanceTranscriptVersion, type InMemoryTranscriptVersion } from "../transcript/in-memory";
import {
  TranscriptLogIncarnation,
  TranscriptRecordViewFromEnvelope,
  type TranscriptRecordView,
} from "../transcript/records";
import type { EDATranscriptStoreShape } from "../transcript/store";
import {
  planTranscriptUpdate,
  readTranscriptSubscription,
  transcriptSubscriptionBaseOf,
  type TranscriptSubscriptionPolicy,
  type TranscriptSubscriptionRequest,
} from "../transcript/subscription";
import { TestLog, generatedTestLog, testDefinition } from "../transcript/transcript-testkit";
import type {
  TranscriptWindow,
  TranscriptWindowLimits,
  TranscriptWindowSelector,
} from "../transcript/window";
import { readTranscriptWindowFrom } from "../transcript/window-read";
import { defaultEDAWebSocketFlowControl } from "./messages";
import { encodeEDATranscriptUpdate } from "./transcript";
import {
  applyEDATranscriptServerMessage,
  edaTranscriptClientReconnected,
  edaTranscriptPageRequest,
  edaTranscriptRetainRequest,
  edaTranscriptSubscribeRequest,
  emptyEDATranscriptClientState,
  type EDATranscriptClientState,
} from "./transcript-client";
import {
  defaultEDATranscriptDeliveryLimits,
  makeTranscriptDeliveryState,
  onTranscriptAck,
  pumpTranscriptTransfer,
  startTranscriptUpdate,
  type EDATranscriptDeliveryState,
} from "./transcript-delivery";
import type { EDATranscriptRequestFrame } from "./transcript";

const definition = testDefinition();
const incarnation = TranscriptLogIncarnation.make("memory:delivery-property");
const policy: TranscriptSubscriptionPolicy = {
  initialCount: 3,
  maxPageCount: 2,
  limits: { maxRecords: 5, maxBytes: 1_000_000, maxInlineBodyBytes: 64 },
};
const encodeRecord = Schema.encodeSync(TranscriptRecordViewFromEnvelope);
const encoded = (records: ReadonlyArray<TranscriptRecordView>) =>
  records.map((record) => JSON.stringify(encodeRecord(record)));

/**
 * A store over one projected version that commits a batch at a time. `commitBeforeRead` commits
 * one batch just before the given read (1-based) of the next request, so a test can commit
 * between a page's helper read and the reads after it.
 */
class SteppedStore {
  version: InMemoryTranscriptVersion = emptyTranscriptVersion(definition.version, "active");
  lastRead: InMemoryTranscriptVersion = this.version;
  commitBeforeRead: number | undefined;
  private next = 0;

  constructor(private readonly batches: ReadonlyArray<ReadonlyArray<CommittedDurableEvent>>) {}

  commit(): boolean {
    const batch = this.batches[this.next];
    if (batch === undefined) return false;
    this.next += 1;
    this.version = advanceTranscriptVersion(this.version, definition, batch);
    return true;
  }

  readAt(
    version: InMemoryTranscriptVersion,
    selector: TranscriptWindowSelector,
    limits: TranscriptWindowLimits = policy.limits,
  ) {
    const source = inMemoryTranscriptWindowSource({
      incarnation,
      config: { current: definition },
      versions: [version],
    });
    if (source === undefined) throw new Error("the stepped store always has an active version");
    return readTranscriptWindowFrom(source, selector, limits);
  }

  readonly store: EDATranscriptStoreShape = {
    readTranscriptWindow: (selector, limits) =>
      Effect.suspend(() => {
        if (this.commitBeforeRead !== undefined) {
          this.commitBeforeRead -= 1;
          if (this.commitBeforeRead === 0) {
            this.commitBeforeRead = undefined;
            this.commit();
          }
        }
        this.lastRead = this.version;
        const window = this.readAt(this.version, selector, limits);
        return Result.isSuccess(window)
          ? Effect.succeed(window.success)
          : Effect.fail(window.failure);
      }),
    readTranscriptMeta: Effect.sync(() => [this.version.meta]),
  };
}

const partition = (
  events: ReadonlyArray<CommittedDurableEvent>,
  cuts: ReadonlyArray<number>,
): ReadonlyArray<ReadonlyArray<CommittedDurableEvent>> => {
  const points = [...new Set(cuts.map((cut) => cut % (events.length + 1)))].sort((a, b) => a - b);
  const batches: Array<ReadonlyArray<CommittedDurableEvent>> = [];
  let start = 0;
  for (const point of [...points, events.length]) {
    if (point > start) batches.push(events.slice(start, point));
    start = Math.max(start, point);
  }
  return batches;
};

const toRequest = (frame: EDATranscriptRequestFrame): TranscriptSubscriptionRequest => {
  switch (frame._tag) {
    case "subscribe":
      return {
        _tag: "Subscribe",
        tail: frame.tail ?? false,
        ...(frame.range === undefined ? {} : { range: frame.range }),
        ...(frame.since === undefined ? {} : { since: frame.since }),
        ...(frame.count === undefined ? {} : { count: frame.count }),
      };
    case "page":
      return { _tag: "Page", direction: frame.direction, cursor: frame.cursor, count: frame.count };
    case "retain":
      return { _tag: "Retain", from: frame.from, to: frame.to };
  }
};

interface Simulation {
  server: EDATranscriptDeliveryState;
  client: EDATranscriptClientState;
  patches: number;
  chunked: number;
}

/** Serve one request through the real transport and client store, then check the client. */
const serve = (
  stepped: SteppedStore,
  simulation: Simulation,
  request: TranscriptSubscriptionRequest,
): void => {
  const read = Effect.runSync(
    readTranscriptSubscription(stepped.store, simulation.server.subscription, request, policy),
  );
  const update = planTranscriptUpdate({
    patchBase: read.patchBase,
    window: read.window,
    updateSeq: simulation.server.nextUpdateSeq,
    activeContent: [],
    includeActiveContent: false,
  });
  if (update.mode === "patch") simulation.patches += 1;
  const started = startTranscriptUpdate(
    simulation.server,
    {
      update,
      encodedUpdate: encodeEDATranscriptUpdate(update),
      subscription: transcriptSubscriptionBaseOf(read.window),
    },
    defaultEDATranscriptDeliveryLimits,
    0,
  );
  if (started._tag !== "Sending") throw new Error("update too large for the test bound");
  let server = started.state;
  let frames = started.frames;
  if (frames.length > 1 || server.transfer !== undefined) simulation.chunked += 1;
  let client = simulation.client;
  while (frames.length > 0) {
    for (const frame of frames) {
      const step = applyEDATranscriptServerMessage(client, frame);
      expect(step.outcome._tag).not.toBe("Resubscribe");
      client = step.state;
      if (step.ack !== undefined) {
        const acked = onTranscriptAck(server, step.ack);
        expect(acked._tag).not.toBe("ProtocolError");
        if (acked._tag === "Applied") server = acked.state;
      }
    }
    const pumped = pumpTranscriptTransfer(server, 0);
    server = pumped.state;
    frames = pumped.frames;
  }
  simulation.server = server;
  simulation.client = client;
  expectCompleteCut(stepped, client, read.window);
};

/** The client holds exactly the served window, which is itself one cut of the projection. */
const expectCompleteCut = (
  stepped: SteppedStore,
  client: EDATranscriptClientState,
  served: TranscriptWindow,
): void => {
  const window = client.window;
  if (window === undefined) throw new Error("an applied update always leaves a window");
  expect(window.throughSeq).toBe(served.throughSeq);
  expect(encoded(window.records)).toEqual(encoded(served.records));
  expect(encoded(window.dependencies)).toEqual(encoded(served.dependencies));
  expect(window.hasOlder).toBe(served.hasOlder);
  expect(window.hasNewer).toBe(served.hasNewer);

  // No record (in particular no pending user) is visible twice.
  const ids = [...window.records, ...window.dependencies].map((record) => record.id);
  expect(new Set(ids).size).toBe(ids.length);
  expect(window.records.length).toBeLessThanOrEqual(policy.limits.maxRecords);

  // The served window equals an independent read of its coverage at the same cut.
  const coverage = window.coverage;
  if (coverage !== undefined) {
    const fresh = stepped.readAt(stepped.lastRead, { _tag: "Range", ...coverage });
    if (!Result.isSuccess(fresh)) throw new Error("coverage cursors belong to the served cut");
    expect(encoded(fresh.success.records)).toEqual(encoded(window.records));
    expect(encoded(fresh.success.dependencies)).toEqual(encoded(window.dependencies));
    if (!window.hasNewer) {
      const newest = stepped.readAt(stepped.lastRead, { _tag: "Latest", count: 1 });
      if (!Result.isSuccess(newest)) throw new Error("a Latest read cannot mismatch");
      expect(newest.success.records[0]?.id).toBe(coverage.to.recordId);
    }
  }
};

describe("transcript window delivery", () => {
  it("leaves every client at one complete cut through commits, pages, retains and reconnects", () => {
    let patches = 0;
    let chunked = 0;
    fc.assert(
      fc.property(
        fc.array(fc.nat({ max: 10_000 }), { minLength: 10, maxLength: 70 }),
        fc.array(fc.nat({ max: 10_000 }), { maxLength: 25 }),
        fc.array(fc.nat({ max: 10_000 }), { minLength: 1, maxLength: 40 }),
        fc.constantFrom(256 * 1024, 700, 2_000),
        (steps, cuts, operations, maxFrameBytes) => {
          const { log } = generatedTestLog(steps);
          const stepped = new SteppedStore(partition(log.events, cuts));
          stepped.commit();
          const flowControl = {
            ...defaultEDAWebSocketFlowControl,
            maxFrameBytes,
            maxInFlightFrames: 3,
          };
          const simulation: Simulation = {
            server: makeTranscriptDeliveryState(flowControl),
            client: emptyEDATranscriptClientState,
            patches: 0,
            chunked: 0,
          };
          serve(stepped, simulation, toRequest(edaTranscriptSubscribeRequest(simulation.client)));
          for (const operation of operations) {
            const client = simulation.client;
            switch (operation % 6) {
              case 0:
                stepped.commit();
                serve(stepped, simulation, { _tag: "Refresh" });
                break;
              case 1:
              case 2: {
                const page = edaTranscriptPageRequest(
                  client,
                  operation % 6 === 1 ? "before" : "after",
                  1 + (operation % 2),
                );
                if (page === undefined) break;
                // Commit after the page's first (helper) read, before the reads that follow it.
                if (operation % 4 === 0) stepped.commitBeforeRead = 2;
                serve(stepped, simulation, toRequest(page));
                stepped.commitBeforeRead = undefined;
                break;
              }
              case 3: {
                const count = client.window?.records.length ?? 0;
                if (count === 0) break;
                const from = operation % count;
                const to = from + ((operation >> 3) % (count - from));
                const retain = edaTranscriptRetainRequest(client, from, to);
                if (retain === undefined) break;
                serve(stepped, simulation, toRequest(retain));
                break;
              }
              case 4:
                stepped.commit();
                stepped.commit();
                serve(stepped, simulation, { _tag: "Refresh" });
                break;
              case 5: {
                // Reconnect: a new socket, then changes since what the client applied.
                stepped.commit();
                simulation.client = edaTranscriptClientReconnected(client);
                simulation.server = makeTranscriptDeliveryState(flowControl);
                serve(
                  stepped,
                  simulation,
                  toRequest(edaTranscriptSubscribeRequest(simulation.client)),
                );
                break;
              }
            }
          }
          patches += simulation.patches;
          chunked += simulation.chunked;
        },
      ),
      { numRuns: 150 },
    );
    // The property exercised the paths it claims to.
    expect(patches).toBeGreaterThan(100);
    expect(chunked).toBeGreaterThan(10);
  });

  it("walks from the start of history to the tail and back, evicting beyond the bound", () => {
    const { log } = generatedTestLog(
      Array.from({ length: 160 }, (_, index) => [0, 2, 3, 6][index % 4] ?? 0),
    );
    const stepped = new SteppedStore([log.events]);
    stepped.commit();
    const simulation: Simulation = {
      server: makeTranscriptDeliveryState(defaultEDAWebSocketFlowControl),
      client: emptyEDATranscriptClientState,
      patches: 0,
      chunked: 0,
    };
    serve(stepped, simulation, { _tag: "Subscribe", tail: false, count: 3 });
    const newest = simulation.client.window?.records.at(-1)?.id;

    // Jump to the start of history: page back until nothing is older.
    let guard = 0;
    while (simulation.client.window?.hasOlder === true && guard++ < 500) {
      const page = edaTranscriptPageRequest(simulation.client, "before", 2);
      if (page === undefined) break;
      serve(stepped, simulation, toRequest(page));
    }
    const atStart = simulation.client.window;
    expect(atStart?.hasOlder).toBe(false);
    expect(atStart?.hasNewer).toBe(true);
    expect(atStart?.records.length).toBe(policy.limits.maxRecords);

    // Reconnect at the start of history: a new socket resubscribes to the retained range.
    simulation.client = edaTranscriptClientReconnected(simulation.client);
    simulation.server = makeTranscriptDeliveryState(defaultEDAWebSocketFlowControl);
    serve(stepped, simulation, toRequest(edaTranscriptSubscribeRequest(simulation.client)));
    expect(simulation.client.window?.records.map((record) => record.id)).toEqual(
      atStart?.records.map((record) => record.id),
    );

    // Page forward to the tail, evicting the oldest.
    guard = 0;
    while (simulation.client.window?.hasNewer === true && guard++ < 500) {
      const page = edaTranscriptPageRequest(simulation.client, "after", 2);
      if (page === undefined) break;
      serve(stepped, simulation, toRequest(page));
    }
    expect(simulation.client.window?.records.at(-1)?.id).toBe(newest);
    expect(simulation.client.window?.records.length).toBe(policy.limits.maxRecords);

    // And back again: paging before evicts the newest and leaves the tail.
    const page = edaTranscriptPageRequest(simulation.client, "before", 2);
    if (page === undefined) throw new Error("a full window has a first record");
    serve(stepped, simulation, toRequest(page));
    expect(simulation.client.window?.hasNewer).toBe(true);
    expect(simulation.client.window?.records.at(-1)?.id).not.toBe(newest);
    expect(simulation.client.window?.throughSeq).toBe(log.events.length);
  });
  it("delivers many small records and large bodies as one chunked update to a slow receiver", () => {
    const log = new TestLog().configure();
    for (let index = 1; index <= 120; index += 1) {
      log.submit({ command: index, message: index, text: `q${index}` });
      log.startRun({ run: 1_000 + index, commands: [index] });
      log.assistantTurn({
        run: 1_000 + index,
        turn: 2_000 + index,
        inference: 3_000 + index,
        message: 4_000 + index,
        text: `a${index}`,
        inputs: [index],
      });
      log.runCompleted(1_000 + index, [index]);
    }
    const call = { run: 9_000, turn: 9_001, inference: 9_002 };
    const tools = [
      { tool: 9_003, providerId: "call_big", name: "fetch", result: { blob: "y".repeat(300_000) } },
      { tool: 9_004, providerId: "call_mid", name: "lookup", result: { text: "z".repeat(10_000) } },
    ];
    log.submit({ command: 500, message: 500, text: "fetch it" });
    log.startRun({ run: call.run, commands: [500] });
    log.turn({ ...call, inputs: [500] });
    log.inferenceStarted(call);
    log.assistant({ ...call, message: 9_005, text: "fetching", toolCalls: tools });
    for (const tool of tools) log.toolCreated({ ...call, ...tool });
    log.inferenceCompleted(call);
    for (const tool of tools) {
      log.toolStarted(tool.tool);
      log.toolCompleted(tool);
    }
    const stepped = new SteppedStore([log.events]);
    stepped.commit();

    const flowControl = {
      ...defaultEDAWebSocketFlowControl,
      maxFrameBytes: 4 * 1024,
      maxInFlightFrames: 4,
    };
    const read = Effect.runSync(
      readTranscriptSubscription(
        stepped.store,
        undefined,
        { _tag: "Subscribe", tail: false, count: 300 },
        {
          ...policy,
          limits: { maxRecords: 300, maxBytes: 4 * 1_048_576, maxInlineBodyBytes: 16_384 },
        },
      ),
    );
    const update = planTranscriptUpdate({
      patchBase: undefined,
      window: read.window,
      updateSeq: 1,
      activeContent: [],
      includeActiveContent: true,
    });
    const started = startTranscriptUpdate(
      makeTranscriptDeliveryState(flowControl),
      {
        update,
        encodedUpdate: encodeEDATranscriptUpdate(update),
        subscription: transcriptSubscriptionBaseOf(read.window),
      },
      defaultEDATranscriptDeliveryLimits,
      0,
    );
    if (started._tag !== "Sending") throw new Error("within the staging bound");
    let server = started.state;
    let client = emptyEDATranscriptClientState;
    const inbox = [...started.frames];
    let frames = 0;
    while (inbox.length > 0) {
      expect(server.inFlight.length).toBeLessThanOrEqual(flowControl.maxInFlightFrames);
      // A slow receiver: it acknowledges only every third frame.
      for (const frame of inbox.splice(0, 3)) {
        frames += 1;
        const step = applyEDATranscriptServerMessage(client, frame);
        expect(step.outcome._tag).not.toBe("Resubscribe");
        client = step.state;
        if (inbox.length > 0 || server.transfer !== undefined) {
          expect(client.window).toBeUndefined();
        }
        if (step.ack !== undefined) {
          const acked = onTranscriptAck(server, step.ack);
          if (acked._tag === "ProtocolError") throw new Error(acked.message);
          if (acked._tag === "Applied") server = acked.state;
        }
      }
      const pumped = pumpTranscriptTransfer(server, 0);
      server = pumped.state;
      inbox.push(...pumped.frames);
    }
    expect(frames).toBeGreaterThan(16);
    expect(server.appliedUpdateSeq).toBe(1);
    expect(client.window?.records.length).toBe(read.window.records.length);
    expect(client.window?.records.length).toBeGreaterThan(240);
    const bodies = new Map(
      (client.window?.dependencies ?? []).flatMap((record) =>
        record.kind === "tool" && record.lifecycle._tag === "Completed"
          ? [[record.providerPartId, record.lifecycle.result] as const]
          : [],
      ),
    );
    expect(bodies.get("call_big")).toMatchObject({ _tag: "Ref" });
    const big = bodies.get("call_big");
    expect(big?._tag === "Ref" ? big.byteLength : 0).toBeGreaterThan(300_000);
    expect(bodies.get("call_mid")).toMatchObject({ _tag: "Inline" });
  });
});
