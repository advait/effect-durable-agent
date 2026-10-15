import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/ai/Prompt";
import * as Response from "effect/ai/Response";
import { afterEach, describe, expect, it } from "vite-plus/test";

import { EDARuntime } from "effect-durable-agent/services/runtime";
import { makeEdaTestLayer, type TestModelParts } from "effect-durable-agent/testkit/layers";
import {
  EDATranscriptStore,
  TranscriptUpdate,
  type EDATranscriptStoreShape,
} from "effect-durable-agent/transcript";
import { SubmitMessageCommand } from "effect-durable-agent/types/commands";
import {
  CommandId,
  EventId,
  InferenceId,
  Position,
  RunId,
  SequenceNumber,
  SessionId,
  SubSequenceNumber,
  TurnId,
  durablePosition,
} from "effect-durable-agent/types/core";
import {
  InferenceCompletedEvent,
  InferenceStartedEvent,
  PositionedEvent,
  ProviderPartId,
  TextDeltaEvent,
  UnixEpochMillis,
  effectDurableAgentNamespace,
  schemaV1,
} from "effect-durable-agent/types/events";
import { SessionEventObserver } from "effect-durable-agent/services/session-event-observer";
import {
  TranscriptLogIncarnation,
  TranscriptVersion,
  type TranscriptWindow,
} from "effect-durable-agent/transcript";
import { makeRootEDATraceMetadata } from "effect-durable-agent/types/tracing";
import {
  EDA_WS_CLOSE_TRANSCRIPT_RESUBSCRIBE,
  applyEDATranscriptServerMessage,
  defaultEDAWebSocketFlowControl,
  edaTranscriptClientReconnected,
  edaTranscriptPageRequest,
  edaTranscriptSubscribeRequest,
  emptyEDATranscriptClientState,
  type EDATranscriptClientState,
  type EDAWebSocketFlowControl,
} from "effect-durable-agent/websocket";
import { EDAWebSocketConnectionManager } from "./connection-manager";

const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const TRACE = makeRootEDATraceMetadata();

const usage = () =>
  new Response.Usage({
    inputTokens: { uncached: undefined, total: 10, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 5, text: undefined, reasoning: undefined },
  });
const finish = () =>
  Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined });
const textDelta = (delta: string, id = "text") => Response.makePart("text-delta", { id, delta });

const command = (slot: number, text: string) =>
  new SubmitMessageCommand({
    commandId: CommandId.make(`018f6bd5-2f2a-7b1e-8f1b-${(0x100000000000 + slot).toString(16)}`),
    disposition: "queue",
    content: [Prompt.textPart({ text })],
  });

const decodeUpdate = Schema.decodeUnknownSync(TranscriptUpdate);

/** One isolate: a connection manager, and optionally a live runtime publishing into it. */
class Isolate {
  readonly timers: Array<() => void> = [];
  nowMs = 1_000;
  readonly manager: EDAWebSocketConnectionManager;
  private store: EDATranscriptStoreShape | undefined;
  runtime: ManagedRuntime.ManagedRuntime<EDARuntime | EDATranscriptStore, never> | undefined;

  constructor(
    sockets: () => ReadonlyArray<TestSocket>,
    flowControl: EDAWebSocketFlowControl = defaultEDAWebSocketFlowControl,
    store?: EDATranscriptStoreShape,
  ) {
    this.store = store;
    const transcriptStore: EDATranscriptStoreShape = {
      readTranscriptWindow: (selector, limits) =>
        Effect.suspend(() =>
          this.store === undefined
            ? Effect.die(new Error("no transcript store"))
            : this.store.readTranscriptWindow(selector, limits),
        ),
      readTranscriptMeta: Effect.suspend(() =>
        this.store === undefined
          ? Effect.die(new Error("no transcript store"))
          : this.store.readTranscriptMeta,
      ),
    };
    this.manager = new EDAWebSocketConnectionManager({
      getWebSockets: () => sockets().map((socket) => socket.asWebSocket()),
      isSessionReady: () => true,
      prepareSession: async () => undefined,
      readEventPage: async () => ({ events: [], head: SequenceNumber.make(0) }),
      transcript: {
        transcriptStore,
        flowControl,
        schedule: (_delayMs, run) => void this.timers.push(run),
        now: () => this.nowMs,
      },
    });
  }

  async start(parts: TestModelParts): Promise<void> {
    this.runtime = ManagedRuntime.make(
      EDARuntime.Live({ modelSelection: { provider: "test", modelId: "test-model" } }).pipe(
        Layer.provideMerge(
          makeEdaTestLayer({
            sessionId: SESSION_ID,
            parts,
            sessionEventObserverLayer: this.manager.observerLayer,
          }),
        ),
      ),
    );
    this.store = await this.runtime.runPromise(Effect.service(EDATranscriptStore));
  }

  get transcriptStore(): EDATranscriptStoreShape {
    if (this.store === undefined) throw new Error("not started");
    return this.store;
  }

  submit(slot: number, text: string): Promise<unknown> {
    const runtime = this.runtime;
    if (runtime === undefined) throw new Error("not started");
    return runtime.runPromise(
      Effect.gen(function* () {
        const eda = yield* EDARuntime;
        return yield* eda.submitAndBlock(command(slot, text));
      }).pipe(Effect.catchCause(() => Effect.void)),
    );
  }

  /** Fire the cadence timers that are due and wait for their flushes. */
  async tick(): Promise<void> {
    for (const run of this.timers.splice(0)) run();
    await this.manager.settled();
  }

  async dispose(): Promise<void> {
    await this.runtime?.dispose();
  }
}

/** A fake socket that records frames, plus the EDA reference client applying them. */
class TestSocket {
  attachment: unknown;
  closeCode: number | undefined;
  readonly inbox: string[] = [];
  client: EDATranscriptClientState = emptyEDATranscriptClientState;
  /** Every applied update; single-frame updates keep their decoded content. */
  readonly applied: Array<{ readonly updateSeq: number; readonly update?: TranscriptUpdate }> = [];
  readonly readyState = 1;

  asWebSocket(): WebSocket {
    // SAFETY: the connection manager uses only send/close and the attachment methods below.
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- test double for a platform socket
    return this as unknown as WebSocket;
  }

  sentCount = 0;

  send(text: string): void {
    this.sentCount += 1;
    this.inbox.push(text);
  }

  close(code?: number): void {
    this.closeCode = code;
  }

  serializeAttachment(attachment: unknown): void {
    this.attachment = attachment;
  }

  deserializeAttachment(): unknown {
    return this.attachment;
  }

  /**
   * Apply received frames with the reference client, ACKing each one, until the server has
   * nothing more to send. `maxAcks` models a slow receiver.
   */
  async pump(
    manager: EDAWebSocketConnectionManager,
    maxAcks = Number.POSITIVE_INFINITY,
  ): Promise<void> {
    let acks = 0;
    while (this.inbox.length > 0 && acks < maxAcks && this.closeCode === undefined) {
      const text = this.inbox.shift() ?? "";
      const step = applyEDATranscriptServerMessage(this.client, text);
      expect(
        step.outcome._tag,
        step.outcome._tag === "Resubscribe" ? step.outcome.reason : "",
      ).not.toBe("Resubscribe");
      this.client = step.state;
      if (step.outcome._tag === "Applied") this.recordApplied(text, step.outcome.updateSeq);
      if (step.ack !== undefined) {
        acks += 1;
        await manager.message(this.asWebSocket(), JSON.stringify(step.ack));
      }
    }
  }

  async request(manager: EDAWebSocketConnectionManager, frame: object): Promise<void> {
    await manager.message(this.asWebSocket(), JSON.stringify(frame));
  }

  /** Keep every applied update and check the no-stale-live-part invariant on each. */
  private recordApplied(text: string, updateSeq: number): void {
    const frame: unknown = JSON.parse(text);
    this.applied.push(
      typeof frame === "object" && frame !== null && "update" in frame
        ? { updateSeq, update: decodeUpdate(frame.update) }
        : { updateSeq },
    );
    const window = this.client.window;
    for (const content of window?.activeContent ?? []) {
      const record = window?.records.find((candidate) => candidate.id === content.assistantId);
      if (record !== undefined && record.kind === "assistant") {
        expect(record.lifecycle._tag).toBe("Streaming");
      }
    }
  }
}

const fenceIds = {
  run: RunId.make("018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a"),
  turn: TurnId.make("018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a"),
  inference: InferenceId.make("018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a"),
};
const envelope = (slot: number) => ({
  namespace: effectDurableAgentNamespace,
  schemaVersion: schemaV1,
  eventId: EventId.make(`018f6bd5-2f2a-7b1e-${(0x9000 + slot).toString(16)}-1f2e3d4c5b6a`),
  sessionId: SESSION_ID,
  createdAtMs: UnixEpochMillis.make(1_715_000_000_000 + slot),
});
const started = (seq: number) =>
  PositionedEvent.make({
    position: durablePosition(SequenceNumber.make(seq)),
    event: InferenceStartedEvent.make({
      ...envelope(seq),
      type: "InferenceStarted",
      durability: "durable",
      payload: { runId: fenceIds.run, turnId: fenceIds.turn, inferenceId: fenceIds.inference },
    }),
  });
const completed = (seq: number, inferenceId: InferenceId = fenceIds.inference) =>
  PositionedEvent.make({
    position: durablePosition(SequenceNumber.make(seq)),
    event: InferenceCompletedEvent.make({
      ...envelope(seq),
      type: "InferenceCompleted",
      durability: "durable",
      payload: { runId: fenceIds.run, turnId: fenceIds.turn, inferenceId },
    }),
  });
const delta = (seq: number, text: string) =>
  PositionedEvent.make({
    position: Position.make({
      seq: SequenceNumber.make(seq),
      subSeq: SubSequenceNumber.make(1),
    }),
    event: TextDeltaEvent.make({
      ...envelope(100 + seq),
      type: "TextDelta",
      durability: "ephemeral",
      payload: { providerPartId: ProviderPartId.make("t"), delta: text },
    }),
  });

/**
 * A store whose head the test moves, and whose reads can be held open so a test can publish
 * while a subscriber's read is in flight.
 */
class GatedStore {
  head = 0;
  reads = 0;
  private gate: Deferred.Deferred<void> | undefined;

  hold(): void {
    this.gate = Effect.runSync(Deferred.make<void>());
  }

  release(): void {
    const gate = this.gate;
    this.gate = undefined;
    if (gate !== undefined) Effect.runSync(Deferred.succeed(gate, undefined));
  }

  readonly store: EDATranscriptStoreShape = {
    readTranscriptWindow: () =>
      Effect.suspend(() => {
        this.reads += 1;
        const window: TranscriptWindow = {
          incarnation: TranscriptLogIncarnation.make("memory:gated"),
          version: TranscriptVersion.make("a".repeat(64)),
          throughSeq: SequenceNumber.make(this.head),
          records: [],
          dependencies: [],
          hasOlder: false,
          hasNewer: false,
          readiness: { _tag: "Ready" },
        };
        return this.gate === undefined
          ? Effect.succeed(window)
          : Deferred.await(this.gate).pipe(Effect.as(window));
      }),
    readTranscriptMeta: Effect.succeed([]),
  };
}

const publishTo = (manager: EDAWebSocketConnectionManager, event: PositionedEvent) =>
  Effect.runPromise(
    Effect.flatMap(Effect.service(SessionEventObserver), (observer) =>
      observer.onEvent(event),
    ).pipe(Effect.provide(manager.observerLayer)),
  );

/** Yield to the event loop so held reads and queued drains reach their await points. */
const yieldToLoop = () => new Promise((resolve) => setTimeout(resolve, 0));

const isolates: Isolate[] = [];
afterEach(async () => {
  await Promise.all(isolates.splice(0).map((isolate) => isolate.dispose()));
});

const assistantRecords = (client: EDATranscriptClientState) =>
  (client.window?.records ?? []).flatMap((record) => (record.kind === "assistant" ? [record] : []));

/** Let the runtime fiber make progress until `done` holds. */
const until = async (done: () => boolean): Promise<void> => {
  for (let attempt = 0; attempt < 2_000 && !done(); attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  expect(done()).toBe(true);
};

describe("transcript-window sockets", () => {
  it("streams active content beyond the raw replay cap and retires it with the commit", async () => {
    const gate = Effect.runSync(Deferred.make<void>());
    let streamed = false;
    const deltas = Array.from({ length: 5_000 }, (_, index) => textDelta(`${index % 10}`));
    const isolate = new Isolate(() => [socket]);
    const socket = new TestSocket();
    isolates.push(isolate);
    await isolate.start(
      Stream.fromIterable(deltas).pipe(
        Stream.concat(
          Stream.fromEffect(
            Effect.sync(() => {
              streamed = true;
            }).pipe(Effect.andThen(Deferred.await(gate))),
          ).pipe(Stream.drain),
        ),
        Stream.concat(Stream.make(finish())),
      ),
    );

    isolate.manager.acceptTranscript({
      sessionId: SESSION_ID,
      trace: TRACE,
      webSocket: socket.asWebSocket(),
    });
    await socket.request(isolate.manager, edaTranscriptSubscribeRequest(socket.client));
    await socket.pump(isolate.manager);
    expect(socket.client.window?.readiness._tag).toBe("Ready");

    const submitted = isolate.submit(1, "hello");
    await until(() => streamed);
    await isolate.tick();
    await socket.pump(isolate.manager);

    const expected = deltas.map((part) => part.delta).join("");
    const streaming = socket.client.window?.activeContent;
    expect(streaming).toHaveLength(1);
    expect(streaming?.[0]?.parts).toEqual([
      { id: "text:text", part: { _tag: "Text", text: expected } },
    ]);
    // Live content is coalesced: a handful of updates, not one per delta.
    expect(socket.applied.length).toBeLessThan(20);

    Effect.runSync(Deferred.succeed(gate, undefined));
    await submitted;
    await isolate.tick();
    await socket.pump(isolate.manager);

    const [assistant] = assistantRecords(socket.client);
    expect(assistant?.lifecycle._tag).toBe("Completed");
    expect(assistant?.parts).toEqual([{ _tag: "Text", text: expected }]);
    expect(socket.client.window?.activeContent).toEqual([]);
    // The update that carries the committed record also retires its live content.
    const committing = socket.applied.find(({ update }) =>
      update?.records.some(
        (record) => record.kind === "assistant" && record.lifecycle._tag === "Completed",
      ),
    );
    expect(committing?.update?.activeContent).toEqual([]);
  });

  it("retires active content on a partial failure and on an empty success", async () => {
    const isolate = new Isolate(() => [socket]);
    const socket = new TestSocket();
    isolates.push(isolate);
    await isolate.start([
      Stream.make(
        textDelta("partial"),
        Response.makePart("error", { error: new Error("provider exploded") }),
      ),
      Stream.make(finish()),
    ]);
    isolate.manager.acceptTranscript({
      sessionId: SESSION_ID,
      trace: TRACE,
      webSocket: socket.asWebSocket(),
    });
    await socket.request(isolate.manager, edaTranscriptSubscribeRequest(socket.client));
    await socket.pump(isolate.manager);

    await isolate.submit(1, "first");
    await isolate.tick();
    await socket.pump(isolate.manager);
    const failed = assistantRecords(socket.client).at(-1);
    expect(failed?.lifecycle._tag).toBe("Failed");
    expect(failed?.parts).toEqual([{ _tag: "Text", text: "partial" }]);
    expect(socket.client.window?.activeContent).toEqual([]);

    await isolate.submit(2, "second");
    await isolate.tick();
    await socket.pump(isolate.manager);
    const empty = assistantRecords(socket.client).at(-1);
    expect(empty?.id).not.toBe(failed?.id);
    expect(empty?.lifecycle._tag).toBe("Completed");
    expect(socket.client.window?.activeContent).toEqual([]);
  });

  it("chunks a large update to a slow receiver and closes a socket that hibernated mid-transfer", async () => {
    const flowControl = { ...defaultEDAWebSocketFlowControl, maxFrameBytes: 8 * 1024 };
    const sockets: TestSocket[] = [];
    const first = new Isolate(() => sockets, flowControl);
    isolates.push(first);
    const big = "x".repeat(400 * 1024);
    await first.start(Stream.make(textDelta(big), finish()));
    await first.submit(1, "write a lot");

    const socket = new TestSocket();
    sockets.push(socket);
    first.manager.acceptTranscript({
      sessionId: SESSION_ID,
      trace: TRACE,
      webSocket: socket.asWebSocket(),
    });
    await socket.request(first.manager, edaTranscriptSubscribeRequest(socket.client));
    // Hello plus exactly the flow-control window of chunks; the rest are staged.
    expect(socket.inbox.length).toBe(1 + flowControl.maxInFlightFrames);
    await socket.pump(first.manager, 4);
    expect(socket.client.window).toBeUndefined();
    expect(socket.client.assembling?.data.length).toBe(4);

    // The isolate is evicted with chunks still staged; the socket wakes in a new isolate.
    const second = new Isolate(() => sockets, flowControl, first.transcriptStore);
    await socket.pump(second.manager, 1);
    expect(socket.closeCode).toBe(EDA_WS_CLOSE_TRANSCRIPT_RESUBSCRIBE);

    // The client reconnects and resubscribes; the interrupted transfer is replaced whole.
    const reconnected = new TestSocket();
    reconnected.client = edaTranscriptClientReconnected(socket.client);
    sockets.push(reconnected);
    second.manager.acceptTranscript({
      sessionId: SESSION_ID,
      trace: TRACE,
      webSocket: reconnected.asWebSocket(),
    });
    await reconnected.request(second.manager, edaTranscriptSubscribeRequest(reconnected.client));
    await reconnected.pump(second.manager);
    // Hello plus more chunks than fit in flight at once, all applied as one update.
    expect(reconnected.sentCount).toBeGreaterThan(flowControl.maxInFlightFrames + 1);
    expect(reconnected.applied).toHaveLength(1);
    expect(assistantRecords(reconnected.client)[0]?.parts).toEqual([{ _tag: "Text", text: big }]);
  });

  it("resumes an idle subscription after hibernation without volatile state", async () => {
    const sockets: TestSocket[] = [];
    const first = new Isolate(() => sockets);
    isolates.push(first);
    await first.start([
      Stream.make(textDelta("one"), finish()),
      Stream.make(textDelta("two"), finish()),
    ]);
    await first.submit(1, "q1");
    await first.submit(2, "q2");

    const socket = new TestSocket();
    sockets.push(socket);
    first.manager.acceptTranscript({
      sessionId: SESSION_ID,
      trace: TRACE,
      webSocket: socket.asWebSocket(),
    });
    await socket.request(first.manager, edaTranscriptSubscribeRequest(socket.client, { count: 1 }));
    await socket.pump(first.manager);
    expect(socket.client.window?.records).toHaveLength(1);

    const second = new Isolate(
      () => sockets,
      defaultEDAWebSocketFlowControl,
      first.transcriptStore,
    );
    const page = edaTranscriptPageRequest(socket.client, "before", 2);
    if (page === undefined) throw new Error("a window with records has a first cursor");
    await socket.request(second.manager, page);
    await socket.pump(second.manager);
    expect(socket.closeCode).toBeUndefined();
    expect(socket.client.window?.records).toHaveLength(3);
  });
  it("holds a capture taken between commit and publication until publication catches up", async () => {
    // The store's committed head runs ahead of what has been published, as between an append and
    // its publication.
    let head = 0;
    const window = (): TranscriptWindow => ({
      incarnation: TranscriptLogIncarnation.make("memory:fence"),
      version: TranscriptVersion.make("a".repeat(64)),
      throughSeq: SequenceNumber.make(head),
      records: [],
      dependencies: [],
      hasOlder: false,
      hasNewer: false,
      readiness: { _tag: "Ready" },
    });
    const socket = new TestSocket();
    const isolate = new Isolate(() => [socket], defaultEDAWebSocketFlowControl, {
      readTranscriptWindow: () => Effect.sync(window),
      readTranscriptMeta: Effect.succeed([]),
    });
    const publish = async (event: PositionedEvent) => {
      await Effect.runPromise(
        Effect.flatMap(Effect.service(SessionEventObserver), (observer) =>
          observer.onEvent(event),
        ).pipe(Effect.provide(isolate.manager.observerLayer)),
      );
      await isolate.manager.settled();
    };

    isolate.manager.acceptTranscript({
      sessionId: SESSION_ID,
      trace: TRACE,
      webSocket: socket.asWebSocket(),
    });
    await socket.request(isolate.manager, edaTranscriptSubscribeRequest(socket.client));
    await socket.pump(isolate.manager);
    head = 1;
    await publish(started(1));
    await publish(delta(1, "live"));
    await isolate.tick();
    await socket.pump(isolate.manager);
    expect(socket.client.window?.activeContent[0]?.parts[0]?.part).toEqual({
      _tag: "Text",
      text: "live",
    });

    // Committed but not yet published: a capture now would pair the commit with stale live parts.
    head = 2;
    const applied = socket.applied.length;
    await socket.request(isolate.manager, { _tag: "subscribe", tail: true });
    await isolate.tick();
    await socket.pump(isolate.manager);
    expect(socket.applied.length).toBe(applied);

    // Publication catches up: the held capture is served at the new head with live content retired.
    await publish(completed(2));
    await isolate.tick();
    await socket.pump(isolate.manager);
    expect(socket.client.window?.throughSeq).toBe(2);
    expect(socket.client.window?.activeContent).toEqual([]);

    // A capture that waits past the fence bound is served without live content.
    head = 3;
    await publish(started(3));
    await publish(delta(3, "again"));
    head = 4;
    await socket.request(isolate.manager, { _tag: "subscribe", tail: true });
    expect(socket.client.window?.throughSeq).toBe(2);
    isolate.nowMs += 5_000;
    await isolate.tick();
    await socket.pump(isolate.manager);
    expect(socket.client.window?.throughSeq).toBe(4);
    expect(socket.client.window?.activeContent).toEqual([]);
  });
  it("delivers a commit published while a read is in flight", async () => {
    const gated = new GatedStore();
    const socket = new TestSocket();
    const isolate = new Isolate(() => [socket], defaultEDAWebSocketFlowControl, gated.store);
    isolate.manager.acceptTranscript({
      sessionId: SESSION_ID,
      trace: TRACE,
      webSocket: socket.asWebSocket(),
    });
    await socket.request(isolate.manager, edaTranscriptSubscribeRequest(socket.client));
    await socket.pump(isolate.manager);

    gated.hold();
    gated.head = 1;
    await publishTo(
      isolate.manager,
      completed(1, InferenceId.make("018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6b")),
    );
    await yieldToLoop();
    gated.head = 2;
    await publishTo(
      isolate.manager,
      completed(2, InferenceId.make("018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6b")),
    );
    gated.release();
    await isolate.manager.settled();
    await socket.pump(isolate.manager);
    expect(socket.client.window?.throughSeq).toBe(2);
  });

  it("retires live content only in the update that carries its commit", async () => {
    const gated = new GatedStore();
    const socket = new TestSocket();
    const isolate = new Isolate(() => [socket], defaultEDAWebSocketFlowControl, gated.store);
    isolate.manager.acceptTranscript({
      sessionId: SESSION_ID,
      trace: TRACE,
      webSocket: socket.asWebSocket(),
    });
    await socket.request(isolate.manager, edaTranscriptSubscribeRequest(socket.client));
    await socket.pump(isolate.manager);
    gated.head = 1;
    await publishTo(isolate.manager, started(1));
    await publishTo(isolate.manager, delta(1, "live"));
    await isolate.tick();
    await socket.pump(isolate.manager);
    expect(socket.client.window?.activeContent).toHaveLength(1);

    // A request's read is in flight at head 1 when the commit lands and is published.
    gated.hold();
    const request = socket.request(isolate.manager, { _tag: "subscribe", tail: true });
    await yieldToLoop();
    gated.head = 2;
    await publishTo(isolate.manager, completed(2));
    gated.release();
    await request;
    await isolate.tick();
    await socket.pump(isolate.manager);

    // Every update after the first (which installed the empty window) that clears live content.
    const retiring = socket.applied
      .slice(1)
      .filter(({ update }) => update?.activeContent?.length === 0);
    expect(retiring.map(({ update }) => update?.throughSeq)).toEqual([2]);
    expect(socket.client.window?.throughSeq).toBe(2);
  });

  it("pushes commits to transcript sockets when the host cannot enumerate sockets", async () => {
    const gated = new GatedStore();
    const manager = new EDAWebSocketConnectionManager({
      isSessionReady: () => true,
      prepareSession: async () => undefined,
      readEventPage: async () => ({ events: [], head: SequenceNumber.make(0) }),
      transcript: { transcriptStore: gated.store, schedule: () => undefined },
    });
    const socket = new TestSocket();
    manager.acceptTranscript({
      sessionId: SESSION_ID,
      trace: TRACE,
      webSocket: socket.asWebSocket(),
    });
    await socket.request(manager, edaTranscriptSubscribeRequest(socket.client));
    await socket.pump(manager);
    gated.head = 1;
    await publishTo(manager, completed(1));
    await manager.settled();
    await socket.pump(manager);
    expect(socket.client.window?.throughSeq).toBe(1);
  });
  it("publishes live content at the display cadence however fast the client acknowledges", async () => {
    const gated = new GatedStore();
    const socket = new TestSocket();
    const isolate = new Isolate(() => [socket], defaultEDAWebSocketFlowControl, gated.store);
    isolate.manager.acceptTranscript({
      sessionId: SESSION_ID,
      trace: TRACE,
      webSocket: socket.asWebSocket(),
    });
    await socket.request(isolate.manager, edaTranscriptSubscribeRequest(socket.client));
    await socket.pump(isolate.manager);
    gated.head = 1;
    await publishTo(isolate.manager, started(1));
    await isolate.manager.settled();
    // Leave that update unacknowledged, so every loop below has an ACK to deliver.
    expect(socket.inbox.length).toBe(1);

    const before = socket.applied.length + 1;
    for (let index = 0; index < 50; index += 1) {
      await publishTo(isolate.manager, delta(1, `${index % 10}`));
      await isolate.manager.settled();
      await socket.pump(isolate.manager, 1);
    }
    // ACKs alone never release live content between cadence ticks.
    expect(socket.applied.length - before).toBeLessThanOrEqual(1);
    await isolate.tick();
    await socket.pump(isolate.manager);
    expect(socket.client.window?.activeContent[0]?.parts[0]?.part).toEqual({
      _tag: "Text",
      text: Array.from({ length: 50 }, (_, index) => `${index % 10}`).join(""),
    });
  });
  it("stops polling once a capture outlives a fence that never catches up", async () => {
    const gated = new GatedStore();
    const socket = new TestSocket();
    const isolate = new Isolate(() => [socket], defaultEDAWebSocketFlowControl, gated.store);
    isolate.manager.acceptTranscript({
      sessionId: SESSION_ID,
      trace: TRACE,
      webSocket: socket.asWebSocket(),
    });
    await socket.request(isolate.manager, edaTranscriptSubscribeRequest(socket.client));
    await socket.pump(isolate.manager);
    gated.head = 1;
    await publishTo(isolate.manager, started(1));
    await publishTo(isolate.manager, delta(1, "live"));
    await isolate.manager.settled();
    // A commit that is never published (the session failed after its append).
    gated.head = 2;
    await socket.request(isolate.manager, { _tag: "subscribe", tail: true });
    for (let tick = 0; tick < 200; tick += 1) {
      isolate.nowMs += 50;
      await isolate.tick();
      await socket.pump(isolate.manager);
    }
    expect(socket.client.window?.throughSeq).toBe(2);
    expect(socket.client.window?.activeContent).toEqual([]);
    // The timed-out capture was served once; nothing keeps re-reading or re-arming the timer.
    expect(gated.reads).toBeLessThan(40);
    expect(isolate.timers).toEqual([]);
  });
});
