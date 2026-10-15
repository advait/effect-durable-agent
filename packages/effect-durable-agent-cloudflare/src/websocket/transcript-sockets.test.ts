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
import { CommandId, SequenceNumber, SessionId } from "effect-durable-agent/types/core";
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
});
