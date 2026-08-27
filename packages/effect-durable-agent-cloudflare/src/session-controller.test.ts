import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as LanguageModel from "effect/unstable/ai/LanguageModel";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { describe, expect, it, vi } from "vite-plus/test";

import { SubmitMessageCommand } from "effect-durable-agent/types/commands";
import { CommandId, EventId, SequenceNumber, SessionId } from "effect-durable-agent/types/core";
import {
  EDASessionController,
  type EDASessionControllerOptions,
  type EDASessionDurableObjectStorage,
} from "./session-controller";
import { makeEDADurableObjectOpenAiModelLayer } from "./providers/openai";
import { DurableObjectKeepAlive } from "./durable-object-keepalive";
import {
  EDA_WEB_SOCKET_PING_MESSAGE,
  EDA_WEB_SOCKET_PONG_MESSAGE,
  EDA_WS_CLOSE_PROTOCOL_ERROR,
  EDAWebSocketAckFrame,
  FrameId,
} from "effect-durable-agent/websocket";
import { EDAWebSocketAttachment } from "./websocket/attachment";
import { CompactionExecutor, CompactionPolicy } from "effect-durable-agent/services/compaction";
import { frameworkReducedStateReducerName } from "effect-durable-agent/domain/reduced-state";
import type { EDAReducer } from "effect-durable-agent/services/reducer-registry";
import type { DurableObjectSqlCursor } from "./durable-object-storage";
import { makeLanguageModelLayer, type TestModelParts } from "effect-durable-agent/testkit/layers";
import {
  DurableEventEnvelope,
  EventNamespace,
  EventType,
  UnixEpochMillis,
  schemaV1,
} from "effect-durable-agent/types/events";
import { makeRootEDATraceMetadata } from "effect-durable-agent/types/tracing";

const SESSION_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const OTHER_SESSION_ID = "018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a";
const COMMAND_ID = "018f6bd5-2f2a-7b1e-8f1c-1f2e3d4c5b6a";
const EVENT_ID_A = "018f6bd5-2f2a-7b1e-8f1d-1f2e3d4c5b6a";
const EVENT_ID_B = "018f6bd5-2f2a-7b1e-8f1e-1f2e3d4c5b6a";
const TRACE = makeRootEDATraceMetadata();

const makeCommand = (commandId = COMMAND_ID) =>
  new SubmitMessageCommand({
    commandId: CommandId.make(commandId),
    disposition: "queue",
    content: [Prompt.textPart({ text: "hello" })],
  });

const appFact = (value: string): DurableEventEnvelope =>
  DurableEventEnvelope.make({
    namespace: EventNamespace.make("test-app"),
    type: EventType.make("ExternalFact"),
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(value === "first" ? EVENT_ID_A : EVENT_ID_B),
    sessionId: SessionId.make(SESSION_ID),
    createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
    payload: { value },
  });

const usage = () =>
  new Response.Usage({
    inputTokens: { total: 1, uncached: undefined, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: undefined, reasoning: undefined },
  });

const fakeAiGateway = (
  onRun?: (request: AIGatewayUniversalRequest | AIGatewayUniversalRequest[]) => void,
): AiGateway =>
  ({
    getLog: async () => ({}),
    getUrl: async () => "https://gateway.example.com/openai",
    patchLog: async () => undefined,
    run: async (request: AIGatewayUniversalRequest | AIGatewayUniversalRequest[]) => {
      onRun?.(request);
      return new globalThis.Response(
        `data: ${JSON.stringify({
          type: "response.completed",
          response: {
            id: "response-1",
            object: "response",
            model: "gpt-5.5",
            created_at: 1_715_000_000,
            output: [],
          },
          sequence_number: 1,
        })}\n\n`,
        { headers: { "content-type": "text/event-stream" }, status: 200 },
      );
    },
  }) as unknown as AiGateway;

const finishedStream = (text: string) =>
  Stream.make(
    Response.makePart("text-delta", { id: "text-1", delta: text }),
    Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
  );

const makeHost = (
  storage: FakeDurableObjectStorage,
  options: {
    readonly compaction?: boolean;
    readonly getWebSockets?: () => ReadonlyArray<WebSocket>;
    readonly keepAlive?: DurableObjectKeepAlive;
    readonly modelLayer?: EDASessionControllerOptions["modelLayer"];
    readonly parts?: TestModelParts;
    readonly reducers?: ReadonlyArray<EDAReducer<any>>;
    readonly summary?: string;
  } = {},
) =>
  new EDASessionController({
    config: { modelSelection: { provider: "test", modelId: "test-model" } },
    ...(options.compaction === true
      ? {
          compactionExecutorLayer: CompactionExecutor.LanguageModelSummary({
            executorId: "test.host-summary",
            summaryMessagePrefix: "Summary:",
          }),
          compactionPolicyLayer: CompactionPolicy.ApproximateTokenThreshold({
            policyId: "test.host-approximate",
            thresholdTokens: 1,
            retainTailTokens: 0,
            minSummarizableTokens: 1,
            charsPerToken: 1,
          }),
        }
      : {}),
    ...(options.getWebSockets === undefined ? {} : { getWebSockets: options.getWebSockets }),
    ...(options.keepAlive === undefined ? {} : { keepAlive: options.keepAlive }),
    modelLayer:
      options.modelLayer ??
      makeLanguageModelLayer({
        generateText: options.summary,
        parts: options.parts ?? finishedStream("pong"),
      }),
    reducers: options.reducers,
    storage,
  });

const waitForReducerCheckpointRow = async (
  storage: FakeDurableObjectStorage,
  reducerName: string,
  predicate: (row: FakeReducerCheckpointRow) => boolean = () => true,
): Promise<FakeReducerCheckpointRow> => {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const row = storage.reducerCheckpointRows.find(
      (row) => row.reducer_name === reducerName && predicate(row),
    );
    if (row !== undefined) {
      return row;
    }
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error(`Timed out waiting for reducer checkpoint ${reducerName}`);
};

describe("makeEDADurableObjectOpenAiModelLayer", () => {
  it("allows Cloudflare AI Gateway binding-backed OpenAI calls without a local provider key", () => {
    expect(() =>
      makeEDADurableObjectOpenAiModelLayer({
        aiGateway: fakeAiGateway(),
        modelId: "gpt-5.5",
      }),
    ).not.toThrow();
  });

  it("requires a provider key when no gateway binding is configured", () => {
    expect(() => makeEDADurableObjectOpenAiModelLayer({ modelId: "gpt-5.5" })).toThrow(
      /OPENAI_API_KEY/,
    );
  });

  it("sends configured reasoning summaries through the Gateway request boundary", async () => {
    let gatewayRequest: AIGatewayUniversalRequest | AIGatewayUniversalRequest[] | undefined;
    const modelLayer = makeEDADurableObjectOpenAiModelLayer({
      aiGateway: fakeAiGateway((request) => {
        gatewayRequest = request;
      }),
      config: {
        reasoning: { summary: "auto" },
        store: false,
      },
      modelId: "gpt-5.5",
    });

    await Effect.runPromise(
      Effect.gen(function* () {
        const model = yield* LanguageModel.LanguageModel;
        yield* model.streamText({ prompt: "hello" }).pipe(Stream.runDrain);
      }).pipe(Effect.provide(modelLayer)),
    );

    expect(gatewayRequest).toMatchObject({
      endpoint: "responses",
      provider: "openai",
      query: {
        model: "gpt-5.5",
        reasoning: { summary: "auto" },
        store: false,
        stream: true,
      },
    });
  });
});

describe("EDASessionController", () => {
  it("runs idempotent startup migrations", async () => {
    const storage = new FakeDurableObjectStorage();

    await Effect.runPromise(EDASessionController.migrate(storage));
    await Effect.runPromise(EDASessionController.migrate(storage));

    expect(storage.appliedMigrations).toEqual([1]);
    expect(storage.migrationInsertCount).toBe(1);
    expect(storage.createdReducerCheckpointTable).toBe(true);
  });

  it("runs a canned-model command through persistence and cold-start hydration", async () => {
    const storage = new FakeDurableObjectStorage();
    await Effect.runPromise(EDASessionController.migrate(storage));
    const host = makeHost(storage);

    const terminal = await host.submitAndBlock({
      command: makeCommand(),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
    });
    const messages = await host.messages({ sessionId: SessionId.make(SESSION_ID), trace: TRACE });
    const snapshot = await host.snapshot({ sessionId: SessionId.make(SESSION_ID), trace: TRACE });

    expect(terminal.event.type).toBe("CommandCompleted");
    expect(messages.map((message) => message._tag)).toEqual(["User", "Assistant"]);
    expect(messages[1]).toMatchObject({ _tag: "Assistant", content: { text: "pong" } });
    expect(storage.eventRows.map((row) => row.type)).toContain("CommandAdmitted");
    expect(storage.eventRows.map((row) => row.type)).toContain("CommandCompleted");
    const turnCompletedSeq = storage.eventRows.find((row) => row.type === "TurnCompleted")?.seq;
    expect(turnCompletedSeq).toBeDefined();
    await waitForReducerCheckpointRow(
      storage,
      frameworkReducedStateReducerName,
      (row) => row.through_seq === turnCompletedSeq,
    );
    expect(snapshot.state.lastSeq).toBe(storage.eventRows.at(-1)?.seq);

    await host.dispose();
    const recreated = makeHost(storage);
    const replayedMessages = await recreated.messages({
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
    });
    const replayedSnapshot = await recreated.snapshot({
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
    });

    expect(replayedMessages).toEqual(messages);
    expect(replayedSnapshot).toEqual(snapshot);
  });

  it("accepts real compaction policy/executor layers in the Durable Object host", async () => {
    const storage = new FakeDurableObjectStorage();
    await Effect.runPromise(EDASessionController.migrate(storage));
    const host = makeHost(storage, {
      compaction: true,
      parts: [finishedStream("first response"), finishedStream("second response")],
      summary: "First turn summary",
    });

    await host.submitAndBlock({
      command: makeCommand(),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
    });
    await host.submitAndBlock({
      command: makeCommand("018f6bd5-2f2a-7b1e-8f1d-1f2e3d4c5b6a"),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
    });
    const snapshot = await host.snapshot({ sessionId: SessionId.make(SESSION_ID), trace: TRACE });

    expect(storage.eventRows.map((row) => row.type)).toEqual(
      expect.arrayContaining([
        "CompactionRequested",
        "CompactionStarted",
        "SummaryCreated",
        "ContextRebased",
        "CompactionCompleted",
      ]),
    );
    expect(storage.summaryRows.length).toBeGreaterThanOrEqual(1);
    expect(snapshot.state.context.currentSummaryId).toBe(storage.summaryRows.at(-1)?.summary_id);
  });

  it("streams live events over WebSocket and advances the ACK attachment", async () => {
    const storage = new FakeDurableObjectStorage();
    await Effect.runPromise(EDASessionController.migrate(storage));
    const host = makeHost(storage);
    const webSocket = new TestWebSocket();

    await host.acceptEventWebSocket({
      afterSeq: SequenceNumber.make(0),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
      webSocket: webSocket.asWebSocket(),
    });
    const eventsPromise = collectWebSocketEventsUntil(
      host,
      webSocket,
      (event) => event.event.type === "CommandCompleted",
    );

    await host.submit({
      command: makeCommand(),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
    });
    const events = await eventsPromise;
    const attachment = EDAWebSocketAttachment.make(webSocket.deserializeAttachment());

    expect(events.map((event) => event.event.type)).toContain("CommandCompleted");
    expect(attachment.delivery.lastAckedSeq).toBeGreaterThan(0);
  });

  it("keeps an idle accepted WebSocket silent: no heartbeats, timers, or frames", async () => {
    const storage = new FakeDurableObjectStorage();
    await Effect.runPromise(EDASessionController.migrate(storage));
    const host = makeHost(storage);
    const webSocket = new TestWebSocket();

    await host.acceptEventWebSocket({
      afterSeq: SequenceNumber.make(0),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
      webSocket: webSocket.asWebSocket(),
    });
    const hello = JSON.parse(await webSocket.nextMessage()) as { readonly _tag: string };
    expect(hello._tag).toBe("hello");
    const sentAfterHello = webSocket.sentCount;

    // The previous implementation parked a subscriber fiber on a 10s heartbeat
    // timer; advancing fake time by an hour would have produced 360 frames.
    vi.useFakeTimers();
    try {
      await vi.advanceTimersByTimeAsync(3_600_000);
    } finally {
      vi.useRealTimers();
    }

    expect(webSocket.sentCount).toBe(sentAfterHello);
    expect(webSocket.closeCode).toBeUndefined();
  });

  it("persists complete in-flight receipts in an attachment that fits Cloudflare's limit", async () => {
    const storage = new FakeDurableObjectStorage();
    await Effect.runPromise(EDASessionController.migrate(storage));
    const host = makeHost(storage);
    const webSocket = new TestWebSocket();

    await host.acceptEventWebSocket({
      afterSeq: SequenceNumber.make(0),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
      webSocket: webSocket.asWebSocket(),
    });
    await webSocket.nextMessage();
    await host.submitAndBlock({
      command: makeCommand(),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
    });

    const rawAttachment = webSocket.deserializeAttachment();
    const attachment = Schema.decodeUnknownSync(EDAWebSocketAttachment)(rawAttachment);
    expect(attachment.delivery.inFlight.length).toBeGreaterThan(0);
    expect(attachment.delivery.lastSentFrameId).toBe(attachment.delivery.inFlight.at(-1)?.frameId);
    expect(new TextEncoder().encode(JSON.stringify(rawAttachment)).byteLength).toBeLessThan(16_384);
  });

  it("answers client pings with a pong without event-socket state", async () => {
    const storage = new FakeDurableObjectStorage();
    await Effect.runPromise(EDASessionController.migrate(storage));
    const host = makeHost(storage);
    const webSocket = new TestWebSocket();

    await host.webSocketMessage(webSocket.asWebSocket(), EDA_WEB_SOCKET_PING_MESSAGE);

    expect(await webSocket.nextMessage()).toBe(EDA_WEB_SOCKET_PONG_MESSAGE);
    expect(webSocket.closeCode).toBeUndefined();
  });

  it("logs and contains WebSocket observer failures instead of failing EDA", async () => {
    const storage = new FakeDurableObjectStorage();
    await Effect.runPromise(EDASessionController.migrate(storage));
    const host = makeHost(storage, {
      getWebSockets: () => {
        throw new Error("socket enumeration failed");
      },
    });

    const terminal = await host.submitAndBlock({
      command: makeCommand(),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
    });

    expect(terminal.event.type).toBe("CommandCompleted");
  });

  it("closes WebSockets with malformed EDA attachments on their first client message", async () => {
    const storage = new FakeDurableObjectStorage();
    await Effect.runPromise(EDASessionController.migrate(storage));
    const host = makeHost(storage);
    const webSocket = new TestWebSocket();
    webSocket.serializeAttachment({ kind: "eda-events-v2", sessionId: SESSION_ID });

    await host.webSocketMessage(
      webSocket.asWebSocket(),
      JSON.stringify(
        EDAWebSocketAckFrame.make({
          _tag: "ack",
          frameId: FrameId.make(1),
          durableThroughSeq: SequenceNumber.make(1),
        }),
      ),
    );

    expect(webSocket.closeCode).toBe(EDA_WS_CLOSE_PROTOCOL_ERROR);
    expect(webSocket.closeReason).toBe("protocol");
  });

  it("closes malformed restored WebSockets discovered during event fanout", async () => {
    const storage = new FakeDurableObjectStorage();
    await Effect.runPromise(EDASessionController.migrate(storage));
    const webSocket = new TestWebSocket();
    webSocket.serializeAttachment({ kind: "eda-events-v1", sessionId: SESSION_ID });
    const host = makeHost(storage, { getWebSockets: () => [webSocket.asWebSocket()] });

    await host.submitAndBlock({
      command: makeCommand(),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
    });

    expect(webSocket.closeCode).toBe(EDA_WS_CLOSE_PROTOCOL_ERROR);
    expect(webSocket.closeReason).toBe("protocol");
  });

  it("resumes delivery from the persisted cursor after isolate eviction", async () => {
    const storage = new FakeDurableObjectStorage();
    await Effect.runPromise(EDASessionController.migrate(storage));
    const webSocket = new TestWebSocket();

    const firstHost = makeHost(storage);
    await firstHost.acceptEventWebSocket({
      afterSeq: SequenceNumber.make(0),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
      webSocket: webSocket.asWebSocket(),
    });
    const firstEvents = collectAckedEventsUntil(
      firstHost,
      webSocket,
      (event) => event.event.type === "CommandCompleted",
    );
    await firstHost.submit({
      command: makeCommand(),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
    });
    await firstEvents;
    await firstHost.dispose();
    const attachment = EDAWebSocketAttachment.make(webSocket.deserializeAttachment());
    expect(attachment.delivery.lastAckedSeq).toBeGreaterThan(0);

    // Simulate isolate eviction: a fresh host with no in-memory socket state,
    // discovering the still-open socket only through getWebSockets().
    const secondHost = makeHost(storage, { getWebSockets: () => [webSocket.asWebSocket()] });
    const secondEvents = collectAckedEventsUntil(
      secondHost,
      webSocket,
      (event) => event.event.type === "CommandCompleted",
    );
    await secondHost.submit({
      command: makeCommand("018f6bd5-2f2a-7b1e-8f1e-1f2e3d4c5b6a"),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
    });
    const events = await secondEvents;

    const durableSeqs = events
      .filter((event) => event.position.subSeq === 0)
      .map((event) => event.position.seq);
    expect(durableSeqs.length).toBeGreaterThan(0);
    expect(Math.min(...durableSeqs)).toBeGreaterThan(attachment.delivery.lastAckedSeq);
    expect(events.map((event) => event.event.type)).toContain("CommandCompleted");

    // A late duplicate ACK from before eviction must be tolerated, not protocol-closed.
    await secondHost.webSocketMessage(
      webSocket.asWebSocket(),
      JSON.stringify(
        EDAWebSocketAckFrame.make({
          _tag: "ack",
          frameId: FrameId.make(1),
          durableThroughSeq: SequenceNumber.make(1),
        }),
      ),
    );
    expect(webSocket.closeCode).toBeUndefined();
  });

  it("runs startup recovery before a cold alarm returns", async () => {
    const storage = new FakeDurableObjectStorage();
    await Effect.runPromise(EDASessionController.migrate(storage));
    const activeStream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "partial" }),
    ).pipe(Stream.concat(Stream.never));
    const firstHost = makeHost(storage, { parts: activeStream });

    await firstHost.submit({ command: makeCommand(), sessionId: SessionId.make(SESSION_ID) });
    await waitForEventType(storage, "TurnStarted");
    await firstHost.dispose();
    storage.setAlarm(1_000);

    expect(storage.eventRows.map((row) => row.type)).not.toContain("CommandCancelled");

    const alarmHost = makeHost(storage);
    await alarmHost.alarm({ sessionId: SessionId.make(SESSION_ID) });

    expect(storage.eventRows.map((row) => row.type)).toEqual(
      expect.arrayContaining(["TurnFailed", "RunFailed", "RunStarted", "TurnStarted"]),
    );
    expect(storage.alarm).toBeNull();
  });

  it("retries runtime construction after a rejected build", async () => {
    const storage = new FakeDurableObjectStorage();
    await Effect.runPromise(EDASessionController.migrate(storage));
    let attempts = 0;
    const modelLayer: Layer.Layer<LanguageModel.LanguageModel> = Layer.unwrap(
      Effect.sync<Layer.Layer<LanguageModel.LanguageModel>>(() => {
        attempts += 1;
        if (attempts === 1) {
          throw new Error("model layer unavailable");
        }
        return makeLanguageModelLayer(finishedStream("pong"));
      }),
    );
    const host = makeHost(storage, { modelLayer });

    await expect(host.snapshot({ sessionId: SessionId.make(SESSION_ID) })).rejects.toThrow(
      "model layer unavailable",
    );

    const snapshot = await host.snapshot({ sessionId: SessionId.make(SESSION_ID) });
    expect(attempts).toBe(2);
    expect(snapshot.state.lastSeq).toBe(0);
  });

  it("shuts down keepalive alarm state on destroy", async () => {
    const storage = new FakeDurableObjectStorage();
    const keepAlive = new DurableObjectKeepAlive(storage, undefined, {
      intervalMs: 5_000,
      now: () => 1_000,
    });
    const lease = await keepAlive.acquire();
    const host = makeHost(storage, { keepAlive });

    expect(storage.alarm).toBe(6_000);

    await host.destroy({ sessionId: SessionId.make(SESSION_ID) });
    await lease.release();

    expect(storage.alarm).toBeNull();
  });

  it("hydrates app reducer state from checkpoint plus tail when the host is recreated", async () => {
    const storage = new FakeDurableObjectStorage();
    const reducer: EDAReducer<{ readonly seen: ReadonlyArray<string> }> = {
      name: "test.app-events",
      schemaVersion: 1,
      initial: { seen: [] },
      stateSchema: Schema.Struct({ seen: Schema.Array(Schema.String) }),
      reduce: (state, event) =>
        event.event.type === EventType.make("ExternalFact")
          ? { seen: [...state.seen, String((event.event.payload as { value: string }).value)] }
          : state,
    };
    await Effect.runPromise(EDASessionController.migrate(storage));
    const firstHost = makeHost(storage, { reducers: [reducer] });

    await firstHost.submitBatch({
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
      items: [appFact("first")],
    });
    await firstHost.dispose();

    const recreated = makeHost(storage, { reducers: [reducer] });
    await recreated.snapshot({ sessionId: SessionId.make(SESSION_ID) });
    await waitForReducerCheckpointRow(storage, reducer.name, (row) => row.through_seq === 1);
    await recreated.submitBatch({
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
      items: [appFact("second")],
    });
    const snapshot = await recreated.snapshot({
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
    });

    expect(snapshot.reducerStates.get(reducer.name)).toEqual({ seen: ["first", "second"] });
  });

  it("rejects attempts to reuse one DO storage instance for a different session", async () => {
    const storage = new FakeDurableObjectStorage();
    await Effect.runPromise(EDASessionController.migrate(storage));
    const host = makeHost(storage);

    await host.submit({
      command: makeCommand(),
      sessionId: SessionId.make(SESSION_ID),
      trace: TRACE,
    });

    await expect(
      host.snapshot({ sessionId: SessionId.make(OTHER_SESSION_ID), trace: TRACE }),
    ).rejects.toThrow("scoped to session");
  });
});

class TestWebSocket {
  private attachment: unknown;
  private closed = false;
  private readonly messages: string[] = [];
  private readonly waiters: Array<(message: string) => void> = [];
  closeCode: number | undefined;
  closeReason: string | undefined;
  sentCount = 0;
  readonly readyState = 1;

  asWebSocket(): WebSocket {
    return this as unknown as WebSocket;
  }

  send(message: string | ArrayBuffer | ArrayBufferView): void {
    if (this.closed) {
      throw new Error("WebSocket is closed");
    }
    if (typeof message !== "string") {
      throw new Error("Expected text WebSocket frame");
    }
    this.sentCount += 1;
    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter(message);
      return;
    }
    this.messages.push(message);
  }

  close(code?: number, reason?: string): void {
    this.closed = true;
    this.closeCode = code;
    this.closeReason = reason;
  }

  serializeAttachment(attachment: unknown): void {
    this.attachment = attachment;
  }

  deserializeAttachment(): unknown {
    return this.attachment;
  }

  nextMessage(): Promise<string> {
    const message = this.messages.shift();
    if (message !== undefined) {
      return Promise.resolve(message);
    }
    return new Promise((resolve) => this.waiters.push(resolve));
  }
}

interface CollectedTestEvent {
  readonly event: { readonly type: string };
  readonly position: { readonly seq: number; readonly subSeq: number };
}

/** ACK every events frame like a live client, without closing the socket afterwards. */
const collectAckedEventsUntil = async (
  host: EDASessionController,
  webSocket: TestWebSocket,
  predicate: (event: CollectedTestEvent) => boolean,
) => {
  const events: CollectedTestEvent[] = [];
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => reject(new Error("Timed out waiting for WebSocket event")), 10_000);
  });
  const readPromise = (async () => {
    while (true) {
      const frame = JSON.parse(await webSocket.nextMessage()) as {
        readonly _tag: string;
        readonly durableThroughSeq?: number;
        readonly events?: Array<CollectedTestEvent>;
        readonly frameId?: number;
      };
      if (frame._tag === "hello" || frame._tag === "heartbeat" || frame._tag === "pong") {
        continue;
      }
      if (frame._tag !== "events" || frame.events === undefined || frame.frameId === undefined) {
        throw new Error(`Unexpected WebSocket frame ${JSON.stringify(frame)}`);
      }
      events.push(...frame.events);
      await host.webSocketMessage(
        webSocket.asWebSocket(),
        JSON.stringify(
          EDAWebSocketAckFrame.make({
            _tag: "ack",
            frameId: FrameId.make(frame.frameId),
            durableThroughSeq: SequenceNumber.make(frame.durableThroughSeq ?? 0),
          }),
        ),
      );
      if (frame.events.some(predicate)) {
        return events;
      }
    }
  })();
  try {
    return await Promise.race([readPromise, timeoutPromise]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
};

const collectWebSocketEventsUntil = async (
  host: EDASessionController,
  webSocket: TestWebSocket,
  predicate: (event: CollectedTestEvent) => boolean,
) => {
  try {
    return await collectAckedEventsUntil(host, webSocket, predicate);
  } finally {
    webSocket.close(1000, "test complete");
    await host.webSocketClose(webSocket.asWebSocket());
  }
};

const waitForEventType = async (
  storage: FakeDurableObjectStorage,
  type: string,
  timeoutMs = 1_000,
): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (storage.eventRows.some((row) => row.type === type)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error(`Timed out waiting for ${type}`);
};

interface FakeEventRow {
  readonly event_id: string;
  readonly namespace: string;
  readonly type: string;
  readonly schema_version: number;
  readonly created_at_ms: number;
  readonly trace_json: string;
  readonly fact_json: string;
  readonly seq: number;
}

interface FakeCommandRow {
  readonly command_id: string;
  readonly admitted_seq: number;
  readonly status: string;
  readonly idempotency_key: string | null;
  readonly payload_json: string;
}

interface FakeCommandInputRow {
  readonly command_id: string;
  readonly admitted_seq: number;
  readonly payload_json: string;
}

interface FakeMessageRow {
  readonly message_id: string;
  readonly context_seq: number;
  readonly payload_json: string;
}

interface FakeSummaryRow {
  readonly summary_id: string;
  readonly created_seq: number;
  readonly payload_json: string;
}

interface FakeReducerCheckpointRow {
  readonly reducer_name: string;
  readonly schema_version: number;
  readonly through_seq: number;
  readonly payload_json: string;
  readonly updated_at_ms: number;
}

class FakeDurableObjectStorage implements EDASessionDurableObjectStorage {
  readonly sql = {
    exec: <Row = Record<string, unknown>>(query: string, ...bindings: ReadonlyArray<unknown>) =>
      this.exec<Row>(query, ...bindings),
  };
  alarm: number | null = null;
  private migrations = new Set<number>();
  private rows: Array<FakeEventRow> = [];
  private commands: Array<FakeCommandRow> = [];
  private commandInputs: Array<FakeCommandInputRow> = [];
  private messages: Array<FakeMessageRow> = [];
  private summaries: Array<FakeSummaryRow> = [];
  private reducerCheckpoints: Array<FakeReducerCheckpointRow> = [];
  private reducerCheckpointTableCreated = false;
  private migrationInserts = 0;

  get appliedMigrations(): ReadonlyArray<number> {
    return Array.from(this.migrations).sort((left, right) => left - right);
  }

  get createdReducerCheckpointTable(): boolean {
    return this.reducerCheckpointTableCreated;
  }

  get eventRows(): ReadonlyArray<{ readonly seq: number; readonly type: string }> {
    return this.rows.map((row) => ({
      seq: row.seq,
      type: row.type,
    }));
  }

  get migrationInsertCount(): number {
    return this.migrationInserts;
  }

  get summaryRows(): ReadonlyArray<FakeSummaryRow> {
    return this.summaries.map((row) => ({ ...row }));
  }

  get reducerCheckpointRows(): ReadonlyArray<FakeReducerCheckpointRow> {
    return this.reducerCheckpoints.map((row) => ({ ...row }));
  }

  getAlarm(): number | null {
    return this.alarm;
  }

  setAlarm(scheduledTimeMs: number): void {
    this.alarm = scheduledTimeMs;
  }

  deleteAlarm(): void {
    this.alarm = null;
  }

  transactionSync<A>(closure: () => A): A {
    const rows = this.rows.map((row) => ({ ...row }));
    const commands = this.commands.map((row) => ({ ...row }));
    const commandInputs = this.commandInputs.map((row) => ({ ...row }));
    const messages = this.messages.map((row) => ({ ...row }));
    const summaries = this.summaries.map((row) => ({ ...row }));
    const reducerCheckpoints = this.reducerCheckpoints.map((row) => ({ ...row }));
    const migrations = new Set(this.migrations);
    const reducerCheckpointTableCreated = this.reducerCheckpointTableCreated;
    const migrationInserts = this.migrationInserts;
    try {
      return closure();
    } catch (error) {
      this.rows = rows;
      this.commands = commands;
      this.commandInputs = commandInputs;
      this.messages = messages;
      this.summaries = summaries;
      this.reducerCheckpoints = reducerCheckpoints;
      this.migrations = migrations;
      this.reducerCheckpointTableCreated = reducerCheckpointTableCreated;
      this.migrationInserts = migrationInserts;
      throw error;
    }
  }

  private exec<Row = Record<string, unknown>>(
    query: string,
    ...bindings: ReadonlyArray<unknown>
  ): DurableObjectSqlCursor<Row> {
    const normalized = query.replace(/\s+/g, " ").trim().toUpperCase();

    if (normalized.startsWith("CREATE TABLE IF NOT EXISTS _EDA_REDUCER_CHECKPOINTS")) {
      this.reducerCheckpointTableCreated = true;
      return cursor<Row>([]);
    }

    if (
      normalized.startsWith("CREATE TABLE") ||
      normalized.startsWith("CREATE INDEX") ||
      normalized.startsWith("CREATE UNIQUE INDEX") ||
      normalized.startsWith("DROP TABLE") ||
      normalized.startsWith("DELETE FROM _EDA_SCHEMA_MIGRATIONS")
    ) {
      return cursor<Row>([]);
    }

    if (normalized.includes("SELECT COALESCE(MAX(ID), 0) AS VERSION")) {
      return cursor<Row>([{ version: Math.max(0, ...Array.from(this.migrations)) } as Row]);
    }

    if (normalized.includes("SELECT COALESCE(MAX(SEQ), 0) AS HEAD FROM _EDA_EVENT_LOG")) {
      return cursor<Row>([
        { head: this.rows.reduce((max, row) => Math.max(max, row.seq), 0) } as Row,
      ]);
    }

    if (normalized.startsWith("INSERT INTO _EDA_SCHEMA_MIGRATIONS")) {
      this.migrations.add(Number(bindings[0]));
      this.migrationInserts += 1;
      return cursor<Row>([]);
    }

    if (normalized.startsWith("INSERT INTO _EDA_EVENT_LOG")) {
      const [eventId, namespace, type, schemaVersion, createdAtMs, traceJson, payloadJson] =
        bindings;
      this.rows.push({
        event_id: String(eventId),
        namespace: String(namespace),
        type: String(type),
        schema_version: Number(schemaVersion),
        created_at_ms: Number(createdAtMs),
        trace_json: String(traceJson),
        fact_json: String(payloadJson),
        seq: nextSeq(this.rows),
      });
      return cursor<Row>([]);
    }

    if (
      normalized.includes(
        "SELECT SEQ, EVENT_ID, NAMESPACE, TYPE, SCHEMA_VERSION, CREATED_AT_MS, TRACE_JSON, FACT_JSON",
      ) &&
      normalized.includes("WHERE EVENT_ID = ?")
    ) {
      const [eventId] = bindings;
      return cursor<Row>(this.rows.filter((row) => row.event_id === eventId) as Array<Row>);
    }

    if (
      normalized.includes(
        "SELECT SEQ, EVENT_ID, NAMESPACE, TYPE, SCHEMA_VERSION, CREATED_AT_MS, TRACE_JSON, FACT_JSON",
      ) &&
      normalized.includes("WHERE SEQ >= ?") &&
      !normalized.includes(" OR ")
    ) {
      const [seq] = bindings;
      return cursor<Row>(
        this.rows
          .filter((row) => row.seq >= Number(seq))
          .sort((left, right) => left.seq - right.seq) as Array<Row>,
      );
    }

    if (
      normalized.includes(
        "SELECT SEQ, EVENT_ID, NAMESPACE, TYPE, SCHEMA_VERSION, CREATED_AT_MS, TRACE_JSON, FACT_JSON",
      ) &&
      normalized.includes("WHERE SEQ > ?")
    ) {
      const [afterSeq, throughSeq, limit] = bindings;
      const numericAfterSeq = Number(afterSeq);
      const numericThroughSeq = normalized.includes("SEQ <= ?") ? Number(throughSeq) : undefined;
      const numericLimit = normalized.includes("LIMIT ?")
        ? Number(numericThroughSeq === undefined ? throughSeq : limit)
        : undefined;
      return cursor<Row>(
        this.rows
          .filter(
            (row) =>
              row.seq > numericAfterSeq &&
              (numericThroughSeq === undefined || row.seq <= numericThroughSeq),
          )
          .sort((left, right) => left.seq - right.seq)
          .slice(0, numericLimit) as Array<Row>,
      );
    }

    if (
      normalized.includes(
        "SELECT SEQ, EVENT_ID, NAMESPACE, TYPE, SCHEMA_VERSION, CREATED_AT_MS, TRACE_JSON, FACT_JSON",
      ) &&
      normalized.includes("WHERE SEQ = ?")
    ) {
      const [seq] = bindings;
      return cursor<Row>(this.rows.filter((row) => row.seq === Number(seq)) as Array<Row>);
    }

    if (normalized.startsWith("INSERT INTO _EDA_COMMAND_STATE")) {
      const [commandId, admittedSeq, status] = bindings;
      const existing = this.commands.findIndex((row) => row.command_id === commandId);
      const next: FakeCommandRow = {
        command_id: String(commandId),
        admitted_seq: Number(admittedSeq),
        status: String(status),
        idempotency_key: bindings.length === 5 ? (bindings[3] as string | null) : null,
        payload_json: String(bindings.length === 5 ? bindings[4] : bindings[3]),
      };
      if (existing === -1) {
        this.commands.push(next);
      } else {
        this.commands[existing] = {
          ...this.commands[existing]!,
          status: next.status,
          payload_json: next.payload_json,
          ...(bindings.length === 5
            ? { admitted_seq: next.admitted_seq, idempotency_key: next.idempotency_key }
            : {}),
        };
      }
      return cursor<Row>([]);
    }

    if (normalized.includes("SELECT ADMITTED_SEQ FROM _EDA_COMMAND_STATE")) {
      if (normalized.includes("WHERE STATUS IN")) {
        return cursor<Row>(
          this.commands
            .filter((row) => row.status === "admitted" || row.status === "started")
            .sort((left, right) => left.admitted_seq - right.admitted_seq)
            .map(({ admitted_seq }) => ({ admitted_seq }) as Row),
        );
      }
      const [value] = bindings;
      if (normalized.includes("WHERE STATUS = ?")) {
        return cursor<Row>(
          this.commands
            .filter((row) => row.status === value)
            .sort((left, right) => left.admitted_seq - right.admitted_seq)
            .map(({ admitted_seq }) => ({ admitted_seq }) as Row),
        );
      }
      const rows = normalized.includes("WHERE IDEMPOTENCY_KEY = ?")
        ? this.commands.filter((row) => row.idempotency_key === value)
        : this.commands.filter((row) => row.command_id === value);
      return cursor<Row>(rows.map(({ admitted_seq }) => ({ admitted_seq }) as Row));
    }

    if (normalized.startsWith("INSERT INTO _EDA_COMMAND_INPUTS")) {
      const [commandId, admittedSeq, payloadJson] = bindings;
      const existing = this.commandInputs.findIndex((row) => row.command_id === commandId);
      const next = {
        command_id: String(commandId),
        admitted_seq: Number(admittedSeq),
        payload_json: String(payloadJson),
      } satisfies FakeCommandInputRow;
      if (existing === -1) {
        this.commandInputs.push(next);
      } else {
        this.commandInputs[existing] = next;
      }
      return cursor<Row>([]);
    }

    if (normalized.includes("SELECT PAYLOAD_JSON FROM _EDA_COMMAND_INPUTS")) {
      const [commandId] = bindings;
      return cursor<Row>(
        this.commandInputs
          .filter((row) => row.command_id === commandId)
          .map(({ payload_json }) => ({ payload_json }) as Row),
      );
    }

    if (normalized.startsWith("INSERT INTO _EDA_CONTEXT_MESSAGES")) {
      const [messageId, contextSeq, payloadJson] = bindings;
      const existing = this.messages.findIndex((row) => row.message_id === messageId);
      const next = {
        message_id: String(messageId),
        context_seq: Number(contextSeq),
        payload_json: String(payloadJson),
      } satisfies FakeMessageRow;
      if (existing === -1) {
        this.messages.push(next);
      } else {
        this.messages[existing] = next;
      }
      return cursor<Row>([]);
    }

    if (normalized.includes("SELECT CONTEXT_SEQ, PAYLOAD_JSON FROM _EDA_CONTEXT_MESSAGES")) {
      const [value] = bindings;
      const selected = normalized.includes("WHERE CONTEXT_SEQ = ?")
        ? this.messages.filter((row) => row.context_seq === Number(value))
        : this.messages.filter((row) => row.message_id === value);
      return cursor<Row>(
        selected.map(({ context_seq, payload_json }) => ({ context_seq, payload_json }) as Row),
      );
    }

    if (normalized.startsWith("INSERT INTO _EDA_CONTEXT_SUMMARIES")) {
      const [summaryId, createdSeq, payloadJson] = bindings;
      const existing = this.summaries.findIndex((row) => row.summary_id === summaryId);
      const next = {
        summary_id: String(summaryId),
        created_seq: Number(createdSeq),
        payload_json: String(payloadJson),
      } satisfies FakeSummaryRow;
      if (existing === -1) {
        this.summaries.push(next);
      } else {
        this.summaries[existing] = next;
      }
      return cursor<Row>([]);
    }

    if (normalized.includes("SELECT SUMMARY_ID, PAYLOAD_JSON FROM _EDA_CONTEXT_SUMMARIES")) {
      return cursor<Row>(
        this.summaries
          .slice()
          .sort((left, right) => right.created_seq - left.created_seq)
          .map(({ summary_id, payload_json }) => ({ summary_id, payload_json }) as Row),
      );
    }

    if (
      normalized.includes("SELECT PAYLOAD_JSON FROM _EDA_CONTEXT_SUMMARIES WHERE CREATED_SEQ = ?")
    ) {
      const [createdSeq] = bindings;
      return cursor<Row>(
        this.summaries
          .filter((row) => row.created_seq === Number(createdSeq))
          .map(({ payload_json }) => ({ payload_json }) as Row),
      );
    }

    if (
      normalized.includes("SELECT PAYLOAD_JSON FROM _EDA_CONTEXT_SUMMARIES WHERE SUMMARY_ID = ?")
    ) {
      const [summaryId] = bindings;
      return cursor<Row>(
        this.summaries
          .filter((row) => row.summary_id === summaryId)
          .map(({ payload_json }) => ({ payload_json }) as Row),
      );
    }

    if (normalized.startsWith("INSERT INTO _EDA_REDUCER_CHECKPOINTS")) {
      const [reducerName, schemaVersion, throughSeq, payloadJson, updatedAtMs] = bindings;
      const existing = this.reducerCheckpoints.findIndex((row) => row.reducer_name === reducerName);
      const next = {
        reducer_name: String(reducerName),
        schema_version: Number(schemaVersion),
        through_seq: Number(throughSeq),
        payload_json: String(payloadJson),
        updated_at_ms: Number(updatedAtMs),
      } satisfies FakeReducerCheckpointRow;
      if (existing === -1) {
        this.reducerCheckpoints.push(next);
      } else {
        this.reducerCheckpoints[existing] = next;
      }
      return cursor<Row>([]);
    }

    if (
      normalized.includes(
        "SELECT REDUCER_NAME, SCHEMA_VERSION, THROUGH_SEQ, PAYLOAD_JSON, UPDATED_AT_MS",
      )
    ) {
      const [reducerName] = bindings;
      return cursor<Row>(
        this.reducerCheckpoints.filter((row) => row.reducer_name === reducerName) as Array<Row>,
      );
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

const nextSeq = (rows: ReadonlyArray<FakeEventRow>): number =>
  rows.reduce((max, row) => Math.max(max, row.seq), 0) + 1;
