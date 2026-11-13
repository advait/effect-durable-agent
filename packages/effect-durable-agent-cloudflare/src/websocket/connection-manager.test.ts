import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { initialReducedState, type ReducedState } from "effect-durable-agent/domain/reduced-state";
import { SessionEventObserver } from "effect-durable-agent/services/session-event-observer";
import { StopTurnCommand } from "effect-durable-agent/types/commands";
import {
  CommandId,
  EventId,
  SequenceNumber,
  SessionId,
  durablePosition,
} from "effect-durable-agent/types/core";
import {
  CommandAdmittedEvent,
  PositionedEvent,
  UnixEpochMillis,
  commandAdmittedEventType,
  effectDurableAgentNamespace,
  schemaV1,
} from "effect-durable-agent/types/events";
import { makeRootEDATraceMetadata } from "effect-durable-agent/types/tracing";
import { EDAWebSocketClientFrame } from "effect-durable-agent/websocket";
import { EDAWebSocketConnectionManager } from "./connection-manager";
import { EDAWebSocketProjectionId, type EDAWebSocketProjection } from "./projection";

const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const TRACE = makeRootEDATraceMetadata();

const durableEventAt = (seq: number) =>
  PositionedEvent.make({
    position: durablePosition(SequenceNumber.make(seq)),
    event: CommandAdmittedEvent.make({
      namespace: effectDurableAgentNamespace,
      type: commandAdmittedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(`018f6bd5-2f2a-7b1e-${(0x9000 + seq).toString(16)}-1f2e3d4c5b6a`),
      sessionId: SESSION_ID,
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000 + seq),
      payload: { command: new StopTurnCommand({ commandId: COMMAND_ID }) },
    }),
  });

const ProjectionState = Schema.Struct({ suppressed: Schema.Number });
type ProjectionState = typeof ProjectionState.Type;

const fullySuppressingProjection: EDAWebSocketProjection<ProjectionState> = {
  id: EDAWebSocketProjectionId.make("test-fully-suppressing-projection-v1"),
  decodeClientMessage: () => Effect.fail("unused"),
  decodeState: Schema.decodeUnknownEffect(ProjectionState),
  encodeState: Schema.encodeUnknownSync(ProjectionState),
  encodeServerFrame: (frame, state) =>
    frame._tag === "events"
      ? { _tag: "SuppressAndAck", state: { suppressed: state.suppressed + 1 } }
      : { _tag: "Send", frame: JSON.stringify(frame), state },
  initialize: ({ requestedAfterSeq, snapshot }) => ({
    afterSeq: requestedAfterSeq ?? snapshot.state.lastSeq,
    state: { suppressed: 0 },
  }),
};

const reducedAt = (seq: number): ReducedState => ({
  ...initialReducedState,
  lastSeq: SequenceNumber.make(seq),
});

/** A page of a log whose head is `head`, read with the state at that head. */
const pageAfter = (afterSeq: number, head: number) => ({
  events: Array.from({ length: Math.max(0, head - afterSeq) }, (_, index) =>
    durableEventAt(afterSeq + index + 1),
  ),
  head: SequenceNumber.make(head),
  reduced: reducedAt(head),
});

interface RecordedFrame {
  readonly seqs: ReadonlyArray<number>;
  readonly reducedSeq: number | undefined;
}

/** Sends every frame as JSON and records each events frame's seqs and the state it was given. */
const recordingProjection = (
  recorded: Array<RecordedFrame>,
): EDAWebSocketProjection<ProjectionState> => ({
  id: EDAWebSocketProjectionId.make("test-recording-projection-v1"),
  decodeClientMessage: (message) =>
    Effect.try(() => JSON.parse(message) as unknown).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(EDAWebSocketClientFrame)),
    ),
  decodeState: Schema.decodeUnknownEffect(ProjectionState),
  encodeState: Schema.encodeUnknownSync(ProjectionState),
  encodeServerFrame: (frame, state, context) => {
    if (frame._tag === "events") {
      recorded.push({
        seqs: frame.events.map((event) => event.position.seq),
        reducedSeq: context.reduced?.lastSeq,
      });
    }
    return { _tag: "Send", frame: JSON.stringify(frame), state };
  },
  initialize: ({ requestedAfterSeq, snapshot }) => ({
    afterSeq: requestedAfterSeq ?? snapshot.state.lastSeq,
    state: { suppressed: 0 },
  }),
});

const publishTo = (
  manager: EDAWebSocketConnectionManager<ProjectionState>,
  seq: number,
  reducedSeq: number,
) =>
  Effect.runPromise(
    Effect.flatMap(Effect.service(SessionEventObserver), (observer) =>
      observer.onEvent(durableEventAt(seq), reducedAt(reducedSeq)),
    ).pipe(Effect.provide(manager.observerLayer)),
  );

const acceptAt = (
  manager: EDAWebSocketConnectionManager<ProjectionState>,
  webSocket: TestWebSocket,
  afterSeq: number,
) =>
  manager.accept({
    afterSeq: SequenceNumber.make(afterSeq),
    projectionState: { suppressed: 0 },
    sessionId: SESSION_ID,
    trace: TRACE,
    webSocket: webSocket.asWebSocket(),
  });

describe("EDAWebSocketConnectionManager projection context", () => {
  it("gives a live frame the state its publication carried, ahead of the frame inside a batch", async () => {
    const recorded: RecordedFrame[] = [];
    const manager = new EDAWebSocketConnectionManager({
      isSessionReady: () => true,
      prepareSession: async () => undefined,
      readEventPage: async ({ afterSeq }) => pageAfter(afterSeq, 0),
      webSocketProjection: recordingProjection(recorded),
    });
    const webSocket = new TestWebSocket();
    await acceptAt(manager, webSocket, 0);

    await publishTo(manager, 1, 1);
    await publishTo(manager, 2, 3);
    await publishTo(manager, 3, 3);

    expect(recorded).toEqual([
      { seqs: [1], reducedSeq: 1 },
      { seqs: [2], reducedSeq: 3 },
      { seqs: [3], reducedSeq: 3 },
    ]);
  });

  it("gives a catch-up frame the state read after its page", async () => {
    const recorded: RecordedFrame[] = [];
    const manager = new EDAWebSocketConnectionManager({
      isSessionReady: () => true,
      prepareSession: async () => undefined,
      readEventPage: async ({ afterSeq }) => ({
        ...pageAfter(afterSeq, 3),
        reduced: reducedAt(5),
      }),
      webSocketProjection: recordingProjection(recorded),
    });
    await acceptAt(manager, new TestWebSocket(), 0);

    expect(recorded).toEqual([
      { seqs: [1], reducedSeq: 5 },
      { seqs: [2], reducedSeq: 5 },
      { seqs: [3], reducedSeq: 5 },
    ]);
  });

  it("gives a socket restored from its attachment the state of the page its ACK reads", async () => {
    const webSocket = new TestWebSocket();
    const before = new EDAWebSocketConnectionManager({
      isSessionReady: () => true,
      prepareSession: async () => undefined,
      readEventPage: async ({ afterSeq }) => pageAfter(afterSeq, 2),
      webSocketProjection: recordingProjection([]),
    });
    await acceptAt(before, webSocket, 0);
    const sent = JSON.parse(webSocket.sent.at(-1) ?? "") as {
      readonly durableThroughSeq: number;
      readonly frameId: string;
    };

    const recorded: RecordedFrame[] = [];
    const afterWake = new EDAWebSocketConnectionManager({
      getWebSockets: () => [webSocket.asWebSocket()],
      isSessionReady: () => true,
      prepareSession: async () => undefined,
      readEventPage: async ({ afterSeq }) => pageAfter(afterSeq, 3),
      webSocketProjection: recordingProjection(recorded),
    });
    await afterWake.message(
      webSocket.asWebSocket(),
      JSON.stringify({
        _tag: "ack",
        durableThroughSeq: sent.durableThroughSeq,
        frameId: sent.frameId,
      }),
    );

    expect(webSocket.closeCode).toBeUndefined();
    expect(recorded).toEqual([{ seqs: [3], reducedSeq: 3 }]);
  });

  it("forgets the destroyed log's state, so the new log's frames meet their own state", async () => {
    const recorded: RecordedFrame[] = [];
    let head = 5;
    const manager = new EDAWebSocketConnectionManager({
      isSessionReady: () => true,
      prepareSession: async () => undefined,
      readEventPage: async ({ afterSeq }) => pageAfter(afterSeq, head),
      webSocketProjection: recordingProjection(recorded),
    });
    await acceptAt(manager, new TestWebSocket(), 5);

    await manager.sessionDestroyed();
    head = 0;
    await acceptAt(manager, new TestWebSocket(), 0);
    head = 1;
    await publishTo(manager, 1, 1);

    expect(recorded).toEqual([{ seqs: [1], reducedSeq: 1 }]);
  });

  it("takes state from a publication while the session is not ready to read", async () => {
    const recorded: RecordedFrame[] = [];
    let ready = true;
    let head = 1;
    const manager = new EDAWebSocketConnectionManager({
      isSessionReady: () => ready,
      prepareSession: async () => undefined,
      readEventPage: async ({ afterSeq }) => {
        if (!ready) throw new Error("the runtime is recovering and cannot serve a read");
        return pageAfter(afterSeq, head);
      },
      webSocketProjection: recordingProjection(recorded),
    });
    const webSocket = new TestWebSocket();
    await acceptAt(manager, webSocket, 0);

    ready = false;
    head = 2;
    await publishTo(manager, 2, 2);
    head = 4;
    await publishTo(manager, 4, 4);
    ready = true;
    await manager.flushDeferredCatchUp(SESSION_ID);

    expect(webSocket.closeCode).toBeUndefined();
    expect(recorded).toEqual([
      { seqs: [1], reducedSeq: 1 },
      { seqs: [2], reducedSeq: 2 },
      { seqs: [3], reducedSeq: 4 },
      { seqs: [4], reducedSeq: 4 },
    ]);
  });
});

describe("EDAWebSocketConnectionManager", () => {
  it("coalesces catch-up reads reopened by a batch of suppressed frames", async () => {
    const events = Array.from({ length: 16 }, (_, index) => durableEventAt(index + 1));
    const readCursors: number[] = [];
    const manager = new EDAWebSocketConnectionManager({
      isSessionReady: () => true,
      prepareSession: async () => undefined,
      readEventPage: async ({ afterSeq }) => {
        readCursors.push(afterSeq);
        return readCursors.length === 1
          ? { events, head: SequenceNumber.make(events.length), reduced: reducedAt(events.length) }
          : {
              events: [],
              head: SequenceNumber.make(events.length),
              reduced: reducedAt(events.length),
            };
      },
      webSocketProjection: fullySuppressingProjection,
    });
    const webSocket = new TestWebSocket();

    await manager.accept({
      afterSeq: SequenceNumber.make(0),
      projectionState: { suppressed: 0 },
      sessionId: SESSION_ID,
      trace: TRACE,
      webSocket: webSocket.asWebSocket(),
    });

    expect(readCursors).toEqual([0, 16]);
    expect(webSocket.sentCount).toBe(1);
    expect(webSocket.closeCode).toBeUndefined();
  });
});

class TestWebSocket {
  private attachment: unknown;
  closeCode: number | undefined;
  readonly sent: string[] = [];
  sentCount = 0;
  readonly readyState = 1;

  asWebSocket(): WebSocket {
    return this as unknown as WebSocket;
  }

  send(message: string): void {
    this.sent.push(message);
    this.sentCount += 1;
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
}
