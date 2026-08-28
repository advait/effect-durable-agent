import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

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

describe("EDAWebSocketConnectionManager", () => {
  it("coalesces catch-up reads reopened by a batch of suppressed frames", async () => {
    const events = Array.from({ length: 16 }, (_, index) => durableEventAt(index + 1));
    let readCalls = 0;
    const manager = new EDAWebSocketConnectionManager({
      isSessionReady: () => true,
      prepareSession: async () => undefined,
      readEventPage: async () => {
        readCalls += 1;
        return readCalls === 1
          ? { events, head: SequenceNumber.make(events.length) }
          : { events: [], head: SequenceNumber.make(events.length) };
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

    expect(readCalls).toBe(2);
    expect(webSocket.sentCount).toBe(1);
    expect(webSocket.closeCode).toBeUndefined();
  });
});

class TestWebSocket {
  private attachment: unknown;
  closeCode: number | undefined;
  sentCount = 0;
  readonly readyState = 1;

  asWebSocket(): WebSocket {
    return this as unknown as WebSocket;
  }

  send(): void {
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
