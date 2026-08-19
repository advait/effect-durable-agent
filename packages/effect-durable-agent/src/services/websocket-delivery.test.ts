import { describe, expect, it } from "vite-plus/test";

import {
  makeWebSocketDeliveryState,
  deliveryHelloFrame,
  onClientAck,
  onDurableEvents,
  onEphemeralEvent,
  type EDAWebSocketDeliveryState,
} from "./websocket-delivery";
import { StopTurnCommand } from "../types/commands";
import {
  CommandId,
  EventId,
  Position,
  SequenceNumber,
  SessionId,
  durablePosition,
} from "../types/core";
import {
  CommandAdmittedEvent,
  PositionedEvent,
  TextDeltaEvent,
  UnixEpochMillis,
  commandAdmittedEventType,
  effectDurableAgentNamespace,
  schemaV1,
  textDeltaEventType,
  ProviderPartId,
} from "../types/events";
import {
  EDA_WS_CLOSE_LAGGED,
  EDA_WS_CLOSE_PROTOCOL_ERROR,
  EDAWebSocketAckFrame,
  FrameId,
  SubscriberId,
  defaultEDAWebSocketFlowControl,
} from "../host/websocket-protocol";

const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const SUBSCRIBER_ID = SubscriberId.make("subscriber-test");
const NOW_MS = 1_715_000_000_000;

const eventId = (index: number) =>
  EventId.make(`018f6bd5-2f2a-7b1e-${(0x9000 + index).toString(16)}-1f2e3d4c5b6a`);

const durableEventAt = (seq: number) =>
  PositionedEvent.make({
    position: durablePosition(SequenceNumber.make(seq)),
    event: CommandAdmittedEvent.make({
      namespace: effectDurableAgentNamespace,
      type: commandAdmittedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: eventId(seq),
      sessionId: SESSION_ID,
      createdAtMs: UnixEpochMillis.make(NOW_MS + seq),
      payload: {
        command: new StopTurnCommand({ commandId: COMMAND_ID }),
      },
    }),
  });

const ephemeralEventAt = (seq: number, subSeq: number) =>
  PositionedEvent.make({
    position: Position.make({ seq: SequenceNumber.make(seq), subSeq }),
    event: TextDeltaEvent.make({
      namespace: effectDurableAgentNamespace,
      type: textDeltaEventType,
      schemaVersion: schemaV1,
      durability: "ephemeral",
      eventId: eventId(1000 + subSeq),
      sessionId: SESSION_ID,
      createdAtMs: UnixEpochMillis.make(NOW_MS + subSeq),
      payload: { providerPartId: ProviderPartId.make("text-1"), delta: `d${subSeq}` },
    }),
  });

const ackFrame = (frameId: number, durableThroughSeq: number) =>
  EDAWebSocketAckFrame.make({
    _tag: "ack",
    frameId: FrameId.make(frameId),
    durableThroughSeq: SequenceNumber.make(durableThroughSeq),
  });

const freshState = (overrides?: Partial<Parameters<typeof makeWebSocketDeliveryState>[0]>) =>
  makeWebSocketDeliveryState({
    subscriberId: SUBSCRIBER_ID,
    resumeSeq: SequenceNumber.make(0),
    policy: defaultEDAWebSocketFlowControl,
    ...overrides,
  });

const seq = (value: number) => SequenceNumber.make(value);

describe("websocket-delivery", () => {
  it("announces the ping cadence and resume cursor in the hello frame", () => {
    const hello = deliveryHelloFrame(freshState({ resumeSeq: seq(7) }));
    expect(hello.resumeSeq).toBe(7);
    expect(hello.flowControl.pingIntervalMs).toBe(30_000);
  });

  it("delivers a durable slice one frame per event and requests more when behind", () => {
    const state = freshState();
    const result = onDurableEvents(state, [durableEventAt(1), durableEventAt(2)], seq(5), NOW_MS);

    expect(result.frames).toHaveLength(2);
    expect(result.state.sentDurableThroughSeq).toBe(2);
    expect(result.state.inFlight).toHaveLength(2);
    expect(result.wantsCatchUpAfterSeq).toBe(2);
    expect(result.close).toBeUndefined();
  });

  it("skips already-sent durable events and stops requesting once at head", () => {
    const first = onDurableEvents(freshState(), [durableEventAt(1)], seq(1), NOW_MS);
    const second = onDurableEvents(first.state, [durableEventAt(1)], seq(1), NOW_MS);

    expect(second.frames).toHaveLength(0);
    expect(second.wantsCatchUpAfterSeq).toBeUndefined();
  });

  it("stops emitting durable frames when the ACK window is full", () => {
    const events = Array.from({ length: 20 }, (_, index) => durableEventAt(index + 1));
    const result = onDurableEvents(freshState(), events, seq(20), NOW_MS);

    expect(result.frames).toHaveLength(defaultEDAWebSocketFlowControl.maxInFlightFrames);
    expect(result.state.sentDurableThroughSeq).toBe(
      defaultEDAWebSocketFlowControl.maxInFlightFrames,
    );
    // Window is full: catch-up resumes when an ACK reopens it.
    expect(result.wantsCatchUpAfterSeq).toBeUndefined();
  });

  it("sends ephemeral events immediately while the window is open", () => {
    const result = onEphemeralEvent(freshState(), ephemeralEventAt(0, 1), NOW_MS);

    expect(result.frames).toHaveLength(1);
    expect(result.state.inFlight).toHaveLength(1);
    expect(result.state.sentDurableThroughSeq).toBe(0);
  });

  it("buffers ephemeral events while the window is full and lag-closes on overflow", () => {
    const events = Array.from({ length: 16 }, (_, index) => durableEventAt(index + 1));
    let state = onDurableEvents(freshState(), events, seq(16), NOW_MS).state;

    for (
      let index = 0;
      index < defaultEDAWebSocketFlowControl.subscriberBufferCapacityEvents;
      index += 1
    ) {
      const result = onEphemeralEvent(state, ephemeralEventAt(16, index + 1), NOW_MS);
      expect(result.close).toBeUndefined();
      expect(result.frames).toHaveLength(0);
      state = result.state;
    }

    const overflow = onEphemeralEvent(state, ephemeralEventAt(16, 9_999), NOW_MS);
    expect(overflow.close?.code).toBe(EDA_WS_CLOSE_LAGGED);
    expect(overflow.frames.at(-1)).toMatchObject({ _tag: "lagged", reason: "buffer-overflow" });
  });

  it("advances the cursor, persists it, and drains buffered ephemeral events on ACK", () => {
    const events = Array.from({ length: 16 }, (_, index) => durableEventAt(index + 1));
    const filled = onDurableEvents(freshState(), events, seq(16), NOW_MS);
    const buffered = onEphemeralEvent(filled.state, ephemeralEventAt(16, 1), NOW_MS);
    expect(buffered.frames).toHaveLength(0);

    const acked = onClientAck(buffered.state, ackFrame(2, 2), NOW_MS + 5);

    expect(acked.persistSeq).toBe(2);
    expect(acked.state.lastAckedSeq).toBe(2);
    // Two window slots reopened: the buffered ephemeral event flushes.
    expect(acked.frames).toHaveLength(1);
    expect(acked.state.pendingEphemeral).toHaveLength(0);
  });

  it("requests durable catch-up when an ACK reopens an idle window", () => {
    const sent = onDurableEvents(freshState(), [durableEventAt(1)], seq(3), NOW_MS);
    const acked = onClientAck(sent.state, ackFrame(1, 1), NOW_MS + 5);

    expect(acked.wantsCatchUpAfterSeq).toBe(1);
  });

  it.each([
    ["duplicate ACK advancing durable seq", ackFrame(1, 1), ackFrame(1, 2)],
    ["ACK for an unsent frame", ackFrame(1, 1), ackFrame(9, 1)],
  ])("protocol-closes on %s", (_name, firstAck, badAck) => {
    const events = Array.from({ length: 4 }, (_, index) => durableEventAt(index + 1));
    const sent = onDurableEvents(freshState(), events, seq(4), NOW_MS);
    const okAck = onClientAck(sent.state, firstAck, NOW_MS + 1);
    expect(okAck.close).toBeUndefined();

    const bad = onClientAck(okAck.state, badAck, NOW_MS + 2);
    expect(bad.close?.code).toBe(EDA_WS_CLOSE_PROTOCOL_ERROR);
    expect(bad.frames.at(-1)).toMatchObject({ _tag: "error" });
  });

  it("protocol-closes when an ACK moves the durable cursor backwards", () => {
    const events = Array.from({ length: 4 }, (_, index) => durableEventAt(index + 1));
    const sent = onDurableEvents(freshState(), events, seq(4), NOW_MS);
    const okAck = onClientAck(sent.state, ackFrame(3, 3), NOW_MS + 1);

    const bad = onClientAck(okAck.state, ackFrame(4, 2), NOW_MS + 2);
    expect(bad.close?.code).toBe(EDA_WS_CLOSE_PROTOCOL_ERROR);
  });

  it("lag-closes lazily when an in-flight frame exceeds the ACK deadline", () => {
    const sent = onDurableEvents(freshState(), [durableEventAt(1)], seq(1), NOW_MS);
    const late = NOW_MS + defaultEDAWebSocketFlowControl.ackTimeoutMs + 1;

    const result = onEphemeralEvent(sent.state, ephemeralEventAt(1, 1), late);
    expect(result.close?.code).toBe(EDA_WS_CLOSE_LAGGED);
    expect(result.frames.at(-1)).toMatchObject({ _tag: "lagged", reason: "ack-timeout" });
  });

  it("never times out an idle socket with no in-flight frames", () => {
    const state = freshState();
    const muchLater = NOW_MS + 100 * defaultEDAWebSocketFlowControl.ackTimeoutMs;

    const result = onDurableEvents(state, [], seq(0), muchLater);
    expect(result.close).toBeUndefined();
    expect(result.frames).toHaveLength(0);
  });

  describe("cold-restored state after isolate eviction", () => {
    const coldState = (): EDAWebSocketDeliveryState =>
      freshState({ resumeSeq: seq(10), coldRestored: true });

    it("tolerates ACKs for unknown pre-eviction frame ids", () => {
      const result = onClientAck(coldState(), ackFrame(500, 12), NOW_MS);

      expect(result.close).toBeUndefined();
      expect(result.persistSeq).toBe(12);
      expect(result.state.lastAckedSeq).toBe(12);
      // The client proved delivery through seq 12: never re-send those events.
      expect(result.state.sentDurableThroughSeq).toBe(12);
    });

    it("ignores stale pre-eviction ACKs instead of protocol-closing", () => {
      const result = onClientAck(coldState(), ackFrame(3, 4), NOW_MS);

      expect(result.close).toBeUndefined();
      expect(result.persistSeq).toBeUndefined();
      expect(result.state.lastAckedSeq).toBe(10);
    });

    it("resumes durable delivery from the persisted cursor", () => {
      const result = onDurableEvents(
        coldState(),
        [durableEventAt(11), durableEventAt(12)],
        seq(12),
        NOW_MS,
      );

      expect(result.frames).toHaveLength(2);
      expect(result.state.sentDurableThroughSeq).toBe(12);
    });
  });
});
