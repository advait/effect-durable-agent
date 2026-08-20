import { describe, expect, it } from "vite-plus/test";

import {
  makeWebSocketDeliveryState,
  checkpointWebSocketDeliveryState,
  deliveryHelloFrame,
  onClientAck,
  onDurableEvents,
  onEphemeralEvent,
  restoreWebSocketDeliveryState,
  type EDAWebSocketDeliveryState,
} from "./delivery";
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
} from "./messages";

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
const sentFrames = (result: ReturnType<typeof onDurableEvents>) =>
  result.actions.flatMap((action) => (action._tag === "Send" ? action.frames : []));
const closeAction = (result: ReturnType<typeof onDurableEvents>) =>
  result.actions.find((action) => action._tag === "Close");
const catchUpAction = (result: ReturnType<typeof onDurableEvents>) =>
  result.actions.find((action) => action._tag === "ReadEventPage");
const persists = (result: ReturnType<typeof onDurableEvents>) =>
  result.actions.some((action) => action._tag === "Persist");

describe("websocket-delivery", () => {
  it("announces the ping cadence and resume cursor in the hello frame", () => {
    const hello = deliveryHelloFrame(freshState({ resumeSeq: seq(7) }));
    expect(hello.resumeSeq).toBe(7);
    expect(hello.flowControl.pingIntervalMs).toBe(30_000);
  });

  it("delivers a durable slice one frame per event and requests more when behind", () => {
    const state = freshState();
    const result = onDurableEvents(state, [durableEventAt(1), durableEventAt(2)], seq(5), NOW_MS);

    expect(sentFrames(result)).toHaveLength(2);
    expect(result.state.sentDurableThroughSeq).toBe(2);
    expect(result.state.inFlight).toHaveLength(2);
    expect(catchUpAction(result)).toMatchObject({ afterSeq: 2 });
    expect(closeAction(result)).toBeUndefined();
    expect(result.actions.slice(0, 2).map((action) => action._tag)).toEqual(["Persist", "Send"]);
  });

  it("skips already-sent durable events and stops requesting once at head", () => {
    const first = onDurableEvents(freshState(), [durableEventAt(1)], seq(1), NOW_MS);
    const second = onDurableEvents(first.state, [durableEventAt(1)], seq(1), NOW_MS);

    expect(sentFrames(second)).toHaveLength(0);
    expect(catchUpAction(second)).toBeUndefined();
  });

  it("stops emitting durable frames when the ACK window is full", () => {
    const events = Array.from({ length: 20 }, (_, index) => durableEventAt(index + 1));
    const result = onDurableEvents(freshState(), events, seq(20), NOW_MS);

    expect(sentFrames(result)).toHaveLength(defaultEDAWebSocketFlowControl.maxInFlightFrames);
    expect(result.state.sentDurableThroughSeq).toBe(
      defaultEDAWebSocketFlowControl.maxInFlightFrames,
    );
    // Window is full: catch-up resumes when an ACK reopens it.
    expect(catchUpAction(result)).toBeUndefined();
  });

  it("sends ephemeral events immediately while the window is open", () => {
    const result = onEphemeralEvent(freshState(), ephemeralEventAt(0, 1), NOW_MS);

    expect(sentFrames(result)).toHaveLength(1);
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
      expect(closeAction(result)).toBeUndefined();
      expect(sentFrames(result)).toHaveLength(0);
      state = result.state;
    }

    const overflow = onEphemeralEvent(state, ephemeralEventAt(16, 9_999), NOW_MS);
    expect(closeAction(overflow)).toMatchObject({ code: EDA_WS_CLOSE_LAGGED });
    expect(sentFrames(overflow).at(-1)).toMatchObject({
      _tag: "lagged",
      reason: "buffer-overflow",
    });
  });

  it("advances the cursor, persists it, and drains buffered ephemeral events on ACK", () => {
    const events = Array.from({ length: 16 }, (_, index) => durableEventAt(index + 1));
    const filled = onDurableEvents(freshState(), events, seq(16), NOW_MS);
    const buffered = onEphemeralEvent(filled.state, ephemeralEventAt(16, 1), NOW_MS);
    expect(sentFrames(buffered)).toHaveLength(0);

    const acked = onClientAck(buffered.state, ackFrame(2, 2), NOW_MS + 5);

    expect(persists(acked)).toBe(true);
    expect(acked.state.lastAckedSeq).toBe(2);
    // Two window slots reopened: the buffered ephemeral event flushes.
    expect(sentFrames(acked)).toHaveLength(1);
    expect(acked.state.pendingEphemeral).toHaveLength(0);
  });

  it("requests durable catch-up when an ACK reopens an idle window", () => {
    const sent = onDurableEvents(freshState(), [durableEventAt(1)], seq(3), NOW_MS);
    const acked = onClientAck(sent.state, ackFrame(1, 1), NOW_MS + 5);

    expect(catchUpAction(acked)).toMatchObject({ afterSeq: 1 });
  });

  it.each([
    ["duplicate ACK advancing durable seq", ackFrame(1, 1), ackFrame(1, 2)],
    ["ACK for an unsent frame", ackFrame(1, 1), ackFrame(9, 1)],
  ])("protocol-closes on %s", (_name, firstAck, badAck) => {
    const events = Array.from({ length: 4 }, (_, index) => durableEventAt(index + 1));
    const sent = onDurableEvents(freshState(), events, seq(4), NOW_MS);
    const okAck = onClientAck(sent.state, firstAck, NOW_MS + 1);
    expect(closeAction(okAck)).toBeUndefined();

    const bad = onClientAck(okAck.state, badAck, NOW_MS + 2);
    expect(closeAction(bad)).toMatchObject({ code: EDA_WS_CLOSE_PROTOCOL_ERROR });
    expect(sentFrames(bad).at(-1)).toMatchObject({ _tag: "error" });
  });

  it("protocol-closes when an ACK moves the durable cursor backwards", () => {
    const events = Array.from({ length: 4 }, (_, index) => durableEventAt(index + 1));
    const sent = onDurableEvents(freshState(), events, seq(4), NOW_MS);
    const okAck = onClientAck(sent.state, ackFrame(3, 3), NOW_MS + 1);

    const bad = onClientAck(okAck.state, ackFrame(4, 2), NOW_MS + 2);
    expect(closeAction(bad)).toMatchObject({ code: EDA_WS_CLOSE_PROTOCOL_ERROR });
  });

  it("lag-closes lazily when an in-flight frame exceeds the ACK deadline", () => {
    const sent = onDurableEvents(freshState(), [durableEventAt(1)], seq(1), NOW_MS);
    const late = NOW_MS + defaultEDAWebSocketFlowControl.ackTimeoutMs + 1;

    const result = onEphemeralEvent(sent.state, ephemeralEventAt(1, 1), late);
    expect(closeAction(result)).toMatchObject({ code: EDA_WS_CLOSE_LAGGED });
    expect(sentFrames(result).at(-1)).toMatchObject({ _tag: "lagged", reason: "ack-timeout" });
  });

  it("never times out an idle socket with no in-flight frames", () => {
    const state = freshState();
    const muchLater = NOW_MS + 100 * defaultEDAWebSocketFlowControl.ackTimeoutMs;

    const result = onDurableEvents(state, [], seq(0), muchLater);
    expect(closeAction(result)).toBeUndefined();
    expect(sentFrames(result)).toHaveLength(0);
  });

  describe("restored state after isolate eviction", () => {
    const restoredState = (): EDAWebSocketDeliveryState => {
      const sent = onDurableEvents(
        freshState({ resumeSeq: seq(10) }),
        [durableEventAt(11), durableEventAt(12)],
        seq(12),
        NOW_MS,
      );
      return restoreWebSocketDeliveryState({
        subscriberId: SUBSCRIBER_ID,
        policy: defaultEDAWebSocketFlowControl,
        checkpoint: checkpointWebSocketDeliveryState(sent.state),
      });
    };

    it("strictly validates and applies an ACK using persisted frame receipts", () => {
      const result = onClientAck(restoredState(), ackFrame(2, 12), NOW_MS + 1);

      expect(closeAction(result)).toBeUndefined();
      expect(result.state.lastAckedSeq).toBe(12);
      expect(result.state.inFlight).toHaveLength(0);
    });

    it("protocol-closes an unknown post-hibernation frame id", () => {
      const result = onClientAck(restoredState(), ackFrame(500, 12), NOW_MS + 1);

      expect(closeAction(result)).toMatchObject({ code: EDA_WS_CLOSE_PROTOCOL_ERROR });
    });

    it("resumes unsent durable delivery after the persisted sent cursor", () => {
      const result = onDurableEvents(
        restoredState(),
        [durableEventAt(11), durableEventAt(12), durableEventAt(13)],
        seq(13),
        NOW_MS + 1,
      );

      expect(sentFrames(result)).toHaveLength(1);
      expect(result.state.sentDurableThroughSeq).toBe(13);
    });
  });
});
