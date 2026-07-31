import * as Schema from "effect/Schema";
import { describe, expect, expectTypeOf, it } from "vitest";

import { edaFrameworkWebSocketWireProtocol, makeEDAWebSocketWireProtocol } from "./websocket-wire";
import { DurableEventEnvelope } from "../types/events";

const CounterIncrementedEvent = Schema.Struct({
  ...DurableEventEnvelope.fields,
  namespace: Schema.Literal("example-counter"),
  type: Schema.Literal("CounterIncremented"),
  schemaVersion: Schema.Literal(1),
  payload: Schema.Struct({ amount: Schema.Number }),
});

const protocol = makeEDAWebSocketWireProtocol({
  appEvents: CounterIncrementedEvent,
});

const customEventFrame = {
  _tag: "events",
  durableThroughSeq: 1,
  events: [
    {
      event: {
        createdAtMs: 1_715_000_000_000,
        durability: "durable",
        eventId: "018f6bd5-2f2a-7b1e-8f1d-1f2e3d4c5b6a",
        namespace: "example-counter",
        payload: { amount: 2 },
        schemaVersion: 1,
        sessionId: "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a",
        trace: {
          links: [],
          span: {
            sampled: false,
            spanId: "0123456789abcdef",
            traceId: "0123456789abcdef0123456789abcdef",
            tracestate: null,
          },
        },
        type: "CounterIncremented",
      },
      position: { seq: 1, subSeq: 0 },
    },
  ],
  frameId: 1,
} as const;

describe("makeEDAWebSocketWireProtocol", () => {
  it("rejects unregistered app events from the framework-only protocol", () => {
    expect(() => edaFrameworkWebSocketWireProtocol.encodeServerFrame(customEventFrame)).toThrow();
  });

  it("includes framework events without app-side union plumbing", () => {
    const event = Schema.decodeUnknownSync(protocol.event)({
      createdAtMs: 1_715_000_000_001,
      durability: "ephemeral",
      eventId: "018f6bd5-2f2a-7b1e-8f1e-1f2e3d4c5b6a",
      namespace: "effect-durable-agent",
      payload: { delta: "hello", providerPartId: "provider-part-1" },
      schemaVersion: 1,
      sessionId: "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a",
      trace: {
        links: [],
        span: {
          sampled: false,
          spanId: "0123456789abcdef",
          traceId: "0123456789abcdef0123456789abcdef",
          tracestate: null,
        },
      },
      type: "TextDelta",
    });

    if (event.type !== "TextDelta") {
      return;
    }
    expectTypeOf(event.payload.delta).toEqualTypeOf<string>();
    expect(event.payload.delta).toBe("hello");
  });

  it("preserves an app event's payload discrimination for wire consumers", () => {
    const frame = Schema.decodeUnknownSync(protocol.serverFrame)(customEventFrame);
    expect(frame._tag).toBe("events");
    if (frame._tag !== "events") {
      return;
    }

    const event = frame.events[0].event;
    if (event.type !== "CounterIncremented") {
      return;
    }

    expectTypeOf(event.payload.amount).toEqualTypeOf<number>();
    expect(event.payload.amount).toBe(2);
  });

  it("validates app event payloads while encoding server frames", () => {
    expect(() =>
      protocol.encodeServerFrame({
        ...customEventFrame,
        events: [
          {
            ...customEventFrame.events[0],
            event: {
              ...customEventFrame.events[0].event,
              payload: { amount: "two" },
            },
          },
        ],
      }),
    ).toThrow();
  });
});
