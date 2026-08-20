import * as Schema from "effect/Schema";
import { describe, expect, expectTypeOf, it } from "vitest";

import { edaFrameworkWebSocketWireProtocol, makeEDAWebSocketWireProtocol } from "./protocol";
import { DurableEventEnvelope } from "../types/events";

const CounterIncrementedEvent = Schema.Struct({
  ...DurableEventEnvelope.fields,
  namespace: Schema.Literal("example-counter"),
  type: Schema.Literal("CounterIncremented"),
  schemaVersion: Schema.Literal(1),
  payload: Schema.Struct({ amount: Schema.NumberFromString }),
});

const protocol = makeEDAWebSocketWireProtocol({
  appEvents: CounterIncrementedEvent,
});

const customEventWireFrame = {
  _tag: "events",
  durableThroughSeq: 1,
  events: [
    {
      event: {
        createdAtMs: 1_715_000_000_000,
        durability: "durable",
        eventId: "018f6bd5-2f2a-7b1e-8f1d-1f2e3d4c5b6a",
        namespace: "example-counter",
        payload: { amount: "2" },
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

const customEventFrame = Schema.decodeUnknownSync(protocol.domain.eventsFrame)(
  customEventWireFrame,
);

describe("makeEDAWebSocketWireProtocol", () => {
  it("rejects unregistered app events from the framework-only protocol", () => {
    expect(() =>
      edaFrameworkWebSocketWireProtocol.host.encodeServerFrame(customEventFrame),
    ).toThrow();
  });

  it("includes framework events without app-side union plumbing", () => {
    const event = Schema.decodeUnknownSync(protocol.wire.event)({
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

  it("round-trips transformed event payloads through the JSON wire representation", () => {
    const encoded = protocol.encodeServerFrame(customEventFrame);
    const wireFrame = Schema.decodeUnknownSync(Schema.fromJsonString(protocol.wire.serverFrame))(
      encoded,
    );
    const domainFrame = Schema.decodeUnknownSync(
      Schema.fromJsonString(protocol.domain.serverFrame),
    )(encoded);

    expect(wireFrame).toEqual(customEventWireFrame);
    expect(domainFrame).toEqual(customEventFrame);

    if (wireFrame._tag !== "events" || domainFrame._tag !== "events") {
      return;
    }
    const wireEvent = wireFrame.events[0].event;
    const domainEvent = domainFrame.events[0].event;
    if (wireEvent.type !== "CounterIncremented" || domainEvent.type !== "CounterIncremented") {
      return;
    }

    expectTypeOf(wireEvent.payload.amount).toEqualTypeOf<string>();
    expectTypeOf(domainEvent.payload.amount).toEqualTypeOf<number>();
    expect(wireEvent.payload.amount).toBe("2");
    expect(domainEvent.payload.amount).toBe(2);
  });

  it("validates app event payloads while encoding server frames", () => {
    expect(() =>
      protocol.host.encodeServerFrame({
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
