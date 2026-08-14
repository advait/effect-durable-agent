import * as Schema from "effect/Schema";

import { makeEDAWebSocketWireProtocol } from "./websocket-wire";
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

const wireFrame = Schema.decodeUnknownSync(protocol.wire.serverFrame)({
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
});
const domainFrame = Schema.decodeUnknownSync(protocol.domain.serverFrame)(wireFrame);

protocol.encodeServerFrame(domainFrame);

// @ts-expect-error Wire frames must be decoded before they are passed to the domain encoder.
protocol.encodeServerFrame(wireFrame);
