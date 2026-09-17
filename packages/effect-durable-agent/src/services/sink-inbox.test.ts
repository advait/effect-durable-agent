import { assert, makeMethods } from "@effect/vitest";
import { describe, it } from "vite-plus/test";
import * as Effect from "effect/Effect";

import { EventId, Position, SequenceNumber, SessionId, SubSequenceNumber } from "../types/core";
import {
  EphemeralEventEnvelope,
  EventType,
  PositionedEvent,
  UnixEpochMillis,
  effectDurableAgentNamespace,
  schemaV1,
} from "../types/events";
import { sequentialUuidV7 } from "./id-generator";
import { makeSinkInbox } from "./sink-inbox";

const eventAt = (seq: number, subSeq = 1, delta = "update") =>
  PositionedEvent.make({
    position: Position.make({
      seq: SequenceNumber.make(seq),
      subSeq: SubSequenceNumber.make(subSeq),
    }),
    event: EphemeralEventEnvelope.make({
      namespace: effectDurableAgentNamespace,
      type: EventType.make("TextDelta"),
      schemaVersion: schemaV1,
      durability: "ephemeral",
      eventId: EventId.make(sequentialUuidV7(subSeq)),
      sessionId: SessionId.make(sequentialUuidV7(1)),
      createdAtMs: UnixEpochMillis.make(1),
      payload: { delta },
    }),
  });

describe("sink delivery inbox", () => {
  makeMethods(it).effect("preserves serialization failure without retaining malformed work", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const inbox = yield* makeSinkInbox(SequenceNumber.make(10));
        const event = eventAt(10);
        const malformed = { ...event, event: { ...event.event, payload: { value: 1n } } };
        const error = yield* inbox.offerEphemeral(malformed).pipe(Effect.flip);
        assert.strictEqual(error._tag, "SinkEphemeralEncodingError");
        assert.strictEqual(error.cause instanceof TypeError, true);
        assert.strictEqual(yield* inbox.poll(SequenceNumber.make(10)), undefined);
        assert.strictEqual(yield* inbox.offerEphemeral(event), true);
      }),
    ),
  );

  makeMethods(it).effect("coalesces heads while preserving ephemeral sequence barriers", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const inbox = yield* makeSinkInbox(SequenceNumber.make(0));
        const first = eventAt(10, 1);
        const second = eventAt(10, 2);
        yield* inbox.notify(SequenceNumber.make(10));
        yield* inbox.offerEphemeral(first);
        yield* inbox.offerEphemeral(second);
        for (let head = 11; head <= 10_000; head++) yield* inbox.notify(SequenceNumber.make(head));
        assert.deepStrictEqual(yield* inbox.poll(SequenceNumber.make(0)), {
          _tag: "Durable",
          throughSeq: 10,
        });
        assert.deepStrictEqual(yield* inbox.poll(SequenceNumber.make(10)), {
          _tag: "Ephemeral",
          event: first,
        });
        assert.deepStrictEqual(yield* inbox.poll(SequenceNumber.make(10)), {
          _tag: "Ephemeral",
          event: second,
        });
        assert.deepStrictEqual(yield* inbox.poll(SequenceNumber.make(10)), {
          _tag: "Durable",
          throughSeq: 10_000,
        });
        assert.strictEqual(yield* inbox.poll(SequenceNumber.make(10_000)), undefined);
      }),
    ),
  );

  makeMethods(it).effect("drops ephemeral overflow without losing the durable head", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const inbox = yield* makeSinkInbox(SequenceNumber.make(10));
        const events = Array.from({ length: 1000 }, (_, i) => eventAt(10, i + 1));
        let accepted = 0;
        for (const event of events) if (yield* inbox.offerEphemeral(event)) accepted++;
        assert.strictEqual(accepted, 128);
        yield* inbox.notify(SequenceNumber.make(20));
        for (let i = 1; i <= accepted; i++) {
          assert.deepStrictEqual(yield* inbox.poll(SequenceNumber.make(10)), {
            _tag: "Ephemeral",
            event: events[i - 1],
          });
        }
        assert.deepStrictEqual(yield* inbox.poll(SequenceNumber.make(10)), {
          _tag: "Durable",
          throughSeq: 20,
        });
        assert.strictEqual(yield* inbox.offerEphemeral(eventAt(20)), true);
      }),
    ),
  );

  makeMethods(it).effect("bounds bytes as well as count and releases capacity after delivery", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const inbox = yield* makeSinkInbox(SequenceNumber.make(10));
        const large = eventAt(10, 1, "x".repeat(150_000));
        assert.strictEqual(yield* inbox.offerEphemeral(large), true);
        assert.strictEqual(yield* inbox.offerEphemeral(eventAt(10, 2, "x".repeat(150_000))), false);
        yield* inbox.poll(SequenceNumber.make(10));
        assert.strictEqual(yield* inbox.offerEphemeral(large), true);
        assert.strictEqual(yield* inbox.offerEphemeral(eventAt(10, 3, "x".repeat(300_000))), false);
      }),
    ),
  );
});
