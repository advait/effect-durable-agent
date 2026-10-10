import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as Stream from "effect/Stream";

import { EventId, SequenceNumber, SessionId } from "../types/core";
import { DurableEventEnvelope, UnixEpochMillis } from "../types/events";
import { durableEventEnvelope } from "../types/events/durable";
import { EDASessionStore, registerAppEvents, selectCommittedEvent } from "./session-store";

const sessionId = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const appEvent = Schema.Struct({
  ...DurableEventEnvelope.fields,
  namespace: Schema.Literal("test.app"),
  type: Schema.Literal("AppFact"),
  payload: Schema.Struct({ count: Schema.Number }),
});

it.effect(
  "decodes registered app facts once and narrows consumers without rerunning the codec",
  () =>
    Effect.gen(function* () {
      let decodeCount = 0;
      const registered = appEvent.pipe(
        Schema.decodeTo(appEvent, {
          decode: SchemaGetter.transform((event) => {
            decodeCount++;
            return { ...event, payload: { count: event.payload.count + 1 } };
          }),
          encode: SchemaGetter.passthrough(),
        }),
      );
      const underlying = yield* EDASessionStore;
      const original = appEvent.make({
        namespace: "test.app",
        type: "AppFact",
        schemaVersion: 1,
        durability: "durable",
        eventId: EventId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a"),
        sessionId,
        createdAtMs: UnixEpochMillis.make(1),
        payload: { count: 42 },
      });
      yield* underlying.append({ entries: [{ event: original }] });
      const store = registerAppEvents(underlying, registered);
      const replay = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      assert.strictEqual(decodeCount, 1);
      const entry = replay[0];
      if (entry === undefined) return yield* Effect.die(new Error("Missing app fact"));
      assert.deepStrictEqual(durableEventEnvelope(entry.event), original);
      const selected = selectCommittedEvent(registered, entry);
      assert.strictEqual(selected?.event.payload.count, 43);
      assert.strictEqual(decodeCount, 1);
    }).pipe(Effect.provide(EDASessionStore.InMemory(sessionId))),
);

it.effect(
  "rejects malformed registered facts before append and when replaying retained bytes",
  () =>
    Effect.gen(function* () {
      const underlying = yield* EDASessionStore;
      const store = registerAppEvents(underlying, appEvent);
      const malformed = DurableEventEnvelope.make({
        namespace: "test.app",
        type: "AppFact",
        schemaVersion: 1,
        durability: "durable",
        eventId: EventId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a"),
        sessionId,
        createdAtMs: UnixEpochMillis.make(1),
        payload: { count: "invalid" },
      });
      const rejected = yield* store.append({ entries: [{ event: malformed }] }).pipe(Effect.result);
      assert.strictEqual(rejected._tag, "Failure");
      const retained = yield* underlying
        .eventsAfter(SequenceNumber.make(0))
        .pipe(Stream.runCollect);
      assert.strictEqual(retained.length, 0);
      yield* underlying.append({ entries: [{ event: malformed }] });
      const replay = yield* store
        .eventsAfter(SequenceNumber.make(0))
        .pipe(Stream.runCollect, Effect.result);
      assert.strictEqual(replay._tag, "Failure");
    }).pipe(Effect.provide(EDASessionStore.InMemory(sessionId))),
);
