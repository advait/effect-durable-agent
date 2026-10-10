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

it.effect("fully decodes refinements before commit and preserves idempotent retained facts", () =>
  Effect.gen(function* () {
    let decodeCount = 0;
    const refined = Schema.Struct({
      ...appEvent.fields,
      payload: Schema.Struct({ count: Schema.NumberFromString.check(Schema.isFinite()) }),
    }).pipe(
      Schema.decodeTo(appEvent, {
        decode: SchemaGetter.transform((event) => {
          decodeCount++;
          return event;
        }),
        encode: SchemaGetter.passthrough(),
      }),
    );
    const underlying = yield* EDASessionStore;
    const store = registerAppEvents(underlying, refined);
    const original = DurableEventEnvelope.make({
      namespace: "test.app",
      type: "AppFact",
      schemaVersion: 1,
      durability: "durable",
      eventId: EventId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a"),
      sessionId,
      createdAtMs: UnixEpochMillis.make(1),
      payload: { count: "not-a-number" },
    });
    assert.strictEqual(Schema.is(Schema.toEncoded(refined))(original), true);
    const rejected = yield* store.append({ entries: [{ event: original }] }).pipe(Effect.result);
    assert.strictEqual(rejected._tag, "Failure");
    const retained = yield* underlying.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
    assert.strictEqual(retained.length, 0);
    const valid = { ...original, payload: { count: "42" } };
    const committed = yield* store.append({ entries: [{ event: valid }] });
    assert.strictEqual(decodeCount, 1);
    const entry = committed[0];
    if (entry === undefined) return yield* Effect.die(new Error("Missing committed app fact"));
    assert.strictEqual(selectCommittedEvent(refined, entry)?.event.payload.count, 42);
    assert.deepStrictEqual(durableEventEnvelope(entry.event), valid);
    yield* store.append({ entries: [{ event: valid }] });
    assert.strictEqual(decodeCount, 2);
    const duplicate = yield* store.append({
      entries: [{ event: { ...valid, payload: { count: "99" } } }],
    });
    const existing = duplicate[0];
    if (existing === undefined) return yield* Effect.die(new Error("Missing idempotent app fact"));
    assert.strictEqual(selectCommittedEvent(refined, existing)?.event.payload.count, 42);
    assert.deepStrictEqual(durableEventEnvelope(existing.event), valid);
    const replay = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
    assert.strictEqual(replay.length, 1);
  }).pipe(Effect.provide(EDASessionStore.InMemory(sessionId))),
);
