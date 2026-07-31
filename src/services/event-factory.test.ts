import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { SubmitMessageCommand } from "../types/commands";
import { CommandId, SessionId } from "../types/core";
import { effectDurableAgentNamespace, schemaV1, UnixEpochMillis } from "../types/events";
import { EventFactory } from "./event-factory";
import { IdGenerator } from "./id-generator";
import { SessionContext } from "./session-context";
import { makeFixedClock } from "../testkit/layers";

const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const FIRST_EVENT_ID = "018f6bd5-2f2a-7b1e-8f2a-1f2e3d4c5b6a";
const SECOND_EVENT_ID = "018f6bd5-2f2a-7b1e-8f2b-1f2e3d4c5b6a";
const CREATED_AT_MS = 1_715_000_000_000;

const command = new SubmitMessageCommand({
  commandId: COMMAND_ID,
  disposition: "queue",
  content: [Prompt.textPart({ text: "hello" })],
});

const makeLayer = (input: { readonly ids: ReadonlyArray<string>; readonly nowMs?: number }) =>
  EventFactory.Live.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        SessionContext.Live(SESSION_ID),
        IdGenerator.Deterministic(input.ids),
        Layer.succeed(Clock.Clock, makeFixedClock(input.nowMs ?? CREATED_AT_MS)),
      ),
    ),
  );

describe("EventFactory", () => {
  it("constructs durable and ephemeral envelopes with fixed session, deterministic event ids, and branded constants", async () => {
    const program = Effect.gen(function* () {
      const events = yield* EventFactory;
      const admitted = yield* events.commandAdmitted({ command });
      const delta = yield* events.textDelta({ providerPartId: "text-1", delta: "hello" });
      return { admitted, delta };
    }).pipe(Effect.provide(makeLayer({ ids: [FIRST_EVENT_ID, SECOND_EVENT_ID] })));

    const { admitted, delta } = await Effect.runPromise(program);

    expect(admitted).toMatchObject({
      namespace: effectDurableAgentNamespace,
      type: "CommandAdmitted",
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: FIRST_EVENT_ID,
      sessionId: SESSION_ID,
      createdAtMs: UnixEpochMillis.make(CREATED_AT_MS),
      payload: { command },
    });
    expect(delta).toMatchObject({
      namespace: effectDurableAgentNamespace,
      type: "TextDelta",
      schemaVersion: schemaV1,
      durability: "ephemeral",
      eventId: SECOND_EVENT_ID,
      sessionId: SESSION_ID,
      createdAtMs: UnixEpochMillis.make(CREATED_AT_MS),
      payload: { providerPartId: "text-1", delta: "hello" },
    });
  });

  it("reads Clock at event construction time instead of layer construction time", async () => {
    let nowMs = CREATED_AT_MS;
    const clock = makeFixedClock(CREATED_AT_MS);
    const liveClock: Clock.Clock = {
      ...clock,
      currentTimeMillisUnsafe: () => nowMs,
      currentTimeMillis: Effect.sync(() => nowMs),
      currentTimeNanosUnsafe: () => BigInt(nowMs) * 1_000_000n,
      currentTimeNanos: Effect.sync(() => BigInt(nowMs) * 1_000_000n),
    };
    const program = Effect.gen(function* () {
      const events = yield* EventFactory;
      const first = yield* events.commandStarted({ commandId: COMMAND_ID });
      nowMs = CREATED_AT_MS + 123;
      const second = yield* events.commandCompleted({ commandId: COMMAND_ID });
      return { first, second };
    }).pipe(
      Effect.provide(
        EventFactory.Live.pipe(
          Layer.provideMerge(
            Layer.mergeAll(
              SessionContext.Live(SESSION_ID),
              IdGenerator.Deterministic([FIRST_EVENT_ID, SECOND_EVENT_ID]),
              Layer.succeed(Clock.Clock, liveClock),
            ),
          ),
        ),
      ),
    );

    const { first, second } = await Effect.runPromise(program);

    expect(first.createdAtMs).toBe(CREATED_AT_MS);
    expect(second.createdAtMs).toBe(CREATED_AT_MS + 123);
  });

  it("fails while minting the event id when the deterministic IdGenerator is exhausted", async () => {
    const program = Effect.gen(function* () {
      const events = yield* EventFactory;
      return yield* events.commandStarted({ commandId: COMMAND_ID });
    }).pipe(Effect.provide(makeLayer({ ids: [] })));

    const exit = await Effect.runPromise(Effect.exit(program));

    expect(Exit.isFailure(exit)).toBe(true);
    expect(Exit.isFailure(exit) ? Cause.pretty(exit.cause) : "").toContain(
      "deterministic IdGenerator exhausted",
    );
  });
});
