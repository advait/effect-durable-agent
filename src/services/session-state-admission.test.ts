import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { CommandIdempotencyKey, StopTurnCommand, SubmitMessageCommand } from "../types/commands";
import type { SubmitMessageDisposition } from "../types/commands";
import { CommandId, SequenceNumber, SessionId } from "../types/core";
import { CommandAdmittedEvent, PositionedEvent } from "../types/events";
import { EDASessionStore } from "./session-store";
import { LiveEventBus } from "./live-event-bus";
import { SessionState } from "./session-state";
import { makeEdaTestLayer, testNowMs } from "../testkit/layers";

const SESSION_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const EVENT_ID = "018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a";
const COMMAND_ID = "018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a";
const MESSAGE_ID = "018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a";
const SUBMITTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f3a-1f2e3d4c5b6a";

const makeTestLayer = () =>
  makeEdaTestLayer({
    sessionId: SessionId.make(SESSION_ID),
    ids: [EVENT_ID, MESSAGE_ID, SUBMITTED_EVENT_ID],
  });

const submitMessage = (disposition: SubmitMessageDisposition) =>
  new SubmitMessageCommand({
    commandId: CommandId.make(COMMAND_ID),
    disposition,
    content: [Prompt.textPart({ text: "hello" })],
  });

describe("SessionState command admission", () => {
  it("mints commandId and persists caller idempotencyKey on admission", async () => {
    const idempotencyKey = CommandIdempotencyKey.make("web:create_session:example");
    const command = new SubmitMessageCommand({
      idempotencyKey,
      disposition: "queue",
      content: [Prompt.textPart({ text: "hello" })],
    });

    const program = Effect.gen(function* () {
      const sessionState = yield* SessionState;
      const store = yield* EDASessionStore;
      const committed = yield* sessionState.admitCommand(command);
      const replay = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { committed, replay: Array.from(replay) };
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          ids: [COMMAND_ID, EVENT_ID, MESSAGE_ID, SUBMITTED_EVENT_ID],
        }),
      ),
    );

    const result = await Effect.runPromise(program);

    expect(result.committed.event).toMatchObject({
      type: "CommandAdmitted",
      eventId: EVENT_ID,
      payload: {
        command: {
          _tag: "SubmitMessage",
          commandId: COMMAND_ID,
          idempotencyKey,
        },
      },
    });
    expect(result.replay.map(({ event }) => event.type)).toEqual([
      "CommandAdmitted",
      "UserMessageSubmitted",
    ]);
  });

  it("deduplicates command admission by caller idempotencyKey", async () => {
    const idempotencyKey = CommandIdempotencyKey.make("slack:event:example");

    const program = Effect.gen(function* () {
      const sessionState = yield* SessionState;
      const store = yield* EDASessionStore;
      const first = yield* sessionState.admitCommand(
        new SubmitMessageCommand({
          idempotencyKey,
          disposition: "queue",
          content: [Prompt.textPart({ text: "first" })],
        }),
      );
      const second = yield* sessionState.admitCommand(
        new SubmitMessageCommand({
          idempotencyKey,
          disposition: "queue",
          content: [Prompt.textPart({ text: "retry with changed text" })],
        }),
      );
      const replay = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { first, second, replay: Array.from(replay) };
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          ids: [COMMAND_ID, EVENT_ID, MESSAGE_ID, SUBMITTED_EVENT_ID],
        }),
      ),
    );

    const result = await Effect.runPromise(program);

    expect(result.second).toEqual(result.first);
    expect(result.replay.map(({ event }) => event.type)).toEqual([
      "CommandAdmitted",
      "UserMessageSubmitted",
    ]);
  });

  it("serializes concurrent duplicate idempotencyKey admissions", async () => {
    const idempotencyKey = CommandIdempotencyKey.make("web:double-click:example");

    const program = Effect.gen(function* () {
      const sessionState = yield* SessionState;
      const store = yield* EDASessionStore;
      const [first, second] = yield* Effect.all(
        [
          sessionState.admitCommand(
            new SubmitMessageCommand({
              idempotencyKey,
              disposition: "queue",
              content: [Prompt.textPart({ text: "first" })],
            }),
          ),
          sessionState.admitCommand(
            new SubmitMessageCommand({
              idempotencyKey,
              disposition: "queue",
              content: [Prompt.textPart({ text: "second" })],
            }),
          ),
        ],
        { concurrency: "unbounded" },
      );
      const replay = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      return { first, second, replay: Array.from(replay) };
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          ids: [COMMAND_ID, EVENT_ID, MESSAGE_ID, SUBMITTED_EVENT_ID],
        }),
      ),
    );

    const result = await Effect.runPromise(program);

    expect(result.second).toEqual(result.first);
    expect(result.replay.map(({ event }) => event.type)).toEqual([
      "CommandAdmitted",
      "UserMessageSubmitted",
    ]);
  });

  it("admits a command by committing and publishing CommandAdmitted", async () => {
    const command = new StopTurnCommand({ commandId: CommandId.make(COMMAND_ID) });

    const program = Effect.scoped(
      Effect.gen(function* () {
        const sessionState = yield* SessionState;
        const liveBus = yield* LiveEventBus;
        const store = yield* EDASessionStore;
        const liveStream = yield* liveBus.subscribe();
        const liveFiber = yield* liveStream.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );
        const committed = yield* sessionState.admitCommand(command);
        const replay = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
        const liveEvents = yield* Fiber.join(liveFiber);
        return { committed, replay, liveEvents };
      }),
    ).pipe(Effect.provide(makeTestLayer()));

    const result = await Effect.runPromise(program);

    expect(result.committed.position).toEqual({ seq: 1, subSeq: 0 });
    expect(result.committed.event).toMatchObject({
      namespace: "effect-durable-agent",
      type: "CommandAdmitted",
      schemaVersion: 1,
      durability: "durable",
      eventId: EVENT_ID,
      sessionId: SESSION_ID,
      createdAtMs: testNowMs,
      payload: { command },
    });
    expect(result.committed.event.trace.links).toEqual([]);
    expect(Schema.is(CommandAdmittedEvent)(result.committed.event)).toBe(true);
    expect(result.replay).toEqual([result.committed]);
    expect(result.liveEvents).toEqual([PositionedEvent.make(result.committed)]);
  });

  for (const disposition of ["queue", "steer", "interrupt"] as const) {
    it(`admits SubmitMessage disposition ${disposition}`, async () => {
      const command = submitMessage(disposition);

      const program = Effect.gen(function* () {
        const sessionState = yield* SessionState;
        const store = yield* EDASessionStore;
        const committed = yield* sessionState.admitCommand(command);
        const replay = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
        return { committed, replay: Array.from(replay) };
      }).pipe(Effect.provide(makeTestLayer()));

      const result = await Effect.runPromise(program);

      expect(result.committed.event).toMatchObject({
        type: "CommandAdmitted",
        eventId: EVENT_ID,
        payload: { command },
      });
      expect(result.replay.map(({ event }) => event.type)).toEqual(
        disposition === "interrupt"
          ? ["CommandAdmitted"]
          : ["CommandAdmitted", "UserMessageSubmitted"],
      );
    });
  }
});
