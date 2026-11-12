import * as fc from "fast-check";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/ai/Prompt";
import * as Response from "effect/ai/Response";
import * as Tool from "effect/ai/Tool";
import { describe, expect, it } from "vite-plus/test";

import { makeEdaTestLayer } from "../testkit/layers";
import { CommandIdempotencyKey, SubmitMessageCommand } from "../types/commands";
import { CommandId, EventId, MessageId, SequenceNumber, SessionId } from "../types/core";
import {
  AssistantMessageImportedEvent,
  DeciderAcceptedEvent,
  DurableEventEnvelope,
  EventDedupeKey,
  SessionConfiguredEvent,
  SystemMessageCommittedEvent,
  UnixEpochMillis,
  assistantMessageImportedEventType,
  deciderAcceptedEventType,
  effectDurableAgentNamespace,
  maxAppEventBytes,
  schemaV1,
  sessionConfiguredEventType,
  systemMessageCommittedEventType,
} from "../types/events";
import { durableEventEnvelope } from "../types/events/durable";
import { makeEDATraceContext } from "../types/tracing";
import {
  EDADecider,
  deciderMarkerDedupeKey,
  type EDADeciderOutcome,
  type EDAEventDraft,
} from "./decider";
import { sequentialUuidV7 } from "./id-generator";
import { EDAReducer, getEDAReducerState } from "./reducer-registry";
import { SessionState } from "./session-state";
import {
  EDASessionStore,
  hasEDASessionStoreError,
  type CommittedDurableEvent,
  type EDASessionStoreShape,
} from "./session-store";
import {
  SESSION_ID,
  collectCommitted,
  command,
  hasCommandCompleted,
  modelSelection,
  NoopParams,
  usage,
} from "./session-state-control-testkit";
import { ToolRecovery, type EDAModelToolkit, type EDAToolRegistryShape } from "./tool-registry";

const sessionId = SessionId.make(SESSION_ID);
const NOW = UnixEpochMillis.make(1_715_000_000_000);

const appFields = { ...DurableEventEnvelope.fields, namespace: Schema.Literal("test.chat") };
const MessageRouted = Schema.Struct({
  ...appFields,
  type: Schema.Literal("MessageRouted"),
  payload: Schema.Struct({ text: Schema.String, note: Schema.optional(Schema.String) }),
});
const ChatEnded = Schema.Struct({
  ...appFields,
  type: Schema.Literal("ChatEnded"),
  payload: Schema.Struct({}),
});
const NoteSet = Schema.Struct({
  ...appFields,
  type: Schema.Literal("NoteSet"),
  payload: Schema.Struct({ note: Schema.UndefinedOr(Schema.String) }),
});
/** What the runtime registers as `appEvents`. */
const ChatEvents = Schema.Union([MessageRouted, ChatEnded, NoteSet]);

const ChatState = Schema.Struct({ ended: Schema.Boolean, routed: Schema.Number });
const chatReducer = EDAReducer.forEvents(ChatEvents, {
  name: "test.chat",
  initial: { ended: false, routed: 0 },
  stateSchema: ChatState,
  reduce: (state, entry) =>
    entry.event.type === "ChatEnded"
      ? { ...state, ended: true }
      : { ...state, routed: state.routed + 1 },
});

const PostInput = Schema.Struct({ key: Schema.String, text: Schema.String });
const decideCalls = { value: 0 };

/** Routes a message to the AI while the chat is open; refuses once it has ended. */
const postMessage = EDADecider.define({
  name: "test.chat.PostMessage",
  input: PostInput,
  events: ChatEvents,
  result: Schema.Struct({ routedNumber: Schema.Number }),
  rejection: Schema.Struct({ reason: Schema.Literal("ended") }),
  idempotencyKey: (input) => input.key,
  decide: (view, input) => {
    decideCalls.value += 1;
    const chat = getEDAReducerState(view.reducerStates, chatReducer);
    if (chat.ended) return { _tag: "Reject", rejection: { reason: "ended" } };
    return {
      _tag: "Accept",
      items: [
        {
          namespace: "test.chat",
          type: "MessageRouted",
          schemaVersion: schemaV1,
          payload: { text: input.text },
        },
        new SubmitMessageCommand({
          idempotencyKey: CommandIdempotencyKey.make(`post:${input.key}`),
          disposition: "steer",
          content: [Prompt.textPart({ text: input.text })],
        }),
      ],
      result: { routedNumber: chat.routed + 1 },
    };
  },
});

const chatEnded = () =>
  ChatEnded.make({
    namespace: "test.chat",
    type: "ChatEnded",
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(sequentialUuidV7(90_000)),
    sessionId,
    createdAtMs: NOW,
    payload: {},
  });

const routed = (text: unknown, eventId = sequentialUuidV7(90_001)) =>
  DurableEventEnvelope.make({
    namespace: "test.chat",
    type: "MessageRouted",
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(eventId),
    sessionId,
    createdAtMs: NOW,
    payload: { text },
  });

const nextSubmit = () =>
  new SubmitMessageCommand({
    idempotencyKey: CommandIdempotencyKey.make("after"),
    disposition: "queue",
    content: [Prompt.textPart({ text: "after" })],
  });

const countAppends = (appends: { value: number }) => (inner: EDASessionStoreShape) => ({
  ...inner,
  append: (batch: Parameters<EDASessionStoreShape["append"]>[0]) =>
    Effect.sync(() => {
      appends.value += 1;
    }).pipe(Effect.andThen(inner.append(batch))),
});

const layer = (
  options: {
    readonly deciders?: Parameters<typeof makeEdaTestLayer>[0]["deciders"];
    readonly wrapStore?: (inner: EDASessionStoreShape) => EDASessionStoreShape;
  } = {},
) =>
  makeEdaTestLayer({
    sessionId,
    appEvents: ChatEvents,
    reducers: [chatReducer],
    deciders: options.deciders ?? [postMessage],
    ...(options.wrapStore === undefined ? {} : { wrapStore: options.wrapStore }),
  });

const run = <A, E>(
  program: Effect.Effect<A, E, SessionState | EDASessionStore>,
  options?: Parameters<typeof layer>[0],
) => Effect.runPromise(Effect.scoped(program).pipe(Effect.provide(layer(options))));

const logOf = Effect.gen(function* () {
  return yield* collectCommitted(yield* EDASessionStore);
});

const markers = (committed: ReadonlyArray<CommittedDurableEvent>) =>
  committed.filter((entry) => entry.event.type === deciderAcceptedEventType);

const failureOf = <A, E>(exit: Exit.Exit<A, E>) =>
  Exit.isFailure(exit) ? Option.getOrUndefined(Exit.findErrorOption(exit)) : undefined;

const reasonOf = <A, E>(exit: Exit.Exit<A, E>) => {
  const error = failureOf(exit);
  return typeof error === "object" && error !== null && "reason" in error
    ? error.reason
    : typeof error === "object" && error !== null && "code" in error
      ? error.code
      : undefined;
};

/** The runtime is still usable: a following submit commits, and nothing was fatal. */
const followingSubmitSucceeds = Effect.gen(function* () {
  const state = yield* SessionState;
  const exit = yield* Effect.exit(state.submitBatch([nextSubmit()]));
  return Exit.isSuccess(exit);
});

describe("SessionState deciders", () => {
  it("commits the marker and the items contiguously in one append, and wakes the control loop", async () => {
    const appends = { value: 0 };
    const result = await run(
      Effect.gen(function* () {
        const state = yield* SessionState;
        yield* state.start({ modelSelection });
        const before = appends.value;
        const outcome = yield* state.decide(postMessage, { key: "m1", text: "hello" });
        const appendsForDecision = appends.value - before;
        const store = yield* EDASessionStore;
        for (let attempt = 0; attempt < 200; attempt += 1) {
          const committed = yield* collectCommitted(store);
          if (committed.some((entry) => entry.event.type === "CommandStarted")) break;
          yield* Effect.yieldNow;
        }
        return { outcome, appendsForDecision, committed: yield* logOf };
      }),
      { wrapStore: countAppends(appends) },
    );

    expect(result.outcome).toEqual({
      _tag: "Accepted",
      result: { routedNumber: 1 },
      acceptedSeq: 1,
      replayed: false,
    });
    expect(result.appendsForDecision).toBe(1);
    expect(result.committed.slice(0, 4).map((entry) => entry.event.type)).toEqual([
      "DeciderAccepted",
      "Foreign",
      "CommandAdmitted",
      "UserMessageSubmitted",
    ]);
    const [markerEntry, draftEntry] = result.committed;
    if (markerEntry === undefined || draftEntry === undefined) throw new Error("missing batch");
    const marker = durableEventEnvelope(markerEntry.event);
    expect(marker).toMatchObject({
      namespace: effectDurableAgentNamespace,
      createdAtMs: NOW,
      dedupeKey: "test.chat.PostMessage\nm1",
      payload: {
        decider: "test.chat.PostMessage",
        idempotencyKey: "m1",
        result: { routedNumber: 1 },
      },
    });
    // The draft is stamped with the decision's clock and session.
    expect(durableEventEnvelope(draftEntry.event)).toMatchObject({
      namespace: "test.chat",
      type: "MessageRouted",
      sessionId,
      createdAtMs: NOW,
      payload: { text: "hello" },
    });
    // The admitted command ran without a separate wakeup.
    expect(result.committed.some((entry) => entry.event.type === "CommandStarted")).toBe(true);
  });

  it("commits nothing on a rejection", async () => {
    const result = await run(
      Effect.gen(function* () {
        const state = yield* SessionState;
        yield* state.submitBatch([chatEnded()]);
        const head = (yield* state.snapshot()).lastSeq;
        const outcome = yield* state.decide(postMessage, { key: "m1", text: "hello" });
        return { outcome, head, after: (yield* state.snapshot()).lastSeq, log: yield* logOf };
      }),
    );
    expect(result.outcome).toEqual({ _tag: "Rejected", rejection: { reason: "ended" } });
    expect(result.after).toBe(result.head);
    expect(result.log).toHaveLength(1);
  });

  it("replays the stored result without deciding again, though state now decides otherwise", async () => {
    const result = await run(
      Effect.gen(function* () {
        const state = yield* SessionState;
        const first = yield* state.decide(postMessage, { key: "m1", text: "hello" });
        // Now the chat has ended: deciding again would reject.
        yield* state.submitBatch([chatEnded()]);
        const head = (yield* state.snapshot()).lastSeq;
        const callsBefore = decideCalls.value;
        const retry = yield* state.decide(postMessage, { key: "m1", text: "a retry may differ" });
        return {
          first,
          retry,
          decided: decideCalls.value - callsBefore,
          head,
          after: (yield* state.snapshot()).lastSeq,
          log: yield* logOf,
        };
      }),
    );
    expect(result.retry).toEqual({ ...result.first, replayed: true });
    expect(result.decided).toBe(0);
    expect(result.after).toBe(result.head);
    expect(markers(result.log)).toHaveLength(1);
  });

  it("accepts an optional field left undefined in the result and in a draft, as JSON stores it", async () => {
    const annotate = EDADecider.define({
      name: "test.Annotate",
      input: Schema.Struct({ key: Schema.String }),
      events: ChatEvents,
      result: Schema.Struct({ note: Schema.optional(Schema.String) }),
      rejection: Schema.Never,
      idempotencyKey: (input) => input.key,
      decide: () => ({
        _tag: "Accept",
        items: [
          {
            namespace: "test.chat",
            type: "MessageRouted",
            schemaVersion: schemaV1,
            payload: { text: "hi", note: undefined },
          },
        ],
        result: { note: undefined },
      }),
    });
    const result = await run(
      Effect.gen(function* () {
        const state = yield* SessionState;
        const first = yield* state.decide(annotate, { key: "k" });
        const retry = yield* state.decide(annotate, { key: "k" });
        return { first, retry, log: yield* logOf };
      }),
      { deciders: [annotate] },
    );
    // Reviewer repro (gpt-6-astra, FR-1 review round 2): the first call returned the in-memory
    // `{ note: undefined }` while the store kept `{}`; both calls now return what the store keeps.
    expect(result.first).toStrictEqual({
      _tag: "Accepted",
      result: {},
      acceptedSeq: 1,
      replayed: false,
    });
    expect(result.retry).toStrictEqual({ ...result.first, replayed: true });
    const marker = markers(result.log)[0]?.event;
    expect(marker === undefined ? undefined : durableEventEnvelope(marker).payload).toEqual({
      decider: "test.Annotate",
      idempotencyKey: "k",
      result: {},
    });
    expect(result.log.map((entry) => durableEventEnvelope(entry.event).payload)[1]).toStrictEqual({
      text: "hi",
    });
  });

  it("refuses a result whose stored JSON its result schema refuses, and commits nothing", async () => {
    // Reviewer repro (gpt-6-astra, FR-1 review round 2): `UndefinedOr` accepts `{ note: undefined }`,
    // the marker kept `{}`, and every same-key retry failed with "stored result does not decode".
    const required = EDADecider.define({
      name: "test.RequiredUndefined",
      input: Schema.Struct({ key: Schema.String }),
      events: ChatEvents,
      result: Schema.Struct({ note: Schema.UndefinedOr(Schema.String) }),
      rejection: Schema.Never,
      idempotencyKey: (input) => input.key,
      decide: () => ({
        _tag: "Accept",
        items: [
          { namespace: "test.chat", type: "ChatEnded", schemaVersion: schemaV1, payload: {} },
        ],
        result: { note: undefined },
      }),
    });
    const result = await run(
      Effect.gen(function* () {
        const state = yield* SessionState;
        const first = yield* Effect.exit(state.decide(required, { key: "k" }));
        const retry = yield* Effect.exit(state.decide(required, { key: "k" }));
        const log = yield* logOf;
        return { first, retry, log, usable: yield* followingSubmitSucceeds };
      }),
      { deciders: [required] },
    );
    expect(reasonOf(result.first)).toBe("invalid_output");
    expect(reasonOf(result.retry)).toBe("invalid_output");
    expect(result.log).toHaveLength(0);
    expect(result.usable).toBe(true);
  });

  it("checks the idempotency key: a line feed or 257 characters is invalid_input; 256 quotes replay", async () => {
    const quotes = '"'.repeat(256);
    const result = await run(
      Effect.gen(function* () {
        const state = yield* SessionState;
        const lineFeed = yield* Effect.exit(state.decide(postMessage, { key: "a\nb", text: "x" }));
        const long = yield* Effect.exit(
          state.decide(postMessage, { key: "k".repeat(257), text: "x" }),
        );
        const empty = yield* Effect.exit(state.decide(postMessage, { key: "", text: "x" }));
        const first = yield* state.decide(postMessage, { key: quotes, text: "x" });
        const again = yield* state.decide(postMessage, { key: quotes, text: "x" });
        return { lineFeed, long, empty, first, again, log: yield* logOf };
      }),
    );
    expect(reasonOf(result.lineFeed)).toBe("invalid_input");
    expect(reasonOf(result.long)).toBe("invalid_input");
    expect(reasonOf(result.empty)).toBe("invalid_input");
    expect(result.first._tag).toBe("Accepted");
    expect(result.again).toMatchObject({ _tag: "Accepted", replayed: true });
    const [markerEntry] = markers(result.log);
    if (markerEntry === undefined) throw new Error("missing marker");
    expect(durableEventEnvelope(markerEntry.event).dedupeKey).toBe(
      deciderMarkerDedupeKey("test.chat.PostMessage", quotes),
    );
    // The longest legal marker key (128 + 1 + 256) fits EventDedupeKey's 512 characters.
    expect(deciderMarkerDedupeKey("n".repeat(128), quotes)).toHaveLength(385);
  });

  it("refuses a draft its own events schema accepts and the registered schema refuses", async () => {
    // Reviewer repro (gpt-6-astra): the decider's schema and `appEvents` disagree on a payload.
    const LooseRouted = Schema.Struct({
      ...appFields,
      type: Schema.Literal("MessageRouted"),
      payload: Schema.Struct({ text: Schema.Unknown }),
    });
    const loose = EDADecider.define({
      name: "test.chat.Loose",
      input: Schema.Struct({ key: Schema.String }),
      events: LooseRouted,
      result: Schema.Null,
      rejection: Schema.Never,
      idempotencyKey: (input) => input.key,
      decide: () => ({
        _tag: "Accept",
        items: [
          {
            namespace: "test.chat",
            type: "MessageRouted",
            schemaVersion: 1,
            payload: { text: 42 },
          },
        ],
        result: null,
      }),
    });
    const result = await run(
      Effect.gen(function* () {
        const state = yield* SessionState;
        const decided = yield* Effect.exit(state.decide(loose, { key: "k" }));
        const submitted = yield* Effect.exit(state.submitBatch([routed(42)]));
        const valid = yield* Effect.exit(state.submitBatch([routed("ok")]));
        return { decided, submitted, valid, log: yield* logOf };
      }),
      { deciders: [postMessage, loose] },
    );
    expect(reasonOf(result.decided)).toBe("invalid_output");
    expect(reasonOf(result.submitted)).toBe("invalid_event");
    expect(Exit.isSuccess(result.valid)).toBe(true);
    for (const exit of [result.decided, result.submitted]) {
      expect(Exit.isFailure(exit) && hasEDASessionStoreError(exit.cause)).toBe(false);
    }
    expect(result.log).toHaveLength(1);
  });

  it("accepts Gia's seed batch through submit and as decider drafts", async () => {
    // Reviewer repro (Claude Opus 5.5): Gia's Slack, cron, web and subagent seed batches, copied
    // from `makeEDAGiaSessionConfigured` and `makeEDAGiaSystemPromptCommitted`.
    const trace = () => ({ span: makeEDATraceContext(), links: [] });
    const configured = SessionConfiguredEvent.make({
      namespace: effectDurableAgentNamespace,
      type: sessionConfiguredEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(sequentialUuidV7(80_000)),
      sessionId,
      createdAtMs: NOW,
      trace: trace(),
      payload: { modelSelection: { provider: "openai", modelId: "gpt-5.5" } },
    });
    const systemPrompt = SystemMessageCommittedEvent.make({
      namespace: effectDurableAgentNamespace,
      type: systemMessageCommittedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(sessionId),
      sessionId,
      createdAtMs: NOW,
      trace: trace(),
      payload: { messageId: MessageId.make(sequentialUuidV7(80_001)), content: "You are Gia." },
    });
    const message = new SubmitMessageCommand({
      content: [Prompt.textPart({ text: "hello" })],
      disposition: "steer",
      idempotencyKey: CommandIdempotencyKey.make("slack:event:Ev1:message:m1"),
    });
    const SeedEvents = Schema.Union([SessionConfiguredEvent, SystemMessageCommittedEvent]);
    const seed = EDADecider.define({
      name: "gia.slack.ReceiveMessage",
      input: Schema.Struct({ key: Schema.String }),
      events: SeedEvents,
      result: Schema.Null,
      rejection: Schema.Never,
      idempotencyKey: (input) => input.key,
      decide: () => ({
        _tag: "Accept",
        items: [
          {
            namespace: effectDurableAgentNamespace,
            type: sessionConfiguredEventType,
            schemaVersion: schemaV1,
            payload: configured.payload,
          },
          {
            namespace: effectDurableAgentNamespace,
            type: systemMessageCommittedEventType,
            schemaVersion: schemaV1,
            eventId: systemPrompt.eventId,
            payload: systemPrompt.payload,
          },
          message,
        ],
        result: null,
      }),
    });
    const submitted = await Effect.runPromise(
      Effect.gen(function* () {
        const state = yield* SessionState;
        return yield* state.submitBatch([configured, systemPrompt, message]);
      }).pipe(Effect.provide(makeEdaTestLayer({ sessionId }))),
    );
    expect(submitted.map((entry) => entry.event.type)).toEqual([
      "SessionConfigured",
      "SystemMessageCommitted",
      "CommandAdmitted",
      "UserMessageSubmitted",
    ]);
    const decided = await Effect.runPromise(
      Effect.gen(function* () {
        const state = yield* SessionState;
        const outcome = yield* state.decide(seed, { key: "slack:event:Ev1:message:m1" });
        return { outcome, log: yield* logOf };
      }).pipe(Effect.provide(makeEdaTestLayer({ sessionId, deciders: [seed] }))),
    );
    expect(decided.outcome._tag).toBe("Accepted");
    expect(decided.log.map((entry) => entry.event.type)).toEqual([
      "DeciderAccepted",
      "SessionConfigured",
      "SystemMessageCommitted",
      "CommandAdmitted",
      "UserMessageSubmitted",
    ]);
    // The system prompt keeps its fixed id.
    expect(decided.log[2]?.event.eventId).toBe(sessionId);
  });

  it("commits nothing and stays usable after decide_threw, invalid_input, invalid_output or an inner conflict", async () => {
    const make = (
      name: string,
      decide: (input: { readonly key: string }) => ReturnType<typeof postMessage.decide>,
      idempotencyKey: (input: { readonly key: string }) => string = (input) => input.key,
    ) =>
      EDADecider.define({
        name,
        input: Schema.Struct({ key: Schema.String }),
        events: ChatEvents,
        result: Schema.Struct({ routedNumber: Schema.Number }),
        rejection: Schema.Struct({ reason: Schema.Literal("ended") }),
        idempotencyKey,
        decide: (_view, input) => decide(input),
      });
    const throws = make("test.Throws", () => {
      throw new Error("boom");
    });
    const keyThrows = make(
      "test.KeyThrows",
      () => ({ _tag: "Accept", items: [], result: { routedNumber: 0 } }),
      () => {
        throw new Error("no key");
      },
    );
    const oversizeResult = EDADecider.define({
      name: "test.OversizeResult",
      input: Schema.Struct({ key: Schema.String }),
      events: ChatEvents,
      result: Schema.String,
      rejection: Schema.Never,
      idempotencyKey: (input) => input.key,
      decide: () => ({ _tag: "Accept", items: [], result: "r".repeat(16_384) }),
    });
    const conflict = make("test.Conflict", () => ({
      _tag: "Accept",
      items: [
        new SubmitMessageCommand({
          disposition: "steer",
          content: [Prompt.textPart({ text: "clear" })],
          // The paused queue is empty, so this confirmation is stale.
          expectedPausedMessageIdsToCancel: [MessageId.make(sequentialUuidV7(70_000))],
        }),
      ],
      result: { routedNumber: 0 },
    }));
    const deciders = [postMessage, throws, keyThrows, oversizeResult, conflict];
    const result = await run(
      Effect.gen(function* () {
        const state = yield* SessionState;
        const exits = {
          threw: yield* Effect.exit(state.decide(throws, { key: "k" })),
          badInput: yield* Effect.exit(state.decide(keyThrows, { key: "k" })),
          badOutput: yield* Effect.exit(state.decide(oversizeResult, { key: "k" })),
          conflict: yield* Effect.exit(state.decide(conflict, { key: "k" })),
        };
        const headBefore = (yield* state.snapshot()).lastSeq;
        return { exits, headBefore, usable: yield* followingSubmitSucceeds };
      }),
      { deciders },
    );
    expect(reasonOf(result.exits.threw)).toBe("decide_threw");
    expect(reasonOf(result.exits.badInput)).toBe("invalid_input");
    expect(reasonOf(result.exits.badOutput)).toBe("invalid_output");
    expect(reasonOf(result.exits.conflict)).toBe("paused_queue_changed");
    expect(result.headBefore).toBe(SequenceNumber.make(0));
    expect(result.usable).toBe(true);
  });

  it("refuses an unregistered decider", async () => {
    const result = await run(
      Effect.gen(function* () {
        const state = yield* SessionState;
        return yield* Effect.exit(state.decide(postMessage, { key: "m1", text: "x" }));
      }),
      { deciders: [] },
    );
    expect(reasonOf(result)).toBe("unknown_decider");
  });

  it("refuses a framework lifecycle event as a draft, through submit and through a tool's emit path", async () => {
    const forged = DeciderAcceptedEvent.make({
      namespace: effectDurableAgentNamespace,
      type: deciderAcceptedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(sequentialUuidV7(60_000)),
      sessionId,
      createdAtMs: NOW,
      dedupeKey: EventDedupeKey.make(deciderMarkerDedupeKey("test.chat.PostMessage", "m1")),
      payload: {
        decider: "test.chat.PostMessage",
        idempotencyKey: "m1",
        result: { routedNumber: 99 },
      },
    });
    const forger = EDADecider.define({
      name: "test.Forger",
      input: Schema.Struct({ key: Schema.String }),
      events: DeciderAcceptedEvent,
      result: Schema.Null,
      rejection: Schema.Never,
      idempotencyKey: (input) => input.key,
      decide: () => ({
        _tag: "Accept",
        items: [
          {
            namespace: effectDurableAgentNamespace,
            type: deciderAcceptedEventType,
            schemaVersion: schemaV1,
            payload: forged.payload,
          },
        ],
        result: null,
      }),
    });
    const result = await run(
      Effect.gen(function* () {
        const state = yield* SessionState;
        const decided = yield* Effect.exit(state.decide(forger, { key: "k" }));
        const submitted = yield* Effect.exit(state.submitBatch([forged]));
        const emitted = yield* Effect.exit(state.appendAppDurable(forged));
        const logAfterForgery = yield* logOf;
        // Had the forged marker committed, this would replay its result without deciding.
        const post = yield* state.decide(postMessage, { key: "m1", text: "hello" });
        return { decided, submitted, emitted, logAfterForgery, post };
      }),
      { deciders: [postMessage, forger] },
    );
    expect(reasonOf(result.decided)).toBe("invalid_output");
    expect(reasonOf(result.submitted)).toBe("framework_event");
    expect(reasonOf(result.emitted)).toBe("framework_event");
    expect(result.logAfterForgery).toHaveLength(0);
    expect(result.post).toMatchObject({
      _tag: "Accepted",
      replayed: false,
      result: { routedNumber: 1 },
    });
  });

  const LooseConfigured = Schema.Struct({
    ...DurableEventEnvelope.fields,
    namespace: Schema.Literal(effectDurableAgentNamespace),
    type: Schema.Literal(sessionConfiguredEventType),
    payload: Schema.Unknown,
  });
  const importedWithUndefinedResult = {
    messageId: MessageId.make(sequentialUuidV7(62_001)),
    runId: sequentialUuidV7(62_002),
    turnId: sequentialUuidV7(62_003),
    inferenceId: sequentialUuidV7(62_004),
    promptParts: [
      Prompt.toolResultPart({ id: "t1", name: "noop", isFailure: false, result: undefined }),
    ],
  };
  const looseDecider = <E extends DurableEventEnvelope>(
    events: Schema.Codec<E, unknown, never, never>,
    draft: EDAEventDraft<E>,
  ) =>
    EDADecider.define({
      name: "test.Loose",
      input: Schema.Struct({ key: Schema.String }),
      events,
      result: Schema.Null,
      rejection: Schema.Never,
      idempotencyKey: (input) => input.key,
      decide: () => ({ _tag: "Accept", items: [draft], result: null }),
    });
  const configuredDraft = {
    namespace: effectDurableAgentNamespace,
    type: sessionConfiguredEventType,
    schemaVersion: schemaV1,
    payload: {},
  } as const;
  const importedDraft = {
    namespace: effectDurableAgentNamespace,
    type: assistantMessageImportedEventType,
    schemaVersion: schemaV1,
    payload: importedWithUndefinedResult,
  } as const;
  const noteDraft = {
    namespace: "test.chat",
    type: "NoteSet",
    schemaVersion: schemaV1,
    payload: { note: undefined },
  } as const;
  it.each([
    // Reviewer repro (gpt-6-astra, FR-1 review round 1): a loose decider schema let a malformed
    // SessionConfigured reach the store, which failed it as a store error and made the runtime fatal.
    {
      name: "a SessionConfigured its framework schema refuses",
      draft: configuredDraft,
      loose: looseDecider(LooseConfigured, configuredDraft),
    },
    // Reviewer repro (gpt-6-astra, FR-1 review round 2): the in-memory part passes the framework
    // schema, but its JSON drops the required `result`, so the store refused what admission passed.
    {
      name: "an imported tool result whose JSON loses its required result",
      draft: importedDraft,
      loose: looseDecider(AssistantMessageImportedEvent, importedDraft),
    },
    {
      name: "an app event whose JSON loses a required field",
      draft: noteDraft,
      loose: looseDecider(ChatEvents, noteDraft),
    },
  ])(
    "refuses $name as a draft, through submit and a tool's emit path",
    async ({ draft, loose }) => {
      const malformed = DurableEventEnvelope.make({
        ...draft,
        durability: "durable",
        eventId: EventId.make(sequentialUuidV7(61_000)),
        sessionId,
        createdAtMs: NOW,
      });
      const result = await run(
        Effect.gen(function* () {
          const state = yield* SessionState;
          const decided = yield* Effect.exit(state.decide(loose, { key: "k" }));
          const submitted = yield* Effect.exit(state.submitBatch([malformed]));
          const emitted = yield* Effect.exit(state.appendAppDurable(malformed));
          const usable = yield* followingSubmitSucceeds;
          return { decided, submitted, emitted, usable, log: yield* logOf };
        }),
        { deciders: [postMessage, loose] },
      );
      expect(reasonOf(result.decided)).toBe("invalid_output");
      expect(reasonOf(result.submitted)).toBe("invalid_event");
      expect(reasonOf(result.emitted)).toBe("invalid_event");
      expect(result.usable).toBe(true);
      expect(result.log.map((entry) => durableEventEnvelope(entry.event).type)).not.toContain(
        draft.type,
      );
    },
  );

  it("refuses a payload that is not JSON-native through submit and a tool's emit path", async () => {
    const result = await run(
      Effect.gen(function* () {
        const state = yield* SessionState;
        const redacted = yield* Effect.exit(
          state.submitBatch([routed({ email: Redacted.make("a@b.co") })]),
        );
        const option = yield* Effect.exit(state.submitBatch([routed(Option.some("hi"))]));
        const date = yield* Effect.exit(state.appendAppDurable(routed(new Date(NOW))));
        return { redacted, option, date, usable: yield* followingSubmitSucceeds };
      }),
    );
    expect(reasonOf(result.redacted)).toBe("payload_not_json");
    expect(reasonOf(result.option)).toBe("payload_not_json");
    expect(reasonOf(result.date)).toBe("payload_not_json");
    expect(result.usable).toBe(true);
  });

  it("refuses an over-limit draft as invalid_output and the same event through submit", async () => {
    const big = "x".repeat(maxAppEventBytes);
    const bigPost = EDADecider.define({
      name: "test.BigPost",
      input: Schema.Struct({ key: Schema.String }),
      events: ChatEvents,
      result: Schema.Null,
      rejection: Schema.Never,
      idempotencyKey: (input) => input.key,
      decide: () => ({
        _tag: "Accept",
        items: [
          {
            namespace: "test.chat",
            type: "MessageRouted",
            schemaVersion: 1,
            payload: { text: big },
          },
        ],
        result: null,
      }),
    });
    const result = await run(
      Effect.gen(function* () {
        const state = yield* SessionState;
        const decided = yield* Effect.exit(state.decide(bigPost, { key: "k" }));
        const submitted = yield* Effect.exit(state.submitBatch([routed(big)]));
        return { decided, submitted, usable: yield* followingSubmitSucceeds };
      }),
      { deciders: [bigPost] },
    );
    expect(reasonOf(result.decided)).toBe("invalid_output");
    expect(reasonOf(result.submitted)).toBe("event_too_large");
    expect(result.usable).toBe(true);
  });
});

describe("tools decide", () => {
  /** Ends the chat once per tool call, keyed by the call: a re-invoked call finds the marker. */
  const EndInput = Schema.Struct({ toolCallId: Schema.String });
  const endChat = EDADecider.define({
    name: "test.chat.End",
    input: EndInput,
    events: ChatEvents,
    result: Schema.Struct({ routedBeforeEnd: Schema.Number }),
    rejection: Schema.Struct({ reason: Schema.Literal("ended") }),
    idempotencyKey: (input) => input.toolCallId,
    decide: (view) => {
      const chat = getEDAReducerState(view.reducerStates, chatReducer);
      if (chat.ended) return { _tag: "Reject", rejection: { reason: "ended" } };
      return {
        _tag: "Accept",
        items: [{ namespace: "test.chat", type: "ChatEnded", schemaVersion: 1, payload: {} }],
        result: { routedBeforeEnd: chat.routed },
      };
    },
  });
  const EndTool = Tool.make("end_chat", {
    parameters: NoopParams,
    success: Schema.Unknown,
  }).annotate(ToolRecovery, "reinvoke");
  const toolkit: EDAModelToolkit = {
    tools: { end_chat: EndTool },
    handle: (() => Effect.die("handled by the registry shape")) as EDAModelToolkit["handle"],
  };
  const outcomes: Array<EDADeciderOutcome<unknown, unknown>> = [];
  const registry = (block: boolean): EDAToolRegistryShape => ({
    getModelToolkit: () => Effect.succeed(toolkit),
    getParamsSchema: () => Effect.succeed(NoopParams),
    execute: (_toolName, _params, context) =>
      Effect.gen(function* () {
        const before = yield* context.snapshot();
        const outcome = yield* context.decide(endChat, { toolCallId: context.toolCallId });
        outcomes.push(outcome);
        if (block) return yield* Effect.never;
        return { outcome, seenHead: before.state.lastSeq };
      }),
  });
  const toolCallThenText = () => [
    Stream.make(
      Response.makePart("tool-call", {
        id: "call-end",
        name: "end_chat",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    ),
    Stream.make(
      Response.makePart("text-delta", { id: "text-final", delta: "bye" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    ),
  ];
  const waitFor = (predicate: (committed: ReadonlyArray<CommittedDurableEvent>) => boolean) =>
    Effect.gen(function* () {
      const store = yield* EDASessionStore;
      for (let attempt = 0; attempt < 500; attempt += 1) {
        const committed = yield* collectCommitted(store);
        if (predicate(committed)) return committed;
        yield* Effect.yieldNow;
      }
      return yield* Effect.die(new Error("timed out waiting for durable events"));
    });
  const toolLayer = (
    block: boolean,
    ids: number,
    seedEvents?: ReadonlyArray<DurableEventEnvelope>,
  ) =>
    makeEdaTestLayer({
      sessionId,
      ids: Array.from({ length: 200 }, (_, index) => sequentialUuidV7(ids + index)),
      appEvents: ChatEvents,
      reducers: [chatReducer],
      deciders: [endChat],
      parts: block ? toolCallThenText().slice(0, 1) : toolCallThenText().slice(1),
      toolRegistry: registry(block),
      ...(seedEvents === undefined ? {} : { seedEvents }),
    });

  it("commits one marker for a call that is interrupted and re-invoked under its tool-call key", async () => {
    const crashed = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const state = yield* SessionState;
          yield* state.start({ modelSelection });
          yield* state.admitCommand(command);
          return yield* waitFor((committed) => markers(committed).length === 1);
        }),
      ).pipe(Effect.provide(toolLayer(true, 10_000))),
    );
    const recovered = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const state = yield* SessionState;
          yield* state.start({ modelSelection });
          return yield* waitFor(hasCommandCompleted(CommandId.make(command.commandId)));
        }),
      ).pipe(
        Effect.provide(
          toolLayer(
            false,
            20_000,
            crashed.map((entry) => durableEventEnvelope(entry.event)),
          ),
        ),
      ),
    );
    expect(outcomes.map((outcome) => outcome._tag === "Accepted" && outcome.replayed)).toEqual([
      false,
      true,
    ]);
    expect(markers(recovered)).toHaveLength(1);
    expect(recovered.filter((entry) => entry.event.type === "Foreign")).toHaveLength(1);
    const completed = recovered.find((entry) => entry.event.type === "ToolCallCompleted");
    expect(JSON.stringify(completed?.event.payload)).toContain('"replayed":true');
  });
});

describe("what the store keeps", () => {
  // The store keeps `JSON.parse(JSON.stringify(value))` (gpt-6-astra, FR-1 review round 2): what is
  // committed and returned must already be that form, or admission checked something else.
  const keys = fc.constantFrom("a", "b", "c");
  const { value: jsonish } = fc.letrec<{ value: unknown }>((tie) => ({
    value: fc.oneof(
      { depthSize: "small" },
      fc.constant(undefined),
      fc.constant(null),
      fc.boolean(),
      fc.integer(),
      fc.string({ maxLength: 4 }),
      fc.array(tie("value"), { maxLength: 3 }),
      fc.dictionary(keys, tie("value"), { maxKeys: 3, noNullPrototype: true }),
    ),
  }));
  const holed = fc.record(
    { optional: jsonish, required: jsonish },
    { requiredKeys: ["required"], noNullPrototype: true },
  );
  const Holed = Schema.Struct({
    optional: Schema.optional(Schema.Unknown),
    required: Schema.Unknown,
  });
  const ProbeEvent = Schema.Struct({
    ...DurableEventEnvelope.fields,
    namespace: Schema.Literal("test.probe"),
    type: Schema.Literal("Probed"),
    payload: Holed,
  });
  const jsonForm = (value: unknown): unknown => JSON.parse(JSON.stringify(value));
  const probe = EDADecider.define({
    name: "test.Probe",
    input: Schema.Struct({ key: Schema.String, result: Holed }),
    events: ProbeEvent,
    result: Holed,
    rejection: Schema.Never,
    idempotencyKey: (input) => input.key,
    decide: (_view, input) => ({
      _tag: "Accept",
      items: [],
      result: input.result,
    }),
  });
  const runProbe = <A, E>(program: Effect.Effect<A, E, SessionState | EDASessionStore>) =>
    Effect.runPromise(
      Effect.scoped(program).pipe(
        Effect.provide(makeEdaTestLayer({ sessionId, appEvents: ProbeEvent, deciders: [probe] })),
      ),
    );

  it("commits an app event only in its JSON form, which its registered schema decodes", async () => {
    await fc.assert(
      fc.asyncProperty(holed, async (payload) => {
        const result = await runProbe(
          Effect.gen(function* () {
            const state = yield* SessionState;
            const exit = yield* Effect.exit(
              state.submitBatch([
                DurableEventEnvelope.make({
                  namespace: "test.probe",
                  type: "Probed",
                  schemaVersion: schemaV1,
                  durability: "durable",
                  eventId: EventId.make(sequentialUuidV7(63_000)),
                  sessionId,
                  createdAtMs: NOW,
                  payload,
                }),
              ]),
            );
            return { exit, log: yield* logOf };
          }),
        );
        if (Exit.isFailure(result.exit)) {
          expect(hasEDASessionStoreError(result.exit.cause)).toBe(false);
          expect(result.log).toHaveLength(0);
          return;
        }
        const committed = result.exit.value.map((entry) => durableEventEnvelope(entry.event));
        for (const event of committed) {
          expect(event.payload).toStrictEqual(jsonForm(event.payload));
          expect(() => Schema.decodeUnknownSync(ProbeEvent)(jsonForm(event))).not.toThrow();
        }
      }),
      { numRuns: 100 },
    );
  });

  it("returns a decider's first result exactly as every replay returns it", async () => {
    await fc.assert(
      fc.asyncProperty(holed, async (value) => {
        const result = await runProbe(
          Effect.gen(function* () {
            const state = yield* SessionState;
            const first = yield* Effect.exit(state.decide(probe, { key: "k", result: value }));
            const retry = yield* Effect.exit(state.decide(probe, { key: "k", result: value }));
            return { first, retry, log: yield* logOf };
          }),
        );
        if (Exit.isFailure(result.first)) {
          expect(reasonOf(result.first)).toBe("invalid_output");
          expect(result.log).toHaveLength(0);
          return;
        }
        expect(result.first.value._tag === "Accepted" && result.first.value.result).toStrictEqual(
          result.first.value._tag === "Accepted" && jsonForm(result.first.value.result),
        );
        expect(Exit.isSuccess(result.retry) && result.retry.value).toStrictEqual({
          ...result.first.value,
          replayed: true,
        });
      }),
      { numRuns: 100 },
    );
  });
});
