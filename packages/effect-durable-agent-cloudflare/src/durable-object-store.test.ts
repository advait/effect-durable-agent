import { it as effectIt } from "@effect/vitest";
import { durableEventEnvelope } from "effect-durable-agent/types/events";
import { resolveTranscriptBody } from "effect-durable-agent/transcript";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { StopTurnCommand, SubmitMessageCommand } from "effect-durable-agent/types/commands";
import {
  InferenceId,
  CommandId,
  CompactionId,
  ContextVersion,
  EventId,
  MessageId,
  RunId,
  SequenceNumber,
  SessionId,
  SummaryId,
  ToolCallId,
  TurnId,
} from "effect-durable-agent/types/core";
import {
  CommandAdmittedEvent,
  CommandCancelledEvent,
  CommandStartedEvent,
  ContextRebasedEvent,
  DurableEventEnvelope,
  EventDedupeKey,
  EventNamespace,
  EventType,
  ProviderPartId,
  SummaryCreatedEvent,
  SystemMessageCommittedEvent,
  SystemPromptText,
  ToolCallCompletedEvent,
  ToolCallCreatedEvent,
  ToolName,
  UserMessageCommittedEvent,
  commandAdmittedEventType,
  commandCancelledEventType,
  commandStartedEventType,
  contextRebasedEventType,
  EDADurableEvent,
  effectDurableAgentNamespace,
  maxAppEventBytes,
  schemaV1,
  summaryCreatedEventType,
  systemMessageCommittedEventType,
  toolCallCompletedEventType,
  toolCallCreatedEventType,
  UnixEpochMillis,
  userMessageCommittedEventType,
} from "effect-durable-agent/types/events";
import {
  CommittedDurableEvent,
  EDASessionStore,
  hasEDASessionStoreError,
  registerAppEvents,
  type EDASessionStoreShape,
} from "effect-durable-agent/services/session-store";
import { EDAReducer, EDAReducerRegistry } from "effect-durable-agent/services/reducer-registry";
import { SessionEventObserver } from "effect-durable-agent/services/session-event-observer";
import { EDADecider, EDADeciderError } from "effect-durable-agent/services/decider";
import {
  SessionCommandAdmissionConflict,
  SessionState,
} from "effect-durable-agent/services/session-state";
import { makeEdaTestLayer } from "effect-durable-agent/testkit/layers";
import { testDefinition } from "effect-durable-agent/testkit/transcript";
import {
  CompactionExecutorId,
  CompactionPolicyId,
  CompactionSummaryArtifact,
} from "effect-durable-agent/domain/context-projection";
import {
  decodeReducedStateCheckpoint,
  encodeReducedStateCheckpoint,
  foldReducedState,
  frameworkReducedStateReducerName,
  frameworkReducedStateReducerSchemaVersion,
  initialReducedState,
  reducedStateCheckpointEventSeqs,
} from "effect-durable-agent/domain/reduced-state";
import {
  durableObjectSerializedJsonHardCapBytes,
  DurableObjectSessionStore,
  readDurableObjectCommittedEvent,
  readDurableObjectSummary,
} from "./durable-object-store";
import { sequentialUuidV7 } from "effect-durable-agent/services/id-generator";
import type {
  DurableObjectSessionStorage,
  DurableObjectSqlCursor,
  DurableObjectSqlStorage,
} from "./durable-object-storage";
import { SqliteDurableObjectStorage } from "./testing/sqlite-durable-object-storage";

const SESSION_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const SESSION_ID_TYPED = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const OTHER_SESSION_ID = "018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a";
const EVENT_ID_A = "018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a";
const EVENT_ID_B = "018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a";
const EVENT_ID_C = "018f6bd5-2f2a-7b1e-8f5a-1f2e3d4c5b6a";
const EVENT_ID_D = "018f6bd5-2f2a-7b1e-8f8a-1f2e3d4c5b6a";
const MESSAGE_ID_A = "018f6bd5-2f2a-7b1e-8f9a-1f2e3d4c5b6a";
const MESSAGE_ID_B = "018f6bd5-2f2a-7b1e-8f9d-1f2e3d4c5b6a";
const COMMAND_ID_A = "018f6bd5-2f2a-7b1e-8f2a-1f2e3d4c5b6a";
const COMMAND_ID_B = "018f6bd5-2f2a-7b1e-8f3a-1f2e3d4c5b6a";
const COMMAND_ID_C = "018f6bd5-2f2a-7b1e-8f4a-1f2e3d4c5b6a";
const COMPACTION_ID = "018f6bd5-2f2a-7b1e-8f6a-1f2e3d4c5b6a";
const SUMMARY_ID = "018f6bd5-2f2a-7b1e-8f7a-1f2e3d4c5b6a";
const TOOL_CALL_ID = "018f6bd5-2f2a-7b1e-8f8b-1f2e3d4c5b6a";
const PROVIDER_PART_ID = "tool-call-1";

interface EDASessionStoreContract {
  readonly name: string;
  readonly makeStore: (sessionId: SessionId) => Effect.Effect<EDASessionStoreShape>;
  readonly sharesStorageAcrossSessions?: boolean;
}

const commandAdmitted = ({
  commandId = COMMAND_ID_A,
  eventId = EVENT_ID_A,
  sessionId = SESSION_ID,
}: {
  readonly commandId?: string;
  readonly eventId?: string;
  readonly sessionId?: string;
} = {}) =>
  CommandAdmittedEvent.make({
    namespace: effectDurableAgentNamespace,
    type: commandAdmittedEventType,
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(eventId),
    sessionId: SessionId.make(sessionId),
    createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
    payload: {
      command: new StopTurnCommand({ commandId: CommandId.make(commandId) }),
    },
  });

const commandStarted = ({
  commandId = COMMAND_ID_A,
  eventId = sequentialUuidV7(101),
}: {
  readonly commandId?: string;
  readonly eventId?: string;
} = {}) =>
  CommandStartedEvent.make({
    namespace: effectDurableAgentNamespace,
    type: commandStartedEventType,
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(eventId),
    sessionId: SessionId.make(SESSION_ID),
    createdAtMs: UnixEpochMillis.make(1_715_000_000_010),
    payload: { commandId: CommandId.make(commandId) },
  });

const commandCancelled = ({
  commandId = COMMAND_ID_A,
  eventId = sequentialUuidV7(102),
}: {
  readonly commandId?: string;
  readonly eventId?: string;
} = {}) =>
  CommandCancelledEvent.make({
    namespace: effectDurableAgentNamespace,
    type: commandCancelledEventType,
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(eventId),
    sessionId: SessionId.make(SESSION_ID),
    createdAtMs: UnixEpochMillis.make(1_715_000_000_011),
    payload: { commandId: CommandId.make(commandId), reason: "test" },
  });

const toolCallCompletedWithResult = (result: unknown) =>
  ToolCallCompletedEvent.make({
    namespace: effectDurableAgentNamespace,
    type: toolCallCompletedEventType,
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(sequentialUuidV7(109)),
    sessionId: SessionId.make(SESSION_ID),
    createdAtMs: UnixEpochMillis.make(1_715_000_000_018),
    payload: {
      toolCallId: ToolCallId.make(TOOL_CALL_ID),
      promptPart: Prompt.toolResultPart({
        id: ProviderPartId.make(PROVIDER_PART_ID),
        name: ToolName.make("noop"),
        isFailure: false,
        providerExecuted: false,
        result,
      }),
    },
  });

const systemMessageCommitted = () =>
  SystemMessageCommittedEvent.make({
    namespace: effectDurableAgentNamespace,
    type: systemMessageCommittedEventType,
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(EVENT_ID_D),
    sessionId: SessionId.make(SESSION_ID),
    createdAtMs: UnixEpochMillis.make(1_715_000_000_003),
    payload: {
      messageId: MessageId.make(MESSAGE_ID_A),
      content: SystemPromptText.make("System prompt"),
    },
  });

const userMessageCommitted = (content: string) =>
  UserMessageCommittedEvent.make({
    namespace: effectDurableAgentNamespace,
    type: userMessageCommittedEventType,
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(EVENT_ID_A),
    sessionId: SessionId.make(SESSION_ID),
    createdAtMs: UnixEpochMillis.make(1_715_000_000_004),
    payload: {
      commandId: CommandId.make(COMMAND_ID_A),
      messageId: MessageId.make(MESSAGE_ID_B),
      content: [Prompt.textPart({ text: content })],
    },
  });

const summaryArtifact = () =>
  CompactionSummaryArtifact.make({
    compactionId: CompactionId.make(COMPACTION_ID),
    summaryId: SummaryId.make(SUMMARY_ID),
    sourceFromSeq: SequenceNumber.make(1),
    sourceToSeq: SequenceNumber.make(2),
    retainedFromContextSeq: SequenceNumber.make(3),
    text: "Summary text",
    promptMessage: Prompt.makeMessage("user", {
      content: [Prompt.textPart({ text: "Summary text" })],
    }),
    policyId: CompactionPolicyId.make("test.policy"),
    executorId: CompactionExecutorId.make("test.executor"),
  });

const summaryCreated = () => {
  const summary = summaryArtifact();
  return SummaryCreatedEvent.make({
    namespace: effectDurableAgentNamespace,
    type: summaryCreatedEventType,
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(EVENT_ID_B),
    sessionId: SessionId.make(SESSION_ID),
    createdAtMs: UnixEpochMillis.make(1_715_000_000_001),
    payload: {
      compactionId: summary.compactionId,
      summaryId: summary.summaryId,
      sourceFromSeq: summary.sourceFromSeq,
      sourceToSeq: summary.sourceToSeq,
      summary,
    },
  });
};

const contextRebased = () =>
  ContextRebasedEvent.make({
    namespace: effectDurableAgentNamespace,
    type: contextRebasedEventType,
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(EVENT_ID_C),
    sessionId: SessionId.make(SESSION_ID),
    createdAtMs: UnixEpochMillis.make(1_715_000_000_002),
    payload: {
      compactionId: CompactionId.make(COMPACTION_ID),
      summaryId: SummaryId.make(SUMMARY_ID),
      contextVersion: ContextVersion.make(1),
      retainedFromContextSeq: SequenceNumber.make(3),
    },
  });

const summaryCreatedFor = (summary: CompactionSummaryArtifact, index: number) =>
  SummaryCreatedEvent.make({
    namespace: effectDurableAgentNamespace,
    type: summaryCreatedEventType,
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(sequentialUuidV7(3_000 + index)),
    sessionId: SessionId.make(SESSION_ID),
    createdAtMs: UnixEpochMillis.make(1_715_000_010_000 + index),
    payload: {
      compactionId: summary.compactionId,
      summaryId: summary.summaryId,
      sourceFromSeq: summary.sourceFromSeq,
      sourceToSeq: summary.sourceToSeq,
      summary,
    },
  });

const replayPageAppEvent = (index: number) =>
  DurableEventEnvelope.make({
    namespace: EventNamespace.make("test-app"),
    type: EventType.make("ReplayPageAppEvent"),
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(sequentialUuidV7(9_000 + index)),
    sessionId: SessionId.make(SESSION_ID),
    createdAtMs: UnixEpochMillis.make(1_715_000_020_000 + index),
    payload: { index },
  });

const keyedFact = ({
  eventId,
  dedupeKey,
  namespace = "test.dedupe",
  type = "FactObserved",
  n,
}: {
  readonly eventId: string;
  readonly dedupeKey?: string;
  readonly namespace?: string;
  readonly type?: string;
  readonly n: number;
}) =>
  DurableEventEnvelope.make({
    namespace: EventNamespace.make(namespace),
    type: EventType.make(type),
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(eventId),
    sessionId: SessionId.make(SESSION_ID),
    createdAtMs: UnixEpochMillis.make(1_715_000_030_000 + n),
    payload: { n },
    ...(dedupeKey === undefined ? {} : { dedupeKey: EventDedupeKey.make(dedupeKey) }),
  });

/** Replay as `[eventId, n, dedupeKey]`, the envelope fields the dedupe rules decide on. */
const replayFacts = (store: EDASessionStoreShape) =>
  collect(store).pipe(
    Effect.map((events) =>
      events.map((entry) => {
        const envelope = durableEventEnvelope(entry.event);
        return [envelope.eventId, (envelope.payload as { n: number }).n, envelope.dedupeKey];
      }),
    ),
  );

const appendMany = (
  store: EDASessionStoreShape,
  events: ReadonlyArray<EDADurableEvent>,
): Effect.Effect<ReadonlyArray<CommittedDurableEvent>, unknown> =>
  store.append({ entries: events.map((event) => ({ event })) });

const appendOne = (store: EDASessionStoreShape, event: EDADurableEvent) =>
  Effect.map(appendMany(store, [event]), (events) => events[0] as CommittedDurableEvent);

const collect = (store: EDASessionStoreShape, afterSeq: SequenceNumber = SequenceNumber.make(0)) =>
  store.eventsAfter(afterSeq).pipe(
    Stream.runCollect,
    Effect.map((events) => Array.from(events)),
  );

const lastCommittedSeq = (store: EDASessionStoreShape) =>
  collect(store).pipe(
    Effect.map((events) => events.at(-1)?.position.seq ?? SequenceNumber.make(0)),
  );

const encodeDurableEvents = (events: ReadonlyArray<CommittedDurableEvent>) =>
  events.map((entry) => Schema.encodeSync(EDADurableEvent)(entry.event));

const encodeDurableEvent = (event: EDADurableEvent) => Schema.encodeSync(EDADurableEvent)(event);

const expectFailure = <A, E>(exit: Exit.Exit<A, E>, message: string) => {
  expect(Exit.isFailure(exit)).toBe(true);
  expect(Exit.isFailure(exit) ? String(exit.cause) : "").toContain(message);
};

const runInMemory = <A>(effect: Effect.Effect<A, never, EDASessionStore>) =>
  Effect.runSync(effect.pipe(Effect.provide(EDASessionStore.InMemory(SessionId.make(SESSION_ID)))));

const runEDASessionStoreContract = (contract: EDASessionStoreContract) => {
  describe(`${contract.name} EDASessionStore contract`, () => {
    it("starts with head 0 and assigns strictly increasing durable positions", () => {
      const firstEvent = commandAdmitted();
      const secondEvent = commandAdmitted({ commandId: COMMAND_ID_B, eventId: EVENT_ID_B });
      const result = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          const initialHead = yield* lastCommittedSeq(store);
          const first = yield* appendOne(store, firstEvent);
          const second = yield* appendOne(store, secondEvent);
          const replay = yield* collect(store);
          const afterFirst = yield* collect(store, first.position.seq);
          const head = yield* lastCommittedSeq(store);
          return { initialHead, first, second, replay, afterFirst, head };
        }),
      );

      expect(result.initialHead).toBe(0);
      expect(result.first.position).toEqual({ seq: 1, subSeq: 0 });
      expect(result.second.position).toEqual({ seq: 2, subSeq: 0 });
      expect(result.head).toBe(2);
      expect(Schema.is(CommittedDurableEvent)(result.first)).toBe(true);
      expect(result.replay.map((entry) => entry.event.eventId)).toEqual([EVENT_ID_A, EVENT_ID_B]);
      expect(result.afterFirst.map((entry) => entry.event.eventId)).toEqual([EVENT_ID_B]);
    });

    it("loads committed events by exact sequence for checkpoint pointer hydration", () => {
      const result = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          const first = yield* appendOne(store, commandAdmitted());
          yield* appendOne(store, commandStarted());
          const third = yield* appendOne(store, commandCancelled());
          return yield* store.loadCommittedEventsBySeq([
            third.position.seq,
            first.position.seq,
            third.position.seq,
          ]);
        }),
      );

      expect(result.map((entry) => entry.position.seq)).toEqual([
        SequenceNumber.make(1),
        SequenceNumber.make(3),
      ]);
      expect(result.map((entry) => entry.event.type)).toEqual([
        "CommandAdmitted",
        "CommandCancelled",
      ]);
    });

    it("returns existing positions for duplicate event ids without advancing head", () => {
      const firstEvent = commandAdmitted();
      const secondEvent = commandAdmitted({ commandId: COMMAND_ID_B, eventId: EVENT_ID_B });
      const result = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          const first = yield* appendOne(store, firstEvent);
          const afterFirstHead = yield* lastCommittedSeq(store);
          const duplicate = yield* appendOne(store, firstEvent);
          const afterDuplicateHead = yield* lastCommittedSeq(store);
          const second = yield* appendOne(store, secondEvent);
          const afterSecondHead = yield* lastCommittedSeq(store);
          const replay = yield* collect(store);
          return {
            first,
            afterFirstHead,
            duplicate,
            afterDuplicateHead,
            second,
            afterSecondHead,
            replay,
          };
        }),
      );

      expect(result.first.position).toEqual({ seq: 1, subSeq: 0 });
      expect(result.afterFirstHead).toBe(1);
      expect(result.duplicate.position).toEqual(result.first.position);
      expect(result.afterDuplicateHead).toBe(1);
      expect(result.second.position).toEqual({ seq: 2, subSeq: 0 });
      expect(result.afterSecondHead).toBe(2);
      expect(result.replay.map((entry) => entry.event.eventId)).toEqual([EVENT_ID_A, EVENT_ID_B]);
    });

    it("collapses an entry whose namespace, type and dedupe key are already committed", () => {
      const result = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          const first = yield* store.append({
            entries: [{ event: keyedFact({ eventId: EVENT_ID_A, dedupeKey: "k", n: 1 }) }],
          });
          const repeat = yield* store.append({
            entries: [{ event: keyedFact({ eventId: EVENT_ID_B, dedupeKey: "k", n: 2 }) }],
          });
          return {
            first,
            repeat,
            head: yield* lastCommittedSeq(store),
            log: yield* replayFacts(store),
          };
        }),
      );

      expect(result.repeat).toEqual(result.first);
      expect(durableEventEnvelope(result.repeat[0]!.event).eventId).toBe(EVENT_ID_A);
      expect(result.head).toBe(1);
      expect(result.log).toEqual([[EVENT_ID_A, 1, "k"]]);
    });

    it("scopes a dedupe key to its namespace and type", () => {
      const log = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          yield* store.append({
            entries: [
              { event: keyedFact({ eventId: EVENT_ID_A, dedupeKey: "k", n: 1 }) },
              { event: keyedFact({ eventId: EVENT_ID_B, dedupeKey: "k", type: "Other", n: 2 }) },
              {
                event: keyedFact({ eventId: EVENT_ID_C, dedupeKey: "k", namespace: "other", n: 3 }),
              },
            ],
          });
          return yield* replayFacts(store);
        }),
      );

      expect(log).toEqual([
        [EVENT_ID_A, 1, "k"],
        [EVENT_ID_B, 2, "k"],
        [EVENT_ID_C, 3, "k"],
      ]);
    });

    it("collapses a dedupe key repeated within one batch onto the first entry", () => {
      const result = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          const returned = yield* store.append({
            entries: [
              { event: keyedFact({ eventId: EVENT_ID_A, dedupeKey: "k", n: 1 }) },
              { event: keyedFact({ eventId: EVENT_ID_B, dedupeKey: "k", n: 2 }) },
              { event: keyedFact({ eventId: EVENT_ID_C, n: 3 }) },
            ],
          });
          return { returned, log: yield* replayFacts(store) };
        }),
      );

      expect(result.returned.map((entry) => entry.position.seq)).toEqual([1, 1, 2]);
      expect(result.log).toEqual([
        [EVENT_ID_A, 1, "k"],
        [EVENT_ID_C, 3, undefined],
      ]);
    });

    it("resolves an entry by eventId before its dedupe key", () => {
      const returned = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          yield* store.append({
            entries: [
              { event: keyedFact({ eventId: EVENT_ID_A, dedupeKey: "a", n: 1 }) },
              { event: keyedFact({ eventId: EVENT_ID_B, dedupeKey: "b", n: 2 }) },
            ],
          });
          return yield* store.append({
            entries: [{ event: keyedFact({ eventId: EVENT_ID_A, dedupeKey: "b", n: 3 }) }],
          });
        }),
      );

      expect(returned.map((entry) => entry.position.seq)).toEqual([1]);
      expect(durableEventEnvelope(returned[0]!.event).dedupeKey).toBe("a");
    });

    it("finds the first event committed for a dedupe key", () => {
      const result = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          yield* store.append({
            entries: [
              { event: keyedFact({ eventId: EVENT_ID_A, n: 1 }) },
              { event: keyedFact({ eventId: EVENT_ID_B, dedupeKey: "k", n: 2 }) },
              { event: keyedFact({ eventId: EVENT_ID_C, dedupeKey: "k", n: 3 }) },
            ],
          });
          const lookup = (namespace: string, type: string) =>
            store.findEventByDedupeKey({
              namespace: EventNamespace.make(namespace),
              type: EventType.make(type),
              dedupeKey: EventDedupeKey.make("k"),
            });
          return {
            found: yield* lookup("test.dedupe", "FactObserved"),
            otherType: yield* lookup("test.dedupe", "Other"),
            otherNamespace: yield* lookup("other", "FactObserved"),
          };
        }),
      );

      expect(result.found?.position.seq).toBe(2);
      expect(durableEventEnvelope(result.found!.event).eventId).toBe(EVENT_ID_B);
      expect(result.otherType).toBeUndefined();
      expect(result.otherNamespace).toBeUndefined();
    });

    it("stores summaries as loadable sidecar artifacts", () => {
      const result = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          const before = yield* store.loadSummaryArtifact(SummaryId.make(SUMMARY_ID));
          const summary = summaryCreated();
          yield* store.append({ entries: [{ event: summary }] });
          const afterSummary = yield* store.loadSummaryArtifact(SummaryId.make(SUMMARY_ID));
          yield* store.append({ entries: [{ event: contextRebased() }] });
          const afterRebase = yield* store.loadSummaryArtifact(SummaryId.make(SUMMARY_ID));
          return { before, afterSummary, afterRebase };
        }),
      );

      expect(result.before).toBeUndefined();
      expect(result.afterSummary).toMatchObject({
        summaryId: SummaryId.make(SUMMARY_ID),
        retainedFromContextSeq: SequenceNumber.make(3),
        text: "Summary text",
      });
      expect(result.afterRebase).toEqual(result.afterSummary);
    });

    it("overwrites reducer checkpoint snapshots by reducer name", () => {
      const result = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          yield* store.saveReducerCheckpoints([
            {
              name: "app.alpha",
              schemaVersion: 1,
              throughSeq: SequenceNumber.make(1),
              payload: { value: "old-alpha" },
              updatedAtMs: 1_715_000_000_000,
            },
            {
              name: "app.beta",
              schemaVersion: 1,
              throughSeq: SequenceNumber.make(1),
              payload: { value: "old-beta" },
              updatedAtMs: 1_715_000_000_000,
            },
          ]);
          yield* store.saveReducerCheckpoints([
            {
              name: "app.alpha",
              schemaVersion: 1,
              throughSeq: SequenceNumber.make(2),
              payload: { value: "new-alpha" },
              updatedAtMs: 1_715_000_000_001,
            },
            {
              name: "app.beta",
              schemaVersion: 1,
              throughSeq: SequenceNumber.make(2),
              payload: { value: "new-beta" },
              updatedAtMs: 1_715_000_000_001,
            },
          ]);
          const alpha = yield* store.loadReducerCheckpoint("app.alpha");
          const beta = yield* store.loadReducerCheckpoint("app.beta");
          return { alpha, beta };
        }),
      );

      expect(result.alpha).toMatchObject({
        name: "app.alpha",
        throughSeq: SequenceNumber.make(2),
        payload: { value: "new-alpha" },
      });
      expect(result.beta).toMatchObject({
        name: "app.beta",
        throughSeq: SequenceNumber.make(2),
        payload: { value: "new-beta" },
      });
    });

    it("rejects SummaryCreated without a summary payload", () => {
      const invalid = DurableEventEnvelope.make({
        namespace: effectDurableAgentNamespace,
        type: summaryCreatedEventType,
        schemaVersion: schemaV1,
        durability: "durable",
        eventId: EventId.make(EVENT_ID_B),
        sessionId: SessionId.make(SESSION_ID),
        createdAtMs: UnixEpochMillis.make(1_715_000_000_001),
        payload: {
          compactionId: CompactionId.make(COMPACTION_ID),
          summaryId: SummaryId.make(SUMMARY_ID),
          sourceFromSeq: SequenceNumber.make(1),
          sourceToSeq: SequenceNumber.make(2),
        },
      });
      const exit = Effect.runSyncExit(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          return yield* store.append({ entries: [{ event: invalid }] });
        }),
      );

      expectFailure(exit, "summary");
    });

    it("commits batches atomically, preserving input order and duplicate positions", () => {
      const firstEvent = commandAdmitted();
      const secondEvent = commandAdmitted({ commandId: COMMAND_ID_B, eventId: EVENT_ID_B });
      const result = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          const empty = yield* appendMany(store, []);
          const headAfterEmpty = yield* lastCommittedSeq(store);
          const committed = yield* appendMany(store, [firstEvent, secondEvent, firstEvent]);
          const replay = yield* collect(store);
          const head = yield* lastCommittedSeq(store);
          return { empty, headAfterEmpty, committed, replay, head };
        }),
      );

      expect(result.empty).toEqual([]);
      expect(result.headAfterEmpty).toBe(0);
      expect(result.committed.map((entry) => entry.position)).toEqual([
        { seq: 1, subSeq: 0 },
        { seq: 2, subSeq: 0 },
        { seq: 1, subSeq: 0 },
      ]);
      expect(result.head).toBe(2);
      expect(result.replay.map((entry) => entry.event.eventId)).toEqual([EVENT_ID_A, EVENT_ID_B]);
    });

    it("preserves encoded durable event payloads through commit and replay", () => {
      const firstEvent = commandAdmitted();
      const secondEvent = commandAdmitted({ commandId: COMMAND_ID_B, eventId: EVENT_ID_B });
      const result = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          const first = yield* appendOne(store, firstEvent);
          const committed = yield* appendMany(store, [secondEvent]);
          const replay = yield* collect(store);
          const afterHead = yield* collect(store, 2);
          return { first, committed, replay, afterHead };
        }),
      );

      expect(encodeDurableEvent(result.first.event)).toEqual(encodeDurableEvent(firstEvent));
      expect(encodeDurableEvents(result.committed)).toEqual([encodeDurableEvent(secondEvent)]);
      expect(encodeDurableEvents(result.replay)).toEqual([
        encodeDurableEvent(firstEvent),
        encodeDurableEvent(secondEvent),
      ]);
      expect(result.afterHead).toEqual([]);
    });

    it("reuses existing positions when a later batch mixes duplicates and new events", () => {
      const firstEvent = commandAdmitted();
      const secondEvent = commandAdmitted({ commandId: COMMAND_ID_B, eventId: EVENT_ID_B });
      const result = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          const first = yield* appendOne(store, firstEvent);
          const batch = yield* appendMany(store, [firstEvent, secondEvent]);
          const head = yield* lastCommittedSeq(store);
          const replay = yield* collect(store);
          return { first, batch, head, replay };
        }),
      );

      expect(result.batch.map((entry) => entry.position)).toEqual([
        result.first.position,
        { seq: 2, subSeq: 0 },
      ]);
      expect(result.head).toBe(2);
      expect(result.replay.map((entry) => entry.event.eventId)).toEqual([EVENT_ID_A, EVENT_ID_B]);
    });

    it("rejects cross-session events without partially committing the batch", () => {
      const firstEvent = commandAdmitted();
      const otherSessionEvent = commandAdmitted({
        commandId: COMMAND_ID_B,
        eventId: EVENT_ID_B,
        sessionId: OTHER_SESSION_ID,
      });
      const thirdEvent = commandAdmitted({ commandId: COMMAND_ID_C, eventId: EVENT_ID_C });
      const result = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          const exit = yield* Effect.exit(
            appendMany(store, [firstEvent, otherSessionEvent, thirdEvent]),
          );
          const replay = yield* collect(store);
          const head = yield* lastCommittedSeq(store);
          return { exit, replay, head };
        }),
      );

      expectFailure(result.exit, "scoped to session");
      expect(result.replay).toEqual([]);
      expect(result.head).toBe(0);
    });

    it("preserves prior state when single commits or later batches fail", () => {
      const firstEvent = commandAdmitted();
      const secondEvent = commandAdmitted({ commandId: COMMAND_ID_B, eventId: EVENT_ID_B });
      const otherSessionEvent = commandAdmitted({
        commandId: COMMAND_ID_C,
        eventId: EVENT_ID_C,
        sessionId: OTHER_SESSION_ID,
      });
      const result = Effect.runSync(
        Effect.gen(function* () {
          const store = yield* contract.makeStore(SessionId.make(SESSION_ID));
          const first = yield* appendOne(store, firstEvent);
          const failedCommit = yield* Effect.exit(appendOne(store, otherSessionEvent));
          const afterFailedCommit = yield* collect(store);
          const headAfterFailedCommit = yield* lastCommittedSeq(store);
          const failedBatch = yield* Effect.exit(
            appendMany(store, [secondEvent, otherSessionEvent]),
          );
          const afterFailedBatch = yield* collect(store);
          const headAfterFailedBatch = yield* lastCommittedSeq(store);
          return {
            first,
            failedCommit,
            afterFailedCommit,
            headAfterFailedCommit,
            failedBatch,
            afterFailedBatch,
            headAfterFailedBatch,
          };
        }),
      );

      expectFailure(result.failedCommit, "scoped to session");
      expectFailure(result.failedBatch, "scoped to session");
      expect(result.afterFailedCommit).toEqual([result.first]);
      expect(result.afterFailedBatch).toEqual([result.first]);
      expect(result.headAfterFailedCommit).toBe(1);
      expect(result.headAfterFailedBatch).toBe(1);
    });

    it("does not leak events across scoped stores that support multiple backing sessions", () => {
      if (contract.sharesStorageAcrossSessions === true) {
        return;
      }
      const firstEvent = commandAdmitted();
      const otherEvent = commandAdmitted({
        commandId: COMMAND_ID_B,
        eventId: EVENT_ID_B,
        sessionId: OTHER_SESSION_ID,
      });

      const result = Effect.runSync(
        Effect.gen(function* () {
          const firstStore = yield* contract.makeStore(SessionId.make(SESSION_ID));
          const otherStore = yield* contract.makeStore(SessionId.make(OTHER_SESSION_ID));
          const first = yield* appendOne(firstStore, firstEvent);
          const other = yield* appendOne(otherStore, otherEvent);
          const firstReplay = yield* collect(firstStore);
          const otherReplay = yield* collect(otherStore);
          const firstHead = yield* lastCommittedSeq(firstStore);
          const otherHead = yield* lastCommittedSeq(otherStore);
          return { first, other, firstReplay, otherReplay, firstHead, otherHead };
        }),
      );

      expect(result.firstReplay.map((entry) => entry.event.sessionId)).toEqual([
        SessionId.make(SESSION_ID),
      ]);
      expect(result.otherReplay.map((entry) => entry.event.sessionId)).toEqual([
        SessionId.make(OTHER_SESSION_ID),
      ]);
      expect(result.firstHead).toBe(result.first.position.seq);
      expect(result.otherHead).toBe(result.other.position.seq);
    });
  });
};

describe("EDASessionStore", () => {
  effectIt.effect(
    "replays retained event rows without changing their bytes or envelope encoding",
    () =>
      Effect.gen(function* () {
        const fakeSql = makeFakeDurableObjectSql();
        const persisted = [
          {
            seq: 1,
            event_id: EVENT_ID_A,
            namespace: "effect-durable-agent",
            type: "TurnCompleted",
            schema_version: 1,
            created_at_ms: 1715000000000,
            trace_json:
              '{"span":{"traceId":"0123456789abcdef0123456789abcdef","spanId":"0123456789abcdef","sampled":true,"tracestate":null},"links":[]}',
            fact_json:
              '{"runId":"018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a","turnId":"018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a","usage":{"inputTokens":3},"retainedExtra":"unchanged"}',
          },
          {
            seq: 2,
            event_id: EVENT_ID_B,
            namespace: "retained.external",
            type: "TurnCompleted",
            schema_version: 1,
            created_at_ms: 1715000000001,
            trace_json:
              '{"span":{"traceId":"0123456789abcdef0123456789abcdef","spanId":"0123456789abcdef","sampled":true,"tracestate":null},"links":[]}',
            fact_json: '{"opaque":"foreign tag collision"}',
          },
        ];
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        fakeSql.eventRows.push(...persisted);
        const before = JSON.stringify(fakeSql.eventRows);
        const replay = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
        expect(JSON.stringify(fakeSql.eventRows)).toBe(before);
        expect(replay[0]?.event.type).toBe("TurnCompleted");
        expect(replay[1]?.event.type).toBe("Foreign");
        for (const [index, entry] of replay.entries()) {
          const row = persisted[index];
          if (row === undefined) return yield* Effect.die(new Error("Missing persisted fixture"));
          const original = DurableEventEnvelope.make({
            namespace: row.namespace,
            type: row.type,
            schemaVersion: row.schema_version,
            durability: "durable",
            eventId: EventId.make(row.event_id),
            sessionId: SessionId.make(SESSION_ID),
            createdAtMs: UnixEpochMillis.make(row.created_at_ms),
            trace: JSON.parse(row.trace_json),
            payload: JSON.parse(row.fact_json),
          });
          expect(durableEventEnvelope(entry.event)).toEqual(original);
          expect(Schema.encodeSync(CommittedDurableEvent)(entry)).toEqual({
            position: entry.position,
            event: Schema.encodeSync(DurableEventEnvelope)(original),
          });
        }
      }),
  );

  runEDASessionStoreContract({
    name: "InMemory",
    makeStore: (sessionId) =>
      Effect.gen(function* () {
        return yield* EDASessionStore;
      }).pipe(Effect.provide(EDASessionStore.InMemory(sessionId))),
  });

  runEDASessionStoreContract({
    name: "DurableObjectSessionStore",
    sharesStorageAcrossSessions: true,
    makeStore: (sessionId) => {
      const fakeSql = makeFakeDurableObjectSql();
      return DurableObjectSessionStore.make({ sessionId, storage: fakeSql.storage });
    },
  });

  runEDASessionStoreContract({
    name: "DurableObjectSessionStore on SQLite",
    sharesStorageAcrossSessions: true,
    makeStore: (sessionId) =>
      DurableObjectSessionStore.make({ sessionId, storage: new SqliteDurableObjectStorage() }),
  });

  it("writes no sidecar or transcript rows for a collapsed entry", () => {
    const storage = new SqliteDurableObjectStorage();
    const tableRowCounts = () =>
      Object.fromEntries(
        storage
          .rows<{ name: string }>(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '_eda_%' ESCAPE '\\'",
          )
          .map(({ name }) => [
            name,
            storage.rows<{ count: number }>(`SELECT COUNT(*) AS count FROM ${name}`)[0]?.count,
          ]),
      );
    const keyedMessage = (eventId: string, messageId: string, text: string) =>
      UserMessageCommittedEvent.make({
        ...userMessageCommitted(text),
        eventId: EventId.make(eventId),
        payload: { ...userMessageCommitted(text).payload, messageId: MessageId.make(messageId) },
        dedupeKey: EventDedupeKey.make("delivery-1"),
      });
    const result = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage,
          transcript: { definitions: { current: testDefinition() } },
        });
        const first = yield* appendOne(store, keyedMessage(EVENT_ID_A, MESSAGE_ID_A, "first"));
        const before = tableRowCounts();
        const repeat = yield* appendOne(store, keyedMessage(EVENT_ID_B, MESSAGE_ID_B, "second"));
        return { first, repeat, before, after: tableRowCounts() };
      }).pipe(Effect.ensuring(Effect.sync(() => storage.db.close()))),
    );

    expect(result.repeat).toEqual(result.first);
    expect(result.before["_eda_context_messages"]).toBe(1);
    expect(Object.keys(result.before).some((name) => name.includes("transcript"))).toBe(true);
    expect(result.after).toEqual(result.before);
  });

  // Migration probes from Claude Opus 5.5 (adversary/fr1-fr5-interrogate@b2c303a,
  // adversary-fr5-migration.test.ts) and gpt-6-astra (review/docs-chat-frs-adversarial@121ac4d).
  it("migrates a schema-3 log to dedupe keys without changing its events", () => {
    const storage = new SqliteDurableObjectStorage();
    const open = () => DurableObjectSessionStore.make({ sessionId: SESSION_ID_TYPED, storage });
    const result = Effect.runSync(
      Effect.gen(function* () {
        const original = yield* open();
        yield* appendMany(original, [
          systemMessageCommitted(),
          userMessageCommitted("hello"),
          commandAdmitted({ eventId: EVENT_ID_B }),
        ]);
        yield* original.append({ entries: [{ event: keyedFact({ eventId: EVENT_ID_C, n: 1 }) }] });
        const encodeLog = (log: ReadonlyArray<CommittedDurableEvent>) =>
          log.map((entry) => Schema.encodeSync(CommittedDurableEvent)(entry));
        const events = encodeLog(yield* collect(original));
        // Return the database to schema 3: 0.1.0 never had the column or the index.
        storage.transactionSync(() => {
          storage.sql.exec("DROP INDEX _eda_event_log_dedupe_key_idx");
          storage.sql.exec("ALTER TABLE _eda_event_log DROP COLUMN dedupe_key");
          storage.sql.exec("DELETE FROM _eda_schema_migrations WHERE id = 4");
        });
        const schema3Rows = storage.rows("SELECT * FROM _eda_event_log ORDER BY seq");

        const migrated = yield* open();
        const migratedEvents = encodeLog(yield* collect(migrated));
        const migratedRows = storage
          .rows<Record<string, unknown>>("SELECT * FROM _eda_event_log ORDER BY seq")
          .map(({ dedupe_key, ...row }) => ({ ...row, keyIsNull: dedupe_key === null }));
        const keyed = keyedFact({ eventId: sequentialUuidV7(76), dedupeKey: "k", n: 2 });
        yield* migrated.append({ entries: [{ event: keyed }] });
        const repeat = yield* migrated.append({
          entries: [{ event: keyedFact({ eventId: sequentialUuidV7(77), dedupeKey: "k", n: 3 }) }],
        });
        const indexRefusal = Effect.runSyncExit(
          Effect.try(() =>
            storage.sql.exec(
              `INSERT INTO _eda_event_log (event_id, namespace, type, schema_version, created_at_ms,
                trace_json, fact_json, dedupe_key)
               SELECT ?, namespace, type, schema_version, created_at_ms, trace_json, fact_json, dedupe_key
               FROM _eda_event_log WHERE dedupe_key = 'k'`,
              sequentialUuidV7(78),
            ),
          ),
        );
        return {
          events,
          migratedEvents,
          schema3Rows,
          migratedRows,
          version: storage.rows<{ version: number }>(
            "SELECT MAX(id) AS version FROM _eda_schema_migrations",
          )[0]?.version,
          repeatSeq: repeat[0]?.position.seq,
          log: yield* replayFacts(migrated),
          indexRefusal,
        };
      }).pipe(Effect.ensuring(Effect.sync(() => storage.db.close()))),
    );

    expect(result.migratedEvents).toEqual(result.events);
    expect(result.migratedRows).toEqual(
      result.schema3Rows.map((row) => ({ ...row, keyIsNull: true })),
    );
    expect(result.version).toBe(4);
    expect(result.repeatSeq).toBe(5);
    expect(result.log.slice(3)).toEqual([
      [EVENT_ID_C, 1, undefined],
      [sequentialUuidV7(76), 2, "k"],
    ]);
    expectFailure(result.indexRefusal, "UNIQUE constraint failed");
  });

  it("exposes InMemory through the EDASessionStore layer tag", () => {
    const event = commandAdmitted();
    const result = runInMemory(
      Effect.gen(function* () {
        const store = yield* EDASessionStore;
        return yield* appendOne(store, event);
      }),
    );

    expect(result.position).toEqual({ seq: 1, subSeq: 0 });
  });

  it("runs DurableObjectSessionStore migrations at construction, not per operation", () => {
    const fakeSql = makeFakeDurableObjectSql();

    const program = Effect.gen(function* () {
      const store = yield* DurableObjectSessionStore.make({
        sessionId: SessionId.make(SESSION_ID),
        storage: fakeSql.storage,
      });
      yield* lastCommittedSeq(store);
      yield* lastCommittedSeq(store);
      yield* appendOne(store, commandAdmitted());
      return yield* lastCommittedSeq(store);
    });

    const head = Effect.runSync(program);

    expect(head).toBe(1);
    expect(fakeSql.appliedMigrations).toEqual([1, 2, 3, 4]);
    expect(fakeSql.migrationInsertCount).toBe(4);
  });

  it("pages DurableObjectSessionStore event replay through bounded SQL reads", () => {
    const fakeSql = makeFakeDurableObjectSql();
    const eventCount = 41;
    const events = Array.from({ length: eventCount }, (_, index) => replayPageAppEvent(index));

    const replay = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        yield* appendMany(store, events);
        return yield* collect(store);
      }),
    );

    const firstPageLimit = fakeSql.eventReplayQueries[0]?.limit;
    expect(firstPageLimit).toBeGreaterThan(0);
    expect(firstPageLimit).toBeLessThan(eventCount);
    expect(replay.map((entry) => entry.position.seq)).toEqual(
      Array.from({ length: eventCount }, (_, index) => SequenceNumber.make(index + 1)),
    );
    expect(fakeSql.eventReplayQueries).toHaveLength(Math.ceil(eventCount / firstPageLimit!));
    expect(fakeSql.eventReplayQueries.flatMap((query) => query.returnedSeqs)).toEqual(
      Array.from({ length: eventCount }, (_, index) => index + 1),
    );
    expect(
      fakeSql.eventReplayQueries.every((query) => query.returnedSeqs.length <= query.limit!),
    ).toBe(true);
    expect(fakeSql.eventReplayQueries.every((query) => query.throughSeq === eventCount)).toBe(true);
  });

  it("does not read the full DurableObjectSessionStore tail when a consumer takes one event", () => {
    const fakeSql = makeFakeDurableObjectSql();
    const eventCount = 41;
    const events = Array.from({ length: eventCount }, (_, index) => replayPageAppEvent(index));

    const first = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        yield* appendMany(store, events);
        return yield* store.eventsAfter(SequenceNumber.make(0)).pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.map((events) => Array.from(events)),
        );
      }),
    );

    expect(first.map((entry) => entry.position.seq)).toEqual([SequenceNumber.make(1)]);
    expect(fakeSql.eventReplayQueries).toHaveLength(1);
    expect(fakeSql.eventReplayQueries[0]?.limit).toBeGreaterThan(0);
    expect(fakeSql.eventReplayQueries[0]?.limit).toBeLessThan(eventCount);
    expect(fakeSql.eventReplayQueries[0]?.returnedSeqs.length).toBe(
      fakeSql.eventReplayQueries[0]?.limit,
    );
  });

  it("stores normalized event facts and synchronous command projections in Durable Object SQLite", () => {
    const fakeSql = makeFakeDurableObjectSql();

    const result = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        return yield* appendOne(store, commandAdmitted());
      }),
    );

    expect(result.position.seq).toBe(1);
    expect(fakeSql.eventRows).toHaveLength(1);
    expect(fakeSql.eventRows[0]).toMatchObject({
      seq: 1,
      event_id: EVENT_ID_A,
      namespace: "effect-durable-agent",
      type: "CommandAdmitted",
      schema_version: 1,
    });
    expect(JSON.parse(fakeSql.eventRows[0]?.fact_json ?? "{}")).toMatchObject({
      command: { _tag: "StopTurn", commandId: COMMAND_ID_A },
    });
    expect(fakeSql.commandRows).toEqual([
      expect.objectContaining({
        command_id: COMMAND_ID_A,
        admitted_seq: 1,
        status: "admitted",
        idempotency_key: null,
      }),
    ]);
    expect(fakeSql.commandInputRows).toEqual([
      expect.objectContaining({
        command_id: COMMAND_ID_A,
        admitted_seq: 1,
        payload_json: expect.stringContaining("StopTurn"),
      }),
    ]);
  });

  it("persists and loads generic reducer checkpoints", () => {
    const fakeSql = makeFakeDurableObjectSql();

    const checkpoint = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        yield* store.saveReducerCheckpoint({
          name: "gia.file-attachments",
          schemaVersion: 1,
          throughSeq: SequenceNumber.make(42),
          payload: { attachmentIds: ["file-1"] },
          updatedAtMs: 1_715_000_000_000,
        });
        return yield* store.loadReducerCheckpoint("gia.file-attachments");
      }),
    );

    expect(checkpoint).toEqual({
      name: "gia.file-attachments",
      schemaVersion: 1,
      throughSeq: SequenceNumber.make(42),
      payload: { attachmentIds: ["file-1"] },
      updatedAtMs: 1_715_000_000_000,
    });
    expect(fakeSql.reducerCheckpointRows).toEqual([
      expect.objectContaining({ reducer_name: "gia.file-attachments", through_seq: 42 }),
    ]);
  });

  it("rolls back Durable Object reducer checkpoint batches when one row fails", () => {
    const fakeSql = makeFakeDurableObjectSql();

    const result = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        yield* store.saveReducerCheckpoints([
          {
            name: "app.alpha",
            schemaVersion: 1,
            throughSeq: SequenceNumber.make(1),
            payload: { value: "old-alpha" },
            updatedAtMs: 1_715_000_000_000,
          },
          {
            name: "app.beta",
            schemaVersion: 1,
            throughSeq: SequenceNumber.make(1),
            payload: { value: "old-beta" },
            updatedAtMs: 1_715_000_000_000,
          },
        ]);
        const exit = yield* Effect.exit(
          store.saveReducerCheckpoints([
            {
              name: "app.alpha",
              schemaVersion: 1,
              throughSeq: SequenceNumber.make(2),
              payload: { value: "new-alpha" },
              updatedAtMs: 1_715_000_000_001,
            },
            {
              name: "app.beta",
              schemaVersion: 1,
              throughSeq: SequenceNumber.make(2),
              payload: { value: "x".repeat(durableObjectSerializedJsonHardCapBytes) },
              updatedAtMs: 1_715_000_000_001,
            },
          ]),
        );
        const alpha = yield* store.loadReducerCheckpoint("app.alpha");
        const beta = yield* store.loadReducerCheckpoint("app.beta");
        return { alpha, beta, exit };
      }),
    );

    expectFailure(result.exit, "hard cap");
    expect(result.alpha).toMatchObject({
      throughSeq: SequenceNumber.make(1),
      payload: { value: "old-alpha" },
    });
    expect(result.beta).toMatchObject({
      throughSeq: SequenceNumber.make(1),
      payload: { value: "old-beta" },
    });
  });

  it("persists the framework reduced-state checkpoint for explicit tail replay", () => {
    const fakeSql = makeFakeDurableObjectSql();

    const hydrated = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        const prefix = yield* appendMany(store, [commandAdmitted(), commandStarted()]);
        const checkpointPayload = encodeReducedStateCheckpoint(
          foldReducedState(initialReducedState, prefix),
        );
        yield* store.saveReducerCheckpoint({
          name: frameworkReducedStateReducerName,
          schemaVersion: frameworkReducedStateReducerSchemaVersion,
          throughSeq: SequenceNumber.make(2),
          payload: checkpointPayload,
          updatedAtMs: 1_715_000_000_001,
        });
        yield* appendOne(store, commandCancelled());
        const checkpoint = yield* store.loadReducerCheckpoint(frameworkReducedStateReducerName);
        if (checkpoint === undefined) {
          throw new Error("missing framework checkpoint");
        }
        const referencedEvents = yield* store.loadCommittedEventsBySeq(
          reducedStateCheckpointEventSeqs(checkpoint.payload),
        );
        const tail = yield* collect(store, checkpoint.throughSeq);
        return foldReducedState(
          decodeReducedStateCheckpoint(checkpoint.payload, referencedEvents),
          tail,
        );
      }),
    );

    expect(hydrated.lastSeq).toBe(3);
    expect(hydrated.commands.get(CommandId.make(COMMAND_ID_A))?.terminal).toMatchObject({
      _tag: "Cancelled",
      seq: 3,
    });
  });

  it("stores context message bodies out-of-line and replays full logical events", () => {
    const fakeSql = makeFakeDurableObjectSql();
    const userEvent = userMessageCommitted("event payload");

    const result = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        const summary = summaryCreated();
        yield* store.append({
          entries: [
            { event: systemMessageCommitted() },
            { event: summary },
            { event: contextRebased() },
            { event: userEvent },
          ],
        });
        const replay = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
        const replayed = Array.from(replay);
        const reduced = foldReducedState(initialReducedState, replayed);
        return { reduced, replay: replayed };
      }),
    );

    expect(JSON.parse(fakeSql.eventRows.at(-1)?.fact_json ?? "{}")).not.toHaveProperty("content");
    expect(JSON.parse(fakeSql.messageRows.at(-1)?.payload_json ?? "{}")).toMatchObject({
      content: [{ text: "event payload" }],
    });
    expect(result.replay.at(-1)?.event.payload).toMatchObject({
      content: [{ text: "event payload" }],
    });
    expect(result.reduced.messages.get(MessageId.make(MESSAGE_ID_B))).toMatchObject({
      _tag: "User",
      content: [{ text: "event payload" }],
    });
  });

  it("stores framework checkpoints as pointers to heavyweight durable payloads", () => {
    const fakeSql = makeFakeDurableObjectSql();
    const heavy = "x".repeat(100_000);
    const command = CommandAdmittedEvent.make({
      namespace: effectDurableAgentNamespace,
      type: commandAdmittedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(EVENT_ID_A),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
      payload: {
        command: new SubmitMessageCommand({
          commandId: CommandId.make(COMMAND_ID_A),
          disposition: "queue",
          content: [Prompt.textPart({ text: heavy })],
        }),
      },
    });
    const user = UserMessageCommittedEvent.make({
      namespace: effectDurableAgentNamespace,
      type: userMessageCommittedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(EVENT_ID_B),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_001),
      payload: {
        commandId: CommandId.make(COMMAND_ID_A),
        messageId: MessageId.make(MESSAGE_ID_A),
        content: [Prompt.textPart({ text: heavy })],
      },
    });
    const toolCreated = ToolCallCreatedEvent.make({
      namespace: effectDurableAgentNamespace,
      type: toolCallCreatedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(EVENT_ID_C),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_002),
      payload: {
        runId: RunId.make("018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a"),
        turnId: TurnId.make("018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a"),
        inferenceId: InferenceId.make("018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a"),
        toolCallId: ToolCallId.make(TOOL_CALL_ID),
        promptPart: Prompt.toolCallPart({
          id: ProviderPartId.make(PROVIDER_PART_ID),
          name: ToolName.make("noop"),
          params: { heavy },
          providerExecuted: false,
        }),
      },
    });
    const toolCompleted = toolCallCompletedWithResult({ heavy });

    const result = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        const committed = yield* appendMany(store, [command, user, toolCreated, toolCompleted]);
        const state = foldReducedState(initialReducedState, committed);
        const payload = encodeReducedStateCheckpoint(state);
        yield* store.saveReducerCheckpoint({
          name: frameworkReducedStateReducerName,
          schemaVersion: frameworkReducedStateReducerSchemaVersion,
          throughSeq: state.lastSeq,
          payload,
          updatedAtMs: 1_715_000_000_010,
        });
        const referencedEvents = yield* store.loadCommittedEventsBySeq(
          reducedStateCheckpointEventSeqs(payload),
        );
        return decodeReducedStateCheckpoint(payload, referencedEvents);
      }),
    );

    const checkpointJson = fakeSql.reducerCheckpointRows[0]?.payload_json ?? "";
    expect(checkpointJson).not.toContain(heavy);
    expect(checkpointJson.length).toBeLessThan(10_000);
    expect(JSON.parse(fakeSql.commandInputRows[0]?.payload_json ?? "{}")).toMatchObject({
      content: [{ text: heavy }],
    });
    expect(JSON.parse(fakeSql.messageRows[0]?.payload_json ?? "{}")).toMatchObject({
      content: [{ text: heavy }],
    });
    expect(result.commands.get(CommandId.make(COMMAND_ID_A))?.command).toMatchObject({
      content: [expect.objectContaining({ text: heavy })],
    });
    expect(result.messages.get(MessageId.make(MESSAGE_ID_A))).toMatchObject({
      content: [expect.objectContaining({ text: heavy })],
    });
    expect(result.toolCalls.get(ToolCallId.make(TOOL_CALL_ID))?.terminal).toMatchObject({
      result: { heavy },
    });
  });

  it("stores submitted command bodies out-of-line while replaying full logical admissions", () => {
    const fakeSql = makeFakeDurableObjectSql();
    const command = CommandAdmittedEvent.make({
      namespace: effectDurableAgentNamespace,
      type: commandAdmittedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(EVENT_ID_A),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
      payload: {
        command: new SubmitMessageCommand({
          commandId: CommandId.make(COMMAND_ID_A),
          disposition: "queue",
          content: [Prompt.textPart({ text: "hello from command input" })],
        }),
      },
    });

    const result = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        yield* appendOne(store, command);
        return yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      }),
    );

    expect(JSON.parse(fakeSql.eventRows[0]?.fact_json ?? "{}")).toMatchObject({
      command: { _tag: "SubmitMessage", commandId: COMMAND_ID_A, disposition: "queue" },
    });
    expect(JSON.parse(fakeSql.eventRows[0]?.fact_json ?? "{}")).not.toHaveProperty(
      "command.content",
    );
    expect(JSON.parse(fakeSql.commandInputRows[0]?.payload_json ?? "{}")).toMatchObject({
      content: [{ text: "hello from command input" }],
    });
    expect(Array.from(result)[0]?.event.payload).toMatchObject({
      command: { content: [{ text: "hello from command input" }] },
    });
  });

  it("stores app namespace events directly in fact_json without EDA sidecars", () => {
    const fakeSql = makeFakeDurableObjectSql();
    const appEvent = DurableEventEnvelope.make({
      namespace: EventNamespace.make("gia-agent"),
      type: EventType.make("GiaSessionMetadataCommitted"),
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(EVENT_ID_A),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
      payload: {
        ownerUserEmail: "alice@goguardian.com",
        title: "App-owned metadata stays fat",
      },
    });

    const result = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        yield* appendOne(store, appEvent);
        return yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      }),
    );

    expect(JSON.parse(fakeSql.eventRows[0]?.fact_json ?? "{}")).toEqual(appEvent.payload);
    expect(fakeSql.commandInputRows).toEqual([]);
    expect(fakeSql.messageRows).toEqual([]);
    expect(fakeSql.summaryRows).toEqual([]);
    expect(Array.from(result)[0]?.event.payload).toEqual(appEvent.payload);
  });

  it("rejects oversized command-input bodies in the sidecar column before committing", () => {
    const fakeSql = makeFakeDurableObjectSql();
    const command = CommandAdmittedEvent.make({
      namespace: effectDurableAgentNamespace,
      type: commandAdmittedEventType,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(EVENT_ID_A),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
      payload: {
        command: new SubmitMessageCommand({
          commandId: CommandId.make(COMMAND_ID_A),
          disposition: "queue",
          content: [Prompt.textPart({ text: "x".repeat(durableObjectSerializedJsonHardCapBytes) })],
        }),
      },
    });

    const result = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        const exit = yield* Effect.exit(appendOne(store, command));
        const head = yield* lastCommittedSeq(store);
        return { exit, head };
      }),
    );

    expectFailure(result.exit, "_eda_command_inputs.payload_json");
    expect(result.head).toBe(0);
    expect(fakeSql.eventRows).toEqual([]);
    expect(fakeSql.commandInputRows).toEqual([]);
  });

  it("stores summary bodies out-of-line while replaying full logical summaries", () => {
    const fakeSql = makeFakeDurableObjectSql();
    const summary = summaryArtifact();
    const created = summaryCreatedFor(summary, 1);

    const result = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        yield* appendOne(store, created);
        return yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
      }),
    );

    expect(JSON.parse(fakeSql.eventRows[0]?.fact_json ?? "{}")).toMatchObject({
      summaryId: SUMMARY_ID,
      sourceFromSeq: 1,
      sourceToSeq: 2,
    });
    expect(JSON.parse(fakeSql.eventRows[0]?.fact_json ?? "{}")).not.toHaveProperty("summary");
    expect(JSON.parse(fakeSql.summaryRows[0]?.payload_json ?? "{}")).toMatchObject({
      summaryId: SUMMARY_ID,
      text: "Summary text",
    });
    expect(Array.from(result)[0]?.event.payload).toMatchObject({
      summary: { summaryId: SUMMARY_ID, text: "Summary text" },
    });
  });

  it("serves single events and summaries storage-only, resolving only transcript body sites", () => {
    const fakeSql = makeFakeDurableObjectSql();
    const summary = summaryArtifact();
    Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        yield* appendOne(store, summaryCreatedFor(summary, 1));
        yield* appendOne(store, toolCallCompletedWithResult({ rows: [1, 2] }));
        yield* appendOne(store, systemMessageCommitted());
      }),
    );
    const sql = fakeSql.storage.sql;
    const sessionId = SessionId.make(SESSION_ID);
    const at = (seq: number) => {
      const committed = readDurableObjectCommittedEvent(sql, sessionId, seq);
      if (committed === undefined) throw new Error(`no event at ${seq}`);
      return committed.event;
    };
    expect(resolveTranscriptBody(at(2), ["promptPart", "result"])).toEqual({
      _tag: "Found",
      value: { rows: [1, 2] },
    });
    // Sidecarred payloads are restored, but nothing outside a body site is reachable.
    expect(at(3).payload).toMatchObject({ content: expect.any(String) });
    expect(resolveTranscriptBody(at(3), ["content"])._tag).toBe("Missing");
    expect(resolveTranscriptBody(at(1), ["summary", "text"])._tag).toBe("Missing");
    expect(readDurableObjectCommittedEvent(sql, sessionId, 4)).toBeUndefined();
    expect(readDurableObjectSummary(sql, SummaryId.make(SUMMARY_ID))?.text).toBe("Summary text");
    expect(
      readDurableObjectSummary(sql, SummaryId.make("018f6bd5-2f2a-7b1e-8f1a-000000000000")),
    ).toBeUndefined();
  });

  it("rejects oversized summary bodies in the sidecar column before committing", () => {
    const fakeSql = makeFakeDurableObjectSql();
    const hugeSummary = CompactionSummaryArtifact.make({
      ...summaryArtifact(),
      text: "x".repeat(durableObjectSerializedJsonHardCapBytes),
      promptMessage: Prompt.makeMessage("user", {
        content: [Prompt.textPart({ text: "summary prompt" })],
      }),
    });
    const created = summaryCreatedFor(hugeSummary, 2);

    const result = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        const exit = yield* Effect.exit(appendOne(store, created));
        const head = yield* lastCommittedSeq(store);
        return { exit, head };
      }),
    );

    expectFailure(result.exit, "_eda_context_summaries.payload_json");
    expect(result.head).toBe(0);
    expect(fakeSql.eventRows).toEqual([]);
    expect(fakeSql.summaryRows).toEqual([]);
  });

  it("rejects oversized context-message bodies in the sidecar column before committing", () => {
    const fakeSql = makeFakeDurableObjectSql();
    const hugeMessage = userMessageCommitted("x".repeat(durableObjectSerializedJsonHardCapBytes));

    const result = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        const exit = yield* Effect.exit(appendOne(store, hugeMessage));
        const head = yield* lastCommittedSeq(store);
        return { exit, head };
      }),
    );

    expectFailure(result.exit, "_eda_context_messages.payload_json");
    expect(result.head).toBe(0);
    expect(fakeSql.eventRows).toEqual([]);
    expect(fakeSql.messageRows).toEqual([]);
  });

  it("rejects oversized tool-result event facts before committing", () => {
    const fakeSql = makeFakeDurableObjectSql();
    const hugeToolResult = toolCallCompletedWithResult({
      stdout: "x".repeat(durableObjectSerializedJsonHardCapBytes),
    });

    const result = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        const exit = yield* Effect.exit(appendOne(store, hugeToolResult));
        const head = yield* lastCommittedSeq(store);
        return { exit, head };
      }),
    );

    expectFailure(result.exit, "_eda_event_log.fact_json");
    expect(result.head).toBe(0);
    expect(fakeSql.eventRows).toEqual([]);
  });

  it("rejects oversized Durable Object event facts before committing", () => {
    const fakeSql = makeFakeDurableObjectSql();
    const hugeEvent = DurableEventEnvelope.make({
      namespace: EventNamespace.make("example.oversized"),
      type: EventType.make("HugePayload"),
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(EVENT_ID_A),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
      payload: { text: "x".repeat(durableObjectSerializedJsonHardCapBytes) },
    });

    const result = Effect.runSync(
      Effect.gen(function* () {
        const store = yield* DurableObjectSessionStore.make({
          sessionId: SessionId.make(SESSION_ID),
          storage: fakeSql.storage,
        });
        const exit = yield* Effect.exit(appendOne(store, hugeEvent));
        const head = yield* lastCommittedSeq(store);
        return { exit, head };
      }),
    );

    expectFailure(result.exit, "hard cap");
    expect(result.head).toBe(0);
    expect(fakeSql.eventRows).toEqual([]);
  });

  it("wraps DurableObjectSessionStore migration failures in EDASessionStoreError", () => {
    const failingStorage: DurableObjectSessionStorage = {
      sql: {
        exec: () => {
          throw new Error("sql unavailable");
        },
      },
      transactionSync: (closure) => closure(),
    };

    const exit = Effect.runSyncExit(
      DurableObjectSessionStore.make({
        sessionId: SessionId.make(SESSION_ID),
        storage: failingStorage,
      }),
    );

    expectFailure(exit, "migrating DurableObjectSessionStore: sql unavailable");
  });
});

const CounterEvent = Schema.Struct({
  ...DurableEventEnvelope.fields,
  namespace: Schema.Literal("test.counter"),
  type: Schema.Literal("Incremented"),
  payload: Schema.Struct({ amount: Schema.Number, note: Schema.optionalKey(Schema.String) }),
});

const counterReducer = EDAReducer.forEvents(CounterEvent, {
  name: "test.counter",
  initial: 0,
  stateSchema: Schema.Number,
  reduce: (total, entry) => total + entry.event.payload.amount,
});

const counterFact = (eventId: string, note?: string, dedupeKey?: string) =>
  CounterEvent.make({
    namespace: "test.counter",
    type: "Incremented",
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(eventId),
    sessionId: SessionId.make(SESSION_ID),
    createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
    payload: { amount: 1, ...(note === undefined ? {} : { note }) },
    ...(dedupeKey === undefined ? {} : { dedupeKey: EventDedupeKey.make(dedupeKey) }),
  });

/** Run `SessionState` over the real SQLite Durable Object store with the counter app event. */
const withSqliteSession = <A, E>(
  program: (input: {
    readonly session: typeof SessionState.Service;
    readonly store: EDASessionStoreShape;
    readonly published: ReadonlyArray<number>;
  }) => Effect.Effect<A, E, EDAReducerRegistry>,
  deciders: ReadonlyArray<EDADecider> = [],
) =>
  Effect.gen(function* () {
    const storage = new SqliteDurableObjectStorage();
    const store = registerAppEvents(
      yield* DurableObjectSessionStore.make({ sessionId: SessionId.make(SESSION_ID), storage }),
      CounterEvent,
    );
    const published: Array<number> = [];
    return yield* Effect.gen(function* () {
      const session = yield* SessionState;
      return yield* program({ session, store, published });
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          reducers: [counterReducer],
          appEvents: CounterEvent,
          deciders,
          wrapStore: () => store,
          sessionEventObserverLayer: SessionEventObserver.FromHandler((event) =>
            Effect.sync(() => {
              published.push(event.position.seq);
            }),
          ),
        }),
      ),
      Effect.ensuring(Effect.sync(() => storage.db.close())),
    );
  });

describe("SessionState over DurableObjectSessionStore on SQLite", () => {
  // Same-batch repeat reproduction from gpt-6-astra's review (review/docs-chat-frs-adversarial@121ac4d).
  it.each([
    ["eventId", [counterFact(EVENT_ID_A), counterFact(EVENT_ID_A)]],
    [
      "dedupe key",
      [counterFact(EVENT_ID_A, undefined, "k"), counterFact(EVENT_ID_B, undefined, "k")],
    ],
  ])("folds and publishes an event repeated by %s within one batch once", async (_, batch) => {
    const result = await Effect.runPromise(
      withSqliteSession(({ session, store, published }) =>
        Effect.gen(function* () {
          const reducers = yield* EDAReducerRegistry;
          const returned = yield* session.submitBatch(batch);
          const log = yield* collect(store);
          const live = yield* session.snapshotData();
          return {
            returnedSeqs: returned.map((entry) => entry.position.seq),
            logRows: log.length,
            liveCount: live.reducerStates.get(counterReducer.name),
            replayCount: reducers.reduce(reducers.initial, log).get(counterReducer.name),
            publishedSeqs: [...published],
          };
        }),
      ),
    );

    expect(result).toEqual({
      returnedSeqs: [1, 1],
      logRows: 1,
      liveCount: 1,
      replayCount: 1,
      publishedSeqs: [1],
    });
  });

  // Oversize path adapted to `submit` from Claude Opus 5.5's review
  // (adversary/fr1-fr5-interrogate@b2c303a, adversary-fr1-decider-fatal.test.ts).
  it("refuses an oversized app event without making the runtime fatal", async () => {
    // `{"amount":1,"note":""}` is 22 bytes of JSON around the note.
    const justOver = "x".repeat(maxAppEventBytes - 22 + 1);
    const overStoreCap = "x".repeat(durableObjectSerializedJsonHardCapBytes);
    const result = await Effect.runPromise(
      withSqliteSession(({ session, store }) =>
        Effect.gen(function* () {
          const outcome = (exit: Exit.Exit<unknown, unknown>) => {
            if (Exit.isSuccess(exit)) return "accepted";
            if (hasEDASessionStoreError(exit.cause)) return "store error";
            const conflict = exit.cause.reasons
              .map((reason) => (Cause.isFailReason(reason) ? reason.error : undefined))
              .find((error) => error instanceof SessionCommandAdmissionConflict);
            return conflict?.code ?? String(exit.cause);
          };
          const overStore = yield* Effect.exit(
            session.submitBatch([counterFact(EVENT_ID_A, overStoreCap)]),
          );
          const overCap = yield* Effect.exit(
            session.submitBatch([counterFact(EVENT_ID_B, justOver)]),
          );
          const small = yield* Effect.exit(session.submitBatch([counterFact(EVENT_ID_C)]));
          const log = yield* collect(store);
          return {
            overStore: outcome(overStore),
            overCap: outcome(overCap),
            small: outcome(small),
            loggedIds: log.map((entry) => entry.event.eventId),
          };
        }),
      ),
    );

    expect(result).toEqual({
      overStore: "event_too_large",
      overCap: "event_too_large",
      small: "accepted",
      loggedIds: [EVENT_ID_C],
    });
  });
});

// Claude Opus 5.5's review (adversary/fr1-fr5-interrogate@b2c303a,
// adversary-fr1-decider-fatal.test.ts): a schema-valid decider draft over the store's row cap.
describe("deciders over DurableObjectSessionStore on SQLite", () => {
  it("refuses an oversized draft as invalid_output without making the runtime fatal", async () => {
    const overStoreCap = "x".repeat(durableObjectSerializedJsonHardCapBytes);
    const justOver = "x".repeat(maxAppEventBytes - 22 + 1);
    const bigNote = EDADecider.define({
      name: "test.counter.BigNote",
      input: Schema.Struct({ key: Schema.String, note: Schema.String }),
      events: CounterEvent,
      result: Schema.Null,
      rejection: Schema.Never,
      idempotencyKey: (input) => input.key,
      decide: (_view, input) => ({
        _tag: "Accept",
        items: [
          {
            namespace: "test.counter",
            type: "Incremented",
            schemaVersion: schemaV1,
            payload: { amount: 1, note: input.note },
          },
        ],
        result: null,
      }),
    });
    const result = await Effect.runPromise(
      withSqliteSession(
        ({ session, store }) =>
          Effect.gen(function* () {
            const reason = (exit: Exit.Exit<unknown, unknown>) => {
              if (Exit.isSuccess(exit)) return "accepted";
              if (hasEDASessionStoreError(exit.cause)) return "store error";
              const error = exit.cause.reasons
                .map((entry) => (Cause.isFailReason(entry) ? entry.error : undefined))
                .find((entry) => entry instanceof EDADeciderError);
              return error?.reason ?? String(exit.cause);
            };
            const overStore = yield* Effect.exit(
              session.decide(bigNote, { key: "a", note: overStoreCap }),
            );
            const overCap = yield* Effect.exit(
              session.decide(bigNote, { key: "b", note: justOver }),
            );
            const small = yield* Effect.exit(session.decide(bigNote, { key: "c", note: "" }));
            const submitted = yield* Effect.exit(session.submitBatch([counterFact(EVENT_ID_C)]));
            const log = yield* collect(store);
            return {
              overStore: reason(overStore),
              overCap: reason(overCap),
              small: reason(small),
              submitted: Exit.isSuccess(submitted),
              loggedTypes: log.map((entry) => entry.event.type),
            };
          }),
        [bigNote],
      ),
    );

    expect(result).toEqual({
      overStore: "invalid_output",
      overCap: "invalid_output",
      small: "accepted",
      submitted: true,
      loggedTypes: ["DeciderAccepted", "Foreign", "Foreign"],
    });
  });
});

interface FakeEventRow {
  readonly seq: number;
  readonly event_id: string;
  readonly namespace: string;
  readonly type: string;
  readonly schema_version: number;
  readonly created_at_ms: number;
  readonly trace_json: string;
  readonly fact_json: string;
  readonly dedupe_key?: string | null;
}

interface FakeCommandRow {
  readonly command_id: string;
  readonly admitted_seq: number;
  readonly status: string;
  readonly idempotency_key: string | null;
  readonly payload_json: string;
}

interface FakeCommandInputRow {
  readonly command_id: string;
  readonly admitted_seq: number;
  readonly payload_json: string;
}

interface FakeMessageRow {
  readonly message_id: string;
  readonly context_seq: number;
  readonly payload_json: string;
}

interface FakeSummaryRow {
  readonly summary_id: string;
  readonly created_seq: number;
  readonly payload_json: string;
}

interface FakeReducerCheckpointRow {
  readonly reducer_name: string;
  readonly schema_version: number;
  readonly through_seq: number;
  readonly payload_json: string;
  readonly updated_at_ms: number;
}

const makeFakeDurableObjectSql = () => {
  let migrations = new Set<number>();
  let rows: Array<FakeEventRow> = [];
  let commands: Array<FakeCommandRow> = [];
  let commandInputs: Array<FakeCommandInputRow> = [];
  let messages: Array<FakeMessageRow> = [];
  let summaries: Array<FakeSummaryRow> = [];
  let reducerCheckpoints: Array<FakeReducerCheckpointRow> = [];
  let migrationInsertCount = 0;
  let eventReplayQueries: Array<{
    readonly afterSeq: number;
    readonly throughSeq: number | undefined;
    readonly limit: number | undefined;
    readonly returnedSeqs: ReadonlyArray<number>;
  }> = [];

  const cursor = <Row>(items: Array<Row>): DurableObjectSqlCursor<Row> => ({
    one: () => {
      const row = items[0];
      if (row === undefined) {
        throw new Error("Expected one SQL row");
      }
      return row;
    },
    toArray: () => items,
  });

  const sql: DurableObjectSqlStorage = {
    exec: <Row = Record<string, unknown>>(query: string, ...bindings: ReadonlyArray<unknown>) => {
      const normalized = query.replace(/\s+/g, " ").trim().toUpperCase();

      if (
        normalized.startsWith("CREATE TABLE") ||
        normalized.startsWith("ALTER TABLE") ||
        normalized.startsWith("CREATE INDEX") ||
        normalized.startsWith("CREATE UNIQUE INDEX") ||
        normalized.startsWith("DROP TABLE") ||
        normalized.startsWith("DELETE FROM _EDA_SCHEMA_MIGRATIONS")
      ) {
        return cursor<Row>([]);
      }

      if (normalized.includes("SELECT COALESCE(MAX(ID), 0) AS VERSION")) {
        return cursor<Row>([{ version: Math.max(0, ...Array.from(migrations)) } as Row]);
      }

      if (normalized.includes("SELECT COALESCE(MAX(SEQ), 0) AS HEAD FROM _EDA_EVENT_LOG")) {
        return cursor<Row>([{ head: rows.reduce((max, row) => Math.max(max, row.seq), 0) } as Row]);
      }

      if (normalized.startsWith("INSERT INTO _EDA_SCHEMA_MIGRATIONS")) {
        migrations.add(Number(bindings[0]));
        migrationInsertCount += 1;
        return cursor<Row>([]);
      }

      if (normalized.startsWith("INSERT INTO _EDA_EVENT_LOG")) {
        const [
          eventId,
          namespace,
          type,
          schemaVersion,
          createdAtMs,
          traceJson,
          payloadJson,
          dedupeKey,
        ] = bindings;
        if (
          dedupeKey !== null &&
          rows.some(
            (row) =>
              row.namespace === namespace && row.type === type && row.dedupe_key === dedupeKey,
          )
        ) {
          throw new Error("UNIQUE constraint failed: _eda_event_log dedupe key");
        }
        rows.push({
          seq: nextSeq(rows),
          event_id: String(eventId),
          namespace: String(namespace),
          type: String(type),
          schema_version: Number(schemaVersion),
          created_at_ms: Number(createdAtMs),
          trace_json: String(traceJson),
          fact_json: String(payloadJson),
          dedupe_key: dedupeKey === null ? null : String(dedupeKey),
        });
        return cursor<Row>([]);
      }

      if (normalized.includes("WHERE NAMESPACE = ? AND TYPE = ? AND DEDUPE_KEY = ?")) {
        const [namespace, type, dedupeKey] = bindings;
        return cursor<Row>(
          withDedupeKeys(
            rows.filter(
              (row) =>
                row.namespace === namespace && row.type === type && row.dedupe_key === dedupeKey,
            ),
          ) as Array<Row>,
        );
      }

      if (
        normalized.includes(
          "SELECT SEQ, EVENT_ID, NAMESPACE, TYPE, SCHEMA_VERSION, CREATED_AT_MS, TRACE_JSON, FACT_JSON",
        ) &&
        normalized.includes("WHERE EVENT_ID = ?")
      ) {
        const [eventId] = bindings;
        return cursor<Row>(
          withDedupeKeys(rows.filter((row) => row.event_id === eventId)) as Array<Row>,
        );
      }

      if (
        normalized.includes(
          "SELECT SEQ, EVENT_ID, NAMESPACE, TYPE, SCHEMA_VERSION, CREATED_AT_MS, TRACE_JSON, FACT_JSON",
        ) &&
        normalized.includes("WHERE SEQ > ?")
      ) {
        const [afterSeq, throughSeq, limit] = bindings;
        const numericAfterSeq = Number(afterSeq);
        const numericThroughSeq = normalized.includes("SEQ <= ?") ? Number(throughSeq) : undefined;
        const numericLimit = normalized.includes("LIMIT ?")
          ? Number(numericThroughSeq === undefined ? throughSeq : limit)
          : undefined;
        const selected = rows
          .filter(
            (row) =>
              row.seq > numericAfterSeq &&
              (numericThroughSeq === undefined || row.seq <= numericThroughSeq),
          )
          .sort((left, right) => left.seq - right.seq)
          .slice(0, numericLimit);
        eventReplayQueries.push({
          afterSeq: numericAfterSeq,
          throughSeq: numericThroughSeq,
          limit: numericLimit,
          returnedSeqs: selected.map((row) => row.seq),
        });
        return cursor<Row>(withDedupeKeys(selected) as Array<Row>);
      }

      if (
        normalized.includes(
          "SELECT SEQ, EVENT_ID, NAMESPACE, TYPE, SCHEMA_VERSION, CREATED_AT_MS, TRACE_JSON, FACT_JSON",
        ) &&
        normalized.includes("WHERE SEQ = ?")
      ) {
        const [seq] = bindings;
        return cursor<Row>(
          withDedupeKeys(rows.filter((row) => row.seq === Number(seq))) as Array<Row>,
        );
      }

      if (normalized.startsWith("INSERT INTO _EDA_COMMAND_STATE")) {
        const [commandId, admittedSeq, status] = bindings;
        const existing = commands.findIndex((row) => row.command_id === commandId);
        const next: FakeCommandRow = {
          command_id: String(commandId),
          admitted_seq: Number(admittedSeq),
          status: String(status),
          idempotency_key: bindings.length === 5 ? (bindings[3] as string | null) : null,
          payload_json: String(bindings.length === 5 ? bindings[4] : bindings[3]),
        };
        if (existing === -1) {
          commands.push(next);
        } else {
          commands[existing] = {
            ...commands[existing]!,
            status: next.status,
            payload_json: next.payload_json,
            ...(bindings.length === 5
              ? { admitted_seq: next.admitted_seq, idempotency_key: next.idempotency_key }
              : {}),
          };
        }
        return cursor<Row>([]);
      }

      if (normalized.includes("SELECT ADMITTED_SEQ FROM _EDA_COMMAND_STATE")) {
        const [value] = bindings;
        if (normalized.includes("WHERE STATUS = ?")) {
          return cursor<Row>(
            commands
              .filter((row) => row.status === value)
              .sort((left, right) => left.admitted_seq - right.admitted_seq)
              .map(({ admitted_seq }) => ({ admitted_seq }) as Row),
          );
        }
        const selected = normalized.includes("WHERE IDEMPOTENCY_KEY = ?")
          ? commands.filter((row) => row.idempotency_key === value)
          : commands.filter((row) => row.command_id === value);
        return cursor<Row>(selected.map(({ admitted_seq }) => ({ admitted_seq }) as Row));
      }

      if (normalized.startsWith("INSERT INTO _EDA_COMMAND_INPUTS")) {
        const [commandId, admittedSeq, payloadJson] = bindings;
        const existing = commandInputs.findIndex((row) => row.command_id === commandId);
        const next = {
          command_id: String(commandId),
          admitted_seq: Number(admittedSeq),
          payload_json: String(payloadJson),
        } satisfies FakeCommandInputRow;
        if (existing === -1) {
          commandInputs.push(next);
        } else {
          commandInputs[existing] = next;
        }
        return cursor<Row>([]);
      }

      if (normalized.includes("SELECT PAYLOAD_JSON FROM _EDA_COMMAND_INPUTS")) {
        const [commandId] = bindings;
        return cursor<Row>(
          commandInputs
            .filter((row) => row.command_id === commandId)
            .map(({ payload_json }) => ({ payload_json }) as Row),
        );
      }

      if (normalized.startsWith("INSERT INTO _EDA_CONTEXT_MESSAGES")) {
        const [messageId, contextSeq, payloadJson] = bindings;
        const existing = messages.findIndex((row) => row.message_id === messageId);
        const next = {
          message_id: String(messageId),
          context_seq: Number(contextSeq),
          payload_json: String(payloadJson),
        } satisfies FakeMessageRow;
        if (existing === -1) {
          messages.push(next);
        } else {
          messages[existing] = next;
        }
        return cursor<Row>([]);
      }

      if (normalized.includes("SELECT CONTEXT_SEQ, PAYLOAD_JSON FROM _EDA_CONTEXT_MESSAGES")) {
        const [value] = bindings;
        const selected = normalized.includes("WHERE CONTEXT_SEQ = ?")
          ? messages.filter((row) => row.context_seq === Number(value))
          : messages.filter((row) => row.message_id === value);
        return cursor<Row>(
          selected.map(({ context_seq, payload_json }) => ({ context_seq, payload_json }) as Row),
        );
      }

      if (normalized.startsWith("INSERT INTO _EDA_CONTEXT_SUMMARIES")) {
        const [summaryId, createdSeq, payloadJson] = bindings;
        const existing = summaries.findIndex((row) => row.summary_id === summaryId);
        const next = {
          summary_id: String(summaryId),
          created_seq: Number(createdSeq),
          payload_json: String(payloadJson),
        } satisfies FakeSummaryRow;
        if (existing === -1) {
          summaries.push(next);
        } else {
          summaries[existing] = next;
        }
        return cursor<Row>([]);
      }

      if (normalized.includes("SELECT SUMMARY_ID, PAYLOAD_JSON FROM _EDA_CONTEXT_SUMMARIES")) {
        return cursor<Row>(
          summaries
            .slice()
            .sort((left, right) => right.created_seq - left.created_seq)
            .map(({ summary_id, payload_json }) => ({ summary_id, payload_json }) as Row),
        );
      }

      if (
        normalized.includes("SELECT PAYLOAD_JSON FROM _EDA_CONTEXT_SUMMARIES WHERE CREATED_SEQ = ?")
      ) {
        const [createdSeq] = bindings;
        return cursor<Row>(
          summaries
            .filter((row) => row.created_seq === Number(createdSeq))
            .map(({ payload_json }) => ({ payload_json }) as Row),
        );
      }

      if (
        normalized.includes("SELECT PAYLOAD_JSON FROM _EDA_CONTEXT_SUMMARIES WHERE SUMMARY_ID = ?")
      ) {
        const [summaryId] = bindings;
        return cursor<Row>(
          summaries
            .filter((row) => row.summary_id === summaryId)
            .map(({ payload_json }) => ({ payload_json }) as Row),
        );
      }

      if (normalized.startsWith("INSERT INTO _EDA_REDUCER_CHECKPOINTS")) {
        const [reducerName, schemaVersion, throughSeq, payloadJson, updatedAtMs] = bindings;
        const existing = reducerCheckpoints.findIndex((row) => row.reducer_name === reducerName);
        const next = {
          reducer_name: String(reducerName),
          schema_version: Number(schemaVersion),
          through_seq: Number(throughSeq),
          payload_json: String(payloadJson),
          updated_at_ms: Number(updatedAtMs),
        } satisfies FakeReducerCheckpointRow;
        if (existing === -1) {
          reducerCheckpoints.push(next);
        } else {
          reducerCheckpoints[existing] = next;
        }
        return cursor<Row>([]);
      }

      if (
        normalized.includes(
          "SELECT REDUCER_NAME, SCHEMA_VERSION, THROUGH_SEQ, PAYLOAD_JSON, UPDATED_AT_MS",
        )
      ) {
        const [reducerName] = bindings;
        return cursor<Row>(
          reducerCheckpoints.filter((row) => row.reducer_name === reducerName) as Array<Row>,
        );
      }

      throw new Error(`Unsupported fake SQL query: ${query}`);
    },
  };

  const storage: DurableObjectSessionStorage = {
    sql,
    transactionSync: (closure) => {
      const rowSnapshot = rows.map((row) => ({ ...row }));
      const commandSnapshot = commands.map((row) => ({ ...row }));
      const commandInputSnapshot = commandInputs.map((row) => ({ ...row }));
      const messageSnapshot = messages.map((row) => ({ ...row }));
      const summarySnapshot = summaries.map((row) => ({ ...row }));
      const reducerCheckpointSnapshot = reducerCheckpoints.map((row) => ({ ...row }));
      const migrationSnapshot = new Set(migrations);
      const migrationInsertCountSnapshot = migrationInsertCount;
      try {
        return closure();
      } catch (error) {
        rows = rowSnapshot;
        commands = commandSnapshot;
        commandInputs = commandInputSnapshot;
        messages = messageSnapshot;
        summaries = summarySnapshot;
        reducerCheckpoints = reducerCheckpointSnapshot;
        migrations = migrationSnapshot;
        migrationInsertCount = migrationInsertCountSnapshot;
        throw error;
      }
    },
  };

  return {
    storage,
    get appliedMigrations() {
      return Array.from(migrations).sort((left, right) => left - right);
    },
    get commandInputRows() {
      return commandInputs;
    },
    get commandRows() {
      return commands;
    },
    get eventRows() {
      return rows;
    },
    get messageRows() {
      return messages;
    },
    get summaryRows() {
      return summaries;
    },
    get reducerCheckpointRows() {
      return reducerCheckpoints;
    },
    get migrationInsertCount() {
      return migrationInsertCount;
    },
    get eventReplayQueries() {
      return eventReplayQueries;
    },
  };
};

/** Rows seeded in the 0.1.0 shape read back with the NULL key real SQLite returns. */
const withDedupeKeys = (rows: ReadonlyArray<FakeEventRow>): Array<FakeEventRow> =>
  rows.map((row) => ({ ...row, dedupe_key: row.dedupe_key ?? null }));

const nextSeq = (rows: ReadonlyArray<FakeEventRow>): number =>
  rows.reduce((max, row) => Math.max(max, row.seq), 0) + 1;
