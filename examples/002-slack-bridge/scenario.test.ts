import * as Effect from "effect/Effect";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { CommandIdempotencyKey, SubmitMessageCommand } from "../../src/types/commands";
import {
  InferenceId,
  CommandId,
  EventId,
  MessageId,
  RunId,
  SequenceNumber,
  SessionId,
  TurnId,
  durablePosition,
} from "../../src/types/core";
import {
  AssistantMessageCommittedPayload,
  CommandAdmittedPayload,
  DurableEventEnvelope,
  EventType,
  RunStartedPayload,
  UnixEpochMillis,
  UserMessageCommittedPayload,
  assistantMessageCommittedEventType,
  commandAdmittedEventType,
  effectDurableAgentNamespace,
  runStartedEventType,
  schemaV1,
  userMessageCommittedEventType,
} from "../../src/types/events";
import { sequentialUuidV7 } from "../../src/services/id-generator";
import { CommittedDurableEvent } from "../../src/services/session-store";
import type { EDASinkContext, EDASinkDurableBatch } from "../../src/services/sink-registry";
import {
  OutboundSlackIdempotencyKey,
  SlackChannelId,
  SlackEventId,
  SlackEvents,
  SlackMessageTs,
  SlackTeamId,
  SlackThreadTs,
  SlackUserId,
} from "./events";
import { SlackBridgeReducer, initialSlackBridgeState, reduceSlackBridgeState } from "./reducer";
import { makeSlackReplySink, type SlackClient } from "./sinks";

const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const COMMAND_ID = CommandId.make(sequentialUuidV7(1));
const USER_MESSAGE_ID = MessageId.make(sequentialUuidV7(2));
const ASSISTANT_MESSAGE_ID = MessageId.make(sequentialUuidV7(3));
const RUN_ID = RunId.make(sequentialUuidV7(4));
const TURN_ID = TurnId.make(sequentialUuidV7(5));
const INFERENCE_ID = InferenceId.make(sequentialUuidV7(6));
const IDEMPOTENCY_KEY = CommandIdempotencyKey.make("slack:event:Ev123");
const MODEL_SELECTION = { provider: "test", modelId: "test-model" };

const command = new SubmitMessageCommand({
  commandId: COMMAND_ID,
  idempotencyKey: IDEMPOTENCY_KEY,
  disposition: "queue",
  content: [Prompt.textPart({ text: "Please summarize this incident." })],
});

describe("Slack bridge example", () => {
  it("derives Slack bridge state from framework events and app events", () => {
    const state = baseScenarioEvents()
      .concat([
        committed(
          6,
          SlackEvents.replyDelivered({
            assistantMessageId: ASSISTANT_MESSAGE_ID,
            teamId: slackMessageReceived.teamId,
            channelId: slackMessageReceived.channelId,
            threadTs: slackMessageReceived.threadTs,
            slackMessageTs: SlackMessageTs.make("1729.001"),
            outboundIdempotencyKey: OutboundSlackIdempotencyKey.make(
              `${SESSION_ID}:${ASSISTANT_MESSAGE_ID}`,
            ),
            createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
            eventId: EventId.make(sequentialUuidV7(206)),
            sessionId: SESSION_ID,
          }),
        ),
      ])
      .reduce(reduceSlackBridgeState, initialSlackBridgeState);

    expect(state.slackByCommandId.get(COMMAND_ID)?.slackEventId).toBe("Ev123");
    expect(state.slackByUserMessageId.get(USER_MESSAGE_ID)?.threadTs).toBe("1729.000");
    expect(state.slackByAssistantMessageId.get(ASSISTANT_MESSAGE_ID)?.channelId).toBe("C1");
    expect(state.deliveredByAssistantMessageId.get(ASSISTANT_MESSAGE_ID)?.slackMessageTs).toBe(
      "1729.001",
    );
  });

  it("keeps Slack delivery idempotent when EDA retries the same sink batch", async () => {
    const calls: Array<{ readonly idempotencyKey: string; readonly text: string }> = [];
    const flakySlackClient: SlackClient = {
      postThreadReply: (input) =>
        Effect.gen(function* () {
          calls.push({ idempotencyKey: input.idempotencyKey, text: input.text });
          if (calls.length === 1) {
            return yield* Effect.fail(new Error("transient Slack outage"));
          }
          return SlackMessageTs.make("1729.002");
        }),
    };
    const sink = makeSlackReplySink(flakySlackClient).durable!;
    const assistantEvent = baseScenarioEvents().at(-1)!;
    const reducerState = baseScenarioEvents().reduce(
      reduceSlackBridgeState,
      initialSlackBridgeState,
    );
    const batch: EDASinkDurableBatch = {
      allEvents: baseScenarioEvents(),
      events: [assistantEvent],
      reducerStates: new Map([[SlackBridgeReducer.name, reducerState]]),
      throughSeq: assistantEvent.position.seq,
      stateAfter: undefined as never,
    };

    const staged: Array<DurableEventEnvelope> = [];
    await expect(Effect.runPromise(sink.process(batch, sinkContext(staged)))).rejects.toThrow(
      "transient Slack outage",
    );
    expect(staged).toEqual([]);

    await Effect.runPromise(sink.process(batch, sinkContext(staged)));

    expect(calls).toHaveLength(2);
    expect(new Set(calls.map((call) => call.idempotencyKey)).size).toBe(1);
    expect(staged.map((event) => event.type)).toEqual(["SlackReplyDelivered"]);
  });
});

const baseScenarioEvents = (): ReadonlyArray<CommittedDurableEvent> => [
  frameworkEvent(1, commandAdmittedEventType, CommandAdmittedPayload.make({ command })),
  committed(
    2,
    SlackEvents.messageReceived({
      ...slackMessageReceived,
      relatedCommandIdempotencyKey: IDEMPOTENCY_KEY,
    }),
  ),
  frameworkEvent(
    3,
    userMessageCommittedEventType,
    UserMessageCommittedPayload.make({
      commandId: COMMAND_ID,
      messageId: USER_MESSAGE_ID,
      content: [Prompt.textPart({ text: "Please summarize this incident." })],
    }),
  ),
  frameworkEvent(
    4,
    runStartedEventType,
    RunStartedPayload.make({
      runId: RUN_ID,
      commandIds: [COMMAND_ID],
      modelSelection: MODEL_SELECTION,
    }),
  ),
  frameworkEvent(
    5,
    assistantMessageCommittedEventType,
    AssistantMessageCommittedPayload.make({
      messageId: ASSISTANT_MESSAGE_ID,
      runId: RUN_ID,
      turnId: TURN_ID,
      inferenceId: INFERENCE_ID,
      promptParts: [Prompt.textPart({ text: "Here is the summary." })],
    }),
  ),
];

const slackMessageReceived = {
  teamId: SlackTeamId.make("T1"),
  channelId: SlackChannelId.make("C1"),
  threadTs: SlackThreadTs.make("1729.000"),
  slackEventId: SlackEventId.make("Ev123"),
  slackUserId: SlackUserId.make("U1"),
  text: "Please summarize this incident.",
  createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
  eventId: EventId.make(sequentialUuidV7(200)),
  sessionId: SESSION_ID,
};

const sinkContext = (staged: Array<DurableEventEnvelope>): EDASinkContext => ({
  sessionId: SESSION_ID,
  checkpoint: {
    get: (_schema, initial) => Effect.succeed(initial),
    save: () => Effect.void,
  },
  events: undefined as never,
  makeEventId: () => Effect.succeed(EventId.make(sequentialUuidV7(900 + staged.length))),
  stageDurable: (event) =>
    Effect.sync(() => {
      staged.push(event);
    }),
  emitEphemeral: () => Effect.die("not used in this example"),
  forkScoped: () => Effect.die("not used in this example"),
});

const frameworkEvent = (seq: number, type: EventType, payload: unknown): CommittedDurableEvent =>
  committed(
    seq,
    DurableEventEnvelope.make({
      namespace: effectDurableAgentNamespace,
      type,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(sequentialUuidV7(100 + seq)),
      sessionId: SESSION_ID,
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000 + seq),
      payload,
    }),
  );

const committed = (seq: number, event: DurableEventEnvelope): CommittedDurableEvent =>
  CommittedDurableEvent.make({
    position: durablePosition(SequenceNumber.make(seq)),
    event,
  });
