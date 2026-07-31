import * as Schema from "effect/Schema";

import type { CommittedDurableEvent } from "../../src/services/session-store";
import { EDAReducer } from "../../src/services/reducer-registry";
import { CommandIdempotencyKey } from "../../src/types/commands";
import { CommandId, MessageId, RunId } from "../../src/types/core";
import {
  AssistantMessageCommittedPayload,
  CommandAdmittedPayload,
  RunStartedPayload,
  UserMessageCommittedPayload,
  assistantMessageCommittedEventType,
  commandAdmittedEventType,
  runStartedEventType,
  userMessageCommittedEventType,
} from "../../src/types/events";
import {
  SlackMessageReceivedPayload,
  SlackReplyDeliveredPayload,
  slackBridgeNamespace,
  slackMessageReceivedEventType,
  slackReplyDeliveredEventType,
} from "./events";

/** Derived Slack bridge state. Every field is reducer-derived, not callback-owned. */
export interface SlackBridgeState {
  readonly commandIdByIdempotencyKey: ReadonlyMap<CommandIdempotencyKey, CommandId>;
  readonly slackByIdempotencyKey: ReadonlyMap<CommandIdempotencyKey, SlackMessageReceivedPayload>;
  readonly slackByCommandId: ReadonlyMap<CommandId, SlackMessageReceivedPayload>;
  readonly slackByUserMessageId: ReadonlyMap<MessageId, SlackMessageReceivedPayload>;
  readonly commandIdsByRunId: ReadonlyMap<RunId, ReadonlyArray<CommandId>>;
  readonly slackByAssistantMessageId: ReadonlyMap<MessageId, SlackMessageReceivedPayload>;
  readonly deliveredByAssistantMessageId: ReadonlyMap<MessageId, SlackReplyDeliveredPayload>;
}

export const initialSlackBridgeState: SlackBridgeState = {
  commandIdByIdempotencyKey: new Map(),
  slackByIdempotencyKey: new Map(),
  slackByCommandId: new Map(),
  slackByUserMessageId: new Map(),
  commandIdsByRunId: new Map(),
  slackByAssistantMessageId: new Map(),
  deliveredByAssistantMessageId: new Map(),
};

const SlackBridgeStateSchema = Schema.Struct({
  commandIdByIdempotencyKey: Schema.ReadonlyMap(CommandIdempotencyKey, CommandId),
  slackByIdempotencyKey: Schema.ReadonlyMap(CommandIdempotencyKey, SlackMessageReceivedPayload),
  slackByCommandId: Schema.ReadonlyMap(CommandId, SlackMessageReceivedPayload),
  slackByUserMessageId: Schema.ReadonlyMap(MessageId, SlackMessageReceivedPayload),
  commandIdsByRunId: Schema.ReadonlyMap(RunId, Schema.Array(CommandId)),
  slackByAssistantMessageId: Schema.ReadonlyMap(MessageId, SlackMessageReceivedPayload),
  deliveredByAssistantMessageId: Schema.ReadonlyMap(MessageId, SlackReplyDeliveredPayload),
});

/**
 * Pure app reducer over both framework events and Slack app events.
 *
 * This is the heart of the example: Slack ingress metadata, EDA command ids,
 * EDA user messages, EDA runs, EDA assistant messages, and outbound delivery
 * confirmations all become one deterministic projection.
 */
export const SlackBridgeReducer = EDAReducer.make<SlackBridgeState>({
  name: "example.slack",
  initial: initialSlackBridgeState,
  stateSchema: SlackBridgeStateSchema,
  reduce: (state, entry) => reduceSlackBridgeState(state, entry),
});

export const reduceSlackBridgeState = (
  state: SlackBridgeState,
  entry: CommittedDurableEvent,
): SlackBridgeState => {
  const event = entry.event;

  if (event.type === commandAdmittedEventType && Schema.is(CommandAdmittedPayload)(event.payload)) {
    return rememberCommandIdempotency(state, event.payload);
  }

  if (event.namespace === slackBridgeNamespace && event.type === slackMessageReceivedEventType) {
    return Schema.is(SlackMessageReceivedPayload)(event.payload)
      ? rememberSlackIngress(state, event.payload)
      : state;
  }

  if (
    event.type === userMessageCommittedEventType &&
    Schema.is(UserMessageCommittedPayload)(event.payload)
  ) {
    return attachUserMessageToSlackMessage(state, event.payload);
  }

  if (event.type === runStartedEventType && Schema.is(RunStartedPayload)(event.payload)) {
    return rememberRunCommands(state, event.payload);
  }

  if (
    event.type === assistantMessageCommittedEventType &&
    Schema.is(AssistantMessageCommittedPayload)(event.payload)
  ) {
    return attachAssistantMessageToSlackThread(state, event.payload);
  }

  if (event.namespace === slackBridgeNamespace && event.type === slackReplyDeliveredEventType) {
    return Schema.is(SlackReplyDeliveredPayload)(event.payload)
      ? markSlackReplyDelivered(state, event.payload)
      : state;
  }

  return state;
};

const rememberCommandIdempotency = (
  state: SlackBridgeState,
  payload: CommandAdmittedPayload,
): SlackBridgeState => {
  const { command } = payload;
  if (command.idempotencyKey === undefined || command.commandId === undefined) {
    return state;
  }

  const slack = state.slackByIdempotencyKey.get(command.idempotencyKey);
  return {
    ...state,
    commandIdByIdempotencyKey: new Map(state.commandIdByIdempotencyKey).set(
      command.idempotencyKey,
      command.commandId,
    ),
    slackByCommandId:
      slack === undefined
        ? state.slackByCommandId
        : new Map(state.slackByCommandId).set(command.commandId, slack),
  };
};

const rememberSlackIngress = (
  state: SlackBridgeState,
  payload: SlackMessageReceivedPayload,
): SlackBridgeState => {
  const commandId = state.commandIdByIdempotencyKey.get(payload.relatedCommandIdempotencyKey);
  return {
    ...state,
    slackByIdempotencyKey: new Map(state.slackByIdempotencyKey).set(
      payload.relatedCommandIdempotencyKey,
      payload,
    ),
    slackByCommandId:
      commandId === undefined
        ? state.slackByCommandId
        : new Map(state.slackByCommandId).set(commandId, payload),
  };
};

const attachUserMessageToSlackMessage = (
  state: SlackBridgeState,
  payload: UserMessageCommittedPayload,
): SlackBridgeState => {
  const slack = state.slackByCommandId.get(payload.commandId);
  if (slack === undefined) {
    return state;
  }
  return {
    ...state,
    slackByUserMessageId: new Map(state.slackByUserMessageId).set(payload.messageId, slack),
  };
};

const rememberRunCommands = (
  state: SlackBridgeState,
  payload: RunStartedPayload,
): SlackBridgeState => ({
  ...state,
  commandIdsByRunId: new Map(state.commandIdsByRunId).set(payload.runId, payload.commandIds),
});

const attachAssistantMessageToSlackThread = (
  state: SlackBridgeState,
  payload: AssistantMessageCommittedPayload,
): SlackBridgeState => {
  const slack = slackForRun(state, payload.runId);
  if (slack === undefined) {
    return state;
  }
  return {
    ...state,
    slackByAssistantMessageId: new Map(state.slackByAssistantMessageId).set(
      payload.messageId,
      slack,
    ),
  };
};

const markSlackReplyDelivered = (
  state: SlackBridgeState,
  payload: SlackReplyDeliveredPayload,
): SlackBridgeState => ({
  ...state,
  deliveredByAssistantMessageId: new Map(state.deliveredByAssistantMessageId).set(
    payload.assistantMessageId,
    payload,
  ),
});

export const slackForRun = (
  state: SlackBridgeState,
  runId: RunId,
): SlackMessageReceivedPayload | undefined => {
  const commandIds = state.commandIdsByRunId.get(runId) ?? [];
  for (const commandId of commandIds) {
    const slack = state.slackByCommandId.get(commandId);
    if (slack !== undefined) {
      return slack;
    }
  }
  return undefined;
};
