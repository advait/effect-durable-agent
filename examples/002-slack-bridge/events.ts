import * as Schema from "effect/Schema";

import { CommandIdempotencyKey } from "effect-durable-agent/types/commands";
import { EventId, MessageId, SessionId } from "effect-durable-agent/types/core";
import {
  DurableEventEnvelope,
  EventNamespace,
  EventType,
  UnixEpochMillis,
  makeRootEDAEventTrace,
  schemaV1,
} from "effect-durable-agent/types/events";

/** App namespace used by the Slack bridge example. */
export const slackBridgeNamespace = EventNamespace.make("example.slack");

/** Durable app event type values used by this example. */
export const slackMessageReceivedEventType = EventType.make("SlackMessageReceived");
export const slackReplyDeliveredEventType = EventType.make("SlackReplyDelivered");

export const SlackTeamId = Schema.NonEmptyString.pipe(Schema.brand("SlackTeamId"));
export type SlackTeamId = typeof SlackTeamId.Type;

export const SlackChannelId = Schema.NonEmptyString.pipe(Schema.brand("SlackChannelId"));
export type SlackChannelId = typeof SlackChannelId.Type;

export const SlackUserId = Schema.NonEmptyString.pipe(Schema.brand("SlackUserId"));
export type SlackUserId = typeof SlackUserId.Type;

export const SlackEventId = Schema.NonEmptyString.pipe(Schema.brand("SlackEventId"));
export type SlackEventId = typeof SlackEventId.Type;

export const SlackMessageTs = Schema.NonEmptyString.pipe(Schema.brand("SlackMessageTs"));
export type SlackMessageTs = typeof SlackMessageTs.Type;

export const SlackThreadTs = Schema.NonEmptyString.pipe(Schema.brand("SlackThreadTs"));
export type SlackThreadTs = typeof SlackThreadTs.Type;

export const OutboundSlackIdempotencyKey = Schema.NonEmptyString.pipe(
  Schema.brand("OutboundSlackIdempotencyKey"),
);
export type OutboundSlackIdempotencyKey = typeof OutboundSlackIdempotencyKey.Type;

/** Durable app event payload recorded atomically with the EDA command. */
export const SlackMessageReceivedPayload = Schema.Struct({
  relatedCommandIdempotencyKey: CommandIdempotencyKey,
  teamId: SlackTeamId,
  channelId: SlackChannelId,
  threadTs: SlackThreadTs,
  slackEventId: SlackEventId,
  slackUserId: SlackUserId,
  text: Schema.NonEmptyString,
});
export type SlackMessageReceivedPayload = typeof SlackMessageReceivedPayload.Type;

/** Durable app event payload recorded after the outbound Slack send succeeds. */
export const SlackReplyDeliveredPayload = Schema.Struct({
  assistantMessageId: MessageId,
  teamId: SlackTeamId,
  channelId: SlackChannelId,
  threadTs: SlackThreadTs,
  slackMessageTs: SlackMessageTs,
  outboundIdempotencyKey: OutboundSlackIdempotencyKey,
});
export type SlackReplyDeliveredPayload = typeof SlackReplyDeliveredPayload.Type;

interface AppEventContext {
  readonly createdAtMs: UnixEpochMillis;
  readonly eventId: EventId;
  readonly sessionId: SessionId;
}

/** Constructors for Slack-specific durable events. */
export const SlackEvents = {
  messageReceived: (input: AppEventContext & SlackMessageReceivedPayload): DurableEventEnvelope => {
    const { createdAtMs, eventId, sessionId, ...payload } = input;
    return slackDurableEvent({
      createdAtMs,
      eventId,
      payload: SlackMessageReceivedPayload.make(payload),
      sessionId,
      type: slackMessageReceivedEventType,
    });
  },

  replyDelivered: (input: AppEventContext & SlackReplyDeliveredPayload): DurableEventEnvelope => {
    const { createdAtMs, eventId, sessionId, ...payload } = input;
    return slackDurableEvent({
      createdAtMs,
      eventId,
      payload: SlackReplyDeliveredPayload.make(payload),
      sessionId,
      type: slackReplyDeliveredEventType,
    });
  },
};

const slackDurableEvent = (input: {
  readonly createdAtMs: UnixEpochMillis;
  readonly eventId: EventId;
  readonly payload: unknown;
  readonly sessionId: SessionId;
  readonly type: EventType;
}): DurableEventEnvelope =>
  DurableEventEnvelope.make({
    namespace: slackBridgeNamespace,
    type: input.type,
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: input.eventId,
    sessionId: input.sessionId,
    createdAtMs: input.createdAtMs,
    trace: makeRootEDAEventTrace(),
    payload: input.payload,
  });
