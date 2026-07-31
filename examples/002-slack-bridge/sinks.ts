import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { getEDAReducerState } from "../../src/services/reducer-registry";
import { EDASink } from "../../src/services/sink-registry";
import {
  AssistantMessageCommittedPayload,
  UnixEpochMillis,
  assistantMessageCommittedEventType,
} from "../../src/types/events";
import {
  OutboundSlackIdempotencyKey,
  SlackChannelId,
  SlackEvents,
  SlackMessageTs,
  SlackTeamId,
  SlackThreadTs,
} from "./events";
import { SlackBridgeReducer } from "./reducer";

/** External Slack API seam used by the durable sink. */
export interface SlackClient {
  readonly postThreadReply: (input: {
    readonly teamId: SlackTeamId;
    readonly channelId: SlackChannelId;
    readonly threadTs: SlackThreadTs;
    readonly idempotencyKey: OutboundSlackIdempotencyKey;
    readonly text: string;
  }) => Effect.Effect<SlackMessageTs, unknown>;
}

/**
 * Durable sink: after an assistant message commits, deliver it to Slack.
 *
 * If delivery fails, the sink cursor does not advance and EDA will retry. If
 * delivery succeeds, the sink stages `SlackReplyDelivered` in the same session
 * log so future retries and all clients can see the durable delivery fact.
 */
export const makeSlackReplySink = (client: SlackClient): EDASink =>
  EDASink.make({
    name: "example.slack.reply-delivery",
    durable: {
      interests: [assistantMessageCommittedEventType],
      process: (batch, ctx) =>
        Effect.gen(function* () {
          const state = getEDAReducerState(batch.reducerStates, SlackBridgeReducer);

          for (const entry of batch.events) {
            const assistant = decodeAssistantMessage(entry.event.payload);
            if (assistant === undefined) {
              continue;
            }
            if (state.deliveredByAssistantMessageId.has(assistant.messageId)) {
              continue;
            }

            const slack = state.slackByAssistantMessageId.get(assistant.messageId);
            if (slack === undefined) {
              continue;
            }

            const outboundKey = OutboundSlackIdempotencyKey.make(
              `${ctx.sessionId}:${assistant.messageId}`,
            );
            const slackMessageTs = yield* client.postThreadReply({
              teamId: slack.teamId,
              channelId: slack.channelId,
              threadTs: slack.threadTs,
              idempotencyKey: outboundKey,
              text: renderAssistantText(assistant),
            });

            yield* ctx.stageDurable(
              SlackEvents.replyDelivered({
                assistantMessageId: assistant.messageId,
                teamId: slack.teamId,
                channelId: slack.channelId,
                threadTs: slack.threadTs,
                slackMessageTs,
                outboundIdempotencyKey: outboundKey,
                createdAtMs: UnixEpochMillis.make(Date.now()),
                eventId: yield* ctx.makeEventId(),
                sessionId: ctx.sessionId,
              }),
            );
          }
        }),
    },
  });

export const loggingSlackClient: SlackClient = {
  postThreadReply: (input) =>
    Effect.gen(function* () {
      yield* Effect.logInfo("Would post Slack thread reply", input);
      return SlackMessageTs.make(`reply-${input.idempotencyKey}`);
    }),
};

const decodeAssistantMessage = (payload: unknown): AssistantMessageCommittedPayload | undefined =>
  Schema.is(AssistantMessageCommittedPayload)(payload) ? payload : undefined;

export const renderAssistantText = (payload: AssistantMessageCommittedPayload): string => {
  const text = payload.promptParts
    .filter((part) => part.type === "text")
    .map((part) => part.text)
    .join("\n")
    .trim();
  return text.length === 0 ? "(assistant response had no visible text)" : text;
};
