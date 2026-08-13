import * as Prompt from "effect/unstable/ai/Prompt";

import { CommandIdempotencyKey, SubmitMessageCommand } from "effect-durable-agent/types/commands";
import { SessionId } from "effect-durable-agent/types/core";
import { UnixEpochMillis } from "effect-durable-agent/types/events";
import { makeRootEDATraceMetadata } from "effect-durable-agent/types/tracing";
import {
  EDASessionDurableObject,
  getEDASessionDurableObjectByName,
} from "../../src/durable-object";
import { encodeEdaRpcSubmittables } from "../../src/durable-object-runtime";
import { mintExampleEventId } from "../_shared/event-id";
import { json, parseJsonObject, pathParam, requiredString } from "../_shared/http";
import { makeExampleOpenAiOptions, type ExampleOpenAiEnv } from "../_shared/openai";
import {
  SlackChannelId,
  SlackEventId,
  SlackEvents,
  SlackTeamId,
  SlackThreadTs,
  SlackUserId,
} from "./events";
import { SlackBridgeReducer } from "./reducer";
import { loggingSlackClient, makeSlackReplySink } from "./sinks";

/** Bindings used by the Slack bridge EDA example Worker. */
export interface SlackBridgeEDAEnv extends ExampleOpenAiEnv {
  readonly SlackBridgeEDASession: DurableObjectNamespace<SlackBridgeEDASession>;
}

/** Concrete session object with Slack reducer + reply sink installed. */
export class SlackBridgeEDASession extends EDASessionDurableObject<SlackBridgeEDAEnv> {
  constructor(ctx: DurableObjectState, env: SlackBridgeEDAEnv) {
    super(ctx, env, {
      ...makeExampleOpenAiOptions(env),
      reducers: [SlackBridgeReducer],
      sinks: [makeSlackReplySink(loggingSlackClient)],
    });
  }
}

/**
 * Minimal Slack-ish facade.
 *
 * The important move is submitting `[command, SlackMessageReceived]` together:
 * one durable admission boundary captures both the model input and its Slack
 * identity/idempotency metadata.
 */
export default {
  async fetch(request, env): Promise<Response> {
    const url = new URL(request.url);

    const rawSlackSessionId = pathParam(url.pathname, /^\/sessions\/([^/]+)\/slack\/events\/?$/);
    if (request.method === "POST" && rawSlackSessionId !== null) {
      const body = await parseJsonObject(request);
      const parsed = parseSlackEvent(body);
      if ("error" in parsed) {
        return json({ error: parsed.error }, 400);
      }

      const sessionId = SessionId.make(rawSlackSessionId);
      const session = getEDASessionDurableObjectByName(env.SlackBridgeEDASession, sessionId);
      const idempotencyKey = CommandIdempotencyKey.make(`slack:event:${parsed.slackEventId}`);
      const trace = makeRootEDATraceMetadata();

      const committed = await session.submitBatch({
        sessionId,
        trace,
        items: encodeEdaRpcSubmittables([
          new SubmitMessageCommand({
            idempotencyKey,
            disposition: "queue",
            content: [Prompt.textPart({ text: parsed.text })],
          }),
          SlackEvents.messageReceived({
            relatedCommandIdempotencyKey: idempotencyKey,
            teamId: SlackTeamId.make(parsed.teamId),
            channelId: SlackChannelId.make(parsed.channelId),
            threadTs: SlackThreadTs.make(parsed.threadTs),
            slackEventId: SlackEventId.make(parsed.slackEventId),
            slackUserId: SlackUserId.make(parsed.slackUserId),
            text: parsed.text,
            createdAtMs: UnixEpochMillis.make(Date.now()),
            eventId: await mintExampleEventId(),
            sessionId,
          }),
        ]),
      });

      return json({ committed, messages: await session.messages({ sessionId, trace }) }, 202);
    }

    const rawMessagesSessionId = pathParam(url.pathname, /^\/sessions\/([^/]+)\/messages\/?$/);
    if (request.method === "GET" && rawMessagesSessionId !== null) {
      const sessionId = SessionId.make(rawMessagesSessionId);
      const session = getEDASessionDurableObjectByName(env.SlackBridgeEDASession, sessionId);
      return json(await session.messages({ sessionId, trace: makeRootEDATraceMetadata() }));
    }

    return json(
      {
        error: "Use POST /sessions/:sessionId/slack/events or GET /sessions/:sessionId/messages",
      },
      404,
    );
  },
} satisfies ExportedHandler<SlackBridgeEDAEnv>;

interface ParsedSlackEvent {
  readonly teamId: string;
  readonly channelId: string;
  readonly threadTs: string;
  readonly slackEventId: string;
  readonly slackUserId: string;
  readonly text: string;
}

const parseSlackEvent = (
  body: Record<string, unknown>,
): ParsedSlackEvent | { readonly error: string } => {
  const teamId = requiredString(body, "teamId");
  const channelId = requiredString(body, "channelId");
  const threadTs = requiredString(body, "threadTs");
  const slackEventId = requiredString(body, "slackEventId");
  const slackUserId = requiredString(body, "slackUserId");
  const text = requiredString(body, "text");
  if (
    teamId === undefined ||
    channelId === undefined ||
    threadTs === undefined ||
    slackEventId === undefined ||
    slackUserId === undefined ||
    text === undefined
  ) {
    return {
      error:
        "Expected JSON body with teamId, channelId, threadTs, slackEventId, slackUserId, and text",
    };
  }
  return { teamId, channelId, threadTs, slackEventId, slackUserId, text };
};
