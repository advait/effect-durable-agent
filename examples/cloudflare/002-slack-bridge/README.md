# 002 — Slack Bridge

A focused example of EDA’s end-to-end durability story:

```text
Slack webhook retry
  -> idempotent EDA command admission
  -> Slack metadata committed in the same durable batch
  -> framework + app reducers derive the bridge state
  -> assistant message commits durably
  -> durable sink posts the Slack reply
  -> SlackReplyDelivered is staged back into the same session log
```

The point is not Slack specifically. The point is that EDA gives you one ordered state-machine history for ingress, agent execution, app metadata, sink delivery, retries, and multi-client sync.

## What to notice

### 1. Ingress is one durable batch

`worker.ts` submits the model command and Slack metadata together:

```ts
const idempotencyKey = CommandIdempotencyKey.make(`slack:event:${slackEventId}`);

await session.submitBatch({
  sessionId,
  items: [
    new SubmitMessageCommand({
      idempotencyKey,
      disposition: "queue",
      content: [Prompt.textPart({ text })],
    }),
    SlackEvents.messageReceived({
      relatedCommandIdempotencyKey: idempotencyKey,
      teamId,
      channelId,
      threadTs,
      slackEventId,
      slackUserId,
      text,
      sessionId,
      eventId,
      createdAtMs,
    }),
  ],
});
```

If the process crashes after this commit, both the EDA command and the Slack metadata survive. If Slack retries the webhook, the same idempotency key identifies the same ingress fact.

### 2. The reducer reads framework events and app events

`reducer.ts` is the heart of the example. It derives mappings across both event families:

```text
SlackMessageReceived.relatedCommandIdempotencyKey
  -> CommandAdmitted.command.commandId
  -> UserMessageCommitted.messageId
  -> RunStarted.commandIds
  -> AssistantMessageCommitted.messageId
  -> SlackReplyDelivered.slackMessageTs
```

This is the pattern you want in real integrations: do not hide correlation state in callback-local variables. Put the durable facts in the log and derive the bridge state with a pure reducer.

### 3. The sink is reliable and idempotent

`sinks.ts` watches for `AssistantMessageCommitted`. It uses reducer state to find the Slack thread, posts a reply with a deterministic outbound idempotency key, and stages `SlackReplyDelivered` only after the send succeeds.

If posting fails, the sink cursor does not advance and EDA retries. If posting succeeds but the worker dies before the delivery event commits, the deterministic outbound key lets the retry collapse into one Slack-visible reply.

### 4. Multi-client sync is a free consequence

A browser transcript, an internal support console, and the Slack bridge are all consuming projections over the same ordered sequence. Render through `seq = N`, request `events > N`, and converge.

## Files

- `events.ts` — Slack durable event schemas and constructors.
- `reducer.ts` — pure bridge projection over framework + Slack events.
- `sinks.ts` — durable Slack reply sink with at-least-once retry semantics.
- `scenario.test.ts` — executable scenarios for reducer correlation and sink retry.
- `worker.ts` — minimal Cloudflare Worker/Durable Object facade.

## Example event sequence

```text
seq  event                                      why it matters
---  -----------------------------------------  -----------------------------------------
1    effect-durable-agent/CommandAdmitted       idempotent model ingress
2    example.slack/SlackMessageReceived         Slack identity committed beside command
3    effect-durable-agent/UserMessageCommitted  durable user transcript
4    effect-durable-agent/RunStarted            links command ids to a run
5    effect-durable-agent/AssistantMessageCommitted final answer is durable
6    example.slack/SlackReplyDelivered          outbound delivery is durable too
```

## Wrangler binding sketch

```jsonc
{
  "main": "./examples/cloudflare/002-slack-bridge/worker.ts",
  "durable_objects": {
    "bindings": [
      {
        "class_name": "SlackBridgeEDASession",
        "name": "SlackBridgeEDASession"
      }
    ]
  },
  "migrations": [
    {
      "new_sqlite_classes": ["SlackBridgeEDASession"],
      "tag": "eda-slack-bridge-v1"
    }
  ]
}
```

## Example request

```bash
curl -X POST \
  http://localhost:8787/sessions/018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a/slack/events \
  -H 'content-type: application/json' \
  -d '{
    "teamId":"T1",
    "channelId":"C1",
    "threadTs":"1729.000",
    "slackEventId":"Ev123",
    "slackUserId":"U1",
    "text":"Please summarize this incident."
  }'
```
