# 001 — No Tools

The smallest useful EDA host: one Durable Object-backed session, no tools, and a tiny HTTP facade.

## What to notice

This example exists to make the baseline durable-agent flow obvious:

```text
POST /sessions/:sessionId/messages
  -> SubmitMessageCommand
  -> effect-durable-agent/CommandAdmitted
  -> model runs
  -> effect-durable-agent/AssistantMessageCommitted
```

The app code does not save a final chat array by hand. It submits a command. EDA admits that command durably before execution starts, owns the run/turn/message lifecycle, and exposes durable messages afterward.

## Files

- `worker.ts` — concrete Durable Object subclass plus a minimal HTTP route.
- `../_shared/*` — boring example plumbing shared by later examples.

This example intentionally stays free of custom reducers, app events, tools, and sinks. Those appear in later examples.

## HTTP facade

- `POST /sessions/:sessionId/messages`
  - body: `{ "text": "hello", "idempotencyKey": "optional-retry-key" }`
  - returns immediately after durable admission
- `GET /sessions/:sessionId/messages`
  - returns durable user/assistant messages in committed order

`sessionId` must be a UUIDv7 because EDA validates lifecycle ids at ingress.

## Wrangler binding sketch

```jsonc
{
  "main": "./examples/001-no-tools/worker.ts",
  "durable_objects": {
    "bindings": [
      {
        "class_name": "NoToolsEDASession",
        "name": "NoToolsEDASession"
      }
    ]
  },
  "migrations": [
    {
      "new_sqlite_classes": ["NoToolsEDASession"],
      "tag": "eda-no-tools-v1"
    }
  ]
}
```

## Example request

```bash
curl -X POST \
  http://localhost:8787/sessions/018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a/messages \
  -H 'content-type: application/json' \
  -d '{"text":"Say pong in one sentence.","idempotencyKey":"demo-message-1"}'
```
