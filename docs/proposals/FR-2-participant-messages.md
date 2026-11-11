# FR-2. External participants as transcript messages

Status: proposal. Not approved to build. Nothing here is implemented.

## The request

Record "a support agent said X" as a positioned transcript record. The sketch is a framework event
`ParticipantMessageCommitted` with a transcript record kind `participant`, a `modelVisibility` of
`hidden` or `context`, and a prompt projector that renders `context` messages as a neutral block.

## What the source shows

The claims are true.

- The transcript record kinds are closed. `src/transcript/records.ts` defines `user`, `assistant`,
  `compaction`, `tool`, `runGroup` and `sessionContext`.
- Mappings only decorate. `TranscriptMappingTarget` in `src/transcript/mapping.ts` is
  `UserByCommandId`, `UserByIdempotencyKey` or `Session`. A mapping cannot create a record.
- `AssistantMessageImported` needs `runId`, `turnId` and `inferenceId` and renders as the
  assistant.

## Is it general

Partly. Split the request in two.

**A positioned, app-owned transcript record.** This is general, and it belongs in EDA. Gia has the
same need. Its Slack threads hold messages from people who are not talking to the bot. Gia records
them as `GiaSlackHistoryCaptured` and has no way to place them in the transcript. Its subagent
notices are app events that the UI has to place by hand. The docs-site needs agent messages, form
cards and handoff status lines. The transcript exists so a client reads one ordered source. An app
that merges transcript records with its own facts by `seq` in the browser is rebuilding the
transcript outside it.

**A participant as a framework concept with model visibility.** This is one app's domain. "Who
spoke, in what voice, and does the AI see it" differs per app. Gia shows other people's Slack
messages to the model through its own prompt projector (`slack-history-prompt.ts`). The docs-site
wants the AI to see the agent's messages only after the handoff ends. Neither wants the other's
rule. Prompt projection is already an app extension point. Model-facing context is also the
riskiest place to add a framework rule, because of invariant 11 (prompt prefixes do not change
outside compaction).

## Designs considered

| Design | Verdict |
| --- | --- |
| A. `ParticipantMessageCommitted` framework event, `participant` record kind, `context` projection. | Rejected. It puts one app's vocabulary and a prompt rule in the framework. |
| B. A generic app record in the transcript: a new mapping operation that turns a mapped app event into a top-level record. | Chosen. |
| C. Leave it to apps. | Rejected for the record. Kept for model visibility. |

## Chosen design

Add one mapping operation to `TranscriptDefinition`. Call it `CreateRecord`.

```ts
// A mapping on (namespace, type, schemaVersion), as today, with a new operation:
{
  _tag: "CreateRecord",
  appKind: "docs.handoff.agentMessage", // opaque to EDA, the client switches on it
  id: { path: ["messageId"] },          // record id is `app:<appKind>:<id>`
  body: { path: [] },                   // payload subtree stored as the record body, bounded
}
```

- The interpreter writes a record `{ kind: "app", appKind, id, body, updatedSeq }` positioned at
  the event's `seq`, like any top-level record.
- A later mapped event with the same id replaces the body and bumps `updatedSeq`. The position
  does not move. That covers "form opened" then "form submitted" on one card.
- The body is normalized JSON under the existing decoration byte budget. An oversize body skips
  the operation, as oversize decorations do today.
- Wire readers that do not know `app` already decode an unknown kind as an explicit `unknown`
  record that keeps its id and position (`spec.md` §14). Old clients degrade, they do not break.
- The model path does not change. Prompts are built from `ReducedState`, which never reads
  transcript records. An app that wants the model to see these messages does it in its prompt
  projector, from its own reducer, as Gia does today.

**Durability and replay.** The transcript is a projection of the log, so there is no new source
of truth. A definition with a new mapping has a new version hash, and sessions that use it rebuild
through the existing builder. Sessions on definitions without the mapping do not rebuild. The
interpreter change itself must leave existing fingerprints unchanged.

**Migration for Gia.** None required. Gia could later map `GiaSubagentNotificationCommitted` to an
app record and delete its client-side placement.

**Verification.** A scenario in `transcript-scenarios.ts` per rule (create, update by id, oversize
body, record before and after compaction). The property oracle in
`interpreter.property.test.ts` covers incremental against rebuild. `durable-object-transcript.test.ts`
covers the SQL projection. A window read test proves paging over app records. In Gia, the
transcript tests pass unchanged, which proves a definition without the mapping is untouched.

## Priority and size

P2. Medium, about three to four days, most of it in transcript tests. It lets the docs chat render
one ordered source. The docs chat can ship without it by merging in the client.

A mapping target `ToolByCallId` is a related small addition. See [FR-8](FR-8-tool-awaits-input.md).
