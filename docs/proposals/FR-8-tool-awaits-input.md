# FR-8. A tool call that awaits external input

Status: proposal. Not approved to build. Nothing here is implemented.

## The request

Let a tool call stay open until outside input arrives. The sketch is
`ctx.awaitExternal({ resumableId, expiresAtMs, facts })`, which puts the call in an `Awaiting`
state, and `runtime.resolveAwaited({ resumableId, result, resume })`, which commits
`ToolCallCompleted` with the real result and can continue the run. It also asks for a transcript
mapping target `ToolByCallId`.

## What the source shows

- A tool call has no waiting state. Its lifecycle is `ToolCallCreated`, `ToolCallStarted`, then
  `ToolCallCompleted` or `ToolCallFailed`.
- The transcript mapping targets are `UserByCommandId`, `UserByIdempotencyKey` and `Session`
  (`src/transcript/mapping.ts`). A mapping cannot decorate a tool record. That claim is true.
- EDA already has a written position on this. `docs/subagents.md` §5.2 is titled "Resumables are
  not tool calls". It says a tool call should not remain open while it waits. The tool opens a
  resumable, commits the fact, and completes with a small handle. A prototype of resumables exists
  on `origin/agent/durable-resumables` (commit `695bf88`, unmerged, based on alpha.10).

## Is it general

The need is general. Approvals, OAuth prompts, forms and subagent joins all wait for something
outside the run.

The requested mechanism is contested, and the evidence is against it for now.

- Two consumers already ship the workaround. Gia's subagents complete the spawn tool with a
  handle and wake the parent with an idempotent `SubmitMessage{queue}`
  (`subagent-session.server.ts`, `notify`). The docs-site plans the same for forms. Both work.
- What the workaround costs is fidelity. The model sees `{ status: "awaiting_user" }` and later a
  synthetic user message, not a tool result.
- What an open tool call costs is much more. Providers require a result for every tool call
  before the next request. So an awaiting call means the run stays open, or the prompt carries an
  unresolved call. Stop, interrupt, steering, compaction, the tool-call budget and tool recovery
  (`ToolRecovery: "reinvoke"`) each need a rule for a call that may wait for days. That is a new
  dispatch state in the center of the state machine.

## Designs considered

| Design | Verdict |
| --- | --- |
| A. An `Awaiting` tool state and `resolveAwaited`, as sketched. | Rejected. It contradicts the accepted resumables design and holds tool lifecycle open across runs. |
| B. Resumables, as designed in `docs/subagents.md` §5. | The right design for the general need. Build it when a consumer adopts it. |
| C. Keep the pattern, document it, and remove its two rough edges. | Chosen for now. |

## Chosen design

1. **Document the pattern** in `docs/spec.md` §12 as the supported way to wait. The tool commits a
   request fact with `emitDurable`, returns a handle, and the turn ends. The answer arrives through
   ingress. A decider ([FR-1](FR-1-deciders.md)) commits the answer fact and an idempotent
   `SubmitMessage` in one step, and refuses an answer for a request that is closed.
2. **Add the mapping target `ToolByCallId`.** A mapped app event that carries a `toolCallId`
   decorates that tool record, with the same operations and byte budget as user decorations. A
   decoration that arrives before the tool record is parked and applied, as for users. The form
   then anchors on the tool call that opened it.
3. **Let [FR-9](FR-9-tool-ends-run.md) end the turn.** A form tool marked `end_run` gives the
   silence the request wants, without the closing sentence.

**Durability and invariants.** Nothing changes in the runtime for 1 and 3. Item 2 is a transcript
interpretation change. A definition that uses the new target has a new version and rebuilds.
Definitions that do not use it keep their fingerprints.

**Migration for Gia.** None. Gia could map its subagent-created fact onto the spawn tool record
later.

**Verification.** Item 2 gets transcript scenarios (decoration before and after the tool record,
unknown tool call id, oversize value) and the existing property oracle. In Gia, the transcript
tests pass unchanged.

**When to build B.** When Gia's subagents or an approval flow commit to it. The trigger is a
consumer that needs the model to see a real resolution, or needs a session to stay `waiting`
against unrelated messages. Start from the existing design and prototype branch.

## Priority and size

P3. The mapping target is small, about one to two days. Resumables are large, more than a week,
and are not part of this recommendation.
