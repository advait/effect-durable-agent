# FR-9. A tool that ends its run

Status: proposal. Recommended next after FR-1 and FR-5. Nothing here is implemented.

## The request

Let a tool end its run with no further inference. The sketch is a tool annotation
`ToolContinuation: "end_run"`, and optionally a per-call form where the returned value asks for
it.

## What the source shows

The claim is true, and it is one branch in a pure function. `decideRunContinuation` in
`src/domain/run-continuation-policy.ts` returns `ContinueWithToolFeedback` whenever a created tool
call in the turn has a terminal. A run can never complete on a tool result. The docs-site review
reproduced the consequence. After an `end_conversation` tool committed `ConversationEnded`, the
runtime ran one more inference and committed one more assistant message.

`StopTurn` is not a substitute. It is an interruption. It records `RunFailed` with
`run.interrupted` and pauses the queue.

## Is it general

Yes. "The tool's result is the answer" is a common shape. A final-answer tool, a handoff tool, a
form tool and a tool that starts background work all have it. Gia's spawn tool is one. Today the model writes a
closing line after it because the framework makes it. The cost of the missing feature is a wasted
inference on every such call, and for the docs chat a broken rule (no AI output after the end).

It belongs in the runtime. Continuation is a framework decision that apps cannot override.

## Designs considered

| Design | Verdict |
| --- | --- |
| A. A static tool annotation, read from the registry when the policy runs. | Rejected as stated. The policy would depend on the deployed registry, so replay and recovery after a deploy could decide differently. |
| B. A static annotation that the runtime records on `ToolCallCompleted`. The policy reads the event. | Chosen. |
| C. A per-call request, from the handler's return value or `ctx.endRun()`. | Deferred. Add it when a tool needs to choose per call. It would record the same field. |
| D. The tool asks ingress to submit `StopTurn`. | Rejected. Wrong terminal, and it races the next inference. |

## Chosen design

**Declaration.** A Context reference beside `ToolRecovery` in
`src/services/tool-registry.ts`:

```ts
export type ToolContinuationPolicy = "continue" | "end_run";
export const ToolContinuation = Context.Reference<ToolContinuationPolicy>(
  "@effect-durable-agent/ToolContinuation",
  { defaultValue: () => "continue" },
);
```

**Durable record.** `ToolCallCompletedPayload` gains an optional field
`continuation: Schema.optionalKey(Schema.Literal("end_run"))`. `ToolExecutor` stamps it when the
tool's annotation is `end_run` and the call succeeded. It is optional and decoded with retained
excess, as the coding standards require for a new event field. `ReducedState` keeps it on the
tool-call record.

**Policy.** `decideRunContinuation` changes in one branch. Today, when tool feedback is ready and
no steer is pending, it returns `ContinueWithToolFeedback`. With this change it returns
`CompleteRun` there if a created call in the turn completed with `continuation: "end_run"` and no
call in the turn was rejected.

- A failed call never ends the run. The model should see the failure and react.
- A rejected call in the same turn still gets its corrective turn.
- A pending steer still wins. The branch that returns `ContinueWithSteering` is unchanged, so a
  run never completes with an eligible steer left behind. An app that needs silence even then
  submits with `queue`, as the docs-site does.
- Queued messages are untouched. After `RunCompleted` they start the next run as today. An app for
  which the tool also means "no more runs" pairs it with a hold ([FR-3](FR-3-hold.md)).

All calls of the turn still execute and commit their results before the decision, as today.

**Prompt.** The run ends with a tool result as the last context message. The next run's prompt
then has a tool result followed by a user message. The OpenAI and Anthropic APIs accept that
order. The implementer confirms it with the opt-in provider smoke.

**Recovery.** This is the part that needs care. A crash after `ToolCallCompleted` with `end_run`
and before `RunCompleted` leaves an open run. Today `planSessionRecovery` would continue that
command in a replacement run, which is one more inference, the exact thing this feature removes.
The recovery policy must treat a run whose last turn holds an `end_run` completion as complete. It
terminalizes the turn and the run as completed and starts no replacement.

**Invariants.** One active run and one active turn are unchanged. Started boundaries still
terminalize. The decision stays pure and reads only `ReducedState`. A log without the field
behaves exactly as today.

**Transcript.** The run group completes normally. No interpretation rule changes, so no
`transcriptSemanticRevision` bump. The fingerprint guard confirms it.

**Migration for Gia.** None is required. Gia can mark its spawn tool `end_run` later if the closing
line is not wanted. That is a product choice.

**Verification.**

- Pure unit. Rows in `run-continuation-policy.test.ts` for each case above. The property test
  gains the field in its generator.
- Canned-model journey in `turn-runner.test.ts`. A model that calls an `end_run` tool gets no
  second `InferenceStarted`, and the run commits `RunCompleted`.
- Crash sweep in `session-state-crash-simulation.test.ts`. At every prefix of that journey,
  recovery never starts an inference after the `end_run` completion.
- `offline-trace.test.ts` proves the next run's prompt after an `end_run` run is well formed.
- In Gia, `mise run //gia-cf:test` passes with no tool marked, which proves the default is
  unchanged.

## Priority and size

P1, next after FR-1 and FR-5. Small to medium, about two days. It removes the inference after an
ending tool and the hidden trailing message from the docs-site design. With
[FR-3](FR-3-hold.md) it makes "no AI output after the end" hold for queued messages too.
