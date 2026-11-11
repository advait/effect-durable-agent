# FR-3. Hold, a session that must not start runs

Status: proposal. Not approved to build. Nothing here is implemented.

## The request

Let the framework enforce "the AI is quiet while a person owns the chat". The sketch is the
resumables design in `docs/subagents.md` §5, or a smaller variant where `RunScheduler.resolve`
receives read-only `reducerStates` and can return `Hold`.

## What the source shows

- `RunSchedulingInput` in `src/domain/run-scheduling.ts` is `{ sessionId, commandId }`. The claim
  that `resolve` cannot see app state is true.
- A hold already exists under another name. `RunScheduler.Deferred` commits a
  `RunSchedulingRequested` reservation and starts nothing until a trusted `grantRun` arrives. The
  reservation survives a restart. Stop and interrupt invalidate it. Gia runs every session this
  way for capacity (`gia-cf/server/features/agent-session/eda/sinks/run-capacity.server.ts`).
- An explicit `StopTurn` pauses queued work, and a paused queue refuses new `queue` submissions
  with `queue_paused` (`prepareCommandAdmission`). Recovery honors a pending stop and does not
  continue the run (`spec.md` §15).

## Is the problem real

Yes, but it is narrower than the request says. Most of it closes with
[FR-1](FR-1-deciders.md) and [FR-9](FR-9-tool-ends-run.md).

- "Enforced only by our routing." With a decider, routing and the append are one step, so no
  `SubmitMessage` can be admitted on stale state.
- "A recovered queued message can still start a run." When the handoff enters through ingress, its
  batch carries `StopTurn`. That pauses the queue, and recovery respects it.
- The one extra inference after a tool result is FR-9.

One case is left. A message is queued while the AI is mid-turn. In that turn a tool ends the
conversation. No `StopTurn` is involved, so the queue is not paused, and after `RunCompleted` the
queued message starts a new run. Routing cannot stop it, because the message was admitted before
the end. Only the run-start boundary can.

The wider need is also real. A session that waits for an approval or for subagents should not start a
run because an unrelated message arrived. That is the `waiting` dispatch state in the resumables
design, which the maintainer has already written and prototyped (`origin/agent/durable-resumables`,
commit `695bf88`, unmerged, based on alpha.10).

## Designs considered

| Design | Verdict |
| --- | --- |
| A. Resumables with a `waiting` dispatch state. | The right long-term design. Not now. |
| B. Pass a read-only view to `RunScheduler.resolve`. A hold is `Deferred` plus a grant the app withholds. | Chosen. |
| C. A new `Hold` resolution that starts nothing and requests nothing. | Rejected. Nothing would wake it. The control loop would have to re-resolve on every event. |

**Why not A now.** It changes the central dispatch table, stop and interrupt semantics, recovery
and the prompt. It has one planned consumer, Gia subagents, which shipped without it by waking the
parent with a queued message. Build it when a consumer adopts it, from the existing design.

## Chosen design

Add the state to the input `resolve` already receives.

```ts
export interface RunSchedulingInput {
  readonly sessionId: SessionId;
  readonly commandId: CommandId;
  /** The state the run would start from. Read-only. */
  readonly state: ReducedState;
  readonly reducerStates: EDAReducerStateSnapshot;
}
```

An app that wants a hold returns `Deferred` while its reducer says "held" or "ended", and `Local`
otherwise.
Its `deliver` records nothing external. When the hold lifts, trusted ingress calls `grantRun` for
the reserved request. Every other piece already exists. The reservation is durable and survives a
restart, a stop invalidates it, and a stale grant is rejected.

- **Invariants.** `resolve` is already called by `SessionState` at the run-start boundary with the
  canonical state in hand. Passing it changes no ordering. The fields are additive.
- **Durability.** Unchanged. The reservation is the durable hold.
- **Migration for Gia.** None. Its scheduler ignores the new fields.
- **Verification.** `run-scheduler.test.ts` and `run-authorization.test.ts` gain a scheduler that
  defers on a reducer flag. The crash sweep proves a held run stays held across every prefix and
  starts after the grant. Gia's `run-capacity.test.ts` passes unchanged.

## Limits

`resolve` runs only when a new run starts. It does not run for a steer that joins an active run
(`spec.md` §7). So this hold covers queued messages and new submissions. It does not cover a steer
that is pending when a tool ends the conversation. An app that needs that case submits with
`queue`.

## Priority and size

P2. Small, about one day. Together with FR-1 and FR-9 it makes "no AI output after the end" a
framework guarantee for an app that submits with `queue`. Resumables stay a separate, larger
decision.
