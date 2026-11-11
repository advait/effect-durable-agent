# Minor findings in 0.1.0

Status: proposal. Nothing here is implemented.

Both findings in the request are true. Both are small, and both fold into the two approved
features.

## 1. `submit` accepts framework events from app code

**True.** `submitBatchAdmitted` in `src/services/session-state.ts` passes every envelope straight
to the append. `EDARpcSubmittable` in the Cloudflare RPC codec is `EDACommand | DurableEventEnvelope`
with no namespace check. App code can append `RunCompleted`. That breaks invariant 8, "no lifecycle
events from app code", which today is a convention.

**It cannot be a plain rejection.** Gia depends on the opening. It submits the framework event
`SystemMessageCommitted` on every session seed, with the session id as its event id
(`gia-cf/server/features/agent-session/eda/system-prompt.ts`), and `AssistantMessageImported` in
its scenario fixtures.

**Fix.** An allowlist, specified in [FR-1](FR-1-deciders.md) under "Namespace rule", because
deciders need the same rule. It ships with FR-1. Size: half a day, plus a grep of Gia for other
framework events passed to `submit`.

## 2. A sink's staged events and its cursor commit are separate transactions

**True.** In `processDurableBatch` in `src/services/sink-registry.ts` the runner calls
`appendDurableBatch(staged)` and then `commitSinkCheckpoint`. A crash between them redelivers the
batch, and the sink stages its events again. Gia's Slack sink stages
`GiaSlackThreadBindingCommitted` with a fresh id, so that crash appends a second binding.

A related fact from the same function: a defect in `process` discards the staged events and still
advances the cursor. That is deliberate. [FR-4](FR-4-sink-wakeups.md) makes it visible.

**Two fixes were considered.**

| Fix | Verdict |
| --- | --- |
| Commit staged events and the cursor in one transaction. | Rejected for now. The cursor goes through `SinkCheckpointStore` and the events through `EDASessionStore`. Joining them needs a combined operation on both ports and in both stores. |
| Give staged events a dedupe key and document the contract. | Chosen. [FR-5](FR-5-event-dedupe-keys.md) provides the key. The duplicate collapses in the store. |

The second fix also covers what a single transaction cannot: the external side effect that ran
before the crash still runs again, so the sink must be idempotent in any case. The documentation
change is listed in FR-5.
