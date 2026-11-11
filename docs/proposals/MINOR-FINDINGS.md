# Minor findings in 0.1.0

Status: proposal. Nothing here is implemented.

The request reported findings 1 and 2. Findings 3 and 4 came later, from the typed docs-site code
(`docs-site/research/handoff-types/REVIEW.md` in the Gia repository, branch
`t3/handoff-effect-types` at `3f68e50a1`). All four are true. Finding 3 loses data silently and
should be fixed in the next release.

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

## 3. App event payloads are stored as raw JSON, without the registered schema

**True, and it loses data without an error.** `encodeEventLogRow` encodes the envelope with
`DurableEventEnvelope`, whose `payload` is `Schema.Unknown`, and then calls `JSON.stringify`. The
schema an app registers through `appEvents` is used to decode on read. It is never used to encode
on write. So a payload must already be JSON when it reaches `submit`. A value whose decoded type
differs from its encoded type is written as whatever `JSON.stringify` makes of it.

**Reproduction.** A scratch test against the real SQLite store
(`SqliteDurableObjectStorage` and `DurableObjectSessionStore.make`), run in this checkout at
`v0.1.0` plus one docs commit. The event's payload schema is
`Schema.Struct({ email: Schema.Redacted(Schema.String), note: Schema.Option(Schema.String) })`,
and the event is built with `Redacted.make("a@b.co")` and `Option.some("hi")`.

```text
$ pnpm --filter effect-durable-agent-cloudflare exec vp test run src/zz-payload-repro.test.ts
REGISTERED APPEND: ok
RAW STORED: [{"fact_json":"{\"email\":\"<redacted>\",\"note\":{\"_id\":\"Option\",\"_tag\":\"Some\",\"value\":\"hi\"}}"}]
```

The append through `registerAppEvents` succeeds. The stored row holds the string `<redacted>` in
place of the email. The address is gone. The scratch file is not committed, because this branch
changes no source.

**Who is affected.** Gia is not, today. Its payloads are JSON-native, so the decoded and encoded
forms are equal. The docs-site hit it and made every payload JSON-native (`null` for a missing
value, a plain string for the email, redaction at the log call). Any app that follows the Gia
coding standard "one schema per value" with `Redacted`, `Option`, `Date` or a branded class will
hit it.

**Fix, in two steps.**

1. **Fail loudly, now.** When an app schema is registered, `registerAppEvents.append` checks that
   each registered event's payload survives `JSON.parse(JSON.stringify(payload))` unchanged. If
   not, the submit fails with a `SessionCommandAdmissionConflict`, which is not fatal to the
   runtime. This turns silent loss into an error at the call site. It ships with FR-1. Half a day.
2. **Encode on write, later.** Decide that a registered event is encoded with its schema before
   the append, in-process and across the RPC. FR-1's decider path already does this for its
   drafts. Doing it for `submit` changes what the RPC carries, so it needs its own design.

## 4. Tools get no reducer state

**True.** `EDAToolExecutionContext` in `src/services/tool-registry.ts` offers `queryEvents`,
`emitDurable`, ids and recovery data. It has no snapshot. The typed docs-site tools fold the whole
journal on every call to learn the phase.

**Fix.** Two additions to the tool context.

- `decide`, specified in [FR-1](FR-1-deciders.md) under "Tools decide too". It is the right call
  when the tool emits a fact that depends on state.
- `snapshot()`, returning the same `{ state, reducerStates }` as `EDARuntime.snapshot`. It is
  for a tool that only reads, for example to prefill a form. Its documentation says the state can
  be stale by the time the tool emits, and points to `decide`.

Gia's tools do not read reducer state today, so there is one consumer. Both additions are a few
lines, because `SessionState` already has both functions. They ship with FR-1.
