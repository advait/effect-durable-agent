# FR-1. Deciders: decide and commit under one gate

Status: proposal, approved to build. Nothing here is implemented.
Targets: the release after `0.1.0` (proposed `0.2.0`).
Build order: second. It needs [FR-5](FR-5-event-dedupe-keys.md).

## The problem

Ingress often has to choose what to append from the session's current state. Route this message to
the AI or to the human agent. Seed the session only if it has no seed. Stop the run only if it is
still the caller's run.

In EDA 0.1.0 the app does this in two calls: `snapshot()`, then `submit(items)`. Nothing ties them
together. `spec.md` §7 already states the rule this breaks: "decisions that mutate state happen
inside the serialized session boundary". EDA follows that rule for its own commands and gives apps
no way to follow it.

A lock in the app does not close the gap. The docs-site proposal plans a promise lock in the
Durable Object subclass around the two calls. That serializes ingress against ingress. It does not
serialize ingress against the writers inside EDA. A tool's `emitDurable`, a sink's staged events
and the control loop all append through `SessionState`'s append gate, which the app cannot hold.
So a tool can commit `ConversationEnded` between the snapshot and the submit, and the submit then
admits a `SubmitMessage` into an ended chat. Only the runtime can make the read and the append one
step.

## Who needs it

| Consumer | The race today | Where |
| --- | --- | --- |
| Gia Slack ingress | Reads `snapshot()` to learn whether the session needs its seed, then submits. Two first messages can both seed. Deterministic ids hide the duplicate system prompt, and the metadata and model events duplicate. It also pays a full snapshot over RPC for every Slack event. | `gia-cf/server/features/agent-session/eda/slack-ingress.server.ts` |
| Gia subagent cancel | Reads `snapshot()`, and if an owned command is active submits `StopTurn`. If that command ends and another starts in between, the stop hits the wrong run. | `gia-cf/server/features/agent-session/eda/subagent-session.server.ts`, `stopOwned` |
| docs-site | Every ingress RPC routes on reducer state: `postMessage`, `acceptOffer`, `submitForm`, `dismissForm`, `endChat`. | `EDA-PROPOSAL.md` §3 |
| Any approval gate or budget hold | Accept only if still pending. Refuse `SubmitMessage` when the budget is spent. | |

This is a runtime feature. It cannot be an optional helper, because the gate is private to
`SessionState`.

## Designs considered

| Design | Verdict |
| --- | --- |
| A. Registered pure deciders that run inside the append gate, with a durable acceptance marker. | Chosen. |
| B. Compare-and-append: `submit(items, { expectedHeadSeq })`, and the caller retries on conflict. | Rejected as the primary API. A subsumes it. |
| C. An app-level lock around `snapshot()` and `submit()`. | Rejected. It cannot exclude tools, sinks or the control loop. |
| D. Submit a request fact and let a reducer and a sink react. | Rejected. Sinks must not submit commands (`docs/subagents.md` §5.5), and the decision becomes asynchronous. |

**A against B.** B is the smaller change to EDA. The decision stays in app code and may do I/O
between the read and the write. Its costs fall on every caller. Each one writes a retry loop. A
conflict fires on any event, including ones the decision does not read. It takes two round trips,
and in Gia each is a Worker to Durable Object RPC that serializes the whole snapshot. It also
leaves retries unsafe: a retried call decides again on newer state, so it can emit different
events the second time.

A decider is a pure function of state, so it is a unit test and a model-checking target with no
runtime. The docs-site `route(state, input)` is already written that way. And A contains B. An app
that wants compare-and-append registers a five-line decider that accepts only when
`view.state.lastSeq` equals the expected value.

The cost of A is that `decide` cannot do I/O. The caller loads what the decision might need and
passes it in the input, or the decider rejects with a reason that tells the caller what to load.
Gia's Slack seed below shows both.

## Public API

New module `packages/effect-durable-agent/src/services/decider.ts`, exported as the subpath
`effect-durable-agent/services/decider`.

```ts
/** What a decider reads: the fold of every committed event at the moment of the decision. */
export interface EDADeciderView {
  readonly sessionId: SessionId;
  /** The runtime clock, read once. The same value stamps the events this decision commits. */
  readonly nowMs: number;
  readonly state: ReducedState;
  readonly reducerStates: EDAReducerStateSnapshot;
}

/** A durable event as a decider writes it. The runtime fills in the rest. */
export type EDAEventDraft<E extends DurableEventEnvelope = DurableEventEnvelope> = Omit<
  E,
  "eventId" | "sessionId" | "createdAtMs" | "trace" | "durability"
> & { readonly eventId?: EventId };

export type EDADeciderItem<E extends DurableEventEnvelope> = EDACommand | EDAEventDraft<E>;

export type EDADecision<E extends DurableEventEnvelope, Result, Rejection> =
  | {
      readonly _tag: "Accept";
      readonly items: ReadonlyArray<EDADeciderItem<E>>;
      readonly result: Result;
    }
  | { readonly _tag: "Reject"; readonly rejection: Rejection };

export interface EDADeciderDefinition<
  Name extends string,
  Input,
  E extends DurableEventEnvelope,
  Result,
  Rejection,
> {
  /** Stable. Matches /^[A-Za-z0-9._-]{1,128}$/. It is stored in the log. */
  readonly name: Name;
  readonly input: Schema.Codec<Input, unknown, never, never>;
  /** The events this decider may emit. Drafts are checked against it before the append. */
  readonly events: Schema.Codec<E, unknown, never, never>;
  /** Stored in the log and returned again on a retry. Keep it small and backward readable. */
  readonly result: Schema.Codec<Result, unknown, never, never>;
  readonly rejection: Schema.Codec<Rejection, unknown, never, never>;
  /** Required. 1 to 256 characters, no line feed. One acceptance per (name, key) in a session. */
  readonly idempotencyKey: (input: Input) => string;
  /** Pure and synchronous. No clock, no ids, no I/O. It runs inside the append gate. */
  readonly decide: (view: EDADeciderView, input: Input) => EDADecision<E, Result, Rejection>;
}

export const EDADecider: {
  define: <const Name extends string, Input, E extends DurableEventEnvelope, Result, Rejection>(
    definition: EDADeciderDefinition<Name, Input, E, Result, Rejection>,
  ) => EDADecider<Name, Input, Result, Rejection>;
};

export type EDADeciderOutcome<Result, Rejection> =
  | {
      readonly _tag: "Accepted";
      readonly result: Result;
      /** Position of the acceptance marker. */
      readonly acceptedSeq: SequenceNumber;
      /** True when this call found an earlier acceptance and committed nothing. */
      readonly replayed: boolean;
    }
  | { readonly _tag: "Rejected"; readonly rejection: Rejection };

export class EDADeciderError extends Schema.TaggedError<EDADeciderError>()("EDADeciderError", {
  decider: Schema.String,
  reason: Schema.Literals(["unknown_decider", "invalid_input", "decide_threw", "invalid_output"]),
  message: Schema.String,
}) {}
```

On `EDARuntimeShape` (`services/runtime.ts`):

```ts
readonly decide: <Input, Result, Rejection>(
  decider: EDADecider<string, Input, Result, Rejection>,
  input: Input,
) => Effect.Effect<
  EDADeciderOutcome<Result, Rejection>,
  EDASessionStoreError | SessionCommandAdmissionConflict | EDADeciderError
>;
```

Deciders are registered once, like reducers and sinks. `EDARuntimeLayerOptions`, the Cloudflare
and celld runtime options, and `makeEdaTestLayer` gain `deciders?: ReadonlyArray<EDADecider>`.
Registration fails at construction on a duplicate name or a name that starts with `_eda.`.

### What a decider may return

- **Commands.** Any `EDACommand` that `submit` accepts. They are admitted by the same code as
  `submit`, including `idempotencyKey` lookup and the paused-queue checks.
- **Event drafts.** Events of the decider's `events` schema, without `eventId`, `sessionId`,
  `createdAtMs`, `trace` or `durability`. The runtime mints the id, uses `view.nowMs` as
  `createdAtMs` and takes the trace from the call. A draft may set `eventId` when the app needs a
  fixed id, as Gia's system prompt does. A draft may set `dedupeKey`.
- The namespace rule for drafts is the rule for `submit` (see "Namespace rule" below).

A decider needs ids that it cannot mint. The caller mints them and passes them in the input. That
is deliberate. A retry carries the same input, so it carries the same ids.

### Tools decide too

`EDAToolExecutionContext` gains the same call, bound to the session.

```ts
readonly decide: <Input, Result, Rejection>(
  decider: EDADecider<string, Input, Result, Rejection>,
  input: Input,
) => Effect.Effect<EDADeciderOutcome<Result, Rejection>, EDASessionStoreError | EDADeciderError>;
```

A tool that emits a fact only when state allows it has the same race as ingress. The docs-site
`end_conversation` tool may end the chat only in an AI-owned phase. In 0.1.0 a tool gets no reducer
state at all, so the typed docs-site handlers fold the whole journal with `ctx.queryEvents` on
every call and then emit, with nothing tying the two together. With `ctx.decide` the handler
passes `toolCallId` in the input and uses it as the idempotency key. A call re-invoked under
`ToolRecovery: "reinvoke"` then finds the marker and returns the first result.

A decider called from a tool should return facts, not commands. A `StopTurn` admitted from inside
the run it stops is legal and confusing. [FR-9](FR-9-tool-ends-run.md) is the way for a tool to
end its run.

### Rejection is not durable

`Reject` commits nothing. A later call with the same key decides again. A caller that wants a
refusal on the record returns `Accept` with a fact that says so and a `result` that reports it.

### Host API

`packages/effect-durable-agent-cloudflare/src/rpc-spec.ts`:

```ts
export const SessionDecideInput = Schema.Struct({
  ...SessionScopedInput.fields,
  decider: Schema.NonEmptyString,
  input: Schema.Unknown,
});
export const SessionDecideResult = Schema.Union([
  Schema.TaggedStruct("Accepted", {
    result: Schema.Json,
    acceptedSeq: SequenceNumber,
    replayed: Schema.Boolean,
  }),
  Schema.TaggedStruct("Rejected", { rejection: Schema.Json }),
]);
// edaSessionRpc gains:  decide: { input: SessionDecideInput, output: SessionDecideResult }
```

`EDASessionController.decide` looks the decider up by name, decodes `input` with its schema, runs
`runtime.decide` and encodes the outcome with the decider's `result` or `rejection` schema.
`EDASessionDurableObject` exposes `decide(raw)` beside `submitBatch(raw)`. The package also exports
`encodeDecideInput(decider, input)` and `decodeDecideOutcome(decider, raw)` so a caller on the
other side of the RPC keeps its types. The RPC is trusted ingress, like `submitBatch`. The host
authorizes callers.

## The acceptance marker

Every acceptance commits one framework event first in its batch.

```ts
// types/events/durable.ts, namespace "effect-durable-agent", schemaVersion 1
export const DeciderAcceptedPayload = Schema.Struct({
  decider: Schema.NonEmptyString,
  idempotencyKey: Schema.NonEmptyString,
  result: Schema.Json,
});
```

Its envelope carries `dedupeKey = decider + "\n" + idempotencyKey`. A decider name cannot hold a
line feed, so the first line feed always ends the name and two different pairs cannot share a
key. The longest key is 128 + 1 + 256 = 385 characters, inside `EventDedupeKey`'s 512. The first
draft used `JSON.stringify([decider, idempotencyKey])`. JSON escaping can double or sextuple a
character, so a legal 256-character key produced a 647 or 1,671 character dedupe key that
`EventDedupeKey` rejects. FR-5's index makes the key unique and gives the lookup. No new table is
needed.

The marker is in the log because the log is the source of truth. `CommandAdmitted` sets the
precedent: the fact is an event, and an index serves the lookup.

**Why a marker and not keys on the decided events.** Without it, a retry runs `decide` again on
newer state. Take docs-site `postMessage`. The first call sees an AI-owned chat and admits a
`SubmitMessage`. The reply is lost. A handoff starts. The retry sees a person-owned chat and emits
`UserMessagePosted` with an outbox item. The same message now goes to the AI and to Salesforce. The
marker records that this command was decided once, at one point in the log.

The typed docs-site code confirms the hazard in the other direction. Its `route` is pure and
decides by the phase at retry time, so a message first sent to the human agent goes to the AI on a
retry after the agent left (`docs-site/research/handoff-types/REVIEW.md`, "Commands and ingress",
question 2, on branch `t3/handoff-effect-types` at `3f68e50a1`). Its stopgap is a bounded set of
routed message ids in reducer state. The marker replaces that set.

**Why the idempotency key is required.** Every ingress call in Gia and in the docs-site design
already has one. One code path is simpler than two, and an ingress call without a retry story is a
defect waiting for a timeout.

**The input of a retry is not compared.** First acceptance wins. Storing an input digest and
failing on a mismatch was considered. It breaks the reject-then-retry flow below, where the second
attempt legitimately carries more input than the first.

## Admission algorithm

In `SessionState`, next to `submitBatchAdmitted`. Call it `decideAdmitted`.

1. Outside any gate, look the decider up, compute `key = idempotencyKey(input)`, and check the
   key against the rule above (1 to 256 characters, no line feed). A failure is `EDADeciderError` with `unknown_decider` or `invalid_input`.
2. Take `admissionGate`, then `withAppendGate`. These are the two gates `submitBatch` takes, in the
   same order. Startup recovery holds `admissionGate` while it plans and commits its repair, so a
   decision never reads state that recovery is about to change.
3. `store.findEventByDedupeKey` for the marker. If it exists, decode `payload.result` with the
   decider's `result` schema and return `Accepted` with `replayed: true`. `decide` is not called.
4. Read `state` and the clock. Build the view. Call `decide` inside `Effect.try`. A throw is
   `decide_threw`.
5. On `Reject`, return `Rejected`. Nothing is committed and the control loop is not woken.
6. On `Accept`:
   - Encode `result`. Encode every draft through `events`, and append the encoded form. This is
     the write path that minor finding 3 asks for, so a decider's events may use any schema.
     Then check the namespace rule. Check that the encoded result is at most 16 KiB and that
     each encoded draft is within `maxAppEventBytes` (see "Size limit" below). A failure is
     `invalid_output`.
   - Admit each command with the existing `prepareCommandAdmission`. A
     `SessionCommandAdmissionConflict` fails the whole call.
   - Commit `[DeciderAccepted, ...items in order]` with `commitDurableEntriesWithinGate`. That is
     one store transaction.
7. If the batch held a command, call `wakeAfterCommandAdmission`.
8. Return `Accepted` with the marker's `seq` and `replayed: false`.

No decider error is an `EDASessionStoreError`. That matters, because `withAppendGate` records a
store error as fatal for the live runtime. A bad input or a throwing decider must leave the runtime
usable.

### Size limit

The store refuses a row over its JSON cap (1,500,000 bytes in the Cloudflare store), and it
refuses with an `EDASessionStoreError`. Inside `withAppendGate` that is fatal. The review proved
it: a schema-valid draft 11 bytes over the cap failed the decision, and the next small `submit`
failed with the same error. Plain `submit` has the same defect in 0.1.0.

So the size check moves in front of the store. Core exports `maxAppEventBytes` (1 MiB, below
every host's cap). `submit` and `decide` both measure each app event's serialized payload before
the append. `submit` fails with a `SessionCommandAdmissionConflict` whose code is
`event_too_large`. `decide` fails with `invalid_output`. Neither is a store error, so neither is
fatal. A store that still refuses a row after this check has a real storage fault, and fatal is
the right answer for that.

## Why the decision is atomic

`state` is the `Ref` that holds `ReducedState` and the app reducer states. It is written in two
places, `publishFreshDurables` and `publishEphemeralCore`, and both run inside `withAppendGate`.
Every durable append in the runtime goes through `commitDurableEntriesWithinGate` under the same
gate. So inside the gate, `state` is exactly the fold of the committed log, and it cannot change
until the gate is released. The decision reads it and appends before releasing.

`decide` is synchronous and runs with the gate held. A slow decider delays every append in the
session, including the model's. Keep deciders to a pure computation over state.

## What atomic does not cover

A command that a decider returns is admitted in the batch. It runs later, on the control loop. A
`StopTurn` in the batch does not stop the run inside the batch. The run can commit one more event
before `StopTurnRequested`. A decider guarantees which facts and commands entered the log together.
It does not hold the run. That is [FR-3](FR-3-hold.md) and
[FR-9](FR-9-tool-ends-run.md).

## Crash semantics

| Crash point | State after restart | A retry with the same key |
| --- | --- | --- |
| Before the append | Nothing committed. | Decides on current state. |
| During the append | The transaction rolls back. Nothing committed. | Decides on current state. |
| After the append, before the reply | Marker and items committed. Recovery treats admitted commands as it does today. | Finds the marker. Returns the stored result with `replayed: true`. Commits nothing. |

There is no state in which the marker exists without its items or the items without the marker.

## Effect on existing invariants

- **`SessionState` is the write authority (invariant 2).** Unchanged. The decision runs inside it.
- **Pure decisions stay pure (invariant 5).** Deciders join reducers and policies. The signature
  is synchronous and receives no services.
- **No lifecycle events from app code (invariant 8).** Enforced for the first time by the
  namespace rule.
- **Admission.** Commands in a decision take the same path as `submit`. `_eda_command_state` and
  command idempotency are unchanged.
- **`ReducedState`.** `foldReducedState` ignores `DeciderAccepted`. The checkpoint schema does not
  change.
- **Recovery.** `planSessionRecovery` ignores the marker.
- **Transcript.** The interpreter adds `DeciderAccepted` to the events it ignores on purpose. No
  existing log contains it and it projects to nothing, so `transcriptSemanticRevision` and the
  recorded fingerprints do not change. The implementer confirms that with
  `src/transcript/fingerprint.test.ts`.
- **Sinks.** A marker advances the head like any event. A sink that takes `"*"` sees it.
- **Wire.** Raw event sockets carry one new framework event type. Gia's clients read transcript
  sockets, which never carry it. A consumer of raw event sockets with an older schema must be
  checked before release.
- **Storage.** No new table. FR-5's column and index carry the marker's key.
- **Forward only.** A 0.1.0 runtime cannot decode `DeciderAccepted`. After the first acceptance in
  production there is no rollback to 0.1.0. The rollout below keeps that step separate.

## Namespace rule

This is minor finding 1 from the request, and deciders need it. `submitBatchAdmitted` passes any
envelope through, so app code can submit `RunCompleted` or a forged `DeciderAccepted` today.

`submit` and decider drafts reject an event in the `effect-durable-agent` namespace unless its type
is on an allowlist. The allowlist is `SystemMessageCommitted`, `SessionConfigured` and
`AssistantMessageImported`. Gia submits the first two in the seed batch of every new session
(`system-prompt.ts`, and `makeEDAGiaSessionConfigured` in `model.ts`, called from `submission.ts`,
`slack-ingress.server.ts` and `subagents/launch.server.ts`). It submits the third in its scenario
fixtures. The rejection is a new `SessionCommandAdmissionConflict` code, `framework_event`, so it
is not fatal to the runtime.

The first draft of this rule left out `SessionConfigured`. The review applied the rule as written
and it rejected the seed batch of every new web, cron, Slack and subagent session. A grep is
therefore not enough to fix the list. The gate is step 3 of the release plan: Gia's full test
suite runs against the packed build with the rule on, and any `framework_event` rejection there
is a missing allowlist entry or a Gia change to make. Test fixtures that need arbitrary framework
events seed the store with `InMemorySeeded`.

## Tests

- **Service contract**, new `session-state-decider.test.ts` beside `session-state-admission.test.ts`:
  - a tool that calls `ctx.decide` with its `toolCallId` as the key, is interrupted and re-invoked,
    commits one marker;
  - accept commits the marker and the items contiguously in one append and wakes the control loop
    when a command is present;
  - reject commits nothing and the head does not move;
  - a replay returns the stored result, does not call `decide`, and commits nothing, in a case
    where state has changed so that `decide` would now return different items;
  - a key with a line feed or of 257 characters is `invalid_input`, and a key of 256 double
    quotes is accepted and replays;
  - a draft over `maxAppEventBytes` is `invalid_output`, and so is the same event through
    `submit` as `event_too_large`, both on the real SQLite store;
  - Gia's seed batch (`SessionConfigured`, `SystemMessageCommitted`, a `SubmitMessage`) is
    accepted through `submit` and as decider drafts;
  - `decide_threw`, `invalid_input`, `invalid_output` and an inner `queue_paused` conflict each
    commit nothing, and a following `submit` succeeds, which proves the runtime is not fatal;
  - a draft in the framework namespace outside the allowlist is rejected, and so is the same event
    through `submit`.
- **Model test.** `session-state-control.model.test.ts` gains a `Decide` action in its generated
  command and control sequences, interleaved with tool `emitDurable` and staged sink events. The
  oracle is replay determinism: for every `DeciderAccepted` at `seq` s, fold the log through s - 1,
  call `decide` with that state, the recorded input and the marker's `createdAtMs`, and require
  exactly the items that follow the marker. This is the atomicity claim as a property.
- **Crash sweep.** `session-state-crash-simulation.test.ts` gains a scenario with a decision that
  admits a `SubmitMessage`. At every durable batch prefix, recovery succeeds, a retry of the
  decision converges, and the log ends with one marker and one admission.
- **Transcript.** `transcript-scenarios.ts` gains a log that contains a marker. The property
  oracle and the fingerprint guard pass unchanged.
- **Host conformance.** `testing/host-conformance/suite.ts` calls the `decide` RPC on workerd and
  celld: accept, hard restart, replay.
- **Types.** A type test next to `durable-object.type-test.ts` proves a decider cannot return a
  draft outside its `events` schema and that `decodeDecideOutcome` returns the declared types.
- **Package.** `mise run package:check` covers the new subpath in the `exports` map.

## Gia adoption

Gia becomes a consumer in the same lockstep, not only a version bump.

### Slack ingress moves onto a decider

Define `gia.slack.ReceiveMessage` beside the Slack ingress reducer in
`gia-cf/server/features/session-core/`.

- **Input.** The normalized message, the event key, and an optional `seed` with what the Worker
  loads today: prompt profile, model, source metadata, optional history capture.
- **Key.** Today's `slackCommandIdempotencyKey`.
- **Decide.**
  - If the metadata reducer shows no session and `seed` is absent, reject with `NeedsSeed`.
  - If it shows no session and `seed` is present, emit the seed events first.
  - Always emit the author event, the attachment events, the `SubmitMessage` with
    `disposition: "steer"` and the same idempotency key, and the receipt event.
- **Caller.** `submitEDAGiaSlackMessagesEffect` calls `decide` without a seed. On `NeedsSeed` it
  loads the profile and calls again with it.

What this removes: the `agent.snapshot()` RPC on every Slack event, `mintEventIdsEffect` and the
event-id bookkeeping, the double-seed race, and the duplicate author, attachment and receipt
events on a Slack redelivery.

`SystemMessageCommitted` and `SessionConfigured` are framework events in that batch. Both are on
the allowlist. The system prompt's draft keeps its fixed `eventId`.

### Second candidate, as a follow-up

`stopOwned` in `subagent-session.server.ts` becomes a decider `gia.subagent.StopOwned` that takes
the owned command ids, cancels their pending messages and admits `StopTurn` only if one of them is
the active command. The caller then waits with `blockOnCommand`. It fixes a real race, but it is a
larger rewrite of a sensitive path. It should follow the Slack change, not ship with it.

## Release and lockstep plan

Follows `docs/releasing.md` and the **release** skill. The rule is that gia-cf and docs-site run
one EDA version.

1. **EDA PR 1, FR-5.** Reviewed by a model that did not write it, because it changes a durable
   format. `mise run ci` passes.
2. **EDA PR 2, FR-1 and the namespace rule.** Same review. `mise run ci` passes.
3. **Validate in Gia before any release.** Pack the three packages from the PR 2 head
   (`pnpm run build`, then `pnpm pack` in each). On a Gia branch, point the catalog at the
   tarballs, apply the adoption changes, and run `mise run //gia-cf:check` and
   `mise run //gia-cf:test`. Drive a Slack message and a cron launch against the local dev server
   with the Gia **verify** skill. Record the commands and results in EDA PR 2. This branch is
   evidence only. It never merges.
4. **Version PR.** `0.2.0` in all three manifests, lockfile, `CHANGELOG.md`. It is a minor
   release: new API, one breaking rule (framework events through `submit`), one forward-only
   storage migration.
5. **Publish.** Only with the maintainer's go-ahead that names `v0.2.0`. Merge, wait for CI on
   `master`, create the GitHub release, wait for `Publish to npm`, verify the registry.
6. **Gia PR A, the pin.** `effect-durable-agent` to `0.2.0` in the catalog and the submodule at the
   `v0.2.0` commit. Move `effect-durable-agent-cloudflare` into the catalog and make gia-cf and
   docs-site both use `catalog:` for both packages. Extend `gia-cf/scripts/check-eda-release.mjs`
   to read the docs-site manifest too, so a second version cannot appear. No adoption code. This
   deploy writes nothing that 0.1.0 cannot read, so it can be rolled back. It is not a no-op.
   The namespace rule, the size limit and the payload check from minor finding 3 are active on
   unchanged Gia code. Step 3 is what proves they reject nothing Gia sends.
7. **Gia PR B, the adoption.** The FR-5 changes and the Slack decider. After its first accepted
   decision in production, rolling back to PR A is safe and rolling back to 0.1.0 is not.
8. **Production check for PR B.** One Slack message to Gia and one cron run. In the session's log,
   one `DeciderAccepted` precedes the Slack batch, and a replayed Slack event adds no events.

## Size and what it unblocks

Medium. About three days for EDA with tests, and one to two days for the Gia Slack change. It
unblocks every docs-site ingress RPC and removes the planned Durable Object lock.

## Open points for the implementer

- The cap on `result` (16 KiB here) is a guess. Pick it from the largest real result.
- Whether `EDARuntime.decide` should also accept a decider that is not registered. Recommended no.
  One registry keeps the RPC and the in-process call identical.
