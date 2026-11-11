# FR-5. Dedupe keys for durable events

Status: proposal, approved to build. Nothing here is implemented.
Targets: the release after `0.1.0` (proposed `0.2.0`).
Build order: first. [FR-1](FR-1-deciders.md) depends on it.

## The problem

An app often learns the same fact twice. A webhook is redelivered. A tool is re-invoked after a
crash. A sink is re-run because its cursor did not advance. The app needs the second append to
collapse into the first.

EDA 0.1.0 collapses appends only by `eventId`. `insertOrRead` in
`packages/effect-durable-agent-cloudflare/src/durable-object-store.ts` reads the row with that
`event_id` and returns it. It compares nothing else. `EventId` must be a UUIDv7
(`packages/effect-durable-agent/src/types/core.ts`). So an app that holds a natural key, such as a
Slack event key or a Salesforce entry id, has to turn it into a UUIDv7.

Three things go wrong with that.

- **The id is forged.** A UUIDv7 carries a timestamp. A hash-derived one carries a fake timestamp,
  and the app must keep that timestamp stable across retries or the ids differ.
- **A collision is silent.** Two different facts with the same id are not an error. The store
  keeps the first and drops the second. The docs-site review hit this with `FormSubmitted` and
  `HandoffRequested`, both derived from `formId`.
- **Every app invents its own scheme.** Gia already has three, listed below.

## Who needs it

| Consumer | What it does today | Where |
| --- | --- | --- |
| Gia cron launch | Builds two event ids by replacing the last character of the session id, because the session id itself is already used as the system prompt's event id. | `gia-cf/server/features/agent-session/eda/cron-session.server.ts`, `cronMetadataEventIdFromSessionId` |
| Gia Slack delivery sink | Stages `GiaSlackThreadBindingCommitted` with a fresh id from `ctx.makeEventId()`. A crash between the staged append and the cursor commit stages a second binding. | `gia-cf/server/features/slack/eda/sinks/slack.server.ts` |
| Gia Slack ingress | Mints fresh ids for the author, attachment and receipt events of every batch. A redelivered Slack event dedupes its command by `idempotencyKey` but appends those events again. | `gia-cf/server/features/agent-session/eda/slack-ingress.server.ts` |
| Gia subagents | Reuses the notification id and the command id as event ids. It works only because both are UUIDv7. | `gia-cf/server/features/session-core/events.ts` |
| docs-site | Plans a hash-derived UUIDv7 for every inbound Salesforce entry, form fact and timer fact. | `EDA-PROPOSAL.md` §2 |

The request is general. It belongs in the store, because only the store can make "insert unless
present" atomic with the append.

## Designs considered

| Design | Verdict |
| --- | --- |
| A. An optional `dedupeKey` on the durable envelope, unique per `(namespace, type, dedupeKey)` in one session log. | Chosen. |
| B. A documented helper `EventId.fromKey(namespace, key, atMs)` that hashes a key into a UUIDv7. | Rejected. |
| C. Dedupe only whole commands, through FR-1's marker. | Rejected as the only mechanism. |

**Why not B.** It keeps the forged timestamp. It needs a SHA-256, which is async on Workers, at
every call site. It keeps the silent collision, because the store still compares only the id. It
is a workaround with a name.

**Why not C alone.** Tools (`emitDurable`) and sinks (`stageDurable`) append outside any command.
A re-invoked tool and a re-run sink need event-level dedupe.

**Why the type is part of the scope.** With `(namespace, dedupeKey)` two different facts that
share a natural key collide, which is the bug the docs-site review found. With
`(namespace, type, dedupeKey)` an app can use one natural key for every fact it derives from it.
An app that wants "first of X or Y wins" can give both the same type or put the choice in a
reducer.

## Choosing a key

The store can only enforce the key it is given. The typed docs-site code
(`docs-site/research/handoff-types/`, branch `t3/handoff-effect-types` at `3f68e50a1`) found two
ways to pick a wrong one. Both belong in the documentation for this feature.

- **Two facts from one natural key.** `FormSubmitted` and `HandoffRequested` both derive from
  `formId`. With a hand-made id the event type has to be hashed in, or the second fact is dropped.
  That code hashes `[namespace, kind, ...parts]` for this reason. Its `test/eda.test.ts` proves on
  EDA's in-memory runtime that both facts are stored and that a replayed batch adds no rows. The
  type in the triple makes this the default.
- **A fact that can happen again.** A key says "this happens once". `SalesforceWorkSettled` was
  keyed on `effectId`. A cleanup item that stays owed after an `Abandoned` outcome settles a
  second time, the second report collapsed into the first, and the item stayed owed forever. The
  property test in `test/rules.property.test.ts` found it. The key became `effectId` plus the
  outcome tag. The same holds for a heartbeat or a recurring timer, which needs its due time in
  the key.

The rule for the docs is short. The key names one occurrence. If the fact can recur for the same
subject, the key includes what distinguishes the occurrences.

## Public API

### Envelope

`packages/effect-durable-agent/src/types/events/envelope.ts`:

```ts
/** App-owned natural key. One event per (namespace, type, dedupeKey) in a session log. */
export const EventDedupeKey = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(512),
).pipe(Schema.brand("EventDedupeKey"));
export type EventDedupeKey = typeof EventDedupeKey.Type;

export const DurableEventEnvelope = Schema.Struct({
  ...EventEnvelope.fields,
  durability: Schema.Literal("durable"),
  dedupeKey: Schema.optionalKey(EventDedupeKey),
});
```

The field is on the durable envelope only. Ephemeral events never carry it. Framework event
schemas spread `DurableEventEnvelope.fields`, so they accept it too. FR-1 uses that.

Callers set it wherever they build an event today:

```ts
yield* runtime.submit([
  SalesforceEvents.agentMessageArrived({
    eventId: yield* ids.makeEventId(), // a fresh id; it no longer has to be derived
    dedupeKey: EventDedupeKey.make(entry.id),
    ...
  }),
]);
```

`ctx.emitDurable` in tools and `ctx.stageDurable` in sinks take the same envelope, so they need no
new method.

### Append contract

`EDASessionStoreShape.append` in `services/session-store.ts` changes its documented contract from
"idempotent by `eventId`" to this rule, applied to each entry in order inside the one append
transaction:

1. If an event with this `eventId` exists, return it.
2. Else if the entry has a `dedupeKey` and an event with the same `(namespace, type, dedupeKey)`
   exists, return that event.
3. Else insert the entry.

A returned existing event keeps its own `eventId`, payload, `createdAtMs` and position. The new
entry's bytes are discarded. First write wins. The durable head does not move for a collapsed
entry, exactly as for a duplicate `eventId` today.

A caller can tell that an entry collapsed, because the returned event's `eventId` differs from the
one it submitted.

Two entries in one batch with the same key collapse the same way. The second resolves to the
first.

### Lookup

One new method on the store port. FR-1 needs it, and it is the read side of the same index.

```ts
readonly findEventByDedupeKey: (input: {
  readonly namespace: EventNamespace;
  readonly type: EventType;
  readonly dedupeKey: EventDedupeKey;
}) => Effect.Effect<CommittedDurableEvent | undefined, EDASessionStoreError>;
```

`registerAppEvents` wraps it like `findCommandAdmission`, so a registered app event comes back
decoded. It is not added to `EDARuntime`. An app that wants to know whether a fact landed submits
it and reads the returned event.

## Storage

### Cloudflare and celld

Migration 4 in `migrateDurableObjectSessionStore`:

```sql
ALTER TABLE _eda_event_log ADD COLUMN dedupe_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS _eda_event_log_dedupe_key_idx
  ON _eda_event_log(namespace, type, dedupe_key) WHERE dedupe_key IS NOT NULL;
```

Both statement kinds have precedent in this store. Migration 3 adds columns with `ALTER TABLE`,
and `_eda_command_state_idempotency_idx` is a partial unique index.

- `encodeEventLogRow` writes `dedupe_key` (or `NULL`).
- Every `SELECT` that lists event-log columns adds `dedupe_key`.
- `rowToCommittedDurableEvent` puts `dedupeKey` back on the envelope when the column is not
  `NULL`. The key must survive a read, because `eventsAfter` feeds replay, sinks, exports and the
  WebSocket.
- `insertOrRead` applies rule 2 with one indexed read before the insert.

The key gets a column, not a place in `fact_json`. `fact_json` holds only the payload, and the
store rebuilds the envelope from columns. (`correlationId` has no column today and is lost on a
read through this store. That is an existing gap, outside this change.)

**Migration cost.** `ADD COLUMN` rewrites no rows. Creating the partial index reads every existing
row once and writes no index entries, because every existing key is `NULL`. The implementer
measures this on the large-log fixture that `pnpm run test:replay-memory` uses and records the
number in the PR. If it is too slow for a Durable Object start, the fallback is a side table
`_eda_event_dedupe_keys(namespace, type, dedupe_key, seq)` with the triple as primary key, joined
on read.

**Rollback.** Storage is forward-only by policy. This migration is still safe under a one-step
rollback: 0.1.0 selects columns by name, ignores the new column, and its migration checks are
`currentVersion < N`. Events written with a key by 0.2.0 stay readable by 0.1.0, which ignores the
key.

### In-memory store

`makeInMemoryStore.append` applies the same three rules inside its `Ref.modify`.
`seedCommittedEvents` throws on a duplicate triple, as it does on a duplicate `eventId`.
`findEventByDedupeKey` scans `committed`.

## Invariants

| Invariant | Why it holds | Test |
| --- | --- | --- |
| At most one event per `(namespace, type, dedupeKey)` in a log. | The unique index, and the rule-2 read inside the append transaction. | Store contract. |
| A collapsed entry does not advance the head, is not folded again and is not published again. | `SessionState.publishFreshDurables` folds only `seq > lastSeq`, once per `seq` (see the fix below). A collapsed entry returns its old position. | Service contract. |
| A collapsed entry does not touch the transcript or the sidecar tables. | Projections and transcript maintenance run only for `inserted` events. | Store contract. |
| Sink cursors are unaffected. | No new event means no head advance and no notification. | Service contract. |
| Replay is unchanged. | The key is data on the event. Reducers never read it. | Existing replay tests still pass. |
| An event without a key behaves exactly as in 0.1.0. | Rule 2 is skipped. | Existing suites. |

## A defect in 0.1.0 that this feature would widen

`publishFreshDurables` keeps every returned entry whose `seq` is above the old head. It does not
remove repeats. When one batch holds the same event twice, the store returns the same position
twice, and `SessionState` folds and publishes that event twice. The live reducer state then
differs from what a replay computes. The review proved it on 0.1.0 with a duplicate `eventId`, on
both stores.

```text
same-batch collapse {"returnedSeqs":[1,1],"logRows":1,"liveCount":2,"replayCount":1,"publishedSeqs":[1,1]}
```

Today this needs a caller that repeats an `eventId` inside one batch, which is rare. Dedupe keys
make it ordinary, because two facts with one key in one batch is a normal thing to submit. FR-1's
atomicity argument also rests on live state being the fold of the log.

The fix is part of this feature. `publishFreshDurables` keeps one entry per `seq` before it folds
and publishes. The service-contract test below covers it.

## Tests

Each test goes to the suite that owns the behavior in `docs/testing.md`.

- **Store contract.** `durable-object-store.test.ts` gains a table that runs against both the
  SQLite store and `EDASessionStore.InMemory`. The rows are these. Same key and same type collapses; same key
  and different type inserts both; same key and different namespace inserts both; a duplicate
  inside one batch collapses; `eventId` match wins over key match; a collapsed entry writes no
  sidecar or transcript rows; `findEventByDedupeKey` returns the first event; a key of 513
  characters fails to decode.
- **Migration.** The same file opens a database created at schema version 3 with events in it,
  runs the migration, appends a keyed event and reads every old event back unchanged.
- **Service contract, same batch.** `session-state.test.ts` submits one batch that holds the same
  key twice, and one that holds the same `eventId` twice. The event is folded once and published
  once, and the live reducer state equals a replay of the log. Run on the in-memory store and on
  SQLite.
- **Service contract.** `session-state.test.ts` proves a collapsed app event is not published to
  `LiveEventBus` a second time and that `snapshot().state.lastSeq` does not move.
- **Sink replay.** `sink-replay.test.ts` gains the crash this feature exists for: a sink stages a
  keyed event, the process dies after the staged append and before the cursor commit, the sink
  runs again, and the log holds one event.
- **Tool recovery.** `session-state-tool-recovery.test.ts` gains a `reinvoke` tool that emits a
  fact keyed by `toolCallId`. After re-invocation the log holds one.
- **Schema contract.** `types/events.test.ts` decodes an envelope with and without the key.
- **Host conformance.** `testing/host-conformance/suite.ts` submits the same keyed event before
  and after a hard restart on workerd and celld and reads one event.

## Documentation changes in the same PR

- `docs/spec.md` §8 (envelope), §9 (append rule and the new column) and §13 (sinks).
- The sink section states the contract that closes minor finding 2: the runner commits staged
  events and the cursor in two transactions, so a staged event needs a `dedupeKey` derived from
  what caused it.
- `CHANGELOG.md` under the next version, with its three impacts. Storage has migration 4. The
  protocol has one optional envelope field on the wire. The API has one optional field and one
  store-port method.

## Gia adoption

These land in Gia after the pin bump. Each is small and each removes code.

1. `cron-session.server.ts`, in two releases. First, keep `cronMetadataEventIdFromSessionId` and
   `cronSessionConfiguredEventIdFromSessionId` and add `dedupeKey: "session-seed"` to both events.
   The type scopes the key, so one literal serves both. Later, when no launch from before that
   deploy can still retry, delete the two functions and mint the ids.

   The first draft deleted the functions at once and accepted one extra seed event for a launch
   that retries across the deploy. The review ran that against Gia's real metadata reducer. The
   reducer replaces the metadata on every seed, so the extra seed erased a Slack thread binding
   made in between (`afterMode: "proactive"`, `afterThread: null`). Keeping the old ids avoids it,
   because the store matches on `eventId` before the key.
2. `slack.server.ts`. Give the staged `GiaSlackThreadBindingCommitted` a `dedupeKey` equal to the
   `eventId` of the delivery event that caused it.
3. Slack ingress. It moves onto a decider in FR-1, which dedupes the whole batch. No event keys are
   needed there.

`events.ts` can keep `EventId.make(notificationId)`. It is correct, and changing it buys nothing.

## Size and what it unblocks

Small. About one day for EDA, and under half a day for the Gia changes. It unblocks FR-1's
idempotency, the docs-site inbound bridge, and the sink and timer helpers in FR-4 and FR-6.

## Open points for the implementer

- Whether `stageDurable` should log a warning for a staged event with no `dedupeKey`. Recommended
  yes. A type-level requirement would also force Gia's sink, which is fine in lockstep, but it is a
  separate decision.
- The measured migration time decides between the column and the side table.
