# FR-4. Durable sink wakeups and an outbox helper

Status: proposal. Not approved to build. Nothing here is implemented.

## The request

Three claims and two APIs. Sinks run only when the log advances. They cannot park. A defect drops
staged events. Read-before-retry is hand-written in every app. The sketch is `ctx.wakeAt(ms, key)`
and an `EDAOutboxSink.make` helper with `execute`, `reconcile`, a schedule and `onSettled`.

## What the source shows

Read against `src/services/sink-registry.ts`.

- **"Sinks run only when the log advances."** True for a sink that returns. It is not the whole
  picture. A sink may retry inside `process` for as long as it likes. The runner holds a
  keep-alive lease around `process`, the heartbeat alarm keeps the object alive, and after an
  eviction the alarm rebuilds the runtime and the sink runs again because its cursor did not
  advance. Gia's capacity sink does exactly that with an unbounded `Effect.retry`
  (`run-capacity.server.ts`). So a transient failure does not have to "wait for the next event".
  It waits only if the sink gives up and returns.
- **"They cannot park."** True, and this is the real gap. Retrying inside `process` keeps the
  Durable Object awake with a heartbeat every 30 s and blocks that sink's lane. That is fine for
  seconds. It is the wrong tool for "try again in ten minutes" or "wake me at 14:00".
- **"A defect drops staged events."** True. On a defect, `completed` is false, the staged events
  are not committed, and the cursor still advances. The skip is deliberate, so one bad batch
  cannot hold a lease forever. But it is silent beyond one log line, and silent loss is the most
  expensive failure in this project.
- **"Read-before-retry is hand-written."** True. Gia has five sinks with five retry schedules
  (`run-capacity`, `postgres`, `session-events`, `usage-events`, `slack`). The Slack sink carries
  a comment that an error can follow a partial success.

## Is it general

Yes, in two parts that belong in different places.

- **A durable wakeup for a sink** belongs in the runtime. Only the runtime can share the platform
  alarm. `DurableObjectAlarmOwner` is a closed set, and an app that calls `setAlarm` itself
  overwrites EDA's alarm.
- **The outbox policy** (attempt counts, backoff, reconcile before retry, give up) is policy over
  public API. It belongs in an optional helper module. Putting it in the runner would force one
  retry model on every sink.

## Designs considered

| Design | Verdict |
| --- | --- |
| A. `ctx.wakeAt` and `EDAOutboxSink.make` as sketched, both in core. | Split. The wakeup is core. The helper is an optional module. |
| B. A `Retry` outcome that leaves the cursor in place and redelivers the window later. | Rejected. A parked cursor makes every wake replay a growing window, and it makes the cursor mean two things. |
| C. Heartbeat facts posted by another actor (the status quo for docs-site). | Rejected. It writes timer noise into the log and needs a second actor. |

## Chosen design

### In the runtime, one durable wakeup per sink

```ts
interface EDASinkContext {
  /** Ask to run again at or after `atMs`, even if no event arrives. One wakeup per sink. */
  readonly wakeAt: (atMs: number) => Effect.Effect<void, EDASessionStoreError>;
}
```

- The sink keeps advancing its cursor as today. A level-triggered sink reads what is owed from
  reducer state, does what it can, and calls `wakeAt` for the earliest thing still owed.
- At the wakeup the runner calls `process` with an empty `events` list, `throughSeq` at the
  current cursor, and the projection at that point. A new event before the wakeup runs the sink
  as today, and the sink sets a new wakeup or none.
- The latest call wins. A run that does not call `wakeAt` clears the wakeup.
- **Durability.** The wakeup time is stored with the sink's cursor row and committed in the same
  write as the cursor. The platform alarm is shared through `DurableObjectAlarmScheduler` with a
  third owner, `sinks`, whose time is the earliest sink wakeup. The alarm handler builds the
  runtime for that owner, as it does for `runtime`.
- **Crash semantics.** A crash before the cursor commit loses the wakeup and the cursor together,
  so the batch is redelivered and sets it again. A crash after it leaves the wakeup in storage and
  the alarm armed.
- **Invariants.** The cursor still means "processed through here". At-least-once is unchanged. The
  keep-alive lease is released while a sink waits for its wakeup, which is the point.

### In the runtime, a visible skipped batch

Keep the skip. Add a counter on the `agent.sink.drain` span and an `onSinkDefect` observer hook on
the host, next to the transcript `onAlert`. A host can then alert on it. No durable event, because
a sink that defects on every batch would write one per batch.

### Optional module `effect-durable-agent/sinks/outbox`

A function that builds an `EDASink` from the pieces in the request's sketch (`select`, `key`,
`execute`, `reconcile`, `schedule` and `onSettled`). It uses only public API.

- Attempt counts and next-attempt times live in the sink checkpoint (`ctx.checkpoint`).
- `reconcile` runs before a re-execution of an item whose last attempt was ambiguous.
- `onSettled` stages a fact with `dedupeKey` equal to the item key
  ([FR-5](FR-5-event-dedupe-keys.md)), so a crash between the staged append and the cursor commit
  cannot settle an item twice.
- It calls `wakeAt` for the earliest next attempt.

**Migration for Gia.** Optional. The capacity sink can replace its unbounded in-process retry with
`wakeAt`, which stops it from holding the object awake during an outage. No existing sink has to
change.

**Verification.** `sink-registry.test.ts` covers the wakeup with `TestClock`. The wakeup fires
with an empty batch, a new event preempts it, and a run that sets none clears it.
`sink-replay.test.ts` covers the crash on each side of the cursor commit.
`durable-object-alarm-handler.test.ts` covers the third owner beside the other two. The host
conformance suite covers a wakeup that survives a hard restart on workerd and celld. The helper
gets a model test with a fake remote that can succeed, fail, or succeed and lose the reply, and
the invariant that every item settles exactly once. In Gia, the existing sink tests pass
unchanged.

## Priority and size

P2. Medium. About three days for the wakeup across both hosts, and two for the helper. It removes
the heartbeat facts from the docs-site design and is the base for
[FR-6](FR-6-app-timers.md).
