# FR-6. App-owned durable timers as facts

Status: proposal. Not approved to build. Nothing here is implemented.

## The request

Durable timers for queue wait, form expiry and idle end. The sketch is `ctx.armTimer` and
`ctx.cancelTimer` from tools, sinks and deciders, a new alarm owner `app`, and a `TimerFired` fact
on expiry.

## What the source shows

The claim is true. `DurableObjectAlarmOwner` in
`packages/effect-durable-agent-cloudflare/src/durable-object-alarms.ts` is `"runtime" |
"transcript"`. The scheduler always sets the platform alarm to the earliest recorded wakeup. An
app that calls `ctx.storage.setAlarm` directly replaces that alarm and is itself replaced at the
next sync.

## Is it general

Yes. A timeout that arrives as a fact is the standard way to keep reducers free of clocks.
Approval expiry, idle shutdown and follow-ups all have this shape. Gia does not use it inside a
session today. Its timers live in other actors (the cron scheduler and the subagent coordinator).
So the evidence is one planned consumer and a well-known pattern.

It does not need its own mechanism.

## Designs considered

| Design | Verdict |
| --- | --- |
| A. Imperative `armTimer` and `cancelTimer`, a timers table, a new alarm owner, a framework `TimerFired`. | Rejected. |
| B. Timers as state. A reducer holds what is due. A sink fires what is due and sleeps until the next one. | Chosen. It needs only [FR-4](FR-4-sink-wakeups.md)'s wakeup and [FR-5](FR-5-event-dedupe-keys.md)'s keys. |
| C. Another actor holds the timers and posts facts (the docs-site plan). | Works. It is what B replaces. |

**Why not A.** It adds a second source of truth. A timer table can disagree with the log. "Was
this timer cancelled" becomes a question about a table, where the app already has the answer in a
reducer. Arm and cancel from a tool also need crash rules of their own (armed but the fact was not
committed, or the reverse). The docs-site reducer already keeps
`timers: Record<TimerId, { dueAtMs, purpose }>`. That is design B waiting for a wakeup.

## Chosen design

An optional helper, `effect-durable-agent/sinks/timers`, built on public API.

```ts
EDATimerSink.make({
  name: "docs.handoff.timers",
  /** Pure. What is scheduled, according to state. */
  select: (reducerStates) => ReadonlyArray<{ timerId: string; dueAtMs: number }>,
  /** Build the app's own fact for a timer that is due. */
  fire: (timer) => EDAEventDraft,
})
```

- On each run the sink reads `select`. For every timer with `dueAtMs <= now` it stages `fire(timer)`
  with `dedupeKey = timerId`. Then it calls `ctx.wakeAt` for the earliest timer still in the
  future.
- The app's reducer removes a timer when it folds the fired fact, or when the thing it guarded
  happened first. Cancelling is a reducer transition. There is no cancel call.
- The fired fact is the app's own event in the app's namespace. EDA adds no framework event.

**Crash semantics.** After any crash the sink reads state again and recomputes. A timer that fired
but whose fact was staged twice collapses on its `dedupeKey`. A timer is never lost, because it
exists as long as the reducer says it does.

**Invariants.** Reducers stay clock-free. The clock is read only in the sink. No alarm owner is
added beyond FR-4's `sinks`.

**Accuracy.** A timer fires at or after its due time, when the alarm is delivered and the runtime
has started. Cloudflare alarms are at-least-once and can be late by seconds. This is right for
expiry and idle timers. It is not a scheduler for sub-second work.

**Migration for Gia.** None.

**Verification.** A `TestClock` service test proves four things. A timer fires once at its time.
A timer removed before its time never fires. Two timers fire in order. A crash between the staged
append and the cursor commit yields one fact. The host conformance suite arms a timer, restarts hard, and
sees one fact.

## Priority and size

P2, after FR-4. Small once FR-4 exists, about one day. It moves the docs-site timers out of the
connector and into the conversation session.
