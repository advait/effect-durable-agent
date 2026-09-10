# Durable run authorization

`RunScheduler.resolve({ sessionId, commandId })` returns `Local` or `Deferred`
promptly. It chooses how work will start; it must not deliver requests or wait for
external approval. `SessionState` remains the sole owner of run allocation,
durable transitions, and execution. Domain policy reads narrow readonly views
of the canonical reduced state, rather than a reconstructed session snapshot.

All runtime builders default to `RunScheduler.Immediate`. Its event history and
ID allocation are unchanged. Steering and tool-feedback turns within an existing
run require no new permission.

## Deferred handoff

Use `RunScheduler.Deferred(deliver)` to defer every new run, or provide a
`RunScheduler` layer with both `resolve` and `deliver` for mixed local/deferred
policy. Direct custom scheduler implementations must now supply `deliver`.

1. The host arms a durable delivery wakeup before the request is committed. A
   crash before commit leaves a harmless wakeup; a crash after commit cannot
   strand the request.
2. `RunSchedulingRequested` records a branded `RunRequestId` and selected work.
   Ordinary work refers to the admitted command. Recovery work also refers to
   the interrupted physical run and its selected, unconsumed input messages.
3. Delivery runs outside the control loop, with at most one attempt in flight.
   Each attempt has a ten-second timeout. A successful return must mean the
   authorizer durably accepted responsibility for eventual permission delivery.
4. Only success records `RunSchedulingDelivered`. Errors, defects, timeouts,
   interruption, and lost acknowledgments leave the request unacknowledged.
   The next host wakeup retries the same request ID. The authorizer must deduplicate
   by that ID. The generic sink checkpoint protocol is deliberately not used.
5. Once acknowledged, the delivery wakeup is cleared. Waiting for permission
   requires no request-specific timer, fiber, or active-work lease. Eviction is safe.

There is at most one current `state.runSchedulingRequest`. Later queued messages
cannot take its reserved next-run slot. `deliveredSeq` records the successful
handoff; authorization waiting is derived from the request's presence. Grants
may arrive before `deliver` returns, so an authorizer may call back immediately.

Custom hosts that permit deferral must provide `runSchedulingWakeupLayer`. Its
`RunSchedulingWakeup.setPending(true)` must persist a retry obligation and invoke
`runtime.retryRunSchedulingDelivery()` after a wakeup until the obligation is
cleared. The default unsupported adapter fails before a deferred request can be
persisted. Cloudflare and celld supply this capability through their shared
Durable Object alarm owner. Delivery obligations survive every active lease's
release and are restored from the event log before startup recovery releases
its work. Their default retry cadence is thirty seconds.

## Trusted permission

An authenticated scheduler calls `runtime.grantRun(new GrantRunCommand({ requestId }))`,
`controller.grantRun({ sessionId, command })`, or the corresponding Durable Object
RPC. The calling host owns scheduler authentication and authorization. Never
forward this RPC from ordinary session-user ingress. `GrantRunCommand` is
excluded from `EDACommand` and its RPC command decoder.

`SessionState` serializes grants with control actions and revalidates eligibility
under the durable append gate. A valid grant records `RunSchedulingGranted` and
`RunStarted` in one atomic batch, along with the required command-start and
recovery-link facts. Only then does execution start. The result is
`Granted { runId }`; duplicate, invalidated, or otherwise stale grants return
`Stale` and cannot create another run. A stale response is terminal for that
permission attempt. This protocol does not allocate coordinator capacity or leases.

## Controls and recovery

- Stop and interrupt invalidate the reservation, cancel its owning command, and
  pause pending messages using the existing queue-pause behavior. An interrupt's
  replacement command needs its own scheduling resolution and, when deferred,
  its own request ID.
- Cancelling a selected pending message invalidates its request. Remaining
  eligible recovery inputs receive a fresh request in the same cancellation
  batch, preserving the predecessor reference. Cancelling unrelated queued
  messages leaves the reservation intact.
- Restarting while waiting keeps the same request ID and delivery acknowledgment.
  Recovery recognizes a started command intentionally parked behind a request.
- Restarting after `RunStarted` requires a fresh scheduling decision. Deferred
  recovery commits interrupted lifecycle repairs, a replacement authorization
  request, and `RecoveryCompleted` atomically. The request preserves the old run
  reference through further restarts. Its eventual grant atomically links the
  replacement run with another `RecoveryCompleted` continuation fact and retains
  the predecessor's recorded model selection.

## Durable compatibility

Framework checkpoint schema 7 rebuilds older checkpoints, including deployed
schema 6, from retained immutable events. No event history is rewritten. The
request projection is bounded to one current reservation and is updated solely
by the four scheduling event types. Grant or invalidation removes it.

`PendingMessagesPaused` accepts either the existing `runId` form or a
`runRequestId` form for interruption before any run exists. Existing pause events
remain readable. Consumers must understand the new scheduling events and pause
variant before enabling `Deferred`; immediate sessions produce neither.

## Verification fixture

`testing/run-authorization` contains a deterministic model, the real session
adapter, and a separate durable manual authorizer. Every HTTP route requires
`STAGING_TOKEN`. The authorizer persists accepted handoffs and counts duplicate
delivery; the verification driver explicitly grants accepted requests and owns
callback retries. It is a test fixture, not a production capacity scheduler.

The service tests cover handoff failures, callback races, controls, and recovery.
The crash simulator restarts from every durable batch boundary of a deferred
run. Host tests cover reconstruction and alarm ownership. The fixture can also
be deployed under a unique staging Worker name to verify actual RPC, alarm,
replay, and redeployment behavior without changing an application deployment.
