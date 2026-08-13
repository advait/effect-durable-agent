# effect-durable-agent message steering

Status: implemented
Runtime: `EDAGiaAgent`
Last reviewed: 2026-07-10

This document specifies durable pending user messages, queue and steer
dispositions, cancellation, promotion, and queue pausing for
`effect-durable-agent` (EDA). It also defines the Gia browser and Slack
integration contracts built on those framework semantics.

The implementation exposes one durable pending-message entity from admission
through consumption, cancellation, or promotion. It batches eligible steers at
turn boundaries, preserves them across unexpected failures, and pauses queued
work after an explicit stop or interrupt.

## 1. One-page decision summary

- Every submitted user message has an explicit `queue` or `steer` disposition.
- EDA mints a durable `messageId` when it admits the submission.
- The message becomes visible immediately after durable admission, before it
  enters model context.
- `queue` starts a run when the session is idle and otherwise waits for a later
  run.
- `steer` joins the active run at its next turn boundary. If no run is active,
  it starts a new run.
- All steers pending at one turn boundary are consumed together in FIFO order.
- Pending queue and steer messages can be cancelled. Queue messages can be
  promoted to steer without changing message identity.
- A pending steer can explicitly interrupt the active run and start immediately
  through an atomic Stop-plus-Resume control action.
- Editing is cancel-then-compose. The composer is populated only after the
  durable cancellation succeeds.
- An explicit Stop terminalizes the run and pauses all remaining queued work.
  Unconsumed steers become paused queue messages rather than disappearing.
- A promoted paused message starts or steers a run; other paused messages stay
  paused.
- Clearing a paused queue and sending a replacement is one conditional atomic
  command.
- Browser mutations use authenticated HTTP. The existing WebSocket remains the
  durable and ephemeral observation channel.
- Slack submits one steer command per inbound Slack message. Slack does not
  expose queue behavior.

## 2. Goals

1. Preserve user intent across client/server state races.
2. Never silently discard an admitted user message because a run ended,
   failed, or changed state unexpectedly.
3. Give every pending message a stable durable identity and renderable state.
4. Make cancellation, promotion, consumption, and pause behavior deterministic
   under concurrent ingress.
5. Keep scheduling semantics in EDA core and product presentation in Gia.
6. Preserve exact message authorship and attachments across every lifecycle
   transition.
7. Use one transport-neutral state machine for browser and Slack ingress.

## 3. Non-goals

- Injecting content into an in-flight model request. Steering waits for a turn
  boundary.
- Promoting a message to interrupt. Interrupt remains a separate run-level
  affordance.
- Supporting browser writes from adjacent users. Gia web writes remain
  owner-only; adjacent users are read-only.
- Sending browser mutations over the live-event WebSocket.
- Restoring a cancelled edit automatically when a local composer draft is
  discarded.
- Deleting workspace files when a message referencing them is cancelled.
- Defining final visual styling. The view model owns presentation details.

## 4. Terms

**Pending message**
: A durably admitted user message that has not been consumed by a `TurnStarted`
  boundary or cancelled.

**Queue message**
: A pending message intended to start separate work after active work no longer
  blocks it.

**Steer message**
: A pending message eligible to join an active run at its next turn boundary.
  In an idle session it starts a new run.

**Consumed message**
: A user message referenced by `TurnStarted.inputMessageIds`. Consumption is
  the boundary after which cancellation and promotion cannot succeed.

**Queue paused**
: A durable scheduling gate created by explicit user interruption. It holds a
  specific set of queue messages until each is cancelled, cleared, or promoted.

**Requested disposition**
: The immutable disposition on the original submission command.

**Current disposition**
: The reducer-derived disposition after any promotion. Scheduling uses this
  value.

## 5. Non-negotiable invariants

1. Durable events are the source of truth. Reducers derive all current state.
2. EDA is the sole minter of `commandId`, `messageId`, `runId`, and `turnId`.
3. One logical user message keeps one `messageId` through promotion,
   cancellation, pause, and consumption.
4. A message is consumed at most once.
5. A cancelled message is never consumed.
6. A consumed message cannot be cancelled or promoted.
7. Promotion changes only a pending queue message into a pending steer message.
8. A queue pause blocks only its remaining member messages. It does not become
   an invisible permanent session latch.
9. Scheduling and conditional message transitions execute inside the serialized
   `SessionState` boundary.
10. HTTP success means durable command admission or a terminal conditional
    outcome; model completion remains observable through events.
11. A client-generated idempotency key is reused across ambiguous HTTP retries.
12. Gia metadata remains app-owned and joins framework state through
    `relatedCommandIdempotencyKey`.

## 6. State model

### 6.1 Pending message record

EDA reducer state gains a first-class message record at command admission:

```ts
interface PendingUserMessage {
  readonly messageId: MessageId;
  readonly originCommandId: CommandId;
  readonly content: UserMessageContent;
  readonly requestedDisposition: "queue" | "steer";
  readonly disposition: "queue" | "steer";
  readonly submittedSeq: SequenceNumber;
  readonly submittedAtMs: UnixEpochMillis;
  readonly promotedSeq?: SequenceNumber;
  readonly pausedByCommandId?: CommandId;
  readonly consumedSeq?: SequenceNumber;
  readonly consumedTurnId?: TurnId;
  readonly cancelledSeq?: SequenceNumber;
  readonly cancelledByCommandId?: CommandId;
  readonly cancellationReason?: PendingMessageCancellationReason;
}
```

The type name is illustrative. The implementation may keep one union for
pending, consumed, and cancelled records so impossible field combinations are
not representable.

Message content must be persisted at admission in framework-owned message
storage. It must not remain available only through the originating command
input because the message becomes the stable lifecycle entity.

### 6.2 Derived collections

The framework reducer derives:

- `pendingQueue`: non-paused queue messages in submission order;
- `pendingSteers`: steer messages in effective-steer order;
- `pausedQueue`: queue messages held by the active pause marker;
- `consumedMessages`: context-visible user messages;
- `cancelledMessages`: durable audit state excluded from the normal transcript.

Reducers never mutate these collections directly. They fold durable facts and
derive the collections from message records.

### 6.3 Ordering

- Queue FIFO uses `submittedSeq`.
- A promotion leaves the normal queue and enters the steer sequence at
  `promotedSeq`.
- Native steers use `submittedSeq` as their effective-steer sequence.
- At a turn boundary, eligible steers are ordered by effective-steer sequence.
- Promoting queue item `C` does not reorder remaining queue items `A` and `B`.

## 7. Commands

### 7.1 `SubmitMessage`

The browser-facing dispositions are narrowed to `queue | steer`. Interrupt is
not a message disposition in the Gia composer.

```ts
interface SubmitMessageCommand {
  readonly _tag: "SubmitMessage";
  readonly commandId?: CommandId;
  readonly idempotencyKey?: CommandIdempotencyKey;
  readonly disposition: "queue" | "steer";
  readonly content: UserMessageContent;
  readonly expectedPausedMessageIdsToCancel?: ReadonlyArray<MessageId>;
}
```

`expectedPausedMessageIdsToCancel` is absent for an ordinary submission. When present, it
is the exact paused-queue set approved by the user in the confirmation modal.
EDA atomically verifies the set, cancels those messages, and admits the new
message. A mismatch returns a typed conflict and changes nothing.

### 7.2 `CancelPendingMessage`

```ts
interface CancelPendingMessageCommand {
  readonly _tag: "CancelPendingMessage";
  readonly commandId?: CommandId;
  readonly idempotencyKey?: CommandIdempotencyKey;
  readonly messageId: MessageId;
  readonly reason: "edit" | "user-cancel";
}
```

Cancellation is a priority control command. It succeeds only while the target
is pending. If consumption wins the serialized race, cancellation returns a
typed `message_not_pending` outcome and emits no cancellation fact.

### 7.3 `PromotePendingMessage`

```ts
interface PromotePendingMessageCommand {
  readonly _tag: "PromotePendingMessage";
  readonly commandId?: CommandId;
  readonly idempotencyKey?: CommandIdempotencyKey;
  readonly messageId: MessageId;
}
```

Promotion is a priority control command. It succeeds only for a pending queue
message and transitions that same message to steer. It never creates a new
message and cannot target a consumed, cancelled, or already-steering message.

If no run is active when promotion is applied, the promoted steer starts a new
run. If a run is active, it joins that run at the next boundary.

### 7.4 `StopTurn`

`StopTurn` remains a separate run-level control command. In addition to its
current run and turn terminalization, explicit user stop creates a queue pause
covering:

- queue messages already waiting behind the active run; and
- steers not consumed by the stopped run.

Those steers transition to queue disposition and remain durable. They do not
enter model context and do not disappear.

### 7.5 Command priority

The external event loop inevitably orders incoming requests. Once admitted,
control dispatch priority is:

1. stop;
2. cancel and conditional clear;
3. promotion;
4. steer consumption at a boundary;
5. ordinary queue dispatch.

The exact ordering between control commands is the order of durable admission
when two commands of equal priority target the same message. First valid state
transition wins; later incompatible commands receive a typed conflict.

### 7.6 Interrupt with a pending steer

Interrupt remains a separate affordance rather than a steer promotion. Gia
atomically admits a `StopTurn` command followed by a `ResumePendingMessages`
command targeting the selected pending steer. Existing interruption semantics
pause all pending work; the resume command immediately starts only the selected
message. Other paused messages remain held.

## 8. Durable events

Names below are normative unless implementation review finds an established EDA
naming convention that is clearer.

### 8.1 `UserMessageSubmitted`

Committed atomically with command admission and contains:

```ts
{
  commandId: CommandId;
  messageId: MessageId;
  disposition: "queue" | "steer";
  content: UserMessageContent;
}
```

This is the durable visibility boundary. The UI can render the message after
this event even though it is not yet model context.

### 8.2 `UserMessagePromoted`

```ts
{
  commandId: CommandId;
  messageId: MessageId;
  from: "queue";
  to: "steer";
}
```

### 8.3 `UserMessageCancelled`

```ts
{
  commandId: CommandId;
  messageId: MessageId;
  reason: "edit" | "user-cancel" | "clear-paused-queue";
}
```

Cancelled records remain in reducer state and the event log but are filtered
from the normal transcript projection.

### 8.4 `PendingMessagesPaused`

```ts
{
  interruptionCommandId: CommandId;
  runId: RunId;
  messageIds: readonly [MessageId, ...MessageId[]];
  reason: "user-interrupted";
}
```

The event captures the exact held set. Cancellation and promotion remove
members through their own events. Queue-paused state clears automatically when
no members remain; a separate clear event is unnecessary unless required for
observability.

### 8.5 Consumption

`TurnStarted.inputMessageIds` remains the authoritative consumption fact. A
new turn may reference multiple pending steer message IDs. Folding
`TurnStarted` marks all referenced messages consumed at the same sequence and
turn.

Legacy `UserMessageCommitted` and `SteeringMessageQueued` facts remain
replayable. The reducer projects legacy and current records into one pending
message view without inventing migration events or rewriting historical facts.
There must be one user message in UI projection, not a second record created at
consumption.

## 9. Dispatch semantics

### 9.1 Truth table

| Input | No active run | Active turn | Completed turn boundary | Queue paused |
|---|---|---|---|---|
| Submit queue | Start new run | Wait in queue | Wait until run terminalizes | Conflict |
| Submit steer | Start new run | Wait as steer | Join next continuation | Start new run; held messages stay paused |
| Promote queue | Start new run | Wait as steer | Join next continuation | Release selected message and start/steer |
| Cancel pending | Cancel | Cancel if boundary has not consumed it | First serialized transition wins | Cancel selected message |

The server evaluates this table against authoritative reducer state. It does
not validate a browser claim that a run was or was not active when the user
clicked Send.

### 9.2 Active turn steering

Steering does not interrupt inference, tool execution, or the run. The current
turn completes normally. At the next continuation decision, EDA snapshots all
eligible steers already admitted and starts one continuation turn whose
`inputMessageIds` contains that ordered batch.

Steers admitted after `TurnStarted` commits wait for the following turn
boundary. The durable sequence establishes the cutoff; wall-clock arrival time
does not.

### 9.3 Normal run completion

At a completed turn boundary:

1. apply higher-priority stop/cancel/promotion commands;
2. snapshot all eligible pending steers;
3. if the snapshot is non-empty, start one continuation turn in the same run;
4. otherwise terminalize the run and active command;
5. if the queue is not paused, start the next queue message as a new run.

### 9.4 Unexpected run failure

Unconsumed steers survive an unexpected run failure. After the failed run
terminalizes, they remain eligible work and start a new run. They are not
cancelled merely because their original target run failed.

If multiple steers survive, the new run consumes the pending steer batch in
order as its initial input boundary.

### 9.5 Explicit stop

Explicit Stop differs from failure:

1. stop the current turn and terminalize the run under existing EDA rules;
2. transition unconsumed steers to queue;
3. collect those messages and all already-waiting queue messages;
4. commit `PendingMessagesPaused` for that exact set;
5. do not dispatch any held message automatically.

The UI can derive copy such as:

- `You stopped after 4s`
- `Queue paused because you interrupted`

The first is a run terminal presentation; the second is a queue state
presentation.

### 9.6 Selective release

Promoting one paused message releases only that message. Other pause members
remain held even while the promoted message runs. Users may promote additional
messages separately; promotions arriving during the new run become steers for
that run.

When the final paused member is promoted or cancelled, the pause is empty and
future ordinary queue submissions behave normally.

## 10. Cancellation and editing UX

### 10.1 Cancel

Queue and steer messages expose cancellation while pending. The UI sends an
HTTP cancellation command and shows the action as pending. It removes the
message only after the durable cancellation outcome arrives through the HTTP
response or event stream.

### 10.2 Edit

Edit uses cancellation reason `edit`:

1. user clicks Edit;
2. UI sends `CancelPendingMessage`;
3. server conditionally cancels the pending message;
4. after success, UI removes it through reducer projection and places its text
   and attachment references into the composer;
5. composer local storage owns the draft from that point;
6. resubmission creates a new message identity.

If consumption wins the race, the edit fails, the message stays in the
transcript, and the composer is not populated. That message is already part of
the running turn context.

Discarding an edited local draft does not restore the cancelled message.

### 10.3 Attachments

Edit restores attachment references along with text. Existing uploaded files
are reused. Cancellation changes message lifecycle only and never deletes
workspace files.

## 11. Promotion UX

Only pending queue messages expose Promote. Promotion:

- keeps the same message identity, author, content, and attachments;
- updates the reducer-derived disposition to steer;
- starts a run if idle or joins the next active-run boundary;
- never offers an interrupt transition.

The UI waits for the durable promotion result before presenting the message as
steering. Concurrent cancel/promote operations use first-transition-wins
semantics and return typed conflicts to the loser.

## 12. Paused queue confirmation

An ordinary queue submission while paused returns `queue_paused` and includes
the current held message IDs. The UI should normally detect the projected pause
before sending and show a confirmation modal.

On confirmation, it retries submission with `expectedPausedMessageIdsToCancel` equal to
the exact IDs shown. EDA atomically:

1. verifies that the current paused set exactly matches the approved set;
2. emits cancellation facts with reason `clear-paused-queue`;
3. admits the replacement message;
4. begins normal dispatch.

If another client or ingress changes the paused set before application, the
operation conflicts and changes nothing. The UI refreshes the modal rather than
deleting unseen work.

## 13. HTTP API

### 13.1 Transport decision

Browser mutations use regular authenticated HTTP. The WebSocket remains
server-to-client except for its existing flow-control ACK frames.

This preserves the current route authentication, authorization, validation,
attachment handling, status codes, and Durable Object submission path. HTTP/2
or HTTP/3 connection reuse may reduce transport overhead, but correctness does
not depend on a particular negotiated protocol.

### 13.2 Endpoints

The exact route layout should follow existing React Router conventions. The
required operations are:

```text
POST /api/eda-agent/sessions/:sessionId/messages
POST /api/eda-agent/sessions/:sessionId/stop
POST /api/eda-agent/sessions/:sessionId/messages/:messageId/cancel
POST /api/eda-agent/sessions/:sessionId/messages/:messageId/promote
POST /api/eda-agent/sessions/:sessionId/messages/:messageId/interrupt
```

Submission accepts:

```ts
{
  text: string;
  attachments?: WorkspaceAttachmentRef[];
  disposition: "queue" | "steer";
  idempotencyKey: string;
  expectedPausedMessageIdsToCancel?: string[];
}
```

Disposition and idempotency key are required for the new client. During rollout
the server may temporarily default missing disposition to queue for older
deployed clients, with telemetry and a defined deletion point.

### 13.3 Outcomes

HTTP responses distinguish:

- durable success with `commandId`, `messageId`, and admitted sequence;
- idempotent replay of an earlier success;
- `queue_paused` with current held IDs;
- `message_not_pending`;
- `message_not_queue` for invalid promotion;
- `paused_queue_changed` for a stale clear precondition;
- authorization and validation failures.

Events can arrive over the WebSocket before the HTTP response. The browser must
apply both idempotently and correlate optimistic/pending submission state using
the client-generated command idempotency key.

### 13.4 Retry

The browser stores an in-flight mutation until it receives a terminal HTTP
outcome. After an ambiguous network failure it may retry the same request with
the same idempotency key. It must not retry the same mutation through a second
transport.

## 14. WebSocket projection

No new client command frame is added to
`docs/websocket-protocol.md`. The existing stream delivers the new
durable lifecycle events and ephemeral active-turn events.

The browser reducer must render, without a fresh snapshot:

- newly submitted queue and steer messages;
- promotion from queue to steer;
- successful cancellation removal;
- batched consumption at `TurnStarted`;
- queue pause membership and selective release;
- run failure fallback;
- exact command terminal/conflict outcomes where exposed durably.

Reconnect resumes by durable sequence exactly as it does today. Pending message
state must reconstruct entirely from the snapshot and replay tail.

## 15. UI projection

### 15.1 Visible message states

The normal transcript projection includes:

- pending queue messages;
- pending steer messages;
- consumed user messages.

It excludes cancelled messages even though reducer state retains them.

The view model may choose final labels and affordances, but it must receive
enough state to distinguish queue, steer, paused queue, action pending, and
consumed. Rendering differences between message author and action actor are a
view-model concern.

### 15.2 Composer

- The composer always sends an explicit queue or steer disposition.
- Projected run state may choose the default but never rewrites a frozen
  submission during retry.
- While a run is active, an empty composer renders Stop in place of Send and
  submits `StopTurn`. Text or attachments keep the normal Send action.
- Pressing Enter in an empty composer does not stop the run; interruption
  requires the explicit Stop button.
- Edit populates the composer only after cancellation success.
- Composer local storage persists edit drafts.

Pending queue overflow actions are Send now (steer), Edit, Copy, and Delete.
Pending steer overflow actions are Interrupt and send now, Edit, Copy, and
Delete. Edit cancels durably before restoring the message to the composer;
Delete uses the normal user-cancellation path.

### 15.3 Queue pause

The UI renders the interrupted run terminal separately from the paused queue.
Each paused message offers Cancel/Edit and Promote. A new ordinary send opens
the destructive confirmation flow described above.

## 16. Gia metadata and authorization

### 16.1 Correlation

Gia continues to atomically submit app facts alongside the framework command:

```text
GiaUserMessageAuthorCommitted(relatedCommandIdempotencyKey)
GiaFileAttachmentCreated(relatedCommandIdempotencyKey)
SubmitMessage(idempotencyKey)
```

EDA mints `commandId` and `messageId`. Reducers join:

```text
messageId
  -> originCommandId
  -> command idempotency key
  -> Gia author and attachment facts
```

Promotion, cancellation, and consumption never copy metadata because the
message identity and origin command do not change.

### 16.2 Author and actor

Gia distinguishes:

- session owner;
- message author;
- actor responsible for cancel, promote, stop, or clear actions.

Control-action actor facts correlate through their command idempotency keys.
Queue pause derives its stopping actor from the Stop command. Gia may render
primarily the author because collaborative web writes are not supported.

### 16.3 Existing sessions

Hundreds of existing sessions require compatibility:

- exact idempotency-key author mapping is primary;
- new pending-message events must always resolve exact attribution;
- existing author span state and checkpoint decoding remain supported;
- span fallback is used only for historical messages without exact correlation;
- session owner remains the final fallback;
- fallback usage is instrumented before any future span removal;
- reducer schema versions are incremented when checkpoint state changes, causing
  replay from durable events rather than event-log rewriting.

### 16.4 Authorization

Gia browser mutation routes remain owner-only. Adjacent users can read the
session and live events but cannot submit, cancel, promote, clear, or stop.
Slack-backed session ingress is authorized through the Slack transport path.

## 17. Slack integration

Slack submits exactly one steer command per inbound Slack message. It does not
batch multiple people or messages into one EDA user message and does not submit
queue messages.

For every inbound Slack message, one atomic batch contains:

1. first-session metadata and system prompt facts when required;
2. exact author metadata;
3. attachment facts;
4. one `SubmitMessage{disposition: "steer"}`;
5. one Slack receipt/correlation fact.

The Slack message/event identity supplies the stable command idempotency key.

Because steer starts work when idle, the same command works for initial and
subsequent ingress. Concurrent Slack messages are independently admitted in
Durable Object order and are batched by EDA, not Slack, at the next turn
boundary. Their transcript rows, authors, attachments, and cancellation audit
identities remain separate.

Slack does not expose queue, promotion, paused-queue confirmation, or browser
edit behavior. A Slack steer can start a run while unrelated browser-created
queue messages remain paused.

## 18. Recovery and compaction

- Pending messages and queue pause membership are durable reducer state.
- Startup has one recovery entrypoint. It hydrates state, calls the pure
  `planSessionRecovery` domain policy once, and interprets the complete plan as
  one atomic durable event batch before starting the live control loop.
- The recovery plan covers unfinished commands, runs, turns, inferences, tool
  calls, stop requests, compactions, and ownerless steers. Open compaction
  repair is not a separate startup pass.
- Recovery admits one framework-owned `ResumePendingMessages` command for an
  ownerless steer batch. It does not synthesize user-message history.
- After the recovery batch is folded, the shell verifies that a second pure
  plan is empty. Failure to reach this fixed point is fatal.
- The live runtime does not perform defensive recovery. It assumes startup
  recovery succeeded and fails loudly if durable active work has no in-memory
  owner.
- Expected model, tool, and run failures while a live owner exists are ordinary
  domain transitions. They may preserve steers and schedule follow-up work,
  but they are not startup recovery.
- Framework checkpoint schema changes must replay correctly from sequence zero
  when the old checkpoint version is incompatible.
- Command input compaction must not delete content still owned by a pending
  message.
- Prompt/context compaction applies only after consumption. Pending and
  cancelled messages are not model context.
- An orphaned active run is terminalized only by the startup recovery plan.
- A crash after durable command admission but before dispatch reconstructs the
  same pending message and resumes scheduling.
- A crash after `TurnStarted` commits reconstructs the message as consumed;
  cancellation cannot subsequently win.

## 19. Observability

Add structured fields and counters for:

- submitted disposition and applied path (`new-run`, `queued`, `steered`);
- pending queue, steer, and paused depths;
- number of steers consumed per turn boundary;
- pending-message age at consumption;
- cancellation and promotion outcomes;
- conditional conflicts by code;
- queue pause creation, depth, duration, and final resolution;
- unexpected failure fallback to a new run;
- HTTP idempotent replay;
- exact versus span/owner author attribution;
- Slack messages submitted and steered individually.

Logs must include `sessionId`, `commandId`, `messageId`, and applicable `runId`
and `turnId` without logging message content.

## 20. Test plan

### 20.1 Pure reducer tests

- submission creates one pending message with framework-minted identity;
- promotion preserves identity and origin metadata correlation;
- cancellation removes only from renderable state;
- `TurnStarted` consumes several steers atomically in FIFO order;
- pause membership derives and clears correctly;
- replay and checkpoint hydration reproduce identical state;
- legacy author spans remain readable while new messages use exact mapping.

Property tests should generate interleavings of submit, promote, cancel, stop,
turn start, turn terminal, and run failure and assert the invariants in section
5.

### 20.2 Session control tests

- queue while active waits and later starts a separate run;
- steer while active waits for the boundary without terminalizing the run;
- steer arriving idle starts a run;
- all pre-boundary steers enter one continuation turn;
- post-boundary steer waits for the next boundary;
- cancellation and consumption races are first-transition-wins;
- promotion idle fallback starts a run;
- failed run preserves unconsumed steers;
- explicit stop demotes steers and pauses all waiting work;
- selective promotion leaves other pause members held;
- ordinary queue submission conflicts while paused;
- conditional clear-and-submit is atomic and detects stale sets.
- interrupting with a pending steer stops the active run and resumes exactly
  that message while other paused messages remain held.

### 20.3 Host and API tests

- HTTP auth remains owner-only;
- duplicate idempotency keys return the original outcome;
- event arrival before HTTP completion does not duplicate UI state;
- attachment and author batches remain atomic with message admission;
- typed conflicts map to stable HTTP response codes and bodies;
- restored WebSockets replay every new lifecycle event.

### 20.4 UI tests

- queue and steer messages render immediately after admission;
- Edit waits for cancellation success before filling the composer;
- failed Edit does not alter the composer;
- promotion updates one existing row without flicker or duplication;
- cancelled messages disappear from the normal transcript;
- Stop renders run and queue-paused notices separately;
- stale clear confirmation refreshes rather than deleting unseen work;
- reload and reconnect preserve every pending state and affordance.
- queued and steering overflow menus expose the correct ordered actions.

### 20.5 Slack tests

- one inbound Slack message produces one steer command;
- multiple inbound events remain separate messages and authors;
- idle Slack steer starts a run;
- active Slack steers batch only at the EDA boundary;
- Slack retries deduplicate without losing metadata;
- first message seeds session state atomically with its steer.

## 21. Implementation map

Primary framework areas:

- `src/types/commands.ts`
- `src/types/events/durable.ts`
- `src/domain/reduced-state.ts`
- `src/domain/command-queues.ts`
- `src/domain/dispatch-policy.ts`
- `src/domain/run-continuation-policy.ts`
- `src/services/session-state.ts`
- `packages/cloudflare/src/durable-object-store.ts`

Primary Gia areas:

- `workers/eda-agent/api.ts`
- `workers/eda-agent/slack-ingress.ts`
- `workers/eda-agent/user-message-authors.ts`
- `workers/eda-agent/attachments.ts`
- `app/routes/api.eda-agent.sessions.$sessionId.messages.ts`
- `app/lib/eda-session-api-client.ts`
- `app/hooks/use-eda-session-live.ts`
- `app/components/screens/eda-session-page-model.ts`
- `app/components/organisms/eda-session-thread-panel.tsx`

## 22. Summary

EDA should treat a submitted user message as a durable entity before it becomes
model context. Queue and steer are user intent, resolved against authoritative
server state. Steering waits for a turn boundary and batches all eligible
messages. Cancellation and promotion are conditional control commands over the
same message identity. Explicit Stop preserves user work by pausing it rather
than dropping or immediately executing it. Gia sends mutations over HTTP,
observes state over WebSocket, correlates metadata through command idempotency,
and retains historical author compatibility. Slack contributes one steer per
human message and delegates batching entirely to EDA.
