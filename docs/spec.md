# effect-durable-agent — Current Implementation

Status: current
Runtime: EDAGiaAgent
Last reviewed: 2026-07-15

This is the agent-facing implementation guide for EDA. It should describe what the runtime **does today**. Product-specific UI and subagent proposals belong in the downstream-consumer documents under `docs/` listed at the end.

If a claim in this document is not backed by code in this package, move it to the specific
future-state proposal that owns it or delete it.

---

## 1. One-page mental model

EDA is an Effect-native agent harness modeled as one durable state machine per session.

```text
mutating input
  -> durable command/app events
  -> pure reducers
  -> live client stream
  -> Effect workflows for model/tool/sink work
  -> deterministic replay/recovery from the durable log
```

The core idea is simple:

- **Durable events are facts.** They are persisted, sequenced, replayable, and are the crash/recovery source of truth.
- **Reducers derive state.** Framework state and app state are pure folds over the same ordered event stream.
- **`SessionState` is the live-process authority.** Every durable append, durable fold, ephemeral position allocation, active-execution mutation, and live publish flows through it.
- **Effect owns execution.** Model streams, tools, sinks, interruption, scopes, retries, finalizers, and observability are Effect workflows, not detached promises.
- **The host is pluggable.** The current production host is Cloudflare Durable Objects with Durable Object SQLite, WebSockets, alarms, and RPC, but the core runtime depends on small service boundaries.

EDA exists because callback-first agent loops do not define the hard boundaries: command admission, event ordering, reconnect, multi-client sync, stop semantics, tool lifecycle, durable side effects, and crash recovery. EDA makes those boundaries first-class.

---

## 2. Current package/API grounding

The implementation is grounded in the versions installed by this package. Check `package.json`
and local `node_modules` before changing API-shaped docs.

| Package | Version | Current role |
| --- | --- | --- |
| `effect` | `4.0.0-beta.102` | Core Effect modules and `effect/unstable/ai`. |
| `@effect/ai-openai` | `4.0.0-beta.102` | OpenAI provider adapter. |

Important version facts:

- Services use `Context.Service<Self, Shape>()("key")`, not newer `ServiceMap.Service` examples.
- Effect AI is imported from `effect/unstable/ai`; there is no separate local `@effect/ai` package.
- EDA exposes model tools through upstream `Tool` / `Toolkit` values and calls `LanguageModel.streamText(..., { disableToolCallResolution: true })` so the harness owns tool lifecycle.
- UUIDv7 schemas use `Schema.String.check(Schema.isUUID(7, ...))` in this beta.

---

## 3. Runtime map

```text
Host route / RPC / WebSocket
        |
        v
EDASessionDurableObjectHost
        |
        v
ManagedRuntime<Effect services> for one session
        |
        +--> EDARuntime public facade
        |
        +--> SessionState  <---- all writes, active execution, recovery
        |       |
        |       +--> EDASessionStore      durable event log + checkpoints
        |       +--> LiveEventBus         live fanout + active-turn ephemeral replay
        |       +--> command-control loop run/turn scheduling
        |
        +--> TurnRunner / InferenceRunner / ToolExecutor
        |
        +--> EDAReducerRegistry / EDASinkRegistry
```

### Important files

| Area | Files |
| --- | --- |
| Public runtime | `services/runtime.ts`, `host/durable-object.ts` |
| Durable store port + implementations | `services/session-store.ts`, `host/durable-object-storage.ts`, `host/durable-object-store.ts` |
| Session authority | `services/session-state.ts` |
| Query/read APIs | `services/session-query.ts` |
| Live stream/WebSocket | `services/live-event-bus.ts`, `services/websocket-subscriber.ts`, `host/websocket-wire.ts`, `host/websocket-protocol.ts` |
| Pure reducers/policies | `domain/reduced-state.ts`, `domain/command-queues.ts`, `domain/dispatch-policy.ts`, `domain/run-continuation-policy.ts`, `domain/inference-state.ts` |
| Model/turn/tool execution | `services/turn-runner.ts`, `services/inference-runner.ts`, `services/tool-executor.ts`, `services/tool-registry.ts` |
| Extension points | `services/reducer-registry.ts`, `services/sink-registry.ts` |
| Compaction | `services/compaction.ts`, `domain/context-projection.ts` |
| Cloudflare host | `host/durable-object-runtime.ts`, `host/durable-object-keepalive.ts`, `host/durable-object-sink-checkpoints.ts` |
| Examples | `examples/` |

---

## 4. Current terms

- **Session** — one durable agent conversation. Current production host stores one session per Durable Object.
- **Command** — mutating user/control input. Current commands include
  `SubmitMessage`, `CancelPendingMessage`, `PromotePendingMessage`, and
  `StopTurn`; `ResumePendingMessages` is framework-internal recovery work.
- **Run** — lifecycle started by a command. At most one active run per session.
- **Turn** — one scheduler unit inside a run. At most one active turn per active run.
- **Inference** — one concrete model stream execution inside a turn. In the happy path there is one inference per turn. Tool continuation, rejected-tool feedback, or later scheduler policy creates later turns rather than nesting multiple model calls under one turn.
- **Tool call** — one model-requested framework-owned tool invocation with durable lifecycle events.
- **Durable event** — persisted event with monotonic per-session `seq` and replay semantics.
- **Ephemeral event** — live-only event positioned against the latest durable head but not persisted.
- **Reducer** — pure fold over durable events. Framework and app reducers share the same log.
- **Sink** — app integration worker over durable or ephemeral events. Durable sinks have durable checkpoints and at-least-once semantics.
- **Host** — adapter that supplies ingress, storage, sockets, clock/id services, scheduler/keep-alive hooks, and provider bindings.

---

## 5. Non-negotiable invariants

These are the current implementation rules worth preserving during edits:

1. **Durable commit before live publish.** A durable event is never published live until `EDASessionStore.append` succeeds.
2. **`SessionState` is the write authority.** No runtime component writes durable events, publishes ephemerals, mutates active execution, or allocates live positions outside `SessionState` / `SessionEventSink` paths.
3. **One active run and one active turn.** This is derived from durable reduced state and enforced by `SessionState` active execution.
4. **`seq` is the only persisted ordering clock.** Durable events are `(seq, 0)`; ephemerals are `(anchorSeq, subSeq)` and live-only.
5. **Pure decisions stay pure.** Reducers, dispatch policy, run continuation policy, and inference-state transitions do not read clocks, refs, stores, services, sockets, or model streams.
6. **Effect owns resources and interruption.** Runners, tools, subscribers, sinks, and finalizers are scoped workflows.
7. **Started boundaries must terminalize.** In a live process, abnormal exits commit matching failure/interruption/cancellation terminals. Host eviction is repaired by recovery.
8. **No lifecycle events from app code.** Applications emit app events and tool results; framework lifecycle boundaries remain framework-owned.
9. **Durable store transactions do not suspend.** The Cloudflare SQLite append kernel is synchronous and contains no external I/O.
10. **Reducer checkpoints are caches.** Missing/stale checkpoints are repaired by replaying durable events after the checkpoint cursor.
11. **Prompt prefixes are immutable outside compaction.** After model-facing context has been sent, later turns append rather than rewrite it. Compaction creates a new context version.

---

## 6. Public runtime surface

`EDARuntime` is the host-facing facade for one session runtime.

Current methods:

- `submit(command)` — admit one command and return the committed `CommandAdmitted` event.
- `submit(items)` — commit an ordered durable batch of commands and/or app durable events.
- `submitAndBlock(command)` — admit a command and wait for its terminal command event.
- `blockOnCommand(commandId, afterSeq?)` — wait for `CommandCompleted | CommandFailed | CommandCancelled`.
- `snapshot()` — read current framework/app reducer snapshot.
- `messages()` — read durable user/assistant transcript messages.
- `eventsAfter(afterSeq)` — return a reconnect-safe stream combining durable backfill, same-process active-turn ephemeral replay, buffered live prefix, and future live events.

Normal long-running ingress should prefer `submit` then observe the event stream rather than blocking a request for model completion.

---

## 7. Commands and scheduling

### Commands

`SubmitMessage` fields:

- optional framework `commandId` (normal callers omit it; EDA mints UUIDv7)
- optional caller-owned `idempotencyKey`
- `disposition: "queue" | "steer" | "interrupt"`
- `content` aligned to Effect AI user message content (`TextPart | FilePart`, or string shorthand)

`StopTurn` fields:

- optional framework `commandId`
- optional caller-owned `idempotencyKey`

### Command dispositions

- **`queue`** — message starts a run when idle or waits as a durable pending
  message. Explicit stop/interrupt pauses remaining queued work.
- **`steer`** — message starts a run when idle or joins the active run at its
  next turn boundary. All eligible steers at that boundary are consumed in FIFO
  order in one continuation turn.
- **`interrupt`** — closes the active turn/run and starts replacement work from the interrupting message.

### Ingress flow

```text
host decodes command
  -> EDARuntime.submit
  -> SessionState.admitCommand
  -> mint commandId / dedupe by idempotencyKey
  -> CommandAdmitted durable event
  -> store assigns seq
  -> SessionState folds ReducedState + app reducers
  -> LiveEventBus publishes (seq, 0)
  -> command-control loop drains runnable durable work
```

The ingress signal is only a wakeup. The durable event log and authoritative in-memory reduced state decide what runs.

### Control loop ownership

`SessionState` owns the command-control loop. It starts commands, creates run/turn scopes, applies stop/steer/interrupt policy, observes active turn fibers, and terminalizes runs/commands. It does not hand state-dependent mutation to callers after `snapshot()`; decisions that mutate state happen inside the serialized session boundary.

Current routing highlights:

- `SubmitMessage{queue|steer}` atomically admits the command and
  `UserMessageSubmitted`, making the pending message immediately renderable.
- Idle `SubmitMessage` starts a run and consumes that admitted message through
  `TurnStarted.inputMessageIds`.
- Idle/stale `StopTurn` commits `CommandStarted` then `CommandCancelled { reason: "no active turn" }`.
- Active `SubmitMessage{queue}` remains pending until the active run completes or is interrupted.
- Active `SubmitMessage{steer}` remains pending until the next turn boundary;
  it never mutates an in-flight provider request.
- Active `SubmitMessage{interrupt}` closes and terminalizes the active run,
  pauses remaining pending messages, then starts the replacement message.
- Active `StopTurn` commits `StopTurnRequested`, interrupts the active scope, then commits stop/run/command terminal events.
- Completed turn + pending steers consumes the entire eligible steer batch in
  another turn in the same run; otherwise the run and active command complete.

---

## 8. Event model

### Position

```ts
type Position = { seq: number; subSeq: number }
```

- Durable event: `(seq, 0)` where `seq` is a per-session monotonic durable ordinal.
- Ephemeral event: `(anchorSeq, subSeq >= 1)` where `anchorSeq` is the latest authoritative durable head in the live process.
- Reconnect resumes by durable `seq` only. `subSeq` is not persisted and has no meaning after process loss.

### Envelope

All events share an envelope:

```ts
type EventEnvelope = {
  namespace: EventNamespace
  type: EventType
  schemaVersion: number
  durability: "durable" | "ephemeral"
  eventId: EventId
  sessionId: SessionId
  createdAtMs: number
  payload: unknown
}
```

`EventFactory` is the framework construction path: it attaches session identity, mints `eventId`, and stamps `createdAtMs`. Framework-owned decoding routes by `(namespace, type, schemaVersion)`. Current framework schemas are `schemaVersion = 1`.

### Current durable framework events

- Command: `CommandAdmitted`, `CommandStarted`, `CommandCompleted`, `CommandFailed`, `CommandCancelled`
- Message/context: `SystemMessageCommitted`, `UserMessageSubmitted`,
  `UserMessagePromoted`, `UserMessageCancelled`, `PendingMessagesPaused`,
  `UserMessageCommitted`, `AssistantMessageCommitted`,
  `AssistantPartialCommitted`. Legacy steering events remain replay-compatible.
- Run: `RunStarted`, `RunCompleted`, `RunFailed`; legacy replay also accepts deprecated `RunInterrupted`
- Turn: `TurnStarted`, `TurnCompleted`, `TurnFailed`; legacy replay also accepts deprecated `TurnStopped`
- Inference: `InferenceStarted`, `InferenceCompleted`, `InferenceFailed`
- Tool: `ToolCallCreated`, `ToolCallRejected`, `ToolCallStarted`, `ToolCallCompleted`, `ToolCallFailed`
- Stop: `StopTurnRequested`, `StopTurnApplied`
- Compaction: `CompactionRequested`, `CompactionStarted`, `SummaryCreated`, `ContextRebased`, `CompactionCompleted`, `CompactionFailed`

Reserved but not currently emitted in normal runtime paths: `ContextProjected`, `BaseStateRequested`, `BaseStateCreated`, `BaseStateFailed`.

### Current ephemeral framework events

- `TextDelta`
- `ReasoningDelta`
- `ToolParamsStart`
- `ToolParamsDelta`
- `ToolParamsEnd`

Ephemeral events are live-only. The current reconnect story keeps a bounded raw active-turn ephemeral replay buffer in memory for the current process; Durable Object eviction loses that buffer.

---

## 9. Durable store

`EDASessionStore` is the semantic storage port. Runtime services depend on this port, not raw SQL.

Current API:

```ts
interface EDASessionStore {
  append(batch): Effect<ReadonlyArray<CommittedDurableEvent>, EDASessionStoreError>
  eventsAfter(afterSeq): Stream<CommittedDurableEvent, EDASessionStoreError>
  loadCommittedEventsBySeq(seqs): Effect<ReadonlyArray<CommittedDurableEvent>, EDASessionStoreError>
  findCommandAdmission(input): Effect<CommittedDurableEvent | undefined, EDASessionStoreError>
  loadSummaryArtifact(summaryId): Effect<CompactionSummaryArtifact | undefined, EDASessionStoreError>
  loadReducerCheckpoint(name): Effect<EDAReducerCheckpoint | undefined, EDASessionStoreError>
  saveReducerCheckpoint(checkpoint): Effect<void, EDASessionStoreError>
  saveReducerCheckpoints(checkpoints): Effect<void, EDASessionStoreError>
}
```

### Current production store

Production uses Cloudflare Durable Object SQLite. One Durable Object stores one EDA session.

Foundational tables:

- `_eda_schema_migrations` — applied host/store migrations.
- `_eda_event_log` — ordered durable occurrences; `seq INTEGER PRIMARY KEY AUTOINCREMENT`.
- `_eda_command_state` — command status and command idempotency lookup.
- `_eda_command_inputs` — submitted command bodies kept out of event-log JSON.
- `_eda_context_messages` — model-visible message bodies.
- `_eda_context_summaries` — compaction summaries.
- `_eda_reducer_checkpoints` — framework `_eda.framework.reduced-state` and app reducer checkpoints.
- `_eda_sink_cursors` — durable sink checkpoints (cursor plus versioned sink-owned payload).

Logical durable events are canonical. The DO store may physically normalize large framework fields into sidecar tables, but `eventsAfter` reconstructs full logical events before returning them to reducers, sinks, query APIs, and live catch-up.

There is intentionally no separate privileged session-state table. Framework `ReducedState` is persisted through the same reducer-checkpoint mechanism as app reducers; `_eda.framework.reduced-state` is reserved for framework-owned state, while app reducers use non-`_eda.` names.

Foreign/app namespace events are currently stored as full logical payloads in `_eda_event_log.fact_json`; there is no app physical sidecar codec registration.

### Append rule

Each durable append is one synchronous store transaction. In the Cloudflare host this means `ctx.storage.transactionSync(() => { ... })` around synchronous `ctx.storage.sql.exec(...)` calls only.

Inside append:

1. Insert event-log rows; SQLite allocates `seq`.
2. Write framework sidecar bodies/projections.
3. Update synchronous command metadata.
4. Return committed logical events with positions.

No `await`, model/tool calls, publishing, or external I/O may occur inside the transaction. Live publish happens after append succeeds and is owned by `SessionState`.

### Current limits

- Serialized JSON payloads are guarded below Cloudflare’s 2 MB SQL value/row limit (current hard cap around 1.5 MB).
- Oversized command inputs, messages, summaries, app events, and tool/app results fail before commit; callers should store large data by reference.
- Storage pruning/BaseState is not implemented. Compaction changes model context; it does not delete retained event/body rows.

---

## 10. Live delivery and reconnect

`LiveEventBus` is in-memory fanout. It is downstream of `SessionState` and never defines durable truth.

Durable write path:

```text
EventFactory durable event
  -> SessionState.appendDurable / appendDurableBatch
  -> EDASessionStore.append
  -> fold ReducedState + app reducers
  -> write reducer checkpoints
  -> LiveEventBus.publish (seq, 0)
```

Ephemeral path:

```text
EventFactory ephemeral event
  -> SessionState.publishEphemeral
  -> anchor on ReducedState.lastSeq
  -> allocate subSeq
  -> LiveEventBus.publish (anchorSeq, subSeq)
```

### `eventsAfter(afterSeq)`

`EDARuntime.eventsAfter(afterSeq)` / `EDASessionQuery.eventsAfter(afterSeq)` returns a reconnect-safe merged stream:

1. Subscribe to `LiveEventBus` first.
2. Capture finite durable `replayHead` from the authoritative `SessionState` snapshot.
3. Replay durable events with `afterSeq < seq <= replayHead`.
4. Include same-process active-turn ephemeral replay buffer.
5. Drain buffered live prefix, de-duplicate durable events at/before `replayHead`, keep ephemerals, and sort that finite prefix by position.
6. Forward future live events.

Pure durable replay remains `EDASessionStore.eventsAfter(seq)`.

### WebSocket subscriber behavior

The current WebSocket transport is described in detail in `docs/websocket-protocol.md`. Current implementation facts:

- Server frames are versioned and batch-capable; current sender emits one event per frame.
- Clients ACK `events` frames after applying them.
- Heartbeats are not ACKed.
- Each subscriber has a bounded buffer and ACK-gated sender.
- Slow subscribers are closed independently; they do not backpressure `SessionState`, the durable append path, model streams, tools, sinks, or other subscribers.
- Cloudflare WebSocket attachment stores the last ACKed durable `seq` for hibernation restore.

### Active-turn ephemeral replay

`LiveEventBus` keeps raw active-turn ephemerals only while a turn is open:

- `TurnStarted` opens the buffer.
- Ephemerals emitted during the active turn are retained up to the current capacity (`4096`, newest retained on overflow).
- `TurnCompleted`, `TurnStopped`, or `TurnFailed` clears it.
- Durable Object eviction loses it.

There is no current coalesced live-state snapshot or generic UI projection layer.

---

## 11. Execution flow

### Turn and inference flow

```text
SessionState starts turn
  -> TurnStarted
  -> TurnRunner
     -> InferenceRunner
        -> InferenceStarted
        -> LanguageModel.streamText(... disableToolCallResolution: true)
        -> InferenceState.step for each Response.StreamPart
        -> ephemerals during streaming
        -> ToolCallCreated/Rejected decisions + InferenceCompleted after finish
     -> ToolExecutor for valid framework-owned tool calls
     -> TurnCompleted/TurnFailed, or later turn scheduling when continuation is needed
  -> SessionState consumes steering or terminalizes run/command
```

`InferenceState` is pure. It owns text/reasoning accumulation, speculative tool-parameter drafts, final tool-call decisions, finish sealing, and payload-only emissions. `InferenceRunner` is the effectful shell that mints IDs, validates tool params, constructs envelopes, publishes ephemerals, and commits durable finalization.

### Current stream-part handling

- `text-delta` / `reasoning-delta` → ephemeral `TextDelta` / `ReasoningDelta` and inference-local accumulation.
- `tool-params-start/delta/end` → ephemeral speculative tool-param UI state.
- final `tool-call` → validate params and stage `ToolCallCreated` or `ToolCallRejected` decision.
- `response-metadata` → record provider response/model metadata.
- `finish` → seal the inference and commit staged tool decisions plus `InferenceCompleted`.
- `file` / `source` → counted/dropped with debug logging; first-class content-reference schemas are future work.
- provider-executed tool calls/results → explicit unsupported failure path; EDA does not replay or project provider-executed tools.

After an inference seals:

- Visible assistant text/reasoning is committed as `AssistantMessageCommitted` before tool continuation when present.
- Valid tool calls execute through `ToolExecutor`.
- Tool results are projected into continuation prompts in original model-call order, even though completion events commit in actual completion order.
- Rejected tool feedback can trigger a corrective turn.
- When no continuation is needed, `TurnCompleted` commits with usage rollup when known.

---

## 12. Tools

EDA does not invent a second model-visible tool schema. Tools are upstream Effect AI `Tool` / `Toolkit` values plus EDA lifecycle ownership.

Current `EDAToolRegistry` responsibilities:

1. `getModelToolkit()` returns provider-visible tool definitions.
2. `getParamsSchema(toolName)` validates final provider tool-call params.
3. `execute(toolName, params, context)` runs a sealed framework-owned tool call after `ToolCallCreated` commits.

Current `EDAToolExecutionContext` provides:

- `sessionId`
- `toolCallId`
- `makeEventId()`
- `emitDurable(event)` for app durable facts

There is no current public direct tool `emitEphemeral`, per-tool retry policy object, coalescer registration, or detached background scope in the tool context.

Tool lifecycle boundaries are framework-owned:

```text
ToolCallCreated | ToolCallRejected
ToolCallStarted
ToolCallCompleted | ToolCallFailed
```

Tools may emit app durable facts, but they do not emit framework tool lifecycle events directly.

### Current `runBash` integration

Gia’s `runBash` returns bounded stdout/stderr presentation plus
exit/timing/sandbox metadata in the framework tool result. It requests a shared
50 KiB / 2,000-line tail policy from `idempotent-exec`; if output exceeds that
budget, the terminal snapshot replaces streamed prefixes and names the
sandbox-local complete stdout/stderr artifacts. Framework `ToolCall*` events and
that result are the complete command lifecycle. Gia emits one durable E2B
`SandboxCreated` fact whenever a physical sandbox becomes authoritative; it
does not duplicate per-command lifecycle events.

Live stdout/stderr tailing, file-change events, and lazy artifact APIs are
future work.

---

## 13. App extension model

EDA has no lifecycle callbacks such as `onMessage`, `onChunk`, or `onToolStart`. Applications extend the state machine with typed data and Effect capabilities.

Current extension points:

- **App durable events** under app namespaces.
- **App reducers** via `EDAReducerRegistry`.
- **Durable sinks** and **ephemeral sinks** via `EDASinkRegistry`.
- **Tools** via `EDAToolRegistry` / Effect AI `Toolkit`.
- **Provider/model layers** and runtime config.
- **Prompt projection policy** through host/app layers.
- **Compaction policy/executor layers**.

Apps bind their custom durable-event union to the live protocol once with
`makeEDAWebSocketWireProtocol({ appEvents })`. The binding automatically includes
all framework durable and ephemeral events, configures strict host encoding, and
exports the same event/frame schemas for browser or other WebSocket consumers.
Framework-only hosts reject unregistered app events rather than widening their
payloads to `unknown`.

### App durable events and reducers

Apps can submit durable app events alongside commands:

```ts
await runtime.submit([
  new SubmitMessageCommand({ idempotencyKey, disposition: "queue", content }),
  SlackEvents.messageReceived({ relatedCommandIdempotencyKey: idempotencyKey, ...slack }),
])
```

This commits one ordered batch. Same-batch app events should reference caller-owned ids such as `idempotencyKey` when they need to correlate with framework IDs that EDA mints at admission. Reducers can resolve the key after folding `CommandAdmitted`.

`EDAReducerRegistry` holds pure reducers. App reducer names must not start with reserved `_eda.`. Reducer state is checkpointed in `_eda_reducer_checkpoints` and also exposed in `EDASessionSnapshot.reducerStates`.

An app defines a discriminated serialized reducer-state union and registers its
complete reducer map once with `defineEDAReducerRegistry`. That registry supplies
the runtime reducer list and the schema-checked snapshot serializer. Adding,
removing, or renaming a reducer—or changing its state type—therefore produces a
type error at the app registration point and in consumers of the serialized
snapshot union.

The current examples show the intended shape:

- `examples/002-slack-bridge` — idempotent Slack ingress, reducer correlation across framework/app events, and durable sink delivery.
- `examples/003-sandbox-lifecycle` — framework tool events + app sandbox events reduced into one UI model, including SSR handoff math.

### Sinks

`EDASinkRegistry` has two lanes.

Durable lane:

- named sink with one durable checkpoint in `_eda_sink_cursors`
- reads durable windows after its cursor
- folds framework `ReducedState` and app reducer states through the batch
- filters by `interests`
- calls `process(batch, ctx)`
- exposes `ctx.checkpoint.get(schema, initial)` and `ctx.checkpoint.save(schema, state)` for typed sink-owned state
- serializes state-only saves with cursor commits, including writes from scoped background sink work
- commits staged durable events after success
- atomically commits the current sink-owned payload when advancing the cursor after successful processing/staged commits
- retries failures with capped exponential backoff + jitter

Ephemeral lane:

- live-only, best-effort
- optional interests and buffer policy
- no cursor and no correctness guarantee

Durable sinks are at-least-once. External side effects must be idempotent because a host can die after a remote success but before the local checkpoint write. Existing cursor rows whose payload predates typed checkpoint state retain their cursor and initialize sink-owned state from the supplied default. A stateful sink and the checkpoint-aware runner must roll out together because an older runner can replace the payload while advancing its cursor.

---

## 14. Prompt/context and compaction

Current prompt context is derived from durable state:

- durable system message from optional `EDARuntimeConfig.systemPrompt`, committed once as `SystemMessageCommitted` when a new session initializes;
- optional current cumulative summary from compaction;
- retained durable prompt/context-visible messages after the summary cursor.

Prompt hydration preserves model-facing prefix fidelity. Durable assistant/tool transcript events store exact typed prompt parts in stable model-facing order. Semantic UI/query views derive from those parts rather than rewriting the model-facing history.

### Current compaction implementation

Compaction is implemented as model-context rebase, not storage pruning.

Lifecycle:

```text
CompactionRequested
CompactionStarted
SummaryCreated
ContextRebased
CompactionCompleted | CompactionFailed
```

`ContextRebased` makes a summary current by updating context metadata. After a rebase, prompt hydration reads:

```text
system message + current summary + retained tail messages
```

Current runtime ships disabled/no-op defaults plus injectable policy/executor layers and common presets:

- approximate token threshold policy;
- language-model summary executor.

Compaction never deletes event/body rows. BaseState/storage pruning is not implemented.

---

## 15. Stop and recovery

### StopTurn

Stop is a durable control transition, not just a client-side stream abort.

Current flow:

1. `StopTurn` is admitted and `StopTurnRequested` commits.
2. `SessionState` closes the active turn scope with interruption.
3. Interruption propagates through turn, inference, provider stream, and running tools.
4. Inference/tool finalizers preserve durable partials/terminals they own:
   - `AssistantPartialCommitted` when visible partial text/reasoning exists;
   - `InferenceFailed` with interruption failure details;
   - `ToolCallFailed` with `error.code = "tool.interrupted"` for unfinished tools.
5. `SessionState` commits scheduler-owned terminals:
   - `TurnFailed` with `error.code = "turn.interrupted"`;
   - `RunFailed` with `error.code = "run.interrupted"`;
   - active command cancellation/failure;
   - `StopTurnApplied`;
   - stop-command completion.
6. Queued commands remain durable. Unexpected run failure preserves unconsumed
   steers and schedules framework-owned recovery; explicit stop/interrupt
   converts unconsumed work into a paused queue.

A stop is complete only after the durable stop/turn/run/command terminal events commit.

### Durable commit failure during finalization

Finalization regions prevent user interruption from tearing down lifecycle writes, but storage can still fail. If a required durable lifecycle commit fails, the active runtime treats that as fatal: it stops making later lifecycle claims and lets recovery inspect the incomplete durable log. It must not swallow the store error and then commit misleading later events.

### Recovery

Recovery is deterministic durable repair before new execution starts.

On startup `SessionState`:

1. hydrates framework `ReducedState` from the `_eda.framework.reduced-state` reducer checkpoint plus event-log tail;
2. hydrates registered app reducers from checkpoints plus tail;
3. calls the pure `planSessionRecovery` policy to classify every incomplete lifecycle, pending stop, open compaction, and ownerless steer;
4. interprets that complete plan as one atomic durable event batch, including a replacement `RunStarted` for eligible interrupted `SubmitMessage` or `ResumePendingMessages` work and a final `RecoveryCompleted` barrier;
5. refolds state and requires recovery to reach either an empty plan or the single intentionally continued command/run;
6. checkpoints the repaired state, starts the replacement turn with only its unconsumed source messages and eligible steers, and only then forks the long-lived control loop.

The synchronous startup pass is part of the host wakeup contract: a Durable Object alarm that cold-starts a session runtime does not return from runtime construction until stale lifecycle repair has had a chance to commit durable terminal events. After that finite pass, the long-lived control loop handles live ingress and active-turn completion.

`RecoveryCompleted` is emitted only when startup recovery commits at least one
lifecycle repair, stop application, compaction repair, message resumption, or
replacement run. An idle cold start emits no recovery event. The event is last
in the same atomic batch as the repairs, so its presence is the durable boundary
for a completed recovery transaction. Its optional `continuation` payload
records `{ commandId, interruptedRunId, replacementRunId }` when recovery
transparently resumes a command. “Completed” describes the repair transaction,
not the later completion of resumed agent work.

The post-recovery live loop does not silently repair stale active durable work. Once startup recovery has completed, durable replay and in-memory execution state must agree: an active durable command must have an in-memory execution owner. If that invariant is violated in a warm runtime, EDA treats it as a fatal runtime error and relies on the dedicated startup recovery path after a clean rebuild rather than patching over it inline.

The recovery functional core has no storage, fibers, clocks, or event minting.
The startup imperative shell owns those effects and is the only code that
interprets a recovery plan. Open-compaction handling is part of this same plan,
not a second repair pass. Normal live model/tool/run failures remain ordinary
domain transitions while an in-memory owner exists.

Current recovery behavior includes:

- started tool calls without terminal → `ToolCallFailed`;
- open inferences → `InferenceFailed`;
- open turns/runs → `TurnFailed` / `RunFailed` with interruption-coded failure metadata;
- one eligible active user-work command remains open and continues in an atomically scheduled replacement run; any additional stale active commands are cancelled;
- pending `StopTurnRequested` takes precedence, cancels the targeted work, pauses pending messages, and prevents transparent continuation;
- completed tool calls are not rerun blindly;
- pending queued commands remain pending;
- eligible pending steers join the replacement turn boundary; other ownerless steers receive one framework-owned `ResumePendingMessages` command;
- open compactions are failed or completed according to their durable record;
- stale stop commands targeting no active turn are cancelled rather than throwing.

This gives reconnecting clients a durable explanation of stale work before replacement live activity begins.

---

## 16. Cloudflare host and pluggability

Current production host:

- concrete subclasses of `EDASessionDurableObject` are registered in Wrangler;
- one Durable Object instance/database stores one session;
- host startup runs EDA schema migrations via `ctx.blockConcurrencyWhile(...)`;
- host entrypoints expose RPC methods for submit, submitBatch, submitAndBlock, blockOnCommand, snapshot, messages, backup/destroy, alarm, and WebSocket events;
- `EDASessionDurableObjectHost` owns the scoped `ManagedRuntime` for the session;
- runtime construction retries after a failed hydration/build instead of pinning the warm isolate to a rejected promise;
- `EDAKeepAlive` integrates with Durable Object alarms/leases so active control/model/sink work can keep the object awake enough to finish or reschedule;
- session destruction shuts down in-memory keep-alive state and deletes the Durable Object alarm before deleting storage;
- WebSocket hibernation restores subscribers from serialized attachment state.

The Cloudflare host imports Cloudflare APIs. Core domain/runtime code depends on services such as `EDASessionStore`, `SessionContext`, `IdGenerator`, `EDAKeepAlive`, model/tool layers, and live subscriber abstractions.

The host boundary is intentionally small enough for future non-Cloudflare hosts. Another host must provide the same semantics: one ordered durable session log, atomic append, per-session coordination, resumable live delivery, durable sink checkpoints, clock/id services, and wakeup/keep-alive hooks.

---

## 17. Observability and tests

### Observability

Current tracing uses compact `eda.*` attributes from `services/tracing.ts`. Spans belong on meaningful runtime boundaries:

- host RPC/WebSocket entrypoints;
- runtime submit/block;
- durable store append/replay/hydration;
- session control drain/start/stop/recovery;
- turn/inference execution;
- tool execution;
- sink drains;
- compaction policy/execution.

Avoid instrumentation noise on pure helpers, refs, and pass-through query helpers.

### Tests

The current suite includes:

- pure domain tests and property tests;
- in-memory runtime tests;
- Durable Object host/storage tests;
- WebSocket subscriber tests;
- sink/reducer tests;
- compaction tests;
- crash/recovery simulations;
- example scenario tests.

Future hardening work should stay out of this current-state doc unless it is clearly marked in the specific proposal document that owns it.

---

## 18. Current-state checklist

EDA currently provides:

- durable command admission with command idempotency lookup;
- one active run/turn per session;
- `queue`, `steer`, and `interrupt` message semantics;
- durable system/user/assistant/partial message commits;
- exact model-facing prompt parts for assistant/tool transcript replay;
- Effect AI model streaming with framework-owned tool resolution;
- ephemeral text/reasoning/tool-parameter live deltas;
- durable run/turn/inference/tool/stop lifecycle boundaries;
- Effect-scoped tool execution and interruption;
- custom app durable events, reducers, and reducer checkpoints;
- durable sinks with at-least-once cursor semantics;
- best-effort ephemeral sinks;
- Cloudflare Durable Object SQLite store and WebSocket host;
- reconnect-safe `eventsAfter` merged stream;
- deterministic recovery of incomplete durable lifecycles;
- model-context compaction/rebase without storage pruning;
- testkit layers for in-memory model/store/host-style tests.

EDA currently does **not** provide:

- storage pruning/BaseState;
- generic persisted UI projection items/pagination;
- coalesced live-state snapshots for reconnect;
- direct tool ephemerals or per-tool retry policy objects;
- provider-executed tool projection/replay;
- durable stdout/stderr chunks or file-change artifacts for sandbox tools;
- app-side physical payload sidecars/chunking;
- a non-Cloudflare production host.

---

## 19. Adjacent documents

- `docs/message-steering.md` — current durable message queue and steering semantics.
- `docs/websocket-protocol.md` — current WebSocket protocol plus marked future hardening notes.
- `docs/testing.md` — test archetypes and fidelity ownership.
- `docs/ui-projection.md` — future generic UI projection proposal, not current runtime behavior.
- `docs/subagents.md` — future EDA/Gia subagent proposal, not current runtime behavior.
