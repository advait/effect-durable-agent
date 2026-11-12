# Changelog

## Unreleased

- **Fix:** an idle session object no longer wakes every 30 s forever. `SessionState.start` forked
  the live control loop, so on a cold start the loop's first drain took its `session-control`
  keep-alive lease after the alarm handler had found no work and cleared the heartbeat. The lease
  armed the heartbeat again and, by design, its release did not clear it. When the object was
  evicted within the interval, the next alarm cold-started it and the cycle repeated, for ended
  and never-used sessions alike: 2,880 alarm invocations a day per object. `start` now runs the
  first drain before it returns and forks the loop after it, so a runtime build, and the alarm
  handler that awaits it, sees that lease released. A drain failure still stops the loop and logs
  as before. Work the drain starts, such as a turn, holds its own lease, so a session with work in
  flight still gets its heartbeat. A cold build waits for that first drain; it is short unless the
  drain reaches a compaction boundary. No storage, protocol or API change. Existing looping objects
  stop at their next alarm after the host deploys this release.

## 0.2.0

Adds dedupe keys for durable events (FR-5) and deciders (FR-1), and admission rules for every
app-written event. Minor release with breaking changes: the framework-namespace rule, the
JSON-native payload rule, new conflict codes on the wire, and the `stageDurable` and sink
`process` signatures. Storage: Durable Object migration 4 (forward-only). **One-way:** a 0.1.0
runtime cannot decode `DeciderAccepted`, so after a host's first accepted decision it cannot roll
back to 0.1.0.

- **Feature:** dedupe keys for durable events. `DurableEventEnvelope` gains an optional
  `dedupeKey: EventDedupeKey` (1-512 characters, exported from `effect-durable-agent/types/events`).
  A session log holds at most one event per `(namespace, type, dedupeKey)`. `EDASessionStore.append`
  resolves each entry, in order, to the event with its `eventId`, else to the event with its
  dedupe triple, else inserts it; a collapsed entry returns the first event's id, payload and
  position, writes nothing and does not advance the head. Callers set the key wherever they build a
  durable event, including `ctx.emitDurable` in tools and `ctx.stageDurable` in sinks. See
  `docs/spec.md` §8, §9 and §13.
- **Breaking:** `EDASessionStoreShape` gains `findEventByDedupeKey({ namespace, type, dedupeKey })`.
  A custom store implementation must add it; `registerAppEvents` decodes its result like
  `findCommandAdmission`.
- **Fix:** `SessionState` folds and publishes each committed position once. A batch that repeated
  an event (the same `eventId` twice, or now the same dedupe key) was folded and published twice,
  so live reducer state differed from a replay of the log.
- **Fix:** `submit` refuses an app event whose serialized payload exceeds the new
  `maxAppEventBytes` (1 MiB) with a `SessionCommandAdmissionConflict` of code `event_too_large`,
  before the append. Before, an event over the store's row cap failed as a store error, which made
  the live runtime fatal, so every later submit failed too. `SessionCommandAdmissionConflict.code`
  gains the `event_too_large` literal.
- `ctx.stageDurable` logs a warning for a staged event without a `dedupeKey`: the runner commits
  staged events and the sink cursor in separate transactions, so a crash between them restages the
  event.

Storage: Durable Object migration 4 adds the nullable `_eda_event_log.dedupe_key` column and the
partial unique index `_eda_event_log_dedupe_key_idx` on `(namespace, type, dedupe_key)`. No stored
event is rewritten; existing rows have a `NULL` key, and their replay is unchanged. A 0.1.0 reader
of a migrated log ignores the column, and with it the key. Protocol: one optional envelope field on
the wire (WebSocket frames and RPC). API: the optional envelope field, `EventDedupeKey`,
`maxAppEventBytes`, the store-port method and the new conflict code. No transcript interpretation
change.

- **Feature:** deciders (`effect-durable-agent/services/decider`). `EDADecider.define` declares a
  pure, synchronous decision over the session's committed state with its `input`, `events`,
  `result` and `rejection` schemas and a required `idempotencyKey`. Register deciders once with the
  new `deciders` option of `makeEDARuntimeLayer`, the Cloudflare and celld runtime and Durable
  Object options, and `makeEdaTestLayer`, then call one with `EDARuntime.decide(decider, input)`, a
  tool's `ctx.decide`, or the new `decide` RPC. The decision reads the fold of the log and appends
  under the same admission and append gates `submit` takes, so no tool, sink or control-loop write
  lands in between. An acceptance commits a `DeciderAccepted` marker first in its batch, with
  `dedupeKey` = decider name, a line feed and the key; a retry with the same key returns the stored
  result with `replayed: true` and commits nothing. A rejection commits nothing. Drafts are encoded
  with the decider's `events` schema and checked against the registered `appEvents`; the result is
  stored in its JSON form (an optional field left `undefined` is absent) and capped at 16 KiB
  (`maxDeciderResultBytes`). Failures are `EDADeciderError` (`unknown_decider`,
  `invalid_input`, `decide_threw`, `invalid_output`), never store errors, so the runtime stays
  usable. Construction fails on a duplicate name, a name outside `/^[A-Za-z0-9._-]{1,128}$/` or
  under `_eda.`, or a decider that emits an app namespace `appEvents` does not register. See
  `docs/spec.md` §6, §7, §12 and §13.
- **Feature:** the Cloudflare host exposes `decide`. `edaSessionRpc.decide`
  (`SessionDecideInput`, `SessionDecideResult`), `EDASessionController.decide`,
  `EDASessionDurableObject.decide(raw)`, and `encodeDecideInput` / `decodeDecideOutcome` for the
  caller's side of the RPC (also re-exported by celld). `decodeDecideOutcome(decider, raw)` takes
  the stub's `decide` return value as is and decodes it. It is trusted ingress, like `submitBatch`.
- **Feature:** tools get `ctx.decide` and `ctx.snapshot()`. `snapshot` returns
  `{ state, reducerStates }` like `EDARuntime.snapshot` and can be stale by the time the tool
  emits; a tool whose fact depends on state uses `decide` with its `toolCallId` as the key.
- **Breaking:** `submit`, decider drafts, `ctx.emitDurable` and `ctx.stageDurable` refuse framework
  events. An event in the `effect-durable-agent` namespace is refused unless its type is
  `SessionConfigured`, `SystemMessageCommitted` or `AssistantMessageImported`, with a
  `SessionCommandAdmissionConflict` of code `framework_event` (`invalid_output` for a draft).
  Before, app code could append `RunCompleted` or any lifecycle event; now a forged
  `DeciderAccepted` cannot make a later decision replay a result nobody decided. Test fixtures that need other framework events seed the
  store with `EDASessionStore.InMemorySeeded`.
- **Breaking:** app event payloads must be JSON-native. `submit`, `ctx.emitDurable` and
  `ctx.stageDurable` refuse a payload that does not survive `JSON.parse(JSON.stringify(payload))`
  unchanged (`Redacted`, `Option`, `Date`, a class instance, `undefined` in an array, a non-finite
  number), failing with a `SessionCommandAdmissionConflict` of code `payload_not_json`. Before, the store wrote whatever `JSON.stringify` made of such a value: a `Redacted` email
  was stored as `"<redacted>"`. An object property whose value is `undefined` is still accepted.
  Encode such values before writing; encode-on-write for `submit` is planned.
- **Breaking:** `submit` checks registered app events against the runtime's `appEvents` schema
  before the append and refuses a mismatch with code `invalid_event`. Before, a mismatch failed
  inside the append as a store error and made the runtime fatal. An allowlisted framework event
  (`SessionConfigured`, `SystemMessageCommitted`, `AssistantMessageImported`) is checked the same
  way against its framework schema. `ctx.emitDurable` and
  `ctx.stageDurable` apply the same schema and `maxAppEventBytes` checks.
- **Breaking:** `ctx.stageDurable` returns `Effect<void, SessionCommandAdmissionConflict>` (was
  `Effect<void>`), and a durable sink's `process` may fail with that conflict. A sink that catches
  it keeps the rest of what it staged. One that lets it escape `process`, as a failure or through
  `orDie`, stops its runner: none of that delivery's staged events commit, the cursor stays, and
  the next runtime start redelivers the batch. A sink whose `process` already swallows every typed
  error compiles unchanged and now sees the refusal there. Hand-built `EDASinkContext` fakes with
  an infallible `stageDurable` still type-check.
- **Breaking:** `ctx.emitDurable` can fail with `SessionCommandAdmissionConflict`, and
  `EDAToolExecutionContext` gains the required `decide` and `snapshot` members, so a hand-built
  tool context (a test fake) must add them. `SessionCommandAdmissionConflict.code` gains
  `framework_event`, `invalid_event` and `payload_not_json`, which also reach RPC callers.
- **Fix:** admission checks the JSON form the store keeps, not the value app code passed
  (gpt-6-astra, FR-1 review round 2). `submit`, decider drafts, `ctx.emitDurable` and
  `ctx.stageDurable` replace each app event's payload with `JSON.parse(JSON.stringify(payload))`
  before the size, `appEvents` and framework-schema checks, and append exactly that event. Before,
  an `AssistantMessageImported` tool-result part with `result: undefined` passed its framework
  schema in memory, lost the required `result` in JSON, and the store refused it as a store error,
  which made the runtime fatal. Such an event, or an app event with a required field left
  `undefined`, is now refused with `invalid_event` (`invalid_output` for a draft). Live reducers
  and sinks see the same JSON payload a replay reads.
- **Fix:** a decider's accepted result is the one its replays return. The result is encoded into
  its JSON form, decoded with the `result` schema and checked to encode again before anything
  commits; a failure is `invalid_output` and commits nothing. Before, a `Schema.UndefinedOr` field
  left `undefined` committed a marker whose `{}` no retry could decode, and through
  `EDASessionController.decide` an optional field left `undefined` committed the marker and its
  facts and then failed `invalid_output` encoding the reply. `Accepted.result` on the first call
  is now the value decoded from the stored JSON. New export `encodeDeciderJson` from
  `effect-durable-agent/services/decider`, which the `decide` RPC uses for results and
  rejections; a rejection with an optional field left `undefined` now encodes without it instead
  of failing.
- A durable sink that lets a `SessionCommandAdmissionConflict` escape `process` stays stopped
  until the host builds a new runtime: the session keeps accepting work, but that sink processes
  nothing more, and the next runtime start redelivers the batch. Catch the conflict around
  `ctx.stageDurable`. A `ctx.checkpoint.save` failure escaping `process` through `orDie` is still
  a generic defect that skips the batch, as in 0.1.0.
- `EDADeciderDefinition.decide` takes its types from the schemas, never from its return value. A
  literal nested in the returned object widens (`payload: { to: "ai" }` checks as `string`), so
  build nested values with their schema's `make` or write them `as const`.

Storage: no new table or migration. A `DeciderAccepted` marker is an ordinary framework event in
`_eda_event_log`, keyed through the `dedupe_key` column and index of migration 4. **One-way:** a 0.1.0 runtime
cannot decode `DeciderAccepted`, so once a session has accepted a decision it cannot be served by
0.1.0. Deploy this release first and adopt deciders in a later deploy. Protocol: raw event sockets
and RPC replies can carry the new `DeciderAccepted` framework event (transcript sockets never do),
the new `decide` RPC, and the new conflict codes. API: the `services/decider` subpath,
`EDARuntimeShape.decide`, the `deciders` options, the tool context members, the host RPC and
helpers, `encodeDeciderJson`, and the admission rules above. Admission writes the same bytes as
before for every event it admits (the store always kept the JSON form); it now refuses the events
whose JSON form a schema refuses, instead of failing them in the store. No transcript interpretation change: the interpreter and
`ReducedState` ignore the marker, and `transcriptSemanticRevision` and the recorded fingerprints
are unchanged.

## 0.1.0

First stable release. It publishes to the `latest` npm dist-tag; `alpha` stays at
`0.1.0-alpha.30`. See [versioning](./docs/releasing.md#versioning): 0.x semver, and durable data is
forward-compatible only (no rollback to an older release after a newer one has written).

- **Breaking:** Effect `4.0.3` to `4.1.2`. The `effect`, `@effect/ai-openai` and
  `@effect/ai-anthropic` peers are now `~4.1.2` instead of an exact pin, so hosts may take Effect
  patch releases without an EDA release. Hosts must upgrade their aligned Effect set to 4.1.
- `IdGenerator.Live` mints UUIDv7s through Effect's `SecureRandom` (`effect/crypto`), which
  replaced `effect/Crypto`.
- Inference response metadata no longer records `request`: Effect AI 4.1 removed it from
  `ResponseMetadataPart`. The stored field is `Schema.Unknown`; existing events read unchanged.
- `JsonPromptFilePart` also encodes Effect's tagged JSON forms of file bytes and URLs.
- Install instructions use the default dist-tag instead of `@alpha`.

No storage format or protocol change.

## 0.1.0-alpha.30

- **Feature:** tool recovery policies. A tool annotated `ToolRecovery: "reinvoke"`
  (`effect-durable-agent/services/tool-registry`) has a call that was running when the runtime
  died re-invoked by startup recovery, with `EDAToolExecutionContext.recovery = { attempt,
  firstStartedAtMs, firstStartedSeq }`, instead of being failed. Tools without the annotation keep
  today's behaviour (`fail`). Recovery re-invokes before planning its repair batch, so one
  interrupted inference can recover in a mixed state (re-invoked results plus restart failures),
  and the replacement run's prompt sees both. At most `maxToolCallAttempts` (3) attempts per call.
  When re-invocations are needed, startup returns without waiting for them: reads and admission
  proceed while drains, grants and scheduling delivery wait for recovery (`agent.session.recover.tools`
  span); admission waits only while recovery commits its repair. If that background recovery
  fails, the runtime records it as fatal so later work fails instead of being accepted unprocessed. See `docs/spec.md` §15 "Tool recovery policies".
- `ToolCallStarted` gains an optional `attempt` (2 or more) for re-invocations; the reducer keeps
  the first start's `startedSeq`/`startedAtMs` and records `attempt` on the tool-call record,
  which reducer checkpoints carry. Older readers retain the field as excess and treat the
  extra start as a restart of the same call. The transcript keeps the first start's time (no
  transcript revision change: logs without re-invocations project identically; after a rollback,
  an older reader shows a re-invoked call's start as the re-invocation's).
- While background recovery runs, an alarm's scheduling redelivery returns at once (recovery
  delivers when it finishes), so the host re-arms its heartbeat promptly.
- `EDAToolExecutionContext.queryEvents` accepts `afterSeq`.
- The test kit's `makeEdaTestLayer` accepts a full `toolRegistry` shape.

## 0.1.0-alpha.29

- **Fix:** the keep-alive heartbeat keeps itself armed when its alarm is delivered at exactly the
  scheduled millisecond. Inside the alarm, the Workers clock commonly reads exactly the scheduled
  time, and `DurableObjectAlarmScheduler`'s owner view reported a wakeup at `now` as still armed.
  The keep-alive then skipped its re-arm and the handler cleared the wakeup it fired for, so
  active work lost its heartbeat at most alarms; work whose next lease came more than 70-140 s
  later (a long inference or tool) was evicted as before alpha.28. Measured in production on
  alpha.28: the handler re-armed after 0 of 31 alarms delivered on time. The owner view now reports
  a wakeup at or before `now` as no alarm. No storage, protocol or API change.

## 0.1.0-alpha.28

- **Fix:** the Cloudflare keep-alive heartbeat is delivered during multi-step work instead of
  being postponed forever. `DurableObjectKeepAlive` deleted the alarm whenever its last lease was
  released and re-armed it one interval ahead on the next acquire. A model run releases and
  re-acquires between turns, so with turns shorter than the interval the alarm never fired, the
  object received no inbound event, and Cloudflare evicted it 70-140 s into the run despite its
  in-flight model and sandbox I/O
  (startup recovery then failed the open inference or tool). Releasing work now leaves an armed
  alarm in place; only the alarm handler, finding no work left, or `shutdown()` clears it. An object
  that goes idle receives one final heartbeat. No storage, protocol or API change.

## 0.1.0-alpha.27

Hosts read user-visible history only from the transcript.

- **Breaking:** the host-facing APIs that exposed the runtime's pruned model transcript as if it
  were history are removed. Compaction prunes that transcript, so hosts rendering it lost earlier
  history (`docs/transcript-history.md` §2); read history with `readTranscriptWindow` instead. A
  host that has not enabled transcripts (opt-in since alpha.25) must enable them first; model-side
  code can still derive the pruned transcript with `durableMessageTranscript(snapshot.state)`.
  - `EDASessionSnapshot` is now `{ state, reducerStates }`: its `messages` field and the exported
    `DurableTranscriptMessages` schema are gone.
  - `EDASessionQueryShape.messages`, `EDARuntimeShape.messages` and
    `EDASessionController.messages` are removed, with the `agent.messages.list` span.
  - **`EDASessionRpcSurface` loses its `messages` member** (and `edaSessionRpc.messages`, the
    Durable Object's `messages()` method, and so celld's `EDASessionCell`). The snapshot RPC output changes shape, so deploy the
    Worker and the Durable Object together.
  - Model-facing code is unchanged (`durableMessageTranscript`, the prompt projector, compaction,
    `ReducedState`): prompt bytes are identical.
- A first build that froze as failed reads as `Unreadable` instead of `Building`: when no version
  is active and the build of `current` is `Failed` (which is never retried), window reads and
  transcript sockets report it with the failed version, seq and error, and connected sockets are
  re-read when it freezes. New export `inactiveTranscriptWindow` (`effect-durable-agent/transcript`).
  Read-side only: no storage migration, semantic revision or fingerprint change.
- Examples 001, 002 and 004 enable transcripts and serve history through `readTranscriptWindow`.

## 0.1.0-alpha.26

- Transcript windows trim to their byte budget in linear time. Previously a window over its budget
  dropped one record at a time and re-measured the whole window after each drop (quadratic), so a
  tool-heavy session's first read with a large inline-body limit could block its Durable Object for
  seconds (about 6 s for 15 tools × 63 KiB at 100 records; now about 0.2 s). Output is identical to
  the previous trim; interpretation and transcript versions are unchanged.

## 0.1.0-alpha.25

Transcript history, part 4: production rollout controls. This is the first release a host may
adopt in production with transcripts enabled.

- **Breaking:** the Durable Object base (`EDASessionDurableObject`, and so celld's `EDASessionCell`)
  maintains no transcript unless the host sets
  `EDASessionTranscriptOptions.enabled` (`boolean | (sessionId) => boolean`, default off; resolved
  once per object instance). alpha.24 maintained every session.
  - A disabled session gets no transcript writes, builder slices or wakeups; `transcriptStatus`
    returns `Disabled` without writing, `readTranscriptWindow` returns `{ _tag: "Disabled" }`, and a
    `?stream=transcript` upgrade is refused with 409.
  - Enabling a session takes the first-introduction path (build from the log, then activate).
    Disabling leaves its rows untouched; re-enabling with an active version behind the head serves
    `UpdatingHistory` until builder slices catch it up, then re-reads every transcript socket.
  - `TranscriptStorageStatus` is now a tagged union `Enabled | Disabled`.
- Bounded builder retries: a slice that keeps failing while advancing a build freezes that build
  as failed after `maxSliceFailures` consecutive failures (default 5, `transcript.sliceLimits`;
  migration 3 adds `_eda_transcript_meta.slice_failures`) and raises `ProjectionFailed`. A failing
  builder alarm backs off exponentially (30 s doubling, capped at 1 h); its failure count survives
  eviction (`_eda_alarm_wakeups.failures`), and after the limit the builder stops scheduling itself
  and raises the new `TranscriptAlert` `BuilderStopped`.
- **Breaking (unions and limits):** `SessionTranscriptWindowResult` gains `Disabled` and
  `TranscriptAlert` gains `BuilderStopped` (exhaustive matches must handle them);
  `DurableObjectTranscriptSliceLimits` gains a required `maxSliceFailures`, and slice limits that
  are not positive integers now throw at construction.
- `alarm()` logic is extracted into `runEDASessionAlarm` and unit-tested.
- A WebSocket upgrade naming another session gets 400 instead of 500.
- `effect-durable-agent-cloudflare`: storage-only RPCs `readTranscriptBody`
  (`SessionTranscriptBodyInput { source: { seq, path }, incarnation }` →
  `Body | NotFound | IncarnationMismatch | Disabled`) and `readTranscriptSummary`
  (`SessionTranscriptSummaryInput { summaryId, incarnation }` →
  `Summary { TranscriptCompactionSummary } | NotFound | IncarnationMismatch | Disabled`). They
  resolve only the interpreter's body sites, require the reference's log incarnation, and are
  pinned to the object's session. **`EDASessionRpcSurface` gains both as required members**, so
  hand-written implementations and mocks must add them. `DurableObjectTranscriptStore` gains
  `incarnation()`.
- `effect-durable-agent/transcript`: `resolveTranscriptBody` / `ResolvedTranscriptBody`, sharing
  the interpreter's body normalization (transcript output is unchanged).
- `effect-durable-agent/testkit/layers`: `TestModelParts` may be a function choosing each model
  call's stream from its prompt.
- `effect-durable-agent/websocket/events`: the raw live-event WebSocket contract (frames, delivery
  rules, messages) without the transcript-window stream. `effect-durable-agent/websocket` is
  unchanged; event-socket clients that import the new entry no longer bundle the transcript
  protocol, whose schemas cannot be tree-shaken.

## 0.1.0-alpha.24

Transcript history, part 3: Durable Object storage. **Development release: do not adopt in
production.** In this release, maintenance and builds run for every `EDASessionDurableObject` /
`EDASessionCell` session;
0.1.0-alpha.25 makes them opt-in per session and bounds builder retries.

- `effect-durable-agent-cloudflare`: the Durable Object session store maintains transcripts inside
  the append transaction (forward migration 2: `_eda_transcript_meta`, `_eda_transcript_records`,
  `_eda_transcript_aliases`, `_eda_log_incarnation`, `_eda_alarm_wakeups`). A projection failure
  freezes only that version; events still commit. Where the platform rejects nested
  `transactionSync` (celld), plans are staged before any write and a write failure fails the
  append atomically.
- `DurableObjectTranscriptStore` (runtime-free windows, meta and status; the `EDATranscriptStore`
  port) and `DurableObjectTranscriptBuilder` (bounded slices, activation at the head, old-row
  cleanup, stale-build discard, missed-session safety net).
- `EDASessionTranscriptOptions` on the Durable Object: `definitions`
  (`TranscriptMaintenanceConfig`), `adoptIncarnationKey` (adopt a host's `ctx.storage.kv`
  incarnation, e.g. Gia's `gia.log_incarnation`), `onAlert`, `onActivated`.
- Storage-only RPCs `transcriptStatus` (advances builds; for host backfills) and
  `readTranscriptWindow` (SSR, CLIs); neither builds the model runtime nor starts sinks.
- Shared alarms (`durable-object-alarms`): runtime and builder each keep a durable wakeup, and an
  alarm armed by an older release is adopted as the runtime's.
- `transcriptSockets: {}` serves `?stream=transcript` from the object's own store; activating a
  rebuilt version refreshes every transcript socket, held or hibernated.
- Destroying a session closes hibernated transcript sockets (4009); the client-frame limit counts
  UTF-8 bytes.
- New `effect-durable-agent/testkit/transcript` test helpers.
- `EDASessionRpcSurface` gains required members `transcriptStatus` and `readTranscriptWindow`
  (hand-written implementations or mocks must add them).
- Additive lower-level options and exports: a `transcript` option on
  `DurableObjectSessionStoreOptions`, `EDADurableObjectRuntimeLayerOptions` and
  `EDASessionRuntimeOptions`; `EDASessionRuntimeOptions.alarmStorage`;
  `EDASessionController.refreshTranscriptWebSockets()`; `TranscriptStorageStatus`,
  `SessionTranscriptWindowInput` / `SessionTranscriptWindowResult`, `AdoptedLogIncarnation`,
  `DurableObjectAlarmOwner`.
- Migration 2 is forward-only; rolling back to an earlier release is unsupported
  (`docs/transcript-history.md` decision 10).

## 0.1.0-alpha.23

Transcript history, part 2: live delivery. Inert until a host passes a transcript store; the raw
event stream and the model path are unchanged.

- Live delivery (design §9): `readTranscriptSubscription` and `planTranscriptUpdate` answer
  subscribe, page (before or after) and retain with one read at one cut through the
  `EDATranscriptStore` port; every update is a complete-cut `replace` or `patch`.
- Active content: the streaming inference's parts under stable ids (`TranscriptActivePart`),
  retired in the same update as the inference's durable
  result on every terminal. `TranscriptActiveContent.parts` changes shape (no producer existed).
- Protocol (`websocket/transcript*`): a negotiation envelope decoded before anything
  version-specific (also for SSR data); request, update and chunk frames; chunked updates applied
  only when complete; `appliedUpdateSeq` on the existing ACK (additive); a framework-agnostic client
  reference store.
- `effect-durable-agent-cloudflare`: `?stream=transcript` (`EDA_WEB_SOCKET_STREAM_PARAM`) sockets
  enabled with `transcriptSockets: { transcriptStore }` (`EDATranscriptSocketOptions` also tunes flow
  control, subscription policy, delivery limits and staging); live content at
  `activeContentCadenceMs` (default 50 ms, `defaultActiveContentCadenceMs`); per-subscriber
  serialization; a publication fence so
  live content is served only with a read at the published head; hibernation checkpoint
  (`eda-transcript-v1`); a socket closes with 4009 (wake mid-transfer, session reset, staging
  budget exceeded) and the client resubscribes. Without a store, or with a projection,
  `?stream=transcript` returns 400.
- `makeEdaTestLayer` accepts a `sessionEventObserverLayer` option.
- Transcript semantic revision 2: submissions consumed by one turn are ordered by submission, as
  the prompt orders them, instead of by the turn's input list (for example a promoted queued
  message and later steering read together). Every transcript version changes, so sessions rebuild
  and hosts run their backfill; no host maintains a transcript yet.
- Transcript mappings: `ReplaceListEntry` accepts an `IdOrOrdinal` key. The list is kept in
  ordinal order, and a new entry replaces every entry with the same id or the same ordinal, so a
  retried input never leaves a stale duplicate within the mapping bounds (Gia's attachments and
  references). List entries keyed this way record their `id`. Existing definitions, their versions
  and their output are unchanged, so no semantic-revision bump; older readers ignore the extra
  entry field.

## 0.1.0-alpha.22

Transcript history, part 1: the EDA core transcript projection (design:
`docs/transcript-history.md`). Hosts are unaffected until they read it; the model path, prompts and
`EDASessionStoreShape` are unchanged.

- New `effect-durable-agent/transcript` export: transcript record, window, cursor, selector and
  readiness schemas; declarative host mapping definitions (executed by `planTranscriptBatch`); version identity
  (`hash(semantic revision, mappings)`); the synchronous interpreter boundary
  (`PlanTranscriptBatch`, `TranscriptLookup`, `TranscriptChangePlan`) and its implementation; the
  `EDATranscriptStore` read port and `TranscriptMaintenanceConfig`.
- `EDASessionStore.InMemory` / `InMemorySeeded` take optional `InMemoryStoreOptions`
  (`{ transcript?: TranscriptMaintenanceConfig }`; default: the definition with no host mappings),
  maintain the transcript inside the append critical section, freeze a failed version, and provide
  `EDATranscriptStore`.
- Oracle (incremental equals full rebuild for every batch partition) and fingerprint tests.

## 0.1.0-alpha.21

Multi-provider sessions (OpenAI and Anthropic), provider prompt caching, and provider-native
compaction.

- Committed assistant prompt parts keep provider metadata from streamed text and reasoning parts:
  Anthropic thinking signatures and redacted thinking, OpenAI item ids and encrypted reasoning.
  Replayed requests therefore carry valid reasoning; previously reasoning was replayed as text and
  providers dropped it. Reasoning with empty display text is kept when it carries metadata.
- `buildEDAPrompt` marks the end of the instruction prefix and the end of durable history with a
  provider-neutral cache hint (`isEDACacheBreakpoint`); per-request projected data messages are
  never marked.
- `ModelResolver` gains an optional `preparePrompt(selection, prompt)` hook applied before every
  inference and compaction call.
- Compaction executors receive `prompt` (the exact provider prompt through the planned source
  range, as an earlier request sent it) and `toolkit`. `CompactionRunnerInput` gains
  `reducerStates`. `ApproximateTokenThreshold` gains `retain: "last-response"`, which keeps the
  latest assistant response verbatim so the summarized range equals an earlier request.
- New `promptPrefixSummaryCompactionExecutor` (provider-neutral summary reusing the conversation
  prefix), `nativeCompactionSummaryMessage` / `getNativeCompaction` prompt helpers, and
  `generateTextOfferingTools` (`effect-durable-agent/services/model-call`).
- `effect-durable-agent-cloudflare`:
  - `./anthropic`: Anthropic client over EDA's transport, `anthropicAgentModelConfig`, and prompt
    preparation that turns cache hints into `cache_control` breakpoints.
  - `./providers`: `makeEDAMultiProviderModelResolverLayer` (routes each recorded selection to its
    provider) and `makeEDANativeCompactionExecutorLayer` (OpenAI `/responses/compact`, Anthropic
    on-demand compaction `compact-2026-09-04`, with usage taken from `usage.iterations`).
  - The OpenAI client now uses the same transport, which retries connection failures before a
    response like the provider SDKs and replays native compaction items.
  - `@effect/ai-anthropic` 4.0.3 is an optional peer dependency.

## 0.1.0-alpha.20

Adopt stable Effect 4: `effect`, `@effect/ai-openai` and `@effect/vitest` move from `4.0.0-beta.102`
to `4.0.3`, with Effect tsgo 0.51.1 and Oxlint 1.87.0.

- Import Effect AI, HTTP and SQL modules from their stable `effect/<area>` paths, and use the
  renamed `Schema.TaggedError` and `LanguageModel.LanguageModel` APIs.
- Add `idGeneratorLayer` to the core and Cloudflare runtime options so hosts can own lifecycle
  identity generation. It defaults to `IdGenerator.Live`.
- Effect 4.0 removed `onExcessProperty: "preserve"`. Framework and registered application facts
  now decode with the default policy and explicitly re-attach payload fields their schema does not
  model, so the excess-field guarantee from alpha.17 still holds. Keys a schema consumes, such as
  renamed keys, are not re-attached. Unknown fields nested under a renamed key, or inside a
  decoded `Option` or class instance, are not retained. Replay only re-encodes events whose
  decode dropped a key, so events written by a newer version cost one extra encode.
- Framework tool-result prompt parts set `providerExecuted: false`, which stable Effect requires.
  Stored parts without the field still decode to `false`.
- With `disableToolCallResolution`, stable Effect streams tool-call `params` in their encoded
  form, so `ToolCallCreated.promptPart.params` now persist encoded. This only differs for tools
  whose parameter schemas transform values.
- Stable `Prompt.fromResponseParts` copies stream metadata into assistant text-part `options`, so
  new `AssistantMessageCommitted` text parts persist provider options such as an OpenAI item id,
  and replayed requests include them.
- Effect AI telemetry follows current GenAI conventions, for example `gen_ai.provider.name`
  instead of `gen_ai.system` and `effect.ai.tool_choice` instead of `toolChoice`.
- `EDAOpenAiModelConfig` follows the stable `@effect/ai-openai` configuration type.
- `@effect/vitest` 4 declares Vitest 5. The workspace keeps Vitest 4.1 because
  `@cloudflare/vitest-pool-workers` does not support Vitest 5 yet, and allows that one peer range.

## 0.1.0-alpha.19

Align the development toolchain with Gia: TypeScript 7.0.2, Vite Plus 0.3.0, Oxlint 1.79.0 and Effect tsgo 0.39.0. Runtime APIs and persisted state are unchanged.

All notable changes to Effect Durable Agent will be documented in this file.

## 0.1.0-alpha.18

- Derive Cloudflare session RPC inputs and outputs from runtime Schemas. Encode command
  classes before transport and decode committed events, terminal events, and Map-based
  snapshots at caller boundaries. Existing structured-clone shapes and durable formats
  remain unchanged.

## 0.1.0-alpha.17

- Retain typed framework facts after store decode and register application events once
  through runtime `appEvents`. Foreign namespaces remain explicitly opaque.
- Select and narrow durable sinks and reducers from event Schemas; remove string-list
  subscriptions. Tool context exposes run/turn identity and typed journal queries.
- Preserve event-log encoding, checkpoint formats, sink names/cursors, and RPC/WebSocket
  envelopes, including retained legacy attempt events and excess payload fields.
- Enable explicit-any, non-null-assertion, and unsafe-type-assertion lint rules, with
  exact-file exemptions for existing assertions outside the event boundary.

## 0.1.0-alpha.15

- Add `state: "none"` for raw consumers: no sink projection hydration, folding,
  or retention, with the same ordered delivery and checkpoint protocol.
- Add explicit `interests: "*"` for durable and ephemeral consumers. Omission still
  selects all event types; existing projected sinks and persisted cursors are unchanged.

## 0.1.0-alpha.14

- Defer lagging sink projection hydration until delivery. Caught-up sinks reuse the
  session projection; historical reconstruction is serialized across sink workers.
- Replace unbounded sink subscriptions with coalesced durable heads and bounded
  best-effort ephemeral inboxes, preserving position order and durable checkpoints.
- Add a sink hydration span and report deferred initialization counts. Existing
  event and checkpoint storage formats remain compatible; no migration is required.

## 0.1.0-alpha.13

- Add two aggregate startup spans for session hydration and sink initialization,
  including session identity, checkpoint sequences, and projection reuse/replay counts.
- Retain these two summaries when startup completes within one clock tick.
  No event, checkpoint, delivery, or RPC contract changes.

## 0.1.0-alpha.10

- Add `runRequestOutcome` to core and Cloudflare trusted query surfaces. Reconcile
  ambiguous grant replies from retained committed facts as Unknown, Waiting,
  Invalidated, or Granted with its run identity and lifecycle outcome, including
  after reducer pruning and restart. No event or storage migration is required.

- Add durable deferred run authorization: persist one request before delivery,
  acknowledge successful handoff only, revalidate trusted grants, and consume
  permission atomically with run start. Stop/cancel/interrupt and recovery retain
  durable ownership across eviction. Cloudflare/celld alarms retry undelivered
  requests independently of active-work leases.
- Add the separate `GrantRun` API and `RunSchedulingWakeup` host capability;
  custom `RunScheduler` layers now provide outbound delivery. Immediate execution
  remains the default and retains its event/ID sequence.
- Rebuild pre-7 framework checkpoints from retained event history. Pause facts
  can identify a pending run request when interruption occurs before run start.

## 0.1.0-alpha.9

- Resolve each new run through the replaceable RunScheduler boundary, including queued work and restart recovery
- Default to immediate local execution without additional scheduling IDs, commands, events, or persisted state
- Accept an optional runSchedulerLayer in core, Cloudflare, celld, and testkit runtime composition
- Require a RunScheduler dependency when composing SessionState.Live directly

## 0.1.0-alpha.8

- Persist creation-time model selection and retain immutable run selections during restart recovery
- Replace the production `modelLayer` host option with `modelResolverLayer`, including a shared-transport OpenAI resolver
- Replace aggregate token consumption with per-provider/model buckets and preserve cache-write counters
- Record compaction usage, including known usage when the summary response contains no text
- Rebuild schema-5 framework checkpoints from retained history into schema 6; no conversation events are rewritten
- Keep model usage records compatible with Cloudflare RPC structured cloning

## 0.1.0-alpha.7

- Let Cloudflare hosts project application WebSocket protocols inside the EDA Durable Object while
  preserving hibernation, strict delivery acknowledgements, and cold recovery
- Persist Effect Schema-encoded application projection state in backward-compatible WebSocket
  attachments
- Preserve client-visible ACK ordering when projections suppress internal frames and compact long
  suppressed runs within Cloudflare attachment limits
- Validate projection selection before accepting sockets and preserve concrete application RPC
  methods on typed Durable Object stubs

## 0.1.0-alpha.6

- Reorganize the host boundary around a platform-neutral session runtime and focused Cloudflare
  controller, provider, RPC, and storage adapters
- Persist complete WebSocket delivery checkpoints as versioned Effect Schema attachments and
  restore strict ACK validation after Durable Object hibernation
- Keep idle WebSockets hibernation-eligible by using append-driven fanout, Cloudflare automatic
  ping responses, and no resident subscriber fiber or heartbeat timer
- Avoid cold-start recovery deadlocks by preparing the session runtime outside per-socket queues
  and flushing deferred durable catch-up after startup repair completes
- Make reducer host configuration heterogeneous and fully typed without `any`, with schema-backed
  codecs materialized by `EDAReducer.make`
- Make native tracing attribute export total for hostile maps and values

## 0.1.0-alpha.5

- Make WebSocket delivery hibernation-native and persist durable resume cursors
- Patch Effect-aware lint plugin loading in the npm publish workflow
- Publish only the core package; Cloudflare and celld remained at `0.1.0-alpha.4` pending their
  first authenticated maintainer publish

## 0.1.0-alpha.4

- Let applications project instruction and data messages from current reducer state into each model prompt
- Build prompts only after turn input selection is committed into durable reducer state
- Split the host-neutral runtime, Cloudflare Durable Objects host, and celld host into lockstep packages
- Add shared real-runtime conformance tests for workerd and celld, including RPC, persistence,
  hard-restart recovery, durable idempotency, WebSocket resume/ACK, and warm/cold session recreation
- Encode Effect Schema values explicitly at Durable Object RPC boundaries

## 0.1.0-alpha.3

- Publish the state-projected prompt context and WebSocket schema changes from the unpublished `0.1.0-alpha.2` package
- Pin prerelease publication to the npm `alpha` dist-tag

## 0.1.0-alpha.2

- Let applications project instruction and data messages from current reducer state into each model prompt
- Build prompts only after turn input selection is committed into durable reducer state
- Separate WebSocket protocol schemas into explicit domain and wire surfaces
- Add a typed domain-frame encoder, a host adapter for app-event narrowing, and transformation
  round-trip coverage

## 0.1.0-alpha.1

- Publish the first public prerelease as `effect-durable-agent`
- Ship compiled ESM and TypeScript declarations for every supported subpath
- Add isolated package-consumer validation for Cloudflare Workers
- Add MIT licensing and trusted-publishing release automation
