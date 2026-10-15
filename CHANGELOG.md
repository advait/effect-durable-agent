# Changelog

## 0.1.0-alpha.24

Transcript history, part 3: Durable Object storage. **Development release: do not adopt in
production.** In this release, maintenance and builds run for every session of every host;
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
