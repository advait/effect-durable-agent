# effect-durable-agent transcript history

Status: future
Runtime: EDAGiaAgent
Last reviewed: 2026-10-15

Audience: EDA runtime maintainers, host maintainers (Gia), and UI maintainers.

This is an accepted design that is not yet implemented. It covers EDA core and the Cloudflare
adapter, which celld reuses.

This document defines the **transcript**: the user-visible history of a session. It replaces the
earlier future-state `ui-projection.md` proposal (removed in the commit that added this document).
**Everything here except §2 and statements marked "today" is planned and not yet implemented.**
Today the record, window, cursor, mapping and store-port contracts of §14 step 1, and the core
interpreter, mapping executor and oracle of §14 step 2, ship as `core/transcript/` (subpath
`effect-durable-agent/transcript`); the in-memory store maintains and serves a transcript. The
delivery of §9 (step 4) also ships: active content, the subscription data source, socket update
schemas, chunking, the negotiation envelope, the publication fence and transcript-window sockets
in the Cloudflare adapter, over any `EDATranscriptStore` (see `docs/websocket-protocol.md`). The
Cloudflare adapter's storage (§14 step 3: tables, maintenance, freezing, runtime-free reads, the
builder, activation, the shared alarm and the log incarnation) ships too, and the session Durable
Object serves its transcript sockets from it; `docs/spec.md` §9, §14 and §16 describe it.
As each part ships, `docs/spec.md` and `docs/websocket-protocol.md` describe the current behavior
and this document keeps the design rationale.

Path abbreviations: `core/` = `packages/effect-durable-agent/src/`, `cf/` =
`packages/effect-durable-agent-cloudflare/src/`. Paths that start with `gia:` are in the
downstream Gia repository (`goguardian/gia`).

## 1. The design in one page

**Six concepts.** Nothing else is new.

| Concept | Meaning |
| --- | --- |
| **Event log** | The immutable source of truth. Unchanged. |
| **Transcript record** | A mutable, typed, rebuildable row with a stable id, derived from the log. Kinds: `user`, `assistant`, `tool`, `compaction`, plus a run group and a session-context record. Records are never semantically deleted. |
| **Transcript version** | Identifies the interpretation rules: EDA semantics plus the host's declarative mappings. A release carries the current definition and the previous one. A session serves one active version and, while it is being rebuilt, builds the current one beside it. |
| **Window** | A bounded slice of top-level records, plus what they depend on, read at one complete cut. |
| **Ordered update** | The only way a live client's window changes: one socket, server-ordered, complete cuts only. |
| **Active content** | The accumulated parts of the inference that is streaming now. Held in memory and published on the same ordered stream. |

**One rule for each audience.**

- **The model** reads execution and model state: the existing `ReducedState`. Compaction is an
  event that gives the model a new base. This code does not change, so prompt bytes do not change.
- **People** read transcript records. `ContextRebased` only adds a `compaction` marker record.
  Nothing is ever removed from the transcript (a cancelled queued message is hidden, not
  deleted).

**One read and one stream.**

- `readTranscriptWindow(selector, limits)` serves server-side rendering (SSR), CLIs, and stream
  refresh.
- A browser uses one ordered socket with three requests: `subscribe`, `page` (in either direction)
  and `retain`. Every update is a complete cut, pages included, so the client never merges two
  sources.

## 2. Problem

Compaction erases user-visible history.

- `ContextRebased` → `pruneForContextRebase` deletes records older than `retainedFromContextSeq`
  (`core/domain/reduced-state.ts:1301-1330, 1421-1488`).
- The snapshot transcript is derived from that pruned state (`core/services/session-query.ts:84-90`).
  Hosts that render it lose everything before the compaction point. Gia's live client also ignores
  `ContextRebased` (`gia:gia-cf/app/features/agent-session/eda-session-page-model.ts:1855`), so a
  reload disagrees with a live view.
- **Diagnosis:** a read-boundary defect. The UI is derived from state shaped for the model. The
  model's prompt is correct.

**Evidence from Gia production (read-only; logs folded offline with 0.1.0-alpha.21).**

| Session | Events | Transcript messages, pruned → unpruned |
| --- | --- | --- |
| `01a13b26-0c07-7aa3-8ea9-d201129c6838` | 1,110 | 2 → 131 |
| `01a0bff4-0d1a-7efd-ae3b-62e33a996c49` | 5,011 | 178 → 636 |

- Pruned and unpruned state give identical prompt bytes in both sessions. Two samples are not
  proof; §13 gate 1 replays every affected session.
- One whole-history document grows from 152 KB to 3.5 MB and from 1.75 MB to 6.4 MB. Tool payloads
  are 60–63% of that, and logs reach about 10k events. Paging is required.
- In the first session, a pending user message was pruned by compaction and re-committed with a new
  message id (`core/services/session-state.ts:1310-1320`). The transcript must treat both ids as one
  submission (§4).

## 3. Decisions

| # | Decision |
| --- | --- |
| 1 | Transcript interpretation is framework-owned (EDA) and app-extensible through **declarative** mappings (§6). |
| 2 | Canonical storage is EDA's, in the session Durable Object (DO). |
| 3 | Full tool bodies are kept. Large bodies are delivered by reference (§8). |
| 4 | The initial window is 100 top-level records. All budgets are tunable. |
| 5 | Paging bounds data; DOM virtualization is a host decision made by measurement. |
| 6 | Ephemeral events stay ephemeral. Streamed text that was never committed is not preserved across a crash. |
| 7 | Browser paging travels on the session socket. HTTP serves SSR and CLIs. |
| 8 | A broken transcript shows the last good view plus an "unavailable" tail and raises an alert. It is repaired by a new definition. |
| 9 | A cancelled queued message disappears from the transcript. Order is **consumption order**: a user record gets its position once, when the agent reads it. |
| 10 | **Forward-only releases; no rollback support.** Every definition change ships with a backfill that rebuilds every session before the next definition change, except a repair release (§7). |
| 11 | Model-projection fixes (pending-user pruning, one seq rule, tool budgets across a rebase) are separate, later changes (§15). |

## 4. Transcript records (EDA core owns meaning)

Top-level records count toward the window size. Dependencies count only against the byte budget.

| Kind | Identity | History position | Contract |
| --- | --- | --- | --- |
| `user` (top-level) | `user:<commandId>`. Every messageId is an **alias** | Assigned **once**, at first consumption. Pending records have none | One logical submission across all states: pending, paused, promoted, steering, consumed, cancelled. Steering and host-authenticated notices are **attributes**, not kinds. A replacement messageId joins the same record. A cancelled record stays in storage so its aliases resolve, but it is never rendered |
| `assistant` (top-level) | `assistant:<inferenceId>`. Imports use `import:<eventId>` | `InferenceStarted`; for an import, the import event | Ordered parts (text, reasoning, tool references). Content, lifecycle and usage update independently. Covers a partial commit followed by a failure in the same batch (`core/services/inference-runner.ts:447-455`) and an empty success. An import needs no start event (`core/domain/reduced-state.ts:1205`) |
| `compaction` (top-level) | `compaction:<compactionId>` | `ContextRebased` | Marker plus a summary reference; no summary text |
| `tool` (dependency) | `tool:<toolCallId>` | none; it belongs to its assistant | Typed `assistantId`; the assistant's parts order it. Full body and lifecycle. Last terminal wins, matching today's fold (`core/domain/reduced-state.ts:1047-1074`). A rejection is terminal at creation |
| run group (dependency) | stable recovery-group id: `run:<rootRunId>`. A run that reuses the group's command ids (a startup-recovery continuation) joins it | none | EDA computes status, timing and the continuation chain, so clients never interpret recovery events (`core/services/session-state.ts:1843-1854`) |
| session context (dependency, singleton) | — | none | Activity, model selection, token totals |

**General rules**

- Every record carries `updatedSeq`, the last event that contributed to it.
- Positions are assigned once and never change. A pending user record has none until it is
  consumed, and its identity does not change when it gets one.
- There is no `final` flag, no generic `parentId`, and no deletion, so there are no tombstones and
  no remove protocol.
- Legacy `TurnAttempt*` events go through the existing normalizer first
  (`core/domain/reduced-state.ts:805`; `core/types/events/durable.ts:973-990`).

## 5. Storage and maintenance

### 5.1 Tables

All tables are EDA-owned, live in the session store, and are keyed by version.

_As built:_ the adapter's tables use its `_eda_` prefix (`_eda_transcript_records`,
`_eda_transcript_aliases`, `_eda_transcript_meta`), and rows are keyed by a **slot**, one stored
instance of one version, rather than by the version string. A slot's role is `active`, `building`
or `retired` (discarded or replaced, awaiting bounded deletion), so a retired slot still being
deleted never collides with a later build of the same version. A slot also counts its consecutive
failed builder slices (`slice_failures`, forward migration 3), which bounds retries (§7).

| Table | Purpose |
| --- | --- |
| `transcript_records` | `(version, id)` → kind, position, `updatedSeq`, body. Indexes: position, `updatedSeq`, pending membership, and the typed dependency lookup |
| `transcript_aliases` | `(version, namespace, key)` → record id. Namespaces: messageId, command idempotency key, run id, run command id and tool part (`<inferenceId>/<providerPartId>`). An explicit indexed join, not an in-memory map. Aliases are only inserted, never moved |
| `transcript_meta` | One row per version present: `role` (`active` or `building`), `cursor` (last fully projected seq, committed with that version's rows), `status` (`ok` or `failed`), `failedAt`, `error` |

No whole-history collection is ever held in memory.

### 5.2 Maintenance: atomic and isolated

- **Where it runs.** EDA core produces a deterministic change plan for each append batch. The
  Cloudflare adapter executes it inside the existing append transaction
  (`cf/durable-object-store.ts:292-331`), as one batch-level hook outside today's per-event,
  EDA-only namespace guard (`cf/durable-object-store.ts:798-807`).
- **Isolation.** Projection writes run in an inner `storage.transactionSync`. If the projection
  fails, the adapter catches the error outside the inner transaction, records the failure, and the
  event append still commits. This uses the platform's nested-transaction API, not raw
  `SAVEPOINT`, which Cloudflare rejects. Gate 3 now runs on workerd and confirms it.
  - _As built:_ **celld rejects nested transactions** ("cannot start a transaction within a
    transaction"), leaving the enclosing transaction intact. So the adapter computes the plan,
    decodes the rows it reads and encodes the rows it will write (serialization and the size cap)
    before any write; every recoverable projection error is raised there, on every platform. It
    probes nesting once per storage object. Where nesting works, the writes also run inside the
    inner transaction, so even a failing write freezes only that version. Where it does not, the
    writes run in the append transaction, and a write that fails after planning succeeded fails the
    append atomically. That is a storage failure, which the "Limits" bullet below already allows.
- **Inside the boundary.** Plan computation, decoding and the SQL writes all run inside the inner
  boundary.
- **Freezing a failed version.** A recoverable failure marks **that version** `failed` at its
  unchanged cursor. Later appends commit their events but never touch that version's rows, aliases
  or cursor, so it stays an exact last-good cut.
  - A failed **active** version serves that cut with an "unavailable" tail under the degraded-read
    rule (§8), and raises an alert.
  - A failed **building** version leaves the active version healthy; the backfill reports it.
  - The only repair is a new definition, which rebuilds from the log (§7).
- **Memory.** In-memory caches are published only after the outer transaction commits.
- **Limits.** Disk or resource failure can still fail an append. The guarantee covers recoverable
  projection errors only.
- **In-memory store.** It mirrors the staging logic inside its `Ref.modify` critical section
  (`core/services/session-store.ts:263-288`). The Cloudflare adapter is the source of truth for
  transaction-rollback tests.

## 6. App extensions are declarative

Hosts register **data, not code**. EDA defines these operations:

- copy authenticated fields;
- associate by command or idempotency key;
- first-wins scalar (for example, an author);
- replace a list entry by id **or** ordinal (attachments and references);
- classify a user record (for example, a host-authenticated subagent notice).

Ownership and limits:

- **Validation happens at registration,** not during append.
- **EDA owns safe execution and the bounds;** the host owns which mappings exist.
- **An invalid decoration degrades only that decoration.** It never touches event admission.
  Transcript decoding does not go through `registerAppEvents`, whose decoder runs before the append
  (`core/services/session-store.ts:447-468`).
- **Arbitrary app code is out of scope.** If it is ever needed, it needs its own isolated execution
  boundary.
- Unregistered app namespaces stay opaque (`__foreign`) and the interpreter ignores them until a
  mapping names them.
- **Decorations may precede their target.** Hosts append decoration events before the command
  they describe (Gia submits `[author, attachments…, references…, SubmitMessage]` in one batch).
  The interpreter holds such operations in an internal, never-served
  `decorations:<association>:<key>` record (bounded per key), applies them in arrival order when
  the user record is created, and drains the buffer. Host-declared **user fallbacks** then copy
  session decorations onto a user record created without them, in declaration order, first present
  wins (Gia: the latest author, else the session owner); a record already classified when it is
  created (its classification preceded its command, as Gia's notices do) takes none.

The first host's coverage (Gia: author identity with its span-then-owner fallback, attachments,
references, subagent notice classification) must be demonstrated against its historical fixtures
before the host switches its reader.

## 7. Versions and upgrades (forward-only)

**Version identity.**

- `version = hash(EDA semantic revision, canonical encoding of the host's declarative mappings)`.
- The mapping hash changes automatically when mapping data changes.
- The EDA semantic revision is bumped deliberately. It covers the interpreter, the normalizer, the
  decoders and the mapping executor.
- A CI fingerprint over a fixed scenario corpus **guards** against forgetting the bump. It is not a
  proof.
- A **new durable event type** (or `schemaVersion`) requires a semantic-revision bump, and therefore
  a backfill, only if `current` gives it transcript meaning. Otherwise both carried interpreters
  ignore it and neither version id changes. The fingerprint corpus reflects this rule: an event the
  interpreter ignores leaves the fingerprint unchanged.
- Physical layout changes (indexes, columns) are ordinary forward migrations and do not rebuild
  history.

**Two definitions per release.** A release carries:

- `current`: what new sessions start with and every session converges to;
- `previous`: kept only so that a session not yet rebuilt keeps its rows maintained.

Ordinary releases carry both unchanged. The next definition change drops `previous`, and it may ship
only after the backfill of `current` is verified. The one exception is a **repair release** (below).
There is no rollback release to stay compatible with (decision 10).

Both carried interpreters must handle every event type the release can write, including deliberately
ignoring it where that is correct, because `previous` keeps processing appends until activation.

**Builds always read the log, never old rows.** A session on any older version can be rebuilt to
`current`, provided current code still decodes every historical event format. Dropping an old
_interpreter_ never drops an old event-schema reader (`core/types/events/durable.ts:1047`).

**Per-session convergence** (adapter).

1. The active version is maintained in every append transaction **if the release carries it and it
   is healthy**. Otherwise it is frozen: its rows, aliases and cursor are left exactly as they are.
2. A persisted building version that is not `current` is discarded.
3. If `active ≠ current`, a building version for `current` is built from the log in bounded slices.
   Each slice commits its rows and cursor together, so an eviction resumes from the cursor. A build
   of `current` that is `failed` is never retried; only a build that is `ok` but behind the head
   resumes. A failed build is repaired by a new definition.
4. When the building cursor equals the head, **re-read inside the activation transaction**, it
   becomes active. Connected sockets receive a window replacement (§9.4). The old version's rows are
   then deleted in bounded slices.
5. A brand-new log initializes `current` at seq 0, before its first append or read.

_As built:_ a version is maintained in the append exactly when it is carried, healthy and caught
up (its cursor equals the head before the batch). A building version is caught up only by
slices, and the slice that reaches the head activates it in the same transaction, so a build is
maintained in the append only in the degenerate case of a build created at the head (an empty
log). This one rule also repairs any active version found behind the head.

**The builder.** State lives in `transcript_meta`. Any request, alarm or constructor that sees
pending work runs or schedules one bounded slice through the storage service; it never constructs
the model runtime. Alarm scheduling is shared with runtime work, and neither owner deletes the
other's wakeup. Today `syncAlarm` deletes the alarm when no lease is held
(`cf/durable-object-keepalive.ts:190-192`); this must change. The in-memory keep-alive lease (`cf/durable-object-keepalive.ts:38-49, 190`) is not
relied on. _As built:_ `cf/durable-object-alarms.ts` keeps one durable wakeup row per owner and
sets the platform alarm to the earliest. The keep-alive is unchanged and uses its owner's view, so
its "delete when idle" deletes only its own wakeup, and the alarm handler knows after an eviction
whether it fired for the builder (storage-only slices) or the runtime: an owner's wakeup is
cleared only after its work succeeds, so a retry after a failure or an eviction is still attributed.

**Backfill (host, once per definition change).** After the release fully rolls out:

- enumerate every session, including sessions created during the cutover by old code. Each
  session's actual version is checked; none is assumed;
- wake each one through a storage-only entry point, so that builds that are `ok` but behind the
  head resume;
- read `transcript_meta` and record proof that each has `active = current` with status `ok`. The
  proof names the environment, the definition and the session incarnations it covers.

The proof is a release precondition for the next definition change (a host CI check). An unreachable
session is retried; it blocks only the next definition change.

**Repair release.** If `current` fails to build on some session, the fix is a new definition, and it
may ship without that backfill's proof. It carries `current` = the repair and `previous` = the
definition that failed on some sessions. Sessions discard the failed build and rebuild the repair
definition from the log. A session whose active version is no longer carried takes the safety net.

**Safety net for a missed session.** If a session wakes with an active version the release no
longer carries:

- that version is frozen (step 1), and new appends still commit to the log;
- reads serve its last-good cut with its real `throughSeq` and an "updating history" tail, under the
  degraded-read rule (§8). The cut is consistent but may be behind the log head;
- `current` builds at once; on activation clients receive one complete replacement;
- an alert is raised.

Stored rows use a backward-readable envelope. A row the current reader cannot decode makes the read
show "unavailable" until the rebuild finishes, never a wrong view.

**First introduction.** "No transcript" is the previous version: a host ships the writers, builder
and readers with its old reader still serving, runs the backfill, and only then switches its reader.

**Rollout rings (as built).** Maintenance is opt-in per session and off by default
(`EDASessionTranscriptOptions.enabled`, a boolean or a `(sessionId) => boolean` predicate,
evaluated once per object instance). A host widens concentric rings with a deterministic predicate,
for example `hash(sessionId) < percentage`, so a session's membership changes only when the host
widens or narrows the ring.

- A disabled session gets no transcript writes, builder slices or wakeups. Reads answer
  `Disabled`, the status is `Disabled` (read without writing), and transcript sockets are refused
  (409).
- Enabling a session takes the first-introduction path: a build from the log in bounded slices,
  then activation. A session enabled from its first event starts `current` at seq 0.
- Disabling a session leaves its rows exactly as they are; nothing maintains them.
- Re-enabling a session whose active version is behind the head never serves that version as
  current. Appends do not maintain it (its cursor is not the head), builder slices catch it up from
  its cursor, and until they reach the head reads report `UpdatingHistory` at its real cursor;
  connected sockets are re-read when it reaches the head.
  Narrowing a ring is therefore safe, though the host normally only widens.
- The backfill's proof covers only enabled sessions; the status's `Enabled`/`Disabled` tag says
  which.

**Bounded retries (as built).** A builder slice that throws while advancing its build target counts
against that target. After a configured number of consecutive failures (default 5) the build is
frozen as `failed`, which is never retried, and alerted. A failing builder alarm backs off
exponentially and stops scheduling itself after the same number of persisted consecutive failures,
so no session can loop on its alarm; only a later object start, status call or pending-work kick
tries again, bounded the same way.

### 7.1 Forward-only formats

For a host that deploys all Workers and DOs together with no rollback (Gia: a single
`wrangler deploy`), Cloudflare fences storage access for an obsolete DO instance once its
replacement starts
([Cloudflare: global uniqueness](https://developers.cloudflare.com/durable-objects/platform/known-issues/#global-uniqueness)).
Old code cannot read the session's storage after new code takes over, so
**DO-private formats ship their reader and writer together**: durable event types and
`schemaVersion`s, app event types, transcript definitions, and storage layout. Current code keeps
decoding every historical event format and stored row envelope.

Within a release line, the stored-row envelope carries a format number: a change that an older
reader of a known record kind could not decode (a new lifecycle or state tag, a new required field)
bumps it, so older readers show such a row as an `unknown` record instead of rejecting the window.

Compatibility remains wherever an **older peer can still be running**, including any event payload
that crosses that boundary: Worker↔DO calls during the cutover (both directions), browser tabs
opened before the deploy (§9.4), and CLIs or other raw-event consumers.

Forward-only **transcript definitions** are required by EDA's design: a rolled-back release would
find active versions it does not carry and deleted old rows. Shipping **event and storage formats**
reader-and-writer together is the host's deployment policy; a host that needs rollback for those
formats must stage them itself.

## 8. Reading a window

`readTranscriptWindow(selector, limits)` is a storage-level read. It never constructs the model
runtime (today's controller queries do: `cf/session-controller.ts:201, 294, 303`).

- **Selectors:** `latest(n)`, `page(before | after, cursor, n)`, `range(retained)`.
- **Cursor:** the total order `(anchorSeq, ordinal, recordId)`, scoped to incarnation and version.
- **Limits:** a count of top-level records and a byte budget.
- **Response,** one synchronous read cut:
  - `{ incarnation, version }`;
  - `throughSeq`, a fully projected commit boundary;
  - top-level records and their dependencies (tools, run groups, session context, pending users);
  - active content captured at the same publication boundary, if a runtime is live;
  - `hasOlder`, `hasNewer`;
  - readiness or degradation.
- **The degraded-read rule.** Every degraded read (a failed active version, §5.2; a missed session
  on the safety net, §7; a stored row the reader cannot decode, which serves an empty "unreadable"
  window, §7) returns at most its last-good cut and contains **nothing newer than its
  `throughSeq`**: no newer pending users, session context or live content. Readiness says why the
  cut is behind, and the window contract makes active content absent whenever readiness is not
  ready.
- **Large bodies** are sent as `bodyRef`: an immutable log location (event seq and field path)
  plus its byte length, resolvable through the log after old projection rows are deleted. Stored rows
  also hold large bodies as references, so no row approaches storage value limits.
  The host authorizes body fetches. Today the object's storage-only `readTranscriptBody` RPC resolves
  a reference, and `readTranscriptSummary` serves a compaction marker's summary text.
- **Log incarnation.** The adapter owns it (adopting a host's existing value, such as Gia's
  `gia.log_incarnation`). A wiped log invalidates every cursor.

## 9. Live delivery

### 9.1 Protocol

The existing socket transport stays: hello, acknowledgements, ping, flow control, hibernation and
per-socket sequencing (`cf/websocket/connection-manager.ts`, `core/websocket/delivery.ts`). A
**window data source** is added beneath the frame encoder. The current projection hook only maps
event frames and initializes from a runtime snapshot (`cf/websocket/projection.ts:43-60`), so this
is new work.

- **Client requests:** `subscribe(range, since)`, `page(before | after, cursor, n)`, `retain(range)`.
- **Server updates** carry identity, a delivery seq, `throughSeq`, coverage, and either replaced
  records or a complete window replacement.

The adapter serializes everything per subscriber:

1. Coalesce committed changes up to a complete boundary H.
2. For `page`, read the new page **and** the changes to the already-retained window at that same H,
   in one synchronous read.
3. Send one update. Only then widen the subscriber's coverage.

Paging, durable changes and live content reach the client through one ordered channel. There is no
cross-channel buffer.

**Atomicity** is a property of a logical update, not a frame.

- An update within the frame limit (256 KiB, `core/websocket/messages.ts:62`) is one frame.
- A larger update is chunked with a completion marker. The client applies it only when complete.
- Committed batches may be coalesced; a single batch is never split across visible states.
- **Two kinds of acknowledgement,** as separate fields of the existing ACK: transport receipts free
  flow-control capacity as today (16 frames in flight, `core/websocket/messages.ts:64`), and the
  client's _applied cut_ advances only when an update is complete.
- Staged bytes per update are bounded. A captured update is sent from its own snapshot. An
  interrupted transfer is discarded and replaced by a complete window.
- If a hibernated socket wakes without the volatile state it needs, it is closed, and the client
  reconnects with a replacement.

### 9.2 Active content

EDA core owns an accumulated value per active inference: its ordered parts, mapped to stable part
identities.

- Text and reasoning deltas carry only a `providerPartId` (`core/types/events/ephemeral.ts:15-30`);
  the producer associates them with the active inference. Tool deltas carry `toolCallId`
  (`:36-52`).
- Updates publish complete replacements of the changed active content, coalesced at a display
  cadence, on the same ordered stream. Append-only deltas are an optimization, added only if
  bandwidth measurements need them.
- EDA retires active content on every inference terminal: commit, partial plus failure, empty
  success, and recovery failure (`core/services/session-state.ts:1679-1689`). The same update carries
  the durable result.
- **Capture fence.** Append publication, ephemeral publication and window capture share one
  publication fence in the adapter. Durable commit precedes live publication
  (`core/services/session-state.ts:595-631`), so a stale live part never overrides a committed or
  failed inference.
- **Isolate loss** (decision 6) loses active content. Recovery records the inference as failed, and
  the record shows "interrupted" with its committed partial content.

### 9.3 SSR to client handoff

1. The host authorizes, calls `readTranscriptWindow(latest(100))`, and adds viewer data.
2. The page renders exactly that data.
3. After hydration, the client sends `subscribe(range = what SSR rendered, since = throughSeq)`.
4. The first update brings everything that changed since `throughSeq` across that range and its
   dependencies, plus current active content, as one complete cut.

Rendering is a pure function of the data plus serialized viewer inputs; times render from a
server-supplied "now". Keys are record ids, so a row streams in place from `InferenceStarted` to
commit. A pending user record moves into the conversation in one update when it is consumed.

### 9.4 Reconnect, long gaps and upgrades

- **Reconnect** re-reads the retained range at one current cut and replaces it in one update. The
  cost scales with the retained window, not the event gap. As built, that update is a patch relative
  to the cut the client last applied (it sent `since`), which leaves the client holding exactly the
  re-read range; only records changed since that cut travel.
- **Long gaps.** The viewport is preserved by anchor record and pixel offset; a "newer messages"
  boundary appears, and `page(after)` walks forward to the live tail.
- **A version change** uses the same replacement path, keeping the anchor where its id still exists.
  As built, the server answers a cursor from another version with a replacement of the latest
  records; the client keeps its viewport anchor by record id.
- **An incarnation change** invalidates the anchor and every cursor, chunk and body response. The
  client takes the new latest window.
- **Compatibility negotiation.** A small, stable envelope (protocol major, wire version,
  capabilities) is decoded before any version-specific schema, on the socket hello and on SSR data.
  Unknown optional fields are dropped. Every extensible kind or attribute has an explicit "unknown"
  representation that keeps its id and position and renders safely. A changed meaning is a major
  bump, and a major mismatch triggers one bounded reload that preserves the viewport and the unsent
  draft.

## 10. Ownership

| Concern | Owner |
| --- | --- |
| Event interpretation, aliases, lifecycle, cancellation, legacy normalization, run-group and active-content semantics, retirement, version identity, mapping executor | **EDA core**: pure typed interpretation over indexed lookups, producing deterministic change plans |
| Model context, execution, recovery | **EDA core**, unchanged. Never reads transcript records |
| Tables, transactions, indexes, per-session convergence, builder, old-version cleanup, runtime-free reads, publication fence, socket sequencing, chunking, flow control, log incarnation | **EDA Cloudflare adapter** (celld inherits it) |
| Declarative mapping data; authorization of every read, page, body and summary; viewer enrichment (names, pictures); SSR; the backfill job and its proof; UI presentation | **Host** |

## 11. Host integration: Gia

| Area | Change in Gia |
| --- | --- |
| Mappings | Author identity (span author, then session owner), attachments and references by id or ordinal, authenticated subagent notice classification |
| Server | Authorization on SSR, the socket upgrade, `page`, bodies and summaries; SSR via `latest(100)` plus profiles; the backfill job and proof check |
| Contract | `gia-api-contract` re-exports EDA's record and window schemas and the negotiation envelope; the CLI pages and exports through the same reader |
| Client | A window store replaces the hand-written event fold (`applyEdaEventsToViewState`) and its overlay rebasing; a compaction divider with "View summary"; paging with `retain`; the navigation cache (`session-snapshot-store.ts`) stores window data |
| Superseded | The never-enabled `agent_session_snapshots` read model, its `gia.session-snapshot` sink, and the snapshot document. The client-cache rollout waits until after the switch |

## 12. Evolution: each scenario has one path

| Scenario | Path |
| --- | --- |
| Cold DO (eviction, restart, first visit) | The runtime-free reader serves storage with no model runtime and no active content. Pending builder work resumes through the storage service |
| Hibernated socket wakes | Normal if its volatile state is present; otherwise close and reconnect with a replacement |
| Brand-new session | `current` initialized at seq 0 |
| Transcript definition change | One release (`current` = new, `previous` = old), then the backfill |
| New definition fails on some sessions | Repair release; failed builds are discarded and rebuilt from the log. Sessions still on the dropped definition take the safety net |
| Session missed by the backfill | Frozen last-good cut with "updating history", immediate build, one replacement, alert |
| Concurrent appends during a build | Active maintained in the append; the build catches up in slices; activation re-reads the head |
| Projection failure | Per-version freeze; fixed forward by a new definition |
| New durable event type or `schemaVersion` | Reader and writer ship together; the interpreter switch is exhaustive (`assertNever`). A semantic-revision bump (and a backfill) is needed only if `current` gives the event transcript meaning; otherwise both carried interpreters ignore it and neither version id changes (§7) |
| New host app event type | Reader and writer ship together; shown through a mapping, which is a definition change |
| New app reducer, an app reducer version bump, or a `ReducedState` checkpoint bump | Transcript unaffected; the runtime replays as today |
| New sink, sink renamed | Transcript unaffected. Sink bootstrap policy is out of scope for this work |
| Package bump with no semantic change | Same version, no build; the fingerprint guards |
| Old browser tab | Negotiation envelope, unknown representations, bounded reload |
| Session storage wiped | New incarnation; clients take the new latest window |
| Bad release | Fix forward |

## 13. Acceptance gates

1. **Prompt parity.** No model-projection code changes. A read-only replay hashes the host's prompt
   for every affected session before and after; the hashes are equal.
2. **Interpretation oracle.** Incremental plans equal a full rebuild for every batch partition,
   compared semantically with `updatedSeq` normalized. Cases include generated logs, recovery,
   interruption, promote/pause/cancel, steering, partial-then-failure, empty success, imports, legacy
   attempts, rejected tools, repeated terminals, mid-run compaction, the replacement message id, and
   host decorations on every ingress path.
3. **Storage.** Appends stay atomic. Injected projection failures leave events committed, the
   projection rolled back, the failure recorded, and memory unpublished. These tests run on workerd
   and in celld's host-conformance suite (`docs/testing.md`), not only against storage fakes. Building, resume after
   eviction, and activation at the head are tested.
4. **Window consistency at every publication cut.** `page` in either direction with concurrent
   changes never shows a mixed cut; no pending user is visible twice; active content retires on
   every terminal; more than 4,096 deltas (the raw replay cap,
   `core/services/live-event-bus.ts:21`) stream correctly; many small records and large bodies in
   one update are handled.
5. **Browser** (host). No hydration warnings; no key changes from start to commit; correct part
   order; viewport kept across long reconnects and scrollback.
6. **Performance** (measured): append hot-path cost, build cost at 10k events, warm and cold SSR,
   update bytes per streamed second, retained client memory.
7. **Evolution:** definition change end to end; missed session with concurrent appends; repair
   release; backfill inventory across a cutover; historical-format fixtures per `schemaVersion`;
   failure freezing; new reducer and checkpoint bumps leave the transcript identical and the runtime
   working; cold reads build no runtime; incarnation wipe; paging both ways with eviction; transport
   chunking beyond 16 frames and hibernation mid-update; old browsers; Worker↔DO calls in both
   directions during cutover; fingerprint failure on an unversioned change.
8. **Shadow before the switch** (host): uncompacted sessions match today's rendering; compacted
   sessions are a superset.

## 14. Implementation plan

1. **Contracts.** Record, window, cursor and update schemas; mapping operations; the store
   interface. Tests define the behavior.
2. **Core interpreter** and mapping executor, with the oracle (gate 2).
3. **Adapter storage:** tables, maintenance in the append transaction, freezing, the
   runtime-free reader, the builder and activation (gate 3).
4. **Adapter delivery:** the window subscription on the existing socket, active content and the
   capture fence (gate 4).
5. **EDA release.**
6. **Host:** bump, mappings, authorization, SSR, client window store, CLI, backfill and its proof.
7. **Rollout:** deploy with the old reader serving; backfill in widening rings; shadow; switch;
   delete superseded code.

## 15. Out of scope, later

Separate EDA changes, each with a production prompt-replay diff:

1. Stop pruning pending user messages (removes the replacement-id re-commit).
2. One seq rule for user messages across prune, planner and summary
   (`core/domain/reduced-state.ts:1483-1488`; `core/services/compaction.ts:711, 733` vs
   `core/domain/message-transcript.ts:296-299`).
3. Keep tool budgets across a mid-run rebase (`core/services/session-state.ts:2789-2814`).

Also out of scope: a Postgres mirror of the transcript, sink bootstrap policies, and arbitrary app
code in the interpreter.
