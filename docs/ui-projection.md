# effect-durable-agent UI projection proposal

Status: future
Runtime: EDAGiaAgent
Last reviewed: 2026-07-11

Audience: EDA/Gia runtime + UI maintainers

Current runtime exposes small Gia attachment and current-sandbox reducer projections in the EDA snapshot; this document describes the desired future full-transcript projection layer.

## 1. Purpose

Gia is moving from Cloudflare Think's chat-message storage model to `effect-durable-agent` (EDA), whose source of truth is a durable event log plus live-only ephemeral events. The browser UI still needs a simple, paginated, renderable transcript.

This proposal defines a future server-side UI projection layer for EDA. There is currently no generic `EDAUIItem`, `EDAUISnapshot`, `_eda_ui_items` table, projection API, or `useEDASessionUI` hook in this repository. The current host-facing read APIs are `EDARuntime.snapshot()` and `EDARuntime.messages()`, derived from canonical `ReducedState`; Gia additionally exposes attachment and current-sandbox state from its app reducers in the EDA snapshot.

The current web product server-renders EDAGiaAgent sessions from
`loadEDAGiaSnapshot(...)`, hydrates that serialized snapshot, then follows
`/api/eda-agent/sessions/:sessionId/events?afterSeq=<snapshot.state.lastSeq>`
with EDA WebSocket ACK flow control. Durable event frames advance the live
cursor and schedule a snapshot refresh; ephemeral text, reasoning, and tool
parameter frames fold into a live overlay. Heartbeats are connection-health
frames only and are not ACKed.

The proposed layer would:

- avoid replaying a whole session from `seq = 0` on every UI load;
- support latest-N transcript rendering and older-message pagination;
- prevent cross-page lifecycle tears for turns, tool calls, and custom app events;
- keep app-specific UI projections, such as current E2B sandbox identity, simple and pluggable;
- preserve EDA's durable/ephemeral semantics instead of forcing the browser to understand the raw event model.

## 2. EDA primer

EDA models one session as ordered facts.

### 2.1 Durable events

Durable events are persisted, sequenced, replayable facts. Examples:

- `CommandAdmitted`
- `CommandStarted`
- `UserMessageCommitted`
- `RunStarted`
- `TurnStarted`
- `InferenceStarted`
- `ToolCallCreated`
- `ToolCallStarted`
- `ToolCallCompleted`
- `AssistantMessageCommitted`
- `TurnCompleted`
- `CommandCompleted`

Every durable event receives an authoritative `seq`. Durable replay reconstructs `ReducedState`, query state, and recovery decisions.

### 2.2 Ephemeral events

Ephemeral events are live-only, ordered against the current durable head as `(seq, subSeq)`. Examples:

- `TextDelta`
- `ReasoningDelta`
- `ToolParamsStart`
- `ToolParamsDelta`
- `ToolParamsEnd`
- transient status events

Ephemeral events are not persisted as a replay log. Current reconnect catches up with durable replay plus the same-process raw active-turn ephemeral replay buffer, then follows future live events. A coalesced UI projection snapshot is proposed by this document but is not implemented.

### 2.3 `SessionState` authority

`SessionState` is the live-process write authority:

1. Commit durable events to `EDASessionStore`.
2. Fold newly committed durable events into authoritative in-memory `ReducedState`.
3. Publish positioned durable events to `LiveEventBus`.
4. Allocate positions for ephemerals and publish them to `LiveEventBus`.

Storage, live bus, subscribers, UI state, and read models are downstream from this authority.

### 2.4 Compaction is model-context compaction

EDA compaction rebases the future model prompt onto a summary plus retained tail. It does not mean the user-visible transcript must disappear. UI pagination must be able to show older conversation history even when model context has compacted.

## 3. Current EDA browser transport

The EDA browser client receives a server-rendered snapshot and follows the
session event stream with explicit durable positions and ACK flow control.

That model is excellent when the server owns one canonical `UIMessage[]`. EDA is more expressive: it has lifecycle facts, app-specific durable events, live-only deltas, tool lifecycle, E2B sandbox identity, recovery events, and context compaction. A raw `UIMessage[]` should become an output projection, not the source of truth.

## 4. Problem statement

The UI needs to render a long-running EDA session efficiently and correctly.

A naive UI would:

```text
connect -> replay durable events from seq 0 -> reduce in browser
```

This is unacceptable for long sessions and compactions.

A less naive UI would:

```text
connect -> get snapshot -> stream later events
```

This is better, but still not enough if the browser receives raw events. Raw durable event ranges are not safe pagination units.

A turn or tool call can span many events:

```text
seq 100 ToolCallCreated
seq 101 ToolCallStarted
seq 130 gia-agent/SandboxCreated
seq 200 ToolCallCompleted
```

If the UI pages by raw event sequence, it can render orphaned or partial state:

- an E2B sandbox replacement without the surrounding session state;
- tool started on one page and completed on another;
- a turn terminal without the turn's child cards;
- custom app lifecycle events with no UI parent.

The UI wants renderable entities, not event fragments.

## 5. Goals

1. **Server-side projection**: browser consumes UI projection state and patches, not raw EDA event facts.
2. **Latest-N rendering**: initial page renders the latest N transcript roots.
3. **Cursor pagination**: older roots load with stable cursors, not offsets.
4. **All-or-zero entity grouping**: a page contains a complete projected entity group or none of it.
5. **Live streaming UX**: text/reasoning/tool-params deltas still render smoothly.
6. **Custom app events**: apps add small projection reducers; they do not add custom pagination engines.
7. **Bounded payloads**: huge outputs/logs are summarized in transcript items and fetched lazily as details.
8. **Reconnect safety**: reconnect uses durable sequence checkpoints plus projection snapshots and idempotent patches.

## 6. Non-goals

- Do not make the browser an EDA event reducer.
- Do not guarantee raw event ranges contain complete lifecycles.
- Do not make ephemerals replayable history.
- Do not conflate model context compaction with UI transcript pagination.
- Do not add one-off UI tables or routes for each app-specific feature.

## 7. Core recommendation (future)

Add an EDA UI projection layer:

```text
Durable + ephemeral EDA events
        ↓
Server-side UI reducers
        ↓
Generic UI projection items
        ↓
Paginated snapshot + live UI patches
        ↓
React transcript rendering
```

The browser pages **projection roots**, not events.

If a page includes an assistant turn root, the server includes all projected children for that root: text block, reasoning block, tool call cards, status notices, etc. If the page does not include the root, it includes none of those children.

This is the central all-or-zero guarantee.

## 8. Projection model

### 8.1 UI item

A UI projection item is a stable renderable unit:

```ts
interface EDAUIItem {
  readonly itemId: string;
  readonly rootItemId: string;
  readonly parentItemId?: string;
  readonly kind: string;
  readonly anchorSeq: number;
  readonly updatedSeq: number;
  readonly ordinal: string;
  readonly payload: unknown;
}
```

Field meanings:

- `itemId`: stable identity, e.g. `eda:tool-call:<toolCallId>`.
- `rootItemId`: transcript page boundary, e.g. `eda:assistant-turn:<turnId>`.
- `parentItemId`: optional nesting parent.
- `kind`: render type, e.g. `user-message`, `assistant-turn`, `tool-call`.
- `anchorSeq`: first durable sequence that made this item visible.
- `updatedSeq`: latest durable sequence folded into this item.
- `ordinal`: deterministic order within a root, including child ordering.
- `payload`: bounded UI payload, versioned by kind.

### 8.2 Root items

Root items are pagination units. Candidate root kinds:

- `system-notice`
- `user-message`
- `assistant-turn`
- `compaction-notice`
- `recovery-notice`

A page selects roots by `anchorSeq`, then returns descendants for those roots.

### 8.3 Child items

Child items render inside a root. Candidate child kinds:

- `assistant-text`
- `assistant-reasoning`
- `tool-call`
- `tool-result`
- `trace-status`

A child must not be returned without its root in transcript pagination.

### 8.4 Detail items

Some data is too large for transcript cards:

- stdout/stderr bodies;
- full tool params/results;
- trace logs;
- long reasoning traces.

Projection items should contain summaries/tails/previews and a `hasMoreDetails` flag. Detail payloads should be fetched separately by `itemId`, with their own pagination if needed.

## 9. All-or-zero page guarantee

We do not guarantee this:

> All raw durable events for an entity are physically contained in one event page.

That is impossible for long-running entities.

We guarantee this instead:

> For a given `projectionThroughSeq`, all durable event effects up to that sequence have been folded into the projected item group. A transcript page includes all items for selected roots, or zero items for non-selected roots.

### 9.1 Invariants

1. **Page roots are selected first.**
   The query chooses root items using cursor pagination over `anchorSeq`.

2. **Descendants are fetched by root.**
   After roots are selected, the query fetches `WHERE root_item_id IN (...)`.

3. **No orphan child rendering.**
   A child item whose root is not selected is not returned in that page.

4. **Idempotent updates.**
   Items carry `updatedSeq`; clients ignore stale patches where `updatedSeq <= current.updatedSeq`.

5. **Complete as of a checkpoint, not necessarily terminal.**
   A running tool card is complete as of `projectionThroughSeq` even if it will later complete. The terminal event will arrive as a future patch or appear in a later page fetch.

6. **Large details are out-of-band.**
   A page may show a complete card summary while full logs remain lazy details.

### 9.2 Example

Raw events:

```text
100 TurnStarted(turn-1)
101 InferenceStarted(inference-1)
102 ToolCallCreated(tool-1)
103 ToolCallStarted(tool-1)
104 gia-agent/SandboxCreated(gia-session-1, e2b-sandbox-1)
105 ToolCallCompleted(tool-1)
106 AssistantMessageCommitted(turn-1)
107 TurnCompleted(turn-1)
```

Projection roots:

```text
root: eda:assistant-turn:turn-1
  child: eda:tool-call:tool-1
  child: eda:assistant-text:message-1
```

Pagination returns either:

```text
assistant-turn turn-1 + all descendants
```

or:

```text
none of turn-1's descendants
```

It never returns `eda:tool-call:tool-1` alone.

## 10. Custom app events without custom pagination

Custom app events participate by registering UI reducers that emit generic projection operations.

### 10.1 Generic reducer interface

```ts
interface EDAUIReducer<State = unknown> {
  readonly name: string;
  readonly interests: ReadonlyArray<string>; // namespace/type keys
  readonly initial: State;
  reduce(
    state: State,
    event: PositionedDurableEvent,
    ctx: EDAUIReducerContext,
  ): EDAUIReducerResult<State>;
}
```

Reducers are pure. They do not write SQL directly and do not send WebSocket messages directly.

### 10.2 Generic projection operations

Reducers emit operations such as:

```ts
type EDAUIProjectionOp =
  | { readonly _tag: "UpsertItem"; readonly item: EDAUIItem }
  | { readonly _tag: "PatchItem"; readonly itemId: string; readonly updatedSeq: number; readonly patch: unknown }
  | { readonly _tag: "AppendDetail"; readonly itemId: string; readonly detail: unknown };
```

The projection engine applies these operations to the generic UI projection store and live patch stream.

### 10.3 Framework reducers

EDA core registers reducers for framework event types:

- command status notices;
- user messages;
- assistant turn roots;
- assistant committed/partial messages;
- reasoning/text summaries;
- tool-call lifecycle;
- compaction/recovery notices.

### 10.4 Gia E2B sandbox reducer

Gia registers one reducer for the E2B `gia-agent/SandboxCreated` fact. It
records the latest authoritative logical sandbox id, E2B metadata, creation
time, and durable sequence. Command cards remain entirely
owned by the framework `ToolCall*` projection; their completed result already
contains the `runBash` exit, timing, output, retryability, and logical sandbox
metadata.

No app-specific pagination route is required. The page query still selects roots and descendants from the same generic projection model.

### 10.5 Custom event conventions

To keep custom app events simple, app event schemas should include:

1. a stable entity id, e.g. `executionKey`;
2. a parent correlation id when the event is attached to framework UI, e.g. `toolCallId`;
3. bounded summary fields for transcript cards;
4. references or lazy detail IDs for large payloads;
5. schema versioning under the app namespace.

If a custom event cannot resolve a parent, the reducer should either:

- create an app-owned root item, or
- emit a diagnostics-only item not shown in transcript pagination.

It should not create orphan transcript children.

## 11. Ephemeral text deltas

Ephemeral `TextDelta` events should render as live patches to an active draft, not as pageable history.

### 11.1 Live flow

1. Provider stream emits text.
2. EDA publishes `TextDelta` at `(currentDurableHead, subSeq)`.
3. UI live reducer appends the delta to an active assistant draft item.
4. Browser receives an idempotent patch such as:

```ts
{
  type: "patch",
  durableThroughSeq: 120,
  ops: [
    { op: "appendText", itemId: "eda:assistant-draft:<inferenceId>", delta: "hello" }
  ]
}
```

### 11.2 Active overlay

Streaming drafts live in an active overlay, not in older transcript pages:

```text
paged roots:
  latest committed transcript roots

active overlay:
  current assistant draft
  current reasoning draft
  current speculative tool params
  current running tool cards
```

If the user is at the bottom, the overlay renders like the latest assistant turn. If the user is paged far back, the UI can show a “new activity” affordance without mutating old pages.

### 11.3 Finalization

When the inference finishes, EDA commits durable final events such as:

- `AssistantMessageCommitted`
- `InferenceCompleted`
- `TurnCompleted`

The UI durable reducer then:

1. upserts or finalizes the committed assistant item;
2. reconciles it with the draft item;
3. retires the ephemeral draft.

After finalization, the assistant message is durable, pageable, and reload-safe.

### 11.4 Reconnect

Ephemeral deltas are not replayed one by one. On reconnect:

1. client sends durable checkpoint;
2. server returns durable/projection snapshot;
3. snapshot includes coalesced active draft text if still available;
4. live stream resumes with future patches.

If the Durable Object restarted and live-only text was lost, the server must not fabricate it. Recovery produces durable explanation events such as `AssistantPartialCommitted`, `InferenceFailed`, interruption-coded `TurnFailed` / `RunFailed`, and a replacement `RunStarted` for transparent continuation. Existing durable histories may still contain deprecated `TurnStopped` / `RunInterrupted` events, which the projection accepts during replay.

## 12. Public query and stream API

### 12.1 `getUiSnapshot`

Initial page load.

```ts
interface GetUiSnapshotInput {
  readonly latestRoots?: number; // default 50
}

interface EDAUISnapshot {
  readonly schemaVersion: 1;
  readonly sessionId: string;
  readonly projectionThroughSeq: number;
  readonly durableHeadSeq: number;
  readonly page: EDAUIPage;
  readonly active: EDAUIActiveOverlay;
}
```

The snapshot returns latest roots plus all descendants for those roots.

### 12.2 `getUiPage`

Older pagination.

```ts
interface GetUiPageInput {
  readonly beforeAnchorSeq?: number;
  readonly limit?: number; // default 50
}

interface EDAUIPage {
  readonly projectionThroughSeq: number;
  readonly roots: ReadonlyArray<EDAUIItem>;
  readonly itemsByRoot: Readonly<Record<string, ReadonlyArray<EDAUIItem>>>;
  readonly oldestAnchorSeq?: number;
  readonly newestAnchorSeq?: number;
  readonly hasMoreBefore: boolean;
}
```

`itemsByRoot[rootItemId]` includes the root and every selected descendant, sorted by `ordinal`.

### 12.3 `subscribeUi`

Reconnect-safe live stream.

```ts
interface SubscribeUiInput {
  readonly afterSeq: number;
  readonly knownProjectionSeq?: number;
}

type EDAUILiveMessage =
  | { readonly type: "snapshot"; readonly snapshot: EDAUISnapshot }
  | { readonly type: "patch"; readonly patch: EDAUIPatch }
  | { readonly type: "status"; readonly status: string; readonly reason?: string };
```

Patches are idempotent and ordered by durable/ephemeral position. A client that misses too much reconnects and asks for a new snapshot.

### 12.4 `getUiItemDetails`

Lazy details for large data.

```ts
interface GetUiItemDetailsInput {
  readonly itemId: string;
  readonly cursor?: string;
  readonly limit?: number;
}
```

Used for full stdout/stderr, full tool params/results, long traces, etc.

## 13. Storage shape

### 13.1 Generic projection table

A future durable projection store can be generic:

```sql
CREATE TABLE _eda_ui_items (
  item_id TEXT PRIMARY KEY,
  root_item_id TEXT NOT NULL,
  parent_item_id TEXT,
  kind TEXT NOT NULL,
  anchor_seq INTEGER NOT NULL,
  updated_seq INTEGER NOT NULL,
  ordinal TEXT NOT NULL,
  payload_json TEXT NOT NULL
);

CREATE INDEX _eda_ui_items_roots_idx
  ON _eda_ui_items(parent_item_id, anchor_seq);

CREATE INDEX _eda_ui_items_root_children_idx
  ON _eda_ui_items(root_item_id, ordinal);

CREATE INDEX _eda_ui_items_updated_seq_idx
  ON _eda_ui_items(updated_seq);
```

Root items have `parent_item_id IS NULL`.

### 13.2 Optional details table

Large details can be split out later:

```sql
CREATE TABLE _eda_ui_item_details (
  item_id TEXT NOT NULL,
  detail_seq INTEGER NOT NULL,
  detail_kind TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  PRIMARY KEY (item_id, detail_seq)
);
```

### 13.3 Projection cursor

The materialized projection needs a cursor:

```sql
CREATE TABLE _eda_ui_projection_metadata (
  id TEXT PRIMARY KEY,
  through_seq INTEGER NOT NULL,
  projection_version INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
```

Snapshot responses expose `projectionThroughSeq` so clients know exactly what the page represents.

## 14. Where projection runs

There are two related concerns.

### 14.1 Live projection

For active sessions, `SessionState` can maintain small in-memory UI projection state for current/active roots. This supports fast live snapshots and coalesced ephemeral reconnect state.

### 14.2 Durable materialized projection

For pagination and old roots, a cursor-backed projection runner should materialize `_eda_ui_items` by folding durable events through registered pure UI reducers.

This should follow EDA's sink principle:

- do not run arbitrary app side effects inline on the append path;
- use durable cursor semantics;
- expose `projectionThroughSeq`;
- use idempotent reducer operations.

Because UI reducers are pure and local, a later implementation may choose an optimized core path for framework-owned projections. The design should not require app-specific SQL or app-specific pagination logic.

## 15. Consistency model

The projection is a read model, so it has a checkpoint.

- `durableHeadSeq`: latest committed durable event.
- `projectionThroughSeq`: latest durable event folded into the UI projection page.

If projection lags durable head, the server has two safe options:

1. wait briefly for projection catch-up before answering; or
2. return the snapshot at `projectionThroughSeq` and start live patching from that sequence.

Clients must treat patches as idempotent by `itemId` + `updatedSeq`.

## 16. React hook shape

The UI hook should not expose EDA internals directly.

```ts
const session = useEDASessionUI({
  sessionId,
  latestRoots: 50,
});

session.roots;
session.itemsByRoot;
session.active;
session.loadOlder();
session.connectionStatus;
session.submitMessage(...);
session.stopTurn(...);
```

The hook owns:

- initial snapshot load;
- WebSocket subscription;
- cursor pagination;
- idempotent patch application;
- scroll anchoring for older-page insertion;
- new-activity indicator when user is not at bottom.

Rendering components consume projection items, not raw events.

## 17. Proposed implementation phases

### Phase 0: agreement and types

- Land this design document.
- Define `EDAUIItem`, `EDAUIPage`, `EDAUISnapshot`, `EDAUIPatch` schemas under `effect-durable-agent/`.
- Define pure reducer interfaces and operation types.

### Phase 1: minimal latest snapshot

- Implement `getUiSnapshot({ latestRoots })` for latest committed transcript roots.
- Include active overlay for current assistant draft/reasoning/tool params.
- Keep raw event replay out of the browser.

### Phase 2: generic projection reducers

- Add framework reducers for:
  - user message roots;
  - assistant turn roots;
  - assistant text/reasoning children;
  - tool-call children;
  - compaction/recovery notices.
- Add Gia reducer for authoritative E2B sandbox identity.
- Add tests proving root-child all-or-zero pages.

### Phase 3: older page query

- Implement `getUiPage({ beforeAnchorSeq, limit })`.
- Select roots first, then descendants.
- Preserve scroll position when prepending older pages.

### Phase 4: live patches

- Implement `subscribeUi({ afterSeq })`.
- Convert durable events and ephemerals into UI patches.
- Coalesce ephemeral text/reasoning/tool-param state for reconnect snapshots.

### Phase 5: materialized UI projection table

- Add `_eda_ui_items` and projection cursor.
- Add projection catch-up runner using the same reducer registry.
- Use table-backed pagination for long sessions.

### Phase 6: details and large payloads

- Add `getUiItemDetails`.
- Move large stdout/stderr/tool-result bodies out of transcript cards.
- Add UI affordances for expanding detail panels.

## 18. Test plan

### 18.1 Pure reducer tests

- Tool lifecycle folds into one tool card.
- Completed/failed/cancelled tool terminals update status idempotently.
- Unknown custom event leaves state unchanged.

### 18.2 Pagination tests

- Latest page includes selected roots and all descendants.
- Older page includes selected roots and all descendants.
- No child item is returned when its root is outside the page.
- Cursor pagination is stable when newer live events arrive.

### 18.3 Live patch tests

- Text deltas append to active draft.
- Reconnect snapshot includes coalesced draft when live state exists.
- `AssistantMessageCommitted` retires draft and creates committed item.
- Stale patches are ignored by `updatedSeq`.

### 18.4 Custom app event tests

- Gia `SandboxCreated` updates authoritative E2B sandbox identity idempotently.
- E2B sandbox identity does not create a separate transcript command card.
- Unknown Gia app events leave projected transcript state unchanged.

### 18.5 Recovery tests

- Restart during streaming does not fabricate lost ephemeral text.
- Durable `AssistantPartialCommitted` appears as a committed partial item.
- Open tool work receives durable cancellation/recovery explanation.

## 19. Open questions

1. Should `_eda_ui_items` be introduced immediately, or should v0 compute latest pages from existing message rows and live state first?
2. How much projection catch-up lag is acceptable before `getUiSnapshot` waits?
3. Should assistant turn be the root for all assistant-side process cards, or should final assistant messages be roots with sibling process groups?
4. What is the first detail API shape for tool-result stdout/stderr?
5. Should UI reducers live in `effect-durable-agent/services/` or a new `ui/` boundary?
6. How do projection versions migrate when reducer payload schemas change?

## 20. Summary

EDA's durable event log remains the source of truth. The browser should not page or reduce raw events.

The UI should page complete server-projected transcript roots. Each page selects roots first and returns all descendants for those roots, which prevents cross-page lifecycle tears. Custom app events participate by registering pure UI reducers that emit generic projection operations. Ephemeral text deltas remain live-only patches to active draft items and become durable/pageable only when EDA commits assistant message events.

This gives Gia a UI model that is efficient for long sessions, compatible with compaction, safe for custom lifecycle events, and still faithful to EDA's durable/ephemeral architecture.
