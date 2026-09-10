# EDA resumables and Gia subagents

Status: future
Runtime: EDAGiaAgent
Last reviewed: 2026-09-10

This is a proposal / docs-only design. It does not propose putting subagent
orchestration into EDA core. The design split is:

- **EDA core** owns generic session waiting/resume semantics and first-party token
  accounting.
- **Gia** owns subagent orchestration as an extension backed by Durable Objects.
- **Gia PostgreSQL** owns run admission, per-person capacity lanes, queue order,
  and leases. The capacity coordinator owns best-effort wakeups, outside the
  immediate-admission path.

The capacity design in sections 6.7 and 12.1 is agreed but not implemented. Gia's
currently deployed global coordinator path is the migration baseline, not the
target described here. The detailed implementation design lives in the Gia
parent repository at `gia-cf/docs/proposals/run-capacity-lanes.md`.

The reason for the split is simple: EDA is a per-session runtime. Subagents are a
cross-session, product-specific orchestration feature involving Gia session
metadata, ACLs, root workspace and sandbox semantics, child creation, capacity
lanes, queue fairness, deadlines, and operator UI. Those concerns should
not become framework core.

---

## 1. Position

Subagents should be implemented as a **Gia extension**, not as an EDA-native
workflow engine.

The EDA core change we do want is a small, reusable primitive: a session can enter
a durable **waiting** state on one or more **resumables**. A resumable is a
durable continuation handle that can be resolved later by an external event,
trusted route, or extension-owned Durable Object.

This same primitive should serve:

- a parent waiting on a subagent batch;
- a tool waiting on human approval for an integration call;
- a workflow waiting on an external webhook;
- any future extension that needs hibernation without a live fiber.

Subagent manifests, child state, join predicates, deadlines, and result
aggregation belong outside EDA in a Gia `SubagentOrchestrator` Durable Object.
Run-capacity leases belong to Gia PostgreSQL and are shared by root sessions and
subagents through the same acquisition interface.

---

## 2. Goals and Non-Goals

### Goals

- Allow a Gia session to fan work out to child sessions and resume durably when a
  declared join condition is satisfied.
- Keep the parent session hibernated while waiting. No live promise, fiber, Web
  Socket, or DO process is required for correctness.
- Support nested subagents through lineage and policy.
- Make waiting visible as first-class session state, not as "idle plus hidden app
  state."
- Keep subagent orchestration outside EDA core while still using EDA's durable
  command/event model for parent pause and resume.
- Track token usage as a framework-level fact in tokens, not dollars.

### Non-goals

- Do not build a general DAG/workflow engine in EDA.
- Do not make EDA core understand subagent manifests or join modes.
- Do not add a model-facing `subagents` skill before `spawnSubagents` exists.
- Do not guarantee exact budget cutoffs under concurrent in-flight model calls.
  Slight overspend from cancel latency is acceptable.
- Do not solve workspace conflict prevention. Subagents share root project state;
  v1 relies on attribution and operator visibility, not locks.

---

## 3. Terminology

| Term | Meaning |
| --- | --- |
| **Resumable** | A durable continuation handle opened by a tool, command, or extension. It represents external work whose result may later resume the session. |
| **Waiting** | Scheduler-visible session state: the session has no active run, but one or more resumables are open. |
| **Resolver** | The trusted actor that resolves, cancels, or expires a resumable. For subagents this is the Gia subagent orchestrator. For approvals this may be an approval route. |
| **Subagent batch** | Gia extension concept: a manifest-defined group of child sessions plus join policy. |
| **Subagent orchestrator** | Gia Durable Object that owns subagent manifest state, child lifecycle, deadlines, joins, and resume calls. |
| **Root session** | Top-level Gia session whose workspace, sandbox, and token budget are shared by a subagent tree. |
| **Parent session** | Session that spawned a child or batch. A child can itself be a parent. |
| **Capacity owner** | Persisted owning person of a root session, inherited by every descendant for run admission. |
| **Capacity lane** | A concurrency constraint with a scope, applicable run kinds, and an effective limit. A run acquires all applicable lanes atomically. |
| **Run lease** | Expiring accounting for an active run across its lane memberships; expiry does not prove execution stopped. |
| **Capacity coordinator** | Gia Durable Object that reads PostgreSQL and wakes sessions after a hint or alarm. It does not own capacity state or authorize a run. |

Use **waiting** for the scheduler/UI state. Use **resumable** for the durable
continuation object. A session is "waiting on resumables," not itself
"resumable."

---

## 4. High-Level Architecture

```text
Parent EDA session DO
  - model calls Gia spawnSubagents tool
  - tool atomically commits ResumableOpened + SubagentBatchRequested
  - batch/resumable identities stay stable across retries of that tool call
  - run completes; session state becomes waiting

Parent durable outbox sink
  - sees SubagentBatchRequested
  - idempotently enqueues manifest with SubagentOrchestrator DO
  - does not submit commands and does not own joins

Gia SubagentOrchestrator DO
  - owns manifest state in DO SQLite
  - owns child launch tickets, deadlines, token sub-budgets, and join predicates
  - creates child Gia EDA sessions through trusted server contract
  - receives child result/progress signals
  - resolves the parent resumable when join criteria hold

Child Gia EDA session DO
  - ordinary EDA session with lineage
  - atomically acquires run capacity directly in PostgreSQL
  - starts immediately when acquired; otherwise retains its queued request
  - shares root workspace and sandbox
  - writes result artifacts by reference
  - reports terminal/progress facts to orchestrator

Gia PostgreSQL
  - owns per-person root/subagent lanes, overrides, queue order, and run leases
  - receives capacity progress/terminal sink facts directly

Gia RunCapacityCoordinator DO
  - accepts minimal poke() hints after database commits
  - reads PostgreSQL for eligible requests and deadlines
  - wakes sessions to retry acquisition; never grants from local state
  - performs a best-effort recovery sweep every ten minutes
```

The parent log should contain durable intent and durable resume facts. It should
not need every child progress event to evaluate the join. The orchestrator owns
that operational state.

---

## 5. EDA Core: Resumables

EDA core should add a generic resumable event family and reducer. These events are
framework events because dispatch policy and session state depend on them.

### 5.1 Core Events

```ts
ResumableOpened = {
  resumableId: ResumableId
  kind: string                // e.g. "gia.subagent.join", "approval.required"
  source: {
    commandId: CommandId
    runId?: RunId
    turnId?: TurnId
    inferenceId?: InferenceId
    toolCallId?: ToolCallId
  }
  resumePolicy: "auto" | "manual"
  interruptPolicy: "queue" | "allow-steer" | "allow-interrupt"
  deadlineMs?: number
  display?: {
    title: string
    summary?: string
  }
}

ResumableResolved = {
  resumableId: ResumableId
  resultRef?: string
  result?: unknown            // small only; large results by reference
}

ResumableCancelled = {
  resumableId: ResumableId
  reason: string
}

ResumableExpired = {
  resumableId: ResumableId
}
```

The reducer tracks all open and terminal resumables. A session is **waiting** when
there is no active command/run and at least one resumable is open.

### 5.2 Resumables Are Not Tool Calls

A tool call should not remain open while waiting on external work. The tool should
open a resumable, commit the durable fact, and then complete normally with a small
handle:

```ts
const handle = yield* ctx.openResumable({
  kind: "approval.required",
  source: { toolCallId: ctx.toolCallId },
  resumePolicy: "auto",
  interruptPolicy: "allow-interrupt",
  display: {
    title: "Approval required",
    summary: "Approve Salesforce write",
  },
});

return {
  status: "waiting",
  resumableId: handle.resumableId,
};
```

This keeps recovery simple. Tool lifecycle remains tool lifecycle. Resumable
lifecycle is a separate continuation contract.

### 5.3 Multiple Resumables

EDA should allow multiple open resumables. Core semantics stay intentionally
small:

- The session remains waiting while any resumable is open.
- A resolved resumable with `resumePolicy: "auto"` may trigger a resume command.
- The resumed model receives a compact context projection of resolved and still
  open resumables.
- Complex joins such as all/k-of-n/quorum are not core. They are represented by
  one app-level resumable whose resolver owns the join logic.

Examples:

- One approval tool call opens one resumable.
- Three independent approval tool calls open three resumables.
- One subagent batch opens one resumable, even if the batch has 200 children.
  The Gia orchestrator owns the 200-child state and resolves the single parent
  resumable when its join predicate holds.

### 5.4 Dispatch Semantics

EDA dispatch policy should distinguish three durable scheduler states:

| State | Meaning |
| --- | --- |
| `running` | A command/run/turn is active. |
| `waiting` | No active run, but at least one resumable is open. |
| `idle` | No active run and no open resumables. |

Command behavior while waiting:

| Command | Waiting behavior |
| --- | --- |
| `SubmitMessage{queue}` | Allowed. It queues user work without resolving existing resumables. It should not start a run until policy says queued work may break waiting. |
| `SubmitMessage{steer}` | Allowed only if the relevant resumables permit steering. Conservative default: record it as queued context but do not resume. |
| `SubmitMessage{interrupt}` | Allowed. Default behavior: cancel or abandon open resumables according to policy, then start a replacement run. |
| `StopTurn` | Cancels or abandons open resumables owned by the waiting session. For subagents this requests downstream cancellation. For approvals this closes pending approval requests. |

The exact defaults should be conservative:

- queue does not resume;
- resolving an `auto` resumable resumes;
- interrupt explicitly resumes/replaces;
- stop explicitly cancels waiting work.

### 5.5 Required Core API Surface

EDA needs a small API surface, not subagent-specific framework code:

- `EDAToolExecutionContext.openResumable(...)`
- an atomic durable append that opens a resumable together with extension-owned
  request events, using stable tool-call retry identities; EDA need not interpret
  the extension event payload
- trusted runtime/DO API to resolve/cancel/expire a resumable
- reducer state for open/terminal resumables
- dispatch policy support for `waiting`
- prompt/UI projection hooks for resumable state
- tests proving recovery after every prefix around open, resolve, cancel, and
  resume

Sinks should not gain a general ability to submit commands as a side effect. A
sink may forward durable facts to an extension-owned orchestrator. Resume should
come back through the trusted resumable resolver API.

---

## 6. Gia Extension: Subagents

Subagents are implemented as a Gia extension with a model-visible
`spawnSubagents` tool and a `SubagentOrchestrator` Durable Object.

### 6.1 Spawn Tool Contract

`spawnSubagents` runs inside a real Gia EDA session. It is visible only when the
session principal has the `agent.spawn_subagent` capability.

The tool:

1. Validates authorization and manifest schema.
2. Assigns stable `batchId` and `resumableId` identities for this parent/tool call.
   Re-executing the same tool call reuses the existing identities and manifest.
3. Commits one EDA resumable with `kind: "gia.subagent.join"` and the Gia app event
   `SubagentBatchRequested` in one atomic durable batch. The request carries the
   manifest and the same `resumableId`.
4. Returns a small handle to the model after that durable commit.
5. Completes normally. The parent run then completes and the session becomes
   waiting.

There must be no committed prefix containing this resumable without its batch
request. A crash or cancellation before commit leaves neither event; one after
commit leaves both for outbox recovery. A lost tool-result reply must not create
a second batch/resumable. Separate `openResumable` and app-event commits are not
a valid implementation of this spawn contract.

The tool is the only way to create a parent-joined subagent batch. The CLI may
observe, wait, and abort, but it should not originate parent-joined work from
outside a live harness session.

### 6.2 Manifest Shape

The manifest belongs to Gia, not EDA core.

```ts
SubagentBatchManifest = {
  batchId: string
  children: ReadonlyArray<{
    name: string
    prompt: UserMessageContent
    agentType?: string
    model?: ModelSelection
    inputRefs?: ReadonlyArray<WorkspaceRef>
    timeoutMs: number
    tokenBudget?: number
    maxDepth?: number
  }>
  join:
    | { mode: "all" }
    | { mode: "any-terminal" }
    | { mode: "any-success" }
    | { mode: "k-terminal"; k: number }
    | { mode: "k-success"; k: number }
    | { mode: "deadline" }
  parallelism: number
  batchDeadlineMs: number
  staleAfterMs?: number
  tokenBudget?: number
  resultMode: "summary" | "summary+artifacts"
  onEarlyJoin?: "cancel-rest" | "let-rest-finish"
  onChildFailure: "continue" | "fail-fast"
}
```

Results should be returned by reference. Child reports write to the shared root
workspace or another durable artifact store and include compact refs in the join
report.

### 6.3 Durable Outbox to Orchestrator

The parent session should not synchronously call the orchestrator as the only
launch path. A crash after opening the resumable but before the orchestrator call
would lose the batch.

Instead:

- `SubagentBatchRequested` and its `ResumableOpened` are atomically durable in the
  parent log, with stable identities across tool retries.
- A Gia durable sink watches for that event and sends an idempotent enqueue to
  the `SubagentOrchestrator`.
- The sink cursor advances only after the orchestrator acknowledges durable
  receipt.

This is a durable outbox pattern. The sink is not evaluating joins and is not
submitting commands. It only bridges parent durable intent to the orchestrator.

### 6.4 Orchestrator Responsibilities

The `SubagentOrchestrator` DO owns:

- manifest state and child ticket state in DO SQLite;
- idempotent enqueue keyed by `(parentSessionId, batchId)`;
- batch-local launch eligibility and manifest parallelism;
- child launch through trusted Gia session-create APIs;
- per-ticket admission timeout;
- per-child timeout;
- batch deadline;
- stale/progress watchdog;
- token sub-budget policy;
- join evaluation;
- cancellation and late-result reconciliation;
- final parent resumable resolution.

The orchestrator should be the operational source of truth for live subagent
batch status. The parent EDA log remains the durable source of truth for "the
parent requested this batch" and "the parent was resumed with this report."
PostgreSQL is the source of truth for run capacity and queue position. A child
ticket is not a capacity permit, and batch parallelism cannot increase the
person's run limit. The orchestrator submits eligible child work through the
ordinary session contract; the child acquires its own run lease.

### 6.5 Child Reporting

Children are ordinary Gia EDA sessions with lineage. They report results to the
orchestrator, not directly to the parent reducer.

Recommended path:

1. Child writes result artifacts by reference.
2. Child emits a compact Gia app event such as `SubagentChildResultReady`.
3. A child durable sink forwards that fact to the orchestrator with an idempotency
   key.
4. The orchestrator folds the child result into batch state.

The orchestrator also reconciles by deadline. If a child never reports, the
orchestrator marks the batch ticket timed out, requests child cancellation, and
evaluates the join. A batch timeout is not proof that execution stopped. Actual
terminal sink facts or lease expiry release PostgreSQL capacity; the
orchestrator must not invent a run terminal outcome to free a permit.

### 6.6 Resolving the Parent

When the join predicate is satisfied, the orchestrator calls a trusted parent
session route that appends, in one durable batch:

- Gia app event `SubagentBatchResolved { batchId, reportRef, summary }`
- EDA core event `ResumableResolved { resumableId, resultRef }`

EDA then applies normal resumable dispatch rules. If the resumable has
`resumePolicy: "auto"`, the parent resumes with a prompt projection of the join
report.

The idempotency key should be deterministic:

```text
subagent-resolve:<parentSessionId>:<batchId>:<resumableId>
```

Late child reports after resolution are accepted by the orchestrator for
observability, but they do not cause a second parent resume.

### 6.7 Direct Admission and Composable Capacity Lanes

| Lane | Scope | Default concurrent run leases |
| --- | --- | ---: |
| `root_runs` | Per capacity owner, top-level sessions | 5 |
| `subagent_runs` | Per capacity owner, descendants at every depth | 10 |

All a person's batches and nested descendants share the subagent lane. These
defaults and optional `(user ID, lane key)` overrides live in PostgreSQL. A
missing override uses the default. The two initial lanes replace the existing
global-only target; a future global lane is disabled until configured.

Use lane definitions (scope, applicability, default limit), per-user overrides,
concrete lane instances, and durable request-to-lane memberships. A request has
one lease generation/deadline across its memberships. Adding a global constraint
later means acquiring the personal lane and the global lane together through the
same interface. Acquisition is all-or-none, with deterministic lane lock order.
Unrelated users and the same user's root/subagent lanes do not share a lock when
no global constraint is enabled.

The trusted session admission path performs one atomic PostgreSQL function call
using the existing durable request ID. It derives lane membership from persisted
lineage and capacity owner, resolves current limits, and reserves immediately
when eligible. It starts the run without awaiting a coordinator notification,
acknowledgment, or alarm write. Acquisition remains outside the EDA control gate
so stop/delete are not blocked by database latency. A lost response retries the
same request; expired reservations and ambiguous grants reconcile against EDA
outcomes rather than creating another user command.

FIFO is per personal lane. With a future global constraint, admit the oldest
eligible request across personal lanes; a user blocked by their own limit cannot
block another eligible user. Limit reductions preserve current runs and constrain
subsequent acquisition; increases issue a best-effort poke. Adding a new lane
requires explicit treatment of existing active and queued memberships.

A waiting resumable holds no active run. When a parent run completes, its
terminal sink releases its lease; when the resumable resolves, a new run request
reacquires the parent's lane. Nested waiting parents use this same rule so the
ten-slot subagent lane cannot fill with parents holding permits while their
children wait for capacity.

The capacity sink writes directly to PostgreSQL. Retain the five-minute soft TTL
and renewal from selected durable agent-loop events only, using original event
timestamps and sequence deduplication. Ordinary progress updates must not take a
global admission lock. Historical events for runs without capacity membership
are skipped efficiently and cannot create fresh leases. Expiry reclaims
accounting without proving physical completion; later genuine progress may
register a new generation and temporarily exceed a configured limit.

After a relevant commit, send a best-effort, scheduling-data-free `poke()` to the
global capacity coordinator. Standard tracing context is allowed; lease state,
limits, owners, and queue contents are not payloads. The coordinator coalesces
hints, queries PostgreSQL, and wakes sessions to retry acquisition. Its deadline
alarms and independent ten-minute best-effort sweep also read PostgreSQL. Pokes
must not indefinitely postpone the periodic sweep, and the sweep remains
scheduled while the queue is empty. No sweep or poke renews a lease.

We accept the crash window after a database commit and before a poke. Subsequent
traffic is expected to poke the coordinator frequently; the periodic sweep is
the quiet-period fallback, not a hard latency guarantee. There is no durable
capacity-notification outbox or coordinator-before-commit requirement. This
exception does not weaken the durable batch-manifest, child-result, or
parent-resumable handoffs in sections 6.3–6.6.

---

## 7. Lineage and Metadata

Gia should add lineage to agent metadata. Minimum fields:

- `rootSessionId`
- `parentSessionId`
- `capacityOwnerUserId`
- `sessionKind`: `"root" | "subagent"`

Recommended fields:

- `depth`
- `subagentName`
- `subagentBatchId`

For root sessions:

```text
sessionId = rootSessionId
parentSessionId = null
sessionKind = root
capacityOwnerUserId = owning person
depth = 0
```

For child sessions:

```text
rootSessionId = parent.rootSessionId
parentSessionId = parent.sessionId
sessionKind = subagent
capacityOwnerUserId = parent.capacityOwnerUserId
depth = parent.depth + 1
```

Lineage must be server-derived. Callers may request a parent, child name, and
prompt, but they must not supply authoritative `rootSessionId`, `parentSessionId`,
`sessionKind`, or `capacityOwnerUserId`. Later messages or delegated credentials
must not silently reattribute an existing run to a different person's quota.

### 7.1 Workspace and Sandbox Ownership

Subagents share the root workspace and root sandbox:

```text
workspace owner = rootSessionId
sandbox id      = gia-${rootSessionId}
```

The current EDA Gia tool context only carries the current `sessionId`. Subagent
work requires Gia tool inputs and opaque session-token credentials to carry both
current and root identity so tools can attribute work to the child while routing
workspace and sandbox access to the root.

### 7.2 Race Contract

Subagents share mutable project state. v1 should not add filesystem locks,
sandbox locks, patch reservations, or automatic merge conflict handling.

The product contract is:

- concurrent writes are allowed;
- last writer wins for whole-file workspace writes unless a tool implements a
  stronger operation;
- logs and artifacts must identify the writing session;
- users can inspect which child changed what.

---

## 8. Authorization and Capability

Add a Gia action:

```ts
agent.spawn_subagent
```

Use two-layer gating:

- **Visibility:** only sessions with the action see `spawnSubagents`.
- **Policy:** the tool re-checks authorization at execution time.

The action should be allowed only for an in-harness session principal. A browser
`web_cookie` or bare CLI OAuth principal may inspect or abort authorized batches,
but cannot create parent-joined work because there is no live parent run to attach
the resumable to.

Recursion should be allowed only under explicit policy:

- child sessions inherit user identity;
- child sessions inherit or receive a delegated capability snapshot;
- `maxDepth` is tighten-only;
- a hard system ceiling caps depth;
- per-child tool access can be reduced.

Prefer delegated capability snapshots over "same owner email gets everything"
when implementing sensitive integrations. That avoids turning one compromised
child prompt into unbounded recursive authority.

---

## 9. Token Accounting and Budgets

Token usage should become first-party EDA state. EDA already sees provider usage
on inference completion; promote that into reducer state:

- tokens by command/run/turn/inference;
- rolling session total;
- optional root/tree aggregate exposed to Gia;
- no dollar calculations in EDA.

EDA should track tokens, not cost. Rate cards change and belong outside the
framework.

Budget enforcement for subagents is Gia policy layered on top:

- root/tree token budget;
- batch token budget;
- child token sub-budget;
- model/provider max-token settings where available;
- pre-admission checks before launching more children;
- cancellation when observed usage exceeds budget.

Exact cutoff is not guaranteed. Concurrent in-flight children may overspend
slightly before cancellation propagates. The contract should be "bounded and
observable," not "mathematically exact."

---

## 10. CLI and UI

The CLI is a control plane, not the primary spawn transport.

Allowed:

```bash
gia subagent list --session <parent-session-id>
gia subagent get --batch <batch-id>
gia subagent wait --batch <batch-id>
gia subagent abort --batch <batch-id>
gia sessions wait --session <any-authorized-session>
```

Not allowed:

```bash
gia subagent run --manifest ./batch.json
```

At least not for parent-joined batches. Creating parent-joined work should require
a live session tool call that opens a resumable.

UI should show:

- session state: running, waiting, idle;
- waiting reason cards from open resumables;
- subagent batch status from the orchestrator;
- child rows grouped under the root or parent session;
- deadlines, queue position, token usage, failures, and result refs.

---

## 11. Liveness and Recovery

Batch and resumable correctness relies on durable handoffs. Capacity wakeup hints
have the explicitly weaker recovery contract in section 6.7:

| Boundary | Durability mechanism |
| --- | --- |
| Tool opens parent wait and requests batch | atomic EDA `ResumableOpened` + Gia `SubagentBatchRequested` batch, with stable retry identities |
| Parent intent reaches orchestrator | durable outbox sink with cursor |
| Orchestrator stores manifest | DO SQLite idempotent enqueue |
| Child sessions are launched | orchestrator ticket state and trusted session-create idempotency |
| Run capacity acquired or queued | atomic PostgreSQL request, memberships, and reservation |
| Queued capacity work is woken | best-effort data-minimal poke, database-derived deadline alarm, or ten-minute recovery sweep |
| Child reports result | child durable event plus durable sink to orchestrator |
| Child never reports | orchestrator deadline alarm and reconciliation |
| Parent resumes | trusted resolver appends `SubagentBatchResolved` + `ResumableResolved` |

No wait depends on a live fiber.

Important liveness rules:

- Every queued child ticket has an admission timeout.
- Every launched child has a wall-clock timeout.
- Every batch has a deadline.
- Every run lease has a TTL and PostgreSQL-backed reconciliation path; batch
  deadlines do not fabricate execution terminal outcomes.
- Every resolver call is idempotent.
- Resolving a resumable twice is harmless and produces one parent resume.

---

## 12. Observability

All subagent logs and spans should include:

- `sessionId`
- `rootSessionId`
- `parentSessionId`
- `sessionKind`
- `batchId`
- `childSessionId`
- `resumableId`
- `ticketId`
- `capacityOwnerUserId`
- `requestId`
- `runId`
- `laneKey`
- `leaseGeneration`
- `queueDepth`
- `tokenInput`
- `tokenOutput`
- `tokenTotal`

Operators should be able to answer:

- why is this session waiting?
- what external actor can resolve it?
- what child sessions exist under this root?
- which children are queued, running, terminal, timed out, or cancelled?
- what budget has been consumed?
- what event/resolver caused the parent to resume?
- is a run waiting for actual lane capacity, a database lock, a wakeup, or EDA
  startup?

Only attach fields applicable to the span's scope. IDs belong in authorized
traces/logs, not high-cardinality metric labels; prompts and tool payloads do not
belong in capacity telemetry.

### 12.1 Coordinator and Admission Tracing

Both `SubagentOrchestrator` and `RunCapacityCoordinator` must install the Gia
Effect tracer on their actual ManagedRuntime, covering RPC, alarm, cold-start,
and background-drain execution. Named effects alone are insufficient. Verify
exported spans from the real Cloudflare host, not only an in-memory test tracer.

For run capacity, instrument message acceptance, durable request creation,
database acquisition, connection/transaction/lane-lock wait, lease mutation and
commit, trusted grant, and run start. Instrument coordinator poke/coalescing,
database scan, session wake RPC, alarm scheduling/firing, deadline lag, and
periodic recovery. Capture scan bounds, eligible/blocked counts, effective limits,
blocking lane, and retry/failure classification where meaningful.
Return database-measured lock/decision durations as bounded timing metadata;
observability must not turn the atomic function call into extra client round
trips or mislabel total SQL time as lock wait.

Separate immediate-admission latency from true queue wait and from wakeup delay.
Recover request trace context from PostgreSQL when issuing wakeups. An alarm has
its own trace; a coalesced drain may link relevant traces without claiming one
request caused all work. Keep poke payloads free of scheduling state. Metrics use
bounded lane/type/outcome labels, and telemetry failure cannot block admission,
terminal release, or recovery.

---

## 13. Implementation Plan

1. **EDA resumables**
   - Add event schemas, reducer state, dispatch state, prompt/UI projection, and
     trusted resolve/cancel/expire API.
   - Add tests for open, resolve, cancel, expire, multiple open resumables, and
     crash-prefix recovery.

2. **EDA token accounting**
   - Promote provider usage into reducer state.
   - Track tokens by inference/run/session.
   - Keep units as tokens only.

3. **Gia lineage metadata**
   - Add `rootSessionId`, `parentSessionId`, `sessionKind`, and
     `capacityOwnerUserId`; preferably also `depth`, `subagentName`, and
     `subagentBatchId`.
   - Update opaque session-token, tool context, workspace, and E2B routing to
     distinguish current session from root owner.

4. **Subagent extension skeleton**
   - Add `agent.spawn_subagent` capability.
   - Add `spawnSubagents` tool that opens a resumable and emits
     `SubagentBatchRequested` in one atomic durable batch, with stable identities
     across retries of the same tool call.
   - Add parent durable outbox sink to enqueue with a stub orchestrator.

5. **SubagentOrchestrator DO**
   - Implement manifest store, idempotent enqueue, ticket state, child launch,
     deadlines, join evaluation, cancellation, and parent resolve.
   - Implement direct PostgreSQL acquisition, lane defaults/overrides,
     all-or-none memberships, and sink-only reporting before enforcing capacity
     on root and child runs. Preserve identities when migrating global leases.
   - Implement a wakeup-only capacity coordinator with minimal pokes, coalesced
     drains, deadline alarms, and an independent ten-minute best-effort sweep.

6. **Child reporting**
   - Add child result artifact contract.
   - Add child durable sink to report terminal/progress facts to orchestrator.

7. **CLI/UI/observability**
   - Add read/wait/abort commands and waiting/subagent UI.
   - Install both coordinator host tracers and verify exported stage spans,
     bounded metrics, and query helpers.

8. **Load and recovery validation**
   - Multi-hour hundreds-child soak.
   - Parent DO eviction mid-wait.
   - Orchestrator eviction mid-batch.
   - Child timeout and late result.
   - Duplicate resolver calls.
   - Interrupt/stop while waiting.

---

## 14. Testing Requirements

EDA core:

- reducer folds multiple resumables correctly;
- dispatch distinguishes running, waiting, and idle;
- queue/steer/interrupt/stop semantics while waiting;
- resolving one of many resumables resumes exactly once when policy says auto;
- recovery from every durable prefix around resumable open/resolve;
- token usage accumulates from provider usage events.

Gia extension:

- tool absent without `agent.spawn_subagent`;
- tool execution opens resumable and completes normally;
- crash/cancellation before or after the atomic resumable/request commit leaves
  neither event or both; retry after a lost tool-result reply creates one batch
  and one resumable;
- durable outbox retries orchestrator enqueue without duplicate batch creation;
- orchestrator launch is idempotent by ticket;
- five root and ten subagent leases are independently enforced per person under
  the documented soft-expiry model, with per-user overrides;
- all descendants share the root capacity owner and subagent lane;
- multi-lane acquisition is atomic, FIFO/oldest-eligible ordering is maintained,
  and unrelated lanes do not block one another;
- waiting parents release their active run lease and reacquire on resume;
- an immediately acquired run starts while the capacity coordinator is down;
- a dropped poke preserves queued work and is recovered by later traffic or the
  periodic sweep; cold/empty-queue/coalesced-poke alarm behavior is tested in
  Workerd;
- duplicate acquisition/grants, expired reservations, late/replayed events, and
  deletion cannot leak memberships or duplicate commands;
- real coordinator spans distinguish lock wait, database work, wakeup delay,
  capacity wait, and run startup;
- admission timeout, child timeout, and batch deadline all terminalize state;
- all join modes resolve the parent once;
- late child reports do not re-resume the parent;
- interrupt/stop while waiting cancels or abandons downstream work according to
  policy;
- child workspace and sandbox access route to root ownership while logs attribute
  current child identity.

---

## 15. Open Questions

- Should `queue` while waiting ever auto-resume, or should only resolve/interrupt
  start a run?
- Should `steer` while waiting be supported in v1, or always degrade to queued
  context?
- What is the default interrupt policy for approval resumables vs subagent
  resumables?
- Should all resumables have deadlines, or are some manual approval waits allowed
  to be indefinite with operator-visible age?
- Should child capability delegation snapshot all tool visibility at spawn time?
- Should orchestrator state be one singleton, sharded by root session, or split
  into broker plus per-root orchestrators?
- Where should large join reports live: root workspace, R2 session files, or a
  dedicated artifact table?
- What exact UI grouping should subagent sessions use in the sidebar?

---

## 16. Design properties

This proposal requires:

- subagents are ordinary Gia sessions;
- lineage is first-class;
- root workspace and sandbox are shared;
- callers do not supply authoritative lineage;
- logs must support root-tree and child-level forensics.
- PostgreSQL owns run capacity through composable per-person lanes, with a
  coordinator used only for best-effort wakeups outside immediate admission.

Completion notification uses a clear split:

- EDA core provides generic resumables and waiting/resume semantics.
- Gia subagent orchestration lives in a sidecar Durable Object.
- The parent is resumed by resolving a durable resumable, not by a hidden Web
  Socket side effect, a blocked tool call, or a sink-submitted command.
