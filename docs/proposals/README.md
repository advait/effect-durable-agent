# Feature requests from the docs-site support chat

Status: proposal. Nothing in this directory is implemented. FR-1 and FR-5 are approved to build.

A second app, the docs-site support chat with Salesforce handoff, asked EDA for nine features and
reported two minor findings. This directory judges each one as library design. The requests are
in the Gia repository at `docs-site/research/handoff/EDA-PROPOSAL.md` §9.

One rule shapes the plan. gia-cf and docs-site always run the same EDA version. So every change
here lands with Gia moving in the same step, and is validated in Gia before it is released.

## How each request was judged

- **Against the source.** Every claim about EDA 0.1.0 was checked in the code at `v0.1.0`. Each
  file says what the source shows.
- **Against two consumers.** A feature belongs in EDA when gia-cf and docs-site both have the
  problem, or when the runtime is the only place it can be solved. One consumer and a guess is
  not enough.
- **Against typed code.** A second agent wrote the docs-site design as Effect Schema code against
  EDA 0.1.0 (`docs-site/research/handoff-types/` in the Gia repository, branch
  `t3/handoff-effect-types`). Where its tests found something, the file cites the test.
- **From the problem.** Each file compares at least two designs. Several sketched APIs were
  replaced by something smaller.

## The nine requests

| Request | Verdict | Chosen design | Priority | Size |
| --- | --- | --- | --- | --- |
| [FR-1 Deciders](FR-1-deciders.md) | Build. Runtime feature. Gia has the same race in Slack ingress and subagent cancel. | Registered pure deciders run inside the append gate and commit a `DeciderAccepted` marker with their items, so a retry never decides twice. | P0, approved | Medium, about 3 days plus 1 to 2 in Gia |
| [FR-2 Participant messages](FR-2-participant-messages.md) | Build half. The transcript record is general. The participant concept and model visibility are app logic. | A mapping operation that turns an app event into a positioned `app` transcript record. Prompt visibility stays in the app's prompt projector. | P2 | Medium, 3 to 4 days |
| [FR-3 Hold](FR-3-hold.md) | Build the small step. A hold already exists as a deferred run with a withheld grant. Only the state is missing from `resolve`. | Add `state` and `reducerStates` to `RunSchedulingInput`. Resumables stay a separate, later decision. | P2 | Small, 1 day |
| [FR-4 Sink wakeups and outbox](FR-4-sink-wakeups.md) | Build, split in two. The wakeup is runtime. The outbox is an optional helper. One claim is overstated: a sink can already retry inside `process`. | `ctx.wakeAt` as one durable wakeup per sink on a third alarm owner, plus an optional `sinks/outbox` module with reconcile. | P2 | Medium, about 5 days |
| [FR-5 Dedupe keys](FR-5-event-dedupe-keys.md) | Build. Store feature. Gia already has three home-made schemes. | An optional `dedupeKey` on durable events, unique per `(namespace, type, dedupeKey)`, enforced in the append. | P0, approved, first | Small, about 1 day plus half in Gia |
| [FR-6 App timers](FR-6-app-timers.md) | Build as a helper, not as the sketched API. | Timers live in reducer state. An optional timer sink fires what is due with a dedupe key and sleeps with FR-4's wakeup. | P2, after FR-4 | Small, 1 day |
| [FR-7 Stream source](FR-7-stream-source.md) | Do not build. One consumer, and a different kind of actor. | Keep the connector as product code. FR-4, FR-5 and FR-6 shrink it. | None | Large if ever |
| [FR-8 Tool awaits input](FR-8-tool-awaits-input.md) | Do not build as sketched. It contradicts the accepted resumables design. Two consumers already ship the workaround. | Document the pattern, add a `ToolByCallId` transcript mapping target, and rely on FR-1 and FR-9. | P3 | Small, 1 to 2 days |
| [FR-9 Tool ends run](FR-9-tool-ends-run.md) | Build. Runtime feature, and a one-branch policy change. | A `ToolContinuation: "end_run"` annotation recorded on `ToolCallCompleted`, read by the pure continuation policy and by recovery. | P1, next | Small to medium, 2 days |

There are four [minor findings](MINOR-FINDINGS.md), two from the request and two from the typed
docs-site code. All are true. The first ships with FR-1 as an allowlist. The second is closed by
FR-5 and a documentation change. The third loses data silently (a `Redacted` email is stored as
the string `<redacted>`) and gets a loud failure in the same release. The fourth is closed by
FR-1's `ctx.decide` and a read-only `ctx.snapshot`.

## Build order

1. **FR-5**, then **FR-1** with the namespace rule. One release, proposed `0.2.0`. The release and
   lockstep plan is in [FR-1](FR-1-deciders.md).
2. **FR-9** and **FR-3**. Both are small. With FR-1 they make "no AI output after the end" a
   framework guarantee for an app that submits with `queue`.
3. **FR-4**, then **FR-6** on top of it.
4. **FR-2** and the mapping target from **FR-8**, together, because both change the transcript.

## What the requests have in common

Seven of the nine reduce to three additions to the runtime.

- **Decide inside the gate** (FR-1). The app reads state and appends in one step.
- **Append once** (FR-5). A fact has a natural key, and the store enforces it.
- **Wake without an event** (FR-4). A sink can ask to run again at a time.

FR-6 is a helper over the second and third. FR-3 and FR-9 are small policy changes at boundaries
the runtime already owns. FR-2 and the useful part of FR-8 are transcript mappings. FR-7 is the one
request that asks EDA to be a different product.

## Adversarial review

FR-1 and FR-5 went through the `interrogate` review: two models tried to break the drafts with
runs against the real code. What they found and what changed is recorded in
[INTERROGATE.md](INTERROGATE.md).
