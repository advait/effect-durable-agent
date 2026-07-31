# 003 — Sandbox Lifecycle

This example shows the design space where EDA becomes much more than chat persistence.

A coding agent session often owns product state tightly coupled to the agent run:

- a sandbox starts;
- commands run inside it;
- a preview URL appears;
- a human approval is requested;
- the tool finishes;
- the sandbox is stopped later.

In EDA, those are not side-channel callbacks. They are durable app events in the same ordered session history as the framework tool and message lifecycle.

## The sequence

```text
seq  event                                             projection effect
---  ------------------------------------------------  -------------------------------
1    effect-durable-agent/ToolCallCreated              tool card appears
2    example.sandbox/SandboxStarting                   sandbox status: starting
3    example.sandbox/SandboxStarted                    sandbox status: running
4    example.sandbox/CommandStarted                    command row starts
5    example.sandbox/CommandCompleted                  command row gets wallClockTimeMs
6    example.sandbox/PreviewReady                      preview card gets URL
7    example.sandbox/ApprovalRequested                 approval card waits
8    example.sandbox/ApprovalGranted                   approval card resolves
9    effect-durable-agent/ToolCallCompleted            tool card completes
10   example.sandbox/SandboxStopped                    sandbox status: stopped
```

## What to notice

### 1. Framework state and app state reduce together

`reducer.ts` folds both EDA framework events and sandbox app events:

```text
ToolCallCreated
SandboxStarting
SandboxStarted
CommandStarted
CommandCompleted
PreviewReady
ApprovalRequested
ApprovalGranted
ToolCallCompleted
SandboxStopped
```

That one reducer derives the UI model: active sandbox, running command, last command timing, preview URL, approval state, tool card status, and stop reason.

### 2. SSR handoff is just reducer math

`scenario.test.ts` demonstrates the handoff directly:

```text
server renders state through seq N
client receives state_N
client requests events where seq > N
client reduces the suffix
client converges to the same state as a full replay
```

No separate sandbox sync protocol is needed. The sandbox lifecycle is already part of the same event sequence as the agent lifecycle.

### 3. Recovery has a real source of truth

If the process restarts, EDA can replay the durable session log and recover the committed facts. The reducer knows whether the sandbox started, which command finished, whether approval was granted, and where the tool lifecycle ended.

## Files

- `events.ts` — sandbox/preview/approval durable event schemas.
- `reducer.ts` — pure UI/app projection over framework + app events.
- `scenario.test.ts` — executable reducer and SSR handoff scenarios.

This example intentionally has no Worker facade. It is about the state-machine model itself, without HTTP or provider plumbing getting in the way.
