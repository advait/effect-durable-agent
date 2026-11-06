---
name: project-context
description: State effect-durable-agent's operating context (who consumes it, what is one-way, what failure costs most), the risk proportion every principle applies in, and the mise tasks. Read before making poteto-mode decisions in this repository.
---

# Project context

Status: implemented

## Who uses it

effect-durable-agent (EDA) is a public library: three lockstep npm packages,
`effect-durable-agent`, `effect-durable-agent-cloudflare` and `effect-durable-agent-celld`
([releasing](../../../docs/releasing.md)). The core gets roughly 2-3k downloads a month. Its main
deployed consumer is GoGuardian's Gia (`goguardian/gia`), which pins an exact published version
through its pnpm catalog and runs EDA sessions in production Durable Objects. Gia carries this
repository as a submodule but installs EDA from npm, so a change reaches Gia only through a
release and a pin bump.

## What is one-way

- **Published versions.** npm versions are immutable; never reuse one
  ([releasing](../../../docs/releasing.md#failure-handling)).
- **Stored histories.** Event logs, reducer and sink checkpoints and transcript tables live in
  deployed Durable Objects and must stay readable by every later version. Unknown fields are
  retained as excess so an older reader survives a rollback; keep that true.
- **Schema migrations.** `_eda_schema_migrations` migrations are forward-only
  ([spec](../../../docs/spec.md)).
- **Wire contracts.** The WebSocket protocol ([websocket-protocol](../../../docs/websocket-protocol.md)),
  the RPC surface and every exported subpath in the three `package.json` `exports` maps
  ([CONTRIBUTING](../../../CONTRIBUTING.md)).

## What failure costs most

A release that misreads, rewrites or fails to replay a stored history in Gia's production Durable
Objects: users lose sessions, and the bytes are already published. Every change states in
`CHANGELOG.md` what it does to storage, protocol and API compatibility ("No storage, protocol or
API change" when nothing). Treat any change to durable event schemas, reducers, replay or
migrations as the high-risk class.

## Task runner

mise, from the repository root (`mise.toml`); tasks wrap the `package.json` scripts.

| Task | Does |
|---|---|
| `mise run install` | pnpm install plus the Effect tsgo oxlint patch. Run first in a new checkout. |
| `mise run check` | Format, lint and typecheck every package and the examples. |
| `mise run test` | Unit, property, example and real workerd/celld host-conformance suites. |
| `mise run ci` | What CI runs: build, check, test, replay-memory, lockstep versions, package check. |
| `mise run package:check` | Packs each package and installs it into an isolated Worker. |
| `mise run lint:fix` | Automated format and lint fixes. |

## Follow-ups

List them in the reply. GitHub issues on `advait/effect-durable-agent` are for users' reports.
