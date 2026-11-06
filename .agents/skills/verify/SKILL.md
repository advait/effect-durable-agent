---
name: verify
description: Prove an effect-durable-agent change works the way a host or consumer would observe it, through the real-host conformance suites, the packed-package check, Gia's adapter tests and, after a release, the public registry. Use before declaring a change done.
---

# Verify

Status: implemented

EDA has no server of its own. Its real surfaces are the hosts it runs on (workerd and celld),
the packages a consumer installs, and Gia.

| Lane | Command | Proves |
|---|---|---|
| Suites | `mise run test` | Unit, property, crash-prefix and canned-model journeys, plus `testing/host-conformance/suite.ts` against real workerd and celld processes (persistence, WebSocket resume, hard-crash recovery). |
| Replay memory | `pnpm run test:replay-memory` | Replay and sink hydration stay within a 128 MB heap for large logs. Run for replay, reducer, sink or checkpoint changes. |
| Packed packages | `mise run package:check` | The tarballs a consumer gets install, typecheck and bundle in an isolated Worker. Run for export, build or manifest changes. |
| Provider smoke | `EDA_OPENAI_SMOKE=true OPENAI_API_KEY=... pnpm --filter effect-durable-agent exec vp test run src/services/runtime.openai-smoke.test.ts` | Provider wire compatibility. Opt-in; needs a key the maintainer provides. |
| Downstream | `mise run //gia-cf:test -- workers/eda-agent` from the enclosing Gia checkout | Gia's EDA adapter still holds. Gia installs EDA from npm, so this lane exercises a local change only when Gia is pointed at it; say which version it ran against. |
| Registry | [releasing](../../../docs/releasing.md) steps 8-9 | After a release: `npm view` metadata and dist-tags for all three packages, then a clean consumer install from the public registry. |

Pick the lanes the change can fail in; `mise run ci` alone covers suites, replay memory and the
packed-package check. Record each command and what it showed.
