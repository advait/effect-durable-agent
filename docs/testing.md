# Effect Durable Agent testing

EDA tests use a small set of archetypes. Choose the lowest-cost archetype that crosses the boundary where the behavior can fail; do not repeat the same assertion at every layer.

## Archetypes

| Archetype | Purpose | Shape |
| --- | --- | --- |
| Pure unit | Decisions, reducers, projections, ordering | Plain inputs and outputs; no layers, clocks, storage, or model |
| Schema contract | Untrusted boundary acceptance and rejection | Decode representative valid values and invalid equivalence classes |
| Property/model | Laws and state-machine invariants over broad input spaces | Fast-check generators with deterministic seeds on failure |
| Effect service contract | One service boundary, including typed failures, interruption, and resources | Isolated layer per test unless shared state is intentional; controllable clocks for time |
| Canned-model journey | High-fidelity runtime behavior without provider nondeterminism | Fake `LanguageModel`; real EDA runtime, tools, event log, reducers, and host adapter |
| Real-provider smoke | Provider wire compatibility only | Explicit opt-in; minimal assertions; never part of the default suite |

Examples and documentation scenarios are not a seventh archetype. They should reuse a pure unit or canned-model journey shape and exist only when they prove an externally useful example.

## Fidelity ownership

- Pure reducers own transition tables and ordering laws.
- Service contracts own typed error, interruption, retry, and resource semantics.
- `session-state-control.model.test.ts` owns generated command/control lifecycle invariants.
- `session-state-crash-simulation.test.ts` owns recovery from every durable batch prefix. Do not replace these prefix sweeps with a few named regressions.
- `packages/effect-durable-agent-cloudflare/src/durable-object-runtime.test.ts` owns the canned-model host journey through admission, streaming, SQLite persistence, reducer checkpoints, cold-start replay, and transcript hydration.
- `packages/effect-durable-agent-cloudflare/src/durable-object-store.test.ts` owns the semantic store contract plus Durable Object SQLite paging, transaction, sidecar, and migration behavior.
- `packages/effect-durable-agent/testing/offline-trace/offline-trace.test.ts` owns multi-turn prompt continuity, tool continuation, parallel tool ordering, and trace artifacts.
- The Cloudflare, celld, and Rivet package-local `testing/integration/host-conformance.test.ts`
  entries register the same `testing/host-conformance/suite.ts` against real workerd, celld, and
  Rivet Engine processes. The shared suite owns persistence, WebSocket resume/ACK, restart
  idempotency, in-flight hard-crash recovery, and warm/cold destruction semantics.
- `packages/effect-durable-agent/src/services/runtime.openai-smoke.test.ts` and `packages/effect-durable-agent-cloudflare/testing/integration/runtime-real.test.ts` are opt-in provider smokes. The default suite must never need a network or provider key.

Before adding a regression test, identify which owner failed. Extend an existing table, generator, crash scenario, or journey when possible. Add a standalone named regression only when the input represents a distinct business rule or failure boundary.

## Effect testing rules

- Use Effect's `TestClock` for timeout, retry, throttle, and polling behavior. A default-suite test must not wait for a production delay.
- Build dependencies as layers and provide them at the test boundary. Share a layer only when shared state is intentional; EDA session tests normally require isolation.
- Keep the fake model at the `LanguageModel` boundary. Do not mock internal runtime, reducer, event-store, or tool orchestration calls in a canned-model journey.
- Prefer generated properties for invariants over enumerating permutations as named examples.
- Assert durable facts and observable projections. Avoid assertions whose only value is proving a constructor returns its literal arguments, a constant has an exact value, or Effect/TypeScript supports an upstream library feature.
- Real-provider tests are smoke tests, not the source of behavioral fidelity.

### Vite+ compatibility

The repository currently runs tests through `vite-plus/test`. `@effect/vitest` imports stock `vitest` and `@vitest/runner` internals, which creates a second incompatible suite context under the Vite+ runner. Therefore EDA tests cannot safely import `@effect/vitest` yet.

Until the runners converge, follow the same boundaries explicitly: run Effect programs at the outer test boundary, use layers for dependencies, and provide `TestClock.layer()` for temporal tests. Do not introduce an ad hoc imitation of `it.effect`; migrate directly to `@effect/vitest` once Vite+ exposes a compatible adapter or the repository returns to stock Vitest.

## Commands

Run every package, example, and real-host conformance suite:

```bash
mise run test
```

Run Gia's Effect-based EDA adapter tests from the sibling consumer package:

```bash
mise run //gia-cf:test -- workers/eda-agent
```

Run only the host-neutral core tests:

```bash
pnpm --filter effect-durable-agent run test
```

Run EDA's opt-in real-provider smoke:

```bash
EDA_OPENAI_SMOKE=true OPENAI_API_KEY=... \
  pnpm --filter effect-durable-agent exec vp test run \
  src/services/runtime.openai-smoke.test.ts
```

Run the larger provider-backed Durable Object host journey:

```bash
EDA_REAL_INTEGRATION=true OPENAI_API_KEY=... \
  pnpm --filter effect-durable-agent-cloudflare exec vp test run \
  testing/integration/runtime-real.test.ts
```
