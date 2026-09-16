# Bounded startup replay regression

Run `pnpm build` once, then `pnpm test:replay-memory` with Node 24. The CI command includes this check.

The probe writes a temporary, disk-backed SQLite session through `DurableObjectSessionStore`, including message sidecars, summary artifacts, and context rebases. Each five-event cycle contains a user message, assistant message, summary, rebase, and compaction completion. It then exercises the real core services with a 128 MiB V8 old-generation heap limit:

- `sinks`: hydrate session state without reducer checkpoints, then initialize seven caught-up sinks from 4,500 events with 32 KiB message bodies (56.25 MiB of message text in the durable log).
- `checkpointed`: use the caught-up fixture with valid framework and app reducer checkpoints at the head, including the real checkpoint codec and pointer hydration.
- `lagging`: use the same fixture with all seven sink cursors five events behind, exercising bounded reconstruction and subsequent delivery.
- `state`: hydrate framework and app reducer state without checkpoints from 4,500 events with 64 KiB message bodies (112.5 MiB of durable message text).

The fixture deliberately compacts history so the current projection fits in memory. The regression concerns retaining historical decoded payloads unnecessarily; it does not promise that an arbitrarily large current projection fits in a Worker.

To compare another core revision, extract that revision's `packages/effect-durable-agent/src` into a temporary directory with access to the same installed dependencies and a parent `package.json` declaring `"type": "module"`, and set `EDA_REPLAY_SOURCE` to it. The storage adapter and fixture stay identical. For example, with a prepared source directory:

```sh
EDA_REPLAY_SOURCE=/tmp/eda-baseline/src node --max-old-space-size=128 --expose-gc --import tsx scripts/check-replay-memory.mjs
EDA_REPLAY_SOURCE=/tmp/eda-baseline/src node --max-old-space-size=128 --expose-gc --import tsx scripts/check-replay-memory.mjs 4500 65536 state
node --max-old-space-size=128 --expose-gc --import tsx scripts/check-replay-memory.mjs 18000 32768
node --max-old-space-size=128 --expose-gc --import tsx scripts/check-replay-memory.mjs 4500 32768 checkpointed
```

On Node 24.19.0, the alpha.10 core exhausted the heap for both default cases. The patched implementation completed at sampled heap peaks of approximately 63 MiB for sinks and 59 MiB for state. Sampling occurs at SQLite reads and after hydration; it is not an exact peak measurement. A larger 18,000-event sink fixture (225 MiB of durable message text) also completed, at a sampled peak of 61 MiB. The hard Node heap limit is the pass/fail guard. Node's heap limit is not Cloudflare's total isolate memory limit, and this synthetic fixture is not a copy of any production session.

Correctness is checked separately by `sink-replay.test.ts`: exact full-replay equivalence, missing/current/stale checkpoints, multiple page sizes, lagging and page-boundary sink cursors, filtered delivery, checkpoint payload preservation, failure, interruption, and sink-generated events during startup. Existing runtime, crash-recovery, and host integration suites remain required.

Startup changes no durable schema or checkpoint format. Framework and app reducers begin at their existing checkpoint sequences. A sink at exactly the hydrated session sequence shares that immutable framework/app projection. A lagging sink reconstructs its own projections from genesis through its cursor, then delivers only later events. Reducers must return new state rather than mutate shared input. The exported `EDASinkRunnerStartInput` now takes `initialProjection` instead of `initialHead`, deriving the head from `initialProjection.reduced.lastSeq`; direct callers must pass framework and app state at that same sequence. Persisted sink cursors and payloads are unchanged. Initial reconstruction is sequential; delivery fibers start independently after every sink has subscribed, preventing an early sink's staged append from bypassing a later sink during startup.

The probe reports session hydration and sink initialization separately, as well as their combined elapsed time. Both baseline and optimized runs hydrate the session projection first; the comparison does not give the optimized revision free precomputed state. A GC between phases provides a comparable retained-heap starting point. Sink statement counts include checkpoint verification reads. `peakHeapMiB` includes both phases; `sinkPeakHeapMiB` covers only sink initialization. Without reducer checkpoints, session hydration can still dominate the overall memory peak even when sink initialization stops allocating historical payloads.

## Hydrated projection reuse measurements

Local Node 24.19.0 comparison against alpha.11 (`d345635`), using the same script,
storage adapters, fixture, and 128 MiB old-generation cap. Each 4,500-event result
below is the median of three fresh processes per revision, run sequentially.
The fixture has seven caught-up sinks, one app counter reducer, and 32 KiB
message bodies. Setup and fixture writes are excluded; hydration is included.

| Case | Alpha.11 | Projection reuse |
| --- | ---: | ---: |
| Valid reducer checkpoints: hydration + sink initialization | 6.437 s | 0.165 s |
| Valid reducer checkpoints: sampled total peak heap | 62.1 MiB | 34.4 MiB |
| Missing reducer checkpoints: hydration + sink initialization | 8.760 s | 2.131 s |
| Missing reducer checkpoints: sampled total peak heap | 62.1 MiB | 62.2 MiB |
| Missing reducer checkpoints: sink initialization only | 6.608 s | 0.0052 s |
| Missing reducer checkpoints: sampled sink peak heap | 62.1 MiB | 29.5 MiB |
| Sink-phase SQLite statements, including checkpoint verification | 20,895 | 14 |

A separate 18,000-event run reduced sink initialization from 49.869 s to 0.0054 s,
with 83,496 versus 14 sink-phase SQLite statements. Both completed under the cap.
The optimized lagging-sink case still performed bounded replay, completed delivery,
and peaked at 61.8 MiB. The unchanged 64 KiB state-hydration case passed at 59.0 MiB.
These are synthetic local measurements, not a prediction of production latency.

The focused runtime/sink/recovery suite covers exact sequence matching, mixed
cursors, immutable framework and array-valued app projections after subsequent
deliveries, preserved checkpoint payloads, read failures, interruption, and staged
startup appends reaching caught-up as well as lagging sinks. A complete runtime
startup test rejects genesis reads with valid session and sink checkpoints.

A local `--expose-gc` probe also advanced all seven live sinks past the shared
initial projection, dropped the caller reference, and verified through `WeakRef`
that the old projection was collectible before the sink scopes closed. Runner
closures retain only their head and delivery capabilities, not the startup input.
