# Bounded startup replay regression

Run `pnpm build` once, then `pnpm test:replay-memory` with Node 24. The CI command includes this check.

The probe writes a temporary, disk-backed SQLite session through `DurableObjectSessionStore`, including message sidecars, summary artifacts, and context rebases. Each five-event cycle contains a user message, assistant message, summary, rebase, and compaction completion. It then exercises the real core services with a 128 MiB V8 old-generation heap limit:

- `sinks`: reconstruct seven caught-up sink projections from 4,500 events with 32 KiB message bodies (56.25 MiB of message text in the durable log).
- `state`: hydrate framework and app reducer state without checkpoints from 4,500 events with 64 KiB message bodies (112.5 MiB of durable message text).

The fixture deliberately compacts history so the current projection fits in memory. The regression concerns retaining historical decoded payloads unnecessarily; it does not promise that an arbitrarily large current projection fits in a Worker.

To compare another core revision, extract that revision's `packages/effect-durable-agent/src` into a temporary directory with access to the same installed dependencies and set `EDA_REPLAY_SOURCE` to it. The storage adapter and fixture stay identical. For example, with a prepared source directory:

```sh
EDA_REPLAY_SOURCE=/tmp/eda-baseline/src node --max-old-space-size=128 --expose-gc --import tsx scripts/check-replay-memory.mjs
EDA_REPLAY_SOURCE=/tmp/eda-baseline/src node --max-old-space-size=128 --expose-gc --import tsx scripts/check-replay-memory.mjs 4500 65536 state
node --max-old-space-size=128 --expose-gc --import tsx scripts/check-replay-memory.mjs 18000 32768
```

On Node 24.19.0, the alpha.10 core exhausted the heap for both default cases. The patched implementation completed at sampled heap peaks of approximately 63 MiB for sinks and 59 MiB for state. Sampling occurs at SQLite reads and after hydration; it is not an exact peak measurement. A larger 18,000-event sink fixture (225 MiB of durable message text) also completed, at a sampled peak of 61 MiB. The hard Node heap limit is the pass/fail guard. Node's heap limit is not Cloudflare's total isolate memory limit, and this synthetic fixture is not a copy of any production session.

Correctness is checked separately by `sink-replay.test.ts`: exact full-replay equivalence, missing/current/stale checkpoints, multiple page sizes, lagging and page-boundary sink cursors, filtered delivery, checkpoint payload preservation, failure, interruption, and sink-generated events during startup. Existing runtime, crash-recovery, and host integration suites remain required.

Startup changes no durable schema or checkpoint format. Framework and app reducers begin at their existing checkpoint sequences. Each sink still reconstructs its own projections from genesis through its own cursor, then delivers only later events. Initial reconstruction is sequential; delivery fibers start independently after every sink has subscribed, preventing an early sink's staged append from bypassing a later sink during startup.
