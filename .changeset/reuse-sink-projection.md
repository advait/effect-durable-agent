---
"effect-durable-agent": patch
---

Reuse the immutable hydrated session projection for sinks checkpointed at the same sequence, avoiding redundant history replay. Lagging sinks retain bounded reconstruction. The exported sink runner startup input now accepts initialProjection instead of initialHead; no persisted checkpoint format changes.
