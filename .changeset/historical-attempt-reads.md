---
"effect-durable-agent": patch
"effect-durable-agent-cloudflare": patch
"effect-durable-agent-celld": patch
---

Read retained TurnAttemptStarted and TurnAttemptCompleted histories, including their assistant and tool references, without rewriting stored events. Interpret attempts only within reduction while preserving original turn identities and event sequence boundaries. Framework checkpoints are not reset.
