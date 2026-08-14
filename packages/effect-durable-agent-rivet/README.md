# effect-durable-agent-rivet

Rivet Actors host for [`effect-durable-agent`](https://www.npmjs.com/package/effect-durable-agent).

This package maps one EDA session to one [Rivet Actor](https://rivet.dev/actors/), using actor-local
SQLite for durable history, actor lifecycle hooks for recovery, `keepAwake` for active work, and
Rivet's low-level WebSocket handler for the EDA resume/ACK protocol.

Start with the runnable [`examples/005-rivet-no-tools`](../../examples/005-rivet-no-tools).

The package is released in lockstep with `effect-durable-agent`. Install identical versions:

```bash
pnpm add effect-durable-agent@alpha effect-durable-agent-rivet@alpha rivetkit@2.3.10
```

```ts
import { setup } from "rivetkit"
import { createEDASessionRivetActor } from "effect-durable-agent-rivet"

export const edaSession = createEDASessionRivetActor({
  config: makeRuntimeConfig(),
  modelLayer: makeModelLayer(),
})

export const registry = setup({ use: { edaSession } })
registry.start()
```

Rivet deployment tokens define the outer access boundary. If the application needs tenant- or
session-level policy too, provide `authorize`; throwing rejects the connection before any action
(including `destroySession`) or raw WebSocket can run:

```ts
export const edaSession = createEDASessionRivetActor({
  authorize: ({ request, sessionId }) => authorizeSession(request, sessionId),
  config: makeRuntimeConfig(),
  modelLayer: makeModelLayer(),
})
```

EDA's `submitAndBlock` and `blockOnCommand` actions follow a command until it reaches a terminal
event, so the factory raises Rivet's 60-second default action timeout to an effectively unbounded
safe value. Set `actionTimeoutMs` explicitly if the application needs a finite request limit.

Use a stable EDA `SessionId` as the actor key:

```ts
import { createClient } from "rivetkit/client"
import type { registry } from "./server"

const client = createClient<typeof registry>(process.env.RIVET_ENDPOINT)
const session = client.edaSession.getOrCreate([sessionId])
```

The package-local conformance test starts a real Rivet Engine and proves the same command,
snapshot, reconnect, hard-crash recovery, durable idempotency, and warm/cold destruction contract
as the Cloudflare and celld hosts.
