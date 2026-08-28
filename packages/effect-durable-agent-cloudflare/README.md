# effect-durable-agent-cloudflare

Cloudflare Durable Objects host for [`effect-durable-agent`](../effect-durable-agent/README.md).

Executable examples live in the repository's shared [`examples`](../../examples) directory.

```sh
pnpm add effect-durable-agent@alpha effect-durable-agent-cloudflare@alpha
```

```ts
import { EDASessionDurableObject } from "effect-durable-agent-cloudflare";

export class AgentSession extends EDASessionDurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env, makeAgentOptions(env));
  }
}
```

The adapter maps one session to one SQLite-backed Durable Object. Ownership is
split deliberately:

- `EDASessionDurableObject` maps Cloudflare callbacks and object identity.
- `EDASessionController` coordinates the session use cases.
- `EDASessionRuntime` owns the disposable Effect runtime and keep-alive.
- `EDAWebSocketConnectionManager` owns accepted sockets and hibernation state.

Idle sockets use `ctx.acceptWebSocket`, Effect Schema attachments, and
automatic ping responses. They own no timer or resident subscriber fiber.
Register only the concrete application subclass in `wrangler.jsonc`.

Apps with an existing client wire contract may register one
`EDAWebSocketProjection`. An authenticated Worker selects it on the internal
request with `EDA_WEB_SOCKET_PROJECTION_HEADER`; the Durable Object still owns
the accepted socket and persists the app projection state through hibernation.
Do not build a Worker-side WebSocket bridge for protocol translation.

Durable Object RPC uses structured clone. Encode Schema class instances before
passing commands or batches across the Worker-to-object boundary with
`encodeEdaRpcCommand` or `encodeEdaRpcSubmittables`.

The core and host packages are published in lockstep and must use the same
version.

Public subpaths are `/durable-object`, `/session-controller`, `/openai`,
`/rpc`, and `/storage`. The WebSocket protocol and pure delivery machine
are exported from `effect-durable-agent/websocket`.
