# effect-durable-agent-cloudflare

Cloudflare Durable Objects host for [`effect-durable-agent`](../../README.md).

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

The host maps one session to one SQLite-backed Durable Object and owns alarms,
hibernatable WebSockets, RPC decoding, recovery, and sink checkpoints. Register
only the concrete application subclass in `wrangler.jsonc`.

Durable Object RPC uses structured clone. Encode Schema class instances before
passing commands or batches across the Worker-to-object boundary with
`encodeEdaRpcCommand` or `encodeEdaRpcSubmittables`.

The core and host packages are published in lockstep and must use the same
version.
