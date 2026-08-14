# effect-durable-agent-celld

[celld](https://celld.dev/) host for [`effect-durable-agent`](../effect-durable-agent/README.md).

Start with the deployable [`examples/004-celld-no-tools`](../../examples/004-celld-no-tools)
application.

```sh
pnpm add effect-durable-agent@alpha effect-durable-agent-celld@alpha
```

```ts
import { EDASessionCell } from "effect-durable-agent-celld";

export class AgentSession extends EDASessionCell<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env, makeAgentOptions(env));
  }
}
```

celld implements the Cloudflare Workers and Durable Objects API. This package
therefore uses the same SQLite, alarm, RPC, and hibernatable-WebSocket adapter
as the Cloudflare host, while providing a celld-specific installation and
deployment boundary. Deploy the Worker with `celld deploy` and run it against
an S3-compatible or Google Cloud Storage bucket as described by celld.

The supported baseline is celld `v0.2.0`. EDA does not require unsupported
managed Cloudflare bindings; application tools may still be limited by celld's
current Workers compatibility surface.

The core, Cloudflare adapter dependency, and celld host are published in
lockstep and must use the same version.
