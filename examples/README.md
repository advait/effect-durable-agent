# Examples

All executable examples live directly in this directory so application patterns are discoverable
without choosing a host first.

| Example | Host | Demonstrates |
| --- | --- | --- |
| [`001-no-tools`](./001-no-tools) | Cloudflare Durable Objects | Minimal session and durable command admission. |
| [`002-slack-bridge`](./002-slack-bridge) | Cloudflare Durable Objects | Idempotent ingress, custom events/reducers, and retrying durable delivery. |
| [`003-sandbox-lifecycle`](./003-sandbox-lifecycle) | Cloudflare Durable Objects | Tool and product events reduced into one UI model. |
| [`004-celld-no-tools`](./004-celld-no-tools) | celld | A deployable cell using the same EDA durability bridge. |

Examples import the public workspace package names. They do not reach into package-local source, so
their typecheck and tests exercise the same API surface available to external consumers.
