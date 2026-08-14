# Examples

All executable examples live here so the supported hosts and application patterns are discoverable
from one place.

## Cloudflare Durable Objects

| Example | Demonstrates |
| --- | --- |
| [`001-no-tools`](./cloudflare/001-no-tools) | Minimal Durable Object session and durable command admission. |
| [`002-slack-bridge`](./cloudflare/002-slack-bridge) | Idempotent ingress, custom events/reducers, and retrying durable delivery. |
| [`003-sandbox-lifecycle`](./cloudflare/003-sandbox-lifecycle) | Tool and product events reduced into one UI model. |

## celld

| Example | Demonstrates |
| --- | --- |
| [`001-no-tools`](./celld/001-no-tools) | A deployable celld cell using the same EDA durability bridge. |

Examples import the public workspace package names. They do not reach into package-local source, so
their typecheck and tests exercise the same API surface available to external consumers.
