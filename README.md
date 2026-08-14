# Effect Durable Agent monorepo

This repository houses the lockstep EDA package family, shared integration harnesses, documentation,
and executable examples.

## Packages

| Package | Source |
| --- | --- |
| [`effect-durable-agent`](./packages/effect-durable-agent) | Host-neutral commands, events, reducers, runtime, and adapter contracts. |
| [`effect-durable-agent-cloudflare`](./packages/effect-durable-agent-cloudflare) | Cloudflare Durable Objects storage, lifecycle, RPC, and WebSocket host. |
| [`effect-durable-agent-celld`](./packages/effect-durable-agent-celld) | celld deployment boundary over the shared Durable Objects-compatible host. |

All public packages use the same version and are released together. Host packages depend on the
exact matching core version after packing.

## Repository layout

```text
packages/   publishable package source and package-owned tests
examples/   executable consumers in one discoverable sequence
docs/       architecture, protocols, testing, and release guidance
scripts/    workspace-wide validation and release orchestration
testing/    cross-package conformance harnesses and package-consumer fixtures
```

The root is private and owns no package implementation. Package source, build configuration, and
package-specific tests stay beneath the corresponding full npm package name.

## Development

```sh
pnpm install --frozen-lockfile
pnpm run ci
```

See the [examples index](./examples), [architecture specification](./docs/spec.md),
[testing strategy](./docs/testing.md), and [release guide](./docs/releasing.md).
