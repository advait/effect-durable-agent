# Contributing

Issues and pull requests for Effect Durable Agent live in
[`advait/effect-durable-agent`](https://github.com/advait/effect-durable-agent).

## Development

Install dependencies and run the full validation suite:

```bash
pnpm install --frozen-lockfile
pnpm run ci
```

Validate all distributable packages, including isolated Cloudflare Worker and Node/Rivet consumers:

```bash
pnpm run build
pnpm run package:check
```

Use `pnpm run lint:fix` for automated formatting and lint fixes. Add tests for behavior changes, keep
the public event and state model explicit, and preserve compatibility across all exported
subpaths.

See [the release guide](./docs/releasing.md) for maintainer-only publishing steps.
