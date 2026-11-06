---
name: coding-standards
description: Apply effect-durable-agent's TypeScript, Effect and testing rules when writing, changing or reviewing code here, and the check a diff must pass before commit.
---

# Coding standards

Status: implemented

These specialize the principle leaves and **typescript-best-practices** for this repository.

- **Effect v4, one aligned minor.** `effect`, `@effect/ai-*` and `@effect/vitest` are pinned
  exactly as devDependencies (`4.1.2`) and allowed patch releases as peers (`~4.1.2`). Effect AI and
  HTTP are `@stability unstable` upstream, so a new Effect minor is an EDA release
  ([releasing](../../../docs/releasing.md#versioning)). Upgrade the whole aligned set together and
  record it in `UPGRADE_LOG.md`.
- **Toolchain.** TypeScript 7 (tsgo), Vite+ (`vp fmt`, `vp pack`, `vp test`) and oxlint with
  `--deny-warnings`. `mise run install` patches oxlint for Effect diagnostics; run it before
  linting. The repository's own oxlint rules live in `packages/effect-durable-agent/tooling/oxlint`
  and ship with the core package.
- **Tests.** Pick the archetype and owning suite from [testing](../../../docs/testing.md) before
  adding a test; extend the owner (scenario corpus, generator, crash-prefix sweep, journey) rather
  than adding a named regression. Do not import `@effect/vitest` under Vite+; use `TestClock` for
  time. The default suite never needs a network or provider key.
- **Durable formats.** A new event field is optional and decoded with retained excess; never
  rewrite stored events. A transcript interpretation change bumps `transcriptSemanticRevision` and
  re-records fingerprints (`UPDATE_TRANSCRIPT_FINGERPRINTS=1`).
- **Public surface.** Every export is a contract across all three packages. Adding a subpath means
  updating its `exports` map; removing or narrowing one is a breaking change for the changelog.
- **Changelog.** Every user-visible change adds an entry under the next version in `CHANGELOG.md`,
  marks breaking items `**Breaking:**`, and states the storage, protocol and API impact.

**Gate before commit:** `mise run ci` passes. It is what `.github/workflows/ci.yml` runs on every
pull request and every push to `master`.
