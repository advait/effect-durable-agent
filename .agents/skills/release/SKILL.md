---
name: release
description: Land a reviewed effect-durable-agent change on master through a pull request, and publish a lockstep npm release through a GitHub release, then verify the registry and bump Gia. Use when a change is implemented and locally verified, or when asked to cut a release.
---

# Release

Status: implemented

Two steps, each with its own authority: landing a change on `master`, and publishing a version
to npm. The full publishing procedure and failure handling are in
[releasing](../../../docs/releasing.md); this skill says who does what.

## Authority

- **Without a go-ahead:** commit on a branch, push it, open a pull request with the review
  summary and the verify evidence, and stop.
- **Merge:** needs the maintainer's explicit go-ahead for that change (or for the release that
  includes it).
- **Publish:** creating a GitHub release publishes immutable npm versions. It needs an explicit
  go-ahead from the maintainer naming the version ("release v0.1.0"). With it, the agent may merge
  the version PR, create the release, verify and open the Gia pin PR.
- Never move or reuse a tag, unpublish, or change npm dist-tags; those are maintainer operations
  (dist-tags need interactive 2FA).

## Landing a change

1. **Review.** Before the PR, a model that did not write the diff reviews it (the contract
   default); each finding is `fix`, `consider` or `drop`, and every `fix` is resolved. Changes to
   durable formats, replay, migrations, the WebSocket or RPC contract, or exports always get this
   review.
2. **Pull request** to `master`. CI (`.github/workflows/ci.yml`, `mise run ci`) runs on it and
   must pass. The `master` ruleset only blocks deletion and force-push; review and CI are this
   skill's rules, not GitHub's.
3. **Merge** with the go-ahead, as a merge commit (history lands as `(#NN)` merges). Confirm CI
   passes on `master`.

## Publishing a version

1. A version PR sets the same new, never-published version in all three package manifests,
   refreshes `pnpm-lock.yaml` and adds the `CHANGELOG.md` entry. A stable version also sets every
   `publishConfig.tag` to `latest`; `pnpm run release:versions` enforces the tag.
2. `pnpm install --frozen-lockfile && pnpm run release:check` passes locally; review and land it
   as above.
3. Create a GitHub release on the merged commit, tag exactly `v<version>`, marked prerelease only
   for a prerelease version.
4. Watch `Publish to npm` (`.github/workflows/publish.yml`) to green. On failure follow
   [releasing](../../../docs/releasing.md#failure-handling); never retry by moving the tag.
5. Run the **verify** skill's registry lane for all three packages.
6. Open a separate PR in `goguardian/gia` bumping the catalog pin and
   `gia-cf/package.json`'s `effect-durable-agent-cloudflare`, landed through Gia's own **release**
   skill.

Hand back the merged SHA, the release URL, the registry evidence and the Gia PR.
