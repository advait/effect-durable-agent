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

Publishing is one sequence. Once the go-ahead names the version, run every step below in order
without handing back between them: a finished wait (CI, the publish run) is the cue for the next
step, never a stopping point. The only stops are a failure, a step this skill says needs the
maintainer, or the hand-back after step 7.

1. **Version PR.** Set the same new, never-published version in all three package manifests,
   refresh `pnpm-lock.yaml` and add the `CHANGELOG.md` entry. A stable version also sets every
   `publishConfig.tag` to `latest`; `pnpm run release:versions` enforces the tag.
2. **Land it.** `pnpm install --frozen-lockfile && pnpm run release:check` passes locally; review
   and merge as above. When the version PR is stacked on another PR, merge the lower one without
   `--delete-branch`, retarget the version PR to `master`, then delete the branch: deleting a base
   branch closes the PRs stacked on it.
3. **Wait for `master` CI** on the merge commit to succeed:
   `gh run list -R advait/effect-durable-agent --branch master --workflow CI --json headSha,conclusion`.
4. **Create the GitHub release.** A merged version PR publishes nothing by itself. Only a release
   triggers `publish.yml`:

   ```bash
   git show origin/master:CHANGELOG.md | awk '/^## <version>$/{f=1;next} /^## /{if(f)exit} f' > "$tmp/notes.md"
   gh release create v<version> -R advait/effect-durable-agent --target <merge sha> \
     --title "<version>" --notes-file "$tmp/notes.md" --latest   # --prerelease instead of --latest for a prerelease
   ```

5. **Wait for `Publish to npm`** (`gh run list -R advait/effect-durable-agent --workflow publish.yml`,
   the run whose branch is `v<version>`) to succeed. On failure follow
   [releasing](../../../docs/releasing.md#failure-handling); never retry by moving the tag.
6. **Verify the registry** with the **verify** skill's registry lane for all three packages.
7. **Gia.** Open a separate PR in `goguardian/gia` bumping the catalog pin and
   `gia-cf/package.json`'s `effect-durable-agent-cloudflare`, landed through Gia's own **release**
   skill. Hand the version to another agent instead when the maintainer names one doing that work.

**Done means all of these hold**; check them before handing back, and if any fails, the release
is not done:

```bash
gh release view v<version> -R advait/effect-durable-agent --json tagName,isPrerelease,url
for p in effect-durable-agent effect-durable-agent-cloudflare effect-durable-agent-celld; do
  npm view "$p@<version>" version && npm view "$p" dist-tags --json
done
```

Hand back the merged SHA, the release URL, the publish run URL, the registry evidence and the Gia
PR (or who it was handed to).
