# Releasing Effect Durable Agent

Releases publish `effect-durable-agent`, `effect-durable-agent-cloudflare`, and
`effect-durable-agent-celld` together from the public
[`advait/effect-durable-agent`](https://github.com/advait/effect-durable-agent) repository. The
three packages always use the same version, and host packages depend on that exact core version in
their published manifests.

## Current public baseline

The current lockstep release is `0.2.2`. The `latest` npm dist-tag
resolves to it for core, Cloudflare, and celld; `alpha` stays at the final alpha,
`0.1.0-alpha.30`. Consumers should name the exact lockstep version so dependency updates remain
deliberate.

Do not reuse a published version or create a GitHub release for an older tag: publishing that
release would run the npm workflow against an immutable registry version. The Git tag alone does
not trigger the workflow.

## Versioning

Releases follow semver for 0.x: a patch release (`0.1.1`) fixes behaviour without changing the
API, and a minor release (`0.2.0`) may change the API. Every change still records its storage,
protocol and API impact in `CHANGELOG.md`.

Durable data is forward-compatible only. Every release reads event logs, checkpoints and
transcript tables written by any earlier release, and schema migrations run forward. Rolling a
host back to an older release after a newer one has written is not supported; fix forward with a
new release.

The Effect peers allow patch releases of the Effect minor the packages were built against
(`~4.1.2`). Moving to a new Effect minor is a new EDA release.

## One-time trusted-publisher setup

Configure the same trusted publisher for each package in its npm package settings:

- Provider: GitHub Actions
- Organization or user: `advait`
- Repository: `effect-durable-agent`
- Workflow filename: `publish.yml`
- Allowed action: `npm publish`
- Environment: leave blank unless `.github/workflows/publish.yml` is updated to use one

The two new package names must exist on npm before their trusted-publisher settings are available.
For the first three-package release only, an authenticated maintainer must run `release:check` and
then `pnpm run publish:workspace`. Publish all three manifests at the same version, and configure
the trusted publisher on both new packages immediately after that bootstrap. The publish script is
idempotent: it verifies and skips an identical artifact already in the registry, but refuses an
existing version with different contents. All subsequent versions use the release workflow. Do not
add a long-lived npm publishing token to the repository.

The workflow uses a GitHub-hosted runner, grants `id-token: write`, and runs a recent Node/npm pair.
npm uses the workflow's short-lived OIDC identity and automatically attaches provenance because
both the repository and packages are public.

After one automated release succeeds, configure npm publishing access to require two-factor
authentication and disallow tokens, then revoke any automation tokens that are no longer needed.
See npm's [trusted publishing guide](https://docs.npmjs.com/trusted-publishers/) for the current
registry requirements.

## Release channels

A stable version publishes with `publishConfig.tag` `latest`, and a prerelease with `alpha`;
`pnpm run release:versions` enforces the pairing, and `publish:workspace` passes the manifest's tag
explicitly because package-manager support for the manifest default is not consistent. An
alpha-tagged publish advances only `alpha` and does not move `latest`. npm may initialize `latest`
when a package name is published for the first time, even when that first publish uses another
tag; verify both tags after bootstrapping a new package name.

Trusted publishing authorizes `npm publish`, not `npm dist-tag`, so changing a dist-tag by hand is
an interactive maintainer operation and may require two-factor authentication. Always name the
exact version:

```bash
npm dist-tag add effect-durable-agent@0.2.2 latest
npm dist-tag add effect-durable-agent-cloudflare@0.2.2 latest
npm dist-tag add effect-durable-agent-celld@0.2.2 latest
```

## Release checklist

1. Choose a new version that has never been published for any of the three packages.
2. Update these three manifests to that exact version and refresh `pnpm-lock.yaml`:

   - `packages/effect-durable-agent/package.json`
   - `packages/effect-durable-agent-cloudflare/package.json`
   - `packages/effect-durable-agent-celld/package.json`
3. Add the release notes to `CHANGELOG.md`.
4. Run the complete release validation:

   ```bash
   pnpm install --frozen-lockfile
   pnpm run release:check
   ```

5. Merge the version PR and confirm `CI` passes on `master`.
6. Create a GitHub release targeting the merged commit. Its tag must be exactly
   `v<package version>`; for example, `v0.1.1`. Mark it as a pre-release only for a prerelease version.
7. Wait for the `Publish to npm` workflow to pass. The workflow independently reruns the full
   release check and rejects a tag that does not match the core package manifest.
8. Verify the registry metadata and dist-tags:

   ```bash
   for package_name in effect-durable-agent effect-durable-agent-cloudflare effect-durable-agent-celld; do
     npm view "$package_name@0.1.1" name version repository.url dist.integrity --json
     npm dist-tag ls "$package_name"
   done
   ```

9. Install from the public registry in a clean consumer and run its typecheck and bundle. Do not
   validate only against the repository or a local tarball.
10. Update downstream consumers, including the `goguardian/gia` submodule pin, in a separate PR.

## Failure handling

- npm package versions are immutable. Never try to reuse a version after any successful publish;
  increment the version and release again.
- npm does not provide a multi-package transaction. If any package uploads before a later publish
  fails, rerun `pnpm run publish:workspace` from the exact same commit: it verifies identical
  published bytes, skips them, and continues the remaining packages. If the bytes differ, increment
  every package and release a new version.
- If the workflow fails before any package publishes, fix the cause on `master` and create a release
  for the corrected, incremented version. Do not move an existing release tag to another commit.
- If only a dist-tag is wrong, correct the tag instead of republishing the package.
- Avoid unpublishing except for a genuine security or legal incident. Coordinate through the
  process in [`SECURITY.md`](../SECURITY.md).
