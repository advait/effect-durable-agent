# Releasing Effect Durable Agent

Releases publish the public `effect-durable-agent` package from the standalone
`advait/effect-durable-agent` repository.

## One-time release setup

Before the first public release:

1. Make `advait/effect-durable-agent` public.
2. Confirm that the `CI` workflow passes on the default branch.
3. Protect the default branch and release tags as appropriate.
4. Confirm that `goguardian/gia` consumes this repository as a submodule pinned to an audited
   commit.

The package metadata and publishing workflow target this standalone repository.

## Bootstrap the npm package

npm requires a package to exist before it can be connected to a trusted publisher. The first
release is therefore a one-time interactive bootstrap:

```bash
pnpm install --frozen-lockfile
pnpm run release:check
npm publish
```

The package metadata makes the scoped package public and publishes this prerelease under the
`alpha` tag. This command requires an authenticated npm account with publishing rights and its
configured two-factor authentication.

After `0.1.0-alpha.1` exists, configure its npm trusted publisher:

- Provider: GitHub Actions
- Organization or user: `advait`
- Repository: `effect-durable-agent`
- Workflow filename: `publish.yml`
- Allowed action: `npm publish`
- Environment: leave blank unless the workflow is updated to use one

The workflow runs on a GitHub-hosted runner with the OIDC permissions and Node/npm versions required
by npm trusted publishing. No npm token should be added to the repository. Trusted publishing
automatically adds provenance when both the package and repository are public.

Keep prereleases on an explicit dist-tag such as `alpha` or `beta`. Change `publishConfig.tag` to
`latest` only when publishing a stable release.

## Subsequent releases

1. Update `package.json` to the intended version and refresh `pnpm-lock.yaml`.
2. Add release notes and run `pnpm run release:check`.
3. Merge the version change.
4. Create a GitHub release whose tag is exactly `v<package version>`.
5. Wait for the `Publish to npm` workflow and verify the version and provenance on npm.

The workflow rejects a release tag that does not exactly match `package.json`.
