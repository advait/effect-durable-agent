# Releasing Effect Durable Agent

Releases publish [`effect-durable-agent`](https://www.npmjs.com/package/effect-durable-agent) from
the public [`advait/effect-durable-agent`](https://github.com/advait/effect-durable-agent)
repository.

## Current public baseline

`0.1.0-alpha.1` was published interactively as the package bootstrap and is marked by the annotated
Git tag `v0.1.0-alpha.1`. Both the `alpha` and `latest` npm dist-tags currently resolve to that
version.

The bootstrap is complete and must not be repeated. In particular, do not create a GitHub release
for `v0.1.0-alpha.1`: publishing that release would run the npm workflow against a version that
already exists. The Git tag alone does not trigger the workflow.

## One-time trusted-publisher setup

Before publishing the next version, configure the package's trusted publisher in the npm package
settings:

- Provider: GitHub Actions
- Organization or user: `advait`
- Repository: `effect-durable-agent`
- Workflow filename: `publish.yml`
- Allowed action: `npm publish`
- Environment: leave blank unless `.github/workflows/publish.yml` is updated to use one

The workflow uses a GitHub-hosted runner, grants `id-token: write`, and runs a recent Node/npm pair.
Do not add an npm publishing token to the repository. npm uses the workflow's short-lived OIDC
identity and automatically attaches provenance because both the repository and package are public.

After one automated release succeeds, configure npm publishing access to require two-factor
authentication and disallow tokens, then revoke any automation tokens that are no longer needed.
See npm's [trusted publishing guide](https://docs.npmjs.com/trusted-publishers/) for the current
registry requirements.

## Release channels

While the API is in alpha, `publishConfig.tag` remains `alpha` and consumers should install
`effect-durable-agent@alpha`. It is acceptable for `latest` to point to an alpha release, but an
alpha-tagged publish advances only `alpha`; it does not automatically move `latest`.

If an alpha has been verified and should also become the default for unqualified installs, an
authenticated maintainer can move `latest` explicitly:

```bash
npm dist-tag add effect-durable-agent@0.1.0-alpha.2 latest
```

Trusted publishing authorizes `npm publish`, not `npm dist-tag`, so changing a dist-tag is an
interactive maintainer operation and may require two-factor authentication. Always name the exact
version being promoted.

For the first stable release, remove the prerelease suffix and change `publishConfig.tag` to
`latest` in the version PR. Leave `alpha` pointing to the final alpha unless there is a deliberate
reason to move or remove it.

## Release checklist

1. Choose a new, never-before-published version.
2. Update `package.json` and refresh `pnpm-lock.yaml`.
3. Add the release notes to `CHANGELOG.md`.
4. Run the complete release validation:

   ```bash
   pnpm install --frozen-lockfile
   pnpm run release:check
   ```

5. Merge the version PR and confirm `CI` passes on `master`.
6. Create a GitHub release targeting the merged commit. Its tag must be exactly
   `v<package version>`; for example, `v0.1.0-alpha.2`.
7. Wait for the `Publish to npm` workflow to pass. The workflow independently reruns the full
   release check and rejects a tag that does not match `package.json`.
8. Verify the registry metadata and dist-tags:

   ```bash
   npm view effect-durable-agent@0.1.0-alpha.2 name version repository.url dist.integrity --json
   npm dist-tag ls effect-durable-agent
   ```

9. Install from the public registry in a clean consumer and run its typecheck and bundle. Do not
   validate only against the repository or a local tarball.
10. If desired for the alpha channel, move `latest` only after the registry install succeeds.
11. Update downstream consumers, including the `goguardian/gia` submodule pin, in a separate PR.

## Failure handling

- npm package versions are immutable. Never try to reuse a version after any successful publish;
  increment the version and release again.
- If the workflow fails before publishing, fix the cause on `master` and create a release for the
  corrected, incremented version. Do not move an existing release tag to a different commit.
- If only a dist-tag is wrong, correct the tag instead of republishing the package.
- Avoid unpublishing except for a genuine security or legal incident. Coordinate through the
  process in [`SECURITY.md`](../SECURITY.md).
