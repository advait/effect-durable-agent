import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const packagePaths = [".", "packages/cloudflare", "packages/celld"];
const packages = packagePaths.map((path) => ({
  path,
  manifest: JSON.parse(readFileSync(join(repositoryRoot, path, "package.json"), "utf8")),
}));
const expectedVersion = packages[0].manifest.version;
const packageNames = new Set(packages.map(({ manifest }) => manifest.name));

for (const { path, manifest } of packages) {
  if (manifest.private === true) {
    throw new Error(`${path} must remain publishable`);
  }
  if (manifest.version !== expectedVersion) {
    throw new Error(
      `${manifest.name} version ${manifest.version} does not match ${expectedVersion}`,
    );
  }
  const expectedTag = expectedVersion.includes("-") ? "alpha" : "latest";
  if (manifest.publishConfig?.tag !== expectedTag) {
    throw new Error(`${manifest.name} must publish ${expectedVersion} with npm tag ${expectedTag}`);
  }

  for (const dependencyGroup of ["dependencies", "peerDependencies"]) {
    for (const [name, version] of Object.entries(manifest[dependencyGroup] ?? {})) {
      if (packageNames.has(name) && version !== "workspace:*") {
        throw new Error(
          `${manifest.name} ${dependencyGroup}.${name} must be workspace:* so packs pin ${expectedVersion}`,
        );
      }
    }
  }
}

process.stdout.write(
  `Validated ${packages.length} lockstep packages at ${expectedVersion}: ${Array.from(packageNames).join(", ")}\n`,
);
