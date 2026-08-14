import { execFileSync } from "node:child_process";
import { cpSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const fixtureRoot = join(repositoryRoot, "testing", "package-consumer");
const packageRoots = [
  join(repositoryRoot, "packages/effect-durable-agent"),
  join(repositoryRoot, "packages/effect-durable-agent-cloudflare"),
  join(repositoryRoot, "packages/effect-durable-agent-celld"),
  join(repositoryRoot, "packages/effect-durable-agent-rivet"),
];
const temporaryRoot = mkdtempSync(join(tmpdir(), "effect-durable-agent-packages-"));
const consumerRoot = join(temporaryRoot, "consumer");
const unsupportedNpmConfig = new Set([
  "npm_config__jsr_registry",
  "npm_config_npm_globalconfig",
  "npm_config_verify_deps_before_run",
]);
const childEnvironment = Object.fromEntries(
  Object.entries(process.env).filter(([key]) => !unsupportedNpmConfig.has(key.toLowerCase())),
);

const run = (command, args, cwd) =>
  execFileSync(command, args, {
    cwd,
    encoding: "utf8",
    env: childEnvironment,
    stdio: ["ignore", "pipe", "inherit"],
  });

const packAndValidate = (packageRoot) => {
  const packOutput = run(
    "pnpm",
    ["--config.ignore-scripts=true", "pack", "--json", "--pack-destination", temporaryRoot],
    packageRoot,
  );
  const parsedPackOutput = JSON.parse(packOutput);
  const packResult = Array.isArray(parsedPackOutput) ? parsedPackOutput[0] : parsedPackOutput;
  if (packResult === undefined) {
    throw new Error(`npm pack did not return metadata for ${packageRoot}`);
  }

  const packageJson = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8"));
  const packedFiles = new Set(packResult.files.map(({ path }) => path));
  for (const path of ["LICENSE", "README.md", "dist/index.js", "dist/index.d.ts", "package.json"]) {
    if (!packedFiles.has(path)) {
      throw new Error(`${packageJson.name} artifact is missing ${path}`);
    }
  }

  const forbiddenPrefixes = ["node_modules/", "patches/", "scripts/", "src/", "testing/"];
  for (const path of packedFiles) {
    if (path === "testing/offline-trace/README.md") {
      continue;
    }
    const forbiddenPrefix = forbiddenPrefixes.find((prefix) => path.startsWith(prefix));
    if (forbiddenPrefix !== undefined) {
      throw new Error(`${packageJson.name} artifact unexpectedly contains ${path}`);
    }
  }

  for (const [subpath, target] of Object.entries(packageJson.exports)) {
    if (typeof target === "string") {
      continue;
    }
    for (const condition of ["types", "import"]) {
      const path = target[condition]?.replace(/^\.\//, "");
      if (path === undefined || !packedFiles.has(path)) {
        throw new Error(`${packageJson.name} export ${subpath} has no packed ${condition} target`);
      }
    }
  }

  const tarballPath = packResult.filename;
  const packedManifest = JSON.parse(run("tar", ["-xOf", tarballPath, "package/package.json"]));
  for (const dependencyGroup of ["dependencies", "peerDependencies"]) {
    for (const [name, version] of Object.entries(packedManifest[dependencyGroup] ?? {})) {
      if (
        packageRoots.some(
          (root) => JSON.parse(readFileSync(join(root, "package.json"), "utf8")).name === name,
        ) &&
        version !== packageJson.version
      ) {
        throw new Error(
          `${packageJson.name} packed ${dependencyGroup}.${name} as ${version}, expected ${packageJson.version}`,
        );
      }
    }
  }

  process.stdout.write(
    `Validated ${packageJson.name} (${packResult.files.length} files, ${statSync(tarballPath).size} packed bytes).\n`,
  );
  return tarballPath;
};

try {
  const tarballs = packageRoots.map(packAndValidate);
  cpSync(fixtureRoot, consumerRoot, { recursive: true });
  run(
    "npm",
    ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--package-lock=false", ...tarballs],
    consumerRoot,
  );
  run("npm", ["run", "typecheck"], consumerRoot);
  run("npm", ["run", "bundle"], consumerRoot);
  run("npm", ["run", "check:rivet"], consumerRoot);
  run("npm", ["run", "check:tooling"], consumerRoot);

  process.stdout.write(
    `Validated isolated consumption of ${tarballs.map((tarball) => basename(tarball)).join(", ")}.\n`,
  );
} finally {
  rmSync(temporaryRoot, { force: true, recursive: true });
}
