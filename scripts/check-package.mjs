import { execFileSync } from "node:child_process";
import { cpSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const fixtureRoot = join(packageRoot, "testing", "package-consumer");
const temporaryRoot = mkdtempSync(join(tmpdir(), "effect-durable-agent-package-"));
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

try {
  const packOutput = run(
    "npm",
    ["pack", "--json", "--ignore-scripts", "--pack-destination", temporaryRoot],
    packageRoot,
  );
  const [packResult] = JSON.parse(packOutput);
  if (packResult === undefined) {
    throw new Error("npm pack did not return package metadata.");
  }

  const packageJson = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8"));
  const packedFiles = new Set(packResult.files.map(({ path }) => path));
  const requiredFiles = [
    "LICENSE",
    "README.md",
    "dist/index.js",
    "dist/index.d.ts",
    "package.json",
  ];
  for (const path of requiredFiles) {
    if (!packedFiles.has(path)) {
      throw new Error(`Packed artifact is missing ${path}.`);
    }
  }

  const forbiddenPrefixes = [
    "node_modules/",
    "patches/",
    "scripts/",
    "src/",
    "testing/package-consumer/",
  ];
  for (const path of packedFiles) {
    const forbiddenPrefix = forbiddenPrefixes.find((prefix) => path.startsWith(prefix));
    if (forbiddenPrefix !== undefined) {
      throw new Error(`Packed artifact unexpectedly contains ${path}.`);
    }
  }

  for (const [subpath, target] of Object.entries(packageJson.exports)) {
    if (typeof target === "string") {
      continue;
    }
    for (const condition of ["types", "import"]) {
      const path = target[condition]?.replace(/^\.\//, "");
      if (path === undefined || !packedFiles.has(path)) {
        throw new Error(`Export ${subpath} has no packed ${condition} target.`);
      }
    }
  }

  cpSync(fixtureRoot, consumerRoot, { recursive: true });
  const tarballPath = join(temporaryRoot, packResult.filename);
  run(
    "npm",
    ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--package-lock=false", tarballPath],
    consumerRoot,
  );
  run("npm", ["run", "typecheck"], consumerRoot);
  run("npm", ["run", "bundle"], consumerRoot);
  run("npm", ["run", "check:tooling"], consumerRoot);

  process.stdout.write(
    `Validated ${basename(tarballPath)} (${packResult.entryCount} files, ${packResult.unpackedSize} bytes unpacked).\n`,
  );
} finally {
  rmSync(temporaryRoot, { force: true, recursive: true });
}
