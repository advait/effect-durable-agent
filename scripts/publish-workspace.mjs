import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const packageRoots = [".", "packages/cloudflare", "packages/celld"];
const dryRun = process.argv.includes("--dry-run");
const temporaryRoot = mkdtempSync(join(tmpdir(), "eda-publish-"));

const run = (command, args, options = {}) =>
  execFileSync(command, args, {
    cwd: options.cwd ?? repositoryRoot,
    encoding: "utf8",
    stdio: options.stdio ?? ["ignore", "pipe", "inherit"],
  });

const sha512 = (path) => createHash("sha512").update(readFileSync(path)).digest("base64");

const registryIntegrity = (name, version) => {
  try {
    return JSON.parse(
      run("npm", ["view", `${name}@${version}`, "dist.integrity", "--json"], {
        stdio: ["ignore", "pipe", "pipe"],
      }),
    );
  } catch (error) {
    const status = error?.status;
    const stderr = typeof error?.stderr === "string" ? error.stderr : "";
    const stdout = typeof error?.stdout === "string" ? error.stdout : "";
    if (status === 1 && (stderr.includes("E404") || stdout.includes('"code": "E404"'))) {
      return undefined;
    }
    throw error;
  }
};

try {
  for (const packageRoot of packageRoots) {
    const cwd = join(repositoryRoot, packageRoot);
    const manifest = JSON.parse(readFileSync(join(cwd, "package.json"), "utf8"));
    const packResult = JSON.parse(
      run(
        "pnpm",
        ["--config.ignore-scripts=true", "pack", "--json", "--pack-destination", temporaryRoot],
        { cwd },
      ),
    );
    const packed = Array.isArray(packResult) ? packResult[0] : packResult;
    const localIntegrity = `sha512-${sha512(packed.filename)}`;
    const publishedIntegrity = registryIntegrity(manifest.name, manifest.version);

    if (publishedIntegrity !== undefined) {
      if (publishedIntegrity !== localIntegrity) {
        throw new Error(
          `${manifest.name}@${manifest.version} already exists with different bytes; increment every workspace package version`,
        );
      }
      process.stdout.write(`Verified existing ${manifest.name}@${manifest.version}; continuing.\n`);
      continue;
    }

    const args = [
      "publish",
      packed.filename,
      "--access",
      "public",
      "--tag",
      manifest.publishConfig.tag,
    ];
    if (dryRun) {
      args.push("--dry-run");
    }
    run("npm", args, { stdio: "inherit" });
  }
} finally {
  rmSync(temporaryRoot, { force: true, recursive: true });
}
