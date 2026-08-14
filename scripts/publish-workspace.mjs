import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  readlinkSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const packageRoots = [
  "packages/effect-durable-agent",
  "packages/effect-durable-agent-cloudflare",
  "packages/effect-durable-agent-celld",
];
const dryRun = process.argv.includes("--dry-run");
const temporaryRoot = mkdtempSync(join(tmpdir(), "eda-publish-"));

const run = (command, args, options = {}) =>
  execFileSync(command, args, {
    cwd: options.cwd ?? repositoryRoot,
    encoding: "utf8",
    stdio: options.stdio ?? ["ignore", "pipe", "inherit"],
  });

const canonicalizeJson = (value) => {
  if (Array.isArray(value)) {
    return value.map(canonicalizeJson);
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonicalizeJson(value[key])]),
    );
  }
  return value;
};

const packageTreeIntegrity = (root) => {
  const hash = createHash("sha512");
  const visit = (directory, prefix) => {
    for (const entry of readdirSync(directory, { withFileTypes: true }).sort((a, b) =>
      a.name.localeCompare(b.name),
    )) {
      const relativePath = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
      const path = join(directory, entry.name);
      const stat = lstatSync(path);
      if (entry.isDirectory()) {
        visit(path, relativePath);
      } else if (entry.isSymbolicLink()) {
        hash.update(`link\0${relativePath}\0${stat.mode & 0o777}\0${readlinkSync(path)}\0`);
      } else if (entry.isFile()) {
        const content =
          relativePath === "package/package.json"
            ? Buffer.from(JSON.stringify(canonicalizeJson(JSON.parse(readFileSync(path, "utf8")))))
            : readFileSync(path);
        hash.update(`file\0${relativePath}\0${stat.mode & 0o777}\0${content.length}\0`);
        hash.update(content);
      }
    }
  };
  visit(root, "");
  return hash.digest("base64");
};

const unpack = (archive, destination) => {
  mkdirSync(destination, { recursive: true });
  run("tar", ["-xzf", archive, "-C", destination]);
};

const packedFilename = (packResult, destination) => {
  const packed = Array.isArray(packResult) ? packResult[0] : packResult;
  return isAbsolute(packed.filename) ? packed.filename : join(destination, packed.filename);
};

const registryVersion = (name, version) => {
  try {
    return JSON.parse(
      run("npm", ["view", `${name}@${version}`, "version", "--json"], {
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
    const localPackResult = JSON.parse(
      run(
        "pnpm",
        ["--config.ignore-scripts=true", "pack", "--json", "--pack-destination", temporaryRoot],
        { cwd },
      ),
    );
    const localArchive = packedFilename(localPackResult, temporaryRoot);
    const publishedVersion = registryVersion(manifest.name, manifest.version);

    if (publishedVersion !== undefined) {
      const packageDirectory = manifest.name.replaceAll("/", "-");
      const registryPackDirectory = join(temporaryRoot, `${packageDirectory}-registry`);
      mkdirSync(registryPackDirectory, { recursive: true });
      const registryPackResult = JSON.parse(
        run(
          "npm",
          [
            "pack",
            `${manifest.name}@${manifest.version}`,
            "--ignore-scripts",
            "--json",
            "--pack-destination",
            registryPackDirectory,
          ],
          { stdio: ["ignore", "pipe", "pipe"] },
        ),
      );
      const registryArchive = packedFilename(registryPackResult, registryPackDirectory);
      const localExtracted = join(temporaryRoot, `${packageDirectory}-local-extracted`);
      const registryExtracted = join(temporaryRoot, `${packageDirectory}-registry-extracted`);
      unpack(localArchive, localExtracted);
      unpack(registryArchive, registryExtracted);

      if (packageTreeIntegrity(localExtracted) !== packageTreeIntegrity(registryExtracted)) {
        throw new Error(
          `${manifest.name}@${manifest.version} already exists with different contents; increment every workspace package version`,
        );
      }
      process.stdout.write(`Verified existing ${manifest.name}@${manifest.version}; continuing.\n`);
      continue;
    }

    const args = [
      "publish",
      localArchive,
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
