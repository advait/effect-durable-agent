import { createHash } from "node:crypto";
import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import { gunzipSync } from "node:zlib";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export const celldVersion = "0.2.0";

const assets = {
  "darwin-arm64": {
    name: "celld-aarch64-apple-darwin.gz",
    sha256: "dc4084ad3416deb09779cf7aceb904c02209bbe67769f696b3ba3c01453c37ea",
  },
  "linux-arm64": {
    name: "celld-aarch64-unknown-linux-gnu.gz",
    sha256: "434abf9904dccd6b6369fa7a4a816038b367a6983e23a5d83ca2e6888202eac0",
  },
  "linux-x64": {
    name: "celld-x86_64-unknown-linux-gnu.gz",
    sha256: "90cd071633d4c8d7956b6828a76d434315c91972750cd82df510b0e5e383d0f7",
  },
};

const sha256 = (value) => createHash("sha256").update(value).digest("hex");

/** Download and verify the pinned celld binary used by host conformance tests. */
export const installCelld = async (
  destination = resolve(
    dirname(fileURLToPath(import.meta.url)),
    `../.artifacts/celld/v${celldVersion}/celld`,
  ),
) => {
  const configured = process.env.CELLD_BIN;
  if (configured !== undefined && configured.trim() !== "") {
    return resolve(configured);
  }

  const asset = assets[`${process.platform}-${process.arch}`];
  if (asset === undefined) {
    throw new Error(
      `celld v${celldVersion} has no test binary for ${process.platform}-${process.arch}`,
    );
  }

  const compressedDestination = `${destination}.gz`;
  try {
    const compressed = await readFile(compressedDestination);
    if (sha256(compressed) === asset.sha256) {
      await mkdir(dirname(destination), { recursive: true });
      await writeFile(destination, gunzipSync(compressed), { mode: 0o755 });
      await chmod(destination, 0o755);
      return destination;
    }
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }

  const url = `https://github.com/denoland/celld/releases/download/v${celldVersion}/${asset.name}`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to download ${url}: ${response.status} ${response.statusText}`);
  }
  const compressed = Buffer.from(await response.arrayBuffer());
  const actualDigest = sha256(compressed);
  if (actualDigest !== asset.sha256) {
    throw new Error(`celld checksum mismatch: expected ${asset.sha256}, received ${actualDigest}`);
  }

  await mkdir(dirname(destination), { recursive: true });
  await writeFile(compressedDestination, compressed);
  await writeFile(destination, gunzipSync(compressed), { mode: 0o755 });
  await chmod(destination, 0o755);
  return destination;
};

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.stdout.write(`${await installCelld()}\n`);
}
