import { TranscriptVersion } from "./records";

/**
 * EDA's transcript semantic revision.
 *
 * Bump it deliberately whenever the interpreter, the legacy-attempt normalizer, the record
 * decoders or the mapping executor would interpret any existing log differently. A bump changes
 * every transcript version, so every session rebuilds and every host must run its backfill.
 * A new durable event type needs a bump only if this release gives it transcript meaning;
 * events the interpreter ignores leave the revision and every version unchanged.
 *
 * The CI fingerprint over a fixed scenario corpus fails when interpretation output changes and
 * this constant does not; update the recorded fingerprint together with the bump.
 */
export const transcriptSemanticRevision = 1;

/**
 * Deterministic JSON: object keys sorted by code unit, no whitespace. Only JSON values are
 * accepted, so equal data always produces equal bytes.
 */
export const canonicalJson = (value: unknown): string => {
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(`canonicalJson cannot encode non-finite number ${value}`);
    }
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }
  if (typeof value === "object") {
    const entries = Object.entries(value).filter(([, entry]) => entry !== undefined);
    entries.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
    return `{${entries.map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`).join(",")}}`;
  }
  throw new Error(`canonicalJson cannot encode ${typeof value}`);
};

/** Transcript version for a semantic revision and already-encoded canonical mapping data. */
export const computeTranscriptVersion = (
  semanticRevision: number,
  encodedMappings: unknown,
): TranscriptVersion =>
  TranscriptVersion.make(sha256Hex(canonicalJson({ semanticRevision, mappings: encodedMappings })));

// SHA-256 (FIPS 180-4). Synchronous so version identity is available inside synchronous storage
// transactions and at module construction; Web Crypto digests are asynchronous only.
const roundConstants = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

const rotateRight = (value: number, bits: number): number =>
  (value >>> bits) | (value << (32 - bits));

// Typed-array reads are always in bounds here; the fallback only satisfies index typing.
const at = (words: Uint32Array, index: number): number => words[index] ?? 0;

/** Lowercase hex SHA-256 of a string's UTF-8 bytes. */
export const sha256Hex = (input: string): string => {
  const message = new TextEncoder().encode(input);
  const bitLength = message.length * 8;
  const paddedLength = Math.ceil((message.length + 9) / 64) * 64;
  const padded = new Uint8Array(paddedLength);
  padded.set(message);
  padded[message.length] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(paddedLength - 8, Math.floor(bitLength / 0x1_0000_0000));
  view.setUint32(paddedLength - 4, bitLength >>> 0);

  const hash = new Uint32Array([
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
  ]);
  const schedule = new Uint32Array(64);
  for (let offset = 0; offset < paddedLength; offset += 64) {
    for (let index = 0; index < 16; index += 1) {
      schedule[index] = view.getUint32(offset + index * 4);
    }
    for (let index = 16; index < 64; index += 1) {
      const w15 = at(schedule, index - 15);
      const w2 = at(schedule, index - 2);
      const s0 = rotateRight(w15, 7) ^ rotateRight(w15, 18) ^ (w15 >>> 3);
      const s1 = rotateRight(w2, 17) ^ rotateRight(w2, 19) ^ (w2 >>> 10);
      schedule[index] = at(schedule, index - 16) + s0 + at(schedule, index - 7) + s1;
    }
    const working = Uint32Array.from(hash);
    for (let index = 0; index < 64; index += 1) {
      const [a, b, c, d, e, f, g, h] = Array.from(working);
      const s1 = rotateRight(e ?? 0, 6) ^ rotateRight(e ?? 0, 11) ^ rotateRight(e ?? 0, 25);
      const choice = ((e ?? 0) & (f ?? 0)) ^ (~(e ?? 0) & (g ?? 0));
      const t1 = (h ?? 0) + s1 + choice + at(roundConstants, index) + at(schedule, index);
      const s0 = rotateRight(a ?? 0, 2) ^ rotateRight(a ?? 0, 13) ^ rotateRight(a ?? 0, 22);
      const majority = ((a ?? 0) & (b ?? 0)) ^ ((a ?? 0) & (c ?? 0)) ^ ((b ?? 0) & (c ?? 0));
      // Uint32Array stores modulo 2^32, which is the algorithm's addition.
      working.set([
        t1 + s0 + majority,
        a ?? 0,
        b ?? 0,
        c ?? 0,
        (d ?? 0) + t1,
        e ?? 0,
        f ?? 0,
        g ?? 0,
      ]);
    }
    for (let index = 0; index < 8; index += 1) {
      hash[index] = at(hash, index) + at(working, index);
    }
  }
  return Array.from(hash, (word) => word.toString(16).padStart(8, "0")).join("");
};
