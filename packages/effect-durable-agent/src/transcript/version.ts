import { sha256Hex } from "./hash";
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
 * A CI fingerprint over a fixed scenario corpus (added with the interpreter) fails when
 * interpretation output changes and this constant does not; re-record it together with the bump.
 *
 * History: 1 — the initial interpreter (0.1.0-alpha.22). 2 — submissions consumed by one turn are
 * ordered by submission, as the prompt orders them, not by the turn's input list.
 */
export const transcriptSemanticRevision = 2;

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
