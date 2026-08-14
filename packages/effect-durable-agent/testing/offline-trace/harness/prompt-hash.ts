import * as Effect from "effect/Effect";
import * as Prompt from "effect/unstable/ai/Prompt";

import { stableJsonStringify, toJsonValue, type JsonValue } from "../json";

/** Stable prompt representation and hash used for cache-prefix verification. */
export interface CanonicalPrompt {
  readonly messages: JsonValue;
  readonly json: string;
  readonly sha256: string;
}

/** Convert a Prompt.RawInput into the stable model-facing message JSON used by trace verification. */
export const canonicalPrompt = (input: Prompt.RawInput): Effect.Effect<CanonicalPrompt> =>
  Effect.gen(function* () {
    const messages = toJsonValue(Prompt.make(input).content);
    const json = stableJsonStringify(messages);
    const sha256 = yield* sha256Hex(json);
    return { messages, json, sha256 };
  });

/** Compute a hex SHA-256 digest with Web Crypto so this helper remains runtime-neutral. */
export const sha256Hex = (input: string): Effect.Effect<string> =>
  Effect.promise(async () => {
    const bytes = new TextEncoder().encode(input);
    const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
    return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join(
      "",
    );
  });
