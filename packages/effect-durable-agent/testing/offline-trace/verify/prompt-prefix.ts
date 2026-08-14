import { stableJsonStringify, type JsonValue } from "../json";
import type { OfflineTracePromptArtifact } from "../harness/artifacts";

/** Result of verifying prompt-cache prefix stability across model requests. */
export interface PromptPrefixVerification {
  readonly checked: number;
  readonly failures: ReadonlyArray<string>;
}

/** Verify every later prompt has the previous prompt's message list as an exact prefix. */
export const verifyPromptPrefixes = (
  prompts: ReadonlyArray<OfflineTracePromptArtifact>,
): PromptPrefixVerification => {
  const failures: Array<string> = [];
  for (let index = 1; index < prompts.length; index += 1) {
    const previous = prompts[index - 1]!;
    const current = prompts[index]!;
    if (!isJsonArrayPrefix(previous.prompt, current.prompt)) {
      failures.push(`prompt ${current.index} does not preserve prompt ${previous.index} as prefix`);
    }
  }
  return { checked: Math.max(0, prompts.length - 1), failures };
};

const isJsonArrayPrefix = (prefix: JsonValue, value: JsonValue): boolean => {
  if (!Array.isArray(prefix) || !Array.isArray(value) || prefix.length > value.length) {
    return false;
  }
  return prefix.every(
    (entry, index) => stableJsonStringify(entry) === stableJsonStringify(value[index]),
  );
};
