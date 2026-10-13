import { stableJsonStringify, type JsonValue } from "../json";
import type { OfflineTracePromptArtifact } from "../harness/artifacts";
import { EDA_PROMPT_OPTIONS_KEY } from "../../../src/services/prompt-projector";

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
    (entry, index) =>
      stableJsonStringify(withoutCacheHint(entry)) ===
      stableJsonStringify(withoutCacheHint(value[index] ?? null)),
  );
};

/**
 * EDA's cache hint marks the current end of history and moves forward every turn; providers treat
 * moving cache breakpoints as prefix-neutral, so the comparison ignores it.
 */
const withoutCacheHint = (message: JsonValue): JsonValue => {
  if (!isJsonObject(message)) return message;
  const options = message["options"];
  if (options === undefined || !isJsonObject(options)) return message;
  const { [EDA_PROMPT_OPTIONS_KEY]: _hint, ...rest } = options;
  return { ...message, options: rest };
};

const isJsonObject = (value: JsonValue): value is { readonly [key: string]: JsonValue } =>
  typeof value === "object" && value !== null && !Array.isArray(value);
