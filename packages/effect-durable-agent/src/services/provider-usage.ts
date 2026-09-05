import type * as Response from "effect/unstable/ai/Response";
import { UsagePayload } from "../types/events";

/** Normalizes provider usage for both conversation and compaction calls. */
export const makeUsagePayload = (usage: Response.Usage) => {
  const uncachedInputTokens = nonNegativeInt(usage.inputTokens.uncached);
  const cachedInputTokens = nonNegativeInt(usage.inputTokens.cacheRead);
  const cacheWriteInputTokens = nonNegativeInt(usage.inputTokens.cacheWrite);
  const inputTokens =
    nonNegativeInt(usage.inputTokens.total) ??
    sumDefined([uncachedInputTokens, cachedInputTokens, cacheWriteInputTokens]);
  const textTokens = nonNegativeInt(usage.outputTokens.text);
  const reasoningTokens = nonNegativeInt(usage.outputTokens.reasoning);
  const outputTokens =
    nonNegativeInt(usage.outputTokens.total) ?? sumDefined([textTokens, reasoningTokens]);

  return UsagePayload.make({
    ...(inputTokens === undefined ? {} : { inputTokens }),
    ...(cachedInputTokens === undefined ? {} : { cachedInputTokens }),
    ...(cacheWriteInputTokens === undefined ? {} : { cacheWriteInputTokens }),
    ...(outputTokens === undefined ? {} : { outputTokens }),
    ...(textTokens === undefined ? {} : { textTokens }),
    ...(reasoningTokens === undefined ? {} : { reasoningTokens }),
  });
};

const sumDefined = (values: ReadonlyArray<number | undefined>) => {
  const present = values.filter((value): value is number => value !== undefined);
  return present.length === 0 ? undefined : present.reduce((sum, value) => sum + value, 0);
};

const nonNegativeInt = (value: number | undefined) =>
  value === undefined || !Number.isFinite(value) ? undefined : Math.max(0, Math.trunc(value));
