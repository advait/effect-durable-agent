import * as Schema from "effect/Schema";
import { NonNegativeInt, UsagePayload } from "../types/events/envelope";

/** Additive provider usage categories; reasoning and text are breakdowns of output. */
export const TokenUsageTotals = Schema.Struct({
  inputTokens: NonNegativeInt,
  cachedInputTokens: NonNegativeInt,
  cacheWriteInputTokens: NonNegativeInt,
  outputTokens: NonNegativeInt,
  textTokens: NonNegativeInt,
  reasoningTokens: NonNegativeInt,
});
export type TokenUsageTotals = typeof TokenUsageTotals.Type;

/** Model attribution survives compaction. Plain records are required by Cloudflare RPC structured cloning. */
export const ModelTokenUsage = Schema.Struct({
  provider: Schema.NonEmptyString,
  modelId: Schema.NonEmptyString,
  usage: TokenUsageTotals,
});
export type ModelTokenUsage = typeof ModelTokenUsage.Type;

/** One bounded bucket per distinct model used by a session. Dollars belong to the host. */
export const TokenConsumptionState = Schema.Struct({ byModel: Schema.Array(ModelTokenUsage) });
export type TokenConsumptionState = typeof TokenConsumptionState.Type;

export const emptyTokenUsage: TokenUsageTotals = {
  inputTokens: 0,
  cachedInputTokens: 0,
  cacheWriteInputTokens: 0,
  outputTokens: 0,
  textTokens: 0,
  reasoningTokens: 0,
};

/** Fold an observed call into its model bucket; omitted provider counters add no known usage. */
export const addModelUsage = (
  state: TokenConsumptionState,
  model: { readonly provider: string; readonly modelId: string },
  usage: UsagePayload | undefined,
): TokenConsumptionState => {
  if (usage === undefined) return state;
  const existing = state.byModel.find(
    (entry) => entry.provider === model.provider && entry.modelId === model.modelId,
  );
  const previous = existing?.usage ?? emptyTokenUsage;
  const next = ModelTokenUsage.make({
    ...model,
    usage: {
      inputTokens: previous.inputTokens + (usage.inputTokens ?? 0),
      cachedInputTokens: previous.cachedInputTokens + (usage.cachedInputTokens ?? 0),
      cacheWriteInputTokens: previous.cacheWriteInputTokens + (usage.cacheWriteInputTokens ?? 0),
      outputTokens: previous.outputTokens + (usage.outputTokens ?? 0),
      textTokens: previous.textTokens + (usage.textTokens ?? 0),
      reasoningTokens: previous.reasoningTokens + (usage.reasoningTokens ?? 0),
    },
  });
  return {
    byModel:
      existing === undefined
        ? [...state.byModel, next]
        : state.byModel.map((entry) => (entry === existing ? next : entry)),
  };
};

/** Totals are a derived display value, never an input to multi-model pricing. */
export const totalTokenUsage = (state: TokenConsumptionState): TokenUsageTotals =>
  state.byModel.reduce(
    (total, entry) => ({
      inputTokens: total.inputTokens + entry.usage.inputTokens,
      cachedInputTokens: total.cachedInputTokens + entry.usage.cachedInputTokens,
      cacheWriteInputTokens: total.cacheWriteInputTokens + entry.usage.cacheWriteInputTokens,
      outputTokens: total.outputTokens + entry.usage.outputTokens,
      textTokens: total.textTokens + entry.usage.textTokens,
      reasoningTokens: total.reasoningTokens + entry.usage.reasoningTokens,
    }),
    emptyTokenUsage,
  );
