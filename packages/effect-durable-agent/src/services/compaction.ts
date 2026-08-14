import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Schema from "effect/Schema";
import * as LanguageModel from "effect/unstable/ai/LanguageModel";

import type { ContextProjection } from "../domain/context-projection";
import {
  durableMessageTranscript,
  type DurableTranscriptMessage,
} from "../domain/message-transcript";
import {
  CompactionExecutorId,
  CompactionPolicyId,
  CompactionSummaryArtifact,
} from "../domain/context-projection";
import type { ReducedState } from "../domain/reduced-state";
import { InferenceId, ContextVersion, SequenceNumber } from "../types/core";
import { FailurePayload, type AssistantMessageContent } from "../types/events";
import { CommittedDurableEvent, DurableAppendEntry, EDASessionStoreError } from "./session-store";
import { EventFactory } from "./event-factory";
import { IdGenerator } from "./id-generator";
import { EDAKeepAlive } from "./keep-alive";
import { annotateEdaSpan, committedBatchAttributes } from "./tracing";

/** Candidate durable range and retained cursor selected by compaction policy. */
export const CompactionPlan = Schema.Struct({
  policyId: CompactionPolicyId,
  sourceFromSeq: SequenceNumber,
  sourceToSeq: SequenceNumber,
  retainedFromContextSeq: SequenceNumber,
});
export type CompactionPlan = typeof CompactionPlan.Type;

/** Rough token heuristic used by the built-in compaction presets. */
export const defaultCompactionCharsPerToken = 4;

/** Production-oriented context threshold for the built-in approximate-token policy. */
export const defaultCompactionThresholdTokens = 220_000;

/** Recent model-facing tail retained verbatim by the built-in approximate-token policy. */
export const defaultCompactionRetainTailTokens = 40_000;

/** Minimum newly summarizable prefix needed before the approximate-token policy plans work. */
export const defaultCompactionMinSummarizableTokens = 10_000;

/** Approximate-token policy options for common production compaction. */
export interface ApproximateTokenThresholdCompactionPolicyOptions {
  /** Stable policy id stored on summary artifacts. */
  readonly policyId?: string;
  /** Context-size threshold that triggers compaction. Defaults to 220k tokens. */
  readonly thresholdTokens?: number;
  /** Recent tail retained verbatim after compaction. Defaults to 40k tokens. */
  readonly retainTailTokens?: number;
  /** Smallest prefix worth summarizing. Defaults to 10k tokens. */
  readonly minSummarizableTokens?: number;
  /** Character-to-token divisor for the approximate estimator. Defaults to 4. */
  readonly charsPerToken?: number;
}

/** LanguageModel-backed summary executor options. */
export interface LanguageModelSummaryCompactionExecutorOptions {
  /** Stable executor id stored on summary artifacts. */
  readonly executorId?: string;
  /** Prefix used for the synthetic prompt message inserted into future context. */
  readonly summaryMessagePrefix?: string;
}

/** Input visible to policy decisions. */
export interface CompactionPolicyInput {
  readonly state: ReducedState;
  readonly context: ContextProjection;
}

/** Result produced by the summary executor before the framework commits it. */
export const CompactionOutput = Schema.Struct({
  text: Schema.NonEmptyString,
  promptMessage: Prompt.UserMessage,
  executorId: Schema.optionalKey(CompactionExecutorId),
});
export type CompactionOutput = typeof CompactionOutput.Type;

/** Executor input with framework-minted lifecycle identities. */
export interface CompactionExecutorInput extends CompactionPolicyInput {
  readonly plan: CompactionPlan;
}

/** Typed failure from policy planning or summary generation. */
export class CompactionError extends Schema.TaggedErrorClass<CompactionError>()("CompactionError", {
  message: Schema.String,
}) {}

/** Service contract for deciding whether the current context should be compacted. */
export interface CompactionPolicyShape {
  readonly id: CompactionPolicyId;
  readonly plan: (
    input: CompactionPolicyInput,
  ) => Effect.Effect<CompactionPlan | undefined, CompactionError>;
}

/** Decides whether a safe prompt-context rebase should be attempted. */
export class CompactionPolicy extends Context.Service<CompactionPolicy, CompactionPolicyShape>()(
  "@effect-durable-agent/CompactionPolicy",
) {
  /** Default policy: compaction is disabled and commits no skip events. */
  static readonly Disabled = Layer.succeed(CompactionPolicy, {
    id: CompactionPolicyId.make("disabled"),
    plan: () => Effect.succeed(undefined),
  } satisfies CompactionPolicyShape);

  /** Test/app hook for replacing the policy with a concrete planner. */
  static readonly FromFunction = (
    id: string,
    plan: CompactionPolicyShape["plan"],
  ): Layer.Layer<CompactionPolicy> =>
    Layer.succeed(CompactionPolicy, {
      id: CompactionPolicyId.make(id),
      plan,
    });

  /**
   * Common production policy: compact when approximate prompt context exceeds a token threshold.
   *
   * The policy keeps a recent tail verbatim, summarizes the older prefix, and advances from the
   * current summary cursor so repeated compactions behave like the first one.
   */
  static readonly ApproximateTokenThreshold = (
    options: ApproximateTokenThresholdCompactionPolicyOptions = {},
  ): Layer.Layer<CompactionPolicy> => {
    const normalized = normalizeApproximateTokenPolicyOptions(options);
    return Layer.succeed(CompactionPolicy, {
      id: normalized.policyId,
      plan: ({ state, context }) =>
        Effect.sync(() => planApproximateTokenCompaction(state, context, normalized)),
    });
  };
}

/** Service contract for turning a selected compaction range into a summary artifact. */
export interface CompactionExecutorShape {
  readonly id: CompactionExecutorId;
  readonly execute: (
    input: CompactionExecutorInput,
  ) => Effect.Effect<CompactionOutput, CompactionError>;
}

/** Produces a cumulative summary artifact for a selected compaction plan. */
export class CompactionExecutor extends Context.Service<
  CompactionExecutor,
  CompactionExecutorShape
>()("@effect-durable-agent/CompactionExecutor") {
  /** Placeholder executor for the disabled default policy. It should never be called. */
  static readonly Disabled = Layer.succeed(CompactionExecutor, {
    id: CompactionExecutorId.make("disabled"),
    execute: () => Effect.die(new Error("CompactionExecutor.Disabled was called")),
  } satisfies CompactionExecutorShape);

  /** Test/app hook for replacing summary generation. */
  static readonly FromFunction = (
    id: string,
    execute: CompactionExecutorShape["execute"],
  ): Layer.Layer<CompactionExecutor> =>
    Layer.succeed(CompactionExecutor, {
      id: CompactionExecutorId.make(id),
      execute,
    });

  /** Summary executor backed by the configured Effect AI LanguageModel. */
  static readonly LanguageModelSummary = (
    options: LanguageModelSummaryCompactionExecutorOptions = {},
  ): Layer.Layer<CompactionExecutor, never, LanguageModel.LanguageModel> =>
    Layer.effect(
      CompactionExecutor,
      Effect.gen(function* () {
        const languageModel = yield* LanguageModel.LanguageModel;
        const executorId = CompactionExecutorId.make(
          options.executorId ?? "language-model-summary",
        );
        const summaryMessagePrefix = options.summaryMessagePrefix ?? "Conversation summary:";
        return {
          id: executorId,
          execute: Effect.fn("agent.compaction")(function* (input) {
            yield* annotateEdaSpan({
              "eda.compaction.policy_id": input.plan.policyId,
              "eda.compaction.source_from_seq": input.plan.sourceFromSeq,
              "eda.compaction.source_to_seq": input.plan.sourceToSeq,
              "eda.compaction.retained_from_context_seq": input.plan.retainedFromContextSeq,
              "eda.compaction.executor_id": executorId,
            });
            const promptText = buildSummaryPrompt(input);
            if (promptText === undefined) {
              return yield* new CompactionError({
                message: "compaction plan selected no source messages",
              });
            }
            const response = yield* languageModel
              .generateText({
                prompt: Prompt.fromMessages([
                  Prompt.makeMessage("system", { content: summarizationSystemPrompt }),
                  Prompt.makeMessage("user", {
                    content: [Prompt.textPart({ text: promptText })],
                  }),
                ]),
                toolChoice: "none",
              })
              .pipe(
                Effect.mapError(
                  (error) =>
                    new CompactionError({
                      message: `summary model failed: ${formatUnknownError(error)}`,
                    }),
                ),
              );
            const text = response.text.trim();
            if (text.length === 0) {
              return yield* new CompactionError({ message: "summary model returned no text" });
            }
            return CompactionOutput.make({
              text,
              promptMessage: Prompt.makeMessage("user", {
                content: [Prompt.textPart({ text: `${summaryMessagePrefix}\n\n${text}` })],
              }),
              executorId,
            });
          }),
        } satisfies CompactionExecutorShape;
      }),
    );
}

/** Inputs needed by the framework-owned compaction lifecycle runner. */
export interface CompactionRunnerInput {
  readonly state: ReducedState;
  readonly context: ContextProjection;
  readonly appendDurableEntries: (
    entries: ReadonlyArray<DurableAppendEntry>,
  ) => Effect.Effect<ReadonlyArray<CommittedDurableEvent>, EDASessionStoreError>;
}

/** Runtime service that executes framework-owned compaction lifecycles. */
export interface CompactionRunnerShape {
  /** Evaluate policy and run one compaction if a plan is selected. */
  readonly maybeCompact: (
    input: CompactionRunnerInput,
  ) => Effect.Effect<ReadonlyArray<CommittedDurableEvent>, EDASessionStoreError>;
}

/** Framework-owned compaction lifecycle runner. */
export class CompactionRunner extends Context.Service<CompactionRunner, CompactionRunnerShape>()(
  "@effect-durable-agent/CompactionRunner",
) {
  static readonly Live = Layer.effect(
    CompactionRunner,
    Effect.suspend(() => makeLiveCompactionRunner),
  );
}

interface NormalizedApproximateTokenPolicyOptions {
  readonly policyId: CompactionPolicyId;
  readonly thresholdTokens: number;
  readonly retainTailTokens: number;
  readonly minSummarizableTokens: number;
  readonly charsPerToken: number;
}

interface EstimatedContextMessage {
  readonly chars: number;
  readonly message: DurableTranscriptMessage;
  readonly seq: SequenceNumber;
}

const makeLiveCompactionRunner = Effect.gen(function* () {
  const ids = yield* IdGenerator;
  const events = yield* EventFactory;
  const policy = yield* CompactionPolicy;
  const executor = yield* CompactionExecutor;
  const keepAlive = yield* EDAKeepAlive;

  const maybeCompact = Effect.fnUntraced(function* (input: CompactionRunnerInput) {
    const plan = yield* policy
      .plan({ state: input.state, context: input.context })
      .pipe(
        Effect.catchTag("CompactionError", (error) =>
          Effect.logError("compaction policy failed", { error }).pipe(Effect.as(undefined)),
        ),
      );
    if (plan === undefined) {
      yield* annotateEdaSpan({ "eda.compaction.selected": false });
      return [];
    }
    yield* annotateEdaSpan({
      "eda.compaction.selected": true,
      "eda.compaction.policy_id": plan.policyId,
      "eda.compaction.source_from_seq": plan.sourceFromSeq,
      "eda.compaction.source_to_seq": plan.sourceToSeq,
      "eda.compaction.retained_from_context_seq": plan.retainedFromContextSeq,
    });
    const invalidPlan = validatePlan(plan, input.state, input.context);
    if (invalidPlan !== undefined) {
      yield* Effect.logError("compaction policy returned invalid plan", { reason: invalidPlan });
      return [];
    }

    const compactionId = yield* ids.makeCompactionId();
    const summaryId = yield* ids.makeSummaryId();
    yield* annotateEdaSpan({
      "eda.compaction.id": compactionId,
      "eda.summary.id": summaryId,
    });
    const requested = yield* events.compactionRequested({
      compactionId,
      sourceFromSeq: plan.sourceFromSeq,
      sourceToSeq: plan.sourceToSeq,
    });
    const started = yield* events.compactionStarted({ compactionId });
    const startedEvents = yield* input.appendDurableEntries([
      { event: requested },
      { event: started },
    ]);

    const output = yield* keepAlive
      .withActiveWork(
        `compaction:${compactionId}`,
        executor.execute({ plan, state: input.state, context: input.context }),
      )
      .pipe(
        Effect.catchTag("CompactionError", (error) =>
          Effect.gen(function* () {
            const failed = yield* events.compactionFailed({
              compactionId,
              error: FailurePayload.make({ message: error.message }),
            });
            const committed = yield* input.appendDurableEntries([{ event: failed }]);
            return { _tag: "Failed" as const, committed };
          }),
        ),
        Effect.map((result) =>
          "_tag" in result ? result : { _tag: "Output" as const, output: result },
        ),
      );

    if (output._tag === "Failed") {
      return [...startedEvents, ...output.committed];
    }

    const artifact = CompactionSummaryArtifact.make({
      compactionId,
      summaryId,
      ...(input.context.currentSummary === undefined
        ? {}
        : { previousSummaryId: input.context.currentSummary.summaryId }),
      sourceFromSeq: plan.sourceFromSeq,
      sourceToSeq: plan.sourceToSeq,
      retainedFromContextSeq: plan.retainedFromContextSeq,
      text: output.output.text,
      promptMessage: output.output.promptMessage,
      policyId: plan.policyId,
      ...(output.output.executorId === undefined ? {} : { executorId: output.output.executorId }),
    });
    const summaryCreated = yield* events.summaryCreated({
      compactionId,
      summaryId,
      sourceFromSeq: plan.sourceFromSeq,
      sourceToSeq: plan.sourceToSeq,
      summary: artifact,
    });
    const contextRebased = yield* events.contextRebased({
      compactionId,
      summaryId,
      contextVersion: ContextVersion.make(input.context.contextVersion + 1),
      retainedFromContextSeq: plan.retainedFromContextSeq,
    });
    const completed = yield* events.compactionCompleted({ compactionId });
    const committed = yield* input.appendDurableEntries([
      { event: summaryCreated },
      { event: contextRebased },
      { event: completed },
    ]);
    yield* annotateEdaSpan(committedBatchAttributes([...startedEvents, ...committed]));
    return [...startedEvents, ...committed];
  });

  return { maybeCompact } satisfies CompactionRunnerShape;
});

const validatePlan = (
  plan: CompactionPlan,
  state: ReducedState,
  context: ContextProjection,
): string | undefined => {
  const currentRetainedFromSeq =
    context.currentSummary?.retainedFromContextSeq ?? SequenceNumber.make(0);
  if (plan.retainedFromContextSeq <= currentRetainedFromSeq) {
    return "retainedFromContextSeq must advance beyond the current summary cursor";
  }
  if (plan.sourceToSeq > state.lastSeq) {
    return "sourceToSeq must not exceed the current durable head";
  }
  if (plan.retainedFromContextSeq > state.lastSeq + 1) {
    return "retainedFromContextSeq must not skip unsummarized future context";
  }
  if (plan.sourceFromSeq > plan.sourceToSeq) {
    return "sourceFromSeq must be less than or equal to sourceToSeq";
  }
  if (plan.retainedFromContextSeq <= plan.sourceToSeq) {
    return "retainedFromContextSeq must start after the summarized source range";
  }
  return undefined;
};

const normalizeApproximateTokenPolicyOptions = (
  options: ApproximateTokenThresholdCompactionPolicyOptions,
): NormalizedApproximateTokenPolicyOptions => ({
  policyId: CompactionPolicyId.make(options.policyId ?? "approximate-token-threshold"),
  thresholdTokens: positiveNumber(
    options.thresholdTokens ?? defaultCompactionThresholdTokens,
    "thresholdTokens",
  ),
  retainTailTokens: nonNegativeNumber(
    options.retainTailTokens ?? defaultCompactionRetainTailTokens,
    "retainTailTokens",
  ),
  minSummarizableTokens: nonNegativeNumber(
    options.minSummarizableTokens ?? defaultCompactionMinSummarizableTokens,
    "minSummarizableTokens",
  ),
  charsPerToken: positiveNumber(
    options.charsPerToken ?? defaultCompactionCharsPerToken,
    "charsPerToken",
  ),
});

const positiveNumber = (value: number, name: string): number => {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive finite number`);
  }
  return value;
};

const nonNegativeNumber = (value: number, name: string): number => {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${name} must be a non-negative finite number`);
  }
  return value;
};

/** Plan one Pi-style approximate-token compaction while keeping a recent tail verbatim. */
const planApproximateTokenCompaction = (
  state: ReducedState,
  context: ContextProjection,
  options: NormalizedApproximateTokenPolicyOptions,
): CompactionPlan | undefined => {
  const messages = estimatedContextMessages(state);
  const totalChars = estimateCurrentPromptChars(state, context, messages);
  if (totalChars <= options.thresholdTokens * options.charsPerToken) {
    return undefined;
  }

  const currentRetainedFromSeq =
    context.currentSummary?.retainedFromContextSeq ?? SequenceNumber.make(0);
  const retainedFromSeq = selectRetainedFromSeq(messages, state.lastSeq, options);
  if (retainedFromSeq <= currentRetainedFromSeq) {
    return undefined;
  }

  const sourceMessages = messages.filter((message) => message.seq < retainedFromSeq);
  if (sourceMessages.length === 0) {
    return undefined;
  }

  const summarizableTokens = charsToTokens(
    sourceMessages.reduce((sum, message) => sum + message.chars, 0),
    options.charsPerToken,
  );
  if (summarizableTokens < options.minSummarizableTokens) {
    return undefined;
  }

  const sourceFromSeq =
    currentRetainedFromSeq > 0 ? currentRetainedFromSeq : sourceMessages[0]!.seq;
  const sourceToSeq = SequenceNumber.make(retainedFromSeq - 1);
  if (sourceFromSeq > sourceToSeq) {
    return undefined;
  }

  return CompactionPlan.make({
    policyId: options.policyId,
    sourceFromSeq,
    sourceToSeq,
    retainedFromContextSeq: retainedFromSeq,
  });
};

const selectRetainedFromSeq = (
  messages: ReadonlyArray<EstimatedContextMessage>,
  lastSeq: SequenceNumber,
  options: NormalizedApproximateTokenPolicyOptions,
): SequenceNumber => {
  if (options.retainTailTokens === 0) {
    return SequenceNumber.make(lastSeq + 1);
  }

  const retainTailChars = options.retainTailTokens * options.charsPerToken;
  let tailChars = 0;
  let retainedIndex = messages.length;
  for (let index = messages.length - 1; index >= 0; index--) {
    tailChars += messages[index]!.chars;
    retainedIndex = index;
    if (tailChars >= retainTailChars) {
      break;
    }
  }
  return retainedIndex >= messages.length
    ? SequenceNumber.make(lastSeq + 1)
    : messages[retainedIndex]!.seq;
};

const estimateCurrentPromptChars = (
  state: ReducedState,
  context: ContextProjection,
  messages: ReadonlyArray<EstimatedContextMessage>,
): number => {
  const systemChars = Array.from(state.messages.values()).reduce(
    (sum, message) => (message._tag === "System" ? sum + message.content.length : sum),
    0,
  );
  const summaryChars =
    context.currentSummary === undefined
      ? 0
      : estimatePromptMessageChars(context.currentSummary.promptMessage);
  const messageChars = messages.reduce((sum, message) => sum + message.chars, 0);
  return systemChars + summaryChars + messageChars;
};

const estimatedContextMessages = (state: ReducedState): ReadonlyArray<EstimatedContextMessage> =>
  durableMessageTranscript(state).map((message) => ({
    message,
    seq: messageSeq(message),
    chars: estimateDurableMessageChars(message) + estimateAssociatedToolChars(message, state),
  }));

const estimateDurableMessageChars = (message: DurableTranscriptMessage): number => {
  switch (message._tag) {
    case "User":
    case "Steering":
      return estimateUserContentChars(message.content);
    case "Assistant":
    case "AssistantPartial":
      return estimateAssistantPromptChars(message.promptParts, message.content);
  }
};

const estimateAssociatedToolChars = (
  message: DurableTranscriptMessage,
  state: ReducedState,
): number => {
  if (message._tag !== "Assistant" && message._tag !== "AssistantPartial") {
    return 0;
  }
  return toolPromptPartsForInference(message.inferenceId, state.toolCalls).reduce(
    (sum, part) => sum + estimatePromptPartChars(part),
    0,
  );
};

const messageSeq = (message: DurableTranscriptMessage): SequenceNumber =>
  message._tag === "Steering" ? message.consumedSeq : message.seq;

const charsToTokens = (chars: number, charsPerToken: number): number =>
  Math.ceil(chars / charsPerToken);

const buildSummaryPrompt = (input: CompactionExecutorInput): string | undefined => {
  const sourceText = serializeCompactionSource(input);
  if (sourceText.trim().length === 0) {
    return undefined;
  }

  const previousSummary =
    input.context.currentSummary === undefined
      ? ""
      : `<previous-summary>\n${input.context.currentSummary.text}\n</previous-summary>\n\n`;
  return `${previousSummary}<conversation>\n${sourceText}\n</conversation>\n\n${summaryPromptText}`;
};

const serializeCompactionSource = (input: CompactionExecutorInput): string => {
  const lines: Array<string> = [];
  for (const message of durableMessageTranscript(input.state)) {
    const seq = messageSeq(message);
    if (seq < input.plan.sourceFromSeq || seq > input.plan.sourceToSeq) {
      continue;
    }
    lines.push(serializeDurableMessage(message));
    if (message._tag === "Assistant" || message._tag === "AssistantPartial") {
      for (const part of toolPromptPartsForInference(message.inferenceId, input.state.toolCalls)) {
        lines.push(serializeToolResultPart(part));
      }
    }
  }
  return lines.filter((line) => line.trim().length > 0).join("\n\n");
};

const serializeDurableMessage = (message: DurableTranscriptMessage): string => {
  switch (message._tag) {
    case "User":
      return `[User]: ${userContentText(message.content)}`;
    case "Steering":
      return `[User steering]: ${userContentText(message.content)}`;
    case "Assistant":
    case "AssistantPartial":
      return serializeAssistantMessage(message.promptParts, message.content);
  }
};

const serializeAssistantMessage = (
  promptParts: ReadonlyArray<Prompt.AssistantMessagePart> | undefined,
  content: AssistantMessageContent,
): string => {
  const parts = promptParts ?? assistantContentToPromptParts(content);
  const text: Array<string> = [];
  const reasoning: Array<string> = [];
  const toolCalls: Array<string> = [];
  const sideband: Array<string> = [];
  for (const part of parts) {
    switch (part.type) {
      case "text":
        text.push(part.text);
        break;
      case "reasoning":
        reasoning.push(part.text);
        break;
      case "tool-call":
        toolCalls.push(`${part.name}(${safeJsonStringify(part.params)})`);
        break;
      case "file":
        sideband.push(`file(${part.fileName ?? "unnamed"}, ${part.mediaType})`);
        break;
      case "tool-result":
        sideband.push(
          `tool-result(${part.name}, ${truncateForSummary(safeJsonStringify(part.result))})`,
        );
        break;
      case "tool-approval-request":
        sideband.push(`tool-approval-request(${part.approvalId}, ${part.toolCallId})`);
        break;
    }
  }
  return [
    reasoning.length === 0 ? "" : `[Assistant reasoning]: ${reasoning.join("\n")}`,
    text.length === 0 ? "" : `[Assistant]: ${text.join("\n")}`,
    toolCalls.length === 0 ? "" : `[Assistant tool calls]: ${toolCalls.join("; ")}`,
    sideband.length === 0 ? "" : `[Assistant context]: ${sideband.join("; ")}`,
  ]
    .filter((line) => line.length > 0)
    .join("\n");
};

const serializeToolResultPart = (part: Prompt.ToolResultPart): string =>
  `[Tool result ${part.name}${part.isFailure ? " failed" : ""}]: ${truncateForSummary(
    safeJsonStringify(part.result),
  )}`;

const toolPromptPartsForInference = (
  inferenceId: InferenceId,
  toolCalls: ReducedState["toolCalls"],
): ReadonlyArray<Prompt.ToolResultPart> =>
  Array.from(toolCalls.values()).flatMap((tool): ReadonlyArray<Prompt.ToolResultPart> => {
    const { decision } = tool;
    if (decision === undefined || decision.inferenceId !== inferenceId) {
      return [];
    }
    if (decision._tag === "Rejected") {
      return decision.promptPart === undefined ? [] : [decision.promptPart];
    }
    const terminal = tool.terminal;
    if (terminal === undefined || terminal.promptPart === undefined) {
      return [];
    }
    return [terminal.promptPart];
  });

const assistantContentToPromptParts = (
  content: AssistantMessageContent,
): ReadonlyArray<Prompt.AssistantMessagePart> => [
  ...(content.reasoning === undefined ? [] : [Prompt.reasoningPart({ text: content.reasoning })]),
  ...(content.text.length === 0 ? [] : [Prompt.textPart({ text: content.text })]),
];

const estimateAssistantPromptChars = (
  promptParts: ReadonlyArray<Prompt.AssistantMessagePart> | undefined,
  content: AssistantMessageContent,
): number =>
  (promptParts ?? assistantContentToPromptParts(content)).reduce(
    (sum, part) => sum + estimatePromptPartChars(part),
    0,
  );

const estimateUserContentChars = (
  content: string | ReadonlyArray<Prompt.UserMessagePart>,
): number =>
  typeof content === "string"
    ? content.length
    : content.reduce((sum, part) => sum + estimatePromptPartChars(part), 0);

const estimatePromptMessageChars = (message: Prompt.Message): number => {
  if (!("content" in message)) {
    return 0;
  }
  const { content } = message;
  return typeof content === "string"
    ? content.length
    : content.reduce((sum, part) => sum + estimatePromptPartChars(part), 0);
};

type PromptContentPart =
  | Prompt.UserMessagePart
  | Prompt.AssistantMessagePart
  | Prompt.ToolMessagePart;

const estimatePromptPartChars = (part: PromptContentPart): number => {
  switch (part.type) {
    case "text":
      return part.text.length;
    case "reasoning":
      return part.text.length;
    case "file":
      return (
        part.mediaType.length +
        (part.fileName?.length ?? 0) +
        (typeof part.data === "string"
          ? part.data.length
          : part.data instanceof URL
            ? part.data.toString().length
            : estimatedBinaryFileChars)
      );
    case "tool-call":
      return part.name.length + safeJsonStringify(part.params).length;
    case "tool-result":
      return part.name.length + safeJsonStringify(part.result).length;
    case "tool-approval-response":
      return part.approvalId.length + (part.reason?.length ?? 0) + 8;
    case "tool-approval-request":
      return part.approvalId.length + part.toolCallId.length;
    default:
      return safeJsonStringify(part).length;
  }
};

const userContentText = (content: string | ReadonlyArray<Prompt.UserMessagePart>): string =>
  typeof content === "string"
    ? content
    : content
        .map((part) => {
          switch (part.type) {
            case "text":
              return part.text;
            case "file":
              return `[file ${part.fileName ?? "unnamed"} ${part.mediaType}]`;
          }
        })
        .join("\n");

const safeJsonStringify = (value: unknown): string => {
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
};

const truncateForSummary = (text: string, maxChars = 2_000): string => {
  if (text.length <= maxChars) {
    return text;
  }
  return `${text.slice(0, maxChars)}\n\n[... ${text.length - maxChars} more characters truncated]`;
};

const formatUnknownError = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

const estimatedBinaryFileChars = 4_800;

const summarizationSystemPrompt =
  "You are a context summarization assistant. Do not continue the conversation. Only output the requested structured summary.";

const summaryPromptText = `The messages above are conversation history selected for compaction. Create a cumulative context checkpoint summary that another LLM will use to continue the work.

Use this exact structure:

## Goal
[What the user is trying to accomplish]

## Constraints & Preferences
- [Requirements, constraints, and preferences]

## Progress
### Done
- [x] [Completed work]

### In Progress
- [ ] [Current unfinished work]

### Blocked
- [Current blockers, or "(none)"]

## Key Decisions
- **[Decision]**: [Rationale]

## Next Steps
1. [Concrete next step]

## Critical Context
- [Important facts, data, IDs, file paths, function names, and exact errors needed to continue]

Rules:
- If <previous-summary> is present, preserve important existing facts and update it with the new conversation.
- Keep the summary concise but sufficient to replace the compacted history.
- Preserve exact file paths, tool names, identifiers, and error messages.
- Do not answer questions from the conversation.`;
