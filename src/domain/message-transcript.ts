import * as Prompt from "effect/unstable/ai/Prompt";

import type { ContextProjection } from "./context-projection";
import type { MessageRecord, ReducedState, ToolCallRecord } from "./reduced-state";
import type { UserMessageContent } from "../types/commands";

/** Durable user/assistant message records suitable for a session transcript. */
export type DurableTranscriptMessage =
  | Extract<MessageRecord, { readonly _tag: "User" | "Assistant" | "AssistantPartial" }>
  | (Extract<MessageRecord, { readonly _tag: "Steering" }> & {
      readonly consumedSeq: NonNullable<
        Extract<MessageRecord, { readonly _tag: "Steering" }>["consumedSeq"]
      >;
    });

/** Derive the durable user/assistant transcript in committed sequence order. */
export const durableMessageTranscript = (
  state: Pick<ReducedState, "messages">,
): ReadonlyArray<DurableTranscriptMessage> =>
  Array.from(state.messages.values()).filter(isDurableTranscriptMessage).sort(compareBySeq);

const isDurableTranscriptMessage = (message: MessageRecord): message is DurableTranscriptMessage =>
  (message._tag === "User" && message.cancelledSeq === undefined) ||
  message._tag === "Assistant" ||
  message._tag === "AssistantPartial" ||
  (message._tag === "Steering" && message.consumedSeq !== undefined);

type SystemContextMessage = Extract<MessageRecord, { readonly _tag: "System" }>;
type PromptContextMessage = DurableTranscriptMessage | SystemContextMessage;
type PromptProjectionState = Pick<ReducedState, "messages"> &
  Partial<Pick<ReducedState, "toolCalls">>;

type AssistantPromptInfo = {
  readonly order: number;
  readonly toolCallOrderByProviderPartId: ReadonlyMap<string, number>;
};

type PromptTranscriptItem =
  | {
      readonly _tag: "Message";
      readonly order: number;
      readonly message: PromptContextMessage;
    }
  | {
      readonly _tag: "Summary";
      readonly order: number;
      readonly message: Prompt.UserMessage;
    }
  | {
      readonly _tag: "ToolResult";
      readonly order: number;
      readonly part: Prompt.ToolResultPart;
    };

type ToolResultPromptTranscriptItem = Extract<
  PromptTranscriptItem,
  { readonly _tag: "ToolResult" }
>;

/** Convert all durable transcript messages and exact tool-result parts into provider prompt history. */
export const durableTranscriptPrompt = (state: PromptProjectionState): Prompt.Prompt =>
  contextProjectionPrompt(state, undefined);

/** Convert the current compacted context into provider prompt history. */
export const contextProjectionPrompt = (
  state: PromptProjectionState,
  context: ContextProjection | undefined,
): Prompt.Prompt => {
  const messages = promptTranscriptItems(state, context).flatMap(
    (item): ReadonlyArray<Prompt.Message> => {
      switch (item._tag) {
        case "Message":
          return [transcriptMessageToPromptMessage(item.message)];
        case "Summary":
          return [item.message];
        case "ToolResult":
          return [Prompt.makeMessage("tool", { content: [item.part] })];
      }
    },
  );
  validateToolCallPairing(messages);
  return Prompt.fromMessages(messages);
};

/** Build a provider prompt with extra user content selected by the scheduler but not yet folded. */
export const durableTranscriptPromptWithUserContent = (
  state: PromptProjectionState,
  content: UserMessageContent,
): Prompt.Prompt => contextProjectionPromptWithUserContent(state, undefined, content);

/** Build a compacted provider prompt with extra user content selected by the scheduler. */
export const contextProjectionPromptWithUserContent = (
  state: PromptProjectionState,
  context: ContextProjection | undefined,
  content: UserMessageContent,
): Prompt.Prompt =>
  Prompt.fromMessages([
    ...contextProjectionPrompt(state, context).content,
    Prompt.makeMessage("user", { content }),
  ]);

/** Build a compacted provider prompt with multiple scheduler-selected user messages. */
export const contextProjectionPromptWithUserContents = (
  state: PromptProjectionState,
  context: ContextProjection | undefined,
  contents: ReadonlyArray<UserMessageContent>,
): Prompt.Prompt =>
  Prompt.fromMessages([
    ...contextProjectionPrompt(state, context).content,
    ...contents.map((content) => Prompt.makeMessage("user", { content })),
  ]);

const compareBySeq = (left: DurableTranscriptMessage, right: DurableTranscriptMessage): number =>
  messageTranscriptSeq(left) - messageTranscriptSeq(right) || left.seq - right.seq;

const promptTranscriptItems = (
  state: PromptProjectionState,
  context: ContextProjection | undefined,
): ReadonlyArray<PromptTranscriptItem> => {
  const messages = promptContextMessages(state, context);
  const assistantInfoByInference = new Map(
    messages.flatMap((message) =>
      message._tag === "Assistant" || message._tag === "AssistantPartial"
        ? [[message.inferenceId, assistantPromptInfo(message)] as const]
        : [],
    ),
  );
  const messageItems = messages.map((message) => ({
    _tag: "Message" as const,
    order: promptContextMessageSeq(message),
    message,
  }));
  const summaryItem =
    context?.currentSummary === undefined
      ? []
      : [
          {
            _tag: "Summary" as const,
            order: Number.MIN_SAFE_INTEGER,
            message: context.currentSummary.promptMessage,
          },
        ];
  const toolItems = Array.from(state.toolCalls?.values() ?? []).flatMap((tool) =>
    toolPromptItems(tool, assistantInfoByInference),
  );
  return [...messageItems, ...summaryItem, ...toolItems].sort(
    (left, right) => left.order - right.order,
  );
};

const toolPromptItems = (
  tool: ToolCallRecord,
  assistantInfoByInference: ReadonlyMap<unknown, AssistantPromptInfo>,
): ReadonlyArray<ToolResultPromptTranscriptItem> => {
  const { decision } = tool;
  if (decision === undefined) {
    return [];
  }
  const assistant = assistantInfoByInference.get(decision.inferenceId);
  if (assistant === undefined) {
    return [];
  }
  if (decision._tag === "Rejected") {
    return decision.promptPart === undefined
      ? []
      : [
          {
            _tag: "ToolResult",
            order: syntheticToolResultOrder(assistant, decision.providerPartId),
            part: decision.promptPart,
          },
        ];
  }
  const terminal = tool.terminal;
  if (terminal === undefined || terminal.promptPart === undefined) {
    return [];
  }
  return [
    {
      _tag: "ToolResult",
      order: syntheticToolResultOrder(assistant, decision.providerPartId),
      part: terminal.promptPart,
    },
  ];
};

const assistantPromptInfo = (
  message: Extract<PromptContextMessage, { readonly _tag: "Assistant" | "AssistantPartial" }>,
): AssistantPromptInfo => ({
  order: messageTranscriptSeq(message),
  toolCallOrderByProviderPartId: new Map(
    assistantToolCallParts(message).map(({ part, index }) => [part.id, index] as const),
  ),
});

const assistantToolCallParts = (
  message: Extract<PromptContextMessage, { readonly _tag: "Assistant" | "AssistantPartial" }>,
): ReadonlyArray<{ readonly part: Prompt.ToolCallPart; readonly index: number }> =>
  (message.promptParts ?? []).flatMap((part, index) =>
    part.type === "tool-call" ? [{ part, index }] : [],
  );

const syntheticToolResultOrder = (assistant: AssistantPromptInfo, providerPartId: string): number =>
  assistant.order +
  0.1 +
  (assistant.toolCallOrderByProviderPartId.get(providerPartId) ?? 0) * 0.001;

const validateToolCallPairing = (messages: ReadonlyArray<Prompt.Message>): void => {
  const pending = new Set<string>();
  for (const message of messages) {
    if (pending.size > 0 && message.role !== "tool") {
      throw new Error(unpairedToolCallMessage(pending));
    }
    if (message.role === "tool") {
      for (const part of message.content) {
        if (part.type === "tool-result") {
          pending.delete(part.id);
        }
      }
      continue;
    }
    if (message.role === "assistant") {
      for (const part of message.content) {
        if (part.type === "tool-call" && !part.providerExecuted) {
          pending.add(part.id);
        }
      }
    }
  }
  if (pending.size > 0) {
    throw new Error(unpairedToolCallMessage(pending));
  }
};

const unpairedToolCallMessage = (pending: ReadonlySet<string>): string =>
  `Prompt transcript contains assistant tool-call(s) without matching tool result(s): ${Array.from(
    pending,
  ).join(", ")}`;

const promptContextMessages = (
  state: Pick<ReducedState, "messages">,
  context: ContextProjection | undefined,
): ReadonlyArray<PromptContextMessage> => {
  const retainedFromContextSeq = context?.currentSummary?.retainedFromContextSeq ?? 0;
  const messages = durableMessageTranscript(state).filter(
    (message) =>
      isConsumedForPrompt(message) && messageTranscriptSeq(message) >= retainedFromContextSeq,
  );
  const system = Array.from(state.messages.values()).find(
    (message): message is SystemContextMessage => message._tag === "System",
  );
  return system === undefined ? messages : [system, ...messages];
};

const isConsumedForPrompt = (message: DurableTranscriptMessage): boolean =>
  message._tag !== "User" ||
  message.requestedDisposition === undefined ||
  message.consumedSeq !== undefined;

const transcriptMessageToPromptMessage = (
  message: PromptContextMessage,
): Prompt.SystemMessage | Prompt.UserMessage | Prompt.AssistantMessage => {
  switch (message._tag) {
    case "System":
      return Prompt.makeMessage("system", { content: message.content });
    case "User":
    case "Steering":
      return Prompt.makeMessage("user", { content: message.content });
    case "Assistant":
    case "AssistantPartial":
      return Prompt.makeMessage("assistant", {
        content: message.promptParts ?? assistantParts(message.content),
      });
  }
};

const messageTranscriptSeq = (message: DurableTranscriptMessage): number =>
  message._tag === "User" || message._tag === "Steering"
    ? (message.consumedSeq ?? message.seq)
    : message.seq;

const promptContextMessageSeq = (message: PromptContextMessage): number =>
  message._tag === "System" ? Number.NEGATIVE_INFINITY : messageTranscriptSeq(message);

const assistantParts = (
  content: Extract<
    DurableTranscriptMessage,
    { readonly _tag: "Assistant" | "AssistantPartial" }
  >["content"],
): ReadonlyArray<Prompt.AssistantMessagePart> => [
  ...(content.reasoning === undefined ? [] : [Prompt.reasoningPart({ text: content.reasoning })]),
  ...(content.text.length === 0 ? [] : [Prompt.textPart({ text: content.text })]),
];
