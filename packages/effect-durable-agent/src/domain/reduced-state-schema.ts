import * as Prompt from "effect/unstable/ai/Prompt";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";
export {
  TokenConsumptionState as TokenConsumptionStateSchema,
  TokenUsageTotals as TokenUsageTotalsSchema,
} from "./model-usage";

import {
  CancelPendingMessageCommand,
  EDACommand,
  PromotePendingMessageCommand,
  ResumePendingMessagesCommand,
  StopTurnCommand,
  SubmitMessageCommand,
  UserMessageContent,
} from "../types/commands";
import {
  CommandId,
  InferenceId,
  MessageId,
  RunId,
  SequenceNumber,
  ToolCallId,
  TurnId,
} from "../types/core";
import {
  AssistantMessageContent,
  EDAEventTrace,
  EDARunTrace,
  FailurePayload,
  ProviderPartId,
  RecoveryContinuation,
  SystemPromptText,
  ToolName,
  UsagePayload,
  ModelSelectionPayload,
} from "../types/events";

const PromptFilePart = Schema.Struct(Prompt.FilePart.fields);
const PromptReasoningPart = Schema.Struct(Prompt.ReasoningPart.fields);
const PromptTextPart = Schema.Struct(Prompt.TextPart.fields);
const PromptToolApprovalRequestPart = Schema.Struct(Prompt.ToolApprovalRequestPart.fields);
const PromptToolCallPart = Schema.Struct(Prompt.ToolCallPart.fields);
const PromptToolResultPart = Schema.Struct(Prompt.ToolResultPart.fields);

const JsonPromptFilePartEncoded = Schema.Struct({
  ...PromptFilePart.fields,
  data: Schema.String,
});

const byteArrayDataUrl = (data: Uint8Array, mediaType: string): string => {
  let binary = "";
  for (const byte of data) {
    binary += String.fromCharCode(byte);
  }
  return `data:${mediaType};base64,${btoa(binary)}`;
};

/**
 * Canonical Effect Prompt file part with an explicit JSON string encoding.
 *
 * Strings pass through unchanged, URLs use their href, and bytes become a data
 * URL. Decoding produces the normal Prompt.FilePart type.
 */
export const JsonPromptFilePart = JsonPromptFilePartEncoded.pipe(
  Schema.decodeTo(
    Prompt.FilePart,
    SchemaTransformation.transform<Prompt.FilePartEncoded, typeof JsonPromptFilePartEncoded.Type>({
      decode: (part) => part,
      encode: (part) => ({
        ...part,
        "~effect/ai/Prompt/Part": "~effect/ai/Prompt/Part",
        data:
          typeof part.data === "string"
            ? part.data
            : part.data instanceof URL
              ? part.data.href
              : byteArrayDataUrl(part.data, part.mediaType),
        options: part.options ?? {},
      }),
    }),
  ),
);

export const JsonUserMessagePart = Schema.Union([PromptTextPart, JsonPromptFilePart]);
export const JsonUserMessageContent = Schema.Union([
  Prompt.ContentFromString,
  Schema.NonEmptyArray(JsonUserMessagePart),
]);

export const JsonAssistantMessagePart = Schema.Union([
  PromptTextPart,
  JsonPromptFilePart,
  PromptReasoningPart,
  PromptToolCallPart,
  PromptToolResultPart,
  PromptToolApprovalRequestPart,
]);

/**
 * Command schema for JSON snapshots.
 *
 * Every command except SubmitMessage is reused directly. SubmitMessage only
 * replaces canonical Prompt file data with its explicit JSON string projection.
 */
export const JsonEDACommand = Schema.Union([
  Schema.Struct({
    ...SubmitMessageCommand.fields,
    content: JsonUserMessageContent,
  }),
  StopTurnCommand,
  CancelPendingMessageCommand,
  PromotePendingMessageCommand,
  ResumePendingMessagesCommand,
]);

export const CommandTerminalSchema = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Completed"), seq: SequenceNumber }),
  Schema.Struct({
    _tag: Schema.Literal("Failed"),
    error: FailurePayload,
    seq: SequenceNumber,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Cancelled"),
    reason: Schema.String,
    seq: SequenceNumber,
  }),
]);

export const RunTerminalSchema = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Completed"), seq: SequenceNumber }),
  Schema.Struct({
    _tag: Schema.Literal("Failed"),
    error: FailurePayload,
    seq: SequenceNumber,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Interrupted"),
    reason: Schema.String,
    seq: SequenceNumber,
  }),
]);

export const TurnTerminalSchema = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("Completed"),
    seq: SequenceNumber,
    usage: Schema.optionalKey(UsagePayload),
  }),
  Schema.Struct({
    _tag: Schema.Literal("Failed"),
    error: FailurePayload,
    seq: SequenceNumber,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Stopped"),
    reason: Schema.String,
    seq: SequenceNumber,
  }),
]);

export const InferenceTerminalSchema = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("Completed"),
    seq: SequenceNumber,
    usage: Schema.optionalKey(UsagePayload),
  }),
  Schema.Struct({
    _tag: Schema.Literal("Failed"),
    error: FailurePayload,
    seq: SequenceNumber,
  }),
]);

const LifecycleTimingFields = {
  durationMs: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))),
  startedAtMs: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  terminalAtMs: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
};

export const CommandRecordSchema = Schema.Struct({
  admittedSeq: Schema.optionalKey(SequenceNumber),
  admissionTrace: Schema.optionalKey(EDAEventTrace),
  command: Schema.optionalKey(EDACommand),
  commandId: CommandId,
  startedSeq: Schema.optionalKey(SequenceNumber),
  terminal: Schema.optionalKey(CommandTerminalSchema),
});

export const JsonCommandRecordSchema = Schema.Struct({
  ...CommandRecordSchema.fields,
  command: Schema.optionalKey(JsonEDACommand),
});

export const RunRecordSchema = Schema.Struct({
  modelSelection: Schema.optionalKey(ModelSelectionPayload),
  ...LifecycleTimingFields,
  commandIds: Schema.Array(CommandId),
  runId: RunId,
  startedSeq: SequenceNumber,
  terminal: Schema.optionalKey(RunTerminalSchema),
  trace: EDARunTrace,
});

export const TurnRecordSchema = Schema.Struct({
  ...LifecycleTimingFields,
  runId: RunId,
  startedSeq: SequenceNumber,
  terminal: Schema.optionalKey(TurnTerminalSchema),
  turnId: TurnId,
});

export const InferenceRecordSchema = Schema.Struct({
  ...LifecycleTimingFields,
  inferenceId: InferenceId,
  runId: RunId,
  startedSeq: SequenceNumber,
  terminal: Schema.optionalKey(InferenceTerminalSchema),
  turnId: TurnId,
});

export const ToolDecisionSchema = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("Created"),
    inferenceId: InferenceId,
    params: Schema.Unknown,
    promptPart: Schema.optionalKey(PromptToolCallPart),
    providerExecuted: Schema.Boolean,
    providerPartId: ProviderPartId,
    runId: RunId,
    seq: SequenceNumber,
    toolName: ToolName,
    turnId: TurnId,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Rejected"),
    inferenceId: InferenceId,
    modelFeedback: Schema.String,
    promptPart: Schema.optionalKey(PromptToolResultPart),
    providerPartId: ProviderPartId,
    reason: Schema.Literals(["unknown-tool", "invalid-params"]),
    runId: RunId,
    seq: SequenceNumber,
    toolName: ToolName,
    turnId: TurnId,
  }),
]);

export const ToolTerminalSchema = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("Completed"),
    promptPart: Schema.optionalKey(PromptToolResultPart),
    result: Schema.Unknown,
    seq: SequenceNumber,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Failed"),
    error: FailurePayload,
    promptPart: Schema.optionalKey(PromptToolResultPart),
    seq: SequenceNumber,
  }),
]);

export const ToolCallRecordSchema = Schema.Struct({
  ...LifecycleTimingFields,
  decision: Schema.optionalKey(ToolDecisionSchema),
  startedSeq: Schema.optionalKey(SequenceNumber),
  terminal: Schema.optionalKey(ToolTerminalSchema),
  toolCallId: ToolCallId,
});

const MessageIdentityFields = {
  createdAtMs: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  messageId: MessageId,
  seq: SequenceNumber,
};

const PendingMessageStateFields = {
  cancellationReason: Schema.optionalKey(Schema.String),
  cancelledByCommandId: Schema.optionalKey(CommandId),
  cancelledSeq: Schema.optionalKey(SequenceNumber),
  consumedSeq: Schema.optionalKey(SequenceNumber),
  consumedTurnId: Schema.optionalKey(TurnId),
  pausedByCommandId: Schema.optionalKey(CommandId),
  pausedSeq: Schema.optionalKey(SequenceNumber),
  promotedSeq: Schema.optionalKey(SequenceNumber),
};

const messageRecordSchema = <UserContent extends Schema.Top, AssistantPart extends Schema.Top>(
  userContent: UserContent,
  assistantPart: AssistantPart,
) =>
  Schema.Union([
    Schema.Struct({
      ...MessageIdentityFields,
      _tag: Schema.Literal("System"),
      content: SystemPromptText,
    }),
    Schema.Struct({
      ...MessageIdentityFields,
      ...PendingMessageStateFields,
      _tag: Schema.Literal("User"),
      commandId: CommandId,
      content: userContent,
      disposition: Schema.optionalKey(Schema.Literals(["queue", "steer"])),
      requestedDisposition: Schema.optionalKey(Schema.Literals(["queue", "steer"])),
    }),
    Schema.Struct({
      ...MessageIdentityFields,
      ...PendingMessageStateFields,
      _tag: Schema.Literal("Steering"),
      commandId: CommandId,
      content: userContent,
      runId: RunId,
    }),
    Schema.Struct({
      ...MessageIdentityFields,
      _tag: Schema.Literal("Assistant"),
      content: AssistantMessageContent,
      imported: Schema.optionalKey(Schema.Literal(true)),
      inferenceId: InferenceId,
      promptParts: Schema.optionalKey(Schema.Array(assistantPart)),
      runId: RunId,
      turnId: TurnId,
    }),
    Schema.Struct({
      ...MessageIdentityFields,
      _tag: Schema.Literal("AssistantPartial"),
      content: AssistantMessageContent,
      inferenceId: InferenceId,
      promptParts: Schema.optionalKey(Schema.Array(assistantPart)),
      reason: Schema.String,
      runId: RunId,
      turnId: TurnId,
    }),
  ]);

export const MessageRecordSchema = messageRecordSchema(
  UserMessageContent,
  Schema.Union([
    PromptTextPart,
    PromptFilePart,
    PromptReasoningPart,
    PromptToolCallPart,
    PromptToolResultPart,
    PromptToolApprovalRequestPart,
  ]),
);

export const JsonMessageRecordSchema = messageRecordSchema(
  JsonUserMessageContent,
  JsonAssistantMessagePart,
);

export const RecoveryContinuationRecordSchema = Schema.Struct({
  ...RecoveryContinuation.fields,
  seq: SequenceNumber,
});
