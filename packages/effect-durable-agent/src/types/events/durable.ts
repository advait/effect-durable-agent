import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Prompt from "effect/unstable/ai/Prompt";

import { EDACommand, UserMessageContent } from "../commands";
import {
  BaseStateId,
  CommandId,
  CompactionId,
  ContextVersion,
  InferenceId,
  MessageId,
  RunId,
  SequenceNumber,
  SummaryId,
  ToolCallId,
  TurnId,
} from "../core";
import {
  DurableEventEnvelope,
  FailurePayload,
  ModelSelectionPayload,
  ProviderPartId,
  SystemPromptText,
  ToolName,
  UsagePayload,
  effectDurableAgentNamespace,
  makeEventType,
  schemaV1,
} from "./envelope";
import { durableEventSchema } from "./internal";
import { EDARunTrace, makeEDARunTrace } from "../tracing";

/** Establishes a session's execution policy before its first run; not a model-switch operation. */
export const sessionConfiguredEventType = makeEventType("SessionConfigured");
export const SessionConfiguredEvent = durableEventSchema(
  sessionConfiguredEventType,
  Schema.Struct({ modelSelection: ModelSelectionPayload }),
);
export type SessionConfiguredEvent = typeof SessionConfiguredEvent.Type;

/** Event type values for durable command lifecycle events. */
export const commandAdmittedEventType = makeEventType("CommandAdmitted");
export const commandStartedEventType = makeEventType("CommandStarted");
export const commandCompletedEventType = makeEventType("CommandCompleted");
export const commandFailedEventType = makeEventType("CommandFailed");
export const commandCancelledEventType = makeEventType("CommandCancelled");

/** Payload recorded when a command enters the durable session log. */
export const CommandAdmittedPayload = Schema.Struct({ command: EDACommand });
export type CommandAdmittedPayload = typeof CommandAdmittedPayload.Type;
export const CommandAdmittedEvent = durableEventSchema(
  commandAdmittedEventType,
  CommandAdmittedPayload,
);
export type CommandAdmittedEvent = typeof CommandAdmittedEvent.Type;

/** Payload recorded when command execution starts. */
export const CommandStartedPayload = Schema.Struct({ commandId: CommandId });
export type CommandStartedPayload = typeof CommandStartedPayload.Type;
export const CommandStartedEvent = durableEventSchema(
  commandStartedEventType,
  CommandStartedPayload,
);
export type CommandStartedEvent = typeof CommandStartedEvent.Type;

/** Payload recorded when command execution completes normally. */
export const CommandCompletedPayload = Schema.Struct({ commandId: CommandId });
export type CommandCompletedPayload = typeof CommandCompletedPayload.Type;
export const CommandCompletedEvent = durableEventSchema(
  commandCompletedEventType,
  CommandCompletedPayload,
);
export type CommandCompletedEvent = typeof CommandCompletedEvent.Type;

/** Payload recorded when command execution fails. */
export const CommandFailedPayload = Schema.Struct({ commandId: CommandId, error: FailurePayload });
export type CommandFailedPayload = typeof CommandFailedPayload.Type;
export const CommandFailedEvent = durableEventSchema(commandFailedEventType, CommandFailedPayload);
export type CommandFailedEvent = typeof CommandFailedEvent.Type;

/** Payload recorded when a command is cancelled before completion. */
export const CommandCancelledPayload = Schema.Struct({
  commandId: CommandId,
  reason: Schema.String,
});
export type CommandCancelledPayload = typeof CommandCancelledPayload.Type;
export const CommandCancelledEvent = durableEventSchema(
  commandCancelledEventType,
  CommandCancelledPayload,
);
export type CommandCancelledEvent = typeof CommandCancelledEvent.Type;

/** Durable event union for the command lifecycle. */
export const CommandEvent = Schema.Union([
  CommandAdmittedEvent,
  CommandStartedEvent,
  CommandCompletedEvent,
  CommandFailedEvent,
  CommandCancelledEvent,
]);
export type CommandEvent = typeof CommandEvent.Type;

/** Event type values for durable message lifecycle events. */
export const systemMessageCommittedEventType = makeEventType("SystemMessageCommitted");
export const userMessageCommittedEventType = makeEventType("UserMessageCommitted");
export const steeringMessageQueuedEventType = makeEventType("SteeringMessageQueued");
export const steeringMessageCancelledEventType = makeEventType("SteeringMessageCancelled");
export const userMessageSubmittedEventType = makeEventType("UserMessageSubmitted");
export const userMessagePromotedEventType = makeEventType("UserMessagePromoted");
export const userMessageCancelledEventType = makeEventType("UserMessageCancelled");
export const messageQueuePausedEventType = makeEventType("MessageQueuePaused");
export const pendingMessagesPausedEventType = makeEventType("PendingMessagesPaused");
export const assistantMessageCommittedEventType = makeEventType("AssistantMessageCommitted");
export const assistantMessageImportedEventType = makeEventType("AssistantMessageImported");
export const assistantPartialCommittedEventType = makeEventType("AssistantPartialCommitted");

/** Semantic assistant content derived from canonical prompt parts for query convenience. */
export const AssistantMessageContent = Schema.Struct({
  text: Schema.String,
  reasoning: Schema.optionalKey(Schema.String),
});
export type AssistantMessageContent = typeof AssistantMessageContent.Type;

/** Exact assistant-role prompt parts sent to the model for cache-faithful replay. */
export const AssistantPromptPart = Schema.Union([
  Prompt.TextPart,
  Prompt.FilePart,
  Prompt.ReasoningPart,
  Prompt.ToolCallPart,
  Prompt.ToolResultPart,
  Prompt.ToolApprovalRequestPart,
]);
export type AssistantPromptPart = typeof AssistantPromptPart.Type;

/** Exact non-empty assistant-role prompt content sent to the model. */
export const AssistantPromptParts = Schema.NonEmptyArray(AssistantPromptPart);
export type AssistantPromptParts = typeof AssistantPromptParts.Type;

/** Exact assistant tool-call prompt part with EDA-branded provider identity. */
export const ToolCallPromptPart = Schema.Struct({
  ...Prompt.ToolCallPart.fields,
  id: ProviderPartId,
  name: ToolName,
});
export type ToolCallPromptPart = typeof ToolCallPromptPart.Type;

/** Exact tool-role prompt part sent to the model with EDA-branded provider identity. */
export const ToolPromptPart = Schema.Struct({
  ...Prompt.ToolResultPart.fields,
  id: ProviderPartId,
  name: ToolName,
});
export type ToolPromptPart = typeof ToolPromptPart.Type;

/** Payload recorded when app instructions become durable system-message context. */
export const SystemMessageCommittedPayload = Schema.Struct({
  messageId: MessageId,
  content: SystemPromptText,
});
export type SystemMessageCommittedPayload = typeof SystemMessageCommittedPayload.Type;
export const SystemMessageCommittedEvent = durableEventSchema(
  systemMessageCommittedEventType,
  SystemMessageCommittedPayload,
);
export type SystemMessageCommittedEvent = typeof SystemMessageCommittedEvent.Type;

/** Payload recorded when a user message becomes durable session context. */
export const UserMessageCommittedPayload = Schema.Struct({
  commandId: CommandId,
  messageId: MessageId,
  content: UserMessageContent,
});
export type UserMessageCommittedPayload = typeof UserMessageCommittedPayload.Type;
export const UserMessageCommittedEvent = durableEventSchema(
  userMessageCommittedEventType,
  UserMessageCommittedPayload,
);
export type UserMessageCommittedEvent = typeof UserMessageCommittedEvent.Type;

/** Payload recorded when steering context is queued for a future turn. */
export const SteeringMessageQueuedPayload = Schema.Struct({
  commandId: CommandId,
  messageId: MessageId,
  runId: RunId,
  content: UserMessageContent,
});
export type SteeringMessageQueuedPayload = typeof SteeringMessageQueuedPayload.Type;
export const SteeringMessageQueuedEvent = durableEventSchema(
  steeringMessageQueuedEventType,
  SteeringMessageQueuedPayload,
);
export type SteeringMessageQueuedEvent = typeof SteeringMessageQueuedEvent.Type;

/** Payload recorded when queued steering will never be consumed by its target run. */
export const SteeringMessageCancelledPayload = Schema.Struct({
  messageId: MessageId,
  runId: RunId,
  reason: Schema.String,
});
export type SteeringMessageCancelledPayload = typeof SteeringMessageCancelledPayload.Type;
export const SteeringMessageCancelledEvent = durableEventSchema(
  steeringMessageCancelledEventType,
  SteeringMessageCancelledPayload,
);
export type SteeringMessageCancelledEvent = typeof SteeringMessageCancelledEvent.Type;

export const UserMessageDisposition = Schema.Literals(["queue", "steer"]);
export type UserMessageDisposition = typeof UserMessageDisposition.Type;

export const UserMessageSubmittedPayload = Schema.Struct({
  commandId: CommandId,
  messageId: MessageId,
  disposition: UserMessageDisposition,
  content: UserMessageContent,
});
export type UserMessageSubmittedPayload = typeof UserMessageSubmittedPayload.Type;
export const UserMessageSubmittedEvent = durableEventSchema(
  userMessageSubmittedEventType,
  UserMessageSubmittedPayload,
);
export type UserMessageSubmittedEvent = typeof UserMessageSubmittedEvent.Type;

export const UserMessagePromotedPayload = Schema.Struct({
  commandId: CommandId,
  messageId: MessageId,
  from: Schema.Literal("queue"),
  to: Schema.Literal("steer"),
});
export type UserMessagePromotedPayload = typeof UserMessagePromotedPayload.Type;
export const UserMessagePromotedEvent = durableEventSchema(
  userMessagePromotedEventType,
  UserMessagePromotedPayload,
);
export type UserMessagePromotedEvent = typeof UserMessagePromotedEvent.Type;

export const UserMessageCancellationReason = Schema.Literals([
  "edit",
  "user-cancel",
  "clear-paused-queue",
]);
export type UserMessageCancellationReason = typeof UserMessageCancellationReason.Type;
export const UserMessageCancelledPayload = Schema.Struct({
  commandId: CommandId,
  messageId: MessageId,
  reason: UserMessageCancellationReason,
});
export type UserMessageCancelledPayload = typeof UserMessageCancelledPayload.Type;
export const UserMessageCancelledEvent = durableEventSchema(
  userMessageCancelledEventType,
  UserMessageCancelledPayload,
);
export type UserMessageCancelledEvent = typeof UserMessageCancelledEvent.Type;

/** Legacy V1 pause fact retained for replay compatibility. */
export const MessageQueuePausedPayload = Schema.Struct({
  stopCommandId: CommandId,
  runId: RunId,
  messageIds: Schema.Array(MessageId),
  reason: Schema.Literal("user-interrupted"),
});
export type MessageQueuePausedPayload = typeof MessageQueuePausedPayload.Type;
export const MessageQueuePausedEvent = durableEventSchema(
  messageQueuePausedEventType,
  MessageQueuePausedPayload,
);
export type MessageQueuePausedEvent = typeof MessageQueuePausedEvent.Type;

export const PendingMessagesPausedPayload = Schema.Struct({
  interruptionCommandId: CommandId,
  runId: RunId,
  messageIds: Schema.NonEmptyArray(MessageId),
  reason: Schema.Literal("user-interrupted"),
});
export type PendingMessagesPausedPayload = typeof PendingMessagesPausedPayload.Type;
export const PendingMessagesPausedEvent = durableEventSchema(
  pendingMessagesPausedEventType,
  PendingMessagesPausedPayload,
);
export type PendingMessagesPausedEvent = typeof PendingMessagesPausedEvent.Type;

/** Payload recorded when an assistant message is durably finalized. */
export const AssistantMessageCommittedPayload = Schema.Struct({
  messageId: MessageId,
  runId: RunId,
  turnId: TurnId,
  inferenceId: InferenceId,
  promptParts: AssistantPromptParts,
});
export type AssistantMessageCommittedPayload = typeof AssistantMessageCommittedPayload.Type;
export const AssistantMessageCommittedEvent = durableEventSchema(
  assistantMessageCommittedEventType,
  AssistantMessageCommittedPayload,
);
export type AssistantMessageCommittedEvent = typeof AssistantMessageCommittedEvent.Type;

/** Payload recorded when pre-existing assistant context is imported from an external transport. */
export const AssistantMessageImportedPayload = Schema.Struct({
  messageId: MessageId,
  runId: RunId,
  turnId: TurnId,
  inferenceId: InferenceId,
  promptParts: AssistantPromptParts,
});
export type AssistantMessageImportedPayload = typeof AssistantMessageImportedPayload.Type;
export const AssistantMessageImportedEvent = durableEventSchema(
  assistantMessageImportedEventType,
  AssistantMessageImportedPayload,
);
export type AssistantMessageImportedEvent = typeof AssistantMessageImportedEvent.Type;

/** Payload recorded when a partial assistant message is preserved durably. */
export const AssistantPartialCommittedPayload = Schema.Struct({
  messageId: MessageId,
  runId: RunId,
  turnId: TurnId,
  inferenceId: InferenceId,
  promptParts: AssistantPromptParts,
  reason: Schema.String,
});
export type AssistantPartialCommittedPayload = typeof AssistantPartialCommittedPayload.Type;
export const AssistantPartialCommittedEvent = durableEventSchema(
  assistantPartialCommittedEventType,
  AssistantPartialCommittedPayload,
);
export type AssistantPartialCommittedEvent = typeof AssistantPartialCommittedEvent.Type;

/** Durable event union for message commit/queue boundaries. */
export const MessageEvent = Schema.Union([
  SystemMessageCommittedEvent,
  UserMessageCommittedEvent,
  SteeringMessageQueuedEvent,
  SteeringMessageCancelledEvent,
  UserMessageSubmittedEvent,
  UserMessagePromotedEvent,
  UserMessageCancelledEvent,
  MessageQueuePausedEvent,
  PendingMessagesPausedEvent,
  AssistantMessageCommittedEvent,
  AssistantMessageImportedEvent,
  AssistantPartialCommittedEvent,
]);
export type MessageEvent = typeof MessageEvent.Type;

/** Event type values for durable run lifecycle events. */
export const runStartedEventType = makeEventType("RunStarted");
export const runCompletedEventType = makeEventType("RunCompleted");
export const runFailedEventType = makeEventType("RunFailed");
/** @deprecated Legacy terminal retained for durable replay. Emit `RunFailed` with an interruption-coded `FailurePayload` instead. */
export const runInterruptedEventType = makeEventType("RunInterrupted");

/** Payload recorded when a run starts with its intended model selection. */
export const RunStartedPayload = Schema.Struct({
  runId: RunId,
  commandIds: Schema.Array(CommandId),
  modelSelection: ModelSelectionPayload,
  trace: EDARunTrace.pipe(Schema.withConstructorDefault(Effect.sync(() => makeEDARunTrace()))),
});
export type RunStartedPayload = typeof RunStartedPayload.Type;
export const RunStartedEvent = durableEventSchema(runStartedEventType, RunStartedPayload);
export type RunStartedEvent = typeof RunStartedEvent.Type;

/** Payload recorded when a run completes. */
export const RunCompletedPayload = Schema.Struct({ runId: RunId });
export type RunCompletedPayload = typeof RunCompletedPayload.Type;
export const RunCompletedEvent = durableEventSchema(runCompletedEventType, RunCompletedPayload);
export type RunCompletedEvent = typeof RunCompletedEvent.Type;

/** Payload recorded when a run fails. */
export const RunFailedPayload = Schema.Struct({ runId: RunId, error: FailurePayload });
export type RunFailedPayload = typeof RunFailedPayload.Type;
export const RunFailedEvent = durableEventSchema(runFailedEventType, RunFailedPayload);
export type RunFailedEvent = typeof RunFailedEvent.Type;

/** @deprecated Legacy payload retained for durable replay. New interruptions use `RunFailedPayload` with `error.code = "run.interrupted"`. */
export const RunInterruptedPayload = Schema.Struct({ runId: RunId, reason: Schema.String });
/** @deprecated Use `RunFailedPayload` with interruption metadata. */
export type RunInterruptedPayload = typeof RunInterruptedPayload.Type;
/** @deprecated Legacy event schema retained for durable replay; new producers emit `RunFailedEvent`. */
export const RunInterruptedEvent = durableEventSchema(
  runInterruptedEventType,
  RunInterruptedPayload,
);
/** @deprecated Use `RunFailedEvent` with interruption metadata. */
export type RunInterruptedEvent = typeof RunInterruptedEvent.Type;

/** Durable event union for run lifecycle boundaries. */
export const RunEvent = Schema.Union([
  RunStartedEvent,
  RunCompletedEvent,
  RunFailedEvent,
  RunInterruptedEvent,
]);
export type RunEvent = typeof RunEvent.Type;

/** Event type value for the durable barrier written after real startup recovery work. */
export const recoveryCompletedEventType = makeEventType("RecoveryCompleted");

/** Explicit relationship between an interrupted run and its startup-recovery replacement. */
export const RecoveryContinuation = Schema.Struct({
  commandId: CommandId,
  interruptedRunId: RunId,
  replacementRunId: RunId,
});
export type RecoveryContinuation = typeof RecoveryContinuation.Type;

/**
 * Durable completion barrier for a startup recovery transaction that repaired
 * unfinished state. Idle runtime construction does not emit this event.
 */
export const RecoveryCompletedPayload = Schema.Struct({
  trigger: Schema.Literal("runtime-restart"),
  continuation: Schema.optionalKey(RecoveryContinuation),
});
export type RecoveryCompletedPayload = typeof RecoveryCompletedPayload.Type;
export const RecoveryCompletedEvent = durableEventSchema(
  recoveryCompletedEventType,
  RecoveryCompletedPayload,
);
export type RecoveryCompletedEvent = typeof RecoveryCompletedEvent.Type;

/** Durable facts written only by the framework startup-recovery transaction. */
export const RecoveryEvent = RecoveryCompletedEvent;
export type RecoveryEvent = typeof RecoveryEvent.Type;

/** Event type values for durable turn lifecycle events. */
export const turnStartedEventType = makeEventType("TurnStarted");
export const turnCompletedEventType = makeEventType("TurnCompleted");
export const turnFailedEventType = makeEventType("TurnFailed");
/** @deprecated Legacy terminal retained for durable replay. Emit `TurnFailed` with an interruption-coded `FailurePayload` instead. */
export const turnStoppedEventType = makeEventType("TurnStopped");

/** Payload recorded when one turn starts inside a run. */
export const TurnStartedPayload = Schema.Struct({
  runId: RunId,
  turnId: TurnId,
  inputMessageIds: Schema.optionalKey(Schema.Array(MessageId)),
});
export type TurnStartedPayload = typeof TurnStartedPayload.Type;
export const TurnStartedEvent = durableEventSchema(turnStartedEventType, TurnStartedPayload);
export type TurnStartedEvent = typeof TurnStartedEvent.Type;

/** Payload recorded when one turn completes. */
export const TurnCompletedPayload = Schema.Struct({
  runId: RunId,
  turnId: TurnId,
  usage: Schema.optionalKey(UsagePayload),
});
export type TurnCompletedPayload = typeof TurnCompletedPayload.Type;
export const TurnCompletedEvent = durableEventSchema(turnCompletedEventType, TurnCompletedPayload);
export type TurnCompletedEvent = typeof TurnCompletedEvent.Type;

/** Payload recorded when one turn fails. */
export const TurnFailedPayload = Schema.Struct({
  runId: RunId,
  turnId: TurnId,
  error: FailurePayload,
});
export type TurnFailedPayload = typeof TurnFailedPayload.Type;
export const TurnFailedEvent = durableEventSchema(turnFailedEventType, TurnFailedPayload);
export type TurnFailedEvent = typeof TurnFailedEvent.Type;

/** @deprecated Legacy payload retained for durable replay. New stops use `TurnFailedPayload` with `error.code = "turn.interrupted"`. */
export const TurnStoppedPayload = Schema.Struct({
  runId: RunId,
  turnId: TurnId,
  reason: Schema.String,
});
/** @deprecated Use `TurnFailedPayload` with interruption metadata. */
export type TurnStoppedPayload = typeof TurnStoppedPayload.Type;
/** @deprecated Legacy event schema retained for durable replay; new producers emit `TurnFailedEvent`. */
export const TurnStoppedEvent = durableEventSchema(turnStoppedEventType, TurnStoppedPayload);
/** @deprecated Use `TurnFailedEvent` with interruption metadata. */
export type TurnStoppedEvent = typeof TurnStoppedEvent.Type;

/** Durable event union for turn lifecycle boundaries. */
export const TurnEvent = Schema.Union([
  TurnStartedEvent,
  TurnCompletedEvent,
  TurnFailedEvent,
  TurnStoppedEvent,
]);
export type TurnEvent = typeof TurnEvent.Type;

/** Event type values for durable model-inference lifecycle events. */
export const inferenceStartedEventType = makeEventType("InferenceStarted");
export const inferenceCompletedEventType = makeEventType("InferenceCompleted");
export const inferenceFailedEventType = makeEventType("InferenceFailed");

/** Payload recorded when one model inference starts for a turn. */
export const InferenceStartedPayload = Schema.Struct({
  runId: RunId,
  turnId: TurnId,
  inferenceId: InferenceId,
});
export type InferenceStartedPayload = typeof InferenceStartedPayload.Type;
export const InferenceStartedEvent = durableEventSchema(
  inferenceStartedEventType,
  InferenceStartedPayload,
);
export type InferenceStartedEvent = typeof InferenceStartedEvent.Type;

/** Payload recorded when one model inference completes. */
export const InferenceCompletedPayload = Schema.Struct({
  runId: RunId,
  turnId: TurnId,
  inferenceId: InferenceId,
  finishReason: Schema.optionalKey(Schema.String),
  usage: Schema.optionalKey(UsagePayload),
  responseMetadata: Schema.optionalKey(Schema.Unknown),
  finishMetadata: Schema.optionalKey(Schema.Unknown),
});
export type InferenceCompletedPayload = typeof InferenceCompletedPayload.Type;
export const InferenceCompletedEvent = durableEventSchema(
  inferenceCompletedEventType,
  InferenceCompletedPayload,
);
export type InferenceCompletedEvent = typeof InferenceCompletedEvent.Type;

/** Payload recorded when one model inference fails or is interrupted. */
export const InferenceFailedPayload = Schema.Struct({
  runId: RunId,
  turnId: TurnId,
  inferenceId: InferenceId,
  error: FailurePayload,
});
export type InferenceFailedPayload = typeof InferenceFailedPayload.Type;
export const InferenceFailedEvent = durableEventSchema(
  inferenceFailedEventType,
  InferenceFailedPayload,
);
export type InferenceFailedEvent = typeof InferenceFailedEvent.Type;

/** Durable event union for canonical model-inference lifecycle boundaries. */
export const InferenceEvent = Schema.Union([
  InferenceStartedEvent,
  InferenceCompletedEvent,
  InferenceFailedEvent,
]);
export type InferenceEvent = typeof InferenceEvent.Type;

/** Event type values for durable tool lifecycle events. */
export const toolCallCreatedEventType = makeEventType("ToolCallCreated");
export const toolCallRejectedEventType = makeEventType("ToolCallRejected");
export const toolCallStartedEventType = makeEventType("ToolCallStarted");
export const toolCallCompletedEventType = makeEventType("ToolCallCompleted");
export const toolCallFailedEventType = makeEventType("ToolCallFailed");

/** Reason a model-requested tool call was rejected before execution. */
export const ToolCallRejectionReason = Schema.Literals(["unknown-tool", "invalid-params"]);
export type ToolCallRejectionReason = typeof ToolCallRejectionReason.Type;

/** Exact synthetic tool-result feedback for a rejected model-requested tool call. */
export const ToolCallRejectionFeedback = Schema.Struct({
  message: Schema.String,
  reason: ToolCallRejectionReason,
  modelFeedback: Schema.NonEmptyString,
});
export type ToolCallRejectionFeedback = typeof ToolCallRejectionFeedback.Type;

/** Exact prompt part for a rejected model-requested tool call. */
export const ToolRejectionPromptPart = Schema.Struct({
  ...ToolPromptPart.fields,
  isFailure: Schema.Literal(true),
  result: ToolCallRejectionFeedback,
});
export type ToolRejectionPromptPart = typeof ToolRejectionPromptPart.Type;

/** Exact prompt part for a successful framework-owned tool result. */
export const ToolSuccessPromptPart = Schema.Struct({
  ...ToolPromptPart.fields,
  isFailure: Schema.Literal(false),
  result: Schema.Unknown,
});
export type ToolSuccessPromptPart = typeof ToolSuccessPromptPart.Type;

/** Exact prompt part for a failed framework-owned tool result. */
export const ToolFailurePromptPart = Schema.Struct({
  ...ToolPromptPart.fields,
  isFailure: Schema.Literal(true),
  result: FailurePayload,
});
export type ToolFailurePromptPart = typeof ToolFailurePromptPart.Type;

/** Payload recorded once streamed tool parameters validate and become executable. */
export const ToolCallCreatedPayload = Schema.Struct({
  runId: RunId,
  turnId: TurnId,
  inferenceId: InferenceId,
  toolCallId: ToolCallId,
  promptPart: ToolCallPromptPart,
});
export type ToolCallCreatedPayload = typeof ToolCallCreatedPayload.Type;
export const ToolCallCreatedEvent = durableEventSchema(
  toolCallCreatedEventType,
  ToolCallCreatedPayload,
);
export type ToolCallCreatedEvent = typeof ToolCallCreatedEvent.Type;

/** Payload recorded when a model-requested tool call is not executable. */
export const ToolCallRejectedPayload = Schema.Struct({
  runId: RunId,
  turnId: TurnId,
  inferenceId: InferenceId,
  toolCallId: ToolCallId,
  promptPart: ToolRejectionPromptPart,
});
export type ToolCallRejectedPayload = typeof ToolCallRejectedPayload.Type;
export const ToolCallRejectedEvent = durableEventSchema(
  toolCallRejectedEventType,
  ToolCallRejectedPayload,
);
export type ToolCallRejectedEvent = typeof ToolCallRejectedEvent.Type;

/** Payload recorded when framework-owned tool execution starts. */
export const ToolCallStartedPayload = Schema.Struct({ toolCallId: ToolCallId });
export type ToolCallStartedPayload = typeof ToolCallStartedPayload.Type;
export const ToolCallStartedEvent = durableEventSchema(
  toolCallStartedEventType,
  ToolCallStartedPayload,
);
export type ToolCallStartedEvent = typeof ToolCallStartedEvent.Type;

/** Payload recorded when a tool call completes with a result. */
export const ToolCallCompletedPayload = Schema.Struct({
  toolCallId: ToolCallId,
  promptPart: ToolSuccessPromptPart,
});
export type ToolCallCompletedPayload = typeof ToolCallCompletedPayload.Type;
export const ToolCallCompletedEvent = durableEventSchema(
  toolCallCompletedEventType,
  ToolCallCompletedPayload,
);
export type ToolCallCompletedEvent = typeof ToolCallCompletedEvent.Type;

/** Payload recorded when a tool call fails. */
export const ToolCallFailedPayload = Schema.Struct({
  toolCallId: ToolCallId,
  error: Schema.optionalKey(FailurePayload),
  promptPart: ToolFailurePromptPart,
});
export type ToolCallFailedPayload = typeof ToolCallFailedPayload.Type;
export const ToolCallFailedEvent = durableEventSchema(
  toolCallFailedEventType,
  ToolCallFailedPayload,
);
export type ToolCallFailedEvent = typeof ToolCallFailedEvent.Type;

/** Durable event union for tool lifecycle boundaries. */
export const ToolCallEvent = Schema.Union([
  ToolCallCreatedEvent,
  ToolCallRejectedEvent,
  ToolCallStartedEvent,
  ToolCallCompletedEvent,
  ToolCallFailedEvent,
]);
export type ToolCallEvent = typeof ToolCallEvent.Type;

/** Event type values for durable stop-control events. */
export const stopTurnRequestedEventType = makeEventType("StopTurnRequested");
export const stopTurnAppliedEventType = makeEventType("StopTurnApplied");

/** Payload recorded when a stop command targets active or future turn work. */
export const StopTurnRequestedPayload = Schema.Struct({
  commandId: CommandId,
  runId: Schema.optionalKey(RunId),
  turnId: Schema.optionalKey(TurnId),
});
export type StopTurnRequestedPayload = typeof StopTurnRequestedPayload.Type;
export const StopTurnRequestedEvent = durableEventSchema(
  stopTurnRequestedEventType,
  StopTurnRequestedPayload,
);
export type StopTurnRequestedEvent = typeof StopTurnRequestedEvent.Type;

/** Payload recorded when a stop request has affected runtime work. */
export const StopTurnAppliedPayload = Schema.Struct({
  commandId: CommandId,
  runId: RunId,
  turnId: TurnId,
  inferenceId: Schema.optionalKey(InferenceId),
});
export type StopTurnAppliedPayload = typeof StopTurnAppliedPayload.Type;
export const StopTurnAppliedEvent = durableEventSchema(
  stopTurnAppliedEventType,
  StopTurnAppliedPayload,
);
export type StopTurnAppliedEvent = typeof StopTurnAppliedEvent.Type;

/** Durable event union for stop-request lifecycle boundaries. */
export const StopControlEvent = Schema.Union([StopTurnRequestedEvent, StopTurnAppliedEvent]);
export type StopControlEvent = typeof StopControlEvent.Type;

/** Event type values for durable context and compaction events. */
export const contextProjectedEventType = makeEventType("ContextProjected");
export const compactionRequestedEventType = makeEventType("CompactionRequested");
export const compactionStartedEventType = makeEventType("CompactionStarted");
export const summaryCreatedEventType = makeEventType("SummaryCreated");
export const compactionCompletedEventType = makeEventType("CompactionCompleted");
export const contextRebasedEventType = makeEventType("ContextRebased");
export const compactionFailedEventType = makeEventType("CompactionFailed");

/** Payload recorded when the prompt context projection advances. */
export const ContextProjectedPayload = Schema.Struct({
  contextVersion: ContextVersion,
  throughSeq: SequenceNumber,
});
export type ContextProjectedPayload = typeof ContextProjectedPayload.Type;
export const ContextProjectedEvent = durableEventSchema(
  contextProjectedEventType,
  ContextProjectedPayload,
);
export type ContextProjectedEvent = typeof ContextProjectedEvent.Type;

/** Payload recorded when compaction is requested for a durable range. */
export const CompactionRequestedPayload = Schema.Struct({
  compactionId: CompactionId,
  sourceFromSeq: SequenceNumber,
  sourceToSeq: SequenceNumber,
});
export type CompactionRequestedPayload = typeof CompactionRequestedPayload.Type;
export const CompactionRequestedEvent = durableEventSchema(
  compactionRequestedEventType,
  CompactionRequestedPayload,
);
export type CompactionRequestedEvent = typeof CompactionRequestedEvent.Type;

/** Payload recorded when compaction starts. */
export const CompactionStartedPayload = Schema.Struct({ compactionId: CompactionId });
export type CompactionStartedPayload = typeof CompactionStartedPayload.Type;
export const CompactionStartedEvent = durableEventSchema(
  compactionStartedEventType,
  CompactionStartedPayload,
);
export type CompactionStartedEvent = typeof CompactionStartedEvent.Type;

/** Payload recorded when a summary artifact is created. */
export const SummaryCreatedPayload = Schema.Struct({
  compactionId: CompactionId,
  summaryId: SummaryId,
  sourceFromSeq: SequenceNumber,
  sourceToSeq: SequenceNumber,
  summary: Schema.Unknown,
});
export type SummaryCreatedPayload = typeof SummaryCreatedPayload.Type;
export const SummaryCreatedEvent = durableEventSchema(
  summaryCreatedEventType,
  SummaryCreatedPayload,
);
export type SummaryCreatedEvent = typeof SummaryCreatedEvent.Type;

/** Payload recorded when compaction completes. */
export const CompactionCompletedPayload = Schema.Struct({
  compactionId: CompactionId,
  modelSelection: Schema.optionalKey(ModelSelectionPayload),
  usage: Schema.optionalKey(UsagePayload),
});
export type CompactionCompletedPayload = typeof CompactionCompletedPayload.Type;
export const CompactionCompletedEvent = durableEventSchema(
  compactionCompletedEventType,
  CompactionCompletedPayload,
);
export type CompactionCompletedEvent = typeof CompactionCompletedEvent.Type;

/** Payload recorded when durable context is rebased onto a summary. */
export const ContextRebasedPayload = Schema.Struct({
  compactionId: CompactionId,
  summaryId: SummaryId,
  contextVersion: ContextVersion,
  retainedFromContextSeq: SequenceNumber,
});
export type ContextRebasedPayload = typeof ContextRebasedPayload.Type;
export const ContextRebasedEvent = durableEventSchema(
  contextRebasedEventType,
  ContextRebasedPayload,
);
export type ContextRebasedEvent = typeof ContextRebasedEvent.Type;

/** Payload recorded when compaction fails. */
export const CompactionFailedPayload = Schema.Struct({
  compactionId: CompactionId,
  error: FailurePayload,
  modelSelection: Schema.optionalKey(ModelSelectionPayload),
  usage: Schema.optionalKey(UsagePayload),
});
export type CompactionFailedPayload = typeof CompactionFailedPayload.Type;
export const CompactionFailedEvent = durableEventSchema(
  compactionFailedEventType,
  CompactionFailedPayload,
);
export type CompactionFailedEvent = typeof CompactionFailedEvent.Type;

/** Durable event union for context projection and compaction boundaries. */
export const ContextAndCompactionEvent = Schema.Union([
  ContextProjectedEvent,
  CompactionRequestedEvent,
  CompactionStartedEvent,
  SummaryCreatedEvent,
  CompactionCompletedEvent,
  ContextRebasedEvent,
  CompactionFailedEvent,
]);
export type ContextAndCompactionEvent = typeof ContextAndCompactionEvent.Type;

/** Event type values for durable BaseState lifecycle events. */
export const baseStateRequestedEventType = makeEventType("BaseStateRequested");
export const baseStateCreatedEventType = makeEventType("BaseStateCreated");
export const baseStateFailedEventType = makeEventType("BaseStateFailed");

/** Payload recorded when durable BaseState generation is requested. */
export const BaseStateRequestedPayload = Schema.Struct({
  baseStateId: BaseStateId,
  requestedThroughSeq: SequenceNumber,
});
export type BaseStateRequestedPayload = typeof BaseStateRequestedPayload.Type;
export const BaseStateRequestedEvent = durableEventSchema(
  baseStateRequestedEventType,
  BaseStateRequestedPayload,
);
export type BaseStateRequestedEvent = typeof BaseStateRequestedEvent.Type;

/** Payload recorded when a BaseState snapshot is created. */
export const BaseStateCreatedPayload = Schema.Struct({
  baseStateId: BaseStateId,
  coversThroughSeq: SequenceNumber,
});
export type BaseStateCreatedPayload = typeof BaseStateCreatedPayload.Type;
export const BaseStateCreatedEvent = durableEventSchema(
  baseStateCreatedEventType,
  BaseStateCreatedPayload,
);
export type BaseStateCreatedEvent = typeof BaseStateCreatedEvent.Type;

/** Payload recorded when BaseState generation fails. */
export const BaseStateFailedPayload = Schema.Struct({
  baseStateId: BaseStateId,
  error: FailurePayload,
});
export type BaseStateFailedPayload = typeof BaseStateFailedPayload.Type;
export const BaseStateFailedEvent = durableEventSchema(
  baseStateFailedEventType,
  BaseStateFailedPayload,
);
export type BaseStateFailedEvent = typeof BaseStateFailedEvent.Type;

/** Durable event union for BaseState lifecycle boundaries. */
export const BaseStateEvent = Schema.Union([
  BaseStateRequestedEvent,
  BaseStateCreatedEvent,
  BaseStateFailedEvent,
]);
export type BaseStateEvent = typeof BaseStateEvent.Type;

/** Built-in durable event union for framework-owned session facts. */
export const EDADurableEvent = Schema.Union([
  SessionConfiguredEvent,
  CommandEvent,
  MessageEvent,
  RunEvent,
  RecoveryEvent,
  TurnEvent,
  InferenceEvent,
  ToolCallEvent,
  StopControlEvent,
  ContextAndCompactionEvent,
  BaseStateEvent,
]);
export type EDADurableEvent = typeof EDADurableEvent.Type;

/** Current V1 framework durable event schema. Future versions route beside this alias. */
export const EDADurableEventV1 = EDADurableEvent;
export type EDADurableEventV1 = typeof EDADurableEventV1.Type;

/** Decode one framework-owned durable event through the namespace/type/version router. */
export const decodeUnknownEDADurableEventSync = (input: unknown): EDADurableEvent => {
  const envelope = Schema.decodeUnknownSync(DurableEventEnvelope)(input);
  if (envelope.namespace !== effectDurableAgentNamespace) {
    throw new Error(
      `EDADurableEvent decoder only accepts namespace ${effectDurableAgentNamespace}; received ${envelope.namespace}`,
    );
  }

  switch (envelope.schemaVersion) {
    case schemaV1:
      return Schema.decodeUnknownSync(EDADurableEventV1)(envelope);
    default:
      throw new Error(
        `Unsupported EDA durable event schemaVersion ${envelope.schemaVersion} for ${envelope.type}`,
      );
  }
};

/** Effectful V1+ router for untrusted framework-owned durable event boundaries. */
export const decodeUnknownEDADurableEvent = (
  input: unknown,
): Effect.Effect<EDADurableEvent, unknown> =>
  Effect.try({
    try: () => decodeUnknownEDADurableEventSync(input),
    catch: (error) => error,
  });
