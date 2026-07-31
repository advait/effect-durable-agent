import type * as Prompt from "effect/unstable/ai/Prompt";

import { assertNever } from "./assert-never";
import { deriveCommandQueues, emptyCommandQueues, type CommandQueues } from "./command-queues";
import {
  InferenceId,
  CommandId,
  CompactionId,
  ContextVersion,
  MessageId,
  RunId,
  SequenceNumber,
  SummaryId,
  ToolCallId,
  TurnId,
} from "../types/core";
import {
  assistantMessageCommittedEventType,
  assistantPartialCommittedEventType,
  baseStateCreatedEventType,
  baseStateFailedEventType,
  baseStateRequestedEventType,
  commandAdmittedEventType,
  commandCancelledEventType,
  commandCompletedEventType,
  commandFailedEventType,
  commandStartedEventType,
  compactionCompletedEventType,
  compactionFailedEventType,
  compactionRequestedEventType,
  compactionStartedEventType,
  contextProjectedEventType,
  effectDurableAgentNamespace,
  contextRebasedEventType,
  inferenceCompletedEventType,
  inferenceFailedEventType,
  inferenceStartedEventType,
  runCompletedEventType,
  runFailedEventType,
  runInterruptedEventType,
  runStartedEventType,
  stopTurnAppliedEventType,
  stopTurnRequestedEventType,
  steeringMessageCancelledEventType,
  steeringMessageQueuedEventType,
  userMessageSubmittedEventType,
  userMessagePromotedEventType,
  userMessageCancelledEventType,
  messageQueuePausedEventType,
  pendingMessagesPausedEventType,
  recoveryCompletedEventType,
  summaryCreatedEventType,
  systemMessageCommittedEventType,
  toolCallCompletedEventType,
  toolCallCreatedEventType,
  toolCallFailedEventType,
  toolCallRejectedEventType,
  toolCallStartedEventType,
  turnCompletedEventType,
  turnFailedEventType,
  turnStartedEventType,
  turnStoppedEventType,
  userMessageCommittedEventType,
} from "../types/events";
import { type EDACommand as EDACommandValue, type UserMessageContent } from "../types/commands";
import type {
  AssistantMessageContent,
  EDAEventTrace,
  EDARunTrace,
  FailurePayload,
  ProviderPartId,
  RecoveryContinuation,
  SystemPromptText,
  ToolName,
  UsagePayload,
} from "../types/events";
import type { CommittedDurableEvent } from "../services/session-store";

/** Failure payload paired with the durable sequence that recorded it. */
export type LifecycleFailure = {
  readonly seq: SequenceNumber;
  readonly error: FailurePayload;
};

/** Terminal command state folded from command lifecycle events. */
export type CommandTerminal =
  | { readonly _tag: "Completed"; readonly seq: SequenceNumber }
  | { readonly _tag: "Failed"; readonly seq: SequenceNumber; readonly error: FailurePayload }
  | { readonly _tag: "Cancelled"; readonly seq: SequenceNumber; readonly reason: string };

/** Durable command lifecycle projection used for scheduling and recovery. */
export interface CommandRecord {
  readonly commandId: CommandId;
  readonly command?: EDACommandValue;
  readonly admittedSeq?: SequenceNumber;
  readonly admissionTrace?: EDAEventTrace;
  readonly startedSeq?: SequenceNumber;
  readonly terminal?: CommandTerminal;
}

/** Terminal run state folded from run lifecycle events. */
export type RunTerminal =
  | { readonly _tag: "Completed"; readonly seq: SequenceNumber }
  | { readonly _tag: "Failed"; readonly seq: SequenceNumber; readonly error: FailurePayload }
  | { readonly _tag: "Interrupted"; readonly seq: SequenceNumber; readonly reason: string };

/** Common wall-clock lifecycle timing folded from durable event envelopes. */
export interface LifecycleTiming {
  readonly startedAtMs?: number;
  readonly terminalAtMs?: number;
  readonly durationMs?: number;
}

/** Durable run lifecycle projection tying one run to its owning command ids. */
export interface RunRecord extends LifecycleTiming {
  readonly runId: RunId;
  readonly commandIds: ReadonlyArray<CommandId>;
  readonly startedSeq: SequenceNumber;
  readonly trace: EDARunTrace;
  readonly terminal?: RunTerminal;
}

/** Terminal turn state folded from turn lifecycle events. */
export type TurnTerminal =
  | { readonly _tag: "Completed"; readonly seq: SequenceNumber; readonly usage?: UsagePayload }
  | { readonly _tag: "Failed"; readonly seq: SequenceNumber; readonly error: FailurePayload }
  | { readonly _tag: "Stopped"; readonly seq: SequenceNumber; readonly reason: string };

/** Durable turn lifecycle projection nested under a run. */
export interface TurnRecord extends LifecycleTiming {
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly startedSeq: SequenceNumber;
  readonly terminal?: TurnTerminal;
}

/** Terminal inference state folded from inference lifecycle events. */
export type InferenceTerminal =
  | { readonly _tag: "Completed"; readonly seq: SequenceNumber; readonly usage?: UsagePayload }
  | { readonly _tag: "Failed"; readonly seq: SequenceNumber; readonly error: FailurePayload };

/** Durable model-inference lifecycle projection nested under a turn. */
export interface InferenceRecord extends LifecycleTiming {
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly inferenceId: InferenceId;
  readonly startedSeq: SequenceNumber;
  readonly terminal?: InferenceTerminal;
}

/** Durable decision produced after a model inference seals final tool calls. */
export type ToolDecision =
  | {
      readonly _tag: "Created";
      readonly seq: SequenceNumber;
      readonly runId: RunId;
      readonly turnId: TurnId;
      readonly inferenceId: InferenceId;
      readonly providerPartId: ProviderPartId;
      readonly toolName: ToolName;
      readonly params: unknown;
      readonly providerExecuted: boolean;
      readonly promptPart?: Prompt.ToolCallPart;
    }
  | {
      readonly _tag: "Rejected";
      readonly seq: SequenceNumber;
      readonly runId: RunId;
      readonly turnId: TurnId;
      readonly inferenceId: InferenceId;
      readonly providerPartId: ProviderPartId;
      readonly toolName: ToolName;
      readonly reason: "unknown-tool" | "invalid-params";
      readonly modelFeedback: string;
      readonly promptPart?: Prompt.ToolResultPart;
    };

/** Terminal framework-owned tool-call execution state. */
export type ToolTerminal =
  | {
      readonly _tag: "Completed";
      readonly seq: SequenceNumber;
      readonly result: unknown;
      readonly promptPart?: Prompt.ToolResultPart;
    }
  | {
      readonly _tag: "Failed";
      readonly seq: SequenceNumber;
      readonly error: FailurePayload;
      readonly promptPart?: Prompt.ToolResultPart;
    };

/** Durable tool-call projection from decision through execution terminal. */
export interface ToolCallRecord extends LifecycleTiming {
  readonly toolCallId: ToolCallId;
  readonly decision?: ToolDecision;
  readonly startedSeq?: SequenceNumber;
  readonly terminal?: ToolTerminal;
}

/** Durable message-like facts folded from the event log. */
export type MessageRecord =
  | {
      readonly _tag: "System";
      readonly messageId: MessageId;
      readonly content: SystemPromptText;
      readonly createdAtMs?: number;
      readonly seq: SequenceNumber;
    }
  | {
      readonly _tag: "User";
      readonly messageId: MessageId;
      readonly commandId: CommandId;
      readonly content: UserMessageContent;
      readonly createdAtMs?: number;
      readonly seq: SequenceNumber;
      /** Present for admission-time message entities; absent on legacy committed users. */
      readonly requestedDisposition?: "queue" | "steer";
      readonly disposition?: "queue" | "steer";
      readonly promotedSeq?: SequenceNumber;
      readonly pausedSeq?: SequenceNumber;
      readonly pausedByCommandId?: CommandId;
      readonly consumedSeq?: SequenceNumber;
      readonly consumedTurnId?: TurnId;
      readonly cancelledSeq?: SequenceNumber;
      readonly cancelledByCommandId?: CommandId;
      readonly cancellationReason?: string;
    }
  | {
      readonly _tag: "Steering";
      readonly messageId: MessageId;
      readonly commandId: CommandId;
      readonly runId: RunId;
      readonly content: UserMessageContent;
      readonly createdAtMs?: number;
      readonly seq: SequenceNumber;
      readonly promotedSeq?: SequenceNumber;
      readonly consumedSeq?: SequenceNumber;
      readonly consumedTurnId?: TurnId;
      readonly cancelledSeq?: SequenceNumber;
      readonly cancelledByCommandId?: CommandId;
      readonly cancellationReason?: string;
      readonly pausedSeq?: SequenceNumber;
      readonly pausedByCommandId?: CommandId;
    }
  | {
      readonly _tag: "Assistant";
      readonly messageId: MessageId;
      readonly runId: RunId;
      readonly turnId: TurnId;
      readonly inferenceId: InferenceId;
      readonly content: AssistantMessageContent;
      readonly createdAtMs?: number;
      readonly promptParts?: ReadonlyArray<Prompt.AssistantMessagePart>;
      readonly seq: SequenceNumber;
    }
  | {
      readonly _tag: "AssistantPartial";
      readonly messageId: MessageId;
      readonly runId: RunId;
      readonly turnId: TurnId;
      readonly inferenceId: InferenceId;
      readonly content: AssistantMessageContent;
      readonly createdAtMs?: number;
      readonly promptParts?: ReadonlyArray<Prompt.AssistantMessagePart>;
      readonly reason: string;
      readonly seq: SequenceNumber;
    };

/** Terminal compaction state folded from compaction lifecycle events. */
export type CompactionTerminal =
  | { readonly _tag: "Completed"; readonly seq: SequenceNumber }
  | { readonly _tag: "Failed"; readonly seq: SequenceNumber; readonly error: FailurePayload };

/** Durable compaction lifecycle projection and its produced summary cursor. */
export interface CompactionRecord {
  readonly compactionId: CompactionId;
  readonly requestedSeq?: SequenceNumber;
  readonly startedSeq?: SequenceNumber;
  readonly summaryCreatedSeq?: SequenceNumber;
  readonly summaryId?: SummaryId;
  readonly sourceFromSeq?: SequenceNumber;
  readonly sourceToSeq?: SequenceNumber;
  readonly rebasedSeq?: SequenceNumber;
  readonly contextVersion?: ContextVersion;
  readonly terminal?: CompactionTerminal;
}

/** Current model-context version and summary selected by compaction. */
export interface ContextRecord {
  readonly version: ContextVersion;
  readonly currentSummaryId?: SummaryId;
}

/** Durable StopTurn request and the lifecycle boundary it eventually applied to. */
export interface StopRequestRecord {
  readonly commandId: CommandId;
  readonly requestedSeq: SequenceNumber;
  readonly requestedRunId?: RunId;
  readonly requestedTurnId?: TurnId;
  readonly appliedSeq?: SequenceNumber;
  readonly appliedRunId?: RunId;
  readonly appliedTurnId?: TurnId;
  readonly appliedInferenceId?: InferenceId;
}

/** Rolling session totals for provider-reported model token usage. */
export interface TokenUsageTotals {
  readonly inputTokens: number;
  readonly cachedInputTokens: number;
  readonly outputTokens: number;
  readonly textTokens: number;
  readonly reasoningTokens: number;
}

/** First-party token consumption metadata folded by the framework reducer. */
export interface TokenConsumptionState {
  readonly total: TokenUsageTotals;
}

/** Admitted command that has not yet crossed the `CommandStarted` boundary. */
export interface PendingCommand {
  /** Lifecycle ID for the admitted command. */
  readonly commandId: CommandId;
  /** Typed command payload admitted durably. */
  readonly command: EDACommandValue;
  /** Admission sequence used for deterministic FIFO/control ordering. */
  readonly admittedSeq: SequenceNumber;
  /** Trace context captured at the durable command admission boundary. */
  readonly admissionTrace?: EDAEventTrace;
}

/** Started command without a terminal command boundary, optionally tied to an open run. */
export interface ActiveCommandEvidence {
  readonly commandId: CommandId;
  readonly runId?: RunId;
}

/** Active run/turn/inference identity derived from unfinished lifecycle records. */
export type ActiveTurnIdentity = {
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly inferenceId?: InferenceId;
};

/** Canonical durable replay product used by queries, recovery, and scheduling. */
export interface ReducedState {
  readonly lastSeq: SequenceNumber;
  readonly commands: ReadonlyMap<CommandId, CommandRecord>;
  readonly runs: ReadonlyMap<RunId, RunRecord>;
  /** Explicit startup-recovery links keyed by replacement physical run id. */
  readonly recoveryContinuations: ReadonlyMap<RunId, RecoveryContinuationRecord>;
  readonly turns: ReadonlyMap<TurnId, TurnRecord>;
  readonly inferences: ReadonlyMap<InferenceId, InferenceRecord>;
  readonly toolCalls: ReadonlyMap<ToolCallId, ToolCallRecord>;
  readonly messages: ReadonlyMap<MessageId, MessageRecord>;
  readonly stopRequests: ReadonlyMap<CommandId, StopRequestRecord>;
  readonly compactions: ReadonlyMap<CompactionId, CompactionRecord>;
  readonly context: ContextRecord;
  /** Rolling model-token usage totals derived from completed provider inferences. */
  readonly tokenConsumption: TokenConsumptionState;
  /** Materialized scheduler queues derived from the maps above after every fold. */
  readonly commandQueues: CommandQueues;
}

/** Durable recovery continuation plus the sequence of its completion barrier. */
export interface RecoveryContinuationRecord extends RecoveryContinuation {
  readonly seq: SequenceNumber;
}

/** Unfinished lifecycle records requiring deterministic startup recovery. */
export interface RecoverableWork {
  readonly activeCommands: ReadonlyArray<CommandRecord>;
  readonly activeRuns: ReadonlyArray<RunRecord>;
  readonly activeTurns: ReadonlyArray<TurnRecord>;
  readonly activeInferences: ReadonlyArray<InferenceRecord>;
  readonly openToolCalls: ReadonlyArray<ToolCallRecord>;
  readonly runningToolCalls: ReadonlyArray<ToolCallRecord>;
  readonly pendingStopRequests: ReadonlyArray<StopRequestRecord>;
  readonly openCompactions: ReadonlyArray<CompactionRecord>;
}

const emptyTokenUsageTotals: TokenUsageTotals = {
  inputTokens: 0,
  cachedInputTokens: 0,
  outputTokens: 0,
  textTokens: 0,
  reasoningTokens: 0,
};

const emptyTokenConsumptionState: TokenConsumptionState = {
  total: emptyTokenUsageTotals,
};

/** Empty durable replay state before any session event has committed. */
export const initialReducedState: ReducedState = {
  lastSeq: SequenceNumber.make(0),
  commands: new Map(),
  runs: new Map(),
  recoveryContinuations: new Map(),
  turns: new Map(),
  inferences: new Map(),
  toolCalls: new Map(),
  messages: new Map(),
  stopRequests: new Map(),
  compactions: new Map(),
  context: { version: ContextVersion.make(0) },
  tokenConsumption: emptyTokenConsumptionState,
  commandQueues: emptyCommandQueues,
};

/** Reserved reducer checkpoint name for the framework-owned `ReducedState`. */
export const frameworkReducedStateReducerName = "_eda.framework.reduced-state";

/** Schema version for the framework-owned `ReducedState` checkpoint payload. */
export const frameworkReducedStateReducerSchemaVersion = 4;

/** JSON payload stored for the framework-owned reduced-state reducer checkpoint. */
export type ReducedStateCheckpointCommandRecord = Omit<CommandRecord, "command">;

type SystemMessageRecord = Extract<MessageRecord, { readonly _tag: "System" }>;
type UserMessageRecord = Extract<MessageRecord, { readonly _tag: "User" }>;
type SteeringMessageRecord = Extract<MessageRecord, { readonly _tag: "Steering" }>;
type AssistantMessageRecord = Extract<MessageRecord, { readonly _tag: "Assistant" }>;
type AssistantPartialMessageRecord = Extract<MessageRecord, { readonly _tag: "AssistantPartial" }>;

export type ReducedStateCheckpointMessageRecord =
  | Omit<SystemMessageRecord, "content">
  | Omit<UserMessageRecord, "content">
  | Omit<SteeringMessageRecord, "content">
  | Omit<AssistantMessageRecord, "content" | "promptParts">
  | Omit<AssistantPartialMessageRecord, "content" | "promptParts">;

type CreatedToolDecision = Extract<ToolDecision, { readonly _tag: "Created" }>;
type RejectedToolDecision = Extract<ToolDecision, { readonly _tag: "Rejected" }>;
type CompletedToolTerminal = Extract<ToolTerminal, { readonly _tag: "Completed" }>;
type FailedToolTerminal = Extract<ToolTerminal, { readonly _tag: "Failed" }>;

export type ReducedStateCheckpointToolDecision =
  | Omit<CreatedToolDecision, "params" | "promptPart">
  | Omit<RejectedToolDecision, "promptPart">;

export type ReducedStateCheckpointToolTerminal =
  | Omit<CompletedToolTerminal, "result" | "promptPart">
  | Omit<FailedToolTerminal, "error" | "promptPart">;

export interface ReducedStateCheckpointToolCallRecord extends Omit<
  ToolCallRecord,
  "decision" | "terminal"
> {
  readonly decision?: ReducedStateCheckpointToolDecision;
  readonly terminal?: ReducedStateCheckpointToolTerminal;
}

export interface ReducedStateCheckpointPayload {
  readonly lastSeq: SequenceNumber;
  readonly commands: ReadonlyArray<readonly [CommandId, ReducedStateCheckpointCommandRecord]>;
  readonly runs: ReadonlyArray<readonly [RunId, RunRecord]>;
  readonly recoveryContinuations?: ReadonlyArray<readonly [RunId, RecoveryContinuationRecord]>;
  readonly turns: ReadonlyArray<readonly [TurnId, TurnRecord]>;
  readonly inferences: ReadonlyArray<readonly [InferenceId, InferenceRecord]>;
  readonly toolCalls: ReadonlyArray<readonly [ToolCallId, ReducedStateCheckpointToolCallRecord]>;
  readonly messages: ReadonlyArray<readonly [MessageId, ReducedStateCheckpointMessageRecord]>;
  readonly stopRequests: ReadonlyArray<readonly [CommandId, StopRequestRecord]>;
  readonly compactions: ReadonlyArray<readonly [CompactionId, CompactionRecord]>;
  readonly context: ContextRecord;
  readonly tokenConsumption?: TokenConsumptionState;
}

/** Encode the framework reducer state into a pointer-based JSON-safe checkpoint payload. */
export const encodeReducedStateCheckpoint = (
  state: ReducedState,
): ReducedStateCheckpointPayload => ({
  lastSeq: state.lastSeq,
  commands: Array.from(state.commands.entries()).map(([commandId, record]) => [
    commandId,
    encodeCheckpointCommandRecord(record),
  ]),
  runs: Array.from(state.runs.entries()),
  recoveryContinuations: Array.from(state.recoveryContinuations.entries()),
  turns: Array.from(state.turns.entries()),
  inferences: Array.from(state.inferences.entries()),
  toolCalls: Array.from(state.toolCalls.entries()).map(([toolCallId, record]) => [
    toolCallId,
    encodeCheckpointToolCallRecord(record),
  ]),
  messages: Array.from(state.messages.entries()).map(([messageId, record]) => [
    messageId,
    encodeCheckpointMessageRecord(record),
  ]),
  stopRequests: Array.from(state.stopRequests.entries()),
  compactions: Array.from(state.compactions.entries()),
  context: state.context,
  tokenConsumption: state.tokenConsumption,
});

/** Event-log positions required to hydrate a pointer-based framework checkpoint. */
export const reducedStateCheckpointEventSeqs = (
  payload: unknown,
): ReadonlyArray<SequenceNumber> => {
  const checkpoint = payload as ReducedStateCheckpointPayload;
  const seqs: Array<SequenceNumber> = [];
  for (const [, record] of checkpoint.commands ?? []) {
    if (record.admittedSeq !== undefined) {
      seqs.push(record.admittedSeq);
    }
  }
  for (const [, record] of checkpoint.messages ?? []) {
    seqs.push(record.seq);
  }
  for (const [, record] of checkpoint.toolCalls ?? []) {
    if (record.decision !== undefined) {
      seqs.push(record.decision.seq);
    }
    if (
      record.terminal !== undefined &&
      (record.terminal._tag === "Completed" || record.terminal._tag === "Failed")
    ) {
      seqs.push(record.terminal.seq);
    }
  }
  return uniqueSequenceNumbers(seqs);
};

/** Decode the framework reducer checkpoint and rebuild derived scheduler queues. */
export const decodeReducedStateCheckpoint = (
  payload: unknown,
  referencedEvents: ReadonlyArray<CommittedDurableEvent>,
): ReducedState => {
  const checkpoint = payload as ReducedStateCheckpointPayload;
  const eventsBySeq = committedEventMapBySeq(referencedEvents);
  const commands = decodeCheckpointCommandRecords(checkpoint.commands ?? [], eventsBySeq);
  const runs = new Map(checkpoint.runs ?? []);
  const messages = decodeCheckpointMessageRecords(checkpoint.messages ?? [], eventsBySeq);
  return {
    lastSeq: SequenceNumber.make(Number(checkpoint.lastSeq ?? 0)),
    commands,
    runs,
    recoveryContinuations: new Map(checkpoint.recoveryContinuations ?? []),
    turns: new Map(checkpoint.turns ?? []),
    inferences: new Map(checkpoint.inferences ?? []),
    toolCalls: decodeCheckpointToolCallRecords(checkpoint.toolCalls ?? [], eventsBySeq),
    messages,
    stopRequests: new Map(checkpoint.stopRequests ?? []),
    compactions: new Map(checkpoint.compactions ?? []),
    context: checkpoint.context ?? { version: ContextVersion.make(0) },
    tokenConsumption: decodeTokenConsumptionState(checkpoint.tokenConsumption),
    commandQueues: deriveCommandQueues({ commands, runs, messages }),
  };
};

const encodeCheckpointCommandRecord = (
  record: CommandRecord,
): ReducedStateCheckpointCommandRecord => {
  const { command: _command, ...checkpoint } = record;
  return checkpoint;
};

const encodeCheckpointMessageRecord = (
  record: MessageRecord,
): ReducedStateCheckpointMessageRecord => {
  switch (record._tag) {
    case "System": {
      const { content: _content, ...checkpoint } = record;
      return checkpoint;
    }
    case "User": {
      const { content: _content, ...checkpoint } = record;
      return checkpoint;
    }
    case "Steering": {
      const { content: _content, ...checkpoint } = record;
      return checkpoint;
    }
    case "Assistant": {
      const { content: _content, promptParts: _promptParts, ...checkpoint } = record;
      return checkpoint;
    }
    case "AssistantPartial": {
      const { content: _content, promptParts: _promptParts, ...checkpoint } = record;
      return checkpoint;
    }
    default:
      return assertNever(record, "checkpoint message record");
  }
};

const encodeCheckpointToolCallRecord = (
  record: ToolCallRecord,
): ReducedStateCheckpointToolCallRecord => ({
  toolCallId: record.toolCallId,
  ...(record.decision === undefined
    ? {}
    : { decision: encodeCheckpointToolDecision(record.decision) }),
  ...(record.durationMs === undefined ? {} : { durationMs: record.durationMs }),
  ...(record.startedAtMs === undefined ? {} : { startedAtMs: record.startedAtMs }),
  ...(record.startedSeq === undefined ? {} : { startedSeq: record.startedSeq }),
  ...(record.terminalAtMs === undefined ? {} : { terminalAtMs: record.terminalAtMs }),
  ...(record.terminal === undefined
    ? {}
    : { terminal: encodeCheckpointToolTerminal(record.terminal) }),
});

const encodeCheckpointToolDecision = (
  decision: ToolDecision,
): ReducedStateCheckpointToolDecision => {
  switch (decision._tag) {
    case "Created": {
      const { params: _params, promptPart: _promptPart, ...checkpoint } = decision;
      return checkpoint;
    }
    case "Rejected": {
      const { promptPart: _promptPart, ...checkpoint } = decision;
      return checkpoint;
    }
    default:
      return assertNever(decision, "checkpoint tool decision");
  }
};

const encodeCheckpointToolTerminal = (
  terminal: ToolTerminal,
): ReducedStateCheckpointToolTerminal => {
  switch (terminal._tag) {
    case "Completed": {
      const { result: _result, promptPart: _promptPart, ...checkpoint } = terminal;
      return checkpoint;
    }
    case "Failed": {
      const { error: _error, promptPart: _promptPart, ...checkpoint } = terminal;
      return checkpoint;
    }
    default:
      return assertNever(terminal, "checkpoint tool terminal");
  }
};

const decodeCheckpointCommandRecords = (
  records: ReducedStateCheckpointPayload["commands"],
  eventsBySeq: ReadonlyMap<number, CommittedDurableEvent>,
): ReadonlyMap<CommandId, CommandRecord> =>
  new Map(
    records.map(([commandId, record]) => {
      if (record.admittedSeq === undefined) {
        return [commandId, record];
      }
      const admitted = reduceSingleCheckpointEvent(
        eventsBySeq,
        record.admittedSeq,
        commandAdmittedEventType,
      ).commands.get(commandId);
      if (admitted?.command === undefined) {
        throw new Error(`Checkpoint command ${commandId} missing admission payload`);
      }
      return [commandId, { ...record, command: admitted.command }];
    }),
  );

const decodeCheckpointMessageRecords = (
  records: ReducedStateCheckpointPayload["messages"],
  eventsBySeq: ReadonlyMap<number, CommittedDurableEvent>,
): ReadonlyMap<MessageId, MessageRecord> =>
  new Map(
    records.map(([messageId, record]) => {
      const message = reduceSingleCheckpointEvent(
        eventsBySeq,
        record.seq,
        checkpointMessageEventType(record),
      ).messages.get(messageId);
      if (message === undefined) {
        throw new Error(`Checkpoint message ${messageId} missing message payload`);
      }
      return [messageId, { ...message, ...record } as MessageRecord];
    }),
  );

const decodeCheckpointToolCallRecords = (
  records: ReducedStateCheckpointPayload["toolCalls"],
  eventsBySeq: ReadonlyMap<number, CommittedDurableEvent>,
): ReadonlyMap<ToolCallId, ToolCallRecord> =>
  new Map(
    records.map(([toolCallId, record]) => [
      toolCallId,
      {
        toolCallId,
        ...(record.decision === undefined
          ? {}
          : { decision: decodeCheckpointToolDecision(toolCallId, record.decision, eventsBySeq) }),
        ...(record.durationMs === undefined ? {} : { durationMs: record.durationMs }),
        ...(record.startedAtMs === undefined ? {} : { startedAtMs: record.startedAtMs }),
        ...(record.startedSeq === undefined ? {} : { startedSeq: record.startedSeq }),
        ...(record.terminalAtMs === undefined ? {} : { terminalAtMs: record.terminalAtMs }),
        ...(record.terminal === undefined
          ? {}
          : { terminal: decodeCheckpointToolTerminal(toolCallId, record.terminal, eventsBySeq) }),
      },
    ]),
  );

const decodeCheckpointToolDecision = (
  toolCallId: ToolCallId,
  decision: ReducedStateCheckpointToolDecision,
  eventsBySeq: ReadonlyMap<number, CommittedDurableEvent>,
): ToolDecision => {
  const hydrated = reduceSingleCheckpointEvent(
    eventsBySeq,
    decision.seq,
    decision._tag === "Created" ? toolCallCreatedEventType : toolCallRejectedEventType,
  ).toolCalls.get(toolCallId)?.decision;
  if (hydrated === undefined || hydrated._tag !== decision._tag) {
    throw new Error(`Checkpoint tool decision ${toolCallId} missing ${decision._tag} payload`);
  }
  return hydrated;
};

const decodeCheckpointToolTerminal = (
  toolCallId: ToolCallId,
  terminal: ReducedStateCheckpointToolTerminal,
  eventsBySeq: ReadonlyMap<number, CommittedDurableEvent>,
): ToolTerminal => {
  const hydrated = reduceSingleCheckpointEvent(
    eventsBySeq,
    terminal.seq,
    terminal._tag === "Completed" ? toolCallCompletedEventType : toolCallFailedEventType,
  ).toolCalls.get(toolCallId)?.terminal;
  if (hydrated === undefined || hydrated._tag !== terminal._tag) {
    throw new Error(`Checkpoint tool terminal ${toolCallId} missing ${terminal._tag} payload`);
  }
  return hydrated;
};

const checkpointMessageEventType = (record: ReducedStateCheckpointMessageRecord) => {
  switch (record._tag) {
    case "System":
      return systemMessageCommittedEventType;
    case "User":
      return record.requestedDisposition === undefined
        ? userMessageCommittedEventType
        : userMessageSubmittedEventType;
    case "Steering":
      return steeringMessageQueuedEventType;
    case "Assistant":
      return assistantMessageCommittedEventType;
    case "AssistantPartial":
      return assistantPartialCommittedEventType;
    default:
      return assertNever(record, "checkpoint message type");
  }
};

const reduceSingleCheckpointEvent = (
  eventsBySeq: ReadonlyMap<number, CommittedDurableEvent>,
  seq: SequenceNumber,
  expectedType: string,
): ReducedState => {
  const event = eventsBySeq.get(Number(seq));
  if (event === undefined) {
    throw new Error(`Checkpoint reference seq ${seq} is missing`);
  }
  if (event.event.type !== expectedType) {
    throw new Error(
      `Checkpoint reference seq ${seq} expected ${expectedType}; got ${event.event.type}`,
    );
  }
  return foldReducedState(initialReducedState, [event]);
};

const committedEventMapBySeq = (
  events: ReadonlyArray<CommittedDurableEvent>,
): ReadonlyMap<number, CommittedDurableEvent> =>
  new Map(events.map((event) => [Number(event.position.seq), event]));

const uniqueSequenceNumbers = (
  seqs: ReadonlyArray<SequenceNumber>,
): ReadonlyArray<SequenceNumber> =>
  Array.from(new Set(seqs.map((seq) => Number(seq))))
    .sort((left, right) => left - right)
    .map((seq) => SequenceNumber.make(seq));

const decodeTokenConsumptionState = (
  value: TokenConsumptionState | undefined,
): TokenConsumptionState => ({
  total: {
    inputTokens: nonNegativeNumber(value?.total?.inputTokens),
    cachedInputTokens: nonNegativeNumber(value?.total?.cachedInputTokens),
    outputTokens: nonNegativeNumber(value?.total?.outputTokens),
    textTokens: nonNegativeNumber(value?.total?.textTokens),
    reasoningTokens: nonNegativeNumber(value?.total?.reasoningTokens),
  },
});

/** Fold newly committed durable events into the canonical replay state. */
export const foldReducedState = (
  state: ReducedState,
  committed: ReadonlyArray<CommittedDurableEvent>,
): ReducedState => {
  let lastSeq = state.lastSeq;
  const commands = new Map(state.commands);
  const runs = new Map(state.runs);
  const recoveryContinuations = new Map(state.recoveryContinuations);
  const turns = new Map(state.turns);
  const inferences = new Map(state.inferences);
  const toolCalls = new Map(state.toolCalls);
  const messages = new Map(state.messages);
  const stopRequests = new Map(state.stopRequests);
  const compactions = new Map(state.compactions);
  let context = state.context;
  let tokenConsumption = state.tokenConsumption;

  for (const entry of committed) {
    lastSeq = entry.position.seq;
    const { event } = entry;
    if (event.namespace !== effectDurableAgentNamespace) {
      continue;
    }

    const payload = event.payload as any;
    const seq = entry.position.seq;
    const eventCreatedAtMs = Number(event.createdAtMs);

    switch (event.type) {
      case commandAdmittedEventType: {
        const { command } = payload;
        const commandId = requireAdmittedCommandId(command);
        upsert(commands, commandId, { commandId }, (record) => ({
          ...record,
          admittedSeq: seq,
          admissionTrace: event.trace,
          command,
        }));
        break;
      }
      case commandStartedEventType: {
        const { commandId } = payload;
        upsert(commands, commandId, { commandId }, (record) => ({ ...record, startedSeq: seq }));
        break;
      }
      case commandCompletedEventType: {
        const { commandId } = payload;
        upsert(commands, commandId, { commandId }, (record) => ({
          ...record,
          terminal: { _tag: "Completed", seq } as CommandTerminal,
        }));
        break;
      }
      case commandFailedEventType: {
        const { commandId, error } = payload;
        upsert(commands, commandId, { commandId }, (record) => ({
          ...record,
          terminal: { _tag: "Failed", seq, error } as CommandTerminal,
        }));
        break;
      }
      case commandCancelledEventType: {
        const { commandId, reason } = payload;
        upsert(commands, commandId, { commandId }, (record) => ({
          ...record,
          terminal: { _tag: "Cancelled", seq, reason } as CommandTerminal,
        }));
        break;
      }
      case runStartedEventType: {
        const { runId, commandIds, trace } = payload;
        const terminal = runs.get(runId)?.terminal;
        runs.set(runId, {
          runId,
          commandIds,
          startedAtMs: eventCreatedAtMs,
          startedSeq: seq,
          trace,
          ...(terminal === undefined ? {} : { terminal }),
        });
        break;
      }
      case runCompletedEventType: {
        const { runId } = payload;
        updateExisting(runs, runId, (record) => ({
          ...record,
          ...terminalTiming(record, eventCreatedAtMs),
          terminal: { _tag: "Completed", seq } as RunTerminal,
        }));
        break;
      }
      case runFailedEventType: {
        const { runId, error } = payload;
        updateExisting(runs, runId, (record) => ({
          ...record,
          ...terminalTiming(record, eventCreatedAtMs),
          terminal: { _tag: "Failed", seq, error } as RunTerminal,
        }));
        break;
      }
      case runInterruptedEventType: {
        const { runId, reason } = payload;
        updateExisting(runs, runId, (record) => ({
          ...record,
          ...terminalTiming(record, eventCreatedAtMs),
          terminal: { _tag: "Interrupted", seq, reason } as RunTerminal,
        }));
        break;
      }
      case turnStartedEventType: {
        const { runId, turnId, inputMessageIds } = payload;
        const terminal = turns.get(turnId)?.terminal;
        turns.set(turnId, {
          runId,
          turnId,
          startedAtMs: eventCreatedAtMs,
          startedSeq: seq,
          ...(terminal === undefined ? {} : { terminal }),
        });
        for (const messageId of inputMessageIds ?? []) {
          const message = messages.get(messageId);
          if (message?._tag === "Steering" || message?._tag === "User") {
            messages.set(messageId, { ...message, consumedSeq: seq, consumedTurnId: turnId });
          }
        }
        break;
      }
      case turnCompletedEventType: {
        const { turnId, usage } = payload;
        updateExisting(turns, turnId, (record) => ({
          ...record,
          ...terminalTiming(record, eventCreatedAtMs),
          terminal: {
            _tag: "Completed",
            seq,
            ...(usage === undefined ? {} : { usage }),
          } as TurnTerminal,
        }));
        break;
      }
      case turnFailedEventType: {
        const { turnId, error } = payload;
        updateExisting(turns, turnId, (record) => ({
          ...record,
          ...terminalTiming(record, eventCreatedAtMs),
          terminal: { _tag: "Failed", seq, error } as TurnTerminal,
        }));
        break;
      }
      case turnStoppedEventType: {
        const { turnId, reason } = payload;
        updateExisting(turns, turnId, (record) => ({
          ...record,
          ...terminalTiming(record, eventCreatedAtMs),
          terminal: { _tag: "Stopped", seq, reason } as TurnTerminal,
        }));
        break;
      }
      case inferenceStartedEventType: {
        const { runId, turnId, inferenceId } = payload;
        const terminal = inferences.get(inferenceId)?.terminal;
        inferences.set(inferenceId, {
          runId,
          turnId,
          inferenceId: inferenceId,
          startedAtMs: eventCreatedAtMs,
          startedSeq: seq,
          ...(terminal === undefined ? {} : { terminal }),
        });
        break;
      }
      case inferenceCompletedEventType: {
        const { inferenceId, usage } = payload;
        const previouslyCompleted = inferences.get(inferenceId)?.terminal?._tag === "Completed";
        updateExisting(inferences, inferenceId, (record) => ({
          ...record,
          ...terminalTiming(record, eventCreatedAtMs),
          terminal: {
            _tag: "Completed",
            seq,
            ...(usage === undefined ? {} : { usage }),
          } as InferenceTerminal,
        }));
        if (!previouslyCompleted && inferences.get(inferenceId)?.terminal?._tag === "Completed") {
          tokenConsumption = addTokenUsage(tokenConsumption, usage);
        }
        break;
      }
      case inferenceFailedEventType: {
        const { inferenceId, error } = payload;
        updateExisting(inferences, inferenceId, (record) => ({
          ...record,
          ...terminalTiming(record, eventCreatedAtMs),
          terminal: { _tag: "Failed", seq, error } as InferenceTerminal,
        }));
        break;
      }
      case toolCallCreatedEventType: {
        const { runId, turnId, inferenceId, toolCallId, promptPart } = payload;
        upsert(toolCalls, toolCallId, { toolCallId }, (record) => ({
          ...record,
          decision: {
            _tag: "Created",
            seq,
            runId,
            turnId,
            inferenceId: inferenceId,
            providerPartId: promptPart.id,
            toolName: promptPart.name,
            params: promptPart.params,
            providerExecuted: promptPart.providerExecuted,
            promptPart,
          } as ToolDecision,
        }));
        break;
      }
      case toolCallRejectedEventType: {
        const { runId, turnId, inferenceId, toolCallId, promptPart } = payload;
        upsert(toolCalls, toolCallId, { toolCallId }, (record) => ({
          ...record,
          decision: {
            _tag: "Rejected",
            seq,
            runId,
            turnId,
            inferenceId: inferenceId,
            providerPartId: promptPart.id,
            toolName: promptPart.name,
            reason: promptPart.result.reason,
            modelFeedback: promptPart.result.modelFeedback,
            promptPart,
          } as ToolDecision,
        }));
        break;
      }
      case toolCallStartedEventType: {
        const { toolCallId } = payload;
        upsert(toolCalls, toolCallId, { toolCallId }, (record) => ({
          ...record,
          startedAtMs: eventCreatedAtMs,
          startedSeq: seq,
        }));
        break;
      }
      case toolCallCompletedEventType: {
        const { toolCallId, promptPart } = payload;
        upsert(toolCalls, toolCallId, { toolCallId }, (record) => ({
          ...record,
          ...terminalTiming(record, eventCreatedAtMs),
          terminal: {
            _tag: "Completed",
            seq,
            result: promptPart.result,
            promptPart,
          } as ToolTerminal,
        }));
        break;
      }
      case toolCallFailedEventType: {
        const { toolCallId, promptPart, error } = payload;
        upsert(toolCalls, toolCallId, { toolCallId }, (record) => ({
          ...record,
          ...terminalTiming(record, eventCreatedAtMs),
          terminal: {
            _tag: "Failed",
            seq,
            error: error ?? promptPart.result,
            promptPart,
          } as ToolTerminal,
        }));
        break;
      }
      case systemMessageCommittedEventType: {
        const { messageId, content } = payload;
        messages.set(messageId, {
          _tag: "System",
          messageId,
          content,
          ...(Number.isFinite(eventCreatedAtMs) ? { createdAtMs: eventCreatedAtMs } : {}),
          seq,
        });
        break;
      }
      case userMessageCommittedEventType: {
        const { commandId, messageId, content } = payload;
        messages.set(messageId, {
          _tag: "User",
          messageId,
          commandId,
          content,
          ...(Number.isFinite(eventCreatedAtMs) ? { createdAtMs: eventCreatedAtMs } : {}),
          seq,
        });
        break;
      }
      case userMessageSubmittedEventType: {
        const { commandId, messageId, disposition: requestedDisposition, content } = payload;
        messages.set(messageId, {
          _tag: "User",
          messageId,
          commandId,
          content,
          requestedDisposition,
          disposition: requestedDisposition,
          ...(Number.isFinite(eventCreatedAtMs) ? { createdAtMs: eventCreatedAtMs } : {}),
          seq,
        });
        break;
      }
      case userMessagePromotedEventType: {
        const { messageId } = payload;
        const message = messages.get(messageId);
        if (
          (message?._tag === "User" && message.disposition === "queue") ||
          (message?._tag === "Steering" && message.pausedByCommandId !== undefined)
        ) {
          const {
            pausedByCommandId: _pausedByCommandId,
            pausedSeq: _pausedSeq,
            ...unpausedMessage
          } = message;
          messages.set(messageId, {
            ...unpausedMessage,
            ...(message._tag === "User" ? { disposition: "steer" as const } : {}),
            promotedSeq: seq,
          });
        }
        break;
      }
      case userMessageCancelledEventType: {
        const { commandId, messageId, reason } = payload;
        const message = messages.get(messageId);
        if (message?._tag === "User" || message?._tag === "Steering") {
          messages.set(messageId, {
            ...message,
            cancelledSeq: seq,
            cancelledByCommandId: commandId,
            cancellationReason: reason,
          });
        }
        break;
      }
      case messageQueuePausedEventType:
      case pendingMessagesPausedEventType: {
        const messageIds = payload.messageIds;
        const interruptionCommandId =
          event.type === messageQueuePausedEventType
            ? payload.stopCommandId
            : payload.interruptionCommandId;
        for (const messageId of messageIds) {
          const message = messages.get(messageId);
          if (
            (message?._tag === "User" || message?._tag === "Steering") &&
            message.consumedSeq === undefined &&
            message.cancelledSeq === undefined
          ) {
            messages.set(messageId, {
              ...message,
              ...(message._tag === "User" ? { disposition: "queue" as const } : {}),
              pausedSeq: seq,
              pausedByCommandId: interruptionCommandId,
            });
          }
        }
        break;
      }
      case steeringMessageQueuedEventType: {
        const { commandId, messageId, runId, content } = payload;
        messages.set(messageId, {
          _tag: "Steering",
          messageId,
          commandId,
          runId,
          content,
          ...(Number.isFinite(eventCreatedAtMs) ? { createdAtMs: eventCreatedAtMs } : {}),
          seq,
        });
        break;
      }
      case steeringMessageCancelledEventType: {
        const { messageId, reason } = payload;
        const message = messages.get(messageId);
        if (message?._tag === "Steering") {
          messages.set(messageId, { ...message, cancelledSeq: seq, cancellationReason: reason });
        }
        break;
      }
      case assistantMessageCommittedEventType: {
        const { messageId, runId, turnId, inferenceId, promptParts } = payload;
        messages.set(messageId, {
          _tag: "Assistant",
          messageId,
          runId,
          turnId,
          inferenceId: inferenceId,
          content: assistantContentFromPromptParts(promptParts),
          ...(Number.isFinite(eventCreatedAtMs) ? { createdAtMs: eventCreatedAtMs } : {}),
          promptParts,
          seq,
        });
        break;
      }
      case assistantPartialCommittedEventType: {
        const { messageId, runId, turnId, inferenceId, promptParts, reason } = payload;
        messages.set(messageId, {
          _tag: "AssistantPartial",
          messageId,
          runId,
          turnId,
          inferenceId: inferenceId,
          content: assistantContentFromPromptParts(promptParts),
          ...(Number.isFinite(eventCreatedAtMs) ? { createdAtMs: eventCreatedAtMs } : {}),
          promptParts,
          reason,
          seq,
        });
        break;
      }
      case stopTurnRequestedEventType: {
        const { commandId, runId, turnId } = payload;
        stopRequests.set(commandId, {
          commandId,
          requestedSeq: seq,
          ...(runId === undefined ? {} : { requestedRunId: runId }),
          ...(turnId === undefined ? {} : { requestedTurnId: turnId }),
          ...appliedFields(stopRequests.get(commandId)),
        });
        break;
      }
      case stopTurnAppliedEventType: {
        const { commandId, runId, turnId, inferenceId } = payload;
        const existing = stopRequests.get(commandId);
        stopRequests.set(commandId, {
          commandId,
          requestedSeq: existing?.requestedSeq ?? seq,
          ...(existing?.requestedRunId === undefined
            ? {}
            : { requestedRunId: existing.requestedRunId }),
          ...(existing?.requestedTurnId === undefined
            ? {}
            : { requestedTurnId: existing.requestedTurnId }),
          appliedSeq: seq,
          appliedRunId: runId,
          appliedTurnId: turnId,
          ...(inferenceId === undefined ? {} : { appliedInferenceId: inferenceId }),
        });
        break;
      }
      case contextProjectedEventType: {
        const { contextVersion } = payload;
        context = { ...context, version: contextVersion };
        break;
      }
      case compactionRequestedEventType: {
        const { compactionId, sourceFromSeq, sourceToSeq } = payload;
        upsert(compactions, compactionId, { compactionId }, (record) => ({
          ...record,
          requestedSeq: seq,
          sourceFromSeq,
          sourceToSeq,
        }));
        break;
      }
      case compactionStartedEventType: {
        const { compactionId } = payload;
        upsert(compactions, compactionId, { compactionId }, (record) => ({
          ...record,
          startedSeq: seq,
        }));
        break;
      }
      case summaryCreatedEventType: {
        const { compactionId, summaryId, sourceFromSeq, sourceToSeq } = payload;
        upsert(compactions, compactionId, { compactionId }, (record) => ({
          ...record,
          summaryCreatedSeq: seq,
          summaryId,
          sourceFromSeq,
          sourceToSeq,
        }));
        break;
      }
      case contextRebasedEventType: {
        const { compactionId, summaryId, contextVersion, retainedFromContextSeq } = payload;
        upsert(compactions, compactionId, { compactionId }, (record) => ({
          ...record,
          summaryId,
          rebasedSeq: seq,
          contextVersion,
        }));
        pruneForContextRebase(
          {
            commands,
            runs,
            recoveryContinuations,
            turns,
            inferences,
            toolCalls,
            messages,
            stopRequests,
            compactions,
          },
          retainedFromContextSeq,
          summaryId,
        );
        context = {
          version: contextVersion,
          currentSummaryId: summaryId,
        };
        break;
      }
      case compactionCompletedEventType: {
        const { compactionId } = payload;
        upsert(compactions, compactionId, { compactionId }, (record) => ({
          ...record,
          terminal: { _tag: "Completed", seq } as CompactionTerminal,
        }));
        break;
      }
      case compactionFailedEventType: {
        const { compactionId, error } = payload;
        upsert(compactions, compactionId, { compactionId }, (record) => ({
          ...record,
          terminal: { _tag: "Failed", seq, error } as CompactionTerminal,
        }));
        const failed = compactions.get(compactionId);
        if (failed?.summaryId === undefined || failed.summaryId !== context.currentSummaryId) {
          compactions.delete(compactionId);
        }
        break;
      }
      case recoveryCompletedEventType: {
        const { continuation } = payload;
        if (continuation !== undefined) {
          recoveryContinuations.set(continuation.replacementRunId, { ...continuation, seq });
        }
        break;
      }
      case baseStateRequestedEventType:
      case baseStateCreatedEventType:
      case baseStateFailedEventType:
        break;
      default:
        assertNever(event as never, "reduced-state durable event");
    }
  }

  return {
    lastSeq,
    commands,
    runs,
    recoveryContinuations,
    turns,
    inferences,
    toolCalls,
    messages,
    stopRequests,
    compactions,
    context,
    tokenConsumption,
    commandQueues: deriveCommandQueues({ commands, runs, messages }),
  };
};

/** Fold committed events from genesis into a fresh `ReducedState`. */
export const reduceCommittedEvents = (committed: ReadonlyArray<CommittedDurableEvent>) =>
  foldReducedState(initialReducedState, committed);

const addTokenUsage = (
  state: TokenConsumptionState,
  usage: UsagePayload | undefined,
): TokenConsumptionState => {
  if (usage === undefined) {
    return state;
  }
  return {
    total: {
      inputTokens: state.total.inputTokens + tokenCount(usage.inputTokens),
      cachedInputTokens: state.total.cachedInputTokens + tokenCount(usage.cachedInputTokens),
      outputTokens: state.total.outputTokens + tokenCount(usage.outputTokens),
      textTokens: state.total.textTokens + tokenCount(usage.textTokens),
      reasoningTokens: state.total.reasoningTokens + tokenCount(usage.reasoningTokens),
    },
  };
};

const tokenCount = (value: number | undefined): number => nonNegativeNumber(value);

const nonNegativeNumber = (value: number | undefined): number =>
  value === undefined || !Number.isFinite(value) ? 0 : Math.max(0, Math.trunc(value));

interface MutableReplayMaps {
  readonly commands: Map<CommandId, CommandRecord>;
  readonly runs: Map<RunId, RunRecord>;
  readonly recoveryContinuations: Map<RunId, RecoveryContinuationRecord>;
  readonly turns: Map<TurnId, TurnRecord>;
  readonly inferences: Map<InferenceId, InferenceRecord>;
  readonly toolCalls: Map<ToolCallId, ToolCallRecord>;
  readonly messages: Map<MessageId, MessageRecord>;
  readonly stopRequests: Map<CommandId, StopRequestRecord>;
  readonly compactions: Map<CompactionId, CompactionRecord>;
}

/** Drop runtime state made model-invisible by a context rebase; storage remains append-only. */
const pruneForContextRebase = (
  maps: MutableReplayMaps,
  retainedFromContextSeq: SequenceNumber,
  currentSummaryId: SummaryId,
): void => {
  for (const [messageId, message] of maps.messages) {
    if (message._tag !== "System" && messagePruneSeq(message) < retainedFromContextSeq) {
      maps.messages.delete(messageId);
    }
  }

  for (const [toolCallId, tool] of maps.toolCalls) {
    const terminalSeq = tool.terminal?.seq;
    if (terminalSeq !== undefined && terminalSeq < retainedFromContextSeq) {
      maps.toolCalls.delete(toolCallId);
    }
  }

  for (const [inferenceId, inference] of maps.inferences) {
    if (inference.terminal !== undefined && inference.terminal.seq < retainedFromContextSeq) {
      maps.inferences.delete(inferenceId);
    }
  }

  for (const [turnId, turn] of maps.turns) {
    if (turn.terminal !== undefined && turn.terminal.seq < retainedFromContextSeq) {
      maps.turns.delete(turnId);
    }
  }

  for (const [runId, run] of maps.runs) {
    if (run.terminal !== undefined && run.terminal.seq < retainedFromContextSeq) {
      maps.runs.delete(runId);
    }
  }

  for (const replacementRunId of maps.recoveryContinuations.keys()) {
    if (!maps.runs.has(replacementRunId)) {
      maps.recoveryContinuations.delete(replacementRunId);
    }
  }

  for (const [commandId, command] of maps.commands) {
    if (command.terminal !== undefined && command.terminal.seq < retainedFromContextSeq) {
      maps.commands.delete(commandId);
    }
  }

  for (const [commandId, request] of maps.stopRequests) {
    if (request.appliedSeq !== undefined && request.appliedSeq < retainedFromContextSeq) {
      maps.stopRequests.delete(commandId);
    }
  }

  for (const [compactionId, compaction] of maps.compactions) {
    if (compaction.terminal !== undefined && compaction.summaryId !== currentSummaryId) {
      maps.compactions.delete(compactionId);
    }
  }
};

const messagePruneSeq = (message: Exclude<MessageRecord, { readonly _tag: "System" }>): number => {
  if (message._tag === "Steering") {
    return message.consumedSeq ?? message.cancelledSeq ?? Number.POSITIVE_INFINITY;
  }
  return message.seq;
};

/** Pending admitted commands in deterministic scheduler order. */
export const pendingCommands = (state: ReducedState): ReadonlyArray<PendingCommand> =>
  state.commandQueues.pendingCommands;

/** First pending StopTurn control command, when one is waiting. */
export const pendingStopCommand = (state: ReducedState): PendingCommand | undefined =>
  state.commandQueues.activeControlCommands.find((command) => command.command._tag === "StopTurn");

/** Currently active command evidence, if any unfinished command is blocking FIFO work. */
export const activeCommand = (state: ReducedState): ActiveCommandEvidence | undefined =>
  state.commandQueues.active;

/** Active run/turn/inference identity owned by a specific active command. */
export const activeTurnIdentityForCommand = (
  state: ReducedState,
  commandId: CommandId,
): ActiveTurnIdentity | undefined => {
  const run = findLast(Array.from(state.runs.values()), (candidate): candidate is RunRecord =>
    candidate.commandIds.includes(commandId),
  );
  if (run === undefined) {
    return undefined;
  }

  const turn = findLast(
    Array.from(state.turns.values()),
    (candidate): candidate is TurnRecord => candidate.runId === run.runId,
  );
  if (turn === undefined) {
    return undefined;
  }

  const inference = findLast(
    Array.from(state.inferences.values()),
    (candidate): candidate is InferenceRecord =>
      candidate.runId === run.runId && candidate.turnId === turn.turnId,
  );

  return {
    runId: run.runId,
    turnId: turn.turnId,
    ...(inference === undefined ? {} : { inferenceId: inference.inferenceId }),
  };
};

/** Classify unfinished durable lifecycles that recovery may need to repair. */
export const classifyRecoverableWork = (state: ReducedState): RecoverableWork => {
  const activeCommands = Array.from(state.commands.values()).filter(
    (command) => command.startedSeq !== undefined && command.terminal === undefined,
  );
  const activeRuns = Array.from(state.runs.values()).filter((run) => run.terminal === undefined);
  const activeTurns = Array.from(state.turns.values()).filter(
    (turn) => turn.terminal === undefined,
  );
  const activeInferences = Array.from(state.inferences.values()).filter(
    (inference) => inference.terminal === undefined,
  );
  const openToolCalls = Array.from(state.toolCalls.values()).filter(
    (tool) =>
      tool.decision?._tag === "Created" &&
      !tool.decision.providerExecuted &&
      tool.terminal === undefined,
  );
  const runningToolCalls = openToolCalls.filter((tool) => tool.startedSeq !== undefined);
  const pendingStopRequests = Array.from(state.stopRequests.values()).filter(
    (request) =>
      request.appliedSeq === undefined &&
      state.commands.get(request.commandId)?.terminal === undefined,
  );
  const openCompactions = Array.from(state.compactions.values()).filter(
    (compaction) => compaction.terminal === undefined,
  );

  return {
    activeCommands,
    activeRuns,
    activeTurns,
    activeInferences,
    openToolCalls,
    runningToolCalls,
    pendingStopRequests,
    openCompactions,
  };
};

const assistantContentFromPromptParts = (
  promptParts: ReadonlyArray<Prompt.AssistantMessagePart>,
): AssistantMessageContent => {
  const text = promptParts
    .filter((part): part is Prompt.TextPart => part.type === "text")
    .map((part) => part.text)
    .join("");
  const reasoning = promptParts
    .filter((part): part is Prompt.ReasoningPart => part.type === "reasoning")
    .map((part) => part.text)
    .join("");
  return {
    text,
    ...(reasoning.length === 0 ? {} : { reasoning }),
  };
};

const findLast = <A, B extends A>(
  items: ReadonlyArray<A>,
  predicate: (item: A) => item is B,
): B | undefined => {
  for (let index = items.length - 1; index >= 0; index -= 1) {
    const item = items[index] as A;
    if (predicate(item)) {
      return item;
    }
  }
  return undefined;
};

const requireAdmittedCommandId = (command: EDACommandValue): CommandId => {
  if (command.commandId !== undefined) {
    return command.commandId;
  }
  throw new Error("CommandAdmitted command is missing a framework commandId");
};

const terminalTiming = (
  record: Pick<LifecycleTiming, "startedAtMs">,
  terminalAtMs: number,
): LifecycleTiming => ({
  terminalAtMs,
  ...(record.startedAtMs === undefined
    ? {}
    : { durationMs: Math.max(0, terminalAtMs - record.startedAtMs) }),
});

const upsert = <K, V>(map: Map<K, V>, key: K, fallback: V, update: (record: V) => V) => {
  map.set(key, update(map.get(key) ?? fallback));
};

const updateExisting = <K, V>(map: Map<K, V>, key: K, update: (record: V) => V) => {
  const record = map.get(key);
  if (record !== undefined) {
    map.set(key, update(record));
  }
};

const appliedFields = (record: StopRequestRecord | undefined) =>
  record?.appliedSeq === undefined
    ? {}
    : {
        appliedSeq: record.appliedSeq,
        appliedRunId: record.appliedRunId,
        appliedTurnId: record.appliedTurnId,
        ...(record.appliedInferenceId === undefined
          ? {}
          : { appliedInferenceId: record.appliedInferenceId }),
      };
