import * as Schema from "effect/Schema";

import { assertNever } from "../domain/assert-never";
import { addModelUsage } from "../domain/model-usage";
import type { CommittedDurableEvent } from "../services/session-store";
import type { UserMessageContent } from "../types/commands";
import {
  type CommandId,
  type InferenceId,
  type MessageId,
  type RunId,
  SequenceNumber,
  type TurnId,
} from "../types/core";
import {
  ProviderPartId,
  UnixEpochMillis,
  effectDurableAgentNamespace,
  type FailurePayload,
  type ModelSelectionPayload,
} from "../types/events/envelope";
import {
  assistantMessageCommittedEventType,
  assistantMessageImportedEventType,
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
  contextRebasedEventType,
  inferenceCompletedEventType,
  inferenceFailedEventType,
  inferenceStartedEventType,
  interpretLegacyAttemptForReduction,
  messageQueuePausedEventType,
  pendingMessagesPausedEventType,
  recoveryCompletedEventType,
  runCompletedEventType,
  runFailedEventType,
  runInterruptedEventType,
  runSchedulingDeliveredEventType,
  runSchedulingGrantedEventType,
  runSchedulingInvalidatedEventType,
  runSchedulingRequestedEventType,
  runStartedEventType,
  sessionConfiguredEventType,
  steeringMessageCancelledEventType,
  steeringMessageQueuedEventType,
  stopTurnAppliedEventType,
  stopTurnRequestedEventType,
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
  userMessageCancelledEventType,
  userMessageCommittedEventType,
  userMessagePromotedEventType,
  userMessageSubmittedEventType,
  type AssistantPromptPart,
} from "../types/events/durable";
import { applyForeignEvent, replayBufferedDecorations } from "./mapping-executor";
import type { TranscriptDefinition } from "./mapping";
import {
  TranscriptProjectionError,
  type PlanTranscriptBatch,
  type TranscriptAlias,
  type TranscriptAliasNamespace,
  type TranscriptChangePlan,
  type TranscriptLookup,
} from "./plan";
import {
  runGroupStatus,
  transcriptRecordId,
  transcriptStoredInlineBodyLimit,
  type AssistantRecordId,
  type TranscriptAssistantPart,
  type TranscriptAssistantRecord,
  type TranscriptBody,
  type TranscriptFieldPath,
  type TranscriptCompactionRecord,
  type TranscriptPosition,
  type TranscriptRecord,
  type TranscriptRecordId,
  type TranscriptRunGroupRecord,
  type TranscriptRunMember,
  type TranscriptRunOutcome,
  type TranscriptSessionContextRecord,
  type TranscriptToolRecord,
  type TranscriptUserRecord,
  type TranscriptUserState,
} from "./records";

/**
 * Accumulates one batch's changes over a committed lookup.
 *
 * Every handler reads through this overlay, so later events in a batch see earlier ones, and the
 * plan is the same whichever way a log is split into batches: interpretation is a sequential fold
 * whose only state is the records themselves.
 */
export class TranscriptBatch {
  private readonly records = new Map<TranscriptRecordId, TranscriptRecord>();
  private readonly aliases = new Map<string, TranscriptAlias>();
  private readonly ordinals = new Map<number, number>();

  constructor(
    private readonly lookup: TranscriptLookup,
    readonly definition: TranscriptDefinition,
  ) {}

  get(id: TranscriptRecordId): TranscriptRecord | undefined {
    return this.records.get(id) ?? this.lookup.record(id);
  }

  put(record: TranscriptRecord): void {
    this.records.set(record.id, record);
  }

  user(id: TranscriptRecordId): TranscriptUserRecord | undefined {
    const record = this.get(id);
    if (record === undefined) return undefined;
    if (record.kind !== "user") throw kindMismatch(id, "user", record.kind);
    return record;
  }

  assistant(id: AssistantRecordId): TranscriptAssistantRecord | undefined {
    const record = this.get(id);
    if (record === undefined) return undefined;
    if (record.kind !== "assistant") throw kindMismatch(id, "assistant", record.kind);
    return record;
  }

  tool(id: TranscriptRecordId): TranscriptToolRecord | undefined {
    const record = this.get(id);
    if (record === undefined) return undefined;
    if (record.kind !== "tool") throw kindMismatch(id, "tool", record.kind);
    return record;
  }

  runGroup(id: TranscriptRecordId): TranscriptRunGroupRecord | undefined {
    const record = this.get(id);
    if (record === undefined) return undefined;
    if (record.kind !== "runGroup") throw kindMismatch(id, "runGroup", record.kind);
    return record;
  }

  session(seq: SequenceNumber): TranscriptSessionContextRecord {
    const record = this.get(transcriptRecordId.session);
    if (record === undefined) {
      return {
        kind: "sessionContext",
        id: transcriptRecordId.session,
        updatedSeq: seq,
        tokenUsage: { byModel: [] },
        decorations: {},
      };
    }
    if (record.kind !== "sessionContext") {
      throw kindMismatch(transcriptRecordId.session, "sessionContext", record.kind);
    }
    return record;
  }

  alias(namespace: TranscriptAliasNamespace, key: string): TranscriptRecordId | undefined {
    return (
      this.aliases.get(aliasKey(namespace, key))?.recordId ?? this.lookup.alias(namespace, key)
    );
  }

  /** Insert an alias unless one exists; an alias never moves to another record. */
  insertAlias(
    namespace: TranscriptAliasNamespace,
    key: string,
    recordId: TranscriptRecordId,
  ): void {
    if (this.alias(namespace, key) !== undefined) return;
    this.aliases.set(aliasKey(namespace, key), { namespace, key, recordId });
  }

  position(anchorSeq: SequenceNumber): TranscriptPosition {
    const ordinal = this.ordinals.get(anchorSeq) ?? this.lookup.nextOrdinal(anchorSeq);
    this.ordinals.set(anchorSeq, ordinal + 1);
    return { anchorSeq, ordinal };
  }

  plan(throughSeq: SequenceNumber): TranscriptChangePlan {
    return {
      records: Array.from(this.records.values()),
      aliases: Array.from(this.aliases.values()),
      throughSeq,
    };
  }
}

const aliasKey = (namespace: TranscriptAliasNamespace, key: string) => `${namespace}\u0000${key}`;

const kindMismatch = (id: string, expected: string, actual: string) =>
  new TranscriptProjectionError({ message: `record ${id} is ${actual}, expected ${expected}` });

/**
 * Interpret one committed batch into a transcript change plan.
 *
 * Mirrors the `ReducedState` message, tool and inference fold without pruning: a
 * `ContextRebased` only adds a compaction marker. Events at or below the lookup cursor are
 * skipped, so an idempotent re-append is harmless. Foreign events are ignored unless a mapping
 * of `definition` names their namespace, type and schema version.
 */
export const planTranscriptBatch: PlanTranscriptBatch = (lookup, committed, definition) => {
  const batch = new TranscriptBatch(lookup, definition);
  let throughSeq = lookup.cursor;
  for (const entry of committed) {
    const seq = entry.position.seq;
    if (seq <= throughSeq) continue;
    throughSeq = seq;
    if (entry.event.namespace === "__foreign") {
      applyForeignEvent(batch, entry.event.envelope, seq);
      continue;
    }
    if (entry.event.namespace !== effectDurableAgentNamespace) continue;
    interpretEvent(batch, entry, seq);
  }
  return batch.plan(throughSeq);
};

const eventTime = (createdAtMs: number) => UnixEpochMillis.make(createdAtMs);

const interpretEvent = (
  batch: TranscriptBatch,
  entry: CommittedDurableEvent,
  seq: SequenceNumber,
) => {
  if (entry.event.namespace === "__foreign") return;
  const event = interpretLegacyAttemptForReduction(entry.event);
  const at = eventTime(event.createdAtMs);
  switch (event.type) {
    case sessionConfiguredEventType: {
      const session = batch.session(seq);
      if (session.modelSelection === undefined) {
        batch.put({ ...session, updatedSeq: seq, modelSelection: event.payload.modelSelection });
      }
      return;
    }
    case commandAdmittedEventType: {
      const { command } = event.payload;
      if (command._tag !== "SubmitMessage" || command.commandId === undefined) return;
      const id = transcriptRecordId.user(command.commandId);
      if (batch.get(id) !== undefined) return;
      createUser(batch, seq, at, {
        commandId: command.commandId,
        content: command.content,
        state: command.disposition === "steer" ? { _tag: "Steering" } : { _tag: "Pending" },
        disposition: command.disposition,
        ...(command.idempotencyKey === undefined ? {} : { idempotencyKey: command.idempotencyKey }),
      });
      return;
    }
    case commandCancelledEventType: {
      const user = batch.user(transcriptRecordId.user(event.payload.commandId));
      // Only a submission that never became a message is withdrawn by its command; messages
      // follow their own lifecycle events, as in the reduced state.
      if (user === undefined || user.messageIds.length > 0 || !isWaiting(user.state)) return;
      batch.put({
        ...user,
        updatedSeq: seq,
        state: { _tag: "Cancelled", reason: event.payload.reason },
      });
      return;
    }
    case userMessageSubmittedEventType: {
      const { commandId, messageId, disposition, content } = event.payload;
      const user = userForMessage(batch, seq, at, commandId, messageId, content);
      batch.put({
        ...user,
        updatedSeq: seq,
        content,
        disposition,
        state: isWaiting(user.state)
          ? disposition === "steer"
            ? { _tag: "Steering" }
            : { _tag: "Pending" }
          : user.state,
      });
      return;
    }
    case userMessageCommittedEventType: {
      // A committed user message is model context immediately: this is its consumption point.
      // A replacement message id for the same command joins the same record as an alias.
      const { commandId, messageId, content } = event.payload;
      const user = userForMessage(batch, seq, at, commandId, messageId, content);
      batch.put(
        user.state._tag === "Consumed"
          ? { ...user, updatedSeq: seq, content }
          : consume(batch, { ...user, content }, seq, at, {}),
      );
      return;
    }
    case steeringMessageQueuedEventType: {
      const { commandId, messageId, runId, content } = event.payload;
      const user = userForMessage(batch, seq, at, commandId, messageId, content);
      batch.put({
        ...user,
        updatedSeq: seq,
        content,
        steering: { runId },
        state: isWaiting(user.state) ? { _tag: "Steering" } : user.state,
      });
      return;
    }
    case steeringMessageCancelledEventType: {
      const user = userByMessage(batch, event.payload.messageId);
      if (user?.steering === undefined || user.state._tag === "Consumed") return;
      batch.put({
        ...user,
        updatedSeq: seq,
        state: { _tag: "Cancelled", reason: event.payload.reason },
      });
      return;
    }
    case userMessagePromotedEventType: {
      const user = userByMessage(batch, event.payload.messageId);
      if (user === undefined) return;
      if (user.state._tag !== "Pending" && user.state._tag !== "Paused") return;
      batch.put({ ...user, updatedSeq: seq, state: { _tag: "Promoted" } });
      return;
    }
    case userMessageCancelledEventType: {
      const { commandId, messageId, reason } = event.payload;
      const user = userByMessage(batch, messageId);
      if (user === undefined || user.state._tag === "Cancelled") return;
      // Steering that the agent already read stays in history; a cancelled user submission is
      // withdrawn from the model context even after consumption, exactly as the prompt does.
      if (user.steering !== undefined && user.state._tag === "Consumed") return;
      batch.put({
        ...user,
        updatedSeq: seq,
        state: { _tag: "Cancelled", reason, byCommandId: commandId },
      });
      return;
    }
    case messageQueuePausedEventType:
    case pendingMessagesPausedEventType: {
      const byCommandId =
        event.type === messageQueuePausedEventType
          ? event.payload.stopCommandId
          : event.payload.interruptionCommandId;
      for (const messageId of event.payload.messageIds) {
        const user = userByMessage(batch, messageId);
        if (user === undefined || !isWaiting(user.state)) continue;
        batch.put({
          ...user,
          updatedSeq: seq,
          ...(user.steering === undefined ? { disposition: "queue" as const } : {}),
          state: { _tag: "Paused", byCommandId },
        });
      }
      return;
    }
    case turnStartedEventType: {
      const { runId, turnId, inputMessageIds } = event.payload;
      for (const messageId of inputMessageIds ?? []) {
        const user = userByMessage(batch, messageId);
        if (user === undefined || user.state._tag === "Consumed") continue;
        // The reduced state marks a cancelled submission consumed but keeps it out of the
        // prompt; cancelled steering re-enters it once consumed.
        if (user.state._tag === "Cancelled" && user.steering === undefined) continue;
        batch.put(consume(batch, user, seq, at, { runId, turnId }));
      }
      return;
    }
    case assistantMessageCommittedEventType:
    case assistantPartialCommittedEventType: {
      const { messageId, runId, turnId, inferenceId, promptParts } = event.payload;
      const id = transcriptRecordId.assistant(inferenceId);
      const existing =
        batch.assistant(id) ??
        newAssistant(batch, id, seq, "inference", inferenceId, runId, turnId);
      const { partial: _partial, ...withoutPartial } = existing;
      batch.put({
        ...withoutPartial,
        updatedSeq: seq,
        messageId,
        parts: assistantParts(batch, inferenceId, promptParts, seq),
        ...(event.type === assistantPartialCommittedEventType
          ? { partial: { reason: event.payload.reason } }
          : {}),
      });
      return;
    }
    case assistantMessageImportedEventType: {
      const { messageId, runId, turnId, inferenceId, promptParts } = event.payload;
      const id = transcriptRecordId.import(event.eventId);
      const created = newAssistant(batch, id, seq, "import", inferenceId, runId, turnId);
      const { runGroupId: _runGroupId, ...imported } = created;
      batch.put({
        ...imported,
        messageId,
        lifecycle: { _tag: "Completed" },
        parts: assistantParts(batch, inferenceId, promptParts, seq),
      });
      return;
    }
    case inferenceStartedEventType: {
      const { runId, turnId, inferenceId } = event.payload;
      const id = transcriptRecordId.assistant(inferenceId);
      const existing =
        batch.assistant(id) ??
        newAssistant(batch, id, seq, "inference", inferenceId, runId, turnId);
      batch.put({ ...existing, updatedSeq: seq, startedAtMs: at });
      return;
    }
    case inferenceCompletedEventType: {
      const { runId, inferenceId, finishReason, usage } = event.payload;
      const assistant = batch.assistant(transcriptRecordId.assistant(inferenceId));
      if (assistant === undefined) return;
      // Token totals follow the reduced state: count the first completion of a started inference.
      if (assistant.startedAtMs !== undefined && assistant.lifecycle._tag !== "Completed") {
        const model = runMember(batch, runId)?.modelSelection;
        if (model !== undefined && usage !== undefined) {
          const session = batch.session(seq);
          batch.put({
            ...session,
            updatedSeq: seq,
            tokenUsage: addModelUsage(session.tokenUsage, model, usage),
          });
        }
      }
      batch.put({
        ...assistant,
        updatedSeq: seq,
        ...terminalTiming(assistant.startedAtMs, at),
        lifecycle: {
          _tag: "Completed",
          ...(finishReason === undefined ? {} : { finishReason }),
          ...(usage === undefined ? {} : { usage }),
        },
      });
      return;
    }
    case inferenceFailedEventType: {
      const assistant = batch.assistant(transcriptRecordId.assistant(event.payload.inferenceId));
      if (assistant === undefined) return;
      batch.put({
        ...assistant,
        updatedSeq: seq,
        ...terminalTiming(assistant.startedAtMs, at),
        lifecycle: { _tag: "Failed", error: event.payload.error },
      });
      return;
    }
    case toolCallCreatedEventType: {
      const { inferenceId, toolCallId, promptPart } = event.payload;
      const id = transcriptRecordId.tool(toolCallId);
      const existing = batch.tool(id);
      batch.put({
        kind: "tool",
        id,
        updatedSeq: seq,
        toolCallId,
        assistantId: transcriptRecordId.assistant(inferenceId),
        providerPartId: promptPart.id,
        toolName: promptPart.name,
        providerExecuted: promptPart.providerExecuted,
        params: inlineBody(promptPart.params, seq, ["promptPart", "params"]),
        lifecycle:
          existing === undefined || existing.lifecycle._tag === "Rejected"
            ? { _tag: "Created" }
            : existing.lifecycle,
        createdAtMs: existing?.createdAtMs ?? at,
        ...timingOf(existing),
      });
      linkToolPart(batch, inferenceId, promptPart.id, id, seq);
      return;
    }
    case toolCallRejectedEventType: {
      const { inferenceId, toolCallId, promptPart } = event.payload;
      const id = transcriptRecordId.tool(toolCallId);
      const existing = batch.tool(id);
      batch.put({
        kind: "tool",
        id,
        updatedSeq: seq,
        toolCallId,
        assistantId: transcriptRecordId.assistant(inferenceId),
        providerPartId: promptPart.id,
        toolName: promptPart.name,
        providerExecuted: false,
        lifecycle: {
          _tag: "Rejected",
          reason: promptPart.result.reason,
          message: promptPart.result.message,
          modelFeedback: promptPart.result.modelFeedback,
        },
        createdAtMs: existing?.createdAtMs ?? at,
        ...timingOf(existing),
      });
      linkToolPart(batch, inferenceId, promptPart.id, id, seq);
      return;
    }
    case toolCallStartedEventType: {
      const tool = batch.tool(transcriptRecordId.tool(event.payload.toolCallId));
      if (tool === undefined) return;
      batch.put({
        ...tool,
        updatedSeq: seq,
        startedAtMs: at,
        lifecycle: tool.lifecycle._tag === "Created" ? { _tag: "Running" } : tool.lifecycle,
      });
      return;
    }
    case toolCallCompletedEventType: {
      const tool = batch.tool(transcriptRecordId.tool(event.payload.toolCallId));
      if (tool === undefined) return;
      batch.put({
        ...tool,
        updatedSeq: seq,
        ...terminalTiming(tool.startedAtMs, at),
        lifecycle: {
          _tag: "Completed",
          result: inlineBody(event.payload.promptPart.result, seq, ["promptPart", "result"]),
        },
      });
      return;
    }
    case toolCallFailedEventType: {
      const tool = batch.tool(transcriptRecordId.tool(event.payload.toolCallId));
      if (tool === undefined) return;
      batch.put({
        ...tool,
        updatedSeq: seq,
        ...terminalTiming(tool.startedAtMs, at),
        lifecycle: {
          _tag: "Failed",
          error: event.payload.error ?? event.payload.promptPart.result,
        },
      });
      return;
    }
    case contextRebasedEventType: {
      const { compactionId, summaryId, contextVersion, retainedFromContextSeq } = event.payload;
      const id = transcriptRecordId.compaction(compactionId);
      const existing = batch.get(id);
      if (existing !== undefined && existing.kind !== "compaction") {
        throw kindMismatch(id, "compaction", existing.kind);
      }
      const record: TranscriptCompactionRecord = {
        kind: "compaction",
        id,
        updatedSeq: seq,
        position: existing?.position ?? batch.position(seq),
        compactionId,
        summaryId,
        contextVersion,
        retainedFromContextSeq,
        createdAtMs: existing?.createdAtMs ?? at,
      };
      batch.put(record);
      return;
    }
    case compactionStartedEventType: {
      const session = batch.session(seq);
      batch.put({ ...session, updatedSeq: seq, activeCompactionId: event.payload.compactionId });
      return;
    }
    case compactionCompletedEventType:
    case compactionFailedEventType: {
      const { compactionId, modelSelection, usage } = event.payload;
      const session = batch.session(seq);
      const { activeCompactionId, ...rest } = session;
      batch.put({
        ...rest,
        updatedSeq: seq,
        ...(activeCompactionId === undefined || activeCompactionId === compactionId
          ? {}
          : { activeCompactionId }),
        tokenUsage:
          modelSelection === undefined
            ? session.tokenUsage
            : addModelUsage(session.tokenUsage, modelSelection, usage),
      });
      return;
    }
    case runStartedEventType: {
      startRun(
        batch,
        seq,
        at,
        event.payload.runId,
        event.payload.commandIds,
        event.payload.modelSelection,
      );
      return;
    }
    case runCompletedEventType:
      finishRun(batch, seq, at, event.payload.runId, { _tag: "Completed" });
      return;
    case runFailedEventType:
      finishRun(batch, seq, at, event.payload.runId, runFailureOutcome(event.payload.error));
      return;
    case runInterruptedEventType:
      finishRun(batch, seq, at, event.payload.runId, {
        _tag: "Interrupted",
        reason: event.payload.reason,
      });
      return;
    case recoveryCompletedEventType: {
      const continuation = event.payload.continuation;
      if (continuation === undefined) return;
      const groupId = batch.alias("runId", continuation.replacementRunId);
      const group = groupId === undefined ? undefined : batch.runGroup(groupId);
      if (group === undefined) return;
      batch.put({
        ...group,
        updatedSeq: seq,
        runs: mapNonEmpty(group.runs, (member) =>
          member.runId === continuation.replacementRunId
            ? { ...member, continuationOf: continuation.interruptedRunId }
            : member,
        ),
      });
      return;
    }
    case systemMessageCommittedEventType:
    case runSchedulingRequestedEventType:
    case runSchedulingDeliveredEventType:
    case runSchedulingInvalidatedEventType:
    case runSchedulingGrantedEventType:
    case commandStartedEventType:
    case commandCompletedEventType:
    case commandFailedEventType:
    case turnCompletedEventType:
    case turnFailedEventType:
    case turnStoppedEventType:
    case stopTurnRequestedEventType:
    case stopTurnAppliedEventType:
    case contextProjectedEventType:
    case compactionRequestedEventType:
    case summaryCreatedEventType:
    case baseStateRequestedEventType:
    case baseStateCreatedEventType:
    case baseStateFailedEventType:
      // Deliberately without transcript meaning in this semantic revision.
      return;
    default:
      return assertNever(event, "transcript durable event");
  }
};

// ---------------------------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------------------------

/** Waiting for the agent: not yet consumed and not withdrawn. */
const isWaiting = (state: TranscriptUserState): boolean =>
  state._tag === "Pending" ||
  state._tag === "Paused" ||
  state._tag === "Promoted" ||
  state._tag === "Steering";

/** What the first event that names a submission knows about it. */
interface NewUserFields {
  readonly commandId: CommandId;
  readonly content: UserMessageContent;
  readonly state: TranscriptUserState;
  readonly disposition?: TranscriptUserRecord["disposition"];
  readonly idempotencyKey?: TranscriptUserRecord["idempotencyKey"];
}

const createUser = (
  batch: TranscriptBatch,
  seq: SequenceNumber,
  at: UnixEpochMillis,
  fields: NewUserFields,
): TranscriptUserRecord => {
  const id = transcriptRecordId.user(fields.commandId);
  if (fields.idempotencyKey !== undefined) {
    batch.insertAlias("idempotencyKey", fields.idempotencyKey, id);
  }
  const user = replayBufferedDecorations(batch, seq, {
    kind: "user",
    id,
    updatedSeq: seq,
    messageIds: [],
    decorations: {},
    submittedSeq: seq,
    submittedAtMs: at,
    ...fields,
  });
  batch.put(user);
  return user;
};

/** The user record a message belongs to, created if the log never admitted its command. */
const userForMessage = (
  batch: TranscriptBatch,
  seq: SequenceNumber,
  at: UnixEpochMillis,
  commandId: CommandId,
  messageId: MessageId,
  content: UserMessageContent,
): TranscriptUserRecord => {
  const id = transcriptRecordId.user(commandId);
  const user =
    batch.user(id) ??
    createUser(batch, seq, at, { commandId, content, state: { _tag: "Pending" } });
  batch.insertAlias("messageId", messageId, id);
  return user.messageIds.includes(messageId)
    ? user
    : { ...user, messageIds: [...user.messageIds, messageId] };
};

const userByMessage = (batch: TranscriptBatch, messageId: MessageId) => {
  const id = batch.alias("messageId", messageId);
  return id === undefined ? undefined : batch.user(id);
};

const consume = (
  batch: TranscriptBatch,
  user: TranscriptUserRecord,
  seq: SequenceNumber,
  at: UnixEpochMillis,
  by: { readonly runId?: RunId; readonly turnId?: TurnId },
): TranscriptUserRecord => ({
  ...user,
  updatedSeq: seq,
  // Positions are assigned once; a later consumption never moves a record.
  position: user.position ?? batch.position(seq),
  consumedAtMs: user.consumedAtMs ?? at,
  state: { _tag: "Consumed", ...by },
});

// ---------------------------------------------------------------------------------------------
// Assistants and tools
// ---------------------------------------------------------------------------------------------

const newAssistant = (
  batch: TranscriptBatch,
  id: AssistantRecordId,
  seq: SequenceNumber,
  source: "inference" | "import",
  inferenceId: InferenceId,
  runId: RunId,
  turnId: TurnId,
): TranscriptAssistantRecord => {
  const groupId = batch.alias("runId", runId);
  const runGroupId = groupId === undefined ? undefined : batch.runGroup(groupId)?.id;
  return {
    kind: "assistant",
    id,
    updatedSeq: seq,
    position: batch.position(seq),
    source,
    inferenceId,
    runId,
    turnId,
    ...(runGroupId === undefined ? {} : { runGroupId }),
    lifecycle: { _tag: "Streaming" },
    parts: [],
  };
};

const assistantParts = (
  batch: TranscriptBatch,
  inferenceId: InferenceId,
  promptParts: ReadonlyArray<AssistantPromptPart>,
  seq: SequenceNumber,
): ReadonlyArray<TranscriptAssistantPart> =>
  promptParts.map((part, index): TranscriptAssistantPart => {
    switch (part.type) {
      case "text":
        return { _tag: "Text", text: part.text };
      case "reasoning":
        return { _tag: "Reasoning", text: part.text };
      case "tool-call": {
        if (!Schema.is(ProviderPartId)(part.id)) return { _tag: "Unknown", partType: part.type };
        const toolAlias = batch.alias("toolPart", toolPartKey(inferenceId, part.id));
        const toolId = toolAlias === undefined ? undefined : batch.tool(toolAlias)?.id;
        return {
          _tag: "ToolCall",
          providerPartId: part.id,
          toolName: part.name,
          providerExecuted: part.providerExecuted,
          ...(toolId === undefined ? {} : { toolId }),
        };
      }
      case "tool-result":
        if (!Schema.is(ProviderPartId)(part.id)) return { _tag: "Unknown", partType: part.type };
        return {
          _tag: "ToolResult",
          providerPartId: part.id,
          toolName: part.name,
          isFailure: part.isFailure,
          result: inlineBody(part.result, seq, ["promptParts", index, "result"]),
        };
      default:
        return { _tag: "Unknown", partType: part.type };
    }
  });

const toolPartKey = (inferenceId: InferenceId, providerPartId: string) =>
  `${inferenceId}/${providerPartId}`;

/** Record the tool for its assistant part, whichever of the two the log wrote first. */
const linkToolPart = (
  batch: TranscriptBatch,
  inferenceId: InferenceId,
  providerPartId: ProviderPartId,
  toolId: ReturnType<typeof transcriptRecordId.tool>,
  seq: SequenceNumber,
) => {
  batch.insertAlias("toolPart", toolPartKey(inferenceId, providerPartId), toolId);
  const assistant = batch.assistant(transcriptRecordId.assistant(inferenceId));
  if (assistant === undefined) return;
  let changed = false;
  const parts = assistant.parts.map((part): TranscriptAssistantPart => {
    if (
      part._tag !== "ToolCall" ||
      part.providerPartId !== providerPartId ||
      part.toolId !== undefined
    ) {
      return part;
    }
    changed = true;
    return { ...part, toolId };
  });
  if (changed) batch.put({ ...assistant, updatedSeq: seq, parts });
};

// ---------------------------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------------------------

const startRun = (
  batch: TranscriptBatch,
  seq: SequenceNumber,
  at: UnixEpochMillis,
  runId: RunId,
  commandIds: ReadonlyArray<CommandId>,
  modelSelection: ModelSelectionPayload,
) => {
  const member: TranscriptRunMember = {
    runId,
    commandIds,
    modelSelection,
    outcome: { _tag: "Running" },
    startedAtMs: at,
  };
  const group = runGroupForStart(batch, seq, runId, commandIds, member);
  batch.put(group);
  batch.insertAlias("runId", runId, group.id);
  for (const commandId of commandIds) batch.insertAlias("runCommand", commandId, group.id);

  const session = batch.session(seq);
  batch.put({
    ...session,
    updatedSeq: seq,
    activeRunId: runId,
    lastRunModelSelection: modelSelection,
    modelSelection: session.modelSelection ?? modelSelection,
  });
};

/**
 * The group a started run belongs to. A repeated start of a known run replaces its member and
 * keeps its outcome, as the reduced state keeps its terminal. A startup-recovery continuation
 * reuses its interrupted predecessor's command ids, so it joins that finished group; any other
 * run roots a new group.
 */
const runGroupForStart = (
  batch: TranscriptBatch,
  seq: SequenceNumber,
  runId: RunId,
  commandIds: ReadonlyArray<CommandId>,
  member: TranscriptRunMember,
): TranscriptRunGroupRecord => {
  const knownId = batch.alias("runId", runId);
  const known = knownId === undefined ? undefined : batch.runGroup(knownId);
  if (known !== undefined) {
    return {
      ...known,
      updatedSeq: seq,
      runs: mapNonEmpty(known.runs, (candidate) =>
        candidate.runId === runId ? { ...member, outcome: candidate.outcome } : candidate,
      ),
    };
  }
  const predecessor = commandIds
    .map((commandId) => batch.alias("runCommand", commandId))
    .map((groupId) => (groupId === undefined ? undefined : batch.runGroup(groupId)))
    .find((group) => group !== undefined && runGroupStatus(group)._tag !== "Running");
  if (predecessor !== undefined) {
    return { ...predecessor, updatedSeq: seq, runs: [...predecessor.runs, member] };
  }
  return {
    kind: "runGroup",
    id: transcriptRecordId.runGroup(runId),
    updatedSeq: seq,
    rootRunId: runId,
    runs: [member],
  };
};

const finishRun = (
  batch: TranscriptBatch,
  seq: SequenceNumber,
  at: UnixEpochMillis,
  runId: RunId,
  outcome: TranscriptRunOutcome,
) => {
  const groupId = batch.alias("runId", runId);
  const group = groupId === undefined ? undefined : batch.runGroup(groupId);
  if (group === undefined) return;
  batch.put({
    ...group,
    updatedSeq: seq,
    runs: mapNonEmpty(group.runs, (member) =>
      member.runId === runId
        ? { ...member, ...terminalTiming(member.startedAtMs, at), outcome }
        : member,
    ),
  });
  const session = batch.session(seq);
  if (session.activeRunId === runId) {
    const { activeRunId: _activeRunId, ...idle } = session;
    batch.put({ ...idle, updatedSeq: seq });
  }
};

const runMember = (batch: TranscriptBatch, runId: RunId) => {
  const groupId = batch.alias("runId", runId);
  const group = groupId === undefined ? undefined : batch.runGroup(groupId);
  return group?.runs.find((member) => member.runId === runId);
};

const runFailureOutcome = (error: FailurePayload): TranscriptRunOutcome =>
  error.code === "run.interrupted"
    ? { _tag: "Interrupted", reason: error.message }
    : { _tag: "Failed", error };

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

const terminalTiming = (
  startedAtMs: UnixEpochMillis | undefined,
  terminalAtMs: UnixEpochMillis,
) => ({
  terminalAtMs,
  ...(startedAtMs === undefined ? {} : { durationMs: Math.max(0, terminalAtMs - startedAtMs) }),
});

const timingOf = (record: TranscriptToolRecord | undefined) => ({
  ...(record?.startedAtMs === undefined ? {} : { startedAtMs: record.startedAtMs }),
  ...(record?.terminalAtMs === undefined ? {} : { terminalAtMs: record.terminalAtMs }),
  ...(record?.durationMs === undefined ? {} : { durationMs: record.durationMs }),
});

const bodyEncoder = new TextEncoder();

/** A body read from a committed event. Logs are JSON, so anything else is a projection defect. */
const inlineBody = (
  value: unknown,
  seq: SequenceNumber,
  path: TranscriptFieldPath,
): TranscriptBody => {
  if (!Schema.is(Schema.Json)(value)) {
    throw new TranscriptProjectionError({
      message: `body at seq ${seq} ${JSON.stringify(path)} is not JSON`,
    });
  }
  // Large bodies stay in the log; the stored row keeps only a reference to them.
  const byteLength = bodyEncoder.encode(JSON.stringify(value)).length;
  return byteLength <= transcriptStoredInlineBodyLimit
    ? { _tag: "Inline", value, source: { seq, path } }
    : { _tag: "Ref", source: { seq, path }, byteLength };
};

const mapNonEmpty = <A>(
  items: readonly [A, ...Array<A>],
  f: (item: A) => A,
): readonly [A, ...Array<A>] => [f(items[0]), ...items.slice(1).map(f)];
