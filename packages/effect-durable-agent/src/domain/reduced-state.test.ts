import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import {
  classifyRecoverableWork,
  decodeReducedStateCheckpoint,
  encodeReducedStateCheckpoint,
  foldReducedState,
  frameworkReducedStateReducerSchemaVersion,
  initialReducedState,
  reduceCommittedEvents,
  reducedStateCheckpointEventSeqs,
} from "./reduced-state";
import { StopTurnCommand, SubmitMessageCommand } from "../types/commands";
import {
  InferenceId,
  CommandId,
  CompactionId,
  ContextVersion,
  EventId,
  MessageId,
  RunId,
  SequenceNumber,
  SessionId,
  SummaryId,
  ToolCallId,
  TurnId,
  durablePosition,
} from "../types/core";
import type { EDADurableEvent } from "../types/events";
import {
  EventNamespace,
  EventType,
  ProviderPartId,
  ToolName,
  UnixEpochMillis,
  effectDurableAgentNamespace,
  schemaV1,
} from "../types/events";
import { makeEDARunTrace } from "../types/tracing";
import { CommittedDurableEvent } from "../services/session-store";

const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const STOP_COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1c-1f2e3d4c5b6a");
const STEER_COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1f-1f2e3d4c5b6a");
const RUN_ID = "018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a";
const TURN_ID = TurnId.make("018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a");
const INFERENCE_ID = InferenceId.make("018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a");
const SECOND_INFERENCE_ID = InferenceId.make("018f6bd5-2f2a-7b1e-bf2a-1f2e3d4c5b6a");
const TOOL_CALL_ID = ToolCallId.make("018f6bd5-2f2a-7b1e-8f1d-1f2e3d4c5b6a");
const MESSAGE_ID = MessageId.make("018f6bd5-2f2a-7b1e-8f1e-1f2e3d4c5b6a");
const STEERING_MESSAGE_ID = MessageId.make("018f6bd5-2f2a-7b1e-8f2e-1f2e3d4c5b6a");

const command = new SubmitMessageCommand({
  commandId: COMMAND_ID,
  disposition: "queue",
  content: [Prompt.textPart({ text: "hello" })],
});

const steerCommand = new SubmitMessageCommand({
  commandId: STEER_COMMAND_ID,
  disposition: "steer",
  content: [Prompt.textPart({ text: "steer" })],
});

const uuid = (n: number): string => `018f6bd5-2f2a-7b1e-8f1a-${n.toString(16).padStart(12, "0")}`;

const committed = (seq: number, type: string, payload: unknown) => {
  const event: EDADurableEvent = {
    namespace: effectDurableAgentNamespace,
    type: EventType.make(type),
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(`018f6bd5-2f2a-7b1e-${(0x9000 + seq).toString(16)}-1f2e3d4c5b6a`),
    sessionId: SESSION_ID,
    createdAtMs: UnixEpochMillis.make(1_715_000_000_000 + seq),
    payload,
  } as EDADurableEvent;

  return CommittedDurableEvent.make({
    position: durablePosition(SequenceNumber.make(seq)),
    event,
  });
};

const activeInferenceEvents = () => [
  committed(1, "CommandAdmitted", { command }),
  committed(2, "CommandStarted", { commandId: COMMAND_ID }),
  committed(3, "UserMessageCommitted", {
    commandId: COMMAND_ID,
    messageId: MESSAGE_ID,
    content: command.content,
  }),
  committed(4, "RunStarted", {
    runId: RUN_ID,
    commandIds: [COMMAND_ID],
    modelSelection: { provider: "test", modelId: "test-model" },
    trace: makeEDARunTrace(),
  }),
  committed(5, "TurnStarted", { runId: RUN_ID, turnId: TURN_ID }),
  committed(6, "InferenceStarted", { runId: RUN_ID, turnId: TURN_ID, inferenceId: INFERENCE_ID }),
  committed(7, "ToolCallCreated", {
    runId: RUN_ID,
    turnId: TURN_ID,
    inferenceId: INFERENCE_ID,
    toolCallId: TOOL_CALL_ID,
    promptPart: Prompt.toolCallPart({
      id: ProviderPartId.make("tool-1"),
      name: ToolName.make("noop"),
      params: {},
      providerExecuted: false,
    }),
  }),
  committed(8, "ToolCallStarted", { toolCallId: TOOL_CALL_ID }),
];

describe("reduced-state", () => {
  it("uses the current checkpoint schema version", () => {
    expect(frameworkReducedStateReducerSchemaVersion).toBe(6);
  });

  it("folds and checkpoint-hydrates imported assistant context", () => {
    const importedMessageId = MessageId.make(uuid(0xd001));
    const imported = committed(1, "AssistantMessageImported", {
      inferenceId: INFERENCE_ID,
      messageId: importedMessageId,
      promptParts: [Prompt.textPart({ text: "Earlier Gia response" })],
      runId: RUN_ID,
      turnId: TURN_ID,
    });

    const state = reduceCommittedEvents([imported]);
    const hydrated = decodeReducedStateCheckpoint(encodeReducedStateCheckpoint(state), [imported]);

    expect(hydrated.messages.get(importedMessageId)).toMatchObject({
      _tag: "Assistant",
      content: { text: "Earlier Gia response" },
      imported: true,
    });
  });

  it("omits terminal fields from active lifecycle records", () => {
    const state = reduceCommittedEvents(activeInferenceEvents().slice(0, 6));
    const run = state.runs.get(RunId.make(RUN_ID));
    const turn = state.turns.get(TURN_ID);
    const inference = state.inferences.get(INFERENCE_ID);

    expect(Object.hasOwn(run ?? {}, "terminal")).toBe(false);
    expect(Object.hasOwn(turn ?? {}, "terminal")).toBe(false);
    expect(Object.hasOwn(inference ?? {}, "terminal")).toBe(false);
  });

  it("roundtrips recovery continuations through framework checkpoints", () => {
    const replacementRunId = RunId.make(uuid(0xe001));
    const events = [
      committed(1, "RunStarted", {
        runId: RUN_ID,
        commandIds: [COMMAND_ID],
        modelSelection: { provider: "test", modelId: "test-model" },
      }),
      committed(2, "RunFailed", { runId: RUN_ID, error: { message: "interrupted" } }),
      committed(3, "RunStarted", {
        runId: replacementRunId,
        commandIds: [COMMAND_ID],
        modelSelection: { provider: "test", modelId: "test-model" },
      }),
      committed(4, "RecoveryCompleted", {
        trigger: "runtime-restart",
        continuation: {
          commandId: COMMAND_ID,
          interruptedRunId: RUN_ID,
          replacementRunId,
        },
      }),
    ];
    const state = reduceCommittedEvents(events);

    const decoded = decodeReducedStateCheckpoint(encodeReducedStateCheckpoint(state), events);

    expect(Array.from(decoded.recoveryContinuations.entries())).toEqual([
      [
        replacementRunId,
        {
          commandId: COMMAND_ID,
          interruptedRunId: RUN_ID,
          replacementRunId,
          seq: SequenceNumber.make(4),
        },
      ],
    ]);
  });

  it("decodes legacy framework checkpoints without recovery continuations", () => {
    const { recoveryContinuations: _recoveryContinuations, ...legacyCheckpoint } =
      encodeReducedStateCheckpoint(initialReducedState);

    const decoded = decodeReducedStateCheckpoint(legacyCheckpoint, []);

    expect(decoded.recoveryContinuations.size).toBe(0);
  });

  it("prunes a recovery continuation after context rebase removes its replacement run", () => {
    const replacementRunId = RunId.make(uuid(0xe002));
    const compactionId = CompactionId.make(uuid(0xe003));
    const summaryId = SummaryId.make(uuid(0xe004));
    const state = reduceCommittedEvents([
      committed(1, "RunStarted", {
        runId: RUN_ID,
        commandIds: [COMMAND_ID],
        modelSelection: { provider: "test", modelId: "test-model" },
      }),
      committed(2, "RunFailed", { runId: RUN_ID, error: { message: "interrupted" } }),
      committed(3, "RunStarted", {
        runId: replacementRunId,
        commandIds: [COMMAND_ID],
        modelSelection: { provider: "test", modelId: "test-model" },
      }),
      committed(4, "RecoveryCompleted", {
        trigger: "runtime-restart",
        continuation: {
          commandId: COMMAND_ID,
          interruptedRunId: RUN_ID,
          replacementRunId,
        },
      }),
      committed(5, "RunCompleted", { runId: replacementRunId }),
      committed(6, "SummaryCreated", {
        compactionId,
        summaryId,
        sourceFromSeq: SequenceNumber.make(1),
        sourceToSeq: SequenceNumber.make(5),
        summary: { summaryId, text: "summary" },
      }),
      committed(7, "ContextRebased", {
        compactionId,
        summaryId,
        contextVersion: ContextVersion.make(1),
        retainedFromContextSeq: SequenceNumber.make(6),
      }),
    ]);

    expect(state.runs.has(replacementRunId)).toBe(false);
    expect(state.recoveryContinuations.size).toBe(0);
  });

  it("folds one pending message through promotion, pause, and multi-message consumption", () => {
    const secondMessageId = MessageId.make("018f6bd5-2f2a-7b1e-8f3e-1f2e3d4c5b6a");
    const state = reduceCommittedEvents([
      committed(1, "UserMessageSubmitted", {
        commandId: COMMAND_ID,
        messageId: MESSAGE_ID,
        disposition: "queue",
        content: command.content,
      }),
      committed(2, "UserMessageSubmitted", {
        commandId: STEER_COMMAND_ID,
        messageId: secondMessageId,
        disposition: "steer",
        content: steerCommand.content,
      }),
      committed(3, "UserMessagePromoted", {
        commandId: STOP_COMMAND_ID,
        from: "queue",
        messageId: MESSAGE_ID,
        to: "steer",
      }),
      committed(4, "TurnStarted", {
        runId: RUN_ID,
        turnId: TURN_ID,
        inputMessageIds: [MESSAGE_ID, secondMessageId],
      }),
    ]);

    expect(state.commandQueues.pendingQueue).toEqual([]);
    expect(state.commandQueues.pendingSteers).toEqual([]);
    expect(state.messages.get(MESSAGE_ID)).toMatchObject({
      requestedDisposition: "queue",
      disposition: "steer",
      promotedSeq: 3,
      consumedSeq: 4,
      consumedTurnId: TURN_ID,
    });
    expect(Object.hasOwn(state.messages.get(MESSAGE_ID) ?? {}, "pausedSeq")).toBe(false);
    expect(Object.hasOwn(state.messages.get(MESSAGE_ID) ?? {}, "pausedByCommandId")).toBe(false);
    expect(state.messages.get(secondMessageId)).toMatchObject({ consumedSeq: 4 });
  });

  it("derives paused queue members and removes cancelled members", () => {
    const paused = reduceCommittedEvents([
      committed(1, "UserMessageSubmitted", {
        commandId: STEER_COMMAND_ID,
        messageId: STEERING_MESSAGE_ID,
        disposition: "steer",
        content: steerCommand.content,
      }),
      committed(2, "MessageQueuePaused", {
        stopCommandId: STOP_COMMAND_ID,
        runId: RUN_ID,
        messageIds: [STEERING_MESSAGE_ID],
        reason: "user-interrupted",
      }),
    ]);
    expect(paused.commandQueues.pausedQueue).toEqual([
      expect.objectContaining({ messageId: STEERING_MESSAGE_ID, disposition: "queue" }),
    ]);

    const cancelled = foldReducedState(paused, [
      committed(3, "UserMessageCancelled", {
        commandId: STOP_COMMAND_ID,
        messageId: STEERING_MESSAGE_ID,
        reason: "user-cancel",
      }),
    ]);
    expect(cancelled.commandQueues.pausedQueue).toEqual([]);
    expect(cancelled.messages.get(STEERING_MESSAGE_ID)).toMatchObject({ cancelledSeq: 3 });
  });

  it("applies current pause, promotion, and cancellation facts to legacy steers", () => {
    const paused = reduceCommittedEvents([
      committed(1, "SteeringMessageQueued", {
        commandId: STEER_COMMAND_ID,
        messageId: STEERING_MESSAGE_ID,
        runId: RUN_ID,
        content: steerCommand.content,
      }),
      committed(2, "PendingMessagesPaused", {
        interruptionCommandId: STOP_COMMAND_ID,
        runId: RUN_ID,
        messageIds: [STEERING_MESSAGE_ID],
        reason: "user-interrupted",
      }),
    ]);
    expect(paused.commandQueues.pausedQueue).toEqual([
      expect.objectContaining({ messageId: STEERING_MESSAGE_ID, disposition: "queue" }),
    ]);

    const promoted = foldReducedState(paused, [
      committed(3, "UserMessagePromoted", {
        commandId: STOP_COMMAND_ID,
        from: "queue",
        messageId: STEERING_MESSAGE_ID,
        to: "steer",
      }),
    ]);
    expect(promoted.commandQueues.pendingSteers).toEqual([
      expect.objectContaining({ messageId: STEERING_MESSAGE_ID, effectiveSeq: 3 }),
    ]);

    const cancelled = foldReducedState(paused, [
      committed(3, "UserMessageCancelled", {
        commandId: STOP_COMMAND_ID,
        messageId: STEERING_MESSAGE_ID,
        reason: "user-cancel",
      }),
    ]);
    expect(cancelled.commandQueues.pausedQueue).toEqual([]);
    expect(cancelled.messages.get(STEERING_MESSAGE_ID)).toMatchObject({
      cancelledByCommandId: STOP_COMMAND_ID,
      cancelledSeq: 3,
    });
  });

  it("hydrates command records from checkpoint event pointers", () => {
    const event = committed(1, "CommandAdmitted", {
      command: new SubmitMessageCommand({
        commandId: COMMAND_ID,
        content: [Prompt.textPart({ text: "checkpoint replay" })],
        disposition: "queue",
        idempotencyKey: "checkpoint-command",
      }),
    });
    const source = reduceCommittedEvents([event]);
    const state = decodeReducedStateCheckpoint(encodeReducedStateCheckpoint(source), [event]);

    const decoded = state.commands.get(COMMAND_ID)?.command;
    expect(decoded).toBeInstanceOf(SubmitMessageCommand);
    expect(decoded).toMatchObject({
      _tag: "SubmitMessage",
      commandId: COMMAND_ID,
      content: [expect.objectContaining({ text: "checkpoint replay" })],
      disposition: "queue",
      idempotencyKey: "checkpoint-command",
    });
    expect(state.commandQueues.pendingCommands[0]?.command).toBe(decoded);
  });

  it("materializes command queues from pending command and steering state", () => {
    const state = reduceCommittedEvents([
      committed(1, "CommandAdmitted", { command }),
      committed(2, "CommandAdmitted", { command: steerCommand }),
      committed(3, "CommandAdmitted", {
        command: new StopTurnCommand({ commandId: STOP_COMMAND_ID }),
      }),
      committed(4, "SteeringMessageQueued", {
        commandId: STEER_COMMAND_ID,
        messageId: STEERING_MESSAGE_ID,
        runId: RUN_ID,
        content: steerCommand.content,
      }),
    ]);

    expect(state.commandQueues.pendingCommands.map((entry) => entry.commandId)).toEqual([
      COMMAND_ID,
      STEER_COMMAND_ID,
      STOP_COMMAND_ID,
    ]);
    expect(state.commandQueues.queuedCommands.map((entry) => entry.commandId)).toEqual([
      COMMAND_ID,
    ]);
    expect(state.commandQueues.activeControlCommands.map((entry) => entry.commandId)).toEqual([
      STEER_COMMAND_ID,
      STOP_COMMAND_ID,
    ]);
    expect(state.commandQueues.steeringByRun.get(RUN_ID)?.map((entry) => entry.messageId)).toEqual([
      STEERING_MESSAGE_ID,
    ]);
  });

  it("removes steering from CommandQueues when a turn consumes it", () => {
    const state = reduceCommittedEvents([
      committed(1, "SteeringMessageQueued", {
        commandId: STEER_COMMAND_ID,
        messageId: STEERING_MESSAGE_ID,
        runId: RUN_ID,
        content: steerCommand.content,
      }),
      committed(2, "TurnStarted", {
        runId: RUN_ID,
        turnId: TURN_ID,
        inputMessageIds: [STEERING_MESSAGE_ID],
      }),
    ]);
    const steering = state.messages.get(STEERING_MESSAGE_ID);

    expect(state.commandQueues.steeringByRun.get(RUN_ID)).toBeUndefined();
    expect(steering).toMatchObject({
      _tag: "Steering",
      consumedSeq: SequenceNumber.make(2),
      consumedTurnId: TURN_ID,
    });
  });

  it("skips foreign namespace events while retaining the durable watermark", () => {
    const foreign = {
      position: durablePosition(SequenceNumber.make(2)),
      event: {
        namespace: EventNamespace.make("foreign-system"),
        type: EventType.make("CommandAdmitted"),
        schemaVersion: schemaV1,
        durability: "durable",
        eventId: EventId.make("018f6bd5-2f2a-7b1e-9ff2-1f2e3d4c5b6a"),
        sessionId: SESSION_ID,
        createdAtMs: UnixEpochMillis.make(1_715_000_000_002),
        payload: { ignored: true },
      } as EDADurableEvent,
    } as CommittedDurableEvent;

    const state = reduceCommittedEvents([committed(1, "CommandAdmitted", { command }), foreign]);

    expect(state.lastSeq).toBe(SequenceNumber.make(2));
    expect(state.commands.get(COMMAND_ID)).toMatchObject({ admittedSeq: SequenceNumber.make(1) });
    expect(state.commandQueues.pendingCommands.map((entry) => entry.commandId)).toEqual([
      COMMAND_ID,
    ]);
  });

  it("aggregates token consumption from completed model inferences", () => {
    const state = reduceCommittedEvents([
      ...activeInferenceEvents(),
      committed(9, "InferenceCompleted", {
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: INFERENCE_ID,
        usage: {
          inputTokens: 10,
          cachedInputTokens: 4,
          outputTokens: 5,
          textTokens: 3,
          reasoningTokens: 2,
        },
      }),
      committed(10, "InferenceStarted", {
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: SECOND_INFERENCE_ID,
      }),
      committed(11, "InferenceCompleted", {
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: SECOND_INFERENCE_ID,
        usage: {
          inputTokens: 20,
          cachedInputTokens: 3,
          outputTokens: 7,
          textTokens: 6,
          reasoningTokens: 1,
        },
      }),
      committed(12, "TurnCompleted", {
        runId: RUN_ID,
        turnId: TURN_ID,
        usage: {
          inputTokens: 20,
          cachedInputTokens: 3,
          outputTokens: 7,
        },
      }),
    ]);

    expect(state.tokenConsumption.byModel).toEqual([
      {
        provider: "test",
        modelId: "test-model",
        usage: {
          inputTokens: 30,
          cachedInputTokens: 7,
          cacheWriteInputTokens: 0,
          outputTokens: 12,
          textTokens: 9,
          reasoningTokens: 3,
        },
      },
    ]);
    expect(state.inferences.get(INFERENCE_ID)?.terminal).toMatchObject({
      _tag: "Completed",
      usage: { cachedInputTokens: 4 },
    });
    expect(state.turns.get(TURN_ID)?.terminal).toMatchObject({
      _tag: "Completed",
      usage: { inputTokens: 20, outputTokens: 7 },
    });
  });

  it("preserves token consumption in framework checkpoints", () => {
    const events = [
      ...activeInferenceEvents(),
      committed(9, "InferenceCompleted", {
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: INFERENCE_ID,
        usage: { inputTokens: 8, outputTokens: 4, cachedInputTokens: 2 },
      }),
    ];
    const state = reduceCommittedEvents(events);

    const decoded = decodeReducedStateCheckpoint(encodeReducedStateCheckpoint(state), events);

    expect(decoded.tokenConsumption).toEqual(state.tokenConsumption);
    expect(decoded.inferences.get(INFERENCE_ID)?.terminal).toMatchObject({
      _tag: "Completed",
      usage: { inputTokens: 8, outputTokens: 4, cachedInputTokens: 2 },
    });
  });

  it("preserves initial selection and attributes calls to their immutable run selections", () => {
    const selected = {
      provider: "openai",
      modelId: "model-a",
      settings: { thinkingLevel: "high" },
    };
    const other = { provider: "openai", modelId: "model-b", settings: { thinkingLevel: "low" } };
    const events = [
      committed(1, "SessionConfigured", { modelSelection: selected }),
      committed(2, "RunStarted", {
        runId: RUN_ID,
        commandIds: [],
        modelSelection: selected,
        trace: makeEDARunTrace(),
      }),
      committed(3, "InferenceStarted", {
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: INFERENCE_ID,
      }),
      committed(4, "InferenceCompleted", {
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: INFERENCE_ID,
        usage: {
          inputTokens: 100,
          cachedInputTokens: 10,
          cacheWriteInputTokens: 5,
          outputTokens: 20,
        },
      }),
      committed(5, "RunStarted", {
        runId: uuid(99),
        commandIds: [],
        modelSelection: other,
        trace: makeEDARunTrace(),
      }),
      committed(6, "InferenceStarted", {
        runId: uuid(99),
        turnId: TURN_ID,
        inferenceId: SECOND_INFERENCE_ID,
      }),
      committed(7, "InferenceCompleted", {
        runId: uuid(99),
        turnId: TURN_ID,
        inferenceId: SECOND_INFERENCE_ID,
        usage: { inputTokens: 50, outputTokens: 10 },
      }),
      committed(8, "CompactionCompleted", {
        compactionId: uuid(100),
        modelSelection: other,
        usage: { inputTokens: 30, outputTokens: 4 },
      }),
      committed(9, "SessionConfigured", { modelSelection: other }),
    ];
    const state = reduceCommittedEvents(events);
    expect(state.modelSelection).toEqual(selected);
    expect(state.tokenConsumption.byModel).toMatchObject([
      {
        modelId: "model-a",
        usage: { inputTokens: 100, cacheWriteInputTokens: 5, outputTokens: 20 },
      },
      { modelId: "model-b", usage: { inputTokens: 80, outputTokens: 14 } },
    ]);
    const restored = decodeReducedStateCheckpoint(encodeReducedStateCheckpoint(state), events);
    expect(restored.modelSelection).toEqual(selected);
    expect(restored.tokenConsumption).toEqual(state.tokenConsumption);
    expect(restored.runs.get(RunId.make(uuid(99)))?.modelSelection).toEqual(other);
  });

  it("roundtrips heavyweight checkpoint fields through event pointers", () => {
    const heavy = "x".repeat(10_000);
    const events = [
      committed(1, "CommandAdmitted", {
        command: new SubmitMessageCommand({
          commandId: COMMAND_ID,
          disposition: "queue",
          content: [Prompt.textPart({ text: heavy })],
        }),
      }),
      committed(2, "UserMessageCommitted", {
        commandId: COMMAND_ID,
        messageId: MESSAGE_ID,
        content: [Prompt.textPart({ text: heavy })],
      }),
      committed(3, "ToolCallCreated", {
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: INFERENCE_ID,
        toolCallId: TOOL_CALL_ID,
        promptPart: Prompt.toolCallPart({
          id: ProviderPartId.make("tool-1"),
          name: ToolName.make("noop"),
          params: { heavy },
          providerExecuted: false,
        }),
      }),
      committed(4, "ToolCallStarted", { toolCallId: TOOL_CALL_ID }),
      committed(5, "ToolCallCompleted", {
        toolCallId: TOOL_CALL_ID,
        promptPart: Prompt.toolResultPart({
          id: ProviderPartId.make("tool-1"),
          name: ToolName.make("noop"),
          isFailure: false,
          result: { heavy },
        }),
      }),
    ];
    const state = reduceCommittedEvents(events);
    const payload = encodeReducedStateCheckpoint(state);
    const encoded = JSON.stringify(payload);
    const decoded = decodeReducedStateCheckpoint(payload, events);

    expect(reducedStateCheckpointEventSeqs(payload)).toEqual([
      SequenceNumber.make(1),
      SequenceNumber.make(2),
      SequenceNumber.make(3),
      SequenceNumber.make(5),
    ]);
    expect(encoded).not.toContain(heavy);
    expect(encoded.length).toBeLessThan(5_000);
    expect(decoded.commands.get(COMMAND_ID)?.command).toMatchObject({
      content: [expect.objectContaining({ text: heavy })],
    });
    expect(decoded.messages.get(MESSAGE_ID)).toMatchObject({
      content: [expect.objectContaining({ text: heavy })],
    });
    expect(decoded.toolCalls.get(TOOL_CALL_ID)?.decision).toMatchObject({
      params: { heavy },
    });
    expect(decoded.toolCalls.get(TOOL_CALL_ID)?.terminal).toMatchObject({
      result: { heavy },
    });
  });

  it("folds wall-clock duration metadata for run, turn, inference, and tool lifecycles", () => {
    const events = [
      ...activeInferenceEvents(),
      committed(11, "ToolCallCompleted", {
        toolCallId: TOOL_CALL_ID,
        promptPart: Prompt.toolResultPart({
          id: ProviderPartId.make("tool-1"),
          name: ToolName.make("noop"),
          isFailure: false,
          result: { ok: true },
        }),
      }),
      committed(12, "InferenceCompleted", {
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: INFERENCE_ID,
      }),
      committed(13, "TurnCompleted", { runId: RUN_ID, turnId: TURN_ID }),
      committed(14, "RunCompleted", { runId: RUN_ID }),
    ];

    const state = reduceCommittedEvents(events);
    const decoded = decodeReducedStateCheckpoint(encodeReducedStateCheckpoint(state), events);

    expect(state.runs.get(RUN_ID)).toMatchObject({
      durationMs: 10,
      startedAtMs: 1_715_000_000_004,
      terminalAtMs: 1_715_000_000_014,
    });
    expect(state.turns.get(TURN_ID)).toMatchObject({
      durationMs: 8,
      startedAtMs: 1_715_000_000_005,
      terminalAtMs: 1_715_000_000_013,
    });
    expect(state.inferences.get(INFERENCE_ID)).toMatchObject({
      durationMs: 6,
      startedAtMs: 1_715_000_000_006,
      terminalAtMs: 1_715_000_000_012,
    });
    expect(state.toolCalls.get(TOOL_CALL_ID)).toMatchObject({
      durationMs: 3,
      startedAtMs: 1_715_000_000_008,
      terminalAtMs: 1_715_000_000_011,
    });
    expect(decoded.runs.get(RUN_ID)?.durationMs).toBe(10);
    expect(decoded.turns.get(TURN_ID)?.durationMs).toBe(8);
    expect(decoded.inferences.get(INFERENCE_ID)?.durationMs).toBe(6);
    expect(decoded.toolCalls.get(TOOL_CALL_ID)?.durationMs).toBe(3);
  });

  it("classifies unfinished command/run/turn/inference/tool lifecycles", () => {
    const state = reduceCommittedEvents(activeInferenceEvents());
    const recoverable = classifyRecoverableWork(state);

    expect(state.lastSeq).toBe(SequenceNumber.make(8));
    expect(state.commands.get(COMMAND_ID)).toMatchObject({ startedSeq: SequenceNumber.make(2) });
    expect(state.messages.get(MESSAGE_ID)).toMatchObject({
      _tag: "User",
      commandId: COMMAND_ID,
      content: command.content,
    });
    expect(recoverable.activeCommands.map((entry) => entry.commandId)).toEqual([COMMAND_ID]);
    expect(recoverable.activeRuns.map((entry) => entry.runId)).toEqual([RUN_ID]);
    expect(recoverable.activeTurns.map((entry) => entry.turnId)).toEqual([TURN_ID]);
    expect(recoverable.activeInferences.map((entry) => entry.inferenceId)).toEqual([INFERENCE_ID]);
    expect(recoverable.openToolCalls.map((entry) => entry.toolCallId)).toEqual([TOOL_CALL_ID]);
    expect(recoverable.runningToolCalls.map((entry) => entry.toolCallId)).toEqual([TOOL_CALL_ID]);
  });

  it("clears recoverable work after interruption terminals and stop application", () => {
    const state = reduceCommittedEvents([
      ...activeInferenceEvents(),
      committed(9, "CommandAdmitted", {
        command: new StopTurnCommand({ commandId: STOP_COMMAND_ID }),
      }),
      committed(10, "CommandStarted", { commandId: STOP_COMMAND_ID }),
      committed(11, "StopTurnRequested", {
        commandId: STOP_COMMAND_ID,
        runId: RUN_ID,
        turnId: TURN_ID,
      }),
      committed(12, "ToolCallFailed", {
        toolCallId: TOOL_CALL_ID,
        error: { message: "turn stopped" },
        promptPart: Prompt.toolResultPart({
          id: ProviderPartId.make("tool-1"),
          name: ToolName.make("noop"),
          isFailure: true,
          result: { message: "turn stopped" },
        }),
      }),
      committed(13, "InferenceFailed", {
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: INFERENCE_ID,
        error: { message: "inference interrupted" },
      }),
      committed(14, "TurnStopped", { runId: RUN_ID, turnId: TURN_ID, reason: "interrupted" }),
      committed(15, "RunInterrupted", { runId: RUN_ID, reason: "interrupted" }),
      committed(16, "CommandCancelled", { commandId: COMMAND_ID, reason: "interrupted" }),
      committed(17, "StopTurnApplied", {
        commandId: STOP_COMMAND_ID,
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: INFERENCE_ID,
      }),
      committed(18, "CommandCompleted", { commandId: STOP_COMMAND_ID }),
    ]);
    const recoverable = classifyRecoverableWork(state);

    expect(recoverable).toMatchObject({
      activeCommands: [],
      activeRuns: [],
      activeTurns: [],
      activeInferences: [],
      openToolCalls: [],
      runningToolCalls: [],
      pendingStopRequests: [],
    });
    expect(state.stopRequests.get(STOP_COMMAND_ID)).toMatchObject({
      requestedSeq: SequenceNumber.make(11),
      appliedSeq: SequenceNumber.make(17),
      appliedInferenceId: INFERENCE_ID,
    });
  });

  it("treats rejected tool decisions as closed model feedback, not recoverable tool work", () => {
    const state = reduceCommittedEvents([
      ...activeInferenceEvents().slice(0, 6),
      committed(7, "ToolCallRejected", {
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: INFERENCE_ID,
        toolCallId: TOOL_CALL_ID,
        promptPart: Prompt.toolResultPart({
          id: ProviderPartId.make("tool-1"),
          name: ToolName.make("noop"),
          isFailure: true,
          result: {
            message: "Expected params",
            reason: "invalid-params",
            modelFeedback: "Tool noop arguments were invalid: Expected params",
          },
        }),
      }),
    ]);
    const recoverable = classifyRecoverableWork(state);

    expect(state.toolCalls.get(TOOL_CALL_ID)?.decision).toMatchObject({ _tag: "Rejected" });
    expect(recoverable.openToolCalls).toEqual([]);
    expect(recoverable.runningToolCalls).toEqual([]);
  });

  it("bounds runtime replay state across repeated context rebases", () => {
    const events: Array<CommittedDurableEvent> = [];
    let seq = 1;
    for (let index = 1; index <= 100; index += 1) {
      const userSeq = seq++;
      const assistantSeq = seq++;
      const summarySeq = seq++;
      const rebaseSeq = seq++;
      const completedSeq = seq++;
      const compactionId = CompactionId.make(uuid(0xa000 + index));
      const summaryId = SummaryId.make(uuid(0xb000 + index));
      const userMessageId = MessageId.make(uuid(0xc000 + index));
      const assistantMessageId = MessageId.make(uuid(0xd000 + index));
      events.push(
        committed(userSeq, "UserMessageCommitted", {
          commandId: COMMAND_ID,
          messageId: userMessageId,
          content: command.content,
        }),
        committed(assistantSeq, "AssistantMessageCommitted", {
          messageId: assistantMessageId,
          runId: RUN_ID,
          turnId: TURN_ID,
          inferenceId: INFERENCE_ID,
          promptParts: [Prompt.textPart({ text: `assistant ${index}` })],
        }),
        committed(summarySeq, "SummaryCreated", {
          compactionId,
          summaryId,
          sourceFromSeq: SequenceNumber.make(1),
          sourceToSeq: SequenceNumber.make(assistantSeq),
          summary: { summaryId, text: `summary ${index}` },
        }),
        committed(rebaseSeq, "ContextRebased", {
          compactionId,
          summaryId,
          contextVersion: ContextVersion.make(index),
          retainedFromContextSeq: SequenceNumber.make(assistantSeq + 1),
        }),
        committed(completedSeq, "CompactionCompleted", { compactionId }),
      );
    }

    const state = reduceCommittedEvents(events);
    const nonSystemMessages = Array.from(state.messages.values()).filter(
      (message) => message._tag !== "System",
    );

    expect(nonSystemMessages).toEqual([]);
    expect(state.compactions.size).toBe(1);
    expect(state.context).toEqual({
      version: ContextVersion.make(100),
      currentSummaryId: SummaryId.make(uuid(0xb000 + 100)),
    });
  });

  it("folds incrementally with the same result as one full replay", () => {
    const events = [
      ...activeInferenceEvents(),
      committed(9, "InferenceCompleted", {
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: INFERENCE_ID,
      }),
      committed(10, "AssistantMessageCommitted", {
        messageId: MESSAGE_ID,
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: INFERENCE_ID,
        promptParts: [Prompt.textPart({ text: "hello" })],
      }),
      committed(11, "TurnCompleted", { runId: RUN_ID, turnId: TURN_ID }),
      committed(12, "RunCompleted", { runId: RUN_ID }),
      committed(13, "CommandCompleted", { commandId: COMMAND_ID }),
    ];

    const allAtOnce = reduceCommittedEvents(events);
    const incremental = foldReducedState(
      foldReducedState(initialReducedState, events.slice(0, 5)),
      events.slice(5),
    );

    expect(incremental).toEqual(allAtOnce);
    expect(classifyRecoverableWork(incremental).activeCommands).toEqual([]);
  });
});
