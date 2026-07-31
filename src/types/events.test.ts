import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { EventId, SessionId, ToolCallId } from "./core";
import {
  CommandAdmittedEvent,
  decodeUnknownEDADurableEvent,
  decodeUnknownEDADurableEventSync,
  EDAEphemeralEvent,
  EDADurableEvent,
  effectDurableAgentNamespace,
  EventEnvelope,
  ProviderPartId,
  ReasoningDeltaEvent,
  reasoningDeltaEventType,
  schemaV1,
  TextDeltaEvent,
  textDeltaEventType,
  ToolParamsStartEvent,
  toolParamsStartEventType,
  UnixEpochMillis,
} from "./events";

const EVENT_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const SESSION_ID = "018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a";
const COMMAND_ID = "018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a";
const RUN_ID = "018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a";
const TURN_ID = "018f6bd5-2f2a-7b1e-8f2a-1f2e3d4c5b6a";
const INFERENCE_ID = "018f6bd5-2f2a-7b1e-8f3a-1f2e3d4c5b6a";
const TOOL_CALL_ID = "018f6bd5-2f2a-7b1e-8f4a-1f2e3d4c5b6a";
const MESSAGE_ID = "018f6bd5-2f2a-7b1e-8f5a-1f2e3d4c5b6a";
const COMPACTION_ID = "018f6bd5-2f2a-7b1e-8f6a-1f2e3d4c5b6a";
const SUMMARY_ID = "018f6bd5-2f2a-7b1e-8f7a-1f2e3d4c5b6a";
const BASE_STATE_ID = "018f6bd5-2f2a-7b1e-8f8a-1f2e3d4c5b6a";
const CREATED_AT_MS = 1_715_000_000_000;
const TRACE_CONTEXT = {
  traceId: "11111111111111111111111111111111",
  spanId: "2222222222222222",
  sampled: true,
  tracestate: null,
};
const EVENT_TRACE = { span: TRACE_CONTEXT, links: [] };
const RUN_TRACE = {
  root: {
    traceId: "33333333333333333333333333333333",
    spanId: "4444444444444444",
    sampled: true,
    tracestate: null,
  },
  links: [],
};

const validEnvelope = {
  namespace: "effect-durable-agent",
  type: "CommandAdmitted",
  schemaVersion: 1,
  durability: "durable",
  eventId: EVENT_ID,
  sessionId: SESSION_ID,
  createdAtMs: 1_715_000_000_000,
  trace: EVENT_TRACE,
  payload: { commandId: "cmd-1" },
};

const validCommandAdmittedEvent = {
  namespace: "effect-durable-agent",
  type: "CommandAdmitted",
  schemaVersion: 1,
  durability: "durable",
  eventId: EVENT_ID,
  sessionId: SESSION_ID,
  createdAtMs: 1_715_000_000_000,
  trace: EVENT_TRACE,
  payload: {
    command: {
      _tag: "SubmitMessage",
      commandId: "018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a",
      disposition: "queue",
      content: "hello",
    },
  },
};

describe("event envelope", () => {
  it("decodes an untrusted envelope through Effect", () => {
    const decoded = Effect.runSync(Schema.decodeUnknownEffect(EventEnvelope)(validEnvelope));

    expect(decoded).toEqual(validEnvelope);
    expect(Schema.is(EventEnvelope)(decoded)).toBe(true);
  });

  it("decodes CommandAdmitted as the first concrete durable event", () => {
    const event = Effect.runSync(
      Schema.decodeUnknownEffect(CommandAdmittedEvent)(validCommandAdmittedEvent),
    );

    expect(event).toEqual({
      ...validCommandAdmittedEvent,
      payload: {
        command: {
          ...validCommandAdmittedEvent.payload.command,
          content: [expect.objectContaining({ type: "text", text: "hello" })],
        },
      },
    });
    expect(Schema.is(EventEnvelope)(event)).toBe(true);
    expect(Schema.is(EDADurableEvent)(event)).toBe(true);
  });

  it("routes framework durable event decoding by schemaVersion", () => {
    const event = Effect.runSync(decodeUnknownEDADurableEvent(validCommandAdmittedEvent));

    expect(event.type).toBe("CommandAdmitted");
    expect(event.payload.command.content).toEqual([
      expect.objectContaining({ type: "text", text: "hello" }),
    ]);
  });

  it("rejects unsupported framework durable event schema versions", () => {
    expect(() =>
      decodeUnknownEDADurableEventSync({
        ...validCommandAdmittedEvent,
        schemaVersion: 2,
      }),
    ).toThrow(/Unsupported EDA durable event schemaVersion 2/);
  });

  it("constructs TextDelta as the first concrete ephemeral event", () => {
    const event = TextDeltaEvent.make({
      namespace: effectDurableAgentNamespace,
      type: textDeltaEventType,
      schemaVersion: schemaV1,
      durability: "ephemeral",
      eventId: EventId.make(EVENT_ID),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
      payload: { providerPartId: ProviderPartId.make("text-1"), delta: "hello" },
    });

    expect(event.type).toBe("TextDelta");
    expect(event.durability).toBe("ephemeral");
    expect(Schema.is(EventEnvelope)(event)).toBe(true);
    expect(Schema.is(EDAEphemeralEvent)(event)).toBe(true);
  });

  it("constructs ReasoningDelta as a concrete ephemeral event", () => {
    const event = ReasoningDeltaEvent.make({
      namespace: effectDurableAgentNamespace,
      type: reasoningDeltaEventType,
      schemaVersion: schemaV1,
      durability: "ephemeral",
      eventId: EventId.make(EVENT_ID),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
      payload: { providerPartId: ProviderPartId.make("reasoning-1"), delta: "considering options" },
    });

    expect(event.type).toBe("ReasoningDelta");
    expect(event.durability).toBe("ephemeral");
    expect(Schema.is(EventEnvelope)(event)).toBe(true);
    expect(Schema.is(EDAEphemeralEvent)(event)).toBe(true);
  });

  it("constructs ToolParamsStart as a concrete ephemeral event", () => {
    const event = ToolParamsStartEvent.make({
      namespace: effectDurableAgentNamespace,
      type: toolParamsStartEventType,
      schemaVersion: schemaV1,
      durability: "ephemeral",
      eventId: EventId.make(EVENT_ID),
      sessionId: SessionId.make(SESSION_ID),
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
      payload: {
        providerPartId: ProviderPartId.make("tool-params-1"),
        toolCallId: ToolCallId.make("018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a"),
        toolName: "runBash",
        providerExecuted: false,
      },
    });

    expect(event.type).toBe("ToolParamsStart");
    expect(event.durability).toBe("ephemeral");
    expect(Schema.is(EventEnvelope)(event)).toBe(true);
    expect(Schema.is(EDAEphemeralEvent)(event)).toBe(true);
  });

  it("accepts every built-in durable event type in the union", () => {
    const durable = (type: string, payload: unknown) => ({
      namespace: "effect-durable-agent",
      type,
      schemaVersion: 1,
      durability: "durable",
      eventId: EVENT_ID,
      sessionId: SESSION_ID,
      createdAtMs: CREATED_AT_MS,
      trace: EVENT_TRACE,
      payload,
    });
    const error = { message: "failed" };
    const runTurn = { runId: RUN_ID, turnId: TURN_ID };
    const inference = { ...runTurn, inferenceId: INFERENCE_ID };

    const events = [
      durable("CommandAdmitted", {
        command: { _tag: "StopTurn", commandId: COMMAND_ID },
      }),
      durable("CommandStarted", { commandId: COMMAND_ID }),
      durable("CommandCompleted", { commandId: COMMAND_ID }),
      durable("CommandFailed", { commandId: COMMAND_ID, error }),
      durable("CommandCancelled", { commandId: COMMAND_ID, reason: "superseded" }),
      durable("SystemMessageCommitted", {
        messageId: MESSAGE_ID,
        content: "You are concise.",
      }),
      durable("UserMessageCommitted", {
        commandId: COMMAND_ID,
        messageId: MESSAGE_ID,
        content: [Prompt.textPart({ text: "hello" })],
      }),
      durable("SteeringMessageQueued", {
        commandId: COMMAND_ID,
        messageId: MESSAGE_ID,
        runId: RUN_ID,
        content: [Prompt.textPart({ text: "steer" })],
      }),
      durable("SteeringMessageCancelled", {
        messageId: MESSAGE_ID,
        runId: RUN_ID,
        reason: "run terminalized",
      }),
      durable("UserMessageSubmitted", {
        commandId: COMMAND_ID,
        messageId: MESSAGE_ID,
        disposition: "queue",
        content: [Prompt.textPart({ text: "pending" })],
      }),
      durable("UserMessagePromoted", {
        commandId: COMMAND_ID,
        messageId: MESSAGE_ID,
        from: "queue",
        to: "steer",
      }),
      durable("UserMessageCancelled", {
        commandId: COMMAND_ID,
        messageId: MESSAGE_ID,
        reason: "user-cancel",
      }),
      durable("MessageQueuePaused", {
        stopCommandId: COMMAND_ID,
        runId: RUN_ID,
        messageIds: [MESSAGE_ID],
        reason: "user-interrupted",
      }),
      durable("PendingMessagesPaused", {
        interruptionCommandId: COMMAND_ID,
        runId: RUN_ID,
        messageIds: [MESSAGE_ID],
        reason: "user-interrupted",
      }),
      durable("AssistantMessageCommitted", {
        messageId: MESSAGE_ID,
        ...inference,
        promptParts: [Prompt.textPart({ text: "hello" })],
      }),
      durable("AssistantPartialCommitted", {
        messageId: MESSAGE_ID,
        ...inference,
        promptParts: [Prompt.textPart({ text: "partial" })],
        reason: "stopped",
      }),
      durable("RunStarted", {
        runId: RUN_ID,
        commandIds: [COMMAND_ID],
        modelSelection: { provider: "openai", modelId: "gpt-4" },
        trace: RUN_TRACE,
      }),
      durable("RunCompleted", { runId: RUN_ID }),
      durable("RunFailed", { runId: RUN_ID, error }),
      durable("RunInterrupted", { runId: RUN_ID, reason: "user" }),
      durable("RecoveryCompleted", {
        trigger: "runtime-restart",
        continuation: {
          commandId: COMMAND_ID,
          interruptedRunId: RUN_ID,
          replacementRunId: RUN_ID,
        },
      }),
      durable("TurnStarted", { ...runTurn, inputMessageIds: [MESSAGE_ID] }),
      durable("TurnCompleted", { ...runTurn, usage: { inputTokens: 1, outputTokens: 2 } }),
      durable("TurnFailed", { ...runTurn, error }),
      durable("TurnStopped", { ...runTurn, reason: "user" }),
      durable("InferenceStarted", inference),
      durable("InferenceCompleted", {
        ...inference,
        finishReason: "stop",
        usage: { outputTokens: 2 },
      }),
      durable("InferenceFailed", { ...inference, error: { ...error, code: "provider.failed" } }),
      durable("ToolCallCreated", {
        ...inference,
        toolCallId: TOOL_CALL_ID,
        promptPart: Prompt.toolCallPart({
          id: "tool-1",
          name: "runBash",
          params: { command: "pwd" },
          providerExecuted: false,
        }),
      }),
      durable("ToolCallRejected", {
        ...inference,
        toolCallId: TOOL_CALL_ID,
        promptPart: Prompt.toolResultPart({
          id: "tool-1",
          name: "runBash",
          isFailure: true,
          result: {
            message: "Expected string",
            reason: "invalid-params",
            modelFeedback: "Tool runBash arguments were invalid: Expected string",
          },
        }),
      }),
      durable("ToolCallStarted", { toolCallId: TOOL_CALL_ID }),
      durable("ToolCallCompleted", {
        toolCallId: TOOL_CALL_ID,
        promptPart: Prompt.toolResultPart({
          id: "tool-1",
          name: "runBash",
          isFailure: false,
          result: { exitCode: 0 },
        }),
      }),
      durable("ToolCallFailed", {
        toolCallId: TOOL_CALL_ID,
        error,
        promptPart: Prompt.toolResultPart({
          id: "tool-1",
          name: "runBash",
          isFailure: true,
          result: error,
        }),
      }),
      durable("StopTurnRequested", { commandId: COMMAND_ID, runId: RUN_ID, turnId: TURN_ID }),
      durable("StopTurnApplied", { commandId: COMMAND_ID, ...inference }),
      durable("ContextProjected", { contextVersion: 1, throughSeq: 1 }),
      durable("CompactionRequested", {
        compactionId: COMPACTION_ID,
        sourceFromSeq: 1,
        sourceToSeq: 2,
      }),
      durable("CompactionStarted", { compactionId: COMPACTION_ID }),
      durable("SummaryCreated", {
        compactionId: COMPACTION_ID,
        summaryId: SUMMARY_ID,
        sourceFromSeq: 1,
        sourceToSeq: 2,
        summary: { text: "summary" },
      }),
      durable("CompactionCompleted", { compactionId: COMPACTION_ID }),
      durable("ContextRebased", {
        compactionId: COMPACTION_ID,
        summaryId: SUMMARY_ID,
        contextVersion: 2,
        retainedFromContextSeq: 2,
      }),
      durable("CompactionFailed", { compactionId: COMPACTION_ID, error }),
      durable("BaseStateRequested", { baseStateId: BASE_STATE_ID, requestedThroughSeq: 2 }),
      durable("BaseStateCreated", { baseStateId: BASE_STATE_ID, coversThroughSeq: 2 }),
      durable("BaseStateFailed", { baseStateId: BASE_STATE_ID, error }),
    ];

    for (const event of events) {
      expect(() => Schema.decodeUnknownSync(EDADurableEvent)(event), event.type).not.toThrow();
    }
  });

  it("accepts every built-in ephemeral event type in the union", () => {
    const ephemeral = (type: string, payload: unknown) => ({
      namespace: "effect-durable-agent",
      type,
      schemaVersion: 1,
      durability: "ephemeral",
      eventId: EVENT_ID,
      sessionId: SESSION_ID,
      createdAtMs: CREATED_AT_MS,
      trace: EVENT_TRACE,
      payload,
    });

    const events = [
      ephemeral("TextDelta", { providerPartId: "text-1", delta: "hello" }),
      ephemeral("ReasoningDelta", { providerPartId: "reasoning-1", delta: "thinking" }),
      ephemeral("ToolParamsStart", {
        providerPartId: "tool-1",
        toolCallId: TOOL_CALL_ID,
        toolName: "runBash",
        providerExecuted: false,
      }),
      ephemeral("ToolParamsDelta", {
        providerPartId: "tool-1",
        toolCallId: TOOL_CALL_ID,
        delta: '{"command"',
      }),
      ephemeral("ToolParamsEnd", { providerPartId: "tool-1", toolCallId: TOOL_CALL_ID }),
      ephemeral("SubscriberStatus", { status: "connected" }),
      ephemeral("TraceStatus", { status: "span-started", spanName: "turn.run" }),
    ];

    for (const event of events) {
      expect(() => Schema.decodeUnknownSync(EDAEphemeralEvent)(event), event.type).not.toThrow();
    }
  });

  it("rejects invalid concrete durable event boundaries", () => {
    expect(() =>
      Schema.decodeUnknownSync(EDADurableEvent)({
        ...validCommandAdmittedEvent,
        namespace: "other-agent",
      }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(EDADurableEvent)({
        ...validCommandAdmittedEvent,
        schemaVersion: 2,
      }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(EDADurableEvent)({
        ...validCommandAdmittedEvent,
        durability: "ephemeral",
      }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(EDADurableEvent)({
        ...validCommandAdmittedEvent,
        eventId: EVENT_ID.replace("7", "4"),
      }),
    ).toThrow();
  });

  it("rejects invalid concrete ToolCallRejected reasons", () => {
    const rejected = {
      namespace: "effect-durable-agent",
      type: "ToolCallRejected",
      schemaVersion: 1,
      durability: "durable",
      eventId: EVENT_ID,
      sessionId: SESSION_ID,
      createdAtMs: CREATED_AT_MS,
      trace: EVENT_TRACE,
      payload: {
        runId: RUN_ID,
        turnId: TURN_ID,
        inferenceId: INFERENCE_ID,
        toolCallId: TOOL_CALL_ID,
        promptPart: Prompt.toolResultPart({
          id: "tool-1",
          name: "runBash",
          isFailure: true,
          result: {
            message: "Expected string",
            reason: "bad-json",
            modelFeedback: "Tool runBash arguments were invalid: Expected string",
          },
        }),
      },
    };

    expect(() => Schema.decodeUnknownSync(EDADurableEvent)(rejected)).toThrow();
  });

  it("rejects invalid concrete ephemeral event boundaries", () => {
    const event = {
      namespace: "effect-durable-agent",
      type: "TextDelta",
      schemaVersion: 1,
      durability: "ephemeral",
      eventId: EVENT_ID,
      sessionId: SESSION_ID,
      createdAtMs: CREATED_AT_MS,
      trace: EVENT_TRACE,
      payload: { providerPartId: "text-1", delta: "hello" },
    };

    expect(() =>
      Schema.decodeUnknownSync(EDAEphemeralEvent)({ ...event, durability: "durable" }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(EDAEphemeralEvent)({ ...event, namespace: "other" }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(EDAEphemeralEvent)({ ...event, schemaVersion: 2 }),
    ).toThrow();
  });

  it("rejects invalid concrete CommandAdmitted events", () => {
    expect(() =>
      Effect.runSync(
        Schema.decodeUnknownEffect(CommandAdmittedEvent)({
          ...validCommandAdmittedEvent,
          type: "RunStarted",
        }),
      ),
    ).toThrow();
    expect(() =>
      Effect.runSync(
        Schema.decodeUnknownEffect(CommandAdmittedEvent)({
          ...validCommandAdmittedEvent,
          durability: "ephemeral",
        }),
      ),
    ).toThrow();
    expect(() =>
      Effect.runSync(
        Schema.decodeUnknownEffect(CommandAdmittedEvent)({
          ...validCommandAdmittedEvent,
          payload: { command: { _tag: "StopTurn", commandId: "not-a-uuid" } },
        }),
      ),
    ).toThrow();
  });

  it("rejects invalid envelope boundaries", () => {
    expect(() =>
      Effect.runSync(
        Schema.decodeUnknownEffect(EventEnvelope)({ ...validEnvelope, namespace: "" }),
      ),
    ).toThrow();
    expect(() =>
      Effect.runSync(
        Schema.decodeUnknownEffect(EventEnvelope)({ ...validEnvelope, schemaVersion: 0 }),
      ),
    ).toThrow();
    expect(() =>
      Effect.runSync(
        Schema.decodeUnknownEffect(EventEnvelope)({ ...validEnvelope, createdAtMs: -1 }),
      ),
    ).toThrow();
    expect(() =>
      Effect.runSync(
        Schema.decodeUnknownEffect(EventEnvelope)({ ...validEnvelope, durability: "stored" }),
      ),
    ).toThrow();
    expect(() =>
      Effect.runSync(
        Schema.decodeUnknownEffect(EventEnvelope)({
          ...validEnvelope,
          sessionId: "not-a-uuid",
        }),
      ),
    ).toThrow();
  });
});
