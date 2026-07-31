import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import {
  durableMessageTranscript,
  durableTranscriptPrompt,
  contextProjectionPromptWithUserContents,
} from "./message-transcript";
import type { MessageRecord } from "./reduced-state";
import {
  InferenceId,
  CommandId,
  MessageId,
  RunId,
  SequenceNumber,
  ToolCallId,
  TurnId,
} from "../types/core";

const COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const RUN_ID = RunId.make("018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a");
const TURN_ID = TurnId.make("018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a");
const INFERENCE_ID = InferenceId.make("018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a");
const SYSTEM_MESSAGE_ID = MessageId.make("018f6bd5-2f2a-7b1e-8f29-1f2e3d4c5b6a");
const USER_MESSAGE_ID = MessageId.make("018f6bd5-2f2a-7b1e-8f2a-1f2e3d4c5b6a");
const STEERING_MESSAGE_ID = MessageId.make("018f6bd5-2f2a-7b1e-8f2b-1f2e3d4c5b6a");
const ASSISTANT_MESSAGE_ID = MessageId.make("018f6bd5-2f2a-7b1e-8f2c-1f2e3d4c5b6a");
const TOOL_CALL_ID = ToolCallId.make("018f6bd5-2f2a-7b1e-8f2d-1f2e3d4c5b6a");
const SECOND_TOOL_CALL_ID = ToolCallId.make("018f6bd5-2f2a-7b1e-8f2e-1f2e3d4c5b6a");

const seq = (value: number) => SequenceNumber.make(value);

describe("message-transcript", () => {
  it("returns durable user and assistant messages in sequence order", () => {
    const user: MessageRecord = {
      _tag: "User",
      messageId: USER_MESSAGE_ID,
      commandId: COMMAND_ID,
      content: [Prompt.textPart({ text: "hello" })],
      seq: seq(2),
    };
    const steering: MessageRecord = {
      _tag: "Steering",
      messageId: STEERING_MESSAGE_ID,
      commandId: COMMAND_ID,
      runId: RUN_ID,
      content: [Prompt.textPart({ text: "steer" })],
      seq: seq(3),
    };
    const assistant: MessageRecord = {
      _tag: "Assistant",
      messageId: ASSISTANT_MESSAGE_ID,
      runId: RUN_ID,
      turnId: TURN_ID,
      inferenceId: INFERENCE_ID,
      content: { text: "hi" },
      seq: seq(4),
    };

    const transcript = durableMessageTranscript({
      messages: new Map([
        [assistant.messageId, assistant],
        [steering.messageId, steering],
        [user.messageId, user],
      ]),
    });

    expect(transcript).toEqual([user, assistant]);
  });

  it("includes consumed steering at the consumption sequence", () => {
    const steering: MessageRecord = {
      _tag: "Steering",
      messageId: STEERING_MESSAGE_ID,
      commandId: COMMAND_ID,
      runId: RUN_ID,
      content: [Prompt.textPart({ text: "steer" })],
      seq: seq(2),
      consumedSeq: seq(5),
      consumedTurnId: TURN_ID,
    };
    const assistant: MessageRecord = {
      _tag: "Assistant",
      messageId: ASSISTANT_MESSAGE_ID,
      runId: RUN_ID,
      turnId: TURN_ID,
      inferenceId: INFERENCE_ID,
      content: { text: "hi" },
      seq: seq(4),
    };

    const transcript = durableMessageTranscript({
      messages: new Map([
        [steering.messageId, steering],
        [assistant.messageId, assistant],
      ]),
    });

    expect(transcript).toEqual([assistant, steering]);
  });

  it("orders lifecycle user messages at their consumption boundaries", () => {
    const priorAssistant: MessageRecord = {
      _tag: "Assistant",
      messageId: ASSISTANT_MESSAGE_ID,
      runId: RUN_ID,
      turnId: TURN_ID,
      inferenceId: INFERENCE_ID,
      content: { text: "prior response" },
      seq: seq(3),
    };
    const steer: MessageRecord = {
      _tag: "User",
      messageId: USER_MESSAGE_ID,
      commandId: COMMAND_ID,
      content: [Prompt.textPart({ text: "test steer" })],
      requestedDisposition: "steer",
      disposition: "steer",
      seq: seq(1),
      consumedSeq: seq(4),
      consumedTurnId: TURN_ID,
    };
    const response: MessageRecord = {
      ...priorAssistant,
      messageId: MessageId.make("018f6bd5-2f2a-7b1e-8f2f-1f2e3d4c5b6a"),
      content: { text: "Done" },
      seq: seq(5),
    };
    const queue: MessageRecord = {
      ...steer,
      messageId: MessageId.make("018f6bd5-2f2a-7b1e-8f30-1f2e3d4c5b6a"),
      content: [Prompt.textPart({ text: "test queue" })],
      requestedDisposition: "queue",
      disposition: "queue",
      seq: seq(2),
      consumedSeq: seq(6),
    };

    expect(
      durableMessageTranscript({
        messages: new Map([
          [steer.messageId, steer],
          [queue.messageId, queue],
          [response.messageId, response],
          [priorAssistant.messageId, priorAssistant],
        ]),
      }),
    ).toEqual([priorAssistant, steer, response, queue]);
  });

  it("shows pending lifecycle users but excludes them from model context until consumed", () => {
    const pending: MessageRecord = {
      _tag: "User",
      messageId: USER_MESSAGE_ID,
      commandId: COMMAND_ID,
      content: [Prompt.textPart({ text: "pending steer" })],
      requestedDisposition: "steer",
      disposition: "steer",
      seq: seq(2),
    };
    const state = { messages: new Map([[pending.messageId, pending]]) };

    expect(durableMessageTranscript(state)).toEqual([pending]);
    expect(Prompt.make(durableTranscriptPrompt(state)).content).toEqual([]);

    const consumed = { ...pending, consumedSeq: seq(3), consumedTurnId: TURN_ID };
    expect(
      Prompt.make(durableTranscriptPrompt({ messages: new Map([[consumed.messageId, consumed]]) }))
        .content,
    ).toMatchObject([{ role: "user", content: [{ text: "pending steer" }] }]);
  });

  it("appends scheduler-selected user messages as distinct prompt messages", () => {
    const prompt = contextProjectionPromptWithUserContents({ messages: new Map() }, undefined, [
      [Prompt.textPart({ text: "first" })],
      [Prompt.textPart({ text: "second" })],
    ]);
    expect(Prompt.make(prompt).content).toMatchObject([
      { role: "user", content: [{ text: "first" }] },
      { role: "user", content: [{ text: "second" }] },
    ]);
  });

  it("builds provider prompt history from durable transcript messages", () => {
    const transcript = durableTranscriptPrompt({
      messages: new Map([
        [
          USER_MESSAGE_ID,
          {
            _tag: "User",
            messageId: USER_MESSAGE_ID,
            commandId: COMMAND_ID,
            content: [Prompt.textPart({ text: "hello" })],
            seq: seq(1),
          },
        ],
        [
          ASSISTANT_MESSAGE_ID,
          {
            _tag: "Assistant",
            messageId: ASSISTANT_MESSAGE_ID,
            runId: RUN_ID,
            turnId: TURN_ID,
            inferenceId: INFERENCE_ID,
            content: { text: "hi", reasoning: "thinking" },
            seq: seq(2),
          },
        ],
      ]),
    });

    expect(Prompt.make(transcript).content).toMatchObject([
      { role: "user", content: [{ type: "text", text: "hello" }] },
      {
        role: "assistant",
        content: [
          { type: "reasoning", text: "thinking" },
          { type: "text", text: "hi" },
        ],
      },
    ]);
  });

  it("includes the session's durable system message in provider context", () => {
    const transcript = durableTranscriptPrompt({
      messages: new Map([
        [
          SYSTEM_MESSAGE_ID,
          {
            _tag: "System",
            messageId: SYSTEM_MESSAGE_ID,
            content: "You are concise.",
            seq: seq(10),
          },
        ],
        [
          USER_MESSAGE_ID,
          {
            _tag: "User",
            messageId: USER_MESSAGE_ID,
            commandId: COMMAND_ID,
            content: [Prompt.textPart({ text: "hello" })],
            seq: seq(2),
          },
        ],
      ]),
    });

    expect(Prompt.make(transcript).content).toMatchObject([
      { role: "system", content: "You are concise." },
      { role: "user", content: [{ type: "text", text: "hello" }] },
    ]);
  });

  it("replays exact assistant and tool prompt parts from existing durable records", () => {
    const assistantPromptParts: ReadonlyArray<Prompt.AssistantMessagePart> = [
      Prompt.textPart({ text: "I will check." }),
      Prompt.reasoningPart({ text: "Need the tool." }),
      Prompt.toolCallPart({ id: "tool-call-1", name: "noop", params: {}, providerExecuted: false }),
    ];
    const toolResultPart = Prompt.toolResultPart({
      id: "tool-call-1",
      name: "noop",
      isFailure: false,
      result: { ok: true },
    });

    const transcript = durableTranscriptPrompt({
      messages: new Map([
        [
          USER_MESSAGE_ID,
          {
            _tag: "User",
            messageId: USER_MESSAGE_ID,
            commandId: COMMAND_ID,
            content: [Prompt.textPart({ text: "hello" })],
            seq: seq(1),
          },
        ],
        [
          ASSISTANT_MESSAGE_ID,
          {
            _tag: "Assistant",
            messageId: ASSISTANT_MESSAGE_ID,
            runId: RUN_ID,
            turnId: TURN_ID,
            inferenceId: INFERENCE_ID,
            content: { text: "I will check.", reasoning: "Need the tool." },
            promptParts: assistantPromptParts,
            seq: seq(3),
          },
        ],
      ]),
      toolCalls: new Map([
        [
          TOOL_CALL_ID,
          {
            toolCallId: TOOL_CALL_ID,
            decision: {
              _tag: "Created",
              seq: seq(2),
              runId: RUN_ID,
              turnId: TURN_ID,
              inferenceId: INFERENCE_ID,
              providerPartId: "tool-call-1" as never,
              toolName: "noop",
              params: {},
              providerExecuted: false,
              promptPart: assistantPromptParts[2] as Prompt.ToolCallPart,
            },
            terminal: {
              _tag: "Completed",
              seq: seq(4),
              result: { ok: true },
              promptPart: toolResultPart,
            },
          },
        ],
      ]),
    });

    expect(Prompt.make(transcript).content).toMatchObject([
      { role: "user", content: [{ type: "text", text: "hello" }] },
      { role: "assistant", content: assistantPromptParts },
      { role: "tool", content: [toolResultPart] },
    ]);
  });

  it("orders created tool results by assistant tool-call order, not terminal sequence", () => {
    const slowToolCallPart = Prompt.toolCallPart({
      id: "tool-call-slow",
      name: "ordered",
      params: { label: "slow" },
      providerExecuted: false,
    });
    const fastToolCallPart = Prompt.toolCallPart({
      id: "tool-call-fast",
      name: "ordered",
      params: { label: "fast" },
      providerExecuted: false,
    });
    const slowResultPart = Prompt.toolResultPart({
      id: "tool-call-slow",
      name: "ordered",
      isFailure: false,
      result: { label: "slow" },
    });
    const fastResultPart = Prompt.toolResultPart({
      id: "tool-call-fast",
      name: "ordered",
      isFailure: false,
      result: { label: "fast" },
    });

    const transcript = durableTranscriptPrompt({
      messages: new Map([
        [
          ASSISTANT_MESSAGE_ID,
          {
            _tag: "Assistant",
            messageId: ASSISTANT_MESSAGE_ID,
            runId: RUN_ID,
            turnId: TURN_ID,
            inferenceId: INFERENCE_ID,
            content: { text: "" },
            promptParts: [slowToolCallPart, fastToolCallPart],
            seq: seq(3),
          },
        ],
      ]),
      toolCalls: new Map([
        [
          TOOL_CALL_ID,
          {
            toolCallId: TOOL_CALL_ID,
            decision: {
              _tag: "Created",
              seq: seq(4),
              runId: RUN_ID,
              turnId: TURN_ID,
              inferenceId: INFERENCE_ID,
              providerPartId: "tool-call-slow" as never,
              toolName: "ordered",
              params: { label: "slow" },
              providerExecuted: false,
              promptPart: slowToolCallPart,
            },
            terminal: {
              _tag: "Completed",
              seq: seq(8),
              result: { label: "slow" },
              promptPart: slowResultPart,
            },
          },
        ],
        [
          SECOND_TOOL_CALL_ID,
          {
            toolCallId: SECOND_TOOL_CALL_ID,
            decision: {
              _tag: "Created",
              seq: seq(5),
              runId: RUN_ID,
              turnId: TURN_ID,
              inferenceId: INFERENCE_ID,
              providerPartId: "tool-call-fast" as never,
              toolName: "ordered",
              params: { label: "fast" },
              providerExecuted: false,
              promptPart: fastToolCallPart,
            },
            terminal: {
              _tag: "Completed",
              seq: seq(7),
              result: { label: "fast" },
              promptPart: fastResultPart,
            },
          },
        ],
      ]),
    });

    const toolMessages = Prompt.make(transcript).content.filter(
      (message) => message.role === "tool",
    );

    expect(toolMessages).toMatchObject([
      { role: "tool", content: [slowResultPart] },
      { role: "tool", content: [fastResultPart] },
    ]);
  });

  it("replays cancellation encoded as a failed tool result", () => {
    const toolCallPart = Prompt.toolCallPart({
      id: "tool-call-1",
      name: "noop",
      params: {},
      providerExecuted: false,
    });
    const toolResultPart = Prompt.toolResultPart({
      id: "tool-call-1",
      name: "noop",
      isFailure: true,
      result: { message: "tool call cancelled: interrupted" },
    });
    const assistantPromptParts: ReadonlyArray<Prompt.AssistantMessagePart> = [
      Prompt.textPart({ text: "I will check." }),
      toolCallPart,
    ];

    const transcript = durableTranscriptPrompt({
      messages: new Map([
        [
          ASSISTANT_MESSAGE_ID,
          {
            _tag: "Assistant",
            messageId: ASSISTANT_MESSAGE_ID,
            runId: RUN_ID,
            turnId: TURN_ID,
            inferenceId: INFERENCE_ID,
            content: { text: "I will check." },
            promptParts: assistantPromptParts,
            seq: seq(3),
          },
        ],
      ]),
      toolCalls: new Map([
        [
          TOOL_CALL_ID,
          {
            toolCallId: TOOL_CALL_ID,
            decision: {
              _tag: "Created",
              seq: seq(2),
              runId: RUN_ID,
              turnId: TURN_ID,
              inferenceId: INFERENCE_ID,
              providerPartId: "tool-call-1" as never,
              toolName: "noop",
              params: {},
              providerExecuted: false,
              promptPart: toolCallPart,
            },
            terminal: {
              _tag: "Failed",
              seq: seq(4),
              error: { message: "tool call cancelled: interrupted" },
              promptPart: toolResultPart,
            },
          },
        ],
      ]),
    });

    expect(Prompt.make(transcript).content).toMatchObject([
      { role: "assistant", content: assistantPromptParts },
      { role: "tool", content: [toolResultPart] },
    ]);
  });

  it("throws when a durable assistant tool call has no matching tool result", () => {
    const toolCallPart = Prompt.toolCallPart({
      id: "tool-call-1",
      name: "noop",
      params: {},
      providerExecuted: false,
    });

    expect(() =>
      durableTranscriptPrompt({
        messages: new Map([
          [
            ASSISTANT_MESSAGE_ID,
            {
              _tag: "Assistant",
              messageId: ASSISTANT_MESSAGE_ID,
              runId: RUN_ID,
              turnId: TURN_ID,
              inferenceId: INFERENCE_ID,
              content: { text: "" },
              promptParts: [toolCallPart],
              seq: seq(3),
            },
          ],
        ]),
        toolCalls: new Map([
          [
            TOOL_CALL_ID,
            {
              toolCallId: TOOL_CALL_ID,
              decision: {
                _tag: "Created",
                seq: seq(2),
                runId: RUN_ID,
                turnId: TURN_ID,
                inferenceId: INFERENCE_ID,
                providerPartId: "tool-call-1" as never,
                toolName: "noop",
                params: {},
                providerExecuted: false,
                promptPart: toolCallPart,
              },
              terminal: { _tag: "Cancelled", seq: seq(4), reason: "interrupted" },
            },
          ],
        ]),
      }),
    ).toThrow("without matching tool result");
  });

  it("throws for partial assistant tool calls that were interrupted before a decision committed", () => {
    const toolCallPart = Prompt.toolCallPart({
      id: "tool-call-1",
      name: "noop",
      params: {},
      providerExecuted: false,
    });

    expect(() =>
      durableTranscriptPrompt({
        messages: new Map([
          [
            ASSISTANT_MESSAGE_ID,
            {
              _tag: "AssistantPartial",
              messageId: ASSISTANT_MESSAGE_ID,
              runId: RUN_ID,
              turnId: TURN_ID,
              inferenceId: INFERENCE_ID,
              content: { text: "partial" },
              promptParts: [Prompt.textPart({ text: "partial" }), toolCallPart],
              reason: "inference interrupted before completion",
              seq: seq(3),
            },
          ],
        ]),
      }),
    ).toThrow("without matching tool result");
  });
});
