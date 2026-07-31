import * as fc from "fast-check";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { durableMessageTranscript, durableTranscriptPrompt } from "./message-transcript";
import type { MessageRecord, ToolCallRecord } from "./reduced-state";
import {
  InferenceId,
  CommandId,
  MessageId,
  RunId,
  SequenceNumber,
  ToolCallId,
  TurnId,
} from "../types/core";

const propertyRuns = 100;

type MessageSpec = {
  readonly kind: "user" | "assistant" | "assistant-partial" | "steering";
  readonly seq: number;
  readonly steeringState: "pending" | "consumed" | "cancelled";
  readonly consumedSeq: number;
};

const seq = (value: number) => SequenceNumber.make(value);

const uuid = (slot: number): string => {
  const fourth = `8${(slot % 0x1000).toString(16).padStart(3, "0")}`;
  const fifth = slot.toString(16).padStart(12, "0").slice(-12);
  return `018f6bd5-2f2a-7b1e-${fourth}-${fifth}`;
};

const commandId = (slot: number) => CommandId.make(uuid(0xa00 + slot));
const messageId = (slot: number) => MessageId.make(uuid(0xb00 + slot));
const runId = (slot: number) => RunId.make(uuid(0xc00 + slot));
const turnId = (slot: number) => TurnId.make(uuid(0xd00 + slot));
const inferenceId = (slot: number) => InferenceId.make(uuid(0xe00 + slot));
const toolCallId = (slot: number) => ToolCallId.make(uuid(0xf00 + slot));
const content = (text: string) => [Prompt.textPart({ text })];

const messageSpecArbitrary = fc.record({
  kind: fc.constantFrom<MessageSpec["kind"]>("user", "assistant", "assistant-partial", "steering"),
  seq: fc.integer({ min: 1, max: 10_000 }),
  steeringState: fc.constantFrom<MessageSpec["steeringState"]>("pending", "consumed", "cancelled"),
  consumedSeq: fc.integer({ min: 1, max: 10_000 }),
});

const messageRecord = (spec: MessageSpec, index: number): MessageRecord => {
  const id = messageId(index);
  switch (spec.kind) {
    case "user":
      return {
        _tag: "User",
        messageId: id,
        commandId: commandId(index),
        content: content(`user-${index}`),
        seq: seq(spec.seq),
      };
    case "assistant":
      return {
        _tag: "Assistant",
        messageId: id,
        runId: runId(index),
        turnId: turnId(index),
        inferenceId: inferenceId(index),
        content: { text: `assistant-${index}` },
        seq: seq(spec.seq),
      };
    case "assistant-partial":
      return {
        _tag: "AssistantPartial",
        messageId: id,
        runId: runId(index),
        turnId: turnId(index),
        inferenceId: inferenceId(index),
        content: { text: `partial-${index}` },
        reason: "interrupted",
        seq: seq(spec.seq),
      };
    case "steering":
      return {
        _tag: "Steering",
        messageId: id,
        commandId: commandId(index),
        runId: runId(index),
        content: content(`steer-${index}`),
        seq: seq(spec.seq),
        ...(spec.steeringState === "consumed"
          ? { consumedSeq: seq(spec.consumedSeq), consumedTurnId: turnId(index) }
          : {}),
        ...(spec.steeringState === "cancelled"
          ? { cancelledSeq: seq(spec.seq + 20_000), cancellationReason: "cancelled" }
          : {}),
      };
  }
};

const isTranscriptVisible = (spec: MessageSpec) =>
  spec.kind !== "steering" || spec.steeringState === "consumed";

const transcriptSeq = (spec: MessageSpec) =>
  spec.kind === "steering" ? spec.consumedSeq : spec.seq;

describe("durableMessageTranscript properties", () => {
  it("shows only transcript-visible messages in effective durable order", () => {
    fc.assert(
      fc.property(fc.array(messageSpecArbitrary, { maxLength: 30 }), (specs) => {
        const messages = new Map<MessageId, MessageRecord>(
          specs.map((spec, index) => [messageId(index), messageRecord(spec, index)]),
        );
        const transcript = durableMessageTranscript({ messages });
        const expected = specs
          .map((spec, index) => ({ spec, index }))
          .filter(({ spec }) => isTranscriptVisible(spec))
          .sort(
            (left, right) =>
              transcriptSeq(left.spec) - transcriptSeq(right.spec) ||
              left.spec.seq - right.spec.seq,
          );

        expect(transcript.map((message) => message.messageId)).toEqual(
          expected.map(({ index }) => messageId(index)),
        );
        expect(
          transcript
            .filter((message) => message._tag === "Steering")
            .every((message) => message.consumedSeq !== undefined),
        ).toBe(true);
      }),
      { numRuns: propertyRuns },
    );
  });
});

const promptToolCallIds = (prompt: Prompt.RawInput): ReadonlyArray<string> =>
  Prompt.make(prompt).content.flatMap((message) =>
    "content" in message && Array.isArray(message.content)
      ? message.content.flatMap((part) => (part.type === "tool-call" ? [part.id] : []))
      : [],
  );

const promptToolResultIds = (prompt: Prompt.RawInput): ReadonlyArray<string> =>
  Prompt.make(prompt).content.flatMap((message) =>
    message.role === "tool" && Array.isArray(message.content)
      ? message.content.flatMap((part) => (part.type === "tool-result" ? [part.id] : []))
      : [],
  );

const validToolMessage = (index: number): MessageRecord => ({
  _tag: "Assistant",
  messageId: messageId(index),
  runId: runId(index),
  turnId: turnId(index),
  inferenceId: inferenceId(index),
  content: { text: "" },
  promptParts: [
    Prompt.toolCallPart({
      id: `tool-call-${index}`,
      name: "noop",
      params: {},
      providerExecuted: false,
    }),
  ],
  seq: seq(index * 10 + 3),
});

const validFailedToolCall = (index: number): readonly [ToolCallId, ToolCallRecord] => {
  const id = toolCallId(index);
  const result = { message: "tool call cancelled: interrupted" };
  return [
    id,
    {
      toolCallId: id,
      decision: {
        _tag: "Created",
        seq: seq(index * 10 + 2),
        runId: runId(index),
        turnId: turnId(index),
        inferenceId: inferenceId(index),
        providerPartId: `tool-call-${index}` as never,
        toolName: "noop",
        params: {},
        providerExecuted: false,
      },
      terminal: {
        _tag: "Failed",
        seq: seq(index * 10 + 4),
        error: result,
        promptPart: Prompt.toolResultPart({
          id: `tool-call-${index}`,
          name: "noop",
          isFailure: true,
          result,
        }),
      },
    },
  ];
};

describe("durableTranscriptPrompt properties", () => {
  it("successful prompt projection pairs assistant tool calls with tool-result prompt items", () => {
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 30 }), (count) => {
        const indexes = Array.from({ length: count }, (_, index) => index);
        const messages = new Map<MessageId, MessageRecord>(
          indexes.map((index) => [messageId(index), validToolMessage(index)]),
        );
        const toolCalls = new Map<ToolCallId, ToolCallRecord>(indexes.map(validFailedToolCall));
        const prompt = durableTranscriptPrompt({ messages, toolCalls });
        const resultIds = new Set(promptToolResultIds(prompt));

        expect(promptToolCallIds(prompt).every((id) => resultIds.has(id))).toBe(true);
      }),
      { numRuns: propertyRuns },
    );
  });
});
