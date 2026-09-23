import { assert } from "@effect/vitest";
import { describe, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as Prompt from "effect/unstable/ai/Prompt";
import { CommittedDurableEvent } from "../services/session-store";
import { decodeUnknownEDADurableEventSync } from "../types/events";
import { makeEDARunTrace, makeRootEDAEventTrace } from "../types/tracing";
import {
  reduceCommittedEvents,
  encodeReducedStateCheckpoint,
  decodeReducedStateCheckpoint,
} from "./reduced-state";
import { totalTokenUsage } from "./model-usage";

const id = (n: number) => `018f6bd5-2f2a-7b1e-8f1a-${String(n).padStart(12, "0")}`;
const fixture = () => {
  const lifecycle = { runId: id(2), turnId: id(3) };
  const first = { ...lifecycle, attemptId: id(4) };
  const second = { ...lifecycle, attemptId: id(5) };
  const events = [
    {
      type: "RunStarted",
      payload: {
        runId: id(2),
        commandIds: [],
        modelSelection: { provider: "openai", modelId: "gpt-5.5" },
        trace: makeEDARunTrace(),
      },
    },
    { type: "TurnStarted", payload: { ...lifecycle, inputMessageIds: [] } },
    { type: "TurnAttemptStarted", payload: first },
    {
      type: "TurnAttemptCompleted",
      payload: { ...first, usage: { inputTokens: 3272, cachedInputTokens: 0, outputTokens: 160 } },
    },
    {
      type: "AssistantMessageCommitted",
      payload: {
        ...first,
        messageId: id(6),
        promptParts: [Prompt.textPart({ text: "First retained response" })],
      },
    },
    {
      type: "ToolCallCreated",
      payload: {
        ...first,
        toolCallId: id(8),
        promptPart: Prompt.toolCallPart({
          id: "tool-1",
          name: "noop",
          params: {},
          providerExecuted: false,
        }),
      },
    },
    { type: "ToolCallStarted", payload: { toolCallId: id(8) } },
    {
      type: "ToolCallCompleted",
      payload: {
        toolCallId: id(8),
        promptPart: Prompt.toolResultPart({
          id: "tool-1",
          name: "noop",
          result: "ok",
          isFailure: false,
          providerExecuted: false,
        }),
      },
    },
    { type: "TurnAttemptStarted", payload: second },
    {
      type: "TurnAttemptCompleted",
      payload: {
        ...second,
        usage: { inputTokens: 3702, cachedInputTokens: 3072, outputTokens: 157 },
      },
    },
    {
      type: "AssistantMessageCommitted",
      payload: {
        ...second,
        messageId: id(7),
        promptParts: [Prompt.textPart({ text: "Second retained response" })],
      },
    },
    { type: "TurnCompleted", payload: lifecycle },
    { type: "RunCompleted", payload: { runId: id(2) } },
  ];
  return events.map((event, index) => ({
    position: { seq: index + 1, subSeq: 0 },
    event: {
      ...event,
      namespace: "effect-durable-agent",
      schemaVersion: 1,
      durability: "durable",
      eventId: id(100 + index),
      sessionId: id(1),
      createdAtMs: 1000 + index,
      trace: makeRootEDAEventTrace(),
    },
  }));
};

describe("immutable historical attempt replay", () => {
  it("retains source identities and accounts both attempts in their original turn", () => {
    const source = fixture();
    const before = JSON.stringify(source);
    const events = source.map((entry) =>
      Schema.decodeUnknownSync(CommittedDurableEvent)({
        ...entry,
        event: decodeUnknownEDADurableEventSync(entry.event),
      }),
    );
    assert.deepEqual(events, source);
    const state = reduceCommittedEvents(events);
    assert.strictEqual(state.turns.size, 1);
    assert.strictEqual(state.inferences.size, 2);
    assert.strictEqual(state.messages.size, 2);
    assert.strictEqual(state.toolCalls.size, 1);
    assert.strictEqual([...state.toolCalls.values()][0]?.decision?.inferenceId, id(4));
    const usage = totalTokenUsage(state.tokenConsumption);
    assert.strictEqual(usage.inputTokens, 6974);
    assert.strictEqual(usage.cachedInputTokens, 3072);
    assert.strictEqual(usage.outputTokens, 317);
    assert.deepEqual(
      decodeReducedStateCheckpoint(encodeReducedStateCheckpoint(state), events),
      state,
    );
    assert.deepEqual(reduceCommittedEvents(events), state);
    assert.strictEqual(JSON.stringify(source), before);
  });
  it("rejects unsupported historical types, malformed identities and future schema versions", () => {
    const source = fixture().find((entry) => entry.event.type === "TurnAttemptStarted");
    assert.isDefined(source);
    if (source === undefined) throw new Error("Missing fixture");
    assert.throws(() =>
      decodeUnknownEDADurableEventSync({ ...source.event, type: "TurnAttemptUnknown" }),
    );
    assert.throws(() => decodeUnknownEDADurableEventSync({ ...source.event, schemaVersion: 2 }));
    assert.throws(() =>
      decodeUnknownEDADurableEventSync({
        ...source.event,
        payload: { runId: id(2), turnId: id(3), attemptId: "invalid" },
      }),
    );
  });
});
