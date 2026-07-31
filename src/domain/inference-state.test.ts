import { describe, expect, it } from "vite-plus/test";

import {
  initialInferenceState,
  step,
  type InferenceState,
  type InferenceStepInput,
} from "./inference-state";
import { InferenceId, RunId, ToolCallId, TurnId } from "../types/core";
import { ProviderPartId } from "../types/events";

const identity = {
  runId: RunId.make("018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a"),
  turnId: TurnId.make("018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a"),
  inferenceId: InferenceId.make("018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a"),
};

const providerPartId = ProviderPartId.make("tool-params-1");
const secondProviderPartId = ProviderPartId.make("tool-params-2");
const toolCallId = ToolCallId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const secondToolCallId = ToolCallId.make("018f6bd5-2f2a-7b1e-8f2a-1f2e3d4c5b6a");

const apply = (state: InferenceState, input: InferenceStepInput) => step(state, input).next;

const validToolCall = (id = providerPartId, callId = toolCallId): InferenceStepInput => ({
  type: "tool-call",
  providerPartId: id,
  toolCallId: callId,
  toolName: "runBash",
  params: { command: "pwd" },
  providerExecuted: false,
  validation: { _tag: "ValidToolParams", params: { command: "pwd" } },
});

describe("inference-state step", () => {
  it("emits durable tool decisions only inside the finish finalization batch", () => {
    let state = initialInferenceState(identity);
    const inputs: ReadonlyArray<InferenceStepInput> = [
      {
        type: "tool-params-start",
        providerPartId,
        toolCallId,
        toolName: "runBash",
        providerExecuted: false,
      },
      { type: "tool-params-delta", providerPartId, delta: '{"command":"pwd"}' },
      { type: "tool-params-end", providerPartId },
      validToolCall(),
    ];

    for (const input of inputs) {
      const result = step(state, input);
      expect(result.emissions.some((emission) => emission.kind === "finalize")).toBe(false);
      state = result.next;
    }

    const finished = step(state, { type: "finish", finishReason: "tool-calls" });

    expect(finished.emissions).toHaveLength(1);
    expect(finished.emissions[0]).toMatchObject({ kind: "finalize" });
    if (finished.emissions[0]?.kind !== "finalize") throw new Error("expected finalize");
    expect(finished.emissions[0].events.map((event) => event.type)).toEqual([
      "ToolCallCreated",
      "InferenceCompleted",
    ]);
  });

  it("keeps text accumulators equal to in-order deltas", () => {
    let state = initialInferenceState(identity);
    state = apply(state, {
      type: "text-delta",
      providerPartId: ProviderPartId.make("text-1"),
      delta: "hello ",
    });
    state = apply(state, {
      type: "reasoning-delta",
      providerPartId: ProviderPartId.make("reasoning-1"),
      delta: "think ",
    });
    state = apply(state, {
      type: "text-delta",
      providerPartId: ProviderPartId.make("text-1"),
      delta: "world",
    });
    state = apply(state, {
      type: "reasoning-delta",
      providerPartId: ProviderPartId.make("reasoning-1"),
      delta: "again",
    });

    expect(state.assistantText).toBe("hello world");
    expect(state.reasoningText).toBe("think again");
  });

  it("handles duplicate starts and chunks after end as values, never thrown errors", () => {
    let state = initialInferenceState(identity);
    const start: InferenceStepInput = {
      type: "tool-params-start",
      providerPartId,
      toolCallId,
      toolName: "runBash",
      providerExecuted: false,
    };
    state = apply(state, start);

    const duplicateStart = step(state, start);
    expect(duplicateStart.next).toBe(state);
    expect(duplicateStart.emissions).toEqual([]);

    state = apply(state, { type: "tool-params-end", providerPartId });
    const afterEnd = step(state, { type: "tool-params-delta", providerPartId, delta: "late" });
    expect(afterEnd.next).toBe(state);
    expect(afterEnd.emissions).toEqual([]);

    const rejected = step(state, {
      type: "tool-call",
      providerPartId,
      toolCallId,
      toolName: "runBash",
      params: { command: 1 },
      providerExecuted: false,
      validation: { _tag: "InvalidToolParams", message: "command must be a string" },
    });

    expect(rejected.next.drafts.get(providerPartId)).toMatchObject({ status: "staged-rejected" });
    expect(rejected.next.decisions).toMatchObject([
      { _tag: "ToolCallRejectedDecision", reason: "invalid-params" },
    ]);
  });

  it("keeps one decision per final tool-call provider part", () => {
    let state = initialInferenceState(identity);
    state = apply(state, {
      type: "tool-params-start",
      providerPartId,
      toolCallId,
      toolName: "runBash",
      providerExecuted: false,
    });
    state = apply(state, { type: "tool-params-end", providerPartId });
    state = apply(state, validToolCall());
    state = apply(state, validToolCall(secondProviderPartId, secondToolCallId));

    expect(state.drafts.get(providerPartId)).toMatchObject({ status: "staged-created" });
    expect(state.drafts.has(secondProviderPartId)).toBe(false);
    expect(state.decisions.map((decision) => decision.providerPartId)).toEqual([
      providerPartId,
      secondProviderPartId,
    ]);

    const replaced = apply(state, {
      type: "tool-call",
      providerPartId,
      toolCallId,
      toolName: "runBash",
      params: { command: 1 },
      providerExecuted: false,
      validation: { _tag: "InvalidToolParams", message: "command must be a string" },
    });

    expect(replaced.decisions).toHaveLength(2);
    expect(
      replaced.decisions.find((decision) => decision.providerPartId === providerPartId),
    ).toMatchObject({ _tag: "ToolCallRejectedDecision", reason: "invalid-params" });
  });

  it("does not emit after the inference is sealed", () => {
    const sealed = step(initialInferenceState(identity), {
      type: "finish",
      finishReason: "stop",
    }).next;
    const afterSeal = step(sealed, {
      type: "text-delta",
      providerPartId: ProviderPartId.make("text-1"),
      delta: "too late",
    });

    expect(afterSeal.next).toBe(sealed);
    expect(afterSeal.emissions).toEqual([]);
  });

  it("emits exactly one terminal across generated terminal interleavings", () => {
    const terminalSequences: ReadonlyArray<ReadonlyArray<InferenceStepInput>> = [
      [
        { type: "finish", finishReason: "stop" },
        { type: "error", error: { message: "late error" } },
      ],
      [
        { type: "error", error: { message: "provider failed" } },
        { type: "finish", finishReason: "late finish" },
      ],
      [
        validToolCall(secondProviderPartId, secondToolCallId),
        { type: "finish", finishReason: "tool-calls" },
        validToolCall(providerPartId, toolCallId),
      ],
    ];

    for (const sequence of terminalSequences) {
      let state = initialInferenceState(identity);
      const terminalKinds: Array<"finalize" | "fail"> = [];
      for (const input of sequence) {
        const result = step(state, input);
        terminalKinds.push(
          ...result.emissions.flatMap((emission) =>
            emission.kind === "finalize" || emission.kind === "fail" ? [emission.kind] : [],
          ),
        );
        state = result.next;
      }
      expect(terminalKinds).toHaveLength(1);
    }
  });
});
