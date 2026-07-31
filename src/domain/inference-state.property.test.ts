import * as fc from "fast-check";
import { describe, expect, it } from "vite-plus/test";

import { initialInferenceState, step, type InferenceStepInput } from "./inference-state";
import { InferenceId, RunId, TurnId } from "../types/core";
import { ProviderPartId } from "../types/events";

const propertyRuns = 100;

type SimpleInputSpec = {
  readonly kind:
    | "text-start"
    | "text-delta"
    | "text-end"
    | "reasoning-start"
    | "reasoning-delta"
    | "reasoning-end"
    | "finish"
    | "error";
  readonly delta: string;
};

const identity = {
  runId: RunId.make("018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a"),
  turnId: TurnId.make("018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a"),
  inferenceId: InferenceId.make("018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a"),
};

const providerPartId = (index: number) => ProviderPartId.make(`part-${index % 4}`);

const simpleInputSpecArbitrary = fc.record({
  kind: fc.constantFrom<SimpleInputSpec["kind"]>(
    "text-start",
    "text-delta",
    "text-end",
    "reasoning-start",
    "reasoning-delta",
    "reasoning-end",
    "finish",
    "error",
  ),
  delta: fc.string({ maxLength: 16 }),
});

const toInput = (spec: SimpleInputSpec, index: number): InferenceStepInput => {
  switch (spec.kind) {
    case "text-start":
    case "text-end":
    case "reasoning-start":
    case "reasoning-end":
      return { type: spec.kind, providerPartId: providerPartId(index) };
    case "text-delta":
      return { type: "text-delta", providerPartId: providerPartId(index), delta: spec.delta };
    case "reasoning-delta":
      return { type: "reasoning-delta", providerPartId: providerPartId(index), delta: spec.delta };
    case "finish":
      return { type: "finish", finishReason: spec.delta.length === 0 ? undefined : spec.delta };
    case "error":
      return { type: "error", error: { message: spec.delta } };
  }
};

describe("inference-state step properties", () => {
  it("accumulates deltas before the first terminal and never emits after sealing", () => {
    fc.assert(
      fc.property(fc.array(simpleInputSpecArbitrary, { maxLength: 50 }), (specs) => {
        let state = initialInferenceState(identity);
        let expectedAssistantText = "";
        let expectedReasoningText = "";
        let sealed = false;
        let terminalEmissionCount = 0;

        specs.forEach((spec, index) => {
          const before = state;
          const result = step(state, toInput(spec, index));
          const terminalEmissions = result.emissions.filter(
            (emission) => emission.kind === "finalize" || emission.kind === "fail",
          );

          if (sealed) {
            expect(result.next).toBe(before);
            expect(result.emissions).toEqual([]);
          } else {
            terminalEmissionCount += terminalEmissions.length;
            if (spec.kind === "text-delta") {
              expectedAssistantText += spec.delta;
              expect(result.emissions).toMatchObject([
                { kind: "ephemeral", event: { type: "TextDelta", payload: { delta: spec.delta } } },
              ]);
            }
            if (spec.kind === "reasoning-delta") {
              expectedReasoningText += spec.delta;
              expect(result.emissions).toMatchObject([
                {
                  kind: "ephemeral",
                  event: { type: "ReasoningDelta", payload: { delta: spec.delta } },
                },
              ]);
            }
            if (spec.kind === "finish" || spec.kind === "error") {
              sealed = true;
            }
          }

          state = result.next;
        });

        expect(state.assistantText).toBe(expectedAssistantText);
        expect(state.reasoningText).toBe(expectedReasoningText);
        expect(terminalEmissionCount).toBeLessThanOrEqual(1);
      }),
      { numRuns: propertyRuns },
    );
  });
});
