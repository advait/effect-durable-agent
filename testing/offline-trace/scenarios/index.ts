import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Prompt from "effect/unstable/ai/Prompt";

import { SubmitMessageCommand } from "../../../src/types/commands";
import { CommandId } from "../../../src/types/core";
import { SystemPromptText, type ModelSelectionPayload } from "../../../src/types/events";
import { sequentialUuidV7 } from "../../../src/services/id-generator";
import { makeEDAToolkit } from "../../../src/services/tool-registry";
import type { OfflineTraceScenario } from "./types";

/** Built-in offline trace scenario identifiers. */
export type OfflineTraceScenarioName =
  | "no-tools"
  | "multi-turn"
  | "prefix-cache"
  | "framework-tool"
  | "parallel-tools"
  | "rejected-tool";

/** Stable list of built-in offline trace scenarios available to the runner. */
export const offlineTraceScenarioNames: ReadonlyArray<OfflineTraceScenarioName> = [
  "no-tools",
  "multi-turn",
  "prefix-cache",
  "framework-tool",
  "parallel-tools",
  "rejected-tool",
];

/** Build one named offline scenario with the requested provider model id. */
export const makeOfflineTraceScenario = (
  name: OfflineTraceScenarioName,
  modelId: string,
): Effect.Effect<OfflineTraceScenario> => {
  switch (name) {
    case "no-tools":
      return Effect.succeed(noToolsScenario(modelId));
    case "multi-turn":
      return Effect.succeed(multiTurnScenario(modelId));
    case "prefix-cache":
      return Effect.succeed(prefixCacheScenario(modelId));
    case "framework-tool":
      return frameworkToolScenario(modelId);
    case "parallel-tools":
      return parallelToolsScenario(modelId);
    case "rejected-tool":
      return rejectedToolScenario(modelId);
  }
};

const modelSelection = (modelId: string): ModelSelectionPayload => ({
  provider: "openai",
  modelId,
});

const command = (id: number, text: string) =>
  new SubmitMessageCommand({
    commandId: CommandId.make(sequentialUuidV7(id)),
    disposition: "queue",
    content: [Prompt.textPart({ text })],
  });

const noToolsScenario = (modelId: string): OfflineTraceScenario => ({
  name: "no-tools",
  description: "One real LLM turn without tools, used to prove offline tracing works.",
  modelSelection: modelSelection(modelId),
  commands: [command(10_001, 'Reply with exactly one short sentence containing the word "pong".')],
});

const cacheStableSystemPrompt = SystemPromptText.make(
  [
    "You are validating prompt-prefix caching for effect-durable-agent.",
    "Repeat the stable marker EDA-PREFIX-CACHE exactly once in every answer.",
    "Keep answers short. The stable context intentionally remains unchanged across turns.",
    ...Array.from(
      { length: 260 },
      (_, index) =>
        `EDA-CACHE-STABLE-${String(index + 1).padStart(4, "0")}: This durable system instruction line must remain byte-for-byte identical so provider prompt-prefix caching can reuse it.`,
    ),
  ].join("\n"),
);

const multiTurnScenario = (modelId: string): OfflineTraceScenario => ({
  name: "multi-turn",
  description:
    "A longer conversational trace with follow-up turns that depend on previous answers.",
  modelSelection: modelSelection(modelId),
  commands: [
    command(
      10_401,
      'We are testing multi-turn memory. Remember the code word "river" and answer with one sentence.',
    ),
    command(10_402, "What code word did I ask you to remember? Answer with only the word."),
    command(
      10_403,
      "Now use the remembered code word in a new three-word phrase about durable agents.",
    ),
    command(10_404, "Summarize what just happened in exactly one short sentence."),
  ],
});

const prefixCacheScenario = (modelId: string): OfflineTraceScenario => ({
  name: "prefix-cache",
  description:
    "Multiple turns with a large durable system prompt, used to measure provider cache hits and EDA prompt-prefix behavior.",
  modelSelection: modelSelection(modelId),
  systemPrompt: cacheStableSystemPrompt,
  commands: [
    command(10_101, "Turn 1: answer with the marker and the word alpha."),
    command(10_102, "Turn 2: answer with the marker and the word beta."),
    command(10_103, "Turn 3: answer with the marker and the word gamma."),
  ],
});

const EchoParams = Schema.Struct({ text: Schema.String });
const EchoTool = Tool.make("edaEcho", {
  description: "Echo a short string for EDA framework tool tracing.",
  parameters: EchoParams,
  success: Schema.Unknown,
});

const frameworkToolScenario = (modelId: string): Effect.Effect<OfflineTraceScenario> =>
  Effect.map(
    makeEDAToolkit([EchoTool], {
      edaEcho: (params) => Effect.succeed({ echoed: params.text }),
    }),
    (toolkit) => ({
      name: "framework-tool",
      description:
        "A real LLM may call one framework-owned Effect tool and continue from the result.",
      modelSelection: modelSelection(modelId),
      toolkit,
      commands: [
        command(
          10_201,
          'Use the edaEcho tool exactly once with text "framework-ok", then answer with the echoed value.',
        ),
      ],
    }),
  );

const ParallelLookupParams = Schema.Struct({ label: Schema.String });
const ParallelLookupTool = Tool.make("edaParallelLookup", {
  description:
    "Look up a labeled value for EDA parallel framework tool-call tracing. Call it once per requested label.",
  parameters: ParallelLookupParams,
  success: Schema.Unknown,
});

const parallelToolsScenario = (modelId: string): Effect.Effect<OfflineTraceScenario> =>
  Effect.map(
    makeEDAToolkit([ParallelLookupTool], {
      edaParallelLookup: (params) =>
        Effect.gen(function* () {
          if (params.label === "slow") {
            yield* Effect.sleep("50 millis");
          }
          return { label: params.label, value: `lookup:${params.label}` };
        }),
    }),
    (toolkit) => ({
      name: "parallel-tools",
      description:
        "A real LLM should emit multiple framework tool calls in one turn; EDA executes them concurrently and preserves model order in the continuation prompt.",
      modelSelection: modelSelection(modelId),
      toolkit,
      commands: [
        command(
          10_501,
          'Call edaParallelLookup twice before answering: once with label "slow" and once with label "fast". After both results return, answer with both values in label order: slow then fast.',
        ),
      ],
    }),
  );

const NumberParams = Schema.Struct({ value: Schema.Number });
const NumberTool = Tool.make("needNumber", {
  description: "Accept only a numeric value for rejected-tool correction tracing.",
  parameters: NumberParams,
  success: Schema.Unknown,
});

const rejectedToolScenario = (modelId: string): Effect.Effect<OfflineTraceScenario> =>
  Effect.map(
    makeEDAToolkit([NumberTool], {
      needNumber: (params) => Effect.succeed({ doubled: params.value * 2 }),
    }),
    (toolkit) => ({
      name: "rejected-tool",
      description:
        "Exercise invalid tool arguments and corrective prompt structure when the model emits them.",
      modelSelection: modelSelection(modelId),
      toolkit,
      commands: [
        command(
          10_301,
          'Call needNumber with value "not-a-number" first. If corrected, explain the correction briefly.',
        ),
      ],
    }),
  );
