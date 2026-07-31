import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as LanguageModel from "effect/unstable/ai/LanguageModel";
import type * as Prompt from "effect/unstable/ai/Prompt";
import type * as Response from "effect/unstable/ai/Response";

import { canonicalPrompt } from "./prompt-hash";
import type { OfflineTraceRecorderShape } from "./trace-recorder";
import { toJsonValue } from "../json";

/** Canonical prompt metadata captured before a traced model request. */
export interface RecordedPrompt {
  readonly index: number;
  readonly promptHash: string;
  readonly messages: unknown;
  readonly json: string;
}

/** Decorate a LanguageModel layer with trace events for requests, stream parts, and finish usage. */
export const tracingLanguageModelLayer = (
  base: Layer.Layer<LanguageModel.LanguageModel>,
  recorder: OfflineTraceRecorderShape,
): Layer.Layer<LanguageModel.LanguageModel> =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const model = yield* LanguageModel.LanguageModel;
      const nextIndex = yield* Ref.make(0);

      const streamText = ((input: {
        readonly prompt: Prompt.RawInput;
        readonly toolkit?: unknown;
      }) =>
        Stream.unwrap(
          Effect.gen(function* () {
            const index = yield* Ref.getAndUpdate(nextIndex, (current) => current + 1);
            const prompt = yield* canonicalPrompt(input.prompt);
            yield* recorder.record("model.request", {
              index,
              promptHash: prompt.sha256,
              prompt: prompt.messages,
              toolNames: Object.keys(inputToolkit(input)?.tools ?? {}),
            });

            return (
              model.streamText(input as never) as Stream.Stream<Response.AnyPart, unknown>
            ).pipe(
              Stream.tap((part) =>
                recorder.record("model.part", {
                  index,
                  part: toJsonValue(part),
                }),
              ),
              Stream.tap((part) =>
                part.type === "finish"
                  ? recorder.record("model.finish", {
                      index,
                      reason: part.reason,
                      usage: toJsonValue(part.usage),
                      response: toJsonValue(part.response),
                      metadata: toJsonValue(part.metadata),
                    })
                  : Effect.void,
              ),
            );
          }),
        )) as LanguageModel.Service["streamText"];

      return {
        ...model,
        streamText,
      } satisfies LanguageModel.Service;
    }),
  ).pipe(Layer.provide(base));

const inputToolkit = (input: {
  readonly toolkit?: unknown;
}): { readonly tools: Record<string, unknown> } | undefined => {
  const toolkit = input.toolkit;
  return typeof toolkit === "object" && toolkit !== null && "tools" in toolkit
    ? (toolkit as { readonly tools: Record<string, unknown> })
    : undefined;
};
