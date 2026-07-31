import * as Layer from "effect/Layer";
import type * as LanguageModel from "effect/unstable/ai/LanguageModel";

import { SessionId } from "../../src/types/core";
import { EDARuntime } from "../../src/services/runtime";
import { makeEdaTestLayer } from "../../src/testkit/layers";
import type { OfflineTraceScenario } from "./scenarios/types";

/** Inputs for composing an offline, non-Cloudflare EDA runtime for one scenario. */
export interface OfflineTraceRuntimeLayerOptions {
  readonly scenario: OfflineTraceScenario;
  readonly sessionId: SessionId;
  readonly modelLayer: Layer.Layer<LanguageModel.LanguageModel>;
}

/** Compose the non-CF in-memory EDA runtime used by offline trace scenarios. */
export const makeOfflineTraceRuntimeLayer = ({
  scenario,
  sessionId,
  modelLayer,
}: OfflineTraceRuntimeLayerOptions) =>
  EDARuntime.Live({
    modelSelection: scenario.modelSelection,
    ...(scenario.systemPrompt === undefined ? {} : { systemPrompt: scenario.systemPrompt }),
  }).pipe(
    Layer.provideMerge(
      makeEdaTestLayer({
        sessionId,
        modelLayer,
        clock: "live",
        ...(scenario.toolkit === undefined ? {} : { toolkit: scenario.toolkit }),
      }),
    ),
  );
