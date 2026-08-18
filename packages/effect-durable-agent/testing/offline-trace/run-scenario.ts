import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import type * as LanguageModel from "effect/unstable/ai/LanguageModel";

import { CommandId, SequenceNumber, SessionId } from "../../src/types/core";
import type { PositionedEvent } from "../../src/types/events";
import { EDASessionStore } from "../../src/services/session-store";
import { LiveEventBus } from "../../src/services/live-event-bus";
import { EDARuntime } from "../../src/services/runtime";
import { sequentialUuidV7 } from "../../src/services/id-generator";
import {
  makeRunSummary,
  type OfflineTraceArtifacts,
  type OfflineTracePromptArtifact,
} from "./harness/artifacts";
import { latencyFromTrace } from "./harness/latency";
import { makeOfflineTraceRecorder } from "./harness/trace-recorder";
import { tracingLanguageModelLayer } from "./harness/tracing-language-model";
import { toJsonValue } from "./json";
import { makeOfflineTraceRuntimeLayer } from "./runtime-layer";
import type { OfflineTraceScenario } from "./scenarios/types";
import { cacheMetricsFromTrace } from "./verify/cache-metrics";
import { verifyPromptPrefixes } from "./verify/prompt-prefix";

/** Inputs for running one scenario and collecting durable/live/model trace artifacts. */
export interface RunOfflineTraceScenarioOptions {
  readonly scenario: OfflineTraceScenario;
  readonly modelLayer: Layer.Layer<LanguageModel.LanguageModel>;
  readonly sessionId?: SessionId;
  readonly runId?: string;
}

/** Run one offline EDA scenario and return all trace artifacts in memory. */
export const runOfflineTraceScenario = ({
  scenario,
  modelLayer,
  sessionId = SessionId.make(sequentialUuidV7(50_000)),
  runId = `${scenario.name}-${Date.now()}`,
}: RunOfflineTraceScenarioOptions): Effect.Effect<OfflineTraceArtifacts, unknown> =>
  Effect.gen(function* () {
    const recorder = yield* makeOfflineTraceRecorder;
    const startedAtMs = yield* Clock.currentTimeMillis;
    yield* recorder.record("run.started", { runId, scenario: scenario.name });

    const tracedModel = tracingLanguageModelLayer(modelLayer, recorder);
    const runtimeLayer = makeOfflineTraceRuntimeLayer({
      scenario,
      sessionId,
      modelLayer: tracedModel,
    });

    const { durableEvents, liveEvents } = yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        const liveBus = yield* LiveEventBus;
        const store = yield* EDASessionStore;
        const live = yield* liveBus.subscribe();
        const finalCommandId = scenario.commands.at(-1)?.commandId;
        if (finalCommandId === undefined) {
          return yield* Effect.die(new Error(`Scenario ${scenario.name} has no commands`));
        }
        const liveFiber = yield* live.pipe(
          Stream.takeUntil((event) => isCommandTerminalFor(finalCommandId, event)),
          Stream.runCollect,
          Effect.forkScoped,
        );

        for (const command of scenario.commands) {
          yield* runtime.submitAndBlock(command);
        }

        const liveEvents = Array.from(yield* Fiber.join(liveFiber));
        const durableEvents = Array.from(
          yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect),
        );
        return { durableEvents, liveEvents };
      }).pipe(Effect.provide(runtimeLayer)),
    );

    for (const event of durableEvents) {
      yield* recorder.record("durable.event", event);
    }
    for (const event of liveEvents) {
      yield* recorder.record("live.event", event);
    }

    const traceBeforeFinish = yield* recorder.events();
    const prompts = promptArtifactsFromTrace(traceBeforeFinish);
    const promptPrefix = verifyPromptPrefixes(prompts);
    const cacheMetrics = cacheMetricsFromTrace(traceBeforeFinish);
    const latencies = latencyFromTrace(traceBeforeFinish);
    const finishedAtMs = yield* Clock.currentTimeMillis;

    yield* recorder.record("verification.result", { promptPrefix, cacheMetrics, latencies });
    yield* recorder.record("run.finished", { runId, scenario: scenario.name, status: "completed" });

    const trace = yield* recorder.events();
    return {
      summary: makeRunSummary({
        runId,
        scenario: scenario.name,
        status: "completed",
        startedAtMs,
        finishedAtMs,
        commandCount: scenario.commands.length,
        durableEventCount: durableEvents.length,
        liveEventCount: liveEvents.length,
        modelRequestCount: prompts.length,
        promptPrefix,
        cacheMetrics,
      }),
      trace,
      durableEvents: durableEvents.map(toJsonValue),
      liveEvents: liveEvents.map(toJsonValue),
      prompts,
    };
  });

const isCommandTerminalFor = (commandId: CommandId, event: PositionedEvent): boolean => {
  if (
    event.event.type !== "CommandCompleted" &&
    event.event.type !== "CommandFailed" &&
    event.event.type !== "CommandCancelled"
  ) {
    return false;
  }
  return (event.event.payload as { readonly commandId: CommandId }).commandId === commandId;
};

const promptArtifactsFromTrace = (
  trace: ReadonlyArray<{ readonly kind: string; readonly payload: unknown }>,
): ReadonlyArray<OfflineTracePromptArtifact> =>
  trace.flatMap((event): ReadonlyArray<OfflineTracePromptArtifact> => {
    if (event.kind !== "model.request") {
      return [];
    }
    const payload = event.payload as {
      readonly index?: unknown;
      readonly promptHash?: unknown;
      readonly prompt?: unknown;
    };
    return typeof payload.index === "number" && typeof payload.promptHash === "string"
      ? [
          {
            index: payload.index,
            promptHash: payload.promptHash,
            prompt: toJsonValue(payload.prompt),
          },
        ]
      : [];
  });
