import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as LanguageModel from "effect/unstable/ai/LanguageModel";
import type * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";

import { SessionId } from "../types/core";
import { type DurableEventEnvelope, UnixEpochMillis } from "../types/events";

import { EDASessionStore } from "../services/session-store";
import { CompactionExecutor, CompactionPolicy, CompactionRunner } from "../services/compaction";
import type { EDASessionStoreShape } from "../services/session-store";
import { EventFactory } from "../services/event-factory";
import { IdGenerator } from "../services/id-generator";
import { EDAKeepAlive } from "../services/keep-alive";
import { LiveEventBus } from "../services/live-event-bus";
import { EDAPromptProjector } from "../services/prompt-projector";
import { EDAReducerRegistry, type EDAReducer } from "../services/reducer-registry";
import { EDASinkRegistry, type EDASink } from "../services/sink-registry";
import { SinkCheckpointStore } from "../services/sink-checkpoint-store";
import { SessionContext } from "../services/session-context";
import { EDASessionQuery } from "../services/session-query";
import { SessionState } from "../services/session-state";
import { ToolExecutor } from "../services/tool-executor";
import { EDAToolRegistry } from "../services/tool-registry";
import type { EDAModelToolkit, ToolParamsSchema } from "../services/tool-registry";
import { InferenceRunner } from "../services/inference-runner";
import type { InferenceRunnerStreamPart } from "../services/inference-runner";
import { TurnRunner } from "../services/turn-runner";

/** Default fixed test wall-clock; matches the historical CREATED_AT_MS test constant. */
export const testNowMs = UnixEpochMillis.make(1_715_000_000_000);

/** A Clock whose current time never advances; sleep resolves immediately. */
export const makeFixedClock = (nowMs: number): Clock.Clock => ({
  currentTimeMillisUnsafe: () => nowMs,
  currentTimeMillis: Effect.sync(() => nowMs),
  currentTimeNanosUnsafe: () => BigInt(nowMs) * 1_000_000n,
  currentTimeNanos: Effect.sync(() => BigInt(nowMs) * 1_000_000n),
  sleep: () => Effect.void,
});

/** Layer pinning the ambient Clock reference to a fixed instant. */
export const FixedClock = (nowMs: number) => Layer.succeed(Clock.Clock, makeFixedClock(nowMs));

/** Fake provider stream input; arrays are consumed one streamText call at a time. */
export type TestModelParts =
  | Stream.Stream<InferenceRunnerStreamPart, unknown>
  | ReadonlyArray<Stream.Stream<InferenceRunnerStreamPart, unknown>>;

/** Options for the fake Effect AI LanguageModel used by EDA runtime tests. */
export interface TestLanguageModelOptions {
  readonly parts: TestModelParts;
  /** Optional fake generateText output for compaction/summarization tests. */
  readonly generateText?: string | ((input: { readonly prompt: Prompt.RawInput }) => string);
  readonly onGenerateText?: (input: { readonly prompt: Prompt.RawInput }) => void;
  readonly onStreamText?: (input: {
    readonly index: number;
    readonly prompt: Prompt.RawInput;
    readonly toolNames: ReadonlyArray<string>;
  }) => void;
}

/** Fake provider whose streamText returns configured test streams; other methods die. */
export const makeLanguageModelLayer = (options: TestModelParts | TestLanguageModelOptions) => {
  const config = isTestLanguageModelOptions(options) ? options : { parts: options };
  const unused = () => Effect.die(new Error("unused fake LanguageModel method"));
  return Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const nextIndex = yield* Ref.make(0);
      const streamText = ((input: { readonly prompt: Prompt.RawInput }) =>
        Stream.unwrap(
          Effect.gen(function* () {
            const index = yield* Ref.getAndUpdate(nextIndex, (current) => current + 1);
            config.onStreamText?.({
              index,
              prompt: input.prompt,
              toolNames: Object.keys(inputToolkit(input)?.tools ?? {}),
            });
            if (!Array.isArray(config.parts)) {
              return config.parts;
            }
            const stream = config.parts[index];
            return stream === undefined
              ? yield* Effect.die(
                  new Error(`No fake LanguageModel stream configured for call ${index}`),
                )
              : stream;
          }),
        )) as LanguageModel.Service["streamText"];
      const generateText = ((input: { readonly prompt: Prompt.RawInput }) =>
        Effect.sync(() => {
          config.onGenerateText?.({ prompt: input.prompt });
          if (config.generateText === undefined) {
            return Effect.runSync(unused()) as never;
          }
          const text =
            typeof config.generateText === "function"
              ? config.generateText({ prompt: input.prompt })
              : config.generateText;
          return new LanguageModel.GenerateTextResponse([Response.makePart("text", { text })]);
        })) as LanguageModel.Service["generateText"];
      return {
        generateText,
        generateObject: unused as LanguageModel.Service["generateObject"],
        streamText,
      };
    }),
  );
};

const inputToolkit = (input: {
  readonly prompt: Prompt.RawInput;
  readonly toolkit?: unknown;
}): { readonly tools: Record<string, unknown> } | undefined => {
  const toolkit = input.toolkit;
  return typeof toolkit === "object" && toolkit !== null && "tools" in toolkit
    ? (toolkit as { readonly tools: Record<string, unknown> })
    : undefined;
};

const isTestLanguageModelOptions = (
  options: TestModelParts | TestLanguageModelOptions,
): options is TestLanguageModelOptions =>
  typeof options === "object" &&
  options !== null &&
  "parts" in options &&
  !Stream.isStream(options);

/** Single-session in-memory layer graph options for EDA service tests. */
export interface EdaTestLayerOptions {
  readonly sessionId: SessionId;
  /** Explicit ID sequence (Deterministic); omit for counter-based Sequential IDs. */
  readonly ids?: ReadonlyArray<string>;
  /** Real/alternate provider layer; when absent, the fake stream provider is used. */
  readonly modelLayer?: Layer.Layer<LanguageModel.LanguageModel>;
  /** Fake provider stream parts; defaults to an empty stream. Ignored when `modelLayer` is provided. */
  readonly parts?: TestModelParts;
  /** Optional fake generateText output for compaction/summarization tests. */
  readonly generateText?: string | ((input: { readonly prompt: Prompt.RawInput }) => string);
  /** Test hook for inspecting each fake generateText call. */
  readonly onGenerateText?: (input: { readonly prompt: Prompt.RawInput }) => void;
  /** Test hook for inspecting each fake streamText call. */
  readonly onStreamText?: (input: {
    readonly index: number;
    readonly prompt: Prompt.RawInput;
    readonly toolNames: ReadonlyArray<string>;
  }) => void;
  /** Handler-backed Effect Toolkit; overrides `toolSchemas` when present. */
  readonly toolkit?: EDAModelToolkit;
  /** Test/convenience schema-only tools; execution fails unless `toolkit` is provided. */
  readonly toolSchemas?: ReadonlyMap<string, ToolParamsSchema>;
  /** Durable prefix used when constructing a fresh in-memory runtime for recovery tests. */
  readonly seedEvents?: ReadonlyArray<DurableEventEnvelope>;
  /** Fixed wall-clock for event timestamps; defaults to `testNowMs`. */
  readonly nowMs?: number;
  /** Use the ambient live Clock instead of pinning test time. Intended for offline trace latency runs. */
  readonly clock?: "fixed" | "live";
  /** Wrap the in-memory store shape, e.g. to inject one-shot failures. Test-only. */
  readonly wrapStore?: (inner: EDASessionStoreShape) => EDASessionStoreShape;
  /** Override host keepAlive tracking. Test-only. */
  readonly keepAliveLayer?: Layer.Layer<EDAKeepAlive>;
  /** Override compaction policy; defaults to disabled. */
  readonly compactionPolicyLayer?: Layer.Layer<CompactionPolicy>;
  /** Override compaction executor; defaults to disabled. */
  readonly compactionExecutorLayer?: Layer.Layer<
    CompactionExecutor,
    never,
    LanguageModel.LanguageModel
  >;
  /** App-specific durable metadata reducers. */
  readonly reducers?: ReadonlyArray<EDAReducer<any>>;
  /** Override state-to-LLM context projection. */
  readonly promptProjectorLayer?: Layer.Layer<EDAPromptProjector>;
  /** App-provided EDA sinks. */
  readonly sinks?: ReadonlyArray<EDASink>;
}

/**
 * One in-memory layer graph for the whole session runtime, exposing every
 * EDA service plus the fixed Clock. Layer constants are shared by reference,
 * so all services see the same store, bus, and ID counter.
 */
export const makeEdaTestLayer = (options: EdaTestLayerOptions) => {
  const BaseStore =
    options.seedEvents === undefined
      ? EDASessionStore.InMemory(options.sessionId)
      : EDASessionStore.InMemorySeeded(options.sessionId, options.seedEvents);
  const Store =
    options.wrapStore === undefined
      ? BaseStore
      : Layer.effect(
          EDASessionStore,
          Effect.gen(function* () {
            const inner = yield* EDASessionStore;
            return options.wrapStore!(inner);
          }),
        ).pipe(Layer.provide(BaseStore));
  const Bus = LiveEventBus.Live;
  const KeepAlive = options.keepAliveLayer ?? EDAKeepAlive.Noop;
  const SinkCheckpoints = SinkCheckpointStore.InMemory;
  const Ids =
    options.ids === undefined ? IdGenerator.Sequential : IdGenerator.Deterministic(options.ids);
  const Session = SessionContext.Live(options.sessionId);
  const TestClock = options.clock === "live" ? Layer.empty : FixedClock(options.nowMs ?? testNowMs);
  const Factory = EventFactory.Live.pipe(Layer.provideMerge(Layer.mergeAll(Session, Ids)));
  const ReducerRegistry = EDAReducerRegistry.Live(options.reducers ?? []);
  const PromptProjector = options.promptProjectorLayer ?? EDAPromptProjector.Default;
  const Model =
    options.modelLayer ??
    makeLanguageModelLayer({
      generateText: options.generateText,
      onGenerateText: options.onGenerateText,
      onStreamText: options.onStreamText,
      parts: options.parts ?? Stream.empty,
    });
  const CompactionPolicyLayer = options.compactionPolicyLayer ?? CompactionPolicy.Disabled;
  const CompactionExecutorLayer = options.compactionExecutorLayer ?? CompactionExecutor.Disabled;
  const Compaction = CompactionRunner.Live.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Store,
        Factory,
        Ids,
        KeepAlive,
        Model,
        CompactionPolicyLayer,
        CompactionExecutorLayer,
      ),
    ),
  );
  const SinkRegistry = EDASinkRegistry.Live(options.sinks ?? []).pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Session,
        Store,
        Bus,
        SinkCheckpoints,
        Factory,
        Ids,
        ReducerRegistry,
        KeepAlive,
      ),
    ),
  );
  const State = SessionState.Live.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Store,
        Bus,
        Session,
        ReducerRegistry,
        SinkRegistry,
        KeepAlive,
        Compaction,
        PromptProjector,
      ),
    ),
  );
  const Registry =
    options.toolkit !== undefined
      ? EDAToolRegistry.FromToolkit(options.toolkit)
      : options.toolSchemas === undefined
        ? EDAToolRegistry.Empty
        : EDAToolRegistry.FromSchemas(options.toolSchemas);
  const InferenceRunnerLayer = InferenceRunner.Live.pipe(
    Layer.provideMerge(Layer.mergeAll(Factory, Model, Registry, Ids)),
  );
  const ToolExec = ToolExecutor.Live.pipe(
    Layer.provideMerge(Layer.mergeAll(Factory, Registry, Ids, Session)),
  );
  const Turn = TurnRunner.Live.pipe(
    Layer.provideMerge(Layer.mergeAll(InferenceRunnerLayer, ToolExec, Factory, Ids)),
  );
  const StateWithRun = State.pipe(Layer.provideMerge(Layer.mergeAll(Turn, Factory, Ids)));
  const Query = EDASessionQuery.Live.pipe(
    Layer.provideMerge(Layer.mergeAll(StateWithRun, Store, Bus)),
  );

  return Layer.mergeAll(StateWithRun, Query, InferenceRunnerLayer, ToolExec, TestClock);
};
