import type { ModelResolver } from "effect-durable-agent/services/model-resolver";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import type * as Tracer from "effect/Tracer";

import type {
  CompactionExecutor,
  CompactionPolicy,
} from "effect-durable-agent/services/compaction";
import {
  EDARuntime,
  type EDARuntimeConfig,
  type EDARuntimeShape,
} from "effect-durable-agent/services/runtime";
import type { EDAPromptProjector } from "effect-durable-agent/services/prompt-projector";
import type { EDAReducer } from "effect-durable-agent/services/reducer-registry";
import type { SessionEventObserver } from "effect-durable-agent/services/session-event-observer";
import type { SessionCommandAdmissionError } from "effect-durable-agent/services/session-state";
import type { EDASink } from "effect-durable-agent/services/sink-registry";
import type {
  EDAModelToolkit,
  EDAToolRegistryShape,
} from "effect-durable-agent/services/tool-registry";
import type { SessionId } from "effect-durable-agent/types/core";
import {
  DurableObjectKeepAlive,
  type DurableObjectBackgroundWaiter,
} from "../durable-object-keepalive";
import { DurableObjectSinkCheckpointStore } from "../durable-object-sink-checkpoints";
import { DurableObjectSessionStore } from "../durable-object-store";
import {
  makeEDADurableObjectRuntimeLayer,
  type EDASessionDurableObjectStorage,
} from "./runtime-layer";

export type EDAToolkitFactory = (input: { readonly sessionId: SessionId }) => EDAModelToolkit;
export type EDAToolRegistryFactory = (input: {
  readonly sessionId: SessionId;
}) => EDAToolRegistryShape;
export type EDATracerFactory = (input: { readonly sessionId: SessionId }) => Tracer.Tracer;

/** Configuration owned by the lazy per-isolate Effect runtime. */
export interface EDASessionRuntimeOptions {
  readonly background?: DurableObjectBackgroundWaiter;
  readonly config: EDARuntimeConfig;
  readonly compactionExecutorLayer?: Layer.Layer<CompactionExecutor>;
  readonly compactionPolicyLayer?: Layer.Layer<CompactionPolicy>;
  readonly keepAlive?: DurableObjectKeepAlive;
  readonly modelResolverLayer: Layer.Layer<ModelResolver>;
  readonly promptProjectorLayer?: Layer.Layer<EDAPromptProjector>;
  readonly reducers?: ReadonlyArray<EDAReducer>;
  readonly sessionEventObserverLayer: Layer.Layer<SessionEventObserver>;
  readonly sinks?: ReadonlyArray<EDASink>;
  readonly storage: EDASessionDurableObjectStorage;
  readonly toolkit?: EDAModelToolkit | EDAToolkitFactory;
  readonly toolRegistry?: EDAToolRegistryShape | EDAToolRegistryFactory;
  readonly tracer?: Tracer.Tracer | EDATracerFactory;
}

interface RuntimeState {
  readonly runtime: ManagedRuntime.ManagedRuntime<EDARuntime, SessionCommandAdmissionError>;
  readonly sessionId: SessionId;
}

/** Owns the disposable ManagedRuntime and nothing about WebSocket transport. */
export class EDASessionRuntime {
  readonly keepAlive: DurableObjectKeepAlive;
  private state: RuntimeState | undefined;
  private building: Promise<RuntimeState> | undefined;

  constructor(private readonly options: EDASessionRuntimeOptions) {
    this.keepAlive =
      options.keepAlive ?? new DurableObjectKeepAlive(options.storage, options.background);
  }

  static readonly migrate = (
    storage: EDASessionDurableObjectStorage,
  ): Effect.Effect<
    void,
    import("effect-durable-agent/services/session-store").EDASessionStoreError
  > =>
    Effect.gen(function* () {
      yield* DurableObjectSessionStore.migrate(storage);
      yield* DurableObjectSinkCheckpointStore.migrate(storage);
    });

  isReady(sessionId: SessionId): boolean {
    return this.state?.sessionId === sessionId;
  }

  async prepare(sessionId: SessionId): Promise<void> {
    await this.get(sessionId);
  }

  async run<A>(
    sessionId: SessionId,
    operation: (runtime: EDARuntimeShape) => Effect.Effect<A, unknown>,
  ): Promise<A> {
    const state = await this.get(sessionId);
    return await state.runtime.runPromise(
      Effect.gen(function* () {
        return yield* operation(yield* EDARuntime);
      }),
    );
  }

  async alarm(sessionId?: SessionId): Promise<void> {
    const resolved = sessionId ?? this.state?.sessionId;
    if (resolved !== undefined) await this.get(resolved);
    await this.keepAlive.alarm();
  }

  async destroy(): Promise<void> {
    await this.keepAlive.shutdown();
    await this.dispose();
    this.state = undefined;
    this.building = undefined;
  }

  async dispose(): Promise<void> {
    await this.state?.runtime.dispose();
  }

  private async get(sessionId: SessionId): Promise<RuntimeState> {
    if (this.state !== undefined) {
      if (this.state.sessionId !== sessionId) {
        throw new Error(
          `EDA session runtime is scoped to session ${this.state.sessionId}; received ${sessionId}`,
        );
      }
      return this.state;
    }

    this.keepAlive.restart();
    const building = this.building ?? this.build(sessionId);
    this.building = building;
    try {
      return await building;
    } catch (error) {
      if (this.building === building) this.building = undefined;
      throw error;
    }
  }

  private async build(sessionId: SessionId): Promise<RuntimeState> {
    const runtime = ManagedRuntime.make(
      makeEDADurableObjectRuntimeLayer({
        config: this.options.config,
        compactionExecutorLayer: this.options.compactionExecutorLayer,
        compactionPolicyLayer: this.options.compactionPolicyLayer,
        keepAlive: this.keepAlive,
        modelResolverLayer: this.options.modelResolverLayer,
        promptProjectorLayer: this.options.promptProjectorLayer,
        reducers: this.options.reducers,
        sessionEventObserverLayer: this.options.sessionEventObserverLayer,
        sessionId,
        sinks: this.options.sinks,
        storage: this.options.storage,
        toolkit: resolveFactory(this.options.toolkit, sessionId),
        toolRegistry: resolveFactory(this.options.toolRegistry, sessionId),
        tracer: resolveFactory(this.options.tracer, sessionId),
      }),
    );
    const state = { runtime, sessionId } satisfies RuntimeState;
    try {
      await runtime.context();
      this.state = state;
      return state;
    } catch (error) {
      await runtime.dispose().catch(() => undefined);
      throw error;
    }
  }
}

const resolveFactory = <A>(
  value: A | ((input: { readonly sessionId: SessionId }) => A) | undefined,
  sessionId: SessionId,
): A | undefined =>
  typeof value === "function"
    ? (value as (input: { readonly sessionId: SessionId }) => A)({ sessionId })
    : value;
