import { ModelResolver } from "./model-resolver";
import * as Layer from "effect/Layer";
import * as Tracer from "effect/Tracer";

import type { SessionId } from "../types/core";
import { CompactionExecutor, CompactionPolicy, CompactionRunner } from "./compaction";
import { EventFactory } from "./event-factory";
import { IdGenerator } from "./id-generator";
import { InferenceRunner } from "./inference-runner";
import { EDAKeepAlive } from "./keep-alive";
import { LiveEventBus } from "./live-event-bus";
import { EDAPromptProjector } from "./prompt-projector";
import { EDAReducerRegistry } from "./reducer-registry";
import { EDARuntime, type EDARuntimeConfig } from "./runtime";
import { SessionContext } from "./session-context";
import { SessionEventObserver } from "./session-event-observer";
import { EDASessionQuery } from "./session-query";
import { EDASessionStore, EDASessionStoreError } from "./session-store";
import { SessionState, type SessionCommandAdmissionError } from "./session-state";
import { SinkCheckpointStore } from "./sink-checkpoint-store";
import { EDASinkRegistry, type EDASink } from "./sink-registry";
import { ToolExecutor } from "./tool-executor";
import { EDAToolRegistry, type EDAModelToolkit, type EDAToolRegistryShape } from "./tool-registry";
import { TurnRunner } from "./turn-runner";

/**
 * Host-owned adapters and application policy needed to construct one EDA runtime.
 *
 * Core owns the service graph, while hosts supply only semantic persistence,
 * checkpoint, and lifecycle layers. Platform SDK types never cross this boundary.
 */
export interface EDARuntimeLayerOptions {
  readonly config: EDARuntimeConfig;
  readonly compactionExecutorLayer?: Layer.Layer<CompactionExecutor>;
  readonly compactionPolicyLayer?: Layer.Layer<CompactionPolicy>;
  readonly keepAliveLayer?: Layer.Layer<EDAKeepAlive>;
  readonly modelResolverLayer: Layer.Layer<ModelResolver>;
  readonly promptProjectorLayer?: Layer.Layer<EDAPromptProjector>;
  readonly reducerRegistryLayer?: Layer.Layer<EDAReducerRegistry>;
  readonly sessionId: SessionId;
  readonly sessionEventObserverLayer?: Layer.Layer<SessionEventObserver>;
  readonly sessionStoreLayer: Layer.Layer<EDASessionStore, EDASessionStoreError>;
  readonly sinkCheckpointStoreLayer: Layer.Layer<SinkCheckpointStore, EDASessionStoreError>;
  readonly sinks?: ReadonlyArray<EDASink>;
  readonly toolkit?: EDAModelToolkit;
  readonly toolRegistry?: EDAToolRegistryShape;
  readonly tracer?: Tracer.Tracer;
}

/** Compose the complete platform-neutral EDA service graph for one session. */
export const makeEDARuntimeLayer = ({
  config,
  compactionExecutorLayer,
  compactionPolicyLayer,
  keepAliveLayer,
  modelResolverLayer,
  promptProjectorLayer,
  reducerRegistryLayer,
  sessionId,
  sessionEventObserverLayer,
  sessionStoreLayer,
  sinkCheckpointStoreLayer,
  sinks,
  toolkit,
  toolRegistry,
  tracer,
}: EDARuntimeLayerOptions): Layer.Layer<EDARuntime, SessionCommandAdmissionError> => {
  const Store = sessionStoreLayer;
  const Models = modelResolverLayer;
  const EventObserver = sessionEventObserverLayer ?? SessionEventObserver.Noop;
  const Bus = LiveEventBus.Live.pipe(Layer.provide(EventObserver));
  const KeepAlive = keepAliveLayer ?? EDAKeepAlive.Noop;
  const Ids = IdGenerator.Live;
  const Session = SessionContext.Live(sessionId);
  const Factory = EventFactory.Live.pipe(Layer.provideMerge(Layer.mergeAll(Session, Ids)));
  const PromptProjector = promptProjectorLayer ?? EDAPromptProjector.Default;
  const ReducerRegistry = reducerRegistryLayer ?? EDAReducerRegistry.Empty;
  const CompactionPolicyLayer = compactionPolicyLayer ?? CompactionPolicy.Disabled;
  const CompactionExecutorLayer = compactionExecutorLayer ?? CompactionExecutor.Disabled;
  const Compaction = CompactionRunner.Live.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Store,
        Factory,
        Ids,
        KeepAlive,
        Models,
        CompactionPolicyLayer,
        CompactionExecutorLayer,
      ),
    ),
  );
  const SinkRegistry = EDASinkRegistry.Live(sinks ?? []).pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Session,
        Store,
        Bus,
        sinkCheckpointStoreLayer,
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
    toolRegistry !== undefined
      ? EDAToolRegistry.FromShape(toolRegistry)
      : toolkit === undefined
        ? EDAToolRegistry.Empty
        : EDAToolRegistry.FromToolkit(toolkit);
  const InferenceRunnerLayer = InferenceRunner.Live.pipe(
    Layer.provideMerge(Layer.mergeAll(Factory, Models, Registry, Ids)),
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
  const Runtime = EDARuntime.Live(config).pipe(
    Layer.provideMerge(Layer.mergeAll(StateWithRun, Query)),
  );

  return tracer === undefined
    ? Runtime
    : Runtime.pipe(Layer.provideMerge(Layer.succeed(Tracer.Tracer, tracer)));
};
