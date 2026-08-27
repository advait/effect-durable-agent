import type * as LanguageModel from "effect/unstable/ai/LanguageModel";
import * as Layer from "effect/Layer";
import type * as Tracer from "effect/Tracer";

import { CompactionExecutor, CompactionPolicy } from "effect-durable-agent/services/compaction";
import { EDAKeepAlive } from "effect-durable-agent/services/keep-alive";
import { EDAPromptProjector } from "effect-durable-agent/services/prompt-projector";
import {
  EDAReducerRegistry,
  type EDAReducer,
} from "effect-durable-agent/services/reducer-registry";
import { EDARuntime, type EDARuntimeConfig } from "effect-durable-agent/services/runtime";
import { makeEDARuntimeLayer } from "effect-durable-agent/services/runtime-layer";
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
  type DurableObjectAlarmStorage,
} from "../durable-object-keepalive";
import { DurableObjectSinkCheckpointStore } from "../durable-object-sink-checkpoints";
import type { DurableObjectSessionStorage } from "../durable-object-storage";
import { DurableObjectSessionStore } from "../durable-object-store";

/** Storage capabilities used by one EDA session Durable Object. */
export type EDASessionDurableObjectStorage = DurableObjectSessionStorage &
  DurableObjectAlarmStorage;

/** Host adapters and application policy needed to build one session runtime. */
export interface EDADurableObjectRuntimeLayerOptions {
  readonly config: EDARuntimeConfig;
  readonly compactionExecutorLayer?: Layer.Layer<
    CompactionExecutor,
    never,
    LanguageModel.LanguageModel
  >;
  readonly compactionPolicyLayer?: Layer.Layer<CompactionPolicy>;
  readonly keepAlive?: DurableObjectKeepAlive;
  readonly modelLayer: Layer.Layer<LanguageModel.LanguageModel>;
  readonly promptProjectorLayer?: Layer.Layer<EDAPromptProjector>;
  readonly reducers?: ReadonlyArray<EDAReducer>;
  readonly sessionEventObserverLayer?: Layer.Layer<SessionEventObserver>;
  readonly sessionId: SessionId;
  readonly sinks?: ReadonlyArray<EDASink>;
  readonly storage: EDASessionDurableObjectStorage;
  readonly toolkit?: EDAModelToolkit;
  readonly toolRegistry?: EDAToolRegistryShape;
  readonly tracer?: Tracer.Tracer;
}

/** Compose the platform-neutral runtime with Cloudflare persistence and lifecycle adapters. */
export const makeEDADurableObjectRuntimeLayer = ({
  config,
  compactionExecutorLayer,
  compactionPolicyLayer,
  keepAlive,
  modelLayer,
  promptProjectorLayer,
  reducers,
  sessionEventObserverLayer,
  sessionId,
  sinks,
  storage,
  toolkit,
  toolRegistry,
  tracer,
}: EDADurableObjectRuntimeLayerOptions): Layer.Layer<EDARuntime, SessionCommandAdmissionError> => {
  const keepAliveLayer =
    keepAlive === undefined
      ? EDAKeepAlive.Noop
      : EDAKeepAlive.FromAcquire(() => keepAlive.acquire());
  return makeEDARuntimeLayer({
    config,
    compactionExecutorLayer,
    compactionPolicyLayer,
    keepAliveLayer,
    modelLayer,
    promptProjectorLayer,
    reducerRegistryLayer: EDAReducerRegistry.Live(reducers ?? []),
    sessionEventObserverLayer,
    sessionId,
    sessionStoreLayer: DurableObjectSessionStore.layer({ sessionId, storage }),
    sinkCheckpointStoreLayer: DurableObjectSinkCheckpointStore.layer(storage),
    sinks,
    toolkit,
    toolRegistry,
    tracer,
  });
};
