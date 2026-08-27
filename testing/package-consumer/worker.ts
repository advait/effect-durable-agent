import * as Runtime from "effect-durable-agent";
import * as CommandQueues from "effect-durable-agent/domain/command-queues";
import * as ContextProjection from "effect-durable-agent/domain/context-projection";
import * as MessageTranscript from "effect-durable-agent/domain/message-transcript";
import * as ReducedState from "effect-durable-agent/domain/reduced-state";
import * as ReducedStateSchema from "effect-durable-agent/domain/reduced-state-schema";
import * as WebSocket from "effect-durable-agent/websocket";
import * as Compaction from "effect-durable-agent/services/compaction";
import * as IdGenerator from "effect-durable-agent/services/id-generator";
import * as KeepAlive from "effect-durable-agent/services/keep-alive";
import * as PromptProjector from "effect-durable-agent/services/prompt-projector";
import * as ReducerRegistry from "effect-durable-agent/services/reducer-registry";
import * as RuntimeService from "effect-durable-agent/services/runtime";
import * as RuntimeLayer from "effect-durable-agent/services/runtime-layer";
import * as SessionEventObserver from "effect-durable-agent/services/session-event-observer";
import * as SessionQuery from "effect-durable-agent/services/session-query";
import * as SessionStore from "effect-durable-agent/services/session-store";
import * as SessionState from "effect-durable-agent/services/session-state";
import * as SinkCheckpointStore from "effect-durable-agent/services/sink-checkpoint-store";
import * as SinkRegistry from "effect-durable-agent/services/sink-registry";
import { SpanNames } from "effect-durable-agent/services/span-names";
import * as ToolRegistry from "effect-durable-agent/services/tool-registry";
import * as Tracing from "effect-durable-agent/services/tracing";
import * as TestLayers from "effect-durable-agent/testkit/layers";
import * as Commands from "effect-durable-agent/types/commands";
import * as Core from "effect-durable-agent/types/core";
import * as Events from "effect-durable-agent/types/events";
import * as DurableEvents from "effect-durable-agent/types/events/durable";
import * as TraceTypes from "effect-durable-agent/types/tracing";
import * as DurableObjectHost from "effect-durable-agent-cloudflare/durable-object";
import * as OpenAiProvider from "effect-durable-agent-cloudflare/openai";
import * as Rpc from "effect-durable-agent-cloudflare/rpc";
import * as SessionController from "effect-durable-agent-cloudflare/session-controller";
import * as DurableObjectStorage from "effect-durable-agent-cloudflare/storage";
import * as CelldHost from "effect-durable-agent-celld";

const publicModules = [
  Runtime,
  CommandQueues,
  ContextProjection,
  MessageTranscript,
  ReducedState,
  ReducedStateSchema,
  DurableObjectHost,
  OpenAiProvider,
  Rpc,
  SessionController,
  DurableObjectStorage,
  CelldHost,
  WebSocket,
  Compaction,
  IdGenerator,
  KeepAlive,
  PromptProjector,
  ReducerRegistry,
  RuntimeService,
  RuntimeLayer,
  SessionEventObserver,
  SessionQuery,
  SessionStore,
  SessionState,
  SinkCheckpointStore,
  SinkRegistry,
  ToolRegistry,
  Tracing,
  TestLayers,
  Commands,
  Core,
  Events,
  DurableEvents,
  TraceTypes,
];

export default {
  fetch() {
    return new Response(`${SpanNames[0]}:${publicModules.length}`);
  },
} satisfies ExportedHandler;
