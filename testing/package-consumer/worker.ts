import * as Runtime from "@advait/effect-durable-agent";
import * as CommandQueues from "@advait/effect-durable-agent/domain/command-queues";
import * as MessageTranscript from "@advait/effect-durable-agent/domain/message-transcript";
import * as ReducedState from "@advait/effect-durable-agent/domain/reduced-state";
import * as ReducedStateSchema from "@advait/effect-durable-agent/domain/reduced-state-schema";
import * as DurableObjectHost from "@advait/effect-durable-agent/host/durable-object";
import * as DurableObjectRuntime from "@advait/effect-durable-agent/host/durable-object-runtime";
import * as DurableObjectStorage from "@advait/effect-durable-agent/host/durable-object-storage";
import * as WebSocketWire from "@advait/effect-durable-agent/host/websocket-wire";
import * as Compaction from "@advait/effect-durable-agent/services/compaction";
import * as IdGenerator from "@advait/effect-durable-agent/services/id-generator";
import * as PromptProjector from "@advait/effect-durable-agent/services/prompt-projector";
import * as ReducerRegistry from "@advait/effect-durable-agent/services/reducer-registry";
import * as RuntimeService from "@advait/effect-durable-agent/services/runtime";
import * as SessionQuery from "@advait/effect-durable-agent/services/session-query";
import * as SessionStore from "@advait/effect-durable-agent/services/session-store";
import * as SinkRegistry from "@advait/effect-durable-agent/services/sink-registry";
import { SpanNames } from "@advait/effect-durable-agent/services/span-names";
import * as ToolRegistry from "@advait/effect-durable-agent/services/tool-registry";
import * as Tracing from "@advait/effect-durable-agent/services/tracing";
import * as TestLayers from "@advait/effect-durable-agent/testkit/layers";
import * as Commands from "@advait/effect-durable-agent/types/commands";
import * as Core from "@advait/effect-durable-agent/types/core";
import * as Events from "@advait/effect-durable-agent/types/events";
import * as DurableEvents from "@advait/effect-durable-agent/types/events/durable";
import * as TraceTypes from "@advait/effect-durable-agent/types/tracing";

const publicModules = [
  Runtime,
  CommandQueues,
  MessageTranscript,
  ReducedState,
  ReducedStateSchema,
  DurableObjectHost,
  DurableObjectRuntime,
  DurableObjectStorage,
  WebSocketWire,
  Compaction,
  IdGenerator,
  PromptProjector,
  ReducerRegistry,
  RuntimeService,
  SessionQuery,
  SessionStore,
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
