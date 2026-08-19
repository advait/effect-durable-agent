import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Tracer from "effect/Tracer";
import type * as LanguageModel from "effect/unstable/ai/LanguageModel";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as HttpBody from "effect/unstable/http/HttpBody";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientError from "effect/unstable/http/HttpClientError";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";

import type { DurableTranscriptMessage } from "effect-durable-agent/domain/message-transcript";
import { EDACommand } from "effect-durable-agent/types/commands";
import { CommandId, SequenceNumber, SessionId } from "effect-durable-agent/types/core";
import { DurableEventEnvelope } from "effect-durable-agent/types/events";
import {
  makeRootEDATraceMetadata,
  type EDATraceMetadata,
} from "effect-durable-agent/types/tracing";
import { CompactionExecutor, CompactionPolicy } from "effect-durable-agent/services/compaction";
import { EDAKeepAlive } from "effect-durable-agent/services/keep-alive";
import { EDAPromptProjector } from "effect-durable-agent/services/prompt-projector";
import {
  EDAReducerRegistry,
  type EDAReducer,
} from "effect-durable-agent/services/reducer-registry";
import { EDARuntime, type EDARuntimeConfig } from "effect-durable-agent/services/runtime";
import { makeEDARuntimeLayer } from "effect-durable-agent/services/runtime-layer";
import type { EDASessionSnapshot } from "effect-durable-agent/services/session-query";
import {
  CommittedDurableEvent,
  EDASessionStoreError,
} from "effect-durable-agent/services/session-store";
import type {
  EDASubmittable,
  SessionCommandAdmissionError,
} from "effect-durable-agent/services/session-state";
import type { EDASink } from "effect-durable-agent/services/sink-registry";
import { compactSpanAttributes, toEdaExternalSpan } from "effect-durable-agent/services/tracing";
import type {
  EDAModelToolkit,
  EDAToolRegistryShape,
} from "effect-durable-agent/services/tool-registry";
import {
  makeWebSocketDeliveryState,
  deliveryHelloFrame,
  onClientAck,
  onDurableEvents,
  onEphemeralEvent,
  type EDAWebSocketDeliveryResult,
  type EDAWebSocketDeliveryState,
} from "effect-durable-agent/services/websocket-delivery";
import { PositionedEvent } from "effect-durable-agent/types/events";
import type {
  CommittedCommandTerminalEvent,
  EDARuntimeShape,
} from "effect-durable-agent/services/runtime";
import {
  DurableObjectKeepAlive,
  type DurableObjectAlarmStorage,
  type DurableObjectBackgroundWaiter,
} from "./durable-object-keepalive";
import { DurableObjectSessionStore } from "./durable-object-store";
import { DurableObjectSinkCheckpointStore } from "./durable-object-sink-checkpoints";
import type { DurableObjectSessionStorage } from "./durable-object-storage";
import {
  EDA_WEB_SOCKET_PONG_MESSAGE,
  EDA_WS_CLOSE_PROTOCOL_ERROR,
  EDA_WS_CLOSE_SEND_FAILED,
  EDAWebSocketAttachment,
  SubscriberId,
  decodeEDAWebSocketAttachment,
  decodeEDAWebSocketClientMessage,
  defaultEDAWebSocketFlowControl,
  encodeEDAWebSocketServerFrame,
  type EDAWebSocketAckFrame,
  type EDAWebSocketClientFrame,
  type EDAWebSocketServerFrame,
} from "./websocket-protocol";
import type { EDAWebSocketServerFrameEncoder } from "effect-durable-agent/host/websocket-wire";

/** Durable Object storage capabilities needed by the EDA session host. */
export type EDASessionDurableObjectStorage = DurableObjectSessionStorage &
  DurableObjectAlarmStorage;

/** Options for composing one EDA runtime inside a raw Durable Object. */
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
  readonly reducers?: ReadonlyArray<EDAReducer<any>>;
  readonly sessionId: SessionId;
  readonly sinks?: ReadonlyArray<EDASink>;
  readonly storage: EDASessionDurableObjectStorage;
  readonly toolkit?: EDAModelToolkit;
  readonly toolRegistry?: EDAToolRegistryShape;
  readonly tracer?: Tracer.Tracer;
}

/** Options for one live raw Durable Object host instance. */
export interface EDASessionDurableObjectHostOptions {
  readonly background?: DurableObjectBackgroundWaiter;
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
  readonly reducers?: ReadonlyArray<EDAReducer<any>>;
  readonly sinks?: ReadonlyArray<EDASink>;
  readonly storage: EDASessionDurableObjectStorage;
  readonly toolkit?: EDAModelToolkit | EDAToolkitFactory;
  readonly toolRegistry?: EDAToolRegistryShape | EDAToolRegistryFactory;
  readonly tracer?: Tracer.Tracer | EDATracerFactory;
  readonly webSocketProtocol?: EDAWebSocketServerFrameEncoder;
  /** Enumerate accepted WebSockets for live event fanout; hosts pass `ctx.getWebSockets`. */
  readonly getWebSockets?: () => ReadonlyArray<WebSocket>;
}

/** Per-session factory for provider-visible model toolkit definitions. */
export type EDAToolkitFactory = (input: { readonly sessionId: SessionId }) => EDAModelToolkit;

/** Per-session factory for custom tool registry implementations. */
export type EDAToolRegistryFactory = (input: {
  readonly sessionId: SessionId;
}) => EDAToolRegistryShape;

export type EDATracerFactory = (input: { readonly sessionId: SessionId }) => Tracer.Tracer;

/** Typed host input for admitting one command through RPC or route code. */
export interface EDASessionSubmitInput {
  readonly command: EDACommand;
  readonly sessionId: SessionId;
  readonly trace: EDATraceMetadata;
}

/** Typed host input for admitting commands and app events as one durable batch. */
export interface EDASessionSubmitBatchInput {
  readonly items: ReadonlyArray<EDASubmittable>;
  readonly sessionId: SessionId;
  readonly trace: EDATraceMetadata;
}

/** Typed host input for waiting on a command terminal boundary. */
export interface EDASessionBlockOnCommandInput {
  readonly afterSeq?: SequenceNumber;
  readonly commandId: CommandId;
  readonly sessionId: SessionId;
  readonly trace: EDATraceMetadata;
}

/** Typed host input for operations scoped only by session identity. */
export interface EDASessionScopedInput {
  readonly sessionId: SessionId;
  readonly trace: EDATraceMetadata;
}

/** Typed host input for opening a live event stream from a durable cursor. */
export interface EDASessionEventsInput {
  readonly afterSeq?: SequenceNumber;
  readonly sessionId: SessionId;
  readonly trace: EDATraceMetadata;
}

interface HostRuntimeState {
  readonly runtime: ManagedRuntime.ManagedRuntime<EDARuntime, SessionCommandAdmissionError>;
  readonly sessionId: SessionId;
}

interface EventWebSocketState {
  readonly sessionId: SessionId;
  readonly subscriberId: SubscriberId;
  readonly trace: EDATraceMetadata;
  delivery: EDAWebSocketDeliveryState;
  /** Best-known committed durable head, raised by slices and published events. */
  head: SequenceNumber;
  /** Serializes async delivery operations for this socket. */
  queue: Promise<void>;
}

const resolveRuntimeFactory = <A>(
  value: A | ((input: { readonly sessionId: SessionId }) => A) | undefined,
  sessionId: SessionId,
): A | undefined =>
  typeof value === "function"
    ? (value as (input: { readonly sessionId: SessionId }) => A)({ sessionId })
    : value;

/**
 * Compose the full EDA service graph for one raw Durable Object session.
 *
 * The layer is intentionally host-owned: tests can pass a fake model layer,
 * while the Worker host can pass a provider-backed layer from Env.
 */
export const makeEDADurableObjectRuntimeLayer = ({
  config,
  compactionExecutorLayer,
  compactionPolicyLayer,
  keepAlive,
  modelLayer,
  promptProjectorLayer,
  reducers,
  sessionId,
  sinks,
  storage,
  toolkit,
  toolRegistry,
  tracer,
}: EDADurableObjectRuntimeLayerOptions): Layer.Layer<EDARuntime, SessionCommandAdmissionError> => {
  const Store = DurableObjectSessionStore.layer({ sessionId, storage });
  const KeepAlive =
    keepAlive === undefined
      ? EDAKeepAlive.Noop
      : EDAKeepAlive.FromAcquire(() => keepAlive.acquire());
  const SinkCheckpoints = DurableObjectSinkCheckpointStore.layer(storage);
  return makeEDARuntimeLayer({
    config,
    compactionExecutorLayer,
    compactionPolicyLayer,
    keepAliveLayer: KeepAlive,
    modelLayer,
    promptProjectorLayer,
    reducerRegistryLayer: EDAReducerRegistry.Live(reducers ?? []),
    sessionId,
    sessionStoreLayer: Store,
    sinkCheckpointStoreLayer: SinkCheckpoints,
    sinks,
    toolkit,
    toolRegistry,
    tracer,
  });
};

/** Provider request defaults accepted by EDA's OpenAI Responses model adapter. */
export type EDAOpenAiModelConfig = Omit<typeof OpenAiLanguageModel.Config.Service, "model">;

/** OpenAI-backed Effect AI model layer for the default Worker host. */
export const makeEDADurableObjectOpenAiModelLayer = (options: {
  readonly aiGateway?: AiGateway;
  readonly apiKey?: string;
  readonly apiUrl?: string;
  readonly config?: EDAOpenAiModelConfig;
  readonly modelId: string;
}): Layer.Layer<LanguageModel.LanguageModel> => {
  const apiKey = optionalNonEmptyString(options.apiKey);
  if (apiKey === undefined && options.aiGateway === undefined) {
    throw new Error(
      "OPENAI_API_KEY is required unless the EDA OpenAI model layer is backed by a Cloudflare AI Gateway binding.",
    );
  }

  const baseHttpClient =
    options.aiGateway === undefined
      ? FetchHttpClient.layer
      : Layer.succeed(HttpClient.HttpClient, makeCloudflareAiGatewayHttpClient(options.aiGateway));
  const client = OpenAiClient.layer({
    ...(apiKey === undefined ? {} : { apiKey: Redacted.make(apiKey) }),
    ...(options.apiUrl === undefined ? {} : { apiUrl: options.apiUrl }),
  }).pipe(Layer.provide(baseHttpClient));

  return OpenAiLanguageModel.layer({
    model: options.modelId,
    ...(options.config === undefined ? {} : { config: options.config }),
  }).pipe(Layer.provide(client));
};

const OPENAI_API_BASE_PATH_PREFIX = "/v1/";

const makeCloudflareAiGatewayHttpClient = (gateway: AiGateway): HttpClient.HttpClient =>
  HttpClient.make((request, url, signal) =>
    Effect.tryPromise({
      try: async () => {
        const response = await gateway.run(
          {
            endpoint: openAiGatewayEndpointFromUrl(url),
            headers: requestHeadersForGateway(request),
            provider: "openai",
            query: requestBodyForGateway(request.body),
          },
          { signal },
        );
        return HttpClientResponse.fromWeb(request, response);
      },
      catch: (cause) =>
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({
            request,
            cause,
            description: "Cloudflare AI Gateway request failed",
          }),
        }),
    }),
  );

const openAiGatewayEndpointFromUrl = (url: URL): string => {
  const endpointPath = url.pathname.startsWith(OPENAI_API_BASE_PATH_PREFIX)
    ? url.pathname.slice(OPENAI_API_BASE_PATH_PREFIX.length)
    : url.pathname.replace(/^\/+/, "");
  return `${endpointPath}${url.search}`;
};

const requestHeadersForGateway = (
  request: HttpClientRequest.HttpClientRequest,
): Record<string, string> =>
  Object.fromEntries(
    Object.entries(request.headers).filter((entry): entry is [string, string] => {
      const [key, value] = entry;
      return typeof value === "string" && key.toLowerCase() !== "host";
    }),
  );

const requestBodyForGateway = (body: HttpBody.HttpBody): unknown => {
  switch (body._tag) {
    case "Empty":
      return undefined;
    case "Raw":
      return body.body;
    case "Uint8Array":
      return JSON.parse(new TextDecoder().decode(body.body)) as unknown;
    default:
      throw new Error(`Unsupported OpenAI request body for Cloudflare AI Gateway: ${body._tag}`);
  }
};

const optionalNonEmptyString = (value: string | undefined): string | undefined => {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
};

const hostSpanOptions = (
  trace: EDATraceMetadata,
  attributes: Record<string, string | number | boolean | undefined>,
): Tracer.SpanOptionsNoTrace => ({
  kind: "server",
  attributes: compactSpanAttributes({ ...trace.attributes, ...attributes }),
  links: trace.links.map((link) => ({
    span: toEdaExternalSpan(link.context),
    attributes: link.attributes,
  })),
  ...(trace.parent === null ? {} : { parent: toEdaExternalSpan(trace.parent) }),
});

/** Raw Durable Object host controller for one EDA session object. */
export class EDASessionDurableObjectHost {
  private readonly keepAlive: DurableObjectKeepAlive;
  private readonly eventSockets = new Map<WebSocket, EventWebSocketState>();
  private runtimeState: HostRuntimeState | undefined;
  private runtimePromise: Promise<HostRuntimeState> | undefined;

  constructor(private readonly options: EDASessionDurableObjectHostOptions) {
    this.keepAlive =
      options.keepAlive ?? new DurableObjectKeepAlive(options.storage, options.background);
  }

  /** Run idempotent host and event-store migrations. Safe from `blockConcurrencyWhile`. */
  static readonly migrate = (
    storage: EDASessionDurableObjectStorage,
  ): Effect.Effect<void, EDASessionStoreError> =>
    Effect.gen(function* () {
      yield* DurableObjectSessionStore.migrate(storage);
      yield* DurableObjectSinkCheckpointStore.migrate(storage);
    });

  /** Admit one command durably. Active runtime work holds EDA keep-alive leases internally. */
  async submit(input: EDASessionSubmitInput): Promise<CommittedDurableEvent> {
    const runtime = await this.getRuntime(input.sessionId);
    const trace = input.trace ?? makeRootEDATraceMetadata();
    return await runtime.runtime.runPromise(
      Effect.gen(function* () {
        const eda = yield* EDARuntime;
        return yield* eda.submit(input.command);
      }).pipe(
        Effect.withSpan(
          "agent.command.submit",
          hostSpanOptions(trace, {
            "eda.session.id": input.sessionId,
            "eda.command.kind": input.command._tag,
            "eda.command.id": input.command.commandId,
          }),
        ),
      ),
    );
  }

  /** Submit an ordered durable batch of commands and app events. */
  async submitBatch(
    input: EDASessionSubmitBatchInput,
  ): Promise<ReadonlyArray<CommittedDurableEvent>> {
    const runtime = await this.getRuntime(input.sessionId);
    const trace = input.trace ?? makeRootEDATraceMetadata();
    return await runtime.runtime.runPromise(
      Effect.gen(function* () {
        const eda = yield* EDARuntime;
        return yield* eda.submit(input.items);
      }).pipe(
        Effect.withSpan(
          "agent.command.submit",
          hostSpanOptions(trace, {
            "eda.session.id": input.sessionId,
            "eda.command.submittable_count": input.items.length,
            "eda.command.count": input.items.filter((item) => "_tag" in item).length,
          }),
        ),
      ),
    );
  }

  /** Admit one command and wait for its terminal command boundary. */
  async submitAndBlock(input: EDASessionSubmitInput): Promise<CommittedCommandTerminalEvent> {
    const runtime = await this.getRuntime(input.sessionId);
    const trace = input.trace ?? makeRootEDATraceMetadata();
    return await runtime.runtime.runPromise(
      Effect.gen(function* () {
        const eda = yield* EDARuntime;
        return yield* eda.submitAndBlock(input.command);
      }).pipe(
        Effect.withSpan(
          "agent.command.submit.wait",
          hostSpanOptions(trace, {
            "eda.session.id": input.sessionId,
            "eda.command.kind": input.command._tag,
            "eda.command.id": input.command.commandId,
          }),
        ),
      ),
    );
  }

  /** Wait for an existing command to reach its terminal command boundary. */
  async blockOnCommand(
    input: EDASessionBlockOnCommandInput,
  ): Promise<CommittedCommandTerminalEvent> {
    const runtime = await this.getRuntime(input.sessionId);
    const trace = input.trace ?? makeRootEDATraceMetadata();
    return await runtime.runtime.runPromise(
      Effect.gen(function* () {
        const eda = yield* EDARuntime;
        return yield* eda.blockOnCommand(input.commandId, input.afterSeq ?? SequenceNumber.make(0));
      }).pipe(
        Effect.withSpan(
          "agent.command.wait",
          hostSpanOptions(trace, {
            "eda.session.id": input.sessionId,
            "eda.command.id": input.commandId,
            "eda.seq.after": input.afterSeq,
          }),
        ),
      ),
    );
  }

  /** Read the authoritative live session snapshot. */
  snapshot(input: EDASessionScopedInput): Promise<EDASessionSnapshot> {
    const trace = input.trace ?? makeRootEDATraceMetadata();
    return this.withRuntime(input.sessionId, (eda) =>
      eda
        .snapshot()
        .pipe(
          Effect.withSpan(
            "agent.session.snapshot",
            hostSpanOptions(trace, { "eda.session.id": input.sessionId }),
          ),
        ),
    );
  }

  /** Read durable transcript messages in committed order. */
  messages(input: EDASessionScopedInput): Promise<ReadonlyArray<DurableTranscriptMessage>> {
    const trace = input.trace ?? makeRootEDATraceMetadata();
    return this.withRuntime(input.sessionId, (eda) =>
      eda
        .messages()
        .pipe(
          Effect.withSpan(
            "agent.messages.list",
            hostSpanOptions(trace, { "eda.session.id": input.sessionId }),
          ),
        ),
    );
  }

  /** Accept one WebSocket as the live event stream for this session. */
  async acceptEventWebSocket(
    input: EDASessionEventsInput & { readonly webSocket: WebSocket },
  ): Promise<void> {
    const trace = input.trace ?? makeRootEDATraceMetadata();
    const attachment = EDAWebSocketAttachment.make({
      kind: "eda-events-v1",
      sessionId: input.sessionId,
      subscriberId: SubscriberId.make(crypto.randomUUID()),
      lastAckedSeq: input.afterSeq ?? SequenceNumber.make(0),
      trace,
    });
    input.webSocket.serializeAttachment(attachment);
    const state: EventWebSocketState = {
      sessionId: attachment.sessionId,
      subscriberId: attachment.subscriberId,
      trace,
      delivery: makeWebSocketDeliveryState({
        subscriberId: attachment.subscriberId,
        resumeSeq: attachment.lastAckedSeq,
        policy: defaultEDAWebSocketFlowControl,
      }),
      head: attachment.lastAckedSeq,
      queue: Promise.resolve(),
    };
    this.eventSockets.set(input.webSocket, state);
    await this.enqueueSocketOp(input.webSocket, state, async () => {
      if (!this.trySendFrame(input.webSocket, deliveryHelloFrame(state.delivery))) {
        this.eventSockets.delete(input.webSocket);
        return;
      }
      await this.runCatchUp(input.webSocket, state);
    });
  }

  /** Apply one inbound client frame: pong liveness pings and advance ACK delivery. */
  async webSocketMessage(webSocket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    if (typeof message !== "string") {
      closeWebSocket(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "binary");
      this.eventSockets.delete(webSocket);
      return;
    }

    let frame: EDAWebSocketClientFrame;
    try {
      frame = await Effect.runPromise(decodeEDAWebSocketClientMessage(message));
    } catch {
      closeWebSocket(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol");
      this.eventSockets.delete(webSocket);
      return;
    }

    if (frame._tag === "ping") {
      // Hibernation-capable hosts answer pings with WebSocket auto-response and
      // never reach this path; this covers hosts without auto-response support.
      try {
        webSocket.send(EDA_WEB_SOCKET_PONG_MESSAGE);
      } catch {
        closeWebSocket(webSocket, EDA_WS_CLOSE_SEND_FAILED, "send-failed");
        this.eventSockets.delete(webSocket);
      }
      return;
    }

    const state = await this.ensureEventWebSocket(webSocket);
    if (state === undefined) {
      closeWebSocket(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol");
      return;
    }
    await this.enqueueSocketOp(webSocket, state, async () => {
      const result = onClientAck(state.delivery, frame as EDAWebSocketAckFrame, Date.now());
      const open = this.applyDeliveryResult(webSocket, state, result);
      if (open && result.wantsCatchUpAfterSeq !== undefined) {
        await this.runCatchUp(webSocket, state);
      }
    });
  }

  /** Drop delivery state after the client closes the WebSocket. */
  async webSocketClose(webSocket: WebSocket): Promise<void> {
    this.eventSockets.delete(webSocket);
  }

  /** Drop delivery state after a host WebSocket error. */
  async webSocketError(webSocket: WebSocket): Promise<void> {
    this.eventSockets.delete(webSocket);
  }

  /** Dispose any live runtime before the host deletes its Durable Object storage. */
  async destroy(_input: EDASessionScopedInput): Promise<void> {
    await this.keepAlive.shutdown();
    await this.dispose();
    this.runtimeState = undefined;
    this.runtimePromise = undefined;
  }

  /** Durable Object alarm hook: wake/recover the runtime and re-arm while EDA has active work. */
  async alarm(input?: EDASessionScopedInput): Promise<void> {
    const sessionId = input?.sessionId ?? this.runtimeState?.sessionId;
    if (sessionId !== undefined) {
      await this.getRuntime(sessionId);
    }
    await this.keepAlive.alarm();
  }

  /** Dispose the Effect runtime if the host wrapper is torn down by tests. */
  async dispose(): Promise<void> {
    await this.runtimeState?.runtime.dispose();
  }

  private async withRuntime<A>(
    sessionId: SessionId,
    f: (runtime: EDARuntimeShape) => Effect.Effect<A, unknown>,
  ): Promise<A> {
    const runtime = await this.getRuntime(sessionId);
    return await runtime.runtime.runPromise(
      Effect.gen(function* () {
        const eda = yield* EDARuntime;
        return yield* f(eda);
      }),
    );
  }

  /** Rebuild disposable delivery state from the persisted attachment after eviction. */
  private async ensureEventWebSocket(
    webSocket: WebSocket,
  ): Promise<EventWebSocketState | undefined> {
    const existing = this.eventSockets.get(webSocket);
    if (existing !== undefined) {
      return existing;
    }
    const decoded = await decodeWebSocketAttachment(webSocket.deserializeAttachment());
    if (decoded._tag !== "Decoded") {
      return undefined;
    }
    const state: EventWebSocketState = {
      sessionId: decoded.attachment.sessionId,
      subscriberId: decoded.attachment.subscriberId,
      trace: decoded.attachment.trace,
      delivery: makeWebSocketDeliveryState({
        subscriberId: decoded.attachment.subscriberId,
        resumeSeq: decoded.attachment.lastAckedSeq,
        policy: defaultEDAWebSocketFlowControl,
        coldRestored: true,
      }),
      head: decoded.attachment.lastAckedSeq,
      queue: Promise.resolve(),
    };
    this.eventSockets.set(webSocket, state);
    return state;
  }

  /** Fan one published event out to every accepted event WebSocket, inline in the turn. */
  private async fanoutPublishedEvent(event: PositionedEvent): Promise<void> {
    // Union host-enumerated sockets (covers post-eviction restores) with the
    // in-memory map (covers hosts without a WebSocket enumerator).
    const webSockets = new Set<WebSocket>([
      ...(this.options.getWebSockets?.() ?? []),
      ...this.eventSockets.keys(),
    ]);
    const deliveries: Promise<void>[] = [];
    for (const webSocket of webSockets) {
      const state = await this.ensureEventWebSocket(webSocket);
      if (state === undefined) {
        continue;
      }
      deliveries.push(
        this.enqueueSocketOp(webSocket, state, () =>
          this.deliverPublishedEvent(webSocket, state, event),
        ),
      );
    }
    await Promise.all(deliveries);
  }

  private async deliverPublishedEvent(
    webSocket: WebSocket,
    state: EventWebSocketState,
    event: PositionedEvent,
  ): Promise<void> {
    if (event.event.durability !== "durable") {
      this.applyDeliveryResult(
        webSocket,
        state,
        onEphemeralEvent(state.delivery, event, Date.now()),
      );
      return;
    }

    state.head = SequenceNumber.make(Math.max(state.head, event.position.seq));
    if (event.position.seq <= state.delivery.sentDurableThroughSeq) {
      return;
    }
    if (event.position.seq === state.delivery.sentDurableThroughSeq + 1) {
      const result = onDurableEvents(state.delivery, [event], state.head, Date.now());
      const open = this.applyDeliveryResult(webSocket, state, result);
      if (open && result.wantsCatchUpAfterSeq !== undefined) {
        await this.runCatchUp(webSocket, state);
      }
      return;
    }
    await this.runCatchUp(webSocket, state);
  }

  /** ACK-clocked durable catch-up: read bounded store slices until the window fills or the head is reached. */
  private async runCatchUp(webSocket: WebSocket, state: EventWebSocketState): Promise<void> {
    const policy = state.delivery.policy;
    const limit = policy.maxInFlightFrames * policy.maxFrameEvents;
    for (;;) {
      let slice: { events: ReadonlyArray<PositionedEvent>; head: SequenceNumber };
      try {
        const afterSeq = state.delivery.sentDurableThroughSeq;
        const runtime = await this.getRuntime(state.sessionId);
        slice = await runtime.runtime.runPromise(
          Effect.gen(function* () {
            const eda = yield* EDARuntime;
            return yield* eda.eventsSlice(afterSeq, limit);
          }).pipe(
            Effect.withSpan(
              "agent.events.stream",
              hostSpanOptions(state.trace, {
                "eda.session.id": state.sessionId,
                "eda.subscriber.id": state.subscriberId,
                "eda.seq.after": afterSeq,
              }),
            ),
          ),
        );
      } catch {
        closeWebSocket(webSocket, EDA_WS_CLOSE_SEND_FAILED, "subscriber-failed");
        this.eventSockets.delete(webSocket);
        return;
      }
      state.head = SequenceNumber.make(Math.max(state.head, slice.head));
      if (slice.events.length === 0) {
        return;
      }
      const result = onDurableEvents(state.delivery, slice.events, state.head, Date.now());
      const open = this.applyDeliveryResult(webSocket, state, result);
      if (!open || result.wantsCatchUpAfterSeq === undefined) {
        return;
      }
    }
  }

  /** Serialize async delivery operations per socket; operations must not throw. */
  private enqueueSocketOp(
    webSocket: WebSocket,
    state: EventWebSocketState,
    op: () => Promise<void>,
  ): Promise<void> {
    const run = async (): Promise<void> => {
      try {
        await op();
      } catch {
        closeWebSocket(webSocket, EDA_WS_CLOSE_SEND_FAILED, "subscriber-failed");
        this.eventSockets.delete(webSocket);
      }
    };
    const next = state.queue.then(run);
    state.queue = next;
    return next;
  }

  /** Apply one pure delivery transition to the socket. Returns false when the socket closed. */
  private applyDeliveryResult(
    webSocket: WebSocket,
    state: EventWebSocketState,
    result: EDAWebSocketDeliveryResult,
  ): boolean {
    state.delivery = result.state;
    for (const frame of result.frames) {
      if (!this.trySendFrame(webSocket, frame)) {
        this.eventSockets.delete(webSocket);
        return false;
      }
    }
    if (result.persistSeq !== undefined) {
      try {
        webSocket.serializeAttachment({
          kind: "eda-events-v1",
          sessionId: state.sessionId,
          subscriberId: state.subscriberId,
          lastAckedSeq: result.persistSeq,
          trace: state.trace,
        } satisfies EDAWebSocketAttachment);
      } catch {
        // Attachment persistence is best-effort; the durable cursor is re-derived from ACKs.
      }
    }
    if (result.close !== undefined) {
      closeWebSocket(webSocket, result.close.code, result.close.reason);
      this.eventSockets.delete(webSocket);
      return false;
    }
    return true;
  }

  private trySendFrame(webSocket: WebSocket, frame: EDAWebSocketServerFrame): boolean {
    try {
      webSocket.send(encodeEDAWebSocketServerFrame(frame, this.options.webSocketProtocol));
      return true;
    } catch {
      closeWebSocket(webSocket, EDA_WS_CLOSE_SEND_FAILED, "send-failed");
      return false;
    }
  }

  private async getRuntime(sessionId: SessionId): Promise<HostRuntimeState> {
    if (this.runtimeState !== undefined) {
      if (this.runtimeState.sessionId !== sessionId) {
        throw new Error(
          `EDASessionDurableObjectHost is scoped to session ${this.runtimeState.sessionId}; received ${sessionId}`,
        );
      }
      return this.runtimeState;
    }

    this.keepAlive.restart();
    const runtimePromise = this.runtimePromise ?? this.buildRuntime(sessionId);
    this.runtimePromise = runtimePromise;
    try {
      return await runtimePromise;
    } catch (error) {
      if (this.runtimePromise === runtimePromise) {
        this.runtimePromise = undefined;
      }
      throw error;
    }
  }

  private async buildRuntime(sessionId: SessionId): Promise<HostRuntimeState> {
    const runtime = ManagedRuntime.make(
      makeEDADurableObjectRuntimeLayer({
        config: this.options.config,
        compactionExecutorLayer: this.options.compactionExecutorLayer,
        compactionPolicyLayer: this.options.compactionPolicyLayer,
        keepAlive: this.keepAlive,
        modelLayer: this.options.modelLayer,
        promptProjectorLayer: this.options.promptProjectorLayer,
        reducers: this.options.reducers,
        sessionId,
        sinks: this.options.sinks,
        storage: this.options.storage,
        toolkit: resolveRuntimeFactory(this.options.toolkit, sessionId),
        toolRegistry: resolveRuntimeFactory(this.options.toolRegistry, sessionId),
        tracer: resolveRuntimeFactory(this.options.tracer, sessionId),
      }),
    );
    const state = { runtime, sessionId } satisfies HostRuntimeState;
    try {
      await runtime.context();
      const fanout = (event: PositionedEvent): Effect.Effect<void> =>
        Effect.promise(() => this.fanoutPublishedEvent(event));
      await runtime.runPromise(
        Effect.gen(function* () {
          const eda = yield* EDARuntime;
          yield* eda.registerDeliveryListener(fanout);
        }),
      );
      this.runtimeState = state;
      return state;
    } catch (error) {
      await runtime.dispose().catch(() => undefined);
      throw error;
    }
  }
}

type DecodedWebSocketAttachment =
  | { readonly _tag: "Missing" }
  | { readonly _tag: "Malformed" }
  | { readonly _tag: "Decoded"; readonly attachment: EDAWebSocketAttachment };

const decodeWebSocketAttachment = async (input: unknown): Promise<DecodedWebSocketAttachment> => {
  if (input === null || input === undefined) {
    return { _tag: "Missing" };
  }
  try {
    return {
      _tag: "Decoded",
      attachment: await Effect.runPromise(decodeEDAWebSocketAttachment(input)),
    };
  } catch {
    return { _tag: "Malformed" };
  }
};

const closeWebSocket = (webSocket: WebSocket, code: number, reason: string): void => {
  try {
    webSocket.close(code, reason);
  } catch {
    // The peer may already have closed or the runtime may reject duplicate close.
  }
};

/** Decode a raw RPC command payload at the Durable Object boundary. */
export const encodeEdaRpcCommand = (input: EDACommand): unknown =>
  Schema.encodeSync(EDACommand)(input);

const EDARpcSubmittable = Schema.Union([EDACommand, DurableEventEnvelope]);

/** Encode a command/app-event batch into structured-clone-safe RPC values. */
export const encodeEdaRpcSubmittables = (
  input: ReadonlyArray<EDASubmittable>,
): ReadonlyArray<unknown> => input.map((item) => Schema.encodeSync(EDARpcSubmittable)(item));

/** Decode a raw RPC command payload at the Durable Object boundary. */
export const decodeEdaRpcCommand = (
  input: unknown,
): Effect.Effect<EDACommand, Schema.SchemaError> =>
  Schema.decodeUnknownExit(EDACommand)(input).pipe(
    Exit.match({ onFailure: Effect.failCause, onSuccess: Effect.succeed }),
  );

/** Decode a raw RPC batch item at the Durable Object boundary. */
export const decodeEdaRpcSubmittable = (
  input: unknown,
): Effect.Effect<EDASubmittable, Schema.SchemaError> =>
  Schema.decodeUnknownExit(EDARpcSubmittable)(input).pipe(
    Exit.match({ onFailure: Effect.failCause, onSuccess: Effect.succeed }),
  );

/** Decode raw RPC batch items at the Durable Object boundary. */
export const decodeEdaRpcSubmittables = (
  input: ReadonlyArray<unknown>,
): Effect.Effect<ReadonlyArray<EDASubmittable>, Schema.SchemaError> =>
  Effect.forEach(input, decodeEdaRpcSubmittable);
