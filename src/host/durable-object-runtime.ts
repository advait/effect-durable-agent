import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Queue from "effect/Queue";
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

import type { DurableTranscriptMessage } from "../domain/message-transcript";
import type { EDASessionSnapshot } from "../services/session-query";
import { EDACommand } from "../types/commands";
import { DurableEventEnvelope } from "../types/events";
import { CommandId, SequenceNumber, SessionId } from "../types/core";
import { CommittedDurableEvent, EDASessionStoreError } from "../services/session-store";
import { CompactionExecutor, CompactionPolicy, CompactionRunner } from "../services/compaction";
import { EventFactory } from "../services/event-factory";
import { IdGenerator } from "../services/id-generator";
import { EDAKeepAlive } from "../services/keep-alive";
import { LiveEventBus } from "../services/live-event-bus";
import { EDAPromptProjector } from "../services/prompt-projector";
import { EDASinkRegistry, type EDASink } from "../services/sink-registry";
import { EDARuntime, type EDARuntimeConfig } from "../services/runtime";
import { compactSpanAttributes, toEdaExternalSpan } from "../services/tracing";
import { makeRootEDATraceMetadata, type EDATraceMetadata } from "../types/tracing";
import {
  SubscriberLagged,
  SubscriberProtocolError,
  SubscriberSendFailed,
  runWebSocketSubscriber,
} from "../services/websocket-subscriber";
import type { EDASubmittable } from "../services/session-state";
import type { CommittedCommandTerminalEvent, EDARuntimeShape } from "../services/runtime";
import { SessionContext } from "../services/session-context";
import { EDAReducerRegistry, type EDAReducer } from "../services/reducer-registry";
import { EDASessionQuery } from "../services/session-query";
import { SessionState, type SessionCommandAdmissionError } from "../services/session-state";
import { ToolExecutor } from "../services/tool-executor";
import {
  EDAToolRegistry,
  type EDAModelToolkit,
  type EDAToolRegistryShape,
} from "../services/tool-registry";
import { InferenceRunner } from "../services/inference-runner";
import { TurnRunner } from "../services/turn-runner";
import {
  DurableObjectKeepAlive,
  type DurableObjectAlarmStorage,
  type DurableObjectBackgroundWaiter,
} from "./durable-object-keepalive";
import { DurableObjectSessionStore } from "./durable-object-store";
import { DurableObjectSinkCheckpointStore } from "./durable-object-sink-checkpoints";
import type { DurableObjectSessionStorage } from "./durable-object-storage";
import {
  EDA_WS_CLOSE_LAGGED,
  EDA_WS_CLOSE_PROTOCOL_ERROR,
  EDA_WS_CLOSE_SEND_FAILED,
  EDAWebSocketAttachment,
  EDAWebSocketErrorFrame,
  EDAWebSocketLaggedFrame,
  SubscriberId,
  decodeEDAWebSocketAttachment,
  decodeEDAWebSocketClientMessage,
  defaultEDAWebSocketFlowControl,
  encodeEDAWebSocketServerFrame,
  laggedCloseReason,
  type EDAWebSocketClientFrame,
  type EDAWebSocketServerFrame,
} from "./websocket-protocol";
import type { EDAWebSocketServerFrameEncoder } from "./websocket-wire";

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
  readonly closed: Deferred.Deferred<void>;
  readonly incoming: Queue.Queue<EDAWebSocketClientFrame>;
  interrupt: (() => void) | undefined;
  lastAckedSeq: SequenceNumber;
  readonly sessionId: SessionId;
  readonly subscriberId: SubscriberId;
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
  const Bus = LiveEventBus.Live;
  const KeepAlive =
    keepAlive === undefined
      ? EDAKeepAlive.Noop
      : EDAKeepAlive.FromAcquire(() => keepAlive.acquire());
  const SinkCheckpoints = DurableObjectSinkCheckpointStore.layer(storage);
  const Ids = IdGenerator.Live;
  const Session = SessionContext.Live(sessionId);
  const Factory = EventFactory.Live.pipe(Layer.provideMerge(Layer.mergeAll(Session, Ids)));
  const PromptProjector = promptProjectorLayer ?? EDAPromptProjector.Default;
  const ReducerRegistry = EDAReducerRegistry.Live(reducers ?? []);
  const CompactionPolicyLayer = compactionPolicyLayer ?? CompactionPolicy.Disabled;
  const CompactionExecutorLayer = compactionExecutorLayer ?? CompactionExecutor.Disabled;
  const Compaction = CompactionRunner.Live.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Store,
        Factory,
        Ids,
        KeepAlive,
        modelLayer,
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
    toolRegistry !== undefined
      ? EDAToolRegistry.FromShape(toolRegistry)
      : toolkit === undefined
        ? EDAToolRegistry.Empty
        : EDAToolRegistry.FromToolkit(toolkit);
  const InferenceRunnerLayer = InferenceRunner.Live.pipe(
    Layer.provideMerge(Layer.mergeAll(Factory, modelLayer, Registry, Ids)),
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
  private readonly eventSockets = new WeakMap<WebSocket, EventWebSocketState>();
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
    const attachment = EDAWebSocketAttachment.make({
      kind: "eda-events-v1",
      sessionId: input.sessionId,
      subscriberId: SubscriberId.make(crypto.randomUUID()),
      lastAckedSeq: input.afterSeq ?? SequenceNumber.make(0),
      trace: input.trace ?? makeRootEDATraceMetadata(),
    });
    input.webSocket.serializeAttachment(attachment);
    await this.startEventWebSocket(input.webSocket, attachment);
  }

  /** Restore hibernated event WebSockets whose in-memory subscriber fibers disappeared. */
  async restoreEventWebSockets(webSockets: ReadonlyArray<WebSocket>): Promise<void> {
    for (const webSocket of webSockets) {
      if (this.eventSockets.has(webSocket)) {
        continue;
      }
      const decoded = await decodeWebSocketAttachment(webSocket.deserializeAttachment());
      if (decoded._tag === "Decoded") {
        await this.startEventWebSocket(webSocket, decoded.attachment);
      } else if (decoded._tag === "Malformed") {
        closeWebSocket(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "attachment");
      }
    }
  }

  /** Route one inbound WebSocket message into the subscriber ACK queue. */
  async webSocketMessage(webSocket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const state = await this.ensureEventWebSocket(webSocket);
    if (state === undefined) {
      closeWebSocket(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol");
      return;
    }
    if (typeof message !== "string") {
      closeWebSocket(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "binary");
      state.interrupt?.();
      return;
    }

    try {
      const frame = await Effect.runPromise(decodeEDAWebSocketClientMessage(message));
      await Effect.runPromise(Queue.offer(state.incoming, frame));
    } catch {
      closeWebSocket(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol");
      state.interrupt?.();
    }
  }

  /** Interrupt subscriber work after the client closes the WebSocket. */
  async webSocketClose(webSocket: WebSocket): Promise<void> {
    const state = this.eventSockets.get(webSocket);
    if (state === undefined) {
      return;
    }
    await Effect.runPromise(Deferred.succeed(state.closed, undefined).pipe(Effect.ignore));
    state.interrupt?.();
    this.eventSockets.delete(webSocket);
  }

  /** Interrupt subscriber work after a host WebSocket error. */
  async webSocketError(webSocket: WebSocket): Promise<void> {
    const state = this.eventSockets.get(webSocket);
    if (state === undefined) {
      return;
    }
    await Effect.runPromise(Deferred.succeed(state.closed, undefined).pipe(Effect.ignore));
    state.interrupt?.();
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
    await this.startEventWebSocket(webSocket, decoded.attachment);
    return this.eventSockets.get(webSocket);
  }

  private async startEventWebSocket(
    webSocket: WebSocket,
    attachment: EDAWebSocketAttachment,
  ): Promise<void> {
    if (this.eventSockets.has(webSocket)) {
      return;
    }
    const runtime = await this.getRuntime(attachment.sessionId);
    const resources = await runtime.runtime.runPromise(
      Effect.gen(function* () {
        const incoming = yield* Queue.unbounded<EDAWebSocketClientFrame>();
        const closed = yield* Deferred.make<void>();
        return { incoming, closed };
      }),
    );
    const state: EventWebSocketState = {
      ...resources,
      interrupt: undefined,
      lastAckedSeq: attachment.lastAckedSeq,
      sessionId: attachment.sessionId,
      subscriberId: attachment.subscriberId,
    };
    this.eventSockets.set(webSocket, state);

    const send = (frame: EDAWebSocketServerFrame): Effect.Effect<void, SubscriberSendFailed> =>
      Effect.try({
        try: () =>
          webSocket.send(encodeEDAWebSocketServerFrame(frame, this.options.webSocketProtocol)),
        catch: (cause) => new SubscriberSendFailed({ subscriberId: state.subscriberId, cause }),
      });
    const persistAck = (seq: SequenceNumber): Effect.Effect<void> =>
      Effect.sync(() => {
        state.lastAckedSeq = seq;
        webSocket.serializeAttachment({
          kind: "eda-events-v1",
          sessionId: state.sessionId,
          subscriberId: state.subscriberId,
          lastAckedSeq: seq,
          trace: attachment.trace,
        } satisfies EDAWebSocketAttachment);
      });

    const closeForError = (error: unknown): Effect.Effect<void> => {
      if (error instanceof SubscriberLagged || isTagged(error, "SubscriberLagged")) {
        const lagged = error as SubscriberLagged;
        return send(
          EDAWebSocketLaggedFrame.make({
            _tag: "lagged",
            resumeSeq: lagged.lastAckedSeq,
            reason: lagged.reason,
          }),
        ).pipe(
          Effect.ignore,
          Effect.andThen(
            Effect.sync(() =>
              closeWebSocket(
                webSocket,
                EDA_WS_CLOSE_LAGGED,
                laggedCloseReason(lagged.lastAckedSeq),
              ),
            ),
          ),
        );
      }
      if (error instanceof SubscriberProtocolError || isTagged(error, "SubscriberProtocolError")) {
        const protocolError = error as SubscriberProtocolError;
        return send(
          EDAWebSocketErrorFrame.make({
            _tag: "error",
            message: protocolError.message.slice(0, 120),
          }),
        ).pipe(
          Effect.ignore,
          Effect.andThen(
            Effect.sync(() => closeWebSocket(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol")),
          ),
        );
      }
      return Effect.sync(() => closeWebSocket(webSocket, EDA_WS_CLOSE_SEND_FAILED, "send-failed"));
    };

    const program = Effect.scoped(
      runWebSocketSubscriber({
        subscriberId: state.subscriberId,
        resumeSeq: state.lastAckedSeq,
        policy: defaultEDAWebSocketFlowControl,
        transport: {
          send,
          incoming: state.incoming,
          closed: state.closed,
          persistAck,
        },
      }).pipe(
        Effect.catch((error) => closeForError(error)),
        Effect.ensuring(
          Effect.sync(() => {
            this.eventSockets.delete(webSocket);
          }),
        ),
        Effect.withSpan(
          "agent.events.stream",
          hostSpanOptions(attachment.trace, {
            "eda.session.id": state.sessionId,
            "eda.subscriber.id": state.subscriberId,
            "eda.seq.after": state.lastAckedSeq,
          }),
        ),
      ),
    );

    state.interrupt = runtime.runtime.runCallback(program, {
      onExit: (exit) => {
        this.eventSockets.delete(webSocket);
        if (Exit.isFailure(exit)) {
          closeWebSocket(webSocket, EDA_WS_CLOSE_SEND_FAILED, "subscriber-failed");
        }
      },
    });
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

const isTagged = (input: unknown, tag: string): boolean =>
  typeof input === "object" && input !== null && "_tag" in input && input._tag === tag;

/** Decode a raw RPC command payload at the Durable Object boundary. */
export const decodeEdaRpcCommand = (
  input: unknown,
): Effect.Effect<EDACommand, Schema.SchemaError> => Schema.decodeUnknownEffect(EDACommand)(input);

const EDARpcSubmittable = Schema.Union([EDACommand, DurableEventEnvelope]);

/** Decode a raw RPC batch item at the Durable Object boundary. */
export const decodeEdaRpcSubmittable = (
  input: unknown,
): Effect.Effect<EDASubmittable, Schema.SchemaError> =>
  Schema.decodeUnknownEffect(EDARpcSubmittable)(input);

/** Decode raw RPC batch items at the Durable Object boundary. */
export const decodeEdaRpcSubmittables = (
  input: ReadonlyArray<unknown>,
): Effect.Effect<ReadonlyArray<EDASubmittable>, Schema.SchemaError> =>
  Effect.forEach(input, decodeEdaRpcSubmittable);
