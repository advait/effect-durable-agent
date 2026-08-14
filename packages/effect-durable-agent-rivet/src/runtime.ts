import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Queue from "effect/Queue";
import * as Redacted from "effect/Redacted";
import * as Tracer from "effect/Tracer";
import type * as LanguageModel from "effect/unstable/ai/LanguageModel";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import type { UniversalWebSocket } from "rivetkit";

import type { DurableTranscriptMessage } from "effect-durable-agent/domain/message-transcript";
import type { EDAWebSocketServerFrameEncoder } from "effect-durable-agent/host/websocket-wire";
import { CompactionExecutor, CompactionPolicy } from "effect-durable-agent/services/compaction";
import { EDAKeepAlive, type EDAKeepAliveLease } from "effect-durable-agent/services/keep-alive";
import { EDAPromptProjector } from "effect-durable-agent/services/prompt-projector";
import {
  EDAReducerRegistry,
  type EDAReducer,
} from "effect-durable-agent/services/reducer-registry";
import {
  EDARuntime,
  type CommittedCommandTerminalEvent,
  type EDARuntimeConfig,
  type EDARuntimeShape,
} from "effect-durable-agent/services/runtime";
import { makeEDARuntimeLayer } from "effect-durable-agent/services/runtime-layer";
import type { EDASessionSnapshot } from "effect-durable-agent/services/session-query";
import type { CommittedDurableEvent } from "effect-durable-agent/services/session-store";
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
  SubscriberLagged,
  SubscriberProtocolError,
  SubscriberSendFailed,
  runWebSocketSubscriber,
} from "effect-durable-agent/services/websocket-subscriber";
import { EDACommand } from "effect-durable-agent/types/commands";
import { CommandId, SequenceNumber, SessionId } from "effect-durable-agent/types/core";
import {
  makeRootEDATraceMetadata,
  type EDATraceMetadata,
} from "effect-durable-agent/types/tracing";
import {
  EDA_WS_CLOSE_LAGGED,
  EDA_WS_CLOSE_GOING_AWAY,
  EDA_WS_CLOSE_PROTOCOL_ERROR,
  EDA_WS_CLOSE_SEND_FAILED,
  EDAWebSocketAttachment,
  EDAWebSocketErrorFrame,
  EDAWebSocketLaggedFrame,
  type EDAWebSocketClientFrame,
  type EDAWebSocketServerFrame,
  SubscriberId,
  decodeEDAWebSocketClientMessage,
  defaultEDAWebSocketFlowControl,
  encodeEDAWebSocketServerFrame,
  laggedCloseReason,
} from "effect-durable-agent/host/websocket-protocol";
import {
  RivetSessionStore,
  RivetSinkCheckpointStore,
  clearRivetSessionStorage,
  type RivetSqlStorage,
} from "./storage";

/** Per-session factory for provider-visible model toolkit definitions. */
export type EDARivetToolkitFactory = (input: { readonly sessionId: SessionId }) => EDAModelToolkit;

/** Per-session factory for custom tool registry implementations. */
export type EDARivetToolRegistryFactory = (input: {
  readonly sessionId: SessionId;
}) => EDAToolRegistryShape;

/** Per-session tracing factory. */
export type EDARivetTracerFactory = (input: { readonly sessionId: SessionId }) => Tracer.Tracer;

/** Rivet's promise-scoped actor retention primitive. */
export type RivetKeepAwake = <A>(promise: Promise<A>) => Promise<A>;

/** Options for composing the EDA runtime inside one Rivet Actor. */
export interface EDARivetRuntimeLayerOptions {
  readonly config: EDARuntimeConfig;
  readonly compactionExecutorLayer?: Layer.Layer<
    CompactionExecutor,
    never,
    LanguageModel.LanguageModel
  >;
  readonly compactionPolicyLayer?: Layer.Layer<CompactionPolicy>;
  readonly keepAlive?: RivetActorKeepAlive;
  readonly modelLayer: Layer.Layer<LanguageModel.LanguageModel>;
  readonly promptProjectorLayer?: Layer.Layer<EDAPromptProjector>;
  readonly reducers?: ReadonlyArray<EDAReducer<any>>;
  readonly sessionId: SessionId;
  readonly sinks?: ReadonlyArray<EDASink>;
  readonly storage: RivetSqlStorage;
  readonly toolkit?: EDAModelToolkit;
  readonly toolRegistry?: EDAToolRegistryShape;
  readonly tracer?: Tracer.Tracer;
}

/** Options for one live EDA controller owned by a Rivet Actor generation. */
export interface EDASessionRivetHostOptions {
  readonly config: EDARuntimeConfig;
  readonly compactionExecutorLayer?: Layer.Layer<
    CompactionExecutor,
    never,
    LanguageModel.LanguageModel
  >;
  readonly compactionPolicyLayer?: Layer.Layer<CompactionPolicy>;
  readonly keepAwake: RivetKeepAwake;
  readonly modelLayer: Layer.Layer<LanguageModel.LanguageModel>;
  readonly promptProjectorLayer?: Layer.Layer<EDAPromptProjector>;
  readonly reducers?: ReadonlyArray<EDAReducer<any>>;
  readonly sinks?: ReadonlyArray<EDASink>;
  readonly storage: RivetSqlStorage;
  readonly toolkit?: EDAModelToolkit | EDARivetToolkitFactory;
  readonly toolRegistry?: EDAToolRegistryShape | EDARivetToolRegistryFactory;
  readonly tracer?: Tracer.Tracer | EDARivetTracerFactory;
  readonly webSocketProtocol?: EDAWebSocketServerFrameEncoder;
}

export interface EDARivetSubmitInput {
  readonly command: EDACommand;
  readonly sessionId: SessionId;
  readonly trace?: EDATraceMetadata;
}

export interface EDARivetSubmitBatchInput {
  readonly items: ReadonlyArray<EDASubmittable>;
  readonly sessionId: SessionId;
  readonly trace?: EDATraceMetadata;
}

export interface EDARivetBlockOnCommandInput {
  readonly afterSeq?: SequenceNumber;
  readonly commandId: CommandId;
  readonly sessionId: SessionId;
  readonly trace?: EDATraceMetadata;
}

export interface EDARivetScopedInput {
  readonly sessionId: SessionId;
  readonly trace?: EDATraceMetadata;
}

/** Connection state persisted by Rivet for hibernated raw WebSockets. */
export type EDARivetWebSocketState = EDAWebSocketAttachment;

export interface EDARivetWebSocketInput {
  readonly attachment: EDARivetWebSocketState;
  readonly persistAck: (attachment: EDARivetWebSocketState) => void;
  readonly webSocket: UniversalWebSocket;
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

/** Reference-counted bridge from EDA active-work scopes to `c.keepAwake`. */
export class RivetActorKeepAlive {
  private activeLeases = 0;
  private releaseGeneration: (() => void) | undefined;

  constructor(private readonly keepAwake: RivetKeepAwake) {}

  async acquire(_label: string): Promise<EDAKeepAliveLease> {
    if (this.activeLeases === 0) {
      const pending = new Promise<void>((resolve) => {
        this.releaseGeneration = resolve;
      });
      void this.keepAwake(pending).catch(() => undefined);
    }
    this.activeLeases += 1;
    let released = false;
    return {
      release: async () => {
        if (released) {
          return;
        }
        released = true;
        this.activeLeases = Math.max(0, this.activeLeases - 1);
        if (this.activeLeases === 0) {
          this.releaseGeneration?.();
          this.releaseGeneration = undefined;
        }
      },
    };
  }

  /** Release lifecycle retention when the actor sleeps or is destroyed. */
  shutdown(): void {
    this.activeLeases = 0;
    this.releaseGeneration?.();
    this.releaseGeneration = undefined;
  }
}

/** Compose the host-neutral EDA service graph with Rivet persistence and lifecycle adapters. */
export const makeEDARivetRuntimeLayer = ({
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
}: EDARivetRuntimeLayerOptions): Layer.Layer<EDARuntime, SessionCommandAdmissionError> =>
  makeEDARuntimeLayer({
    config,
    compactionExecutorLayer,
    compactionPolicyLayer,
    keepAliveLayer:
      keepAlive === undefined
        ? EDAKeepAlive.Noop
        : EDAKeepAlive.FromAcquire((label) => keepAlive.acquire(label)),
    modelLayer,
    promptProjectorLayer,
    reducerRegistryLayer: EDAReducerRegistry.Live(reducers ?? []),
    sessionId,
    sessionStoreLayer: RivetSessionStore.layer({ sessionId, storage }),
    sinkCheckpointStoreLayer: RivetSinkCheckpointStore.layer(storage),
    sinks,
    toolkit,
    toolRegistry,
    tracer,
  });

/** Provider request defaults accepted by EDA's OpenAI Responses model adapter. */
export type EDAOpenAiModelConfig = Omit<typeof OpenAiLanguageModel.Config.Service, "model">;

/** OpenAI-backed model layer suitable for Node-hosted Rivet Actors. */
export const makeEDARivetOpenAiModelLayer = (options: {
  readonly apiKey: string;
  readonly apiUrl?: string;
  readonly config?: EDAOpenAiModelConfig;
  readonly modelId: string;
}): Layer.Layer<LanguageModel.LanguageModel> => {
  const apiKey = options.apiKey.trim();
  if (apiKey.length === 0) {
    throw new Error("OPENAI_API_KEY is required for the Rivet OpenAI model layer");
  }
  const client = OpenAiClient.layer({
    apiKey: Redacted.make(apiKey),
    ...(options.apiUrl === undefined ? {} : { apiUrl: options.apiUrl }),
  }).pipe(Layer.provide(FetchHttpClient.layer));
  return OpenAiLanguageModel.layer({
    model: options.modelId,
    ...(options.config === undefined ? {} : { config: options.config }),
  }).pipe(Layer.provide(client));
};

/** Runtime controller stored in Rivet's per-wake `vars`. */
export class EDASessionRivetHost {
  private readonly keepAlive: RivetActorKeepAlive;
  private readonly eventSockets = new Map<UniversalWebSocket, EventWebSocketState>();
  private destructionPromise: Promise<void> | undefined;
  private runtimeGeneration = 0;
  private runtimeState: HostRuntimeState | undefined;
  private runtimePromise: Promise<HostRuntimeState> | undefined;

  constructor(private readonly options: EDASessionRivetHostOptions) {
    this.keepAlive = new RivetActorKeepAlive(options.keepAwake);
  }

  /** Build and recover the session runtime during `onWake`. */
  async wake(sessionId: SessionId): Promise<void> {
    await this.getRuntime(sessionId);
  }

  async submit(input: EDARivetSubmitInput): Promise<CommittedDurableEvent> {
    return this.withRuntime(input.sessionId, (eda) => eda.submit(input.command), input.trace);
  }

  async submitBatch(
    input: EDARivetSubmitBatchInput,
  ): Promise<ReadonlyArray<CommittedDurableEvent>> {
    return this.withRuntime(input.sessionId, (eda) => eda.submit(input.items), input.trace);
  }

  async submitAndBlock(input: EDARivetSubmitInput): Promise<CommittedCommandTerminalEvent> {
    return this.withRuntime(
      input.sessionId,
      (eda) => eda.submitAndBlock(input.command),
      input.trace,
    );
  }

  async blockOnCommand(input: EDARivetBlockOnCommandInput): Promise<CommittedCommandTerminalEvent> {
    return this.withRuntime(
      input.sessionId,
      (eda) => eda.blockOnCommand(input.commandId, input.afterSeq ?? SequenceNumber.make(0)),
      input.trace,
    );
  }

  async snapshot(input: EDARivetScopedInput): Promise<EDASessionSnapshot> {
    return this.withRuntime(input.sessionId, (eda) => eda.snapshot(), input.trace);
  }

  async messages(input: EDARivetScopedInput): Promise<ReadonlyArray<DurableTranscriptMessage>> {
    return this.withRuntime(input.sessionId, (eda) => eda.messages(), input.trace);
  }

  /** Attach the EDA event protocol to one Rivet raw WebSocket. */
  async acceptEventWebSocket(input: EDARivetWebSocketInput): Promise<void> {
    if (this.eventSockets.has(input.webSocket)) {
      return;
    }
    const { attachment, webSocket } = input;
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

    webSocket.onmessage = (event: { readonly data: unknown }) =>
      this.webSocketMessage(webSocket, event.data);
    webSocket.onclose = () => this.webSocketClosed(webSocket);
    webSocket.onerror = () => this.webSocketClosed(webSocket);

    const send = (frame: EDAWebSocketServerFrame): Effect.Effect<void, SubscriberSendFailed> =>
      Effect.try({
        try: () =>
          webSocket.send(encodeEDAWebSocketServerFrame(frame, this.options.webSocketProtocol)),
        catch: (cause) => new SubscriberSendFailed({ subscriberId: state.subscriberId, cause }),
      });
    const persistAck = (seq: SequenceNumber): Effect.Effect<void> =>
      Effect.sync(() => {
        state.lastAckedSeq = seq;
        input.persistAck(
          EDAWebSocketAttachment.make({
            ...attachment,
            lastAckedSeq: seq,
          }),
        );
      });

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
        Effect.catch((error) => this.closeForSubscriberError(webSocket, state, send, error)),
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

  /** Clear one session without poisoning the warm actor for later recreation. */
  async destroy(input: EDARivetScopedInput): Promise<void> {
    if (this.destructionPromise !== undefined) {
      await this.destructionPromise;
      return;
    }
    const destruction = this.destroyNow(input);
    this.destructionPromise = destruction;
    try {
      await destruction;
    } finally {
      if (this.destructionPromise === destruction) {
        this.destructionPromise = undefined;
      }
    }
  }

  private async destroyNow(input: EDARivetScopedInput): Promise<void> {
    const state = this.runtimeState;
    if (state !== undefined && state.sessionId !== input.sessionId) {
      throw new Error(`Rivet host is scoped to ${state.sessionId}; received ${input.sessionId}`);
    }
    await this.closeEventWebSockets();
    await this.dispose();
    await Effect.runPromise(clearRivetSessionStorage(this.options.storage));
  }

  /** Dispose the generation-local Effect runtime during actor sleep/shutdown. */
  async dispose(): Promise<void> {
    this.keepAlive.shutdown();
    this.runtimeGeneration += 1;
    const state = this.runtimeState;
    const pending = this.runtimePromise;
    this.runtimeState = undefined;
    this.runtimePromise = undefined;
    await state?.runtime.dispose();
    await pending?.catch(() => undefined);
  }

  private async closeEventWebSockets(): Promise<void> {
    const webSockets = [...this.eventSockets.keys()];
    for (const webSocket of webSockets) {
      closeWebSocket(webSocket, EDA_WS_CLOSE_GOING_AWAY, "session-destroyed");
    }
    await Promise.all(webSockets.map((webSocket) => this.webSocketClosed(webSocket)));
  }

  private async webSocketMessage(webSocket: UniversalWebSocket, message: unknown): Promise<void> {
    const state = this.eventSockets.get(webSocket);
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

  private async webSocketClosed(webSocket: UniversalWebSocket): Promise<void> {
    const state = this.eventSockets.get(webSocket);
    if (state === undefined) {
      return;
    }
    await Effect.runPromise(Deferred.succeed(state.closed, undefined).pipe(Effect.ignore));
    state.interrupt?.();
    this.eventSockets.delete(webSocket);
  }

  private closeForSubscriberError(
    webSocket: UniversalWebSocket,
    state: EventWebSocketState,
    send: (frame: EDAWebSocketServerFrame) => Effect.Effect<void, SubscriberSendFailed>,
    error: unknown,
  ): Effect.Effect<void> {
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
            closeWebSocket(webSocket, EDA_WS_CLOSE_LAGGED, laggedCloseReason(lagged.lastAckedSeq)),
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
  }

  private async withRuntime<A>(
    sessionId: SessionId,
    use: (runtime: EDARuntimeShape) => Effect.Effect<A, unknown>,
    trace = makeRootEDATraceMetadata(),
  ): Promise<A> {
    const runtime = await this.getRuntime(sessionId);
    return runtime.runtime.runPromise(
      Effect.gen(function* () {
        const eda = yield* EDARuntime;
        return yield* use(eda);
      }).pipe(
        Effect.withSpan(
          "agent.rivet.request",
          hostSpanOptions(trace, { "eda.session.id": sessionId }),
        ),
      ),
    );
  }

  private async getRuntime(sessionId: SessionId): Promise<HostRuntimeState> {
    await this.destructionPromise;
    if (this.runtimeState !== undefined) {
      if (this.runtimeState.sessionId !== sessionId) {
        throw new Error(
          `EDASessionRivetHost is scoped to ${this.runtimeState.sessionId}; received ${sessionId}`,
        );
      }
      return this.runtimeState;
    }
    const pending = this.runtimePromise ?? this.buildRuntime(sessionId, this.runtimeGeneration);
    this.runtimePromise = pending;
    try {
      return await pending;
    } catch (error) {
      if (this.runtimePromise === pending) {
        this.runtimePromise = undefined;
      }
      throw error;
    }
  }

  private async buildRuntime(sessionId: SessionId, generation: number): Promise<HostRuntimeState> {
    const runtime = ManagedRuntime.make(
      makeEDARivetRuntimeLayer({
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
        toolkit: resolveFactory(this.options.toolkit, sessionId),
        toolRegistry: resolveFactory(this.options.toolRegistry, sessionId),
        tracer: resolveFactory(this.options.tracer, sessionId),
      }),
    );
    const state = { runtime, sessionId } satisfies HostRuntimeState;
    try {
      await runtime.context();
      if (generation !== this.runtimeGeneration) {
        throw new Error("EDA Rivet runtime was disposed while it was starting");
      }
      this.runtimeState = state;
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

const closeWebSocket = (webSocket: UniversalWebSocket, code: number, reason: string): void => {
  try {
    webSocket.close(code, reason);
  } catch {
    // The peer may already be closed or reject a duplicate close.
  }
};

const isTagged = (input: unknown, tag: string): boolean =>
  typeof input === "object" && input !== null && "_tag" in input && input._tag === tag;
