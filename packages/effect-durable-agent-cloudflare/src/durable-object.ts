import type { CommittedDurableEvent } from "effect-durable-agent/services/session-store";
import { durableEventEnvelope } from "effect-durable-agent/types/events/durable";
import { encodeEdaRpcDurableEvent } from "./rpc-codec";
import {
  SessionScopedInput,
  SessionTranscriptWindowInput,
  SessionCommandInput,
  SessionGrantRunInput,
  SessionBatchInput,
  SessionBlockInput,
  SessionOutcomeInput,
  edaSessionRpc,
} from "./rpc-spec";
import { DurableObject } from "cloudflare:workers";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { EDARuntimeConfig } from "effect-durable-agent/services/runtime";
import {
  CommandId,
  RunRequestId,
  SequenceNumber,
  SessionId,
} from "effect-durable-agent/types/core";
import {
  EDATraceMetadata,
  makeEDATraceMetadataFromParent,
  makeRootEDATraceMetadata,
  parseEDATraceparent,
} from "effect-durable-agent/types/tracing";
import {
  EDASessionController,
  type EDASessionControllerOptions,
  type EDASessionDurableObjectStorage,
} from "./session-controller";
import {
  EDA_WEB_SOCKET_PING_MESSAGE,
  EDA_WEB_SOCKET_PONG_MESSAGE,
} from "effect-durable-agent/websocket";
import * as Result from "effect/Result";
import {
  defaultTranscriptWindowLimits,
  emptyTranscriptMappings,
  makeTranscriptDefinition,
  type TranscriptMaintenanceConfig,
} from "effect-durable-agent/transcript";
import { DurableObjectAlarmScheduler } from "./durable-object-alarms";
import {
  DurableObjectTranscriptBuilder,
  DurableObjectTranscriptStore,
} from "./durable-object-transcript";
import type {
  DurableObjectTranscriptObserver,
  DurableObjectTranscriptOptions,
  TranscriptAlert,
} from "./durable-object-transcript-maintenance";

export type { EDAWebSocketProjection } from "./websocket/projection";
/** Internal Worker-to-object header selecting an app-owned WebSocket projection. */
export const EDA_WEB_SOCKET_PROJECTION_HEADER = "x-eda-websocket-projection";

/**
 * Upgrade query parameter selecting the socket's stream: `transcript` selects the ordered
 * transcript-window stream; anything else keeps the raw event stream.
 */
export const EDA_WEB_SOCKET_STREAM_PARAM = "stream";

/** Encoded session RPC inputs, decoded before invoking the controller. */
export type EDASessionCommandRpcInput = typeof SessionCommandInput.Encoded;
export type EDASessionGrantRunRpcInput = typeof SessionGrantRunInput.Encoded;
export type EDASessionSubmitBatchRpcInput = typeof SessionBatchInput.Encoded;
export type EDASessionScopedRpcInput = typeof SessionScopedInput.Encoded;
export type EDASessionEventsRpcInput = EDASessionScopedRpcInput & { readonly afterSeq?: number };
export type EDASessionBlockOnCommandRpcInput = typeof SessionBlockInput.Encoded;
export type EDASessionRunRequestOutcomeRpcInput = typeof SessionOutcomeInput.Encoded;

/** EDA RPC methods required by the session namespace helper. */
export interface EDASessionRpcSurface {
  readonly runRequestOutcome: (
    input: EDASessionRunRequestOutcomeRpcInput,
  ) => Promise<typeof edaSessionRpc.runRequestOutcome.output.Encoded>;
  readonly grantRun: (
    input: EDASessionGrantRunRpcInput,
  ) => Promise<typeof edaSessionRpc.grantRun.output.Encoded>;
  readonly submit: (
    input: EDASessionCommandRpcInput,
  ) => Promise<typeof edaSessionRpc.submit.output.Encoded>;
  readonly submitBatch: (
    input: EDASessionSubmitBatchRpcInput,
  ) => Promise<typeof edaSessionRpc.submitBatch.output.Encoded>;
  readonly submitAndBlock: (
    input: EDASessionCommandRpcInput,
  ) => Promise<typeof edaSessionRpc.submitAndBlock.output.Encoded>;
  readonly blockOnCommand: (
    input: EDASessionBlockOnCommandRpcInput,
  ) => Promise<typeof edaSessionRpc.blockOnCommand.output.Encoded>;
  readonly snapshot: (
    input: EDASessionScopedRpcInput,
  ) => Promise<typeof edaSessionRpc.snapshot.output.Encoded>;
  readonly messages: (
    input: EDASessionScopedRpcInput,
  ) => Promise<typeof edaSessionRpc.messages.output.Encoded>;
  readonly destroySession: (
    input: EDASessionScopedRpcInput,
  ) => Promise<typeof edaSessionRpc.destroySession.output.Encoded>;
  readonly transcriptStatus: (
    input: EDASessionScopedRpcInput,
  ) => Promise<typeof edaSessionRpc.transcriptStatus.output.Encoded>;
  readonly readTranscriptWindow: (
    input: typeof SessionTranscriptWindowInput.Encoded,
  ) => Promise<typeof edaSessionRpc.readTranscriptWindow.output.Encoded>;
}

/** Host transcript configuration of a session Durable Object. */
export interface EDASessionTranscriptOptions {
  /**
   * The transcript definitions this release carries. Defaults to the definition with no host
   * mappings. A host changing its mappings ships `current` = new, `previous` = old, then runs
   * its backfill through `transcriptStatus` (see `docs/transcript-history.md` §7).
   */
  readonly definitions?: TranscriptMaintenanceConfig;
  /**
   * Adopt the host's existing log incarnation from this `ctx.storage.kv` key (Gia:
   * `gia.log_incarnation`). Without it the adapter mints its own.
   */
  readonly adoptIncarnationKey?: string;
  /** Alerts (frozen versions, missed sessions). Defaults to `console.error`. */
  readonly onAlert?: (alert: TranscriptAlert) => void;
  /** A rebuilt version became active. */
  readonly onActivated?: DurableObjectTranscriptObserver["onActivated"];
}

/** Constructor options for concrete app subclasses of the EDA Durable Object base. */
export type EDASessionDurableObjectOptions<ProjectionState extends object = never> = Omit<
  EDASessionControllerOptions<ProjectionState>,
  "alarmStorage" | "background" | "getWebSockets" | "storage" | "transcript"
> & { readonly transcript?: EDASessionTranscriptOptions };

/** Slices a backfill `transcriptStatus` call may run before it returns. */
const transcriptStatusMaxSlices = 16;
/** Slices one builder alarm runs before rescheduling itself. */
const transcriptAlarmMaxSlices = 4;

/** Resolve a concrete EDA session Durable Object binding by domain session id. */
export const getEDASessionDurableObjectByName = <
  T extends Rpc.DurableObjectBranded & EDASessionRpcSurface,
>(
  namespace: DurableObjectNamespace<T>,
  sessionId: string,
): DurableObjectStub<T> => namespace.getByName(sessionId);

/**
 * Base class for raw Cloudflare Durable Object hosts of one EDA session.
 *
 * Concrete products should extend this class, pass provider/tool layers from
 * their own constructor, and register only that subclass in Wrangler. The base
 * class is deliberately not exported from `workers/app.ts` and owns no binding
 * name or routing convention.
 */
export abstract class EDASessionDurableObject<
  EnvType extends object = object,
  ProjectionState extends object = never,
> extends DurableObject<EnvType> {
  readonly #controller: EDASessionController<ProjectionState>;
  readonly #webSocketProjectionId: string | undefined;
  readonly #alarms: DurableObjectAlarmScheduler;
  readonly #transcriptOptions: DurableObjectTranscriptOptions;
  #transcript:
    | {
        readonly sessionId: SessionId;
        readonly store: DurableObjectTranscriptStore;
        readonly builder: DurableObjectTranscriptBuilder;
      }
    | undefined;

  protected constructor(
    ctx: DurableObjectState,
    env: EnvType,
    options: EDASessionDurableObjectOptions<ProjectionState>,
  ) {
    super(ctx, env);
    this.#webSocketProjectionId = options.webSocketProjection?.id;
    const storage = toEDASessionStorage(this.ctx.storage);
    this.#alarms = new DurableObjectAlarmScheduler(storage);
    const transcript = this.transcriptOptions(options.transcript);
    this.#transcriptOptions = transcript;
    this.#controller = new EDASessionController({
      ...options,
      alarmStorage: this.#alarms.owner("runtime"),
      background: this.ctx,
      getWebSockets: () => this.ctx.getWebSockets(),
      storage,
      transcript,
    });

    // Answer client liveness pings at the runtime layer so idle sockets never
    // wake a hibernated object.
    this.ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair(EDA_WEB_SOCKET_PING_MESSAGE, EDA_WEB_SOCKET_PONG_MESSAGE),
    );

    this.ctx.blockConcurrencyWhile(async () => {
      await Effect.runPromise(EDASessionController.migrate(storage));
      // Resume any builder work an earlier instance left (or a new release created).
      await this.objectTranscript()?.builder.kick();
    });
  }

  async fetch(request: Request): Promise<Response> {
    const upgrade = request.headers.get("Upgrade");
    if (upgrade?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket upgrade.", { status: 426 });
    }

    const url = new URL(request.url);
    const sessionIdRaw = url.searchParams.get("sessionId");
    if (sessionIdRaw === null) {
      return new Response("Missing sessionId.", { status: 400 });
    }
    const afterSeqRaw = url.searchParams.get("afterSeq");
    const afterSeq = afterSeqRaw === null ? undefined : Number(afterSeqRaw);
    if (afterSeq !== undefined && (!Number.isInteger(afterSeq) || afterSeq < 0)) {
      return new Response("Invalid afterSeq.", { status: 400 });
    }
    const projectionId = request.headers.get(EDA_WEB_SOCKET_PROJECTION_HEADER) ?? undefined;
    if (projectionId !== undefined && projectionId !== this.#webSocketProjectionId) {
      return new Response("Unsupported WebSocket projection.", { status: 400 });
    }

    const sessionId = this.parseSessionId(sessionIdRaw);
    const trace = traceMetadataFromRequest(request);

    if (url.searchParams.get(EDA_WEB_SOCKET_STREAM_PARAM) === "transcript") {
      if (projectionId !== undefined || !this.#controller.supportsTranscriptWebSocket) {
        return new Response("Unsupported transcript stream.", { status: 400 });
      }
      const pair = new WebSocketPair();
      this.ctx.acceptWebSocket(pair[1]);
      this.#controller.acceptTranscriptWebSocket({ sessionId, trace, webSocket: pair[1] });
      return new Response(null, { status: 101, webSocket: pair[0] });
    }

    const prepared = await this.#controller.prepareEventWebSocket({
      ...(afterSeq === undefined ? {} : { afterSeq: SequenceNumber.make(afterSeq) }),
      sessionId,
      trace,
      ...(projectionId === undefined ? {} : { projectionId }),
    });

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.ctx.acceptWebSocket(server);
    await this.#controller.acceptPreparedEventWebSocket({
      ...prepared,
      webSocket: server,
    });
    return new Response(null, { status: 101, webSocket: client });
  }

  async submit(
    raw: typeof SessionCommandInput.Encoded,
  ): Promise<typeof edaSessionRpc.submit.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionCommandInput)(raw);
    const committed = await this.#controller.submit({
      command: input.command,
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    return encodeEdaRpcCommittedDurableEvent(committed);
  }

  /** Reconcile lost grant replies from durable request and run facts. */
  async runRequestOutcome(
    raw: typeof SessionOutcomeInput.Encoded,
  ): Promise<typeof edaSessionRpc.runRequestOutcome.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionOutcomeInput)(raw);
    const result = await this.#controller.runRequestOutcome({
      requestId: Schema.decodeUnknownSync(RunRequestId)(input.requestId),
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    return Schema.encodeSync(edaSessionRpc.runRequestOutcome.output)(result);
  }

  /** Trusted RPC only: the calling Worker owns scheduler authentication and authorization. */
  async grantRun(
    raw: typeof SessionGrantRunInput.Encoded,
  ): Promise<typeof edaSessionRpc.grantRun.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionGrantRunInput)(raw);
    const command = input.command;
    const result = await this.#controller.grantRun({
      command,
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    return Schema.encodeSync(edaSessionRpc.grantRun.output)(result);
  }

  async submitBatch(
    raw: typeof SessionBatchInput.Encoded,
  ): Promise<typeof edaSessionRpc.submitBatch.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionBatchInput)(raw);
    const committed = await this.#controller.submitBatch({
      items: input.items,
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    return committed.map(encodeEdaRpcCommittedDurableEvent);
  }

  async submitAndBlock(
    raw: typeof SessionCommandInput.Encoded,
  ): Promise<typeof edaSessionRpc.submitAndBlock.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionCommandInput)(raw);
    const committed = await this.#controller.submitAndBlock({
      command: input.command,
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    return Schema.encodeSync(edaSessionRpc.submitAndBlock.output)(committed);
  }

  async blockOnCommand(
    raw: typeof SessionBlockInput.Encoded,
  ): Promise<typeof edaSessionRpc.blockOnCommand.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionBlockInput)(raw);
    const committed = await this.#controller.blockOnCommand({
      ...(input.afterSeq === undefined ? {} : { afterSeq: SequenceNumber.make(input.afterSeq) }),
      commandId: CommandId.make(input.commandId),
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    return Schema.encodeSync(edaSessionRpc.blockOnCommand.output)(committed);
  }

  async snapshot(
    raw: typeof SessionScopedInput.Encoded,
  ): Promise<typeof edaSessionRpc.snapshot.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionScopedInput)(raw);
    const snapshot = await this.#controller.snapshot({
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    return Schema.encodeSync(edaSessionRpc.snapshot.output)(snapshot);
  }

  async messages(
    raw: typeof SessionScopedInput.Encoded,
  ): Promise<typeof edaSessionRpc.messages.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionScopedInput)(raw);
    const result = await this.#controller.messages({
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    return Schema.encodeSync(edaSessionRpc.messages.output)(result);
  }

  async webSocketMessage(webSocket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    await this.#controller.webSocketMessage(webSocket, message);
  }

  async webSocketClose(
    webSocket: WebSocket,
    _code: number,
    _reason: string,
    _wasClean: boolean,
  ): Promise<void> {
    this.#controller.webSocketClose(webSocket);
  }

  async webSocketError(webSocket: WebSocket, _error: unknown): Promise<void> {
    this.#controller.webSocketError(webSocket);
  }

  async destroySession(
    raw: typeof SessionScopedInput.Encoded,
  ): Promise<typeof edaSessionRpc.destroySession.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionScopedInput)(raw);
    await this.#controller.destroy({
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    await this.ctx.storage.deleteAlarm();
    await this.ctx.storage.deleteAll();
    // The wiped storage is a brand-new log: recreate the tables storage-only entry points read.
    await Effect.runPromise(EDASessionController.migrate(toEDASessionStorage(this.ctx.storage)));
  }

  /**
   * Storage-only backfill entry point: advance pending transcript build work by bounded slices
   * and return the transcript status. It never starts the model runtime, reducers or sinks.
   */
  async transcriptStatus(
    raw: typeof SessionScopedInput.Encoded,
  ): Promise<typeof edaSessionRpc.transcriptStatus.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionScopedInput)(raw);
    const transcript = this.transcriptFor(this.parseSessionId(input.sessionId));
    await transcript.builder.run(transcriptStatusMaxSlices);
    return Schema.encodeSync(edaSessionRpc.transcriptStatus.output)(transcript.store.status());
  }

  /** Runtime-free transcript window read for SSR and CLIs. Hosts authorize the caller. */
  async readTranscriptWindow(
    raw: typeof SessionTranscriptWindowInput.Encoded,
  ): Promise<typeof edaSessionRpc.readTranscriptWindow.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionTranscriptWindowInput)(raw);
    const transcript = this.transcriptFor(this.parseSessionId(input.sessionId));
    const window = transcript.store.readWindow(
      input.selector,
      input.limits ?? defaultTranscriptWindowLimits,
    );
    return Schema.encodeSync(edaSessionRpc.readTranscriptWindow.output)(
      Result.isSuccess(window)
        ? { _tag: "Window", window: window.success }
        : { _tag: "CursorMismatch", message: window.failure.message },
    );
  }

  /**
   * The alarm is shared by the runtime heartbeat and the transcript builder. A builder wakeup
   * runs storage-only slices; the runtime runs only for its own wakeup, or for an alarm neither
   * owner recorded (one armed before wakeups were recorded).
   */
  async alarm(): Promise<void> {
    const due = this.#alarms.takeDue(Date.now());
    const transcript = this.objectTranscript();
    if (due.has("transcript") && transcript !== undefined) {
      try {
        await transcript.builder.run(transcriptAlarmMaxSlices);
      } catch (error) {
        console.error("EDA transcript builder slice failed", error);
        await this.#alarms.request("transcript", Date.now() + 30_000);
      }
    }
    if (due.has("runtime") || !due.has("transcript")) {
      const sessionId = this.sessionIdFromObjectName();
      await this.#controller.alarm(
        sessionId === undefined ? undefined : { sessionId, trace: makeRootEDATraceMetadata() },
      );
    }
    await this.#alarms.sync();
  }

  private transcriptOptions(
    options: EDASessionTranscriptOptions | undefined,
  ): DurableObjectTranscriptOptions {
    const key = options?.adoptIncarnationKey;
    return {
      definitions: options?.definitions ?? {
        current: Effect.runSync(makeTranscriptDefinition(emptyTranscriptMappings)),
      },
      ...(key === undefined ? {} : { adoptIncarnation: { key, kv: this.ctx.storage.kv } }),
      observer: {
        onAlert:
          options?.onAlert ??
          ((alert) => console.error("EDA transcript alert", JSON.stringify(alert))),
        onPendingWork: () => {
          const builder = this.objectTranscript()?.builder;
          if (builder !== undefined) this.ctx.waitUntil(builder.kick());
        },
        ...(options?.onActivated === undefined ? {} : { onActivated: options.onActivated }),
      },
    };
  }

  /** The transcript service of this object's session, created on first use. */
  private transcriptFor(sessionId: SessionId) {
    if (this.#transcript !== undefined) {
      if (this.#transcript.sessionId !== sessionId) {
        throw new Error(
          `EDA transcript is scoped to session ${this.#transcript.sessionId}; received ${sessionId}`,
        );
      }
      return this.#transcript;
    }
    const store = new DurableObjectTranscriptStore({
      sessionId,
      storage: toEDASessionStorage(this.ctx.storage),
      transcript: this.#transcriptOptions,
    });
    const builder = new DurableObjectTranscriptBuilder(store, () =>
      this.#alarms.request("transcript", Date.now()),
    );
    this.#transcript = { sessionId, store, builder };
    return this.#transcript;
  }

  /** The transcript service when the object is named by a valid session id. */
  private objectTranscript() {
    if (this.#transcript !== undefined) return this.#transcript;
    const name = this.ctx.id.name;
    return name !== undefined && Schema.is(SessionId)(name)
      ? this.transcriptFor(SessionId.make(name))
      : undefined;
  }

  private parseSessionId(input: string): SessionId {
    const sessionId = SessionId.make(input);
    const objectName = this.ctx.id.name;
    if (objectName !== undefined && objectName !== sessionId) {
      throw new Error(`EDA Durable Object name ${objectName} cannot serve session ${sessionId}`);
    }
    return sessionId;
  }

  private sessionIdFromObjectName(): SessionId | undefined {
    return this.ctx.id.name === undefined ? undefined : SessionId.make(this.ctx.id.name);
  }
}

const toEDASessionStorage = (storage: DurableObjectStorage): EDASessionDurableObjectStorage => {
  // SAFETY: the adapter interface is the exact SQL, transaction, and alarm subset
  // of DurableObjectStorage; its looser row generic also supports test implementations.
  return storage as EDASessionDurableObjectStorage;
};

const decodeTraceMetadata = (input: unknown): EDATraceMetadata =>
  Schema.decodeUnknownSync(EDATraceMetadata)(input);

const traceMetadataFromRequest = (request: Request): EDATraceMetadata => {
  const parent = parseEDATraceparent(
    request.headers.get("traceparent"),
    request.headers.get("tracestate"),
  );
  return parent === null ? makeRootEDATraceMetadata() : makeEDATraceMetadataFromParent(parent);
};

/** Convenience config constructor for subclasses that need only provider/model ids. */
export const edaRuntimeConfig = (input: {
  readonly maxToolCallsPerRun?: number;
  readonly modelId: string;
  readonly provider: string;
  readonly systemPrompt?: string;
}): EDARuntimeConfig => ({
  modelSelection: { modelId: input.modelId, provider: input.provider },
  ...(input.maxToolCallsPerRun === undefined
    ? {}
    : { maxToolCallsPerRun: input.maxToolCallsPerRun }),
  ...(input.systemPrompt === undefined ? {} : { systemPrompt: input.systemPrompt }),
});

/** Encode committed events into the existing structured-clone envelope. */
export const encodeEdaRpcCommittedDurableEvent = (
  entry: CommittedDurableEvent,
): typeof edaSessionRpc.submit.output.Encoded => ({
  position: entry.position,
  event: encodeEdaRpcDurableEvent(durableEventEnvelope(entry.event)),
});
/** Encode query snapshots without asserting that encoded command values are decoded classes. */
export const encodeEdaRpcSessionSnapshot = Schema.encodeSync(edaSessionRpc.snapshot.output);
