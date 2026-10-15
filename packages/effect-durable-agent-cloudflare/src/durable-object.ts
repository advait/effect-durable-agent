import type { CommittedDurableEvent } from "effect-durable-agent/services/session-store";
import { durableEventEnvelope } from "effect-durable-agent/types/events/durable";
import { encodeEdaRpcDurableEvent } from "./rpc-codec";
import {
  SessionScopedInput,
  SessionTranscriptBodyInput,
  SessionTranscriptSummaryInput,
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
  resolveTranscriptBody,
  type EDATranscriptStoreShape,
  type TranscriptLogIncarnation,
  type TranscriptMaintenanceConfig,
} from "effect-durable-agent/transcript";
import { runEDASessionAlarm } from "./durable-object-alarm-handler";
import { DurableObjectAlarmScheduler } from "./durable-object-alarms";
import { readDurableObjectCommittedEvent, readDurableObjectSummary } from "./durable-object-store";
import type { EDATranscriptSocketOptions } from "./websocket/transcript-sockets";
import { EDASessionStoreError } from "effect-durable-agent/services/session-store";
import {
  DurableObjectTranscriptBuilder,
  DurableObjectTranscriptStore,
} from "./durable-object-transcript";
import {
  defaultTranscriptSliceLimits,
  resolveTranscriptSliceLimits,
  type DurableObjectTranscriptObserver,
  type DurableObjectTranscriptOptions,
  type DurableObjectTranscriptSliceLimits,
  type TranscriptAlert,
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
  readonly destroySession: (
    input: EDASessionScopedRpcInput,
  ) => Promise<typeof edaSessionRpc.destroySession.output.Encoded>;
  readonly transcriptStatus: (
    input: EDASessionScopedRpcInput,
  ) => Promise<typeof edaSessionRpc.transcriptStatus.output.Encoded>;
  readonly readTranscriptWindow: (
    input: typeof SessionTranscriptWindowInput.Encoded,
  ) => Promise<typeof edaSessionRpc.readTranscriptWindow.output.Encoded>;
  readonly readTranscriptBody: (
    input: typeof SessionTranscriptBodyInput.Encoded,
  ) => Promise<typeof edaSessionRpc.readTranscriptBody.output.Encoded>;
  readonly readTranscriptSummary: (
    input: typeof SessionTranscriptSummaryInput.Encoded,
  ) => Promise<typeof edaSessionRpc.readTranscriptSummary.output.Encoded>;
}

/** Host transcript configuration of a session Durable Object. */
export interface EDASessionTranscriptOptions {
  /**
   * Which sessions maintain a transcript; default **off**. A disabled session gets no transcript
   * writes, builder slices or wakeups: `readTranscriptWindow` answers `Disabled`,
   * `transcriptStatus` reports `Disabled` without writing, and transcript sockets are
   * refused. Enabling a session takes the first-introduction path (a build from the log, then
   * activation). Rollout rings use a deterministic predicate (for example a hash of the session
   * id under a percentage); disabling leaves a session's rows untouched, and re-enabling catches
   * an active version up to the head before it is served as current. Evaluated once per object
   * instance, so a change takes effect when the object next starts.
   */
  readonly enabled?: boolean | ((sessionId: SessionId) => boolean);
  /**
   * Builder bounds: events per slice, rows deleted per slice, and `maxSliceFailures` (default 5),
   * the consecutive failures after which a build freezes as failed and a failing builder alarm
   * stops scheduling itself.
   */
  readonly sliceLimits?: Partial<DurableObjectTranscriptSliceLimits>;
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
  "alarmStorage" | "background" | "getWebSockets" | "storage" | "transcript" | "transcriptSockets"
> & {
  readonly transcript?: EDASessionTranscriptOptions;
  /**
   * Serve transcript-window sockets (`?stream=transcript`). `transcriptStore` defaults to this
   * object's own runtime-free Durable Object transcript store.
   */
  readonly transcriptSockets?: Omit<EDATranscriptSocketOptions, "transcriptStore"> & {
    readonly transcriptStore?: EDATranscriptSocketOptions["transcriptStore"];
  };
};

/** Slices a backfill `transcriptStatus` call may run before it returns. */
const transcriptStatusMaxSlices = 16;
/** Server bounds on one `readTranscriptWindow` RPC, whatever limits the caller asks for. */
const transcriptWindowMaxRecords = 500;
const transcriptWindowMaxBytes = 8 * 1_048_576;
/** First retry delay after a failed builder alarm; doubled per consecutive failure. */
const transcriptBuilderRetryDelayMs = 30_000;
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
        /** Whether the host enabled transcript maintenance for this session. */
        readonly enabled: boolean;
        readonly store: DurableObjectTranscriptStore;
        readonly builder: DurableObjectTranscriptBuilder;
      }
    | undefined;
  readonly #transcriptEnabled: (sessionId: SessionId) => boolean;

  protected constructor(
    ctx: DurableObjectState,
    env: EnvType,
    options: EDASessionDurableObjectOptions<ProjectionState>,
  ) {
    super(ctx, env);
    this.#webSocketProjectionId = options.webSocketProjection?.id;
    const storage = toEDASessionStorage(this.ctx.storage);
    this.#alarms = new DurableObjectAlarmScheduler(storage);
    const { transcript: transcriptHostOptions, transcriptSockets, ...controllerOptions } = options;
    const transcript = this.transcriptOptions(transcriptHostOptions);
    this.#transcriptOptions = transcript;
    const enabled = transcriptHostOptions?.enabled ?? false;
    this.#transcriptEnabled = typeof enabled === "function" ? enabled : () => enabled;
    this.#controller = new EDASessionController({
      ...controllerOptions,
      alarmStorage: this.#alarms.owner("runtime"),
      background: this.ctx,
      getWebSockets: () => this.ctx.getWebSockets(),
      storage,
      // The runtime's store maintains the transcript only for an enabled session.
      transcript: ({ sessionId }) =>
        this.transcriptFor(sessionId).enabled ? transcript : undefined,
      ...(transcriptSockets === undefined
        ? {}
        : {
            transcriptSockets: {
              ...transcriptSockets,
              transcriptStore: transcriptSockets.transcriptStore ?? this.transcriptPort(),
            },
          }),
    });

    // Answer client liveness pings at the runtime layer so idle sockets never
    // wake a hibernated object.
    this.ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair(EDA_WEB_SOCKET_PING_MESSAGE, EDA_WEB_SOCKET_PONG_MESSAGE),
    );

    this.ctx.blockConcurrencyWhile(async () => {
      await Effect.runPromise(EDASessionController.migrate(storage));
      // Keep an alarm armed by an earlier release for the runtime, then resume any builder
      // work an earlier instance left (or a new release created).
      await this.#alarms.adoptUnrecordedAlarm();
      await this.maintainedTranscript()?.builder.start();
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

    const sessionId = this.parseSessionIdOrUndefined(sessionIdRaw);
    if (sessionId === undefined) {
      return new Response("This object does not serve that session.", { status: 400 });
    }
    const trace = traceMetadataFromRequest(request);

    if (url.searchParams.get(EDA_WEB_SOCKET_STREAM_PARAM) === "transcript") {
      if (projectionId !== undefined || !this.#controller.supportsTranscriptWebSocket) {
        return new Response("Unsupported transcript stream.", { status: 400 });
      }
      if (!this.transcriptFor(sessionId).enabled) {
        return new Response("Transcript is not enabled for this session.", { status: 409 });
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
    // The wiped storage is a brand-new log: recreate the tables storage-only entry points read,
    // synchronously, so no other work observes the object without them.
    Effect.runSync(EDASessionController.migrate(toEDASessionStorage(this.ctx.storage)));
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
    if (!transcript.enabled) {
      return Schema.encodeSync(edaSessionRpc.transcriptStatus.output)(
        transcript.store.disabledStatus(),
      );
    }
    await transcript.builder.run(transcriptStatusMaxSlices);
    return Schema.encodeSync(edaSessionRpc.transcriptStatus.output)(transcript.store.status());
  }

  /** Runtime-free transcript window read for SSR and CLIs. Hosts authorize the caller. */
  async readTranscriptWindow(
    raw: typeof SessionTranscriptWindowInput.Encoded,
  ): Promise<typeof edaSessionRpc.readTranscriptWindow.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionTranscriptWindowInput)(raw);
    const transcript = this.transcriptFor(this.parseSessionId(input.sessionId));
    if (!transcript.enabled) {
      return Schema.encodeSync(edaSessionRpc.readTranscriptWindow.output)({ _tag: "Disabled" });
    }
    const limits = input.limits ?? defaultTranscriptWindowLimits;
    const window = transcript.store.readWindow(input.selector, {
      ...limits,
      maxRecords: Math.min(limits.maxRecords, transcriptWindowMaxRecords),
      maxBytes: Math.min(limits.maxBytes, transcriptWindowMaxBytes),
    });
    return Schema.encodeSync(edaSessionRpc.readTranscriptWindow.output)(
      Result.isSuccess(window)
        ? { _tag: "Window", window: window.success }
        : { _tag: "CursorMismatch", message: window.failure.message },
    );
  }

  /**
   * Runtime-free read of the body a transcript `Ref` names, resolved through the immutable log
   * (so it stays valid after projection rows are rebuilt). Hosts authorize the caller.
   */
  async readTranscriptBody(
    raw: typeof SessionTranscriptBodyInput.Encoded,
  ): Promise<typeof edaSessionRpc.readTranscriptBody.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionTranscriptBodyInput)(raw);
    const scope = this.transcriptReadScope(input.sessionId, input.incarnation);
    if (scope._tag !== "Scope") {
      return Schema.encodeSync(edaSessionRpc.readTranscriptBody.output)(scope);
    }
    const sessionId = scope.sessionId;
    const committed = readDurableObjectCommittedEvent(
      toEDASessionStorage(this.ctx.storage).sql,
      sessionId,
      input.source.seq,
    );
    const resolved =
      committed === undefined
        ? ({ _tag: "Missing", message: `no event at seq ${input.source.seq}` } as const)
        : resolveTranscriptBody(committed.event, input.source.path);
    return Schema.encodeSync(edaSessionRpc.readTranscriptBody.output)(
      resolved._tag === "Found"
        ? { _tag: "Body", value: resolved.value }
        : { _tag: "NotFound", message: resolved.message },
    );
  }

  /** Runtime-free read of a compaction summary's text. Hosts authorize the caller. */
  async readTranscriptSummary(
    raw: typeof SessionTranscriptSummaryInput.Encoded,
  ): Promise<typeof edaSessionRpc.readTranscriptSummary.output.Encoded> {
    const input = Schema.decodeUnknownSync(SessionTranscriptSummaryInput)(raw);
    const scope = this.transcriptReadScope(input.sessionId, input.incarnation);
    if (scope._tag !== "Scope") {
      return Schema.encodeSync(edaSessionRpc.readTranscriptSummary.output)(scope);
    }
    const artifact = readDurableObjectSummary(
      toEDASessionStorage(this.ctx.storage).sql,
      input.summaryId,
    );
    return Schema.encodeSync(edaSessionRpc.readTranscriptSummary.output)(
      artifact === undefined
        ? { _tag: "NotFound", message: `no summary ${input.summaryId}` }
        : {
            _tag: "Summary",
            summary: {
              compactionId: artifact.compactionId,
              summaryId: artifact.summaryId,
              text: artifact.text,
              sourceFromSeq: artifact.sourceFromSeq,
              sourceToSeq: artifact.sourceToSeq,
            },
          },
    );
  }

  /**
   * The alarm is shared by the runtime heartbeat and the transcript builder; see
   * `runEDASessionAlarm`.
   */
  async alarm(): Promise<void> {
    const transcript = this.maintainedTranscript();
    await runEDASessionAlarm({
      alarms: this.#alarms,
      now: () => Date.now(),
      runRuntime: async () => {
        const sessionId = this.sessionIdFromObjectName();
        await this.#controller.alarm(
          sessionId === undefined ? undefined : { sessionId, trace: makeRootEDATraceMetadata() },
        );
      },
      runBuilderSlices:
        transcript === undefined
          ? undefined
          : () => transcript.builder.runSlices(transcriptAlarmMaxSlices),
      maxBuilderFailures:
        this.#transcriptOptions.sliceLimits?.maxSliceFailures ??
        defaultTranscriptSliceLimits.maxSliceFailures,
      builderRetryDelayMs: transcriptBuilderRetryDelayMs,
      onBuilderFailure: ({ error, consecutiveFailures, stopped }) => {
        console.error("EDA transcript builder slice failed", error);
        if (stopped) {
          transcript?.store.alert({
            _tag: "BuilderStopped",
            consecutiveFailures,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      },
    });
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
      // Validated here so a misconfigured host fails at construction, not on a failing slice.
      sliceLimits: resolveTranscriptSliceLimits(options?.sliceLimits),
      observer: {
        onAlert:
          options?.onAlert ??
          ((alert) => console.error("EDA transcript alert", JSON.stringify(alert))),
        onPendingWork: () => {
          const builder = this.maintainedTranscript()?.builder;
          if (builder !== undefined) this.ctx.waitUntil(builder.kick());
        },
        onServedChanged: () => {
          // A behind active version reached the head, or the active version froze: sockets re-read.
          this.ctx.waitUntil(this.#controller.refreshTranscriptWebSockets());
        },
        onActivated: (activation) => {
          // Connected transcript sockets get one complete replacement in the new version.
          this.ctx.waitUntil(this.#controller.refreshTranscriptWebSockets());
          options?.onActivated?.(activation);
        },
      },
    };
  }

  /** This object's transcript store as the port transcript sockets read, resolved per call. */
  private transcriptPort(): EDATranscriptStoreShape {
    const port = () => this.maintainedTranscript()?.store.port;
    const unbound = () =>
      Effect.fail(
        new EDASessionStoreError({
          message:
            this.objectTranscript()?.enabled === false
              ? "EDA transcript is not enabled for this session"
              : "EDA transcript read before the session was named",
        }),
      );
    return {
      readTranscriptWindow: (selector, limits) =>
        Effect.suspend(() => port()?.readTranscriptWindow(selector, limits) ?? unbound()),
      readTranscriptMeta: Effect.suspend(() => port()?.readTranscriptMeta ?? unbound()),
    };
  }

  /**
   * The session and log a body or summary read may serve: pinned to the session this object's
   * transcript serves, like window reads, and to the incarnation the caller's reference names.
   */
  private transcriptReadScope(
    rawSessionId: string,
    incarnation: TranscriptLogIncarnation,
  ):
    | { readonly _tag: "Scope"; readonly sessionId: SessionId }
    | { readonly _tag: "Disabled" }
    | { readonly _tag: "IncarnationMismatch"; readonly incarnation: TranscriptLogIncarnation } {
    const transcript = this.transcriptFor(this.parseSessionId(rawSessionId));
    // Outside the rollout nothing is initialized or minted, and no reference was ever served.
    if (!transcript.enabled) return { _tag: "Disabled" };
    const current = transcript.store.incarnation();
    return incarnation === current
      ? { _tag: "Scope", sessionId: transcript.sessionId }
      : { _tag: "IncarnationMismatch", incarnation: current };
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
    this.#transcript = { sessionId, enabled: this.#transcriptEnabled(sessionId), store, builder };
    return this.#transcript;
  }

  /** The transcript service when this object's session is enabled for maintenance. */
  private maintainedTranscript() {
    const transcript = this.objectTranscript();
    return transcript?.enabled === true ? transcript : undefined;
  }

  /** The transcript service when the object is named by a valid session id. */
  private objectTranscript() {
    if (this.#transcript !== undefined) return this.#transcript;
    const name = this.ctx.id.name;
    return name !== undefined && Schema.is(SessionId)(name)
      ? this.transcriptFor(SessionId.make(name))
      : undefined;
  }

  /** `parseSessionId` for request paths that answer a foreign session with a 400. */
  private parseSessionIdOrUndefined(input: string): SessionId | undefined {
    if (!Schema.is(SessionId)(input)) return undefined;
    const name = this.ctx.id.name;
    if (name !== undefined && name !== input) return undefined;
    const bound = this.#transcript?.sessionId;
    return bound === undefined || bound === input ? SessionId.make(input) : undefined;
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
