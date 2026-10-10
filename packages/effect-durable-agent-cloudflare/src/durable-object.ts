import type { CommittedDurableEvent } from "effect-durable-agent/services/session-store";
import { durableEventEnvelope } from "effect-durable-agent/types/events/durable";
import { encodeEdaRpcDurableEvent } from "./rpc-codec";
import {
  SessionScopedInput,
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

export type { EDAWebSocketProjection } from "./websocket/projection";
/** Internal Worker-to-object header selecting an app-owned WebSocket projection. */
export const EDA_WEB_SOCKET_PROJECTION_HEADER = "x-eda-websocket-projection";

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
}

/** Constructor options for concrete app subclasses of the EDA Durable Object base. */
export type EDASessionDurableObjectOptions<ProjectionState extends object = never> = Omit<
  EDASessionControllerOptions<ProjectionState>,
  "background" | "getWebSockets" | "storage"
>;

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

  protected constructor(
    ctx: DurableObjectState,
    env: EnvType,
    options: EDASessionDurableObjectOptions<ProjectionState>,
  ) {
    super(ctx, env);
    this.#webSocketProjectionId = options.webSocketProjection?.id;
    const storage = toEDASessionStorage(this.ctx.storage);
    this.#controller = new EDASessionController({
      ...options,
      background: this.ctx,
      getWebSockets: () => this.ctx.getWebSockets(),
      storage,
    });

    // Answer client liveness pings at the runtime layer so idle sockets never
    // wake a hibernated object.
    this.ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair(EDA_WEB_SOCKET_PING_MESSAGE, EDA_WEB_SOCKET_PONG_MESSAGE),
    );

    this.ctx.blockConcurrencyWhile(async () => {
      await Effect.runPromise(EDASessionController.migrate(storage));
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
  }

  async alarm(): Promise<void> {
    const sessionId = this.sessionIdFromObjectName();
    await this.#controller.alarm(
      sessionId === undefined ? undefined : { sessionId, trace: makeRootEDATraceMetadata() },
    );
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
