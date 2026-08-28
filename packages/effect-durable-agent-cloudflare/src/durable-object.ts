import { DurableObject } from "cloudflare:workers";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { DurableTranscriptMessage } from "effect-durable-agent/domain/message-transcript";
import {
  decodeEdaRpcCommand,
  decodeEdaRpcSubmittables,
  encodeEdaRpcDurableEvent,
} from "./rpc-codec";
import type { EDASessionSnapshot } from "effect-durable-agent/services/session-query";
import type { CommittedDurableEvent as CommittedDurableEventValue } from "effect-durable-agent/services/session-store";
import type {
  CommittedCommandTerminalEvent,
  EDARuntimeConfig,
} from "effect-durable-agent/services/runtime";
import {
  EDACommand,
  type EDACommand as EDACommandValue,
} from "effect-durable-agent/types/commands";
import type { EDASubmittable } from "effect-durable-agent/services/session-state";
import { CommandId, SequenceNumber, SessionId } from "effect-durable-agent/types/core";
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

/** Raw RPC shape decoded before admitting one session command. */
export interface EDASessionCommandRpcInput {
  readonly command: unknown;
  readonly sessionId: string;
  readonly trace: unknown;
}

/** Raw RPC shape decoded before admitting a durable command/app-event batch. */
export interface EDASessionSubmitBatchRpcInput {
  readonly items: ReadonlyArray<unknown>;
  readonly sessionId: string;
  readonly trace: unknown;
}

/** Raw RPC shape for read/destroy operations that only need a session id. */
export interface EDASessionScopedRpcInput {
  readonly sessionId: string;
  readonly trace: unknown;
}

/** Raw RPC shape for WebSocket event-stream upgrades with a resume cursor. */
export interface EDASessionEventsRpcInput {
  readonly afterSeq?: number;
  readonly sessionId: string;
  readonly trace: unknown;
}

/** Raw RPC shape decoded before blocking on one command terminal event. */
export interface EDASessionBlockOnCommandRpcInput {
  readonly afterSeq?: number;
  readonly commandId: string;
  readonly sessionId: string;
  readonly trace: unknown;
}

/** Constructor options for concrete app subclasses of the EDA Durable Object base. */
export type EDASessionDurableObjectOptions<ProjectionState extends object = never> = Omit<
  EDASessionControllerOptions<ProjectionState>,
  "background" | "getWebSockets" | "storage"
>;

/** Resolve a concrete EDA session Durable Object binding by domain session id. */
export const getEDASessionDurableObjectByName = <T extends EDASessionDurableObject<object>>(
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

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.ctx.acceptWebSocket(server);
    await this.#controller.acceptEventWebSocket({
      ...(afterSeq === undefined ? {} : { afterSeq: SequenceNumber.make(afterSeq) }),
      sessionId: this.parseSessionId(sessionIdRaw),
      trace: traceMetadataFromRequest(request),
      webSocket: server,
      ...(projectionId === undefined ? {} : { projectionId }),
    });
    return new Response(null, { status: 101, webSocket: client });
  }

  async submit(input: EDASessionCommandRpcInput): Promise<CommittedDurableEventValue> {
    const committed = await this.#controller.submit({
      command: await decodeCommand(input.command),
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    return encodeEdaRpcCommittedDurableEvent(committed);
  }

  async submitBatch(
    input: EDASessionSubmitBatchRpcInput,
  ): Promise<ReadonlyArray<CommittedDurableEventValue>> {
    const committed = await this.#controller.submitBatch({
      items: await decodeSubmittables(input.items),
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    return committed.map(encodeEdaRpcCommittedDurableEvent);
  }

  async submitAndBlock(input: EDASessionCommandRpcInput): Promise<CommittedCommandTerminalEvent> {
    const committed = await this.#controller.submitAndBlock({
      command: await decodeCommand(input.command),
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    return encodeEdaRpcCommittedDurableEvent(committed) as CommittedCommandTerminalEvent;
  }

  async blockOnCommand(
    input: EDASessionBlockOnCommandRpcInput,
  ): Promise<CommittedCommandTerminalEvent> {
    const committed = await this.#controller.blockOnCommand({
      ...(input.afterSeq === undefined ? {} : { afterSeq: SequenceNumber.make(input.afterSeq) }),
      commandId: CommandId.make(input.commandId),
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    return encodeEdaRpcCommittedDurableEvent(committed) as CommittedCommandTerminalEvent;
  }

  async snapshot(input: EDASessionScopedRpcInput): Promise<EDASessionSnapshot> {
    const snapshot = await this.#controller.snapshot({
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
    return encodeEdaRpcSessionSnapshot(snapshot);
  }

  async messages(
    input: EDASessionScopedRpcInput,
  ): Promise<ReadonlyArray<DurableTranscriptMessage>> {
    return await this.#controller.messages({
      sessionId: this.parseSessionId(input.sessionId),
      trace: decodeTraceMetadata(input.trace),
    });
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

  async destroySession(input: EDASessionScopedRpcInput): Promise<void> {
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

/** Encode committed events into structured-clone-safe Durable Object RPC payloads. */
export const encodeEdaRpcCommittedDurableEvent = (
  event: CommittedDurableEventValue,
): CommittedDurableEventValue => ({
  position: event.position,
  event: encodeEdaRpcDurableEvent(event.event),
});

/** Encode runtime snapshots into structured-clone-safe Durable Object RPC payloads. */
export const encodeEdaRpcSessionSnapshot = (snapshot: EDASessionSnapshot): EDASessionSnapshot => ({
  ...snapshot,
  reducerStates: new Map(snapshot.reducerStates),
  state: {
    ...snapshot.state,
    commands: new Map(
      Array.from(snapshot.state.commands, ([commandId, record]) => [
        commandId,
        encodeCommandCarrierForRpc(record),
      ]),
    ),
    commandQueues: {
      ...snapshot.state.commandQueues,
      activeControlCommands: snapshot.state.commandQueues.activeControlCommands.map(
        encodeCommandCarrierForRpc,
      ),
      pendingCommands: snapshot.state.commandQueues.pendingCommands.map(encodeCommandCarrierForRpc),
      queuedCommands: snapshot.state.commandQueues.queuedCommands.map(encodeCommandCarrierForRpc),
    },
  },
});

const encodeCommandCarrierForRpc = <A extends { readonly command?: EDACommandValue }>(
  value: A,
): A =>
  value.command === undefined
    ? value
    : {
        ...value,
        command: Schema.encodeSync(EDACommand)(value.command) as EDACommandValue,
      };

const decodeCommand = (input: unknown): Promise<EDACommandValue> =>
  Effect.runPromise(decodeEdaRpcCommand(input));

const decodeSubmittables = (
  input: ReadonlyArray<unknown>,
): Promise<ReadonlyArray<EDASubmittable>> => Effect.runPromise(decodeEdaRpcSubmittables(input));
