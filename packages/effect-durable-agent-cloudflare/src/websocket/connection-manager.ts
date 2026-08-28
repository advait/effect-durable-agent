import * as Effect from "effect/Effect";

import { SessionEventObserver } from "effect-durable-agent/services/session-event-observer";
import type { EDASessionEventPage } from "effect-durable-agent/services/session-query";
import { SequenceNumber, type SessionId } from "effect-durable-agent/types/core";
import type { PositionedEvent } from "effect-durable-agent/types/events";
import type { EDATraceMetadata } from "effect-durable-agent/types/tracing";
import {
  EDA_WEB_SOCKET_PING_MESSAGE,
  EDA_WEB_SOCKET_PONG_MESSAGE,
  EDA_WS_CLOSE_PROTOCOL_ERROR,
  EDA_WS_CLOSE_SEND_FAILED,
  SubscriberId,
  checkpointWebSocketDeliveryState,
  decodeEDAWebSocketClientMessage,
  defaultEDAWebSocketFlowControl,
  deliveryHelloFrame,
  encodeEDAWebSocketServerFrame,
  makeWebSocketDeliveryState,
  onClientAck,
  onDurableEvents,
  onEphemeralEvent,
  restoreWebSocketDeliveryState,
  type EDAWebSocketDeliveryResult,
  type EDAWebSocketDeliveryAction,
  type EDAWebSocketDeliveryState,
  type EDAWebSocketClientFrame,
  type EDAWebSocketServerFrame,
  type EDAWebSocketServerFrameEncoder,
} from "effect-durable-agent/websocket";
import {
  EDAWebSocketAttachment,
  decodeWebSocketAttachment,
  encodeWebSocketAttachment,
} from "./attachment";
import type { EDAWebSocketProjection } from "./projection";

export interface EDAWebSocketConnectionManagerOptions<ProjectionState extends object = never> {
  readonly getWebSockets?: () => ReadonlyArray<WebSocket>;
  readonly isSessionReady: (sessionId: SessionId) => boolean;
  readonly prepareSession: (sessionId: SessionId) => Promise<void>;
  readonly readEventPage: (input: {
    readonly afterSeq: SequenceNumber;
    readonly limit: number;
    readonly sessionId: SessionId;
    readonly subscriberId: SubscriberId;
    readonly trace: EDATraceMetadata;
  }) => Promise<EDASessionEventPage>;
  readonly webSocketProjection?: EDAWebSocketProjection<ProjectionState>;
  readonly webSocketProtocol?: EDAWebSocketServerFrameEncoder;
}

interface EventWebSocketState<ProjectionState extends object> {
  readonly sessionId: SessionId;
  readonly subscriberId: SubscriberId;
  readonly trace: EDATraceMetadata;
  delivery: EDAWebSocketDeliveryState;
  catchUpDeferred: boolean;
  head: SequenceNumber;
  queue: Promise<void>;
  projectionState?: ProjectionState;
}

/**
 * Owns Cloudflare WebSockets, hibernation attachments, and interpretation of
 * the platform-neutral delivery state machine.
 */
export class EDAWebSocketConnectionManager<ProjectionState extends object = never> {
  readonly observerLayer = SessionEventObserver.FromHandler((event) =>
    Effect.tryPromise(() => this.fanoutPublishedEvent(event)).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("EDA WebSocket event observer recovered from a host failure", { cause }),
      ),
    ),
  );

  private readonly sockets = new Map<WebSocket, EventWebSocketState<ProjectionState>>();

  constructor(private readonly options: EDAWebSocketConnectionManagerOptions<ProjectionState>) {}

  resolveProjection(id: string | undefined): EDAWebSocketProjection<ProjectionState> | undefined {
    if (id === undefined) return undefined;
    if (this.options.webSocketProjection?.id === id) return this.options.webSocketProjection;
    return undefined;
  }

  async accept(input: {
    readonly afterSeq: SequenceNumber;
    readonly sessionId: SessionId;
    readonly trace: EDATraceMetadata;
    readonly webSocket: WebSocket;
    readonly projectionState?: ProjectionState;
  }): Promise<void> {
    const subscriberId = SubscriberId.make(crypto.randomUUID());
    const delivery = makeWebSocketDeliveryState({
      subscriberId,
      resumeSeq: input.afterSeq,
      policy: defaultEDAWebSocketFlowControl,
    });
    const state: EventWebSocketState<ProjectionState> = {
      sessionId: input.sessionId,
      subscriberId,
      trace: input.trace,
      delivery,
      catchUpDeferred: false,
      head: input.afterSeq,
      queue: Promise.resolve(),
      ...(input.projectionState === undefined ? {} : { projectionState: input.projectionState }),
    };
    this.sockets.set(input.webSocket, state);
    // The Durable Object calls this immediately after ctx.acceptWebSocket().
    // Register and attach synchronously before the first await so no platform
    // callback can observe an accepted socket without recovery state.
    if (!this.persist(input.webSocket, state)) return;
    if (!(await this.send(input.webSocket, state, [deliveryHelloFrame(delivery)]))) return;
    if (!(await this.prepare(input.webSocket, state))) return;
    await this.enqueue(input.webSocket, state, async () => {
      await this.readAndDeliver(input.webSocket, state, input.afterSeq);
    });
  }

  async message(webSocket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    if (typeof message !== "string") {
      this.close(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "binary");
      return;
    }
    if (message === EDA_WEB_SOCKET_PING_MESSAGE) {
      // Cloudflare auto-response normally intercepts this without waking the
      // object. Keep the fallback independent of attachment recovery for
      // direct controller hosts and unit tests.
      try {
        webSocket.send(EDA_WEB_SOCKET_PONG_MESSAGE);
      } catch {
        this.close(webSocket, EDA_WS_CLOSE_SEND_FAILED, "send-failed");
      }
      return;
    }

    const state = await this.restore(webSocket);
    if (state === undefined) {
      this.close(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol");
      return;
    }
    const frame = await Effect.runPromise(
      this.decodeClientMessage(message, state).pipe(Effect.option),
    );
    if (frame._tag === "None") {
      this.close(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol");
      return;
    }
    const clientFrame = frame.value;
    if (clientFrame._tag === "ping") return;

    if (!(await this.prepare(webSocket, state))) return;
    await this.enqueue(webSocket, state, async () => {
      await this.interpret(webSocket, state, onClientAck(state.delivery, clientFrame, Date.now()));
    });
  }

  closed(webSocket: WebSocket): void {
    this.sockets.delete(webSocket);
  }

  async flushDeferredCatchUp(sessionId: SessionId): Promise<void> {
    const deliveries: Promise<void>[] = [];
    for (const [webSocket, state] of this.sockets) {
      if (state.sessionId !== sessionId || !state.catchUpDeferred) continue;
      deliveries.push(
        this.enqueue(webSocket, state, async () => {
          if (!state.catchUpDeferred) return;
          state.catchUpDeferred = false;
          await this.readAndDeliver(webSocket, state, state.delivery.sentDurableThroughSeq);
        }),
      );
    }
    await Promise.all(deliveries);
  }

  private async fanoutPublishedEvent(event: PositionedEvent): Promise<void> {
    const webSockets = new Set([...(this.options.getWebSockets?.() ?? []), ...this.sockets.keys()]);
    const deliveries: Promise<void>[] = [];
    for (const webSocket of webSockets) {
      const state = await this.restore(webSocket);
      if (state === undefined) continue;
      deliveries.push(
        this.enqueue(webSocket, state, async () => {
          if (event.event.durability !== "durable") {
            await this.interpret(
              webSocket,
              state,
              onEphemeralEvent(state.delivery, event, Date.now()),
            );
            return;
          }
          state.head = SequenceNumber.make(Math.max(state.head, event.position.seq));
          if (event.position.seq <= state.delivery.sentDurableThroughSeq) return;
          if (event.position.seq === state.delivery.sentDurableThroughSeq + 1) {
            await this.interpret(
              webSocket,
              state,
              onDurableEvents(state.delivery, [event], state.head, Date.now()),
            );
            return;
          }
          if (!this.options.isSessionReady(state.sessionId)) {
            state.catchUpDeferred = true;
            return;
          }
          await this.readAndDeliver(webSocket, state, state.delivery.sentDurableThroughSeq);
        }),
      );
    }
    await Promise.all(deliveries);
  }

  private async restore(
    webSocket: WebSocket,
  ): Promise<EventWebSocketState<ProjectionState> | undefined> {
    const existing = this.sockets.get(webSocket);
    if (existing !== undefined) return existing;
    const decoded = await decodeWebSocketAttachment(webSocket.deserializeAttachment());
    if (decoded._tag !== "Decoded") {
      this.close(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol");
      return undefined;
    }
    const attachment = decoded.attachment;
    const projectionState =
      attachment.projection === undefined
        ? undefined
        : await this.decodeProjectionState(attachment.projection.id, attachment.projection.state);
    if (attachment.projection !== undefined && projectionState === undefined) {
      this.close(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol");
      return undefined;
    }
    const state: EventWebSocketState<ProjectionState> = {
      sessionId: attachment.sessionId,
      subscriberId: attachment.subscriberId,
      trace: attachment.trace,
      delivery: restoreWebSocketDeliveryState({
        subscriberId: attachment.subscriberId,
        policy: defaultEDAWebSocketFlowControl,
        checkpoint: attachment.delivery,
      }),
      catchUpDeferred: false,
      head: attachment.delivery.sentDurableThroughSeq,
      queue: Promise.resolve(),
      ...(projectionState === undefined ? {} : { projectionState }),
    };
    this.sockets.set(webSocket, state);
    return state;
  }

  private async prepare(
    webSocket: WebSocket,
    state: EventWebSocketState<ProjectionState>,
  ): Promise<boolean> {
    try {
      await this.options.prepareSession(state.sessionId);
      return this.sockets.get(webSocket) === state;
    } catch (error) {
      await Effect.runPromise(
        Effect.logError("EDA WebSocket session preparation failed", {
          error,
          subscriberId: state.subscriberId,
        }),
      );
      this.close(webSocket, EDA_WS_CLOSE_SEND_FAILED, "subscriber-failed");
      return false;
    }
  }

  private async interpret(
    webSocket: WebSocket,
    state: EventWebSocketState<ProjectionState>,
    result: EDAWebSocketDeliveryResult,
  ): Promise<boolean> {
    state.delivery = result.state;
    for (const action of result.actions) {
      switch (action._tag) {
        case "Persist":
          if (!this.persist(webSocket, state)) return false;
          break;
        case "Send":
          if (!(await this.send(webSocket, state, action.frames))) return false;
          break;
        case "ReadEventPage":
          if (!(await this.readAndDeliver(webSocket, state, action.afterSeq))) return false;
          break;
        case "Close":
          this.close(webSocket, action.code, action.reason);
          return false;
      }
    }
    return true;
  }

  private async readAndDeliver(
    webSocket: WebSocket,
    state: EventWebSocketState<ProjectionState>,
    afterSeq: SequenceNumber,
  ): Promise<boolean> {
    const page = await this.options.readEventPage({
      afterSeq,
      limit: state.delivery.policy.maxInFlightFrames * state.delivery.policy.maxFrameEvents,
      sessionId: state.sessionId,
      subscriberId: state.subscriberId,
      trace: state.trace,
    });
    state.head = SequenceNumber.make(Math.max(state.head, page.head));
    if (page.events.length === 0) return true;
    return await this.interpret(
      webSocket,
      state,
      onDurableEvents(state.delivery, page.events, state.head, Date.now()),
    );
  }

  private persist(webSocket: WebSocket, state: EventWebSocketState<ProjectionState>): boolean {
    try {
      const projection =
        state.projectionState === undefined ? undefined : this.options.webSocketProjection;
      if (state.projectionState !== undefined && projection === undefined) {
        this.close(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol");
        return false;
      }
      const attachment = EDAWebSocketAttachment.make({
        kind: "eda-events-v2",
        sessionId: state.sessionId,
        subscriberId: state.subscriberId,
        trace: state.trace,
        delivery: checkpointWebSocketDeliveryState(state.delivery),
        ...(state.projectionState === undefined || projection === undefined
          ? {}
          : {
              projection: {
                id: projection.id,
                state: projection.encodeState(state.projectionState),
              },
            }),
      });
      webSocket.serializeAttachment(encodeWebSocketAttachment(attachment));
      return true;
    } catch {
      this.close(webSocket, EDA_WS_CLOSE_SEND_FAILED, "attachment-failed");
      return false;
    }
  }

  private send(
    webSocket: WebSocket,
    state: EventWebSocketState<ProjectionState>,
    frames: ReadonlyArray<EDAWebSocketServerFrame>,
  ): Promise<boolean> {
    return this.sendPrepared(webSocket, state, frames);
  }

  private async sendPrepared(
    webSocket: WebSocket,
    state: EventWebSocketState<ProjectionState>,
    frames: ReadonlyArray<EDAWebSocketServerFrame>,
  ): Promise<boolean> {
    try {
      const encodedFrames: string[] = [];
      const followUpActions: EDAWebSocketDeliveryAction[] = [];
      for (const frame of frames) {
        const projected =
          state.projectionState === undefined
            ? undefined
            : this.options.webSocketProjection?.encodeServerFrame(frame, state.projectionState);
        if (state.projectionState !== undefined && projected === undefined) {
          this.close(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol");
          return false;
        }
        if (projected?._tag === "SuppressAndAck") {
          if (frame._tag !== "events") {
            this.close(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol");
            return false;
          }
          state.projectionState = projected.state;
          const acknowledged = onClientAck(
            state.delivery,
            {
              _tag: "ack",
              durableThroughSeq: frame.durableThroughSeq,
              frameId: frame.frameId,
            },
            Date.now(),
          );
          state.delivery = acknowledged.state;
          followUpActions.push(
            ...acknowledged.actions.filter((action) => action._tag !== "Persist"),
          );
          continue;
        }
        if (projected !== undefined) state.projectionState = projected.state;
        const encoded =
          projected?._tag === "Send"
            ? projected.frame
            : encodeEDAWebSocketServerFrame(frame, this.options.webSocketProtocol);
        if (new TextEncoder().encode(encoded).byteLength > state.delivery.policy.maxFrameBytes) {
          this.close(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "frame-too-large");
          return false;
        }
        encodedFrames.push(encoded);
      }
      // Projection encoders may update app-owned state. Persist that state
      // before publishing any frame so eviction cannot roll the projection
      // back behind the client-visible stream.
      if (state.projectionState !== undefined && !this.persist(webSocket, state)) return false;
      for (const encoded of encodedFrames) webSocket.send(encoded);
      if (followUpActions.length > 0) {
        return await this.interpret(webSocket, state, {
          state: state.delivery,
          actions: followUpActions,
        });
      }
      return true;
    } catch {
      this.close(webSocket, EDA_WS_CLOSE_SEND_FAILED, "send-failed");
      return false;
    }
  }

  private enqueue(
    webSocket: WebSocket,
    state: EventWebSocketState<ProjectionState>,
    operation: () => Promise<void>,
  ): Promise<void> {
    const run = async (): Promise<void> => {
      try {
        await operation();
      } catch (error) {
        await Effect.runPromise(
          Effect.logError("EDA WebSocket operation failed", {
            error,
            subscriberId: state.subscriberId,
          }),
        );
        this.close(webSocket, EDA_WS_CLOSE_SEND_FAILED, "subscriber-failed");
      }
    };
    const next = state.queue.then(run);
    state.queue = next;
    return next;
  }

  private close(webSocket: WebSocket, code: number, reason: string): void {
    this.sockets.delete(webSocket);
    try {
      webSocket.close(code, reason);
    } catch {
      // Closing and already-closed sockets can still be returned by getWebSockets().
    }
  }

  private decodeClientMessage(
    message: string,
    state: EventWebSocketState<ProjectionState>,
  ): Effect.Effect<EDAWebSocketClientFrame, unknown> {
    return state.projectionState === undefined
      ? decodeEDAWebSocketClientMessage(message)
      : (this.options.webSocketProjection?.decodeClientMessage(message) ?? Effect.fail("protocol"));
  }

  private async decodeProjectionState(
    id: string,
    encoded: unknown,
  ): Promise<ProjectionState | undefined> {
    const projection = this.resolveProjection(id);
    if (projection === undefined) return undefined;
    const decoded = await Effect.runPromise(projection.decodeState(encoded).pipe(Effect.option));
    return decoded._tag === "Some" ? decoded.value : undefined;
  }
}
