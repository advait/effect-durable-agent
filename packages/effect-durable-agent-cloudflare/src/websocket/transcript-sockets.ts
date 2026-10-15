import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import {
  defaultTranscriptSubscriptionPolicy,
  emptyTranscriptActiveContentState,
  foldTranscriptActiveContent,
  planTranscriptUpdate,
  readTranscriptSubscription,
  transcriptActiveContentOf,
  transcriptSubscriptionBaseOf,
  type EDATranscriptStoreShape,
  type TranscriptActiveContentState,
  type TranscriptReadiness,
  type TranscriptSubscriptionPolicy,
  type TranscriptSubscriptionRequest,
  type TranscriptUpdate,
} from "effect-durable-agent/transcript";
import type { SessionId } from "effect-durable-agent/types/core";
import type { PositionedEvent } from "effect-durable-agent/types/events";
import type { EDATraceMetadata } from "effect-durable-agent/types/tracing";
import {
  EDA_WEB_SOCKET_PING_MESSAGE,
  EDA_WEB_SOCKET_PONG_MESSAGE,
  EDA_WEB_SOCKET_PROTOCOL_VERSION,
  EDA_WS_CLOSE_LAGGED,
  EDA_WS_CLOSE_PROTOCOL_ERROR,
  EDA_WS_CLOSE_SEND_FAILED,
  EDA_WS_CLOSE_TRANSCRIPT_RESUBSCRIBE,
  EDATranscriptClientFrame,
  SubscriberId,
  checkpointTranscriptDelivery,
  defaultEDATranscriptDeliveryLimits,
  defaultEDAWebSocketFlowControl,
  edaTranscriptNegotiation,
  encodeEDATranscriptServerFrame,
  encodeEDATranscriptUpdate,
  enqueueTranscriptRequest,
  laggedCloseReason,
  makeTranscriptDeliveryState,
  onTranscriptAck,
  pumpTranscriptTransfer,
  restoreTranscriptDeliveryState,
  startTranscriptUpdate,
  transcriptAckTimedOut,
  transcriptDeliveryCapacity,
  type EDATranscriptDeliveryLimits,
  type EDATranscriptDeliveryState,
  type EDATranscriptRequestFrame,
  type EDAWebSocketFlowControl,
} from "effect-durable-agent/websocket";
import {
  EDATranscriptWebSocketAttachment,
  encodeTranscriptWebSocketAttachment,
} from "./attachment";

/** Tuning for transcript-window sockets; every field has a production default. */
export interface EDATranscriptSocketOptions {
  /** Runtime-free transcript reads for this session. */
  readonly transcriptStore: EDATranscriptStoreShape;
  readonly flowControl?: EDAWebSocketFlowControl;
  readonly subscriptionPolicy?: TranscriptSubscriptionPolicy;
  readonly deliveryLimits?: EDATranscriptDeliveryLimits;
  /**
   * Display cadence for live content: changed active content is published as one complete
   * replacement at most once per interval, never as raw deltas.
   */
  readonly activeContentCadenceMs?: number;
  /**
   * How long a capture may wait for publication to catch up with a commit before it is served
   * without live content. Publication normally follows commit within one append.
   */
  readonly fenceWaitMs?: number;
  /** Timer seam for the cadence; the default uses `setTimeout`. */
  readonly schedule?: (delayMs: number, run: () => void) => void;
  readonly now?: () => number;
}

export const defaultActiveContentCadenceMs = 50;

/** Volatile and persisted state of one transcript socket. */
interface TranscriptSocket {
  readonly sessionId: SessionId;
  readonly subscriberId: SubscriberId;
  readonly trace: EDATraceMetadata;
  delivery: EDATranscriptDeliveryState;
  queue: Promise<void>;
  /** Committed changes (or a wake from hibernation) not yet read for this subscriber. */
  dirty: boolean;
  /** Active-content revision the client last received; unknown after a wake. */
  sentActiveRevision: number | undefined;
  /** What the last full read served, kept to send live-content-only updates without a read. */
  last:
    | {
        readonly readiness: TranscriptReadiness;
        readonly hasOlder: boolean;
        readonly hasNewer: boolean;
        readonly dependencyIds: ReadonlyArray<string>;
      }
    | undefined;
  /** When captures started waiting on the publication fence. */
  fenceWaitingSinceMs: number | undefined;
}

const decodeClientFrame = Schema.decodeUnknownExit(EDATranscriptClientFrame);

/**
 * Owns transcript-window sockets: the window data source beneath the frame encoder.
 *
 * Every subscriber is served strictly in order. Committed changes are coalesced to the complete
 * boundary a store read returns; a request is answered by one read at one cut; and each update is
 * sent from its own captured snapshot before the next is planned.
 *
 * **Publication fence.** Append publication, ephemeral publication and window capture share one
 * fence: the active-content accumulator folds every published event in order, and a capture is
 * served with live content only when the accumulator has folded exactly the window's cut. Durable
 * commit precedes live publication, so a window read between the two would otherwise pair a
 * committed inference with its stale live parts. A capture that would cross the fence waits for
 * publication to catch up.
 */
export class EDATranscriptSockets {
  private readonly sockets = new Map<WebSocket, TranscriptSocket>();
  private active: TranscriptActiveContentState = emptyTranscriptActiveContentState;
  private timerArmed = false;
  private readonly background = new Set<Promise<void>>();
  private readonly flowControl: EDAWebSocketFlowControl;
  private readonly policy: TranscriptSubscriptionPolicy;
  private readonly limits: EDATranscriptDeliveryLimits;
  private readonly cadenceMs: number;
  private readonly fenceWaitMs: number;
  private readonly schedule: (delayMs: number, run: () => void) => void;
  private readonly now: () => number;

  constructor(private readonly options: EDATranscriptSocketOptions) {
    this.flowControl = options.flowControl ?? defaultEDAWebSocketFlowControl;
    this.policy = options.subscriptionPolicy ?? defaultTranscriptSubscriptionPolicy;
    this.limits = options.deliveryLimits ?? defaultEDATranscriptDeliveryLimits;
    this.cadenceMs = options.activeContentCadenceMs ?? defaultActiveContentCadenceMs;
    this.fenceWaitMs = options.fenceWaitMs ?? 1_000;
    this.schedule =
      options.schedule ??
      ((delayMs, run) => {
        setTimeout(run, delayMs);
      });
    this.now = options.now ?? Date.now;
  }

  has(webSocket: WebSocket): boolean {
    return this.sockets.has(webSocket);
  }

  /**
   * Register a freshly accepted socket and greet it. Synchronous so the attachment exists before
   * any platform callback can observe the socket.
   */
  accept(input: {
    readonly sessionId: SessionId;
    readonly trace: EDATraceMetadata;
    readonly webSocket: WebSocket;
  }): void {
    const socket: TranscriptSocket = {
      sessionId: input.sessionId,
      subscriberId: SubscriberId.make(crypto.randomUUID()),
      trace: input.trace,
      delivery: makeTranscriptDeliveryState(this.flowControl),
      queue: Promise.resolve(),
      dirty: false,
      sentActiveRevision: undefined,
      last: undefined,
      fenceWaitingSinceMs: undefined,
    };
    this.sockets.set(input.webSocket, socket);
    if (!this.persist(input.webSocket, socket)) return;
    this.sendText(
      input.webSocket,
      encodeEDATranscriptServerFrame({
        _tag: "hello",
        protocolVersion: EDA_WEB_SOCKET_PROTOCOL_VERSION,
        subscriberId: socket.subscriberId,
        resumeSeq: 0,
        flowControl: this.flowControl,
        negotiation: edaTranscriptNegotiation,
      }),
    );
  }

  /**
   * Adopt a socket that survived hibernation. A socket that was mid-transfer lost its staged
   * chunks with the isolate: it is closed, and the client reconnects and resubscribes.
   */
  restore(webSocket: WebSocket, attachment: EDATranscriptWebSocketAttachment): boolean {
    const restored = restoreTranscriptDeliveryState(this.flowControl, attachment.delivery);
    if (restored._tag === "TransferLost") {
      this.close(webSocket, EDA_WS_CLOSE_TRANSCRIPT_RESUBSCRIBE, "transfer-lost");
      return false;
    }
    this.sockets.set(webSocket, {
      sessionId: attachment.sessionId,
      subscriberId: attachment.subscriberId,
      trace: attachment.trace,
      delivery: restored.state,
      queue: Promise.resolve(),
      // Commits may have happened while it slept; the next opportunity re-reads its coverage.
      dirty: true,
      sentActiveRevision: undefined,
      last: undefined,
      fenceWaitingSinceMs: undefined,
    });
    return true;
  }

  closed(webSocket: WebSocket): void {
    this.sockets.delete(webSocket);
  }

  /**
   * Observe one published event, in publication order. Folds it into the active-content
   * accumulator synchronously, then lets each listed subscriber act on it: a durable event
   * schedules a read at the new boundary; changed live content is coalesced to the display
   * cadence.
   *
   * Reads are scheduled, not awaited, so subscribers never hold the session's append gate. The
   * fence does not need the gate: every capture is checked against the accumulator's folded head
   * when its read resolves.
   */
  onPublished(event: PositionedEvent, webSockets: ReadonlyArray<WebSocket>): void {
    const revision = this.active.revision;
    this.active = foldTranscriptActiveContent(this.active, event);
    if (event.event.durability !== "durable") {
      if (this.active.revision !== revision) this.armCadence();
      return;
    }
    for (const webSocket of webSockets) {
      const socket = this.sockets.get(webSocket);
      if (socket === undefined) continue;
      socket.dirty = true;
      this.track(this.drain(webSocket, socket));
    }
  }

  /**
   * The session's log was destroyed: live content belongs to a log that no longer exists and
   * every subscription names its incarnation. Close transcript sockets so clients resubscribe.
   */
  reset(): void {
    this.active = emptyTranscriptActiveContentState;
    for (const webSocket of [...this.sockets.keys()]) {
      this.close(webSocket, EDA_WS_CLOSE_TRANSCRIPT_RESUBSCRIBE, "session-reset");
    }
  }

  async message(webSocket: WebSocket, message: string): Promise<void> {
    const socket = this.sockets.get(webSocket);
    if (socket === undefined) {
      this.close(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol");
      return;
    }
    if (message === EDA_WEB_SOCKET_PING_MESSAGE) {
      this.sendText(webSocket, EDA_WEB_SOCKET_PONG_MESSAGE);
      return;
    }
    let json: unknown;
    try {
      json = JSON.parse(message);
    } catch {
      this.protocolError(webSocket, "Malformed client frame");
      return;
    }
    const decoded = decodeClientFrame(json);
    if (Exit.isFailure(decoded)) {
      this.protocolError(webSocket, "Unknown client frame");
      return;
    }
    const frame = decoded.value;
    if (frame._tag === "ping") return;
    await this.enqueue(webSocket, socket, async () => {
      if (frame._tag === "ack") {
        const acked = onTranscriptAck(socket.delivery, frame);
        if (acked._tag === "ProtocolError") {
          this.protocolError(webSocket, acked.message);
          return;
        }
        if (acked._tag === "Ignored") return;
        socket.delivery = acked.state;
        if (!this.persist(webSocket, socket)) return;
      } else {
        const queued = enqueueTranscriptRequest(socket.delivery, frame, this.limits);
        if (queued === undefined) {
          this.protocolError(webSocket, "Too many unanswered transcript requests");
          return;
        }
        socket.delivery = queued;
        if (!this.persist(webSocket, socket)) return;
      }
      await this.drainNow(webSocket, socket);
    });
  }

  /** Wait for scheduled reads and cadence flushes; for orderly shutdown and tests. */
  async settled(): Promise<void> {
    while (this.background.size > 0) await Promise.all([...this.background]);
  }

  /**
   * Own scheduled subscriber work until it settles. Drains never reject: `enqueue` contains every
   * failure and closes the socket.
   */
  private track(work: Promise<void>): void {
    const tracked: Promise<void> = work.finally(() => {
      this.background.delete(tracked);
    });
    this.background.add(tracked);
  }

  private armCadence(): void {
    if (this.timerArmed) return;
    this.timerArmed = true;
    this.schedule(this.cadenceMs, () => {
      this.timerArmed = false;
      for (const [webSocket, socket] of this.sockets) this.track(this.drain(webSocket, socket));
    });
  }

  private drain(webSocket: WebSocket, socket: TranscriptSocket): Promise<void> {
    return this.enqueue(webSocket, socket, () => this.drainNow(webSocket, socket));
  }

  /** Send everything this subscriber is owed, one complete update at a time. */
  private async drainNow(webSocket: WebSocket, socket: TranscriptSocket): Promise<void> {
    for (;;) {
      if (this.sockets.get(webSocket) !== socket) return;
      const nowMs = this.now();
      if (transcriptAckTimedOut(socket.delivery, nowMs)) {
        this.sendText(
          webSocket,
          encodeEDATranscriptServerFrame({
            _tag: "lagged",
            resumeSeq: socket.delivery.appliedThroughSeq,
            reason: "ack-timeout",
          }),
        );
        this.close(
          webSocket,
          EDA_WS_CLOSE_LAGGED,
          laggedCloseReason(socket.delivery.appliedThroughSeq),
        );
        return;
      }
      if (socket.delivery.transfer !== undefined) {
        const pumped = pumpTranscriptTransfer(socket.delivery, nowMs);
        if (pumped.frames.length === 0) return;
        socket.delivery = pumped.state;
        if (!this.persistAndSend(webSocket, socket, pumped.frames)) return;
        continue;
      }
      if (transcriptDeliveryCapacity(socket.delivery) <= 0) return;

      const request = socket.delivery.requests[0];
      const base = socket.delivery.subscription;
      if (request === undefined) {
        if (base === undefined) return;
        if (!socket.dirty) {
          if (socket.sentActiveRevision === this.active.revision) return;
          const sent = await this.sendActiveOnly(webSocket, socket);
          if (sent === "NeedsRead") {
            socket.dirty = true;
            continue;
          }
          if (sent === "Failed") return;
          continue;
        }
      }

      const subscriptionRequest =
        request === undefined ? ({ _tag: "Refresh" } as const) : toSubscriptionRequest(request);
      const read = await Effect.runPromiseExit(
        readTranscriptSubscription(
          this.options.transcriptStore,
          base,
          subscriptionRequest,
          this.policy,
        ),
      );
      if (this.sockets.get(webSocket) !== socket) return;
      if (Exit.isFailure(read)) {
        await Effect.runPromise(
          Effect.logError("EDA transcript window read failed", {
            cause: read.cause,
            subscriberId: socket.subscriberId,
          }),
        );
        this.close(webSocket, EDA_WS_CLOSE_SEND_FAILED, "read-failed");
        return;
      }
      const { window, patchBase } = read.value;

      // The fence: read the accumulator synchronously after the store read resolves.
      const active = this.active;
      const fenced =
        active.inference === undefined ||
        window.readiness._tag !== "Ready" ||
        window.throughSeq === active.publishedThroughSeq;
      if (!fenced) {
        socket.fenceWaitingSinceMs ??= nowMs;
        if (nowMs - socket.fenceWaitingSinceMs < this.fenceWaitMs) {
          // Publication of this cut is still in progress and normally triggers another drain;
          // the cadence timer retries anything nothing else would wake.
          this.armCadence();
          return;
        }
      }
      socket.fenceWaitingSinceMs = undefined;

      const update = planTranscriptUpdate({
        patchBase,
        window,
        updateSeq: socket.delivery.nextUpdateSeq,
        activeContent: fenced ? transcriptActiveContentOf(active) : [],
        // Also on a return to Ready: live content was withheld while the cut was degraded.
        includeActiveContent:
          !fenced ||
          socket.sentActiveRevision !== active.revision ||
          (window.readiness._tag === "Ready" &&
            socket.last !== undefined &&
            socket.last.readiness._tag !== "Ready"),
        ...(request?.requestId === undefined ? {} : { requestId: request.requestId }),
      });
      const remaining =
        request === undefined ? socket.delivery.requests : socket.delivery.requests.slice(1);
      if (request === undefined && this.isNoop(socket, update)) {
        socket.dirty = false;
        continue;
      }
      if (
        !(await this.start(
          webSocket,
          socket,
          update,
          transcriptSubscriptionBaseOf(window),
          remaining,
        ))
      ) {
        return;
      }
      socket.dirty = false;
      socket.sentActiveRevision = fenced ? active.revision : undefined;
      socket.last = {
        readiness: window.readiness,
        hasOlder: window.hasOlder,
        hasNewer: window.hasNewer,
        dependencyIds: update.dependencyIds,
      };
    }
  }

  /**
   * Publish changed live content without a store read. Valid only while the accumulator has
   * folded exactly the subscriber's last cut; otherwise a commit is pending and a read is needed.
   */
  private async sendActiveOnly(
    webSocket: WebSocket,
    socket: TranscriptSocket,
  ): Promise<"Sent" | "NeedsRead" | "Failed"> {
    const base = socket.delivery.subscription;
    const last = socket.last;
    const active = this.active;
    // A degraded cut carries no live content; the next Ready read sends it.
    if (last !== undefined && last.readiness._tag !== "Ready") {
      socket.sentActiveRevision = active.revision;
      return "Sent";
    }
    if (
      base === undefined ||
      last === undefined ||
      active.publishedThroughSeq !== base.throughSeq
    ) {
      return "NeedsRead";
    }
    const update: TranscriptUpdate = {
      updateSeq: socket.delivery.nextUpdateSeq,
      mode: "patch",
      incarnation: base.incarnation,
      version: base.version,
      throughSeq: base.throughSeq,
      ...(base.coverage === undefined ? {} : { coverage: base.coverage }),
      hasOlder: last.hasOlder,
      hasNewer: last.hasNewer,
      readiness: last.readiness,
      records: [],
      dependencies: [],
      dependencyIds: last.dependencyIds,
      activeContent: transcriptActiveContentOf(active),
    };
    if (!(await this.start(webSocket, socket, update, base, socket.delivery.requests))) {
      return "Failed";
    }
    socket.sentActiveRevision = active.revision;
    return "Sent";
  }

  /** A refresh that changes nothing the client holds is not sent. */
  private isNoop(socket: TranscriptSocket, update: TranscriptUpdate): boolean {
    const base = socket.delivery.subscription;
    const last = socket.last;
    return (
      base !== undefined &&
      last !== undefined &&
      update.mode === "patch" &&
      update.records.length === 0 &&
      update.dependencies.length === 0 &&
      update.activeContent === undefined &&
      update.throughSeq === base.throughSeq &&
      update.hasOlder === last.hasOlder &&
      update.hasNewer === last.hasNewer &&
      JSON.stringify(update.coverage) === JSON.stringify(base.coverage) &&
      JSON.stringify(update.readiness) === JSON.stringify(last.readiness)
    );
  }

  private async start(
    webSocket: WebSocket,
    socket: TranscriptSocket,
    update: TranscriptUpdate,
    subscription: ReturnType<typeof transcriptSubscriptionBaseOf>,
    requests: ReadonlyArray<EDATranscriptRequestFrame>,
  ): Promise<boolean> {
    const started = startTranscriptUpdate(
      { ...socket.delivery, requests },
      { update, encodedUpdate: encodeEDATranscriptUpdate(update), subscription },
      this.limits,
      this.now(),
    );
    if (started._tag === "TooLarge") {
      await Effect.runPromise(
        Effect.logError("EDA transcript update exceeds the staging bound", {
          bytes: started.bytes,
          subscriberId: socket.subscriberId,
        }),
      );
      this.close(webSocket, EDA_WS_CLOSE_SEND_FAILED, "update-too-large");
      return false;
    }
    socket.delivery = started.state;
    return this.persistAndSend(webSocket, socket, started.frames);
  }

  /** Persist the checkpoint before any frame it accounts for becomes client-visible. */
  private persistAndSend(
    webSocket: WebSocket,
    socket: TranscriptSocket,
    frames: ReadonlyArray<string>,
  ): boolean {
    if (!this.persist(webSocket, socket)) return false;
    for (const frame of frames) {
      if (!this.sendText(webSocket, frame)) return false;
    }
    return true;
  }

  private persist(webSocket: WebSocket, socket: TranscriptSocket): boolean {
    try {
      webSocket.serializeAttachment(
        encodeTranscriptWebSocketAttachment(
          EDATranscriptWebSocketAttachment.make({
            kind: "eda-transcript-v1",
            sessionId: socket.sessionId,
            subscriberId: socket.subscriberId,
            trace: socket.trace,
            delivery: checkpointTranscriptDelivery(socket.delivery),
          }),
        ),
      );
      return true;
    } catch {
      this.close(webSocket, EDA_WS_CLOSE_SEND_FAILED, "attachment-failed");
      return false;
    }
  }

  private sendText(webSocket: WebSocket, text: string): boolean {
    try {
      webSocket.send(text);
      return true;
    } catch {
      this.close(webSocket, EDA_WS_CLOSE_SEND_FAILED, "send-failed");
      return false;
    }
  }

  private protocolError(webSocket: WebSocket, message: string): void {
    this.sendText(
      webSocket,
      encodeEDATranscriptServerFrame({ _tag: "error", message: message.slice(0, 120) }),
    );
    this.close(webSocket, EDA_WS_CLOSE_PROTOCOL_ERROR, "protocol");
  }

  private enqueue(
    webSocket: WebSocket,
    socket: TranscriptSocket,
    operation: () => Promise<void>,
  ): Promise<void> {
    const run = async (): Promise<void> => {
      try {
        await operation();
      } catch (error) {
        await Effect.runPromise(
          Effect.logError("EDA transcript socket operation failed", {
            error,
            subscriberId: socket.subscriberId,
          }),
        );
        this.close(webSocket, EDA_WS_CLOSE_SEND_FAILED, "subscriber-failed");
      }
    };
    const next = socket.queue.then(run);
    socket.queue = next;
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
}

const toSubscriptionRequest = (frame: EDATranscriptRequestFrame): TranscriptSubscriptionRequest => {
  switch (frame._tag) {
    case "subscribe":
      return {
        _tag: "Subscribe",
        tail: frame.tail ?? false,
        ...(frame.range === undefined ? {} : { range: frame.range }),
        ...(frame.since === undefined ? {} : { since: frame.since }),
        ...(frame.count === undefined ? {} : { count: frame.count }),
      };
    case "page":
      return { _tag: "Page", direction: frame.direction, cursor: frame.cursor, count: frame.count };
    case "retain":
      return { _tag: "Retain", from: frame.from, to: frame.to };
  }
};
