import * as Effect from "effect/Effect";
import type * as Tracer from "effect/Tracer";

import type { DurableTranscriptMessage } from "effect-durable-agent/domain/message-transcript";
import type {
  CommittedCommandTerminalEvent,
  EDARuntimeShape,
} from "effect-durable-agent/services/runtime";
import type { EDASessionSnapshot } from "effect-durable-agent/services/session-query";
import type { CommittedDurableEvent } from "effect-durable-agent/services/session-store";
import type { EDASubmittable } from "effect-durable-agent/services/session-state";
import { compactSpanAttributes, toEdaExternalSpan } from "effect-durable-agent/services/tracing";
import type { EDACommand } from "effect-durable-agent/types/commands";
import { CommandId, SequenceNumber, SessionId } from "effect-durable-agent/types/core";
import {
  makeRootEDATraceMetadata,
  type EDATraceMetadata,
} from "effect-durable-agent/types/tracing";
import { EDASessionRuntime, type EDASessionRuntimeOptions } from "./runtime/session-runtime";
import type { EDASessionDurableObjectStorage } from "./runtime/runtime-layer";
import { EDAWebSocketConnectionManager } from "./websocket/connection-manager";
import type { EDAWebSocketServerFrameEncoder } from "effect-durable-agent/websocket";

/** Composition options for the session use cases and Cloudflare adapters. */
export interface EDASessionControllerOptions extends Omit<
  EDASessionRuntimeOptions,
  "sessionEventObserverLayer"
> {
  readonly getWebSockets?: () => ReadonlyArray<WebSocket>;
  readonly webSocketProtocol?: EDAWebSocketServerFrameEncoder;
}

export interface EDASessionSubmitInput {
  readonly command: EDACommand;
  readonly sessionId: SessionId;
  readonly trace?: EDATraceMetadata;
}

export interface EDASessionSubmitBatchInput {
  readonly items: ReadonlyArray<EDASubmittable>;
  readonly sessionId: SessionId;
  readonly trace?: EDATraceMetadata;
}

export interface EDASessionBlockOnCommandInput {
  readonly afterSeq?: SequenceNumber;
  readonly commandId: CommandId;
  readonly sessionId: SessionId;
  readonly trace?: EDATraceMetadata;
}

export interface EDASessionScopedInput {
  readonly sessionId: SessionId;
  readonly trace?: EDATraceMetadata;
}

export interface EDASessionEventsInput {
  readonly afterSeq?: SequenceNumber;
  readonly sessionId: SessionId;
  readonly trace?: EDATraceMetadata;
}

/**
 * Public application-facing use cases for one EDA session.
 *
 * Runtime lifecycle belongs to EDASessionRuntime; WebSocket transport belongs
 * to EDAWebSocketConnectionManager. This class only coordinates them.
 */
export class EDASessionController {
  private readonly runtime: EDASessionRuntime;
  private readonly webSockets: EDAWebSocketConnectionManager;

  constructor(options: EDASessionControllerOptions) {
    this.webSockets = new EDAWebSocketConnectionManager({
      getWebSockets: options.getWebSockets,
      readEventPage: async ({ afterSeq, limit, sessionId, subscriberId, trace }) =>
        await this.runtime.run(sessionId, (eda) =>
          eda.readEventPage(afterSeq, limit).pipe(
            Effect.withSpan(
              "agent.events.page",
              hostSpanOptions(trace, {
                "eda.session.id": sessionId,
                "eda.subscriber.id": subscriberId,
                "eda.seq.after": afterSeq,
              }),
            ),
          ),
        ),
      webSocketProtocol: options.webSocketProtocol,
    });
    this.runtime = new EDASessionRuntime({
      ...options,
      sessionEventObserverLayer: this.webSockets.observerLayer,
    });
  }

  static readonly migrate = EDASessionRuntime.migrate;

  submit(input: EDASessionSubmitInput): Promise<CommittedDurableEvent> {
    const trace = input.trace ?? makeRootEDATraceMetadata();
    return this.runtime.run(input.sessionId, (eda) =>
      eda.submit(input.command).pipe(
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

  submitBatch(input: EDASessionSubmitBatchInput): Promise<ReadonlyArray<CommittedDurableEvent>> {
    const trace = input.trace ?? makeRootEDATraceMetadata();
    return this.runtime.run(input.sessionId, (eda) =>
      eda.submit(input.items).pipe(
        Effect.withSpan(
          "agent.command.submit",
          hostSpanOptions(trace, {
            "eda.session.id": input.sessionId,
            "eda.command.submittable_count": input.items.length,
          }),
        ),
      ),
    );
  }

  submitAndBlock(input: EDASessionSubmitInput): Promise<CommittedCommandTerminalEvent> {
    const trace = input.trace ?? makeRootEDATraceMetadata();
    return this.runtime.run(input.sessionId, (eda) =>
      eda.submitAndBlock(input.command).pipe(
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

  blockOnCommand(input: EDASessionBlockOnCommandInput): Promise<CommittedCommandTerminalEvent> {
    const trace = input.trace ?? makeRootEDATraceMetadata();
    return this.runtime.run(input.sessionId, (eda) =>
      eda.blockOnCommand(input.commandId, input.afterSeq ?? SequenceNumber.make(0)).pipe(
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

  snapshot(input: EDASessionScopedInput): Promise<EDASessionSnapshot> {
    return this.query(input, (eda) => eda.snapshot(), "agent.session.snapshot");
  }

  messages(input: EDASessionScopedInput): Promise<ReadonlyArray<DurableTranscriptMessage>> {
    return this.query(input, (eda) => eda.messages(), "agent.messages.list");
  }

  acceptEventWebSocket(
    input: EDASessionEventsInput & { readonly webSocket: WebSocket },
  ): Promise<void> {
    return this.webSockets.accept({
      afterSeq: input.afterSeq ?? SequenceNumber.make(0),
      sessionId: input.sessionId,
      trace: input.trace ?? makeRootEDATraceMetadata(),
      webSocket: input.webSocket,
    });
  }

  webSocketMessage(webSocket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    return this.webSockets.message(webSocket, message);
  }

  webSocketClose(webSocket: WebSocket): void {
    this.webSockets.closed(webSocket);
  }

  webSocketError(webSocket: WebSocket): void {
    this.webSockets.closed(webSocket);
  }

  async destroy(_input: EDASessionScopedInput): Promise<void> {
    await this.runtime.destroy();
  }

  alarm(input?: EDASessionScopedInput): Promise<void> {
    return this.runtime.alarm(input?.sessionId);
  }

  dispose(): Promise<void> {
    return this.runtime.dispose();
  }

  private query<A>(
    input: EDASessionScopedInput,
    operation: (runtime: EDARuntimeShape) => Effect.Effect<A, unknown>,
    spanName: string,
  ): Promise<A> {
    const trace = input.trace ?? makeRootEDATraceMetadata();
    return this.runtime.run(input.sessionId, (eda) =>
      operation(eda).pipe(
        Effect.withSpan(spanName, hostSpanOptions(trace, { "eda.session.id": input.sessionId })),
      ),
    );
  }
}

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

export type { EDASessionDurableObjectStorage };
