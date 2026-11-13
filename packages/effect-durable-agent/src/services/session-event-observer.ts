import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import type { ReducedState } from "../domain/reduced-state";
import type { PositionedEvent } from "../types/events";

/**
 * Outbound integration point notified synchronously after EDA publishes an event.
 *
 * Hosts provide this service while composing the runtime. Implementations own
 * their failures and never expose host errors to the session runtime.
 */
export interface SessionEventObserverShape {
  /**
   * `state` is the session's reduced state when `event` is published. A durable batch folds
   * before it publishes, so `state.lastSeq >= event.position.seq`, with equality on the batch's
   * last event. An ephemeral event is published at the head, so `state.lastSeq` equals its seq.
   * State with `lastSeq` equal to a durable event's seq is exactly the fold through that event.
   */
  readonly onEvent: (event: PositionedEvent, state: ReducedState) => Effect.Effect<void>;
}

/** Host-provided observer for append-driven integrations such as hibernatable WebSockets. */
export class SessionEventObserver extends Context.Service<
  SessionEventObserver,
  SessionEventObserverShape
>()("@effect-durable-agent/SessionEventObserver") {
  static readonly Noop = Layer.succeed(SessionEventObserver, {
    onEvent: () => Effect.void,
  });

  static readonly FromHandler = (
    onEvent: SessionEventObserverShape["onEvent"],
  ): Layer.Layer<SessionEventObserver> => Layer.succeed(SessionEventObserver, { onEvent });
}
