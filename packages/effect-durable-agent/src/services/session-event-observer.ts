import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import type { PositionedEvent } from "../types/events";

/**
 * Outbound integration point notified synchronously after EDA publishes an event.
 *
 * Hosts provide this service while composing the runtime. Implementations own
 * their failures and never expose host errors to the session runtime.
 */
export interface SessionEventObserverShape {
  readonly onEvent: (event: PositionedEvent) => Effect.Effect<void>;
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
