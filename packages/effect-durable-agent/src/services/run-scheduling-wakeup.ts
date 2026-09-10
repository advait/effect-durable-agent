import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import type { EDASessionStoreError } from "./session-store";

/**
 * Host-owned durable retry obligation for undelivered run authorization requests.
 * setPending(true) must arm a wakeup before returning, survive eviction, and keep
 * retrying until setPending(false). The callback invokes retryRunSchedulingDelivery.
 * Hosts must reconcile this obligation with their active-work alarms, never delete
 * it when an unrelated lease ends, and reconstruct it before recovery releases work.
 */
export interface RunSchedulingWakeupShape {
  readonly setPending: (pending: boolean) => Effect.Effect<void, EDASessionStoreError>;
}

/** Required durable host capability when a scheduler can resolve Deferred. */
export class RunSchedulingWakeup extends Context.Service<
  RunSchedulingWakeup,
  RunSchedulingWakeupShape
>()("@effect-durable-agent/RunSchedulingWakeup") {
  /** Immediate runtimes need no wakeup; fail before persisting unsupported deferral. */
  static readonly Unsupported = Layer.succeed(RunSchedulingWakeup, {
    setPending: (pending) =>
      pending
        ? Effect.die(
            new Error(
              "Deferred run scheduling requires a durable RunSchedulingWakeup host adapter",
            ),
          )
        : Effect.void,
  });
}
