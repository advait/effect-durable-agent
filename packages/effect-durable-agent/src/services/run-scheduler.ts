import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import {
  DeferredRunResolution,
  LocalRunResolution,
  type RunResolution,
  type RunSchedulingInput,
  type RunSchedulingRequest,
} from "../domain/run-scheduling";
import type { SessionId } from "../types/core";

export {
  DeferredRunResolution,
  LocalRunResolution,
  type RunResolution,
  type RunSchedulingInput,
} from "../domain/run-scheduling";

/** Recoverable failure delivering a persisted request; it never acknowledges that request. */
export class RunSchedulingDeliveryError extends Schema.TaggedErrorClass<RunSchedulingDeliveryError>()(
  "RunSchedulingDeliveryError",
  {
    message: Schema.String,
  },
) {}

/** At-least-once outbound delivery, separate from the serialized run-start decision. */
export type RunSchedulingDelivery = (input: {
  readonly sessionId: SessionId;
  readonly request: RunSchedulingRequest;
}) => Effect.Effect<void, RunSchedulingDeliveryError>;

/**
 * Run-start boundary called by SessionState after pure dispatch or recovery policy
 * selects work, before allocating its run ID or committing RunStarted.
 *
 * Resolution must finish promptly with Local or Deferred intent and must not contact
 * an external scheduler. SessionState persists Deferred intent before calling deliver
 * outside the control loop. A successful delivery must mean the scheduler durably
 * accepted responsibility for the request and will retry its trusted grant callback.
 * Calls may repeat after a restart; command identity is not an exactly-once key.
 */
export interface RunSchedulerShape {
  readonly resolve: (input: RunSchedulingInput) => Effect.Effect<RunResolution>;
  readonly deliver: RunSchedulingDelivery;
}

/** Replaceable run-start integration, independent of persistence and host SDKs. */
export class RunScheduler extends Context.Service<RunScheduler, RunSchedulerShape>()(
  "@effect-durable-agent/RunScheduler",
) {
  /** Preserve local execution without allocating IDs, issuing commands, or writing events. */
  static readonly Immediate = Layer.succeed(RunScheduler, {
    resolve: () => Effect.succeed(LocalRunResolution.make({})),
    deliver: () => Effect.die(new Error("Immediate RunScheduler cannot deliver deferred requests")),
  });

  /** Defer every new run to a durable, idempotent external authorizer. */
  static readonly Deferred = (deliver: RunSchedulingDelivery) =>
    Layer.succeed(RunScheduler, {
      resolve: () => Effect.succeed(DeferredRunResolution.make({})),
      deliver,
    });
}
