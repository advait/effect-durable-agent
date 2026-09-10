import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { LocalRunResolution, type RunSchedulingInput } from "../domain/run-scheduling";

export { LocalRunResolution, type RunSchedulingInput } from "../domain/run-scheduling";

/**
 * Run-start boundary called by SessionState after pure dispatch or recovery policy
 * selects work, before allocating its run ID or committing RunStarted.
 *
 * Resolution must finish promptly with local permission. It must not wait for an
 * external approval: the caller owns the serialized control loop. Deferred scheduling
 * needs a future durable protocol, not a suspended effect behind this interface.
 * Calls may repeat after a restart; command identity is not an exactly-once key.
 */
export interface RunSchedulerShape {
  readonly resolve: (input: RunSchedulingInput) => Effect.Effect<LocalRunResolution>;
}

/** Replaceable run-start integration, independent of persistence and host SDKs. */
export class RunScheduler extends Context.Service<RunScheduler, RunSchedulerShape>()(
  "@effect-durable-agent/RunScheduler",
) {
  /** Preserve local execution without allocating IDs, issuing commands, or writing events. */
  static readonly Immediate = Layer.succeed(RunScheduler, {
    resolve: () => Effect.succeed(LocalRunResolution.make({})),
  });
}
