import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

/** Host lifecycle lease held while finite EDA work is actively running. */
export interface EDAKeepAliveLease {
  readonly release: () => Promise<void>;
}

/** Host-provided wrapper for keeping finite active work alive. */
export interface EDAKeepAliveShape {
  /**
   * A host may suspend an idle session after ingress returns while model turns,
   * recovery drains, and durable sink catch-up still run in Effect fibers. Wrap
   * those finite active-work regions so the host can retain or schedule the
   * session until work completes, parks durably, fails, or is interrupted.
   * Long-lived fibers blocked on queues should not hold a lease.
   */
  readonly withActiveWork: <A, E, R>(
    label: string,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
}

/** Host keep-alive service for finite active EDA work roots. */
export class EDAKeepAlive extends Context.Service<EDAKeepAlive, EDAKeepAliveShape>()(
  "@effect-durable-agent/EDAKeepAlive",
) {
  static readonly Noop = Layer.succeed(EDAKeepAlive, {
    withActiveWork: (_label, effect) => effect,
  } satisfies EDAKeepAliveShape);

  static readonly FromAcquire = (
    acquire: (label: string) => Promise<EDAKeepAliveLease>,
  ): Layer.Layer<EDAKeepAlive> =>
    Layer.succeed(EDAKeepAlive, {
      withActiveWork: (label, effect) =>
        Effect.acquireUseRelease(
          Effect.promise(() => acquire(label)),
          () => effect,
          (lease) => Effect.promise(() => lease.release()),
        ),
    } satisfies EDAKeepAliveShape);
}
