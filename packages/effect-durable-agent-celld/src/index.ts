import {
  EDASessionDurableObject,
  type EDASessionDurableObjectOptions,
} from "effect-durable-agent-cloudflare";

/**
 * One EDA session hosted as a celld cell.
 *
 * celld implements the Workers Durable Objects contract, so this class shares
 * the production adapter with Cloudflare while keeping deployment choice at a
 * package boundary.
 */
export abstract class EDASessionCell<
  EnvType extends object = object,
> extends EDASessionDurableObject<EnvType> {
  protected constructor(
    ctx: DurableObjectState,
    env: EnvType,
    options: EDASessionDurableObjectOptions,
  ) {
    super(ctx, env, options);
  }
}

/** Resolve a celld session cell by its stable domain session id. */
export const getEDASessionCellByName = <T extends EDASessionCell<object>>(
  namespace: DurableObjectNamespace<T>,
  sessionId: string,
): DurableObjectStub<T> => namespace.getByName(sessionId);

export {
  edaRuntimeConfig,
  encodeEdaRpcCommand,
  encodeEdaRpcSubmittables,
  makeEDADurableObjectOpenAiModelLayer as makeEDACelldOpenAiModelLayer,
  EDASessionDurableObjectHost as EDASessionCellHost,
  type EDASessionDurableObjectOptions as EDASessionCellOptions,
  type EDASessionDurableObjectStorage as EDASessionCellStorage,
} from "effect-durable-agent-cloudflare";
