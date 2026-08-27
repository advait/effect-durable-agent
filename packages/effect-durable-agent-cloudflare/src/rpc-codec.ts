import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import { EDACommand } from "effect-durable-agent/types/commands";
import type { EDASubmittable } from "effect-durable-agent/services/session-state";
import {
  DurableEventEnvelope,
  EDADurableEvent,
  effectDurableAgentNamespace,
} from "effect-durable-agent/types/events";

/** Encode one durable event into a structured-clone-safe Durable Object RPC value. */
export const encodeEdaRpcDurableEvent = (event: DurableEventEnvelope): DurableEventEnvelope => {
  const encoded =
    event.namespace === effectDurableAgentNamespace
      ? Schema.encodeSync(EDADurableEvent)(Schema.decodeUnknownSync(EDADurableEvent)(event))
      : Schema.encodeSync(DurableEventEnvelope)(event);
  return Schema.decodeUnknownSync(DurableEventEnvelope)(encoded);
};

const EDARpcSubmittable = Schema.Union([EDACommand, DurableEventEnvelope]);

export const encodeEdaRpcCommand = (input: EDACommand): unknown =>
  Schema.encodeSync(EDACommand)(input);

export const encodeEdaRpcSubmittables = (
  input: ReadonlyArray<EDASubmittable>,
): ReadonlyArray<unknown> => input.map((item) => Schema.encodeSync(EDARpcSubmittable)(item));

export const decodeEdaRpcCommand = (
  input: unknown,
): Effect.Effect<EDACommand, Schema.SchemaError> =>
  Schema.decodeUnknownExit(EDACommand)(input).pipe(
    Exit.match({ onFailure: Effect.failCause, onSuccess: Effect.succeed }),
  );

export const decodeEdaRpcSubmittables = (
  input: ReadonlyArray<unknown>,
): Effect.Effect<ReadonlyArray<EDASubmittable>, Schema.SchemaError> =>
  Effect.forEach(input, (item) =>
    Schema.decodeUnknownExit(EDARpcSubmittable)(item).pipe(
      Exit.match({ onFailure: Effect.failCause, onSuccess: Effect.succeed }),
    ),
  );
