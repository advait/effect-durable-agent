import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import { EDACommand } from "effect-durable-agent/types/commands";
import type { EDASubmittable } from "effect-durable-agent/services/session-state";
import type { EDADecider, EDADeciderOutcome } from "effect-durable-agent/services/decider";
import { SequenceNumber } from "effect-durable-agent/types/core";
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

/** Command/application-event RPC admission union; controller validation owns admission authorization. */
export const EDARpcSubmittable = Schema.Union([EDACommand, DurableEventEnvelope]);

export const encodeEdaRpcCommand = (input: EDACommand): typeof EDACommand.Encoded =>
  Schema.encodeSync(EDACommand)(input);

export const encodeEdaRpcSubmittables = (
  input: ReadonlyArray<EDASubmittable>,
): ReadonlyArray<typeof EDARpcSubmittable.Encoded> =>
  input.map((item) => Schema.encodeSync(EDARpcSubmittable)(item));

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

/** The `decider` and `input` fields of a `decide` RPC, with `input` encoded by the decider. */
export const encodeDecideInput = <Input>(
  decider: EDADecider<string, Input, unknown, unknown>,
  input: Input,
): { readonly decider: string; readonly input: unknown } => ({
  decider: decider.name,
  input: Schema.encodeUnknownSync(decider.input)(input),
});

/**
 * The `decide` RPC's outcome, its result or rejection encoded with the decider's schemas. Lives
 * here, not in `rpc-spec`, so `decodeDecideOutcome` can decode it without an import cycle.
 */
export const SessionDecideResult = Schema.Union([
  Schema.TaggedStruct("Accepted", {
    result: Schema.Json,
    acceptedSeq: SequenceNumber,
    replayed: Schema.Boolean,
  }),
  Schema.TaggedStruct("Rejected", { rejection: Schema.Json }),
]);
export type SessionDecideResult = typeof SessionDecideResult.Type;

/**
 * Decode what a `decide` RPC returns, as the session stub returns it: the envelope (including
 * `acceptedSeq`), then the result or rejection with the decider's schemas.
 */
export const decodeDecideOutcome = <Result, Rejection>(
  decider: EDADecider<string, unknown, Result, Rejection>,
  raw: typeof SessionDecideResult.Encoded,
): Effect.Effect<EDADeciderOutcome<Result, Rejection>, Schema.SchemaError> =>
  Schema.decodeUnknownEffect(SessionDecideResult)(raw).pipe(
    Effect.flatMap(
      (outcome): Effect.Effect<EDADeciderOutcome<Result, Rejection>, Schema.SchemaError> =>
        outcome._tag === "Accepted"
          ? Schema.decodeUnknownEffect(decider.result)(outcome.result).pipe(
              Effect.map((result) => ({
                _tag: "Accepted" as const,
                result,
                acceptedSeq: outcome.acceptedSeq,
                replayed: outcome.replayed,
              })),
            )
          : Schema.decodeUnknownEffect(decider.rejection)(outcome.rejection).pipe(
              Effect.map((rejection) => ({ _tag: "Rejected" as const, rejection })),
            ),
    ),
  );
