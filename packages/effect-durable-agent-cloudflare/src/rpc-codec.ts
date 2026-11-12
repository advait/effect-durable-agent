import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import { EDACommand } from "effect-durable-agent/types/commands";
import type { EDASubmittable } from "effect-durable-agent/services/session-state";
import type { EDADecider, EDADeciderOutcome } from "effect-durable-agent/services/decider";
import type { SequenceNumber } from "effect-durable-agent/types/core";
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

/** Decode a `decide` RPC outcome with the decider's `result` and `rejection` schemas. */
export const decodeDecideOutcome = <Result, Rejection>(
  decider: EDADecider<string, unknown, Result, Rejection>,
  raw:
    | {
        readonly _tag: "Accepted";
        readonly result: unknown;
        readonly acceptedSeq: SequenceNumber;
        readonly replayed: boolean;
      }
    | { readonly _tag: "Rejected"; readonly rejection: unknown },
): Effect.Effect<EDADeciderOutcome<Result, Rejection>, Schema.SchemaError> =>
  raw._tag === "Accepted"
    ? Schema.decodeUnknownEffect(decider.result)(raw.result).pipe(
        Effect.map((result) => ({
          _tag: "Accepted" as const,
          result,
          acceptedSeq: raw.acceptedSeq,
          replayed: raw.replayed,
        })),
      )
    : Schema.decodeUnknownEffect(decider.rejection)(raw.rejection).pipe(
        Effect.map((rejection) => ({ _tag: "Rejected" as const, rejection })),
      );
