import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { EDADecider, type EDADeciderOutcome } from "effect-durable-agent/services/decider";
import { DurableEventEnvelope, schemaV1 } from "effect-durable-agent/types/events";

import type { EDASessionRpcSurface } from "./durable-object";
import { decodeDecideOutcome, encodeDecideInput } from "./rpc-codec";

const Routed = Schema.Struct({
  ...DurableEventEnvelope.fields,
  namespace: Schema.Literal("docs.chat"),
  type: Schema.Literal("MessageRouted"),
  payload: Schema.Struct({ to: Schema.Literals(["ai", "human"]) }),
});

const Input = Schema.Struct({ messageId: Schema.String });
const Result = Schema.Struct({ to: Schema.Literals(["ai", "human"]) });
const Rejection = Schema.Struct({ reason: Schema.Literal("ended") });

const route = EDADecider.define({
  name: "docs.chat.PostMessage",
  input: Input,
  events: Routed,
  result: Result,
  rejection: Rejection,
  idempotencyKey: (input) => input.messageId,
  decide: (_view, _input) => ({
    _tag: "Accept",
    items: [
      {
        namespace: "docs.chat",
        type: "MessageRouted",
        schemaVersion: schemaV1,
        payload: { to: "ai" as const },
      },
    ],
    result: { to: "ai" as const },
  }),
});

EDADecider.define({
  name: "docs.chat.Forger",
  input: Input,
  events: Routed,
  result: Result,
  rejection: Rejection,
  idempotencyKey: (input) => input.messageId,
  decide: (_view, _input) => ({
    _tag: "Accept",
    items: [
      // @ts-expect-error A draft outside the decider's `events` schema does not type-check.
      { namespace: "docs.chat", type: "ChatEnded", schemaVersion: schemaV1, payload: {} },
    ],
    result: { to: "ai" as const },
  }),
});

EDADecider.define({
  name: "docs.chat.BadPayload",
  input: Input,
  events: Routed,
  result: Result,
  rejection: Rejection,
  idempotencyKey: (input) => input.messageId,
  decide: (_view, _input) => ({
    _tag: "Accept",
    items: [
      {
        namespace: "docs.chat",
        type: "MessageRouted",
        schemaVersion: schemaV1,
        // @ts-expect-error A draft payload must match the decider's `events` schema.
        payload: { to: "bot" },
      },
    ],
    result: { to: "ai" as const },
  }),
});

// @ts-expect-error The input must be the decider's input type.
encodeDecideInput(route, { messageId: 1 });
const encoded: { readonly decider: string; readonly input: unknown } = encodeDecideInput(route, {
  messageId: "m1",
});
void encoded;

// A caller passes what the session stub returns, as is.
declare const stub: DurableObjectStub<Rpc.DurableObjectBranded & EDASessionRpcSurface>;
declare const scoped: Omit<Parameters<EDASessionRpcSurface["decide"]>[0], "decider" | "input">;
export const decideOverRpc = async () => {
  const raw = await stub.decide({ ...scoped, ...encodeDecideInput(route, { messageId: "m1" }) });
  const outcome: Effect.Effect<
    EDADeciderOutcome<{ readonly to: "ai" | "human" }, { readonly reason: "ended" }>,
    Schema.SchemaError
  > = decodeDecideOutcome(route, raw);

  const unrelated = Effect.map(outcome, (decided) =>
    decided._tag === "Accepted" ? decided.result.to : decided.rejection.reason,
  );
  // @ts-expect-error The decoded result keeps the declared types, not `unknown`.
  const wrong: Effect.Effect<number, Schema.SchemaError> = unrelated;
  return wrong;
};
