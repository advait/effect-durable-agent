import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import {
  InferenceId,
  CommandId,
  CompactionId,
  CorrelationId,
  EventId,
  MessageId,
  RunId,
  SessionId,
  SummaryId,
  ToolCallId,
  TurnId,
} from "../types/core";

/** UUIDv7 minting capability for all framework-owned lifecycle identities. */
export interface IdGeneratorShape {
  readonly makeSessionId: () => Effect.Effect<SessionId>;
  readonly makeEventId: () => Effect.Effect<EventId>;
  readonly makeCommandId: () => Effect.Effect<CommandId>;
  readonly makeRunId: () => Effect.Effect<RunId>;
  readonly makeTurnId: () => Effect.Effect<TurnId>;
  readonly makeInferenceId: () => Effect.Effect<InferenceId>;
  readonly makeToolCallId: () => Effect.Effect<ToolCallId>;
  readonly makeMessageId: () => Effect.Effect<MessageId>;
  readonly makeCompactionId: () => Effect.Effect<CompactionId>;
  readonly makeSummaryId: () => Effect.Effect<SummaryId>;
  readonly makeCorrelationId: () => Effect.Effect<CorrelationId>;
}

const WebCryptoLive = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => {
      const bytes = new Uint8Array(size);
      globalThis.crypto.getRandomValues(bytes);
      return bytes;
    },
    digest: () => Effect.die(new Error("IdGenerator does not use Crypto.digest")),
  }),
);

/** Mints framework-owned UUIDv7 lifecycle IDs for effect-durable-agent. */
export class IdGenerator extends Context.Service<IdGenerator, IdGeneratorShape>()(
  "@effect-durable-agent/IdGenerator",
) {
  static readonly Live = Layer.effect(
    IdGenerator,
    Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;
      return makeIdGenerator(crypto.randomUUIDv7.pipe(Effect.orDie));
    }),
  ).pipe(Layer.provide(WebCryptoLive));

  static readonly Deterministic = (ids: ReadonlyArray<string>) =>
    Layer.sync(IdGenerator, () => {
      let index = 0;
      return makeIdGenerator(
        Effect.sync(() => {
          const next = ids[index];
          if (next === undefined) {
            throw new Error("deterministic IdGenerator exhausted");
          }
          index += 1;
          return next;
        }),
      );
    });

  /** Counter-based deterministic IDs; the nth minted ID is `sequentialUuidV7(n)`. */
  static readonly Sequential = Layer.sync(IdGenerator, () => {
    let counter = 0;
    return makeIdGenerator(
      Effect.sync(() => {
        counter += 1;
        return sequentialUuidV7(counter);
      }),
    );
  });
}

/** The UUIDv7-shaped value minted by `IdGenerator.Sequential` for counter `n` (1-based). */
export const sequentialUuidV7 = (n: number): string =>
  `00000000-0000-7000-8000-${n.toString(16).padStart(12, "0")}`;

const makeIdGenerator = (nextUuidV7: Effect.Effect<string>): IdGeneratorShape => ({
  makeSessionId: () => nextUuidV7.pipe(Effect.map((id) => SessionId.make(id))),
  makeEventId: () => nextUuidV7.pipe(Effect.map((id) => EventId.make(id))),
  makeCommandId: () => nextUuidV7.pipe(Effect.map((id) => CommandId.make(id))),
  makeRunId: () => nextUuidV7.pipe(Effect.map((id) => RunId.make(id))),
  makeTurnId: () => nextUuidV7.pipe(Effect.map((id) => TurnId.make(id))),
  makeInferenceId: () => nextUuidV7.pipe(Effect.map((id) => InferenceId.make(id))),
  makeToolCallId: () => nextUuidV7.pipe(Effect.map((id) => ToolCallId.make(id))),
  makeMessageId: () => nextUuidV7.pipe(Effect.map((id) => MessageId.make(id))),
  makeCompactionId: () => nextUuidV7.pipe(Effect.map((id) => CompactionId.make(id))),
  makeSummaryId: () => nextUuidV7.pipe(Effect.map((id) => SummaryId.make(id))),
  makeCorrelationId: () => nextUuidV7.pipe(Effect.map((id) => CorrelationId.make(id))),
});
