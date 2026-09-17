import * as Effect from "effect/Effect";
import { expectTypeOf, it } from "vite-plus/test";

import {
  EDASink,
  type EDASinkContext,
  type EDASinkDurableBatch,
  type EDASinkRawDurableBatch,
} from "../src/services/sink-registry";

it("preserves default, explicit, raw, and inline sink callback inference", () => {
  const implicit: EDASink = {
    name: "implicit",
    durable: {
      process: (batch, ctx) => {
        expectTypeOf(batch).toEqualTypeOf<EDASinkDurableBatch>();
        expectTypeOf(ctx).toEqualTypeOf<EDASinkContext>();
        return Effect.void;
      },
    },
  };
  const explicit: EDASink = {
    name: "explicit",
    state: "projected",
    durable: {
      process: (batch, ctx) => {
        expectTypeOf(batch).toEqualTypeOf<EDASinkDurableBatch>();
        expectTypeOf(ctx).toEqualTypeOf<EDASinkContext>();
        return Effect.void;
      },
    },
  };
  const raw: EDASink = {
    name: "raw",
    state: "none",
    durable: {
      process: (batch, ctx) => {
        expectTypeOf(batch).toEqualTypeOf<EDASinkRawDurableBatch>();
        expectTypeOf(ctx).toEqualTypeOf<EDASinkContext>();
        // @ts-expect-error Raw consumers cannot depend on a session projection.
        void batch.stateAfter;
        return Effect.void;
      },
    },
  };
  const inline: ReadonlyArray<EDASink> = [
    implicit,
    explicit,
    raw,
    {
      name: "inline",
      durable: {
        process: (batch, ctx) => {
          expectTypeOf(batch).toEqualTypeOf<EDASinkDurableBatch>();
          expectTypeOf(ctx).toEqualTypeOf<EDASinkContext>();
          return Effect.void;
        },
      },
    },
    {
      name: "inline.raw",
      state: "none",
      durable: {
        process: (batch) => {
          expectTypeOf(batch).toEqualTypeOf<EDASinkRawDurableBatch>();
          return Effect.void;
        },
      },
    },
  ];
  const made = EDASink.make({
    name: "made",
    durable: {
      process: (batch, ctx) => {
        expectTypeOf(batch).toEqualTypeOf<EDASinkDurableBatch>();
        expectTypeOf(ctx).toEqualTypeOf<EDASinkContext>();
        return Effect.void;
      },
    },
  });
  const madeRaw = EDASink.make({
    name: "made.raw",
    state: "none",
    durable: {
      process: (batch, ctx) => {
        expectTypeOf(batch).toEqualTypeOf<EDASinkRawDurableBatch>();
        expectTypeOf(ctx).toEqualTypeOf<EDASinkContext>();
        return Effect.void;
      },
    },
  });
  expectTypeOf(made.name).toEqualTypeOf<"made">();
  expectTypeOf(madeRaw.name).toEqualTypeOf<"made.raw">();
  expectTypeOf(inline).toEqualTypeOf<ReadonlyArray<EDASink>>();
});
