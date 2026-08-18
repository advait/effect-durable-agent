import { describe, expect, expectTypeOf, it } from "vitest";
import * as Schema from "effect/Schema";

import { EDAReducer, defineEDAReducerRegistry } from "./reducer-registry";

const CountState = Schema.Struct({ count: Schema.Number });
const LabelState = Schema.Struct({ label: Schema.String });
const TestReducerState = Schema.Union([
  Schema.Struct({ name: Schema.Literal("test.count"), state: CountState }),
  Schema.Struct({ name: Schema.Literal("test.label"), state: LabelState }),
]);

const countReducer = EDAReducer.make({
  name: "test.count",
  stateSchema: CountState,
  initial: { count: 0 },
  reduce: (state) => state,
});
const labelReducer = EDAReducer.make({
  name: "test.label",
  stateSchema: LabelState,
  initial: { label: "" },
  reduce: (state) => state,
});

const registry = defineEDAReducerRegistry(TestReducerState, {
  "test.count": countReducer,
  "test.label": labelReducer,
});

describe("typed EDA reducer registry", () => {
  it("preserves reducer-name discrimination and state types through serialization", () => {
    const serialized = registry.serialize(
      new Map([
        [countReducer.name, { count: 2 }],
        [labelReducer.name, { label: "ready" }],
      ]),
    );
    const count = serialized.find((entry) => entry.name === countReducer.name);

    expectTypeOf(count?.state.count).toEqualTypeOf<number | undefined>();
    expect(serialized).toEqual([
      { name: "test.count", state: { count: 2 } },
      { name: "test.label", state: { label: "ready" } },
    ]);
  });

  it("validates reducer state at the snapshot boundary", () => {
    expect(() =>
      registry.serialize(
        new Map([
          [countReducer.name, { count: "not-a-number" }],
          [labelReducer.name, { label: "ready" }],
        ]),
      ),
    ).toThrow();
  });
});
