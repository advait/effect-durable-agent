# Effect span lint

This Oxlint plugin keeps named Effect spans on an explicit, statically validated catalog while
leaving application code on the native `Effect.fn` and `Effect.withSpan` APIs.

The plugin owns reusable mechanics only. Consumers configure one or more string-array catalogs for
each source tree. Call sites stay on native Effect APIs and use readable literals:

```ts
const runTurn = Effect.fn("agent.turn")(function* () {
  // ...
})

effect.pipe(Effect.withSpan("agent.sink.drain"))
```

```ts
import { SpanNames as EDASpanNames } from "./src/services/span-names.ts"

lint: {
  jsPlugins: ["./tooling/oxlint/index.mjs"],
  overrides: [
    {
      files: ["src/**/*.ts", "examples/**/*.ts", "testing/**/*.ts"],
      rules: {
        "effect-durable-agent/effect-span-from-catalog": [
          "error",
          {
            catalogs: [EDASpanNames],
          },
        ],
      },
    },
  ],
}
```

Use separate overrides for application-owned catalogs. This prevents product-specific span names
from entering the reusable framework catalog and makes the policy portable when the library is
extracted into its own package.

Only exact string literals found in a configured catalog are accepted. Dynamic names,
interpolation, and unknown literals fail lint.
