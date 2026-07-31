import { RuleTester } from "oxlint/plugins-dev";
import { describe, it } from "vitest";

import { effectSpanFromCatalog } from "./rules/effect-span-from-catalog.mjs";
import { spanCatalogFormat } from "./rules/span-catalog-format.mjs";

RuleTester.describe = describe;
RuleTester.it = it;

const ruleTester = new RuleTester({
  languageOptions: {
    parserOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
    },
  },
});

const frameworkOptions = [
  {
    catalogs: [["agent.run", "agent.turn", "agent.inference"]],
  },
];

const applicationOptions = [
  {
    catalogs: [["sandbox.command.execute"]],
  },
];

ruleTester.run("effect-span-from-catalog", effectSpanFromCatalog, {
  valid: [
    {
      code: `
        import * as Effect from "effect/Effect";
        const run = Effect.fn("agent.run")(function* () {});
      `,
      options: frameworkOptions,
    },
    {
      code: `
        import * as Fx from "effect/Effect";
        const helper = Fx.fn(function* () {});
        const turn = Fx.fn("agent.turn")(function* () {});
      `,
      options: frameworkOptions,
    },
    {
      code: `
        import { withSpan as traced } from "effect/Effect";
        effect.pipe(traced("agent.inference"));
      `,
      options: frameworkOptions,
    },
    {
      code: `
        import { Effect } from "effect";
        Effect.makeSpanScoped("sandbox.command.execute");
      `,
      options: applicationOptions,
    },
    {
      code: `
        import * as Effect from "effect/Effect";
        Effect.withSpan(effect, "agent.turn");
      `,
      options: frameworkOptions,
    },
  ],
  invalid: [
    {
      code: `
        import * as Effect from "effect/Effect";
        const run = Effect.fn("SessionState.run")(function* () {});
      `,
      errors: [{ messageId: "unknown" }],
      options: frameworkOptions,
    },
    {
      code: `
        import * as Effect from "effect/Effect";
        const name = "agent.run";
        const run = Effect.fn(name)(function* () {});
      `,
      errors: [{ messageId: "dynamic" }],
      options: frameworkOptions,
    },
    {
      code: `
        import * as Effect from "effect/Effect";
        const run = Effect.fn("agent.run")(function* () {});
      `,
      errors: [{ messageId: "unknown" }],
      options: applicationOptions,
    },
    {
      code: `
        import * as Effect from "effect/Effect";
        effect.pipe(Effect.withSpan(\`agent.sink.\${sink.name}\`));
      `,
      errors: [{ messageId: "dynamic" }],
      options: frameworkOptions,
    },
  ],
});

const catalogOptions = [{ catalogs: ["SpanNames"] }];

ruleTester.run("span-catalog-format", spanCatalogFormat, {
  valid: [
    {
      code: `const SpanNames = defineSpanNames(["agent.run", "agent.turn"]);`,
      options: catalogOptions,
    },
  ],
  invalid: [
    {
      code: `const SpanNames = defineSpanNames(["EDAAgent.run"]);`,
      errors: [{ messageId: "format" }],
      options: catalogOptions,
    },
    {
      code: `const SpanNames = defineSpanNames(["agent.run", "agent.run"]);`,
      errors: [{ messageId: "duplicate" }],
      options: catalogOptions,
    },
    {
      code: `
        const SpanNames = defineSpanNames(["agent.run"]);
        const OtherSpanNames = defineSpanNames(["agent.run"]);
      `,
      errors: [{ messageId: "duplicate" }],
      options: [{ catalogs: ["SpanNames", "OtherSpanNames"] }],
    },
    {
      code: `const SpanNames = defineSpanNames([makeName()]);`,
      errors: [{ messageId: "static" }],
      options: catalogOptions,
    },
    {
      code: `const SpanNames = defineSpanNames({ run: "agent.run" });`,
      errors: [{ messageId: "static" }],
      options: catalogOptions,
    },
  ],
});
