import { defineConfig } from "vite-plus";
import { configDefaults } from "vite-plus/test/config";

import { SpanNames } from "./packages/effect-durable-agent/src/services/span-names.ts";

/** Repository-wide formatting and linting policy, including package-specific overrides. */
export default defineConfig({
  fmt: {
    ignorePatterns: [
      ".artifacts/**",
      "**/dist/**",
      "**/node_modules/**",
      "pnpm-lock.yaml",
      "**/*.md",
      "**/*.tsbuildinfo",
      "**/worker-configuration.d.ts",
    ],
    semi: true,
    singleQuote: false,
    sortPackageJson: false,
    tabWidth: 2,
    useTabs: false,
  },
  lint: {
    ignorePatterns: [
      ".artifacts/**",
      "**/dist/**",
      "**/node_modules/**",
      "pnpm-lock.yaml",
      "**/*.md",
      "**/*.tsbuildinfo",
      "**/worker-configuration.d.ts",
    ],
    jsPlugins: ["./packages/effect-durable-agent/tooling/oxlint/index.mjs"],
    options: { typeAware: true },
    // @ts-expect-error @effect/tsgo patches in this plugin, which Oxlint's generated plugin-name union omits: https://github.com/Effect-TS/tsgo/blob/main/docs/README.md
    plugins: ["effecttsgo"],
    rules: {
      // Plugin activation promotes this upstream-disabled diagnostic to a warning.
      "effecttsgo/any-unknown-in-error-context": "off",
    },
    overrides: [
      {
        // These generic test adapters intentionally erase channels at their test-layer boundary.
        files: [
          "packages/effect-durable-agent/src/services/session-state-control-recovery-partial.test.ts",
          "packages/effect-durable-agent/src/services/tool-executor.test.ts",
          "packages/effect-durable-agent-cloudflare/src/durable-object-store.test.ts",
        ],
        rules: {
          "effecttsgo/missing-effect-context": "off",
          "effecttsgo/missing-effect-error": "off",
        },
      },
      {
        files: [
          "packages/effect-durable-agent/src/**/*.ts",
          "packages/effect-durable-agent/testing/**/*.ts",
        ],
        rules: {
          "effect-durable-agent/effect-span-from-catalog": [
            "error",
            {
              catalogs: [SpanNames],
            },
          ],
        },
      },
      {
        files: ["packages/effect-durable-agent/src/services/span-names.ts"],
        rules: {
          "effect-durable-agent/span-catalog-format": ["error", { catalogs: ["SpanNames"] }],
        },
      },
    ],
  },
  test: { exclude: [...configDefaults.exclude] },
});
