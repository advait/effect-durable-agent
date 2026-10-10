import { defineConfig } from "vite-plus";
import { configDefaults } from "vite-plus/test/config";

import { SpanNames } from "./packages/effect-durable-agent/src/services/span-names.ts";

/** Repository-wide formatting and linting policy, including package-specific overrides. */
export default defineConfig({
  fmt: {
    ignorePatterns: [
      "testing/**/*.d.ts",
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
    plugins: ["typescript", "effecttsgo"],
    rules: {
      "typescript/no-explicit-any": "error",
      "typescript/no-non-null-assertion": "error",
      "typescript/no-unsafe-type-assertion": "error",
      "typescript/await-thenable": "off",
      "typescript/no-base-to-string": "off",
      "typescript/no-duplicate-type-constituents": "off",
      "typescript/no-floating-promises": "off",
      "typescript/no-misused-spread": "off",
      "typescript/no-redundant-type-constituents": "off",
      "typescript/unbound-method": "off",
      "typescript/no-useless-default-assignment": "off",
      // Plugin activation promotes this upstream-disabled diagnostic to a warning.
      "effecttsgo/any-unknown-in-error-context": "off",
    },
    overrides: [
      {
        // Retained offline test harness boundary assertions, deferred from this event refactor.
        files: [
          "packages/effect-durable-agent/testing/offline-trace/offline-trace.test.ts",
          "packages/effect-durable-agent/testing/offline-trace/verify/prompt-prefix.ts",
        ],
        rules: { "typescript/no-non-null-assertion": "off" },
      },
      {
        // Retained offline test harness boundary assertions, deferred from this event refactor.
        files: [
          "packages/effect-durable-agent-cloudflare/testing/integration/runtime-real.test.ts",
          "packages/effect-durable-agent/testing/offline-trace/harness/latency.ts",
          "packages/effect-durable-agent/testing/offline-trace/harness/tracing-language-model.ts",
          "packages/effect-durable-agent/testing/offline-trace/json.ts",
          "packages/effect-durable-agent/testing/offline-trace/node/run.ts",
          "packages/effect-durable-agent/testing/offline-trace/offline-trace.test.ts",
          "packages/effect-durable-agent/testing/offline-trace/run-scenario.ts",
          "packages/effect-durable-agent/testing/offline-trace/verify/cache-metrics.ts",
        ],
        rules: { "typescript/no-unsafe-type-assertion": "off" },
      },
      {
        // Existing assertions are deferred outside the typed-event boundary; new files inherit the rule.
        files: [
          "examples/002-slack-bridge/scenario.test.ts",
          "examples/_shared/http.ts",
          "packages/effect-durable-agent-cloudflare/src/durable-object-store.test.ts",
          "packages/effect-durable-agent-cloudflare/src/session-controller.test.ts",
          "packages/effect-durable-agent/src/domain/recovery-policy.ts",
          "packages/effect-durable-agent/src/services/compaction.test.ts",
          "packages/effect-durable-agent/src/services/compaction.ts",
          "packages/effect-durable-agent/src/services/inference-runner.test.ts",
          "packages/effect-durable-agent/src/services/inference-runner.ts",
          "packages/effect-durable-agent/src/services/runtime.ts",
          "packages/effect-durable-agent/src/services/session-query.test.ts",
          "packages/effect-durable-agent/src/services/session-state-control-ingress.test.ts",
          "packages/effect-durable-agent/src/services/session-state.ts",
          "packages/effect-durable-agent/src/services/sink-registry.test.ts",
          "packages/effect-durable-agent/src/services/started-boundary-guard.test.ts",
          "packages/effect-durable-agent/src/testkit/layers.ts",
          "testing/host-conformance/suite.ts",
        ],
        rules: { "typescript/no-non-null-assertion": "off" },
      },
      {
        // Existing assertions are deferred outside the typed-event boundary; new files inherit the rule.
        files: [
          "examples/002-slack-bridge/scenario.test.ts",
          "examples/_shared/http.ts",
          "packages/effect-durable-agent-cloudflare/src/durable-object-sink-checkpoints.test.ts",
          "packages/effect-durable-agent-cloudflare/src/durable-object-store.test.ts",
          "packages/effect-durable-agent-cloudflare/src/durable-object-store.ts",
          "packages/effect-durable-agent-cloudflare/src/durable-object.ts",
          "packages/effect-durable-agent-cloudflare/src/runtime/session-runtime.ts",
          "packages/effect-durable-agent-cloudflare/src/session-controller.test.ts",
          "packages/effect-durable-agent-cloudflare/src/websocket/connection-manager.test.ts",
          "packages/effect-durable-agent/src/domain/dispatch-policy.ts",
          "packages/effect-durable-agent/src/domain/message-transcript.property.test.ts",
          "packages/effect-durable-agent/src/domain/message-transcript.test.ts",
          "packages/effect-durable-agent/src/domain/reduced-state.property.test.ts",
          "packages/effect-durable-agent/src/domain/reduced-state.test.ts",
          "packages/effect-durable-agent/src/domain/reduced-state.ts",
          "packages/effect-durable-agent/src/services/event-factory.ts",
          "packages/effect-durable-agent/src/services/inference-runner.ts",
          "packages/effect-durable-agent/src/services/runtime.test.ts",
          "packages/effect-durable-agent/src/services/runtime.ts",
          "packages/effect-durable-agent/src/services/session-query.test.ts",
          "packages/effect-durable-agent/src/services/session-state-control-completed-turn.test.ts",
          "packages/effect-durable-agent/src/services/session-state-control-interruption.test.ts",
          "packages/effect-durable-agent/src/services/session-state-control-priority.test.ts",
          "packages/effect-durable-agent/src/services/session-state-control-testkit.ts",
          "packages/effect-durable-agent/src/services/session-state.ts",
          "packages/effect-durable-agent/src/services/sink-registry.test.ts",
          "packages/effect-durable-agent/src/services/tool-executor.ts",
          "packages/effect-durable-agent/src/services/tool-registry.test.ts",
          "packages/effect-durable-agent/src/services/tool-registry.ts",
          "packages/effect-durable-agent/src/services/turn-runner.test.ts",
          "packages/effect-durable-agent/src/testkit/layers.ts",
          "testing/host-conformance/suite.ts",
        ],
        rules: { "typescript/no-unsafe-type-assertion": "off" },
      },
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
