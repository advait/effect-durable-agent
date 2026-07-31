import { defineConfig } from "vite-plus";
import { configDefaults } from "vite-plus/test/config";

import { SpanNames } from "./src/services/span-names.ts";

const publicEntries = {
  index: "src/index.ts",
  "domain/command-queues": "src/domain/command-queues.ts",
  "domain/message-transcript": "src/domain/message-transcript.ts",
  "domain/reduced-state": "src/domain/reduced-state.ts",
  "domain/reduced-state-schema": "src/domain/reduced-state-schema.ts",
  "host/durable-object": "src/host/durable-object.ts",
  "host/durable-object-runtime": "src/host/durable-object-runtime.ts",
  "host/durable-object-storage": "src/host/durable-object-storage.ts",
  "host/websocket-wire": "src/host/websocket-wire.ts",
  "services/compaction": "src/services/compaction.ts",
  "services/id-generator": "src/services/id-generator.ts",
  "services/prompt-projector": "src/services/prompt-projector.ts",
  "services/reducer-registry": "src/services/reducer-registry.ts",
  "services/runtime": "src/services/runtime.ts",
  "services/session-query": "src/services/session-query.ts",
  "services/session-store": "src/services/session-store.ts",
  "services/sink-registry": "src/services/sink-registry.ts",
  "services/span-names": "src/services/span-names.ts",
  "services/tool-registry": "src/services/tool-registry.ts",
  "services/tracing": "src/services/tracing.ts",
  "testkit/layers": "src/testkit/layers.ts",
  "types/commands": "src/types/commands.ts",
  "types/core": "src/types/core.ts",
  "types/events": "src/types/events.ts",
  "types/events/durable": "src/types/events/durable.ts",
  "types/tracing": "src/types/tracing.ts",
};

const authoredFileIgnorePatterns = [
  ".artifacts/**",
  "dist/**",
  "node_modules/**",
  "pnpm-lock.yaml",
  "**/*.md",
  "**/*.tsbuildinfo",
];

export default defineConfig({
  pack: {
    deps: {
      neverBundle: [/^@effect\/ai-openai(?:\/|$)/, /^cloudflare:/, /^effect(?:\/|$)/],
    },
    dts: true,
    entry: publicEntries,
    platform: "neutral",
    publint: true,
    sourcemap: true,
    target: "es2022",
  },
  fmt: {
    ignorePatterns: authoredFileIgnorePatterns,
    semi: true,
    singleQuote: false,
    sortPackageJson: false,
    tabWidth: 2,
    useTabs: false,
  },
  lint: {
    ignorePatterns: authoredFileIgnorePatterns,
    jsPlugins: ["./tooling/oxlint/index.mjs"],
    overrides: [
      {
        files: ["src/**/*.ts", "examples/**/*.ts", "testing/**/*.ts"],
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
        files: ["src/services/span-names.ts"],
        rules: {
          "effect-durable-agent/span-catalog-format": ["error", { catalogs: ["SpanNames"] }],
        },
      },
    ],
  },
  test: {
    exclude: [...configDefaults.exclude],
  },
});
