import { defineConfig } from "vite-plus";
import { configDefaults } from "vite-plus/test/config";

const publicEntries = {
  "services/model-resolver": "src/services/model-resolver.ts",
  "domain/model-usage": "src/domain/model-usage.ts",
  index: "src/index.ts",
  "domain/command-queues": "src/domain/command-queues.ts",
  "domain/context-projection": "src/domain/context-projection.ts",
  "domain/message-transcript": "src/domain/message-transcript.ts",
  "domain/reduced-state": "src/domain/reduced-state.ts",
  "domain/reduced-state-schema": "src/domain/reduced-state-schema.ts",
  websocket: "src/websocket/index.ts",
  "services/compaction": "src/services/compaction.ts",
  "services/id-generator": "src/services/id-generator.ts",
  "services/keep-alive": "src/services/keep-alive.ts",
  "services/prompt-projector": "src/services/prompt-projector.ts",
  "services/reducer-registry": "src/services/reducer-registry.ts",
  "services/runtime": "src/services/runtime.ts",
  "services/runtime-layer": "src/services/runtime-layer.ts",
  "services/session-query": "src/services/session-query.ts",
  "services/session-event-observer": "src/services/session-event-observer.ts",
  "services/session-store": "src/services/session-store.ts",
  "services/session-state": "src/services/session-state.ts",
  "services/sink-checkpoint-store": "src/services/sink-checkpoint-store.ts",
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

export default defineConfig({
  pack: {
    deps: {
      neverBundle: [/^effect(?:\/|$)/],
    },
    dts: true,
    entry: publicEntries,
    platform: "neutral",
    publint: true,
    sourcemap: true,
    target: "es2022",
  },
  test: {
    exclude: [...configDefaults.exclude],
  },
});
