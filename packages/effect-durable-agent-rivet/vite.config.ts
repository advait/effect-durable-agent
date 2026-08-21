import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    deps: {
      neverBundle: [
        /^@effect\/ai-openai(?:\/|$)/,
        /^effect(?:\/|$)/,
        /^effect-durable-agent(?:\/|$)/,
        /^rivetkit(?:\/|$)/,
      ],
    },
    dts: true,
    entry: {
      actor: "src/actor.ts",
      index: "src/index.ts",
      runtime: "src/runtime.ts",
      storage: "src/storage.ts",
      "websocket-protocol": "src/websocket-protocol.ts",
    },
    platform: "neutral",
    publint: true,
    sourcemap: true,
    target: "es2022",
  },
});
