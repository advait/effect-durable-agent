import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    deps: {
      neverBundle: [
        /^@effect\/ai-openai(?:\/|$)/,
        /^cloudflare:/,
        /^effect(?:\/|$)/,
        /^effect-durable-agent(?:\/|$)/,
      ],
    },
    dts: true,
    entry: {
      index: "src/index.ts",
      "durable-object": "src/durable-object.ts",
      "durable-object-runtime": "src/durable-object-runtime.ts",
      "durable-object-storage": "src/durable-object-storage.ts",
      "websocket-protocol": "src/websocket-protocol.ts",
    },
    platform: "neutral",
    publint: true,
    sourcemap: true,
    target: "es2022",
  },
});
