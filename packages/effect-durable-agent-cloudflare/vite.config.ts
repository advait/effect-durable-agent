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
      openai: "src/providers/openai.ts",
      rpc: "src/rpc-codec.ts",
      "session-controller": "src/session-controller.ts",
      storage: "src/durable-object-storage.ts",
    },
    platform: "neutral",
    publint: true,
    sourcemap: true,
    target: "es2022",
  },
});
