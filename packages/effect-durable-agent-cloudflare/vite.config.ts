import { fileURLToPath } from "node:url";
import { defineConfig } from "vite-plus";

export default defineConfig({
  test: {
    alias: { vitest: fileURLToPath(import.meta.resolve("vite-plus/test")) },
    server: { deps: { inline: ["@effect/vitest"] } },
  },
  pack: {
    deps: {
      neverBundle: [
        /^@effect\/ai-anthropic(?:\/|$)/,
        /^@effect\/ai-openai(?:\/|$)/,
        /^cloudflare:/,
        /^effect(?:\/|$)/,
        /^effect-durable-agent(?:\/|$)/,
      ],
    },
    dts: true,
    entry: {
      anthropic: "src/providers/anthropic.ts",
      index: "src/index.ts",
      "durable-object": "src/durable-object.ts",
      openai: "src/providers/openai.ts",
      providers: "src/providers/providers.ts",
      rpc: "src/rpc.ts",
      "session-controller": "src/session-controller.ts",
      storage: "src/durable-object-storage.ts",
    },
    platform: "neutral",
    publint: true,
    sourcemap: true,
    target: "es2022",
  },
});
