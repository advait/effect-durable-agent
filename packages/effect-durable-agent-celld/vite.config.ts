import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    deps: {
      neverBundle: [
        /^cloudflare:/,
        /^effect(?:\/|$)/,
        /^effect-durable-agent(?:\/|$)/,
        /^effect-durable-agent-cloudflare(?:\/|$)/,
      ],
    },
    dts: true,
    entry: { index: "src/index.ts" },
    platform: "neutral",
    publint: true,
    sourcemap: true,
    target: "es2022",
  },
});
