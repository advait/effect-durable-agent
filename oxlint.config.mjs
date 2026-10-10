import config from "./vite.config.ts";

// Vite Plus embeds its own linter; Effect patches the standalone pinned Oxlint.
export default config.lint;
