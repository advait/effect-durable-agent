import { effectSpanFromCatalog } from "./rules/effect-span-from-catalog.mjs";
import { spanCatalogFormat } from "./rules/span-catalog-format.mjs";

/** Deterministic Effect span conventions that travel with effect-durable-agent. */
const plugin = {
  meta: {
    name: "effect-durable-agent",
  },
  rules: {
    "effect-span-from-catalog": effectSpanFromCatalog,
    "span-catalog-format": spanCatalogFormat,
  },
};

export default plugin;
