import plugin from "@advait/effect-durable-agent/tooling/oxlint";

const expectedRules = ["effect-span-from-catalog", "span-catalog-format"];
for (const rule of expectedRules) {
  if (!(rule in plugin.rules)) {
    throw new Error(`Published Oxlint plugin is missing ${rule}.`);
  }
}
