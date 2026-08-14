import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

import type { OfflineTraceArtifacts } from "../harness/artifacts";
import { stableJsonStringify } from "../json";

/** Write offline trace artifacts to a directory for inspection and replay. */
export const writeOfflineTraceArtifacts = async (
  directory: string,
  artifacts: OfflineTraceArtifacts,
): Promise<void> => {
  await mkdir(join(directory, "prompts"), { recursive: true });
  await writeFile(join(directory, "run.json"), `${stableJsonStringify(artifacts.summary)}\n`);
  await writeFile(join(directory, "summary.md"), summaryMarkdown(artifacts));
  await writeJsonl(join(directory, "trace.jsonl"), artifacts.trace);
  await writeJsonl(join(directory, "durable-events.jsonl"), artifacts.durableEvents);
  await writeJsonl(join(directory, "live-events.jsonl"), artifacts.liveEvents);
  for (const prompt of artifacts.prompts) {
    await writeFile(
      join(directory, "prompts", `inference-${prompt.index}.json`),
      `${stableJsonStringify(prompt.prompt)}\n`,
    );
    await writeFile(
      join(directory, "prompts", `inference-${prompt.index}.sha256`),
      `${prompt.promptHash}\n`,
    );
  }
};

const writeJsonl = async (path: string, rows: ReadonlyArray<unknown>): Promise<void> => {
  await writeFile(path, rows.map((row) => stableJsonStringify(row)).join("\n") + "\n");
};

const summaryMarkdown = (
  artifacts: OfflineTraceArtifacts,
): string => `# EDA offline trace: ${artifacts.summary.scenario}

- Run ID: ${artifacts.summary.runId}
- Status: ${artifacts.summary.status}
- Commands: ${artifacts.summary.commandCount}
- Model requests: ${artifacts.summary.modelRequestCount}
- Durable events: ${artifacts.summary.durableEventCount}
- Live events: ${artifacts.summary.liveEventCount}
- Prompt prefix checks: ${artifacts.summary.promptPrefix.checked}
- Prompt prefix failures: ${artifacts.summary.promptPrefix.failures.length}
- Input tokens: ${artifacts.summary.cacheMetrics.inputTokens ?? "n/a"}
- Cached input tokens: ${artifacts.summary.cacheMetrics.cachedInputTokens ?? "n/a"}
`;
