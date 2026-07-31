import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import process from "node:process";

import * as Effect from "effect/Effect";

import { makeOfflineOpenAiLanguageModelLayer } from "../openai-layer";
import { runOfflineTraceScenario } from "../run-scenario";
import {
  makeOfflineTraceScenario,
  offlineTraceScenarioNames,
  type OfflineTraceScenarioName,
} from "../scenarios";
import { writeOfflineTraceArtifacts } from "./artifact-writer";

interface CliOptions {
  readonly scenario: OfflineTraceScenarioName;
  readonly out: string;
  readonly modelId: string;
}

const main = async () => {
  const options = parseArgs(process.argv.slice(2));
  const apiKey = process.env.OPENAI_API_KEY;
  if (apiKey === undefined || apiKey.length === 0) {
    await writeBlocked(options.out, "OPENAI_API_KEY is required for real offline trace runs");
    process.exitCode = 2;
    return;
  }

  const artifacts = await Effect.runPromise(
    Effect.gen(function* () {
      const scenario = yield* makeOfflineTraceScenario(options.scenario, options.modelId);
      return yield* runOfflineTraceScenario({
        scenario,
        modelLayer: makeOfflineOpenAiLanguageModelLayer({
          apiKey,
          modelId: options.modelId,
          ...(process.env.OPENAI_API_URL === undefined
            ? {}
            : { apiUrl: process.env.OPENAI_API_URL }),
        }),
      });
    }),
  );
  await writeOfflineTraceArtifacts(options.out, artifacts);
  console.log(`Wrote EDA offline trace artifacts to ${options.out}`);
};

const parseArgs = (args: ReadonlyArray<string>): CliOptions => {
  const scenario = readOption(args, "--scenario") ?? "no-tools";
  if (!isScenarioName(scenario)) {
    throw new Error(
      `Unknown --scenario ${scenario}. Expected one of: ${offlineTraceScenarioNames.join(", ")}`,
    );
  }
  const modelId = readOption(args, "--model") ?? process.env.EDA_OPENAI_MODEL ?? "gpt-4.1-mini";
  const out =
    readOption(args, "--out") ??
    `.eda-traces/${new Date().toISOString().replaceAll(":", "-")}-${scenario}`;
  return { scenario, modelId, out };
};

const readOption = (args: ReadonlyArray<string>, name: string): string | undefined => {
  const index = args.indexOf(name);
  return index === -1 ? undefined : args[index + 1];
};

const isScenarioName = (value: string): value is OfflineTraceScenarioName =>
  offlineTraceScenarioNames.includes(value as OfflineTraceScenarioName);

const writeBlocked = async (directory: string, reason: string): Promise<void> => {
  await mkdir(directory, { recursive: true });
  const payload = {
    status: "blocked",
    reason,
    requiredInput: "Set OPENAI_API_KEY and rerun the offline trace command.",
  };
  await writeFile(join(directory, "blocked.json"), `${JSON.stringify(payload, null, 2)}\n`);
  await writeFile(
    join(directory, "summary.md"),
    `# EDA offline trace blocked\n\n${reason}\n\nSet OPENAI_API_KEY and rerun the command.\n`,
  );
  console.error(reason);
  console.error(`Wrote blocked report to ${directory}`);
};

await main();
