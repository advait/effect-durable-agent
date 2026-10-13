/**
 * Paid end-to-end check of provider caching and native compaction through the real EDA runtime.
 * Run with EDA_LIVE_PROVIDERS=true plus OPENAI_API_KEY and/or ANTHROPIC_API_KEY.
 */
import { CompactionPolicy } from "effect-durable-agent/services/compaction";
import { EDARuntime } from "effect-durable-agent/services/runtime";
import { makeEDAToolkit } from "effect-durable-agent/services/tool-registry";
import { makeEdaTestLayer } from "effect-durable-agent/testkit/layers";
import { SubmitMessageCommand } from "effect-durable-agent/types/commands";
import { CommandId, SequenceNumber, SessionId } from "effect-durable-agent/types/core";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/ai/Prompt";
import * as Tool from "effect/ai/Tool";
import { describe, expect, it } from "vite-plus/test";

import { ANTHROPIC_THINKING_BINDING_BETA, anthropicAgentModelConfig } from "./anthropic";
import {
  makeEDAMultiProviderModelResolverLayer,
  makeEDANativeCompactionExecutorLayer,
} from "./providers";

const live = process.env.EDA_LIVE_PROVIDERS === "true";

const MODELS = [
  { provider: "anthropic", modelId: "claude-opus-5-5", key: "ANTHROPIC_API_KEY" },
  { provider: "anthropic", modelId: "claude-sonnet-5-5", key: "ANTHROPIC_API_KEY" },
  { provider: "openai", modelId: "gpt-6.1-sol", key: "OPENAI_API_KEY" },
] as const;

const WORDS =
  "account session token cache window summary invoice ledger tenant region policy schedule retry backoff cursor shard replica quota budget metric trace span event batch queue worker lease handle buffer stream frame codec schema record field index".split(
    " ",
  );
const TEAMS = ["heron", "otter", "falcon", "lynx", "badger", "marten", "kestrel", "ibis"];
const ownerOf = (n: number) => `team-${TEAMS[(n * 7) % TEAMS.length]}-${100 + n * 13}`;
const random = (seed: number) => () => {
  seed = (seed * 1664525 + 1013904223) >>> 0;
  return seed / 2 ** 32;
};
const pick = (next: () => number) => WORDS[Math.floor(next() * WORDS.length)] ?? "word";

const fileBody = (n: number): string => {
  const next = random(n * 9973);
  const lines = [
    `// src/mod_${String(n).padStart(2, "0")}.ts`,
    `export const OWNER = "${ownerOf(n)}";`,
  ];
  for (let f = 0; f < 34; f++) {
    lines.push(
      `/** Computes the ${pick(next)} ${pick(next)} for a ${pick(next)} ${pick(next)}. */`,
    );
    lines.push(`export function ${pick(next)}${f}(${pick(next)}: Input, n: number): Result {`);
    for (let s = 0; s < 4; s++) {
      lines.push(
        `  const ${pick(next)}${s} = ${pick(next)}.${pick(next)}(n, ${Math.floor(next() * 9000)});`,
      );
    }
    lines.push(`  return { ${pick(next)}: n, ${pick(next)}: ${Math.floor(next() * 100)} };`, "}");
  }
  return lines.join("\n");
};

const SYSTEM_PROMPT = (() => {
  const next = random(42);
  const rules = Array.from(
    { length: 160 },
    (_, i) =>
      `${i + 1}. When the ${pick(next)} ${pick(next)} touches a ${pick(next)}, prefer the ${pick(next)} path.`,
  );
  return [
    "You are an engineering agent auditing TypeScript modules.",
    "Use read_file to read files the user names. Never invent file contents. Keep answers short.",
    ...rules,
  ].join("\n");
})();

const ReadFile = Tool.make("read_file", {
  description: "Read a repository file. Paths look like src/mod_01.ts.",
  parameters: Schema.Struct({ path: Schema.String }),
  success: Schema.String,
});

const pair = (a: number, b: number) =>
  `Read src/mod_${String(a).padStart(2, "0")}.ts and src/mod_${String(b).padStart(2, "0")}.ts and describe each in one sentence, including its OWNER constant.`;
const TURNS = [pair(1, 2), pair(3, 4), pair(5, 6), pair(7, 8), pair(9, 10)];
const RECALL =
  "Without calling tools, list the OWNER constant of every file you have read (mod_01 onward), one per line as `mod_NN: OWNER`.";

const commandId = (index: number) =>
  CommandId.make(`018f6bd5-2f2a-7b1e-8f1b-${String(index).padStart(12, "0")}`);

interface CallRow {
  readonly kind: string;
  readonly input: number;
  readonly cached: number;
  readonly written: number;
  readonly output: number;
}

const ratio = (rows: ReadonlyArray<CallRow>) => {
  const input = rows.reduce((sum, row) => sum + row.input, 0);
  return input === 0 ? 0 : rows.reduce((sum, row) => sum + row.cached, 0) / input;
};

/** With EDA_LIVE_TRACE=true, log each provider request's latency to headers and to body end. */
const traceFetch = () => {
  if (process.env.EDA_LIVE_TRACE !== "true") return;
  const original = globalThis.fetch;
  const traceStart = Date.now();
  let sequence = 0;
  globalThis.fetch = async (input, init) => {
    const id = ++sequence;
    const started = Date.now();
    const body = typeof init?.body === "string" ? init.body : "";
    const parsed: unknown = body.length > 0 ? JSON.parse(body) : undefined;
    const keys = typeof parsed === "object" && parsed !== null ? Object.keys(parsed).join(",") : "";
    console.log(
      `#${id} start t=${((started - traceStart) / 1000).toFixed(1)}s bytes=${body.length} keys=${keys} headers=${JSON.stringify(init?.headers)}`,
    );
    const elapsed = () => `${((Date.now() - started) / 1000).toFixed(1)}s`;
    const response = await original(input, init);
    console.log(`#${id} ${String(input)} ${response.status} headers ${elapsed()}`);
    if (response.body === null) return response;
    const [forCaller, forTimer] = response.body.tee();
    void new Response(forTimer).arrayBuffer().then(
      () => console.log(`#${id} body done ${elapsed()}`),
      (error: unknown) => console.log(`#${id} body failed ${elapsed()} ${String(error)}`),
    );
    return new Response(forCaller, response);
  };
};

describe.skipIf(!live)("live provider caching and native compaction", () => {
  for (const model of MODELS) {
    it.skipIf(process.env[model.key] === undefined)(
      `${model.provider}/${model.modelId} keeps the cache warm across native compaction`,
      async () => {
        traceFetch();
        const toolkit = await Effect.runPromise(
          makeEDAToolkit([ReadFile], {
            read_file: ({ path }) => {
              const match = /mod_(\d+)/u.exec(path);
              return Effect.succeed(
                match === null ? `${path} not found` : fileBody(Number(match[1])),
              );
            },
          }),
        );
        const resolver = makeEDAMultiProviderModelResolverLayer({
          ...(process.env.OPENAI_API_KEY === undefined
            ? {}
            : { openai: { apiKey: process.env.OPENAI_API_KEY } }),
          ...(process.env.ANTHROPIC_API_KEY === undefined
            ? {}
            : {
                anthropic: {
                  apiKey: process.env.ANTHROPIC_API_KEY,
                  betas: [ANTHROPIC_THINKING_BINDING_BETA],
                },
              }),
          resolve: (selection) =>
            selection?.provider === "anthropic"
              ? {
                  provider: "anthropic",
                  modelId: selection.modelId,
                  // "error": any thinking replayed out of its conversation fails the test.
                  config: anthropicAgentModelConfig({
                    effort: "medium",
                    prefixMismatchBehavior: "error",
                  }),
                }
              : {
                  provider: "openai",
                  modelId: selection?.modelId ?? model.modelId,
                  config: { reasoning: { effort: "medium", summary: "auto" }, store: false },
                },
        });
        const layer = EDARuntime.Live({
          modelSelection: {
            provider: model.provider,
            modelId: model.modelId,
            settings: { effort: "medium" },
          },
          systemPrompt: SYSTEM_PROMPT,
        }).pipe(
          Layer.provideMerge(
            makeEdaTestLayer({
              sessionId: SessionId.make(crypto.randomUUID()),
              clock: "live",
              modelResolverLayer: resolver,
              toolkit,
              compactionPolicyLayer: CompactionPolicy.ApproximateTokenThreshold({
                policyId: "live.last-response",
                thresholdTokens: 25_000,
                minSummarizableTokens: 1_000,
                retain: "last-response",
              }),
              compactionExecutorLayer: makeEDANativeCompactionExecutorLayer(),
            }),
          ),
        );

        const program = Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* EDARuntime;
            for (const [index, text] of [...TURNS, RECALL].entries()) {
              yield* runtime.submitAndBlock(
                new SubmitMessageCommand({
                  commandId: commandId(index + 1),
                  disposition: "queue",
                  content: [Prompt.textPart({ text })],
                }),
              );
            }
            const snapshot = yield* runtime.snapshot();
            const replay = yield* runtime.eventsAfter(SequenceNumber.make(0));
            return {
              events: Array.from(
                yield* replay.pipe(Stream.take(snapshot.state.lastSeq), Stream.runCollect),
              ),
              snapshot,
            };
          }),
        ).pipe(Effect.provide(layer));

        const { events } = await Effect.runPromise(program);

        const calls: Array<CallRow> = [];
        let compactions = 0;
        let executors: Array<string> = [];
        let reasoningChars = 0;
        let finalText = "";
        for (const { event } of events) {
          if (event.type === "InferenceFailed" || event.type === "CompactionFailed") {
            throw new Error(
              `${event.type} after ${JSON.stringify(calls)}: ${JSON.stringify(event.payload).slice(0, 800)}`,
            );
          }
          if (event.type === "SummaryCreated") {
            executors = [...executors, event.payload.summary.executorId ?? "unknown"];
          }
          if (event.type === "AssistantMessageCommitted") {
            for (const part of event.payload.promptParts) {
              if (part.type === "reasoning") reasoningChars += part.text.length;
              if (part.type === "text") finalText = part.text;
            }
          }
          const usage =
            event.type === "InferenceCompleted" || event.type === "CompactionCompleted"
              ? event.payload.usage
              : undefined;
          if (usage !== undefined) {
            if (event.type === "CompactionCompleted") compactions++;
            calls.push({
              kind:
                event.type === "CompactionCompleted"
                  ? "compaction"
                  : compactions > 0
                    ? "post"
                    : "pre",
              input: usage.inputTokens ?? 0,
              cached: usage.cachedInputTokens ?? 0,
              written: usage.cacheWriteInputTokens ?? 0,
              output: usage.outputTokens ?? 0,
            });
          }
        }
        const recall = Array.from({ length: 10 }, (_, i) => ownerOf(i + 1)).filter((owner) =>
          finalText.includes(owner),
        ).length;
        console.log(
          JSON.stringify({
            model: `${model.provider}/${model.modelId}`,
            executors,
            recall,
            reasoningChars,
            calls,
          }),
        );

        const compactionCalls = calls.filter((call) => call.kind === "compaction");
        expect(compactionCalls.length).toBeGreaterThanOrEqual(1);
        expect(executors[0]).toBe(`${model.provider}-native`);
        // The compaction call re-sends an earlier request, so it reads the conversation from cache.
        expect(ratio(compactionCalls)).toBeGreaterThan(0.9);
        expect(ratio(calls.filter((call) => call.kind === "pre"))).toBeGreaterThan(0.5);
        expect(ratio(calls.filter((call) => call.kind === "post"))).toBeGreaterThan(0.5);
        expect(recall).toBeGreaterThanOrEqual(8);
      },
      1_500_000,
    );
  }
});
