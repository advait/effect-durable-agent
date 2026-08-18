import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Response from "effect/unstable/ai/Response";
import * as Tool from "effect/unstable/ai/Tool";
import { describe, expect, it } from "vite-plus/test";

import { CommittedDurableEvent } from "../../src/services/session-store";
import { makeLanguageModelLayer } from "../../src/testkit/layers";
import { makeEDAToolkit } from "../../src/services/tool-registry";
import { canonicalPrompt } from "./harness/prompt-hash";
import { makeRunSummary } from "./harness/artifacts";
import { writeOfflineTraceArtifacts } from "./node/artifact-writer";
import { runOfflineTraceScenario } from "./run-scenario";
import { makeOfflineTraceScenario } from "./scenarios";
import { cacheMetricsFromTrace } from "./verify/cache-metrics";
import { replayDurablePrompt } from "./verify/replay";

const usage = (inputTokens = 10, cacheRead = 0) =>
  new Response.Usage({
    inputTokens: { uncached: undefined, total: inputTokens, cacheRead, cacheWrite: undefined },
    outputTokens: { total: 5, text: undefined, reasoning: undefined },
  });

const textFinishStream = (text: string, id = "text-1") =>
  Stream.make(
    Response.makePart("text-delta", { id, delta: text }),
    Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
  );

const eventTypes = (events: ReadonlyArray<unknown>): ReadonlyArray<string> =>
  events.map((entry) => (entry as { readonly event: { readonly type: string } }).event.type);

const toolCompletedLabels = (events: ReadonlyArray<unknown>): ReadonlyArray<string> =>
  events.flatMap((entry) => {
    const event = (
      entry as {
        readonly event: {
          readonly type: string;
          readonly payload?: {
            readonly promptPart?: { readonly result?: { readonly label?: string } };
          };
        };
      }
    ).event;
    return event.type === "ToolCallCompleted" &&
      event.payload?.promptPart?.result?.label !== undefined
      ? [event.payload.promptPart.result.label]
      : [];
  });

describe("offline trace harness", () => {
  it("hashes canonical prompts deterministically", async () => {
    const left = await Effect.runPromise(canonicalPrompt("hello"));
    const right = await Effect.runPromise(canonicalPrompt("hello"));
    const other = await Effect.runPromise(canonicalPrompt("goodbye"));

    expect(left.sha256).toBe(right.sha256);
    expect(left.sha256).not.toBe(other.sha256);
    expect(left.messages).toEqual(right.messages);
  });

  it("runs the no-tools scenario and writes trace artifacts", async () => {
    const scenario = await Effect.runPromise(makeOfflineTraceScenario("no-tools", "fake-model"));
    const artifacts = await Effect.runPromise(
      runOfflineTraceScenario({
        scenario,
        modelLayer: makeLanguageModelLayer(textFinishStream("pong")),
        runId: "test-no-tools",
      }),
    );

    expect(artifacts.summary).toMatchObject({
      scenario: "no-tools",
      status: "completed",
      commandCount: 1,
      modelRequestCount: 1,
    });
    expect(artifacts.trace.map((event) => event.kind)).toEqual(
      expect.arrayContaining(["model.request", "model.part", "model.finish", "durable.event"]),
    );
    expect(JSON.stringify(artifacts.durableEvents)).toContain("CommandCompleted");
    expect(artifacts.prompts).toHaveLength(1);

    const out = await mkdtemp(join(tmpdir(), "eda-offline-trace-"));
    await writeOfflineTraceArtifacts(out, artifacts);
    await expect(readFile(join(out, "run.json"), "utf8")).resolves.toContain("test-no-tools");
    await expect(readFile(join(out, "prompts", "inference-0.sha256"), "utf8")).resolves.toContain(
      artifacts.prompts[0]!.promptHash,
    );
  });

  it("runs an explicit multi-turn scenario and preserves prompt prefixes", async () => {
    const scenario = await Effect.runPromise(makeOfflineTraceScenario("multi-turn", "fake-model"));
    const artifacts = await Effect.runPromise(
      runOfflineTraceScenario({
        scenario,
        modelLayer: makeLanguageModelLayer([
          textFinishStream("I will remember river.", "text-1"),
          textFinishStream("river", "text-2"),
          textFinishStream("river durable agents", "text-3"),
          textFinishStream("We validated multi-turn continuity.", "text-4"),
        ]),
        runId: "test-multi-turn",
      }),
    );

    expect(artifacts.summary).toMatchObject({
      scenario: "multi-turn",
      status: "completed",
      commandCount: 4,
      modelRequestCount: 4,
    });
    expect(artifacts.summary.promptPrefix).toEqual({ checked: 3, failures: [] });
    expect(
      eventTypes(artifacts.durableEvents).filter((type) => type === "CommandCompleted"),
    ).toHaveLength(4);
  });

  it("verifies stable prompt prefixes for the prefix-cache scenario", async () => {
    const scenario = await Effect.runPromise(
      makeOfflineTraceScenario("prefix-cache", "fake-model"),
    );
    const artifacts = await Effect.runPromise(
      runOfflineTraceScenario({
        scenario,
        modelLayer: makeLanguageModelLayer([
          textFinishStream("EDA-PREFIX-CACHE alpha", "text-1"),
          textFinishStream("EDA-PREFIX-CACHE beta", "text-2"),
          textFinishStream("EDA-PREFIX-CACHE gamma", "text-3"),
        ]),
        runId: "test-prefix-cache",
      }),
    );

    expect(artifacts.prompts).toHaveLength(3);
    expect(artifacts.summary.promptPrefix).toEqual({ checked: 2, failures: [] });
    expect(JSON.stringify(artifacts.prompts[0]?.prompt)).toContain("EDA-CACHE-STABLE-0001");
    expect(JSON.stringify(artifacts.prompts[0]?.prompt)).toContain('"role":"system"');
  });

  it("traces framework tool execution and replay introspection", async () => {
    const scenario = await Effect.runPromise(
      makeOfflineTraceScenario("framework-tool", "fake-model"),
    );
    const artifacts = await Effect.runPromise(
      runOfflineTraceScenario({
        scenario,
        modelLayer: makeLanguageModelLayer([
          Stream.make(
            Response.makePart("tool-call", {
              id: "tool-call-1",
              name: "edaEcho",
              params: { text: "framework-ok" },
              providerExecuted: false,
            }),
            Response.makePart("finish", {
              reason: "tool-calls",
              usage: usage(20, 3),
              response: undefined,
            }),
          ),
          textFinishStream("framework-ok", "text-2"),
        ]),
        runId: "test-framework-tool",
      }),
    );

    expect(JSON.stringify(artifacts.durableEvents)).toContain("ToolCallCompleted");
    expect(artifacts.prompts).toHaveLength(2);
    expect(cacheMetricsFromTrace(artifacts.trace)).toMatchObject({
      inputTokens: 30,
      cachedInputTokens: 3,
    });

    const committed = artifacts.durableEvents.map((event) =>
      Schema.decodeUnknownSync(CommittedDurableEvent)(event),
    );
    const replay = replayDurablePrompt(committed);
    expect(replay.messageCount).toBeGreaterThan(0);
    expect(JSON.stringify(replay.prompt)).toContain("framework-ok");
  });

  it("runs multiple tool calls concurrently and preserves model order in continuation prompts", async () => {
    const ParallelLookupParams = Schema.Struct({ label: Schema.String });
    const ParallelLookupTool = Tool.make("edaParallelLookup", {
      parameters: ParallelLookupParams,
      success: Schema.Unknown,
    });
    const artifacts = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const baseScenario = yield* makeOfflineTraceScenario("parallel-tools", "fake-model");
          const slowStarted = yield* Deferred.make<void>();
          const fastStarted = yield* Deferred.make<void>();
          const slowRelease = yield* Deferred.make<void>();
          const fastRelease = yield* Deferred.make<void>();
          const toolkit = yield* makeEDAToolkit([ParallelLookupTool], {
            edaParallelLookup: (params) =>
              Effect.gen(function* () {
                const label = params.label;
                if (label === "slow") {
                  yield* Deferred.succeed(slowStarted, undefined);
                  yield* Deferred.await(slowRelease);
                  return { label, value: "lookup:slow" };
                }
                yield* Deferred.succeed(fastStarted, undefined);
                yield* Deferred.await(fastRelease);
                return { label, value: `lookup:${label}` };
              }),
          });
          const runFiber = yield* runOfflineTraceScenario({
            scenario: { ...baseScenario, toolkit },
            modelLayer: makeLanguageModelLayer([
              Stream.make(
                Response.makePart("tool-call", {
                  id: "tool-call-slow",
                  name: "edaParallelLookup",
                  params: { label: "slow" },
                  providerExecuted: false,
                }),
                Response.makePart("tool-call", {
                  id: "tool-call-fast",
                  name: "edaParallelLookup",
                  params: { label: "fast" },
                  providerExecuted: false,
                }),
                Response.makePart("finish", {
                  reason: "tool-calls",
                  usage: usage(30, 5),
                  response: undefined,
                }),
              ),
              textFinishStream("slow lookup:slow, fast lookup:fast", "text-2"),
            ]),
            runId: "test-parallel-tools",
          }).pipe(Effect.forkScoped);

          yield* Deferred.await(slowStarted).pipe(Effect.timeout("1 second"));
          yield* Deferred.await(fastStarted).pipe(Effect.timeout("1 second"));
          yield* Deferred.succeed(fastRelease, undefined);
          yield* Effect.sleep("10 millis");
          yield* Deferred.succeed(slowRelease, undefined);
          return yield* Fiber.join(runFiber);
        }),
      ),
    );

    const types = eventTypes(artifacts.durableEvents);
    const completedLabels = toolCompletedLabels(artifacts.durableEvents);
    const continuationPrompt = JSON.stringify(artifacts.prompts[1]?.prompt);
    if (continuationPrompt === undefined) {
      throw new Error("Expected parallel tool continuation prompt");
    }

    expect(artifacts.summary).toMatchObject({
      scenario: "parallel-tools",
      status: "completed",
      commandCount: 1,
      modelRequestCount: 2,
    });
    expect(types.filter((type) => type === "ToolCallCreated")).toHaveLength(2);
    expect(types.filter((type) => type === "ToolCallStarted")).toHaveLength(2);
    expect(types.filter((type) => type === "ToolCallCompleted")).toHaveLength(2);
    expect(completedLabels).toEqual(["fast", "slow"]);
    expect(continuationPrompt.indexOf("tool-call-slow")).toBeLessThan(
      continuationPrompt.indexOf("tool-call-fast"),
    );
    expect(continuationPrompt.indexOf("lookup:slow")).toBeLessThan(
      continuationPrompt.indexOf("lookup:fast"),
    );
  });

  it("traces rejected tool correction prompts", async () => {
    const scenario = await Effect.runPromise(
      makeOfflineTraceScenario("rejected-tool", "fake-model"),
    );
    const artifacts = await Effect.runPromise(
      runOfflineTraceScenario({
        scenario,
        modelLayer: makeLanguageModelLayer([
          Stream.make(
            Response.makePart("tool-call", {
              id: "tool-call-1",
              name: "needNumber",
              params: { value: "not-a-number" },
              providerExecuted: false,
            }),
            Response.makePart("finish", {
              reason: "tool-calls",
              usage: usage(),
              response: undefined,
            }),
          ),
          textFinishStream("I corrected the invalid number argument.", "text-2"),
        ]),
        runId: "test-rejected-tool",
      }),
    );

    expect(JSON.stringify(artifacts.durableEvents)).toContain("ToolCallRejected");
    expect(artifacts.prompts).toHaveLength(2);
    expect(JSON.stringify(artifacts.prompts[1]!.prompt)).toContain("invalid-params");
  });

  it("validates run summary artifacts", () => {
    expect(() =>
      makeRunSummary({
        runId: "run",
        scenario: "no-tools",
        status: "completed",
        startedAtMs: 1,
        finishedAtMs: 2,
        commandCount: 1,
        durableEventCount: 1,
        liveEventCount: 1,
        modelRequestCount: 1,
        promptPrefix: { checked: 0, failures: [] },
        cacheMetrics: {},
      }),
    ).not.toThrow();
  });
});
