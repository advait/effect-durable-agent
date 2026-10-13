import {
  CompactionExecutor,
  CompactionPlan,
  type CompactionExecutorInput,
} from "effect-durable-agent/services/compaction";
import { ModelResolver } from "effect-durable-agent/services/model-resolver";
import {
  getNativeCompaction,
  nativeCompactionSummaryMessage,
} from "effect-durable-agent/services/prompt-projector";
import {
  CompactionPolicyId,
  emptyContextProjection,
} from "effect-durable-agent/domain/context-projection";
import { initialReducedState } from "effect-durable-agent/domain/reduced-state";
import { SequenceNumber } from "effect-durable-agent/types/core";
import * as Effect from "effect/Effect";
import * as Prompt from "effect/ai/Prompt";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { prepareEDAAnthropicPrompt } from "./anthropic";
import {
  makeEDAMultiProviderModelResolverLayer,
  makeEDANativeCompactionExecutorLayer,
} from "./providers";
import { gatewayEndpoint, prepareNativeCompaction, rehydrateNativeCompaction } from "./transport";

const anthropicBlock = { type: "compaction", content: "summary text", signature: "sig-1" };

const sentinelText = (provider: "anthropic" | "openai", items: ReadonlyArray<unknown>) => {
  const prepared = prepareNativeCompaction(
    provider,
    Prompt.fromMessages([
      nativeCompactionSummaryMessage("readable", {
        provider,
        modelId: "m",
        items: items.map((item) => JSON.parse(JSON.stringify(item))),
      }),
    ]),
  );
  const [message] = prepared.content;
  const part = message?.role === "user" ? message.content[0] : undefined;
  return part?.type === "text" ? part.text : "";
};

describe("native compaction transport", () => {
  it("replays an Anthropic block as its own assistant message, keeping the cache breakpoint", () => {
    const text = sentinelText("anthropic", [anthropicBlock]);
    const request = rehydrateNativeCompaction("anthropic", {
      url: new URL("https://api.anthropic.com/v1/messages"),
      headers: { "anthropic-beta": "existing-beta" },
      body: {
        messages: [
          { role: "user", content: [{ type: "text", text, cache_control: { type: "ephemeral" } }] },
          { role: "assistant", content: [{ type: "text", text: "kept" }] },
        ],
      },
    });
    expect(request.headers["anthropic-beta"]).toBe("existing-beta,compact-2026-09-04");
    expect(request.body).toEqual({
      messages: [
        {
          role: "assistant",
          content: [{ ...anthropicBlock, cache_control: { type: "ephemeral" } }],
        },
        { role: "assistant", content: [{ type: "text", text: "kept" }] },
      ],
    });
  });

  it("keeps the block first in a merged user message", () => {
    const text = sentinelText("anthropic", [anthropicBlock]);
    const request = rehydrateNativeCompaction("anthropic", {
      url: new URL("https://api.anthropic.com/v1/messages"),
      headers: {},
      body: {
        messages: [
          {
            role: "user",
            content: [
              { type: "text", text },
              { type: "text", text: "next question" },
            ],
          },
        ],
      },
    });
    expect(request.body).toEqual({
      messages: [
        { role: "user", content: [anthropicBlock, { type: "text", text: "next question" }] },
      ],
    });
  });

  it("splices an OpenAI compacted window in place of the summary message", () => {
    const window = [
      { type: "message", role: "user", content: [{ type: "input_text", text: "kept user" }] },
      { type: "compaction", encrypted_content: "enc" },
    ];
    const text = sentinelText("openai", window);
    const request = rehydrateNativeCompaction("openai", {
      url: new URL("https://api.openai.com/v1/responses"),
      headers: {},
      body: {
        input: [
          { role: "developer", content: "instructions" },
          { role: "user", content: [{ type: "input_text", text }] },
          { role: "user", content: [{ type: "input_text", text: "next" }] },
        ],
      },
    });
    expect(request.body).toEqual({
      input: [
        { role: "developer", content: "instructions" },
        ...window,
        { role: "user", content: [{ type: "input_text", text: "next" }] },
      ],
    });
  });

  it("keeps other providers' native summaries readable", () => {
    const prompt = Prompt.fromMessages([
      nativeCompactionSummaryMessage("readable summary", {
        provider: "openai",
        modelId: "gpt",
        items: [{ type: "compaction" }],
      }),
    ]);
    const [message] = prepareNativeCompaction("anthropic", prompt).content;
    expect(message?.role === "user" ? message.content : []).toMatchObject([
      { type: "text", text: "readable summary" },
    ]);
  });

  it("routes gateway endpoints per provider", () => {
    expect(gatewayEndpoint("openai", new URL("https://api.openai.com/v1/responses"))).toBe(
      "responses",
    );
    expect(gatewayEndpoint("anthropic", new URL("https://api.anthropic.com/v1/messages"))).toBe(
      "v1/messages",
    );
  });
});

describe("Anthropic prompt preparation", () => {
  it("turns EDA cache hints into cache_control and leaves other messages alone", () => {
    const hint = { "effect-durable-agent": { cacheBreakpoint: true } };
    const prompt = Prompt.fromMessages([
      Prompt.makeMessage("system", { content: "instructions", options: hint }),
      Prompt.makeMessage("user", {
        content: [Prompt.textPart({ text: "history" })],
        options: hint,
      }),
      Prompt.makeMessage("user", { content: [Prompt.textPart({ text: "per-request data" })] }),
    ]);
    const prepared = prepareEDAAnthropicPrompt(prompt).content;
    expect(prepared.map((message) => message.options.anthropic)).toEqual([
      { cacheControl: { type: "ephemeral" } },
      { cacheControl: { type: "ephemeral" } },
      undefined,
    ]);
  });
});

interface Recorded {
  readonly url: string;
  readonly headers: Record<string, string>;
  readonly body: Record<string, unknown>;
}

const stubFetch = (respond: (request: Recorded) => unknown) => {
  const recorded: Array<Recorded> = [];
  vi.stubGlobal("fetch", async (input: URL | string, init: RequestInit) => {
    const request: Recorded = {
      url: String(input),
      headers: Object.fromEntries(new Headers(init.headers).entries()),
      body: JSON.parse(String(init.body)),
    };
    recorded.push(request);
    return new Response(JSON.stringify(respond(request)), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  });
  return recorded;
};

const compactionInput = (provider: string, modelId: string) =>
  Effect.gen(function* () {
    const resolver = yield* ModelResolver;
    const selection = { provider, modelId };
    const prompt = resolver.preparePrompt?.(
      selection,
      Prompt.make([
        { role: "system", content: "instructions" },
        { role: "user", content: [{ type: "text", text: "earlier question" }] },
      ]),
    );
    return {
      plan: CompactionPlan.make({
        policyId: CompactionPolicyId.make("test"),
        sourceFromSeq: SequenceNumber.make(1),
        sourceToSeq: SequenceNumber.make(1),
        retainedFromContextSeq: SequenceNumber.make(2),
      }),
      state: { ...initialReducedState, modelSelection: selection },
      context: emptyContextProjection,
      model: yield* resolver.resolve(selection),
      prompt: prompt ?? Prompt.empty,
      toolkit: undefined,
    } satisfies CompactionExecutorInput;
  });

const runCompaction = (provider: string, modelId: string) =>
  Effect.gen(function* () {
    const executor = yield* CompactionExecutor;
    return yield* executor.execute(yield* compactionInput(provider, modelId));
  }).pipe(
    Effect.provide(makeEDANativeCompactionExecutorLayer()),
    Effect.provide(
      makeEDAMultiProviderModelResolverLayer({
        openai: { apiKey: "sk-test" },
        anthropic: { apiKey: "sk-ant-test" },
        resolve: (selection) =>
          selection?.provider === "anthropic"
            ? { provider: "anthropic", modelId: selection.modelId, config: {} }
            : { provider: "openai", modelId: selection?.modelId ?? "gpt", config: {} },
      }),
    ),
    Effect.runPromise,
  );

describe("native compaction executors", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("compacts Anthropic sessions with on-demand compaction and sums iteration usage", async () => {
    const recorded = stubFetch(() => ({
      id: "msg_1",
      type: "message",
      role: "assistant",
      model: "claude-opus-5-5",
      content: [anthropicBlock],
      stop_reason: "compaction",
      stop_sequence: null,
      usage: {
        input_tokens: 0,
        output_tokens: 0,
        iterations: [
          {
            type: "compaction",
            input_tokens: 60,
            cache_read_input_tokens: 45_000,
            cache_creation_input_tokens: 0,
            output_tokens: 900,
          },
        ],
      },
    }));

    const output = await runCompaction("anthropic", "claude-opus-5-5");

    expect(recorded).toHaveLength(1);
    expect(recorded[0]?.headers["anthropic-beta"]).toContain("compact-2026-09-04");
    expect(recorded[0]?.body).toMatchObject({
      compaction: { type: "summarize" },
      max_tokens: 16_000,
      model: "claude-opus-5-5",
    });
    expect(output.text).toBe("summary text");
    expect(output.usage).toEqual({
      inputTokens: 45_060,
      cachedInputTokens: 45_000,
      cacheWriteInputTokens: 0,
      outputTokens: 900,
    });
    expect(getNativeCompaction(output.promptMessage)).toEqual({
      provider: "anthropic",
      modelId: "claude-opus-5-5",
      items: [anthropicBlock],
    });
  });

  it("compacts OpenAI sessions with /responses/compact and drops instruction items", async () => {
    const window = [
      { type: "message", role: "developer", content: [{ type: "input_text", text: "dup" }] },
      {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "earlier question" }],
      },
      { type: "compaction", id: "cmp_1", encrypted_content: "enc" },
    ];
    const recorded = stubFetch(() => ({
      id: "resp_1",
      object: "response.compaction",
      output: window,
      usage: {
        input_tokens: 36_547,
        input_tokens_details: { cached_tokens: 36_346, cache_write_tokens: 0 },
        output_tokens: 1_026,
      },
    }));

    const output = await runCompaction("openai", "gpt-6.1-sol");

    expect(recorded).toHaveLength(1);
    expect(recorded[0]?.url).toBe("https://api.openai.com/v1/responses/compact");
    expect(Object.keys(recorded[0]?.body ?? {}).sort()).toEqual(["input", "model"]);
    expect(output.usage).toEqual({
      inputTokens: 36_547,
      cachedInputTokens: 36_346,
      cacheWriteInputTokens: 0,
      outputTokens: 1_026,
    });
    expect(getNativeCompaction(output.promptMessage)?.items).toEqual(window.slice(1));
  });
});
