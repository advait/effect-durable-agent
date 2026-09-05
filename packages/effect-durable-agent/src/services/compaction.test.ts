import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";
import { describe, expect, it } from "vite-plus/test";

import { SubmitMessageCommand } from "../types/commands";
import { CommandId, CompactionId, SequenceNumber, SessionId } from "../types/core";
import {
  CompactionExecutorId,
  CompactionPolicyId,
  ContextProjection,
} from "../domain/context-projection";
import { contextProjectionPromptWithUserContent } from "../domain/message-transcript";
import {
  CompactionError,
  CompactionExecutor,
  CompactionOutput,
  CompactionPlan,
  CompactionPolicy,
} from "./compaction";
import { EDARuntime } from "./runtime";
import { EDASessionStore } from "./session-store";
import type { CommittedDurableEvent } from "./session-store";
import { EventFactory } from "./event-factory";
import { SessionState } from "./session-state";
import type { InferenceRunnerStreamPart } from "./inference-runner";
import { makeEdaTestLayer } from "../testkit/layers";

const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const SECOND_COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1c-1f2e3d4c5b6a");
const OPEN_COMPACTION_ID = CompactionId.make("018f6bd5-2f2a-7b1e-8f1d-1f2e3d4c5b6a");

const modelSelection = { provider: "test", modelId: "test-model" };

const command = new SubmitMessageCommand({
  commandId: COMMAND_ID,
  disposition: "queue",
  content: [Prompt.textPart({ text: "first" })],
});

const secondCommand = new SubmitMessageCommand({
  commandId: SECOND_COMMAND_ID,
  disposition: "queue",
  content: [Prompt.textPart({ text: "second" })],
});

const usage = () =>
  new Response.Usage({
    inputTokens: {
      uncached: undefined,
      total: 10,
      cacheRead: undefined,
      cacheWrite: undefined,
    },
    outputTokens: { total: 5, text: undefined, reasoning: undefined },
  });

const finishedStream = (text: string): Stream.Stream<InferenceRunnerStreamPart, unknown> =>
  Stream.make(
    Response.makePart("text-delta", { id: "text-1", delta: text }),
    Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
  );

const makeRuntimeLayer = (
  parts: ReadonlyArray<Stream.Stream<InferenceRunnerStreamPart, unknown>>,
  options: {
    readonly compactionPolicyLayer?: Layer.Layer<CompactionPolicy>;
    readonly compactionExecutorLayer?: Layer.Layer<CompactionExecutor>;
    readonly generateText?: string;
    readonly onGenerateText?: (input: { readonly prompt: Prompt.RawInput }) => void;
    readonly onStreamText?: (input: { readonly prompt: Prompt.RawInput }) => void;
  } = {},
) =>
  EDARuntime.Live({ modelSelection }).pipe(
    Layer.provideMerge(
      makeEdaTestLayer({
        sessionId: SESSION_ID,
        parts,
        ...(options.compactionPolicyLayer === undefined
          ? {}
          : { compactionPolicyLayer: options.compactionPolicyLayer }),
        ...(options.compactionExecutorLayer === undefined
          ? {}
          : { compactionExecutorLayer: options.compactionExecutorLayer }),
        ...(options.generateText === undefined ? {} : { generateText: options.generateText }),
        ...(options.onGenerateText === undefined
          ? {}
          : {
              onGenerateText: ({ prompt }) => options.onGenerateText?.({ prompt }),
            }),
        ...(options.onStreamText === undefined
          ? {}
          : {
              onStreamText: ({ prompt }) => options.onStreamText?.({ prompt }),
            }),
      }),
    ),
  );

describe("EDA compaction", () => {
  it("commits no durable events when the disabled policy declines compaction", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        yield* runtime.submitAndBlock(command);
        const snapshot = yield* runtime.snapshot();
        const replay = yield* runtime.eventsAfter(SequenceNumber.make(0));
        return yield* replay.pipe(Stream.take(snapshot.state.lastSeq), Stream.runCollect);
      }),
    ).pipe(Effect.provide(makeRuntimeLayer([finishedStream("first response")])));

    const committed = Array.from(await Effect.runPromise(program));

    expect(
      committed.map((entry) => entry.event.type).filter((type) => type.startsWith("Compaction")),
    ).toEqual([]);
    expect(committed.map((entry) => entry.event.type)).not.toContain("SummaryCreated");
    expect(committed.map((entry) => entry.event.type)).not.toContain("ContextRebased");
  });

  it("ships built-in approximate-token policy and LanguageModel summary executor", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const summaryPrompts: Array<Prompt.RawInput> = [];
    const policyLayer = CompactionPolicy.ApproximateTokenThreshold({
      policyId: "test.approximate",
      thresholdTokens: 1,
      retainTailTokens: 0,
      minSummarizableTokens: 1,
      charsPerToken: 1,
    });
    const executorLayer = CompactionExecutor.LanguageModelSummary({
      executorId: "test.language-model-summary",
      summaryMessagePrefix: "Summary:",
    });
    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        yield* runtime.submitAndBlock(command);
        yield* runtime.submitAndBlock(secondCommand);
        const snapshot = yield* runtime.snapshot();
        const replay = yield* runtime.eventsAfter(SequenceNumber.make(0));
        const committed = yield* replay.pipe(
          Stream.take(snapshot.state.lastSeq),
          Stream.runCollect,
        );
        return { committed: Array.from(committed) };
      }),
    ).pipe(
      Effect.provide(
        makeRuntimeLayer([finishedStream("first response"), finishedStream("second response")], {
          compactionPolicyLayer: policyLayer,
          compactionExecutorLayer: executorLayer,
          generateText: "First turn summary",
          onGenerateText: ({ prompt }) => summaryPrompts.push(prompt),
          onStreamText: ({ prompt }) => prompts.push(prompt),
        }),
      ),
    );

    const { committed } = await Effect.runPromise(program);
    const eventTypes = committed.map((entry) => entry.event.type);
    const promptMessages = prompts.map((prompt) =>
      Prompt.make(prompt).content.map(promptMessageText),
    );
    const summaryPromptText = summaryPrompts
      .flatMap((prompt) => Prompt.make(prompt).content.map(promptMessageText))
      .join("\n");

    expect(eventTypes).toEqual(
      expect.arrayContaining([
        "CompactionRequested",
        "CompactionStarted",
        "SummaryCreated",
        "ContextRebased",
        "CompactionCompleted",
      ]),
    );
    expect(summaryPromptText).toContain("[User]: first");
    expect(summaryPromptText).toContain("[Assistant]: first response");
    expect(promptMessages[1]).toEqual(["Summary:\n\nFirst turn summary", "second"]);
  });

  it("rebases the next turn prompt onto a summary plus retained tail", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const policyLayer = CompactionPolicy.FromFunction("test.retain-none", ({ state, context }) =>
      Effect.sync(() => {
        if (context.currentSummary !== undefined || !hasAssistantMessage(state)) {
          return undefined;
        }
        return CompactionPlan.make({
          policyId: CompactionPolicyId.make("test.retain-none"),
          sourceFromSeq: SequenceNumber.make(1),
          sourceToSeq: state.lastSeq,
          retainedFromContextSeq: SequenceNumber.make(state.lastSeq + 1),
        });
      }),
    );
    const executorLayer = CompactionExecutor.FromFunction("test.executor", () =>
      Effect.succeed(
        CompactionOutput.make({
          text: "First turn summary",
          promptMessage: Prompt.makeMessage("user", {
            content: [Prompt.textPart({ text: "Summary: first turn" })],
          }),
          executorId: CompactionExecutorId.make("test.executor"),
        }),
      ),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        yield* runtime.submitAndBlock(command);
        yield* runtime.submitAndBlock(secondCommand);
        const snapshot = yield* runtime.snapshot();
        const replay = yield* runtime.eventsAfter(SequenceNumber.make(0));
        const committed = yield* replay.pipe(
          Stream.take(snapshot.state.lastSeq),
          Stream.runCollect,
        );
        return { committed: Array.from(committed) };
      }),
    ).pipe(
      Effect.provide(
        makeRuntimeLayer([finishedStream("first response"), finishedStream("second response")], {
          compactionPolicyLayer: policyLayer,
          compactionExecutorLayer: executorLayer,
          onStreamText: ({ prompt }) => prompts.push(prompt),
        }),
      ),
    );

    const { committed } = await Effect.runPromise(program);
    const eventTypes = committed.map((entry) => entry.event.type);
    const promptMessages = prompts.map((prompt) =>
      Prompt.make(prompt).content.map(promptMessageText),
    );

    expect(eventTypes).toEqual(
      expect.arrayContaining([
        "CompactionRequested",
        "CompactionStarted",
        "SummaryCreated",
        "ContextRebased",
        "CompactionCompleted",
      ]),
    );
    expect(eventTypes.indexOf("SummaryCreated")).toBeLessThan(eventTypes.indexOf("ContextRebased"));
    expect(promptMessages[0]).toEqual(["first"]);
    expect(promptMessages[1]).toEqual(["Summary: first turn", "second"]);
  });

  it("rehydrates prompt state from summary plus retained tail after compaction recovery", async () => {
    const policyLayer = CompactionPolicy.FromFunction(
      "test.recovery-summary",
      ({ state, context }) =>
        Effect.sync(() => {
          if (context.currentSummary !== undefined || !hasAssistantMessage(state)) {
            return undefined;
          }
          return CompactionPlan.make({
            policyId: CompactionPolicyId.make("test.recovery-summary"),
            sourceFromSeq: SequenceNumber.make(1),
            sourceToSeq: state.lastSeq,
            retainedFromContextSeq: SequenceNumber.make(state.lastSeq + 1),
          });
        }),
    );
    const executorLayer = CompactionExecutor.FromFunction("test.recovery-executor", () =>
      Effect.succeed(
        CompactionOutput.make({
          text: "Recovered summary",
          promptMessage: Prompt.makeMessage("user", {
            content: [Prompt.textPart({ text: "Summary: recovered first turn" })],
          }),
          executorId: CompactionExecutorId.make("test.recovery-executor"),
        }),
      ),
    );

    const recoveredPrompt = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* EDARuntime;
          const store = yield* EDASessionStore;
          yield* runtime.submitAndBlock(command);
          const snapshot = yield* runtime.snapshot();
          const summaryId = snapshot.state.context.currentSummaryId;
          expect(summaryId).toBeDefined();
          const summary = yield* store.loadSummaryArtifact(summaryId!);
          expect(summary).toBeDefined();
          expect(
            Array.from(snapshot.state.messages.values()).filter(
              (message) => message._tag !== "System",
            ),
          ).toEqual([]);
          const context = ContextProjection.make({
            contextVersion: snapshot.state.context.version,
            currentSummary: summary!,
          });
          return contextProjectionPromptWithUserContent(
            snapshot.state,
            context,
            secondCommand.content,
          );
        }),
      ).pipe(
        Effect.provide(
          makeRuntimeLayer([finishedStream("first response")], {
            compactionPolicyLayer: policyLayer,
            compactionExecutorLayer: executorLayer,
          }),
        ),
      ),
    );

    expect(Prompt.make(recoveredPrompt).content.map(promptMessageText)).toEqual([
      "Summary: recovered first turn",
      "second",
    ]);
  });

  it("rejects invalid future retained cursors without durable compaction events", async () => {
    const policyLayer = CompactionPolicy.FromFunction("test.invalid-plan", ({ state }) =>
      Effect.succeed(
        CompactionPlan.make({
          policyId: CompactionPolicyId.make("test.invalid-plan"),
          sourceFromSeq: SequenceNumber.make(1),
          sourceToSeq: state.lastSeq,
          retainedFromContextSeq: SequenceNumber.make(state.lastSeq + 2),
        }),
      ),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        yield* runtime.submitAndBlock(command);
        const snapshot = yield* runtime.snapshot();
        const replay = yield* runtime.eventsAfter(SequenceNumber.make(0));
        return yield* replay.pipe(Stream.take(snapshot.state.lastSeq), Stream.runCollect);
      }),
    ).pipe(
      Effect.provide(
        makeRuntimeLayer([finishedStream("first response")], {
          compactionPolicyLayer: policyLayer,
        }),
      ),
    );

    const committed = Array.from(await Effect.runPromise(program));

    expect(committed.map((entry) => entry.event.type)).not.toContain("CompactionRequested");
    expect(committed.map((entry) => entry.event.type)).not.toContain("ContextRebased");
  });

  it("records compaction failure without changing the next prompt context", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const policyLayer = CompactionPolicy.FromFunction(
      "test.fail-after-first",
      ({ state, context }) =>
        Effect.sync(() => {
          if (context.currentSummary !== undefined || !hasAssistantMessage(state)) {
            return undefined;
          }
          return CompactionPlan.make({
            policyId: CompactionPolicyId.make("test.fail-after-first"),
            sourceFromSeq: SequenceNumber.make(1),
            sourceToSeq: state.lastSeq,
            retainedFromContextSeq: SequenceNumber.make(state.lastSeq + 1),
          });
        }),
    );
    const executorLayer = CompactionExecutor.FromFunction("test.failing-executor", () =>
      Effect.fail(new CompactionError({ message: "summary model unavailable" })),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        yield* runtime.submitAndBlock(command);
        yield* runtime.submitAndBlock(secondCommand);
        const snapshot = yield* runtime.snapshot();
        const replay = yield* runtime.eventsAfter(SequenceNumber.make(0));
        const committed = yield* replay.pipe(
          Stream.take(snapshot.state.lastSeq),
          Stream.runCollect,
        );
        return { committed: Array.from(committed) };
      }),
    ).pipe(
      Effect.provide(
        makeRuntimeLayer([finishedStream("first response"), finishedStream("second response")], {
          compactionPolicyLayer: policyLayer,
          compactionExecutorLayer: executorLayer,
          onStreamText: ({ prompt }) => prompts.push(prompt),
        }),
      ),
    );

    const { committed } = await Effect.runPromise(program);
    const eventTypes = committed.map((entry) => entry.event.type);
    const promptMessages = prompts.map((prompt) =>
      Prompt.make(prompt).content.map(promptMessageText),
    );

    expect(eventTypes).toContain("CompactionFailed");
    expect(eventTypes).not.toContain("SummaryCreated");
    expect(eventTypes).not.toContain("ContextRebased");
    expect(promptMessages[1]).toEqual(["first", "first response", "second"]);
  });

  it("does not run regular compaction policy during startup recovery", async () => {
    let calls = 0;
    const policyLayer = CompactionPolicy.FromFunction("test.startup-check", () => {
      calls += 1;
      return Effect.succeed(undefined);
    });
    const program = Effect.scoped(
      Effect.gen(function* () {
        const sessionState = yield* SessionState;
        const store = yield* EDASessionStore;
        yield* sessionState.start({ modelSelection });
        yield* Effect.yieldNow;
        return yield* collectCommitted(store);
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SESSION_ID,
          parts: Stream.empty,
          compactionPolicyLayer: policyLayer,
        }),
      ),
    );

    const committed = await Effect.runPromise(program);

    expect(calls).toBe(0);
    expect(committed).toEqual([]);
  });

  it("fails open requested or started compactions during startup recovery", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const events = yield* EventFactory;
        const sessionState = yield* SessionState;
        const store = yield* EDASessionStore;
        yield* sessionState.appendDurableBatch([
          yield* events.compactionRequested({
            compactionId: OPEN_COMPACTION_ID,
            sourceFromSeq: SequenceNumber.make(1),
            sourceToSeq: SequenceNumber.make(2),
          }),
          yield* events.compactionStarted({ compactionId: OPEN_COMPACTION_ID }),
        ]);
        yield* sessionState.start({ modelSelection });
        return yield* waitForCommitted(store, (committed) =>
          committed.some((entry) => entry.event.type === "CompactionFailed"),
        );
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SESSION_ID,
          parts: Stream.empty,
        }),
      ),
    );

    const committed = await Effect.runPromise(program);
    const failed = committed.find((entry) => entry.event.type === "CompactionFailed");

    expect(failed).toMatchObject({
      event: {
        payload: {
          compactionId: OPEN_COMPACTION_ID,
          error: { message: "runtime restarted before compaction completed" },
        },
      },
    });
  });
});

const hasAssistantMessage = (state: {
  readonly messages: ReadonlyMap<unknown, { readonly _tag: string }>;
}) => Array.from(state.messages.values()).some((message) => message._tag === "Assistant");

const promptMessageText = (message: Prompt.Message): string => {
  const content = "content" in message ? message.content : [];
  if (typeof content === "string") {
    return content;
  }
  return content
    .filter((part): part is Prompt.TextPart => part.type === "text")
    .map((part) => part.text)
    .join("");
};

const collectCommitted = (store: EDASessionStoreShape) =>
  store.eventsAfter(SequenceNumber.make(0)).pipe(
    Stream.runCollect,
    Effect.map((committed) => Array.from(committed)),
  );

const waitForCommitted = (
  store: EDASessionStoreShape,
  predicate: (committed: ReadonlyArray<CommittedDurableEvent>) => boolean,
) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const committed = yield* collectCommitted(store);
      if (predicate(committed)) {
        return committed;
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(new Error("Timed out waiting for committed durable events"));
  });
