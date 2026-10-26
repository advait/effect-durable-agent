import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/ai/Prompt";
import * as Response from "effect/ai/Response";
import * as Tool from "effect/ai/Tool";
import { assert, describe, it } from "@effect/vitest";

import { reduceCommittedEvents } from "../domain/reduced-state";
import { CommandId, SessionId, ToolCallId } from "../types/core";
import { sequentialUuidV7 } from "./id-generator";
import type { CommittedDurableEvent } from "./session-store";
import { EDASessionStore } from "./session-store";
import { SessionState } from "./session-state";
import {
  ToolRecovery,
  type EDAModelToolkit,
  type EDAToolExecutionContext,
  type EDAToolRegistryShape,
} from "./tool-registry";
import { makeEdaTestLayer, type TestModelParts } from "../testkit/layers";
import {
  SESSION_ID,
  collectCommitted,
  command,
  secondCommand,
  hasCommandCompleted,
  modelSelection,
  NoopParams,
  usage,
} from "./session-state-control-testkit";

/**
 * Startup recovery with tool recovery policies. Two artificial tools run in one inference:
 * `resumable` declares `ToolRecovery: "reinvoke"` (a second invocation of the same call returns
 * the original execution's outcome), `fragile` keeps the default `fail`. The runtime dies while
 * both are running; recovery must re-invoke the first and fail the second.
 */
const ResumableTool = Tool.make("resumable", {
  parameters: NoopParams,
  success: Schema.Unknown,
}).annotate(ToolRecovery, "reinvoke");
const FragileTool = Tool.make("fragile", { parameters: NoopParams, success: Schema.Unknown });

const toolkit: EDAModelToolkit = {
  tools: { resumable: ResumableTool, fragile: FragileTool },
  handle: (() => Effect.die("handled by the registry shape")) as EDAModelToolkit["handle"],
};

interface Invocation {
  readonly tool: string;
  readonly toolCallId: ToolCallId;
  readonly recovery: EDAToolExecutionContext["recovery"];
}

/** A registry whose handlers record each invocation; `block` keeps them running until released. */
const registry = (
  invocations: Array<Invocation>,
  block: boolean | Deferred.Deferred<void>,
): EDAToolRegistryShape => ({
  getModelToolkit: () => Effect.succeed(toolkit),
  getParamsSchema: () => Effect.succeed(NoopParams),
  execute: (toolName, _params, context) =>
    Effect.gen(function* () {
      invocations.push({
        tool: String(toolName),
        toolCallId: context.toolCallId,
        recovery: context.recovery,
      });
      if (block === true) return yield* Effect.never;
      if (block !== false) yield* Deferred.await(block);
      return { tool: String(toolName), attempt: context.recovery?.attempt ?? 1 };
    }),
});

const toolCall = (id: string, name: string) =>
  Response.makePart("tool-call", { id, name, params: {}, providerExecuted: false });

const bothToolsThenText = (): TestModelParts => [
  Stream.make(
    toolCall("call-resumable", "resumable"),
    toolCall("call-fragile", "fragile"),
    Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
  ),
];

const finalText = (): TestModelParts => [
  Stream.make(
    Response.makePart("text-delta", { id: "text-final", delta: "done" }),
    Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
  ),
];

/** Each simulated runtime mints ids from its own range, as separate isolates would. */
let runtimes = 0;
const freshIds = () => {
  runtimes += 1;
  return Array.from({ length: 200 }, (_, i) => sequentialUuidV7(runtimes * 10_000 + i));
};

const startedCount = (committed: ReadonlyArray<CommittedDurableEvent>) =>
  committed.filter((entry) => entry.event.type === "ToolCallStarted").length;

const waitFor = (predicate: (committed: ReadonlyArray<CommittedDurableEvent>) => boolean) =>
  Effect.gen(function* () {
    const store = yield* EDASessionStore;
    for (let attempt = 0; attempt < 500; attempt += 1) {
      const committed = yield* collectCommitted(store);
      if (predicate(committed)) return committed;
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(new Error("timed out waiting for durable events"));
  });

/** Run until both tools have durably started, then take the log as a dead runtime left it. */
const crashWhileBothToolsRun = Effect.scoped(
  Effect.gen(function* () {
    const state = yield* SessionState;
    yield* state.start({ modelSelection });
    yield* state.admitCommand(command);
    return yield* waitFor((committed) => startedCount(committed) === 2);
  }),
).pipe(
  Effect.provide(
    makeEdaTestLayer({
      sessionId: SessionId.make(SESSION_ID),
      ids: freshIds(),
      parts: bothToolsThenText(),
      toolRegistry: registry([], true),
    }),
  ),
);

/** Start a fresh runtime over `seed` and run until the command completes. */
const recover = (seed: ReadonlyArray<CommittedDurableEvent>, invocations: Array<Invocation>) => {
  const prompts: Array<Prompt.RawInput> = [];
  return Effect.scoped(
    Effect.gen(function* () {
      const state = yield* SessionState;
      yield* state.start({ modelSelection });
      return yield* waitFor(hasCommandCompleted(CommandId.make(command.commandId)));
    }),
  ).pipe(
    Effect.provide(
      makeEdaTestLayer({
        sessionId: SessionId.make(SESSION_ID),
        ids: freshIds(),
        seedEvents: seed.map((entry) => entry.event),
        parts: finalText(),
        toolRegistry: registry(invocations, false),
        onStreamText: ({ prompt }) => prompts.push(prompt),
      }),
    ),
    Effect.map((committed) => ({ committed, prompts })),
  );
};

const toolTerminal = (committed: ReadonlyArray<CommittedDurableEvent>, name: string) => {
  const state = reduceCommittedEvents(committed);
  const tool = Array.from(state.toolCalls.values()).find(
    (record) => record.decision?._tag === "Created" && record.decision.toolName === name,
  );
  return tool;
};

describe("startup recovery with tool recovery policies", () => {
  it.effect("re-invokes a reinvoke tool and fails a fail tool from the same inference", () =>
    Effect.gen(function* () {
      const seed = yield* crashWhileBothToolsRun;
      const resumableStart = seed.find(
        (entry) =>
          entry.event.type === "ToolCallStarted" &&
          toolTerminal(seed, "resumable")?.toolCallId ===
            (entry.event.payload as { toolCallId: ToolCallId }).toolCallId,
      );
      const invocations: Array<Invocation> = [];
      const { committed, prompts } = yield* recover(seed, invocations);

      // Only the reinvoke tool ran again, once, told it is attempt 2 of the original start.
      assert.deepStrictEqual(
        invocations.map((invocation) => invocation.tool),
        ["resumable"],
      );
      assert.strictEqual(invocations[0]?.recovery?.attempt, 2);
      assert.strictEqual(invocations[0]?.recovery?.firstStartedSeq, resumableStart?.position.seq);

      const resumable = toolTerminal(committed, "resumable");
      const fragile = toolTerminal(committed, "fragile");
      assert.strictEqual(resumable?.terminal?._tag, "Completed");
      assert.strictEqual(resumable?.attempt, 2);
      assert.strictEqual(resumable?.startedSeq, resumableStart?.position.seq);
      assert.strictEqual(fragile?.terminal?._tag, "Failed");
      if (fragile?.terminal?._tag === "Failed")
        assert.include(
          JSON.stringify(fragile.terminal.error),
          "runtime restarted before lifecycle completed",
        );

      // The replacement run's first inference sees both outcomes.
      const prompt = JSON.stringify(prompts[0]);
      assert.include(prompt, '"attempt":2');
      assert.include(prompt, "runtime restarted before lifecycle completed");
    }),
  );

  it.effect("spends an attempt when the runtime dies again during re-invocation", () =>
    Effect.gen(function* () {
      const seed = yield* crashWhileBothToolsRun;
      // A second runtime re-invokes `resumable` (attempt 2) and dies before it finishes.
      const dying = yield* Effect.scoped(
        Effect.gen(function* () {
          const state = yield* SessionState;
          yield* state.start({ modelSelection }).pipe(Effect.forkScoped);
          return yield* waitFor((committed) => startedCount(committed) === 3);
        }),
      ).pipe(
        Effect.provide(
          makeEdaTestLayer({
            sessionId: SessionId.make(SESSION_ID),
            ids: freshIds(),
            seedEvents: seed.map((entry) => entry.event),
            parts: finalText(),
            toolRegistry: registry([], true),
          }),
        ),
      );
      const invocations: Array<Invocation> = [];
      const { committed } = yield* recover(dying, invocations);
      assert.deepStrictEqual(
        invocations.map((invocation) => invocation.recovery?.attempt),
        [3],
      );
      assert.strictEqual(toolTerminal(committed, "resumable")?.terminal?._tag, "Completed");
    }),
  );

  it.effect("fails a reinvoke tool once its attempts are spent", () =>
    Effect.gen(function* () {
      let seed = yield* crashWhileBothToolsRun;
      for (const attempt of [3, 4]) {
        seed = yield* Effect.scoped(
          Effect.gen(function* () {
            const state = yield* SessionState;
            yield* state.start({ modelSelection }).pipe(Effect.forkScoped);
            return yield* waitFor((committed) => startedCount(committed) === attempt);
          }),
        ).pipe(
          Effect.provide(
            makeEdaTestLayer({
              sessionId: SessionId.make(SESSION_ID),
              ids: freshIds(),
              seedEvents: seed.map((entry) => entry.event),
              parts: finalText(),
              toolRegistry: registry([], true),
            }),
          ),
        );
      }
      const invocations: Array<Invocation> = [];
      const { committed } = yield* recover(seed, invocations);
      assert.deepStrictEqual(invocations, []);
      const resumable = toolTerminal(committed, "resumable");
      assert.strictEqual(resumable?.attempt, 3);
      assert.strictEqual(resumable?.terminal?._tag, "Failed");
    }),
  );

  it.effect("serves reads and holds new work while a re-invoked tool is still running", () =>
    Effect.gen(function* () {
      const seed = yield* crashWhileBothToolsRun;
      const release = yield* Deferred.make<void>();
      const invocations: Array<Invocation> = [];
      const committed = yield* Effect.scoped(
        Effect.gen(function* () {
          const state = yield* SessionState;
          // Startup returns although recovery is still waiting on the re-invoked tool.
          yield* state.start({ modelSelection });
          yield* waitFor((entries) => startedCount(entries) === 3);
          const during = yield* state.snapshot();
          assert.strictEqual(during.toolCalls.size, 2);
          yield* state.admitCommand(secondCommand);
          for (let i = 0; i < 50; i += 1) yield* Effect.yieldNow;
          const held = yield* waitFor(() => true);
          assert.strictEqual(
            held.filter((entry) => entry.event.type === "RunStarted").length,
            1,
            "no run starts while the interrupted run is still being recovered",
          );
          yield* Deferred.succeed(release, undefined);
          return yield* waitFor(hasCommandCompleted(CommandId.make(secondCommand.commandId)));
        }),
      ).pipe(
        Effect.provide(
          makeEdaTestLayer({
            sessionId: SessionId.make(SESSION_ID),
            ids: freshIds(),
            seedEvents: seed.map((entry) => entry.event),
            parts: [...finalText(), ...finalText()],
            toolRegistry: registry(invocations, release),
          }),
        ),
      );
      assert.strictEqual(toolTerminal(committed, "resumable")?.terminal?._tag, "Completed");
      assert.isTrue(hasCommandCompleted(CommandId.make(command.commandId))(committed));
    }),
  );
});
