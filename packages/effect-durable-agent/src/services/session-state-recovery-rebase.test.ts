import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/ai/Prompt";
import * as Response from "effect/ai/Response";
import * as Tool from "effect/ai/Tool";
import { describe, expect, it } from "vite-plus/test";
import { CompactionPolicyId } from "../domain/context-projection";
import { CancelPendingMessageCommand } from "../types/commands";
import { CommandId, SequenceNumber, SessionId } from "../types/core";
import { makeEdaTestLayer, type TestModelParts } from "../testkit/layers";
import { CompactionExecutor, CompactionPolicy } from "./compaction";
import { sequentialUuidV7 } from "./id-generator";
import { SessionState } from "./session-state";
import {
  SESSION_ID,
  collectCommitted,
  command,
  modelSelection,
  steerCommand,
  usage,
} from "./session-state-control-testkit";
import type { CommittedDurableEvent, EDASessionStoreShape } from "./session-store";
import { EDASessionStore } from "./session-store";
import { makeEDAToolkit, type EDAModelToolkit } from "./tool-registry";

const sessionId = SessionId.make(SESSION_ID);
const cancelCommandId = CommandId.make("018f6bd5-2f2a-7b1e-8f31-1f2e3d4c5b6a");
const NoopTool = Tool.make("noop", { parameters: Schema.Struct({}), success: Schema.Unknown });
const noopToolkit: EDAModelToolkit = Effect.runSync(
  makeEDAToolkit([NoopTool], { noop: () => Effect.succeed({ ok: true }) }),
);

const textStream = (text: string) =>
  Stream.make(
    Response.makePart("text-delta", { id: `text-${text}`, delta: text }),
    Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
  );

const waitForLog = (
  store: EDASessionStoreShape,
  done: (log: ReadonlyArray<CommittedDurableEvent>) => boolean,
) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 1_000; attempt += 1) {
      const log = yield* collectCommitted(store);
      if (done(log)) return log;
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(new Error("timed out waiting for the durable log"));
  });

const commandCompletedAfter = (from: number) => (log: ReadonlyArray<CommittedDurableEvent>) =>
  log.some(
    ({ event }, index) =>
      index >= from &&
      (event.type === "CommandCompleted" || event.type === "CommandFailed") &&
      event.payload.commandId === command.commandId,
  );

/** Assistant messages from completed inferences that requested no tools. */
const finalAnswers = (log: ReadonlyArray<CommittedDurableEvent>): number => {
  const completed = new Set<string>();
  const withTools = new Set<string>();
  for (const { event } of log) {
    if (event.type === "InferenceCompleted") completed.add(event.payload.inferenceId);
    if (event.type === "ToolCallCreated" || event.type === "ToolCallRejected") {
      withTools.add(event.payload.inferenceId);
    }
  }
  return log.filter(
    ({ event }) =>
      event.type === "AssistantMessageCommitted" &&
      completed.has(event.payload.inferenceId) &&
      !withTools.has(event.payload.inferenceId),
  ).length;
};

const eventTypes = (log: ReadonlyArray<CommittedDurableEvent>) =>
  log.map(({ event }) => event.type);

/** Restarts a session from `prefix` and returns the log once the command is terminal. */
const recover = (prefix: ReadonlyArray<CommittedDurableEvent>, toolkit?: EDAModelToolkit) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* SessionState;
        const store = yield* EDASessionStore;
        yield* session.start({ modelSelection });
        return yield* waitForLog(store, commandCompletedAfter(prefix.length));
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId,
          ids: Array.from({ length: 200 }, (_, index) => sequentialUuidV7(20_000 + index)),
          seedEvents: prefix.map((entry) => entry.event),
          parts: [textStream("recovered answer")],
          ...(toolkit === undefined ? {} : { toolkit }),
        }),
      ),
    ),
  );

/**
 * The prefix a kill leaves when the turn ends, a steer queues, boundary compaction rebases the
 * whole history away with the run still open, and the steer is then cancelled: the run's turns
 * are pruned and no steering is pending.
 */
const steerCancelledAfterRebase = (input: {
  readonly parts: TestModelParts;
  readonly turnEnded: "TurnFailed" | "TurnCompleted";
  readonly toolkit?: EDAModelToolkit;
}) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        return yield* Effect.gen(function* () {
          const session = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* session.admitCommand(command);
          yield* session.drainReadyWork({ modelSelection });
          yield* waitForLog(store, (log) => eventTypes(log).includes(input.turnEnded));
          yield* session.admitCommand(steerCommand);
          const steer = Array.from((yield* session.snapshot()).messages.values()).find(
            (message) => "commandId" in message && message.commandId === steerCommand.commandId,
          );
          if (steer === undefined) return yield* Effect.die(new Error("steer not admitted"));
          const drain = yield* session.drainReadyWork({ modelSelection }).pipe(Effect.forkScoped);
          yield* Deferred.await(entered);
          yield* session.admitCommand(
            new CancelPendingMessageCommand({
              commandId: cancelCommandId,
              messageId: steer.messageId,
              reason: "user-cancel",
            }),
          );
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(drain);
          const log = yield* waitForLog(store, (entries) =>
            entries.some(
              ({ event }) =>
                event.type === "CommandCompleted" && event.payload.commandId === cancelCommandId,
            ),
          );
          const cancelled = log.findIndex(
            ({ event }) =>
              event.type === "CommandCompleted" && event.payload.commandId === cancelCommandId,
          );
          return log.slice(0, cancelled + 1);
        }).pipe(
          Effect.provide(
            makeEdaTestLayer({
              sessionId,
              ids: Array.from({ length: 200 }, (_, index) => sequentialUuidV7(10_000 + index)),
              parts: input.parts,
              ...(input.toolkit === undefined ? {} : { toolkit: input.toolkit }),
              compactionPolicyLayer: CompactionPolicy.ApproximateTokenThreshold({
                thresholdTokens: 1,
                retainTailTokens: 0,
                minSummarizableTokens: 1,
                charsPerToken: 1,
              }),
              compactionExecutorLayer: CompactionExecutor.FromFunction("test.held-summary", () =>
                Effect.gen(function* () {
                  yield* Deferred.succeed(entered, undefined);
                  yield* Deferred.await(release);
                  return {
                    text: "summary",
                    promptMessage: Prompt.makeMessage("user", {
                      content: [Prompt.textPart({ text: "summary" })],
                    }),
                  };
                }),
              ),
            }),
          ),
        );
      }),
    ),
  );

describe("startup recovery after a context rebase pruned the interrupted run", () => {
  it("continues a run whose failed turn the rebase pruned, instead of completing it unanswered", async () => {
    const prefix = await steerCancelledAfterRebase({
      turnEnded: "TurnFailed",
      parts: Stream.make(Response.makePart("text-delta", { id: "partial", delta: "partial" })).pipe(
        Stream.concat(Stream.fail(new Error("provider failed"))),
      ),
    });
    expect(eventTypes(prefix)).toEqual(expect.arrayContaining(["TurnFailed", "ContextRebased"]));
    expect(eventTypes(prefix)).not.toContain("RunFailed");

    const recovered = await recover(prefix);
    expect(finalAnswers(recovered)).toBe(1);
    expect(eventTypes(recovered.slice(prefix.length))).toContain("RunStarted");
  });

  it("continues a run whose tool feedback the rebase pruned, instead of completing it unanswered", async () => {
    const prefix = await steerCancelledAfterRebase({
      turnEnded: "TurnCompleted",
      toolkit: noopToolkit,
      parts: [
        Stream.make(
          Response.makePart("tool-call", {
            id: "tool-call-1",
            name: "noop",
            params: {},
            providerExecuted: false,
          }),
          Response.makePart("finish", {
            reason: "tool-calls",
            usage: usage(),
            response: undefined,
          }),
        ),
      ],
    });
    expect(eventTypes(prefix)).toEqual(
      expect.arrayContaining(["ToolCallCompleted", "TurnCompleted", "ContextRebased"]),
    );
    expect(finalAnswers(prefix)).toBe(0);

    const recovered = await recover(prefix, noopToolkit);
    expect(finalAnswers(recovered)).toBe(1);
    expect(eventTypes(recovered.slice(prefix.length))).toContain("RunStarted");
  });

  it("completes a run whose rebase kept its completed turn but pruned the answering inference", async () => {
    const golden = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* SessionState;
          const store = yield* EDASessionStore;
          yield* session.admitCommand(command);
          yield* session.drainReadyWork({ modelSelection });
          yield* waitForLog(store, (log) => eventTypes(log).includes("TurnCompleted"));
          yield* session.drainReadyWork({ modelSelection });
          return yield* waitForLog(store, commandCompletedAfter(0));
        }),
      ).pipe(
        Effect.provide(
          makeEdaTestLayer({
            sessionId,
            ids: Array.from({ length: 200 }, (_, index) => sequentialUuidV7(10_000 + index)),
            parts: textStream("pong"),
            generateText: "summary of pong",
            // Retain from the TurnCompleted, so the rebase prunes the inference but not the turn.
            compactionPolicyLayer: CompactionPolicy.FromFunction("test.keep-turn", ({ state }) => {
              const turn = Array.from(state.turns.values()).at(-1);
              if (turn?.terminal?._tag !== "Completed") return Effect.succeed(undefined);
              return Effect.succeed({
                policyId: CompactionPolicyId.make("test.keep-turn"),
                sourceFromSeq: SequenceNumber.make(1),
                sourceToSeq: SequenceNumber.make(turn.terminal.seq - 1),
                retainedFromContextSeq: turn.terminal.seq,
              });
            }),
            compactionExecutorLayer: CompactionExecutor.LanguageModelSummary(),
          }),
        ),
      ),
    );
    const prefix = golden.slice(0, eventTypes(golden).indexOf("RunCompleted"));
    expect(eventTypes(prefix)).toContain("ContextRebased");

    const recovered = await recover(prefix);
    expect(finalAnswers(recovered)).toBe(1);
    expect(eventTypes(recovered).filter((type) => type === "RunStarted")).toHaveLength(1);
    expect(eventTypes(recovered.slice(prefix.length))).toEqual(
      expect.arrayContaining(["RunCompleted", "CommandCompleted", "RecoveryCompleted"]),
    );
  });
});
