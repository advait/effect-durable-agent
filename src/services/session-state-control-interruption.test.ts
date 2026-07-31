import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";
import * as Tool from "effect/unstable/ai/Tool";
import { describe, expect, it } from "vite-plus/test";

import { EDASessionQuery } from "./session-query";
import type { SessionStateShape } from "./session-state";
import type { CommittedDurableEvent } from "./session-store";
import { makeEDAToolkit } from "./tool-registry";
import {
  CommandId,
  EDASessionStore,
  LiveEventBus,
  SessionId,
  SessionState,
  makeEdaTestLayer,
  SESSION_ID,
  NoopParams,
  command,
  secondCommand,
  interruptCommand,
  modelSelection,
  stopTurnCommand,
  usage,
  collectCommitted,
  hasEventType,
  waitForCommitted,
} from "./session-state-control-testkit";

const eventTypes = (entries: ReadonlyArray<CommittedDurableEvent>): ReadonlyArray<string> =>
  entries.map((entry) => entry.event.type);

const countEvents = (entries: ReadonlyArray<CommittedDurableEvent>, type: string): number =>
  entries.filter((entry) => entry.event.type === type).length;

const indexOfType = (entries: ReadonlyArray<CommittedDurableEvent>, type: string): number =>
  entries.findIndex((entry) => entry.event.type === type);

const indexOfCommandEvent = (
  entries: ReadonlyArray<CommittedDurableEvent>,
  type: string,
  commandId: CommandId,
): number =>
  entries.findIndex(
    (entry) =>
      entry.event.type === type &&
      "commandId" in entry.event.payload &&
      entry.event.payload.commandId === commandId,
  );

const promptToolCallIds = (prompt: Prompt.RawInput): ReadonlyArray<string> =>
  Prompt.make(prompt).content.flatMap((message) =>
    "content" in message && Array.isArray(message.content)
      ? message.content.flatMap((part) => (part.type === "tool-call" ? [part.id] : []))
      : [],
  );

const promptToolResultIds = (prompt: Prompt.RawInput): ReadonlyArray<string> =>
  Prompt.make(prompt).content.flatMap((message) =>
    message.role === "tool" && Array.isArray(message.content)
      ? message.content.flatMap((part) => (part.type === "tool-result" ? [part.id] : []))
      : [],
  );

const expectEveryToolCallPaired = (prompt: Prompt.RawInput) => {
  const resultIds = new Set(promptToolResultIds(prompt));
  expect(promptToolCallIds(prompt).every((id) => resultIds.has(id))).toBe(true);
};

const drainUntilCommitted = (
  dispatcher: SessionStateShape,
  store: { readonly eventsAfter: Parameters<typeof collectCommitted>[0]["eventsAfter"] },
  predicate: (entries: ReadonlyArray<CommittedDurableEvent>) => boolean,
) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const drain = yield* dispatcher.drainReadyWork({ modelSelection });
      const committed = yield* collectCommitted(store as Parameters<typeof collectCommitted>[0]);
      if (predicate(committed)) {
        return { drain, committed };
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(new Error("Timed out waiting for committed scheduler state"));
  });

const waitForLiveType = (type: string) =>
  Effect.gen(function* () {
    const liveBus = yield* LiveEventBus;
    const liveStream = yield* liveBus.subscribe();
    return yield* liveStream.pipe(
      Stream.filter((event) => event.event.type === type),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  });

const textAndReasoningThenNever = Stream.make(
  Response.makePart("text-delta", { id: "text-1", delta: "partial answer" }),
  Response.makePart("reasoning-delta", { id: "reasoning-1", delta: "thinking" }),
).pipe(Stream.concat(Stream.never));

describe("SessionState control loop - interruption ownership and partial persistence", () => {
  it("persists assistant partial content on interrupt and hydrates the replacement prompt", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const replacementStream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );

    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const store = yield* EDASessionStore;
        const query = yield* EDASessionQuery;
        const reasoningSeen = yield* waitForLiveType("ReasoningDelta");

        yield* dispatcher.admitCommand(command);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* Fiber.join(reasoningSeen);
        yield* dispatcher.admitCommand(interruptCommand);
        yield* dispatcher.drainReadyWork({ modelSelection });
        const committed = yield* waitForCommitted(
          store,
          (entries) => countEvents(entries, "InferenceStarted") >= 2,
        );
        const messages = yield* query.messages();
        return { committed, messages };
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: [textAndReasoningThenNever, replacementStream],
          toolSchemas: new Map([["noop", NoopParams]]),
          onStreamText: ({ prompt }) => prompts.push(prompt),
        }),
      ),
    );

    const { committed, messages } = await Effect.runPromise(program);
    const partial = committed.find((entry) => entry.event.type === "AssistantPartialCommitted");
    const replacementStarted = indexOfCommandEvent(
      committed,
      "CommandStarted",
      interruptCommand.commandId,
    );
    const replacementPrompt = JSON.stringify(Prompt.make(prompts[1] ?? "").content);

    expect(eventTypes(committed)).toEqual(
      expect.arrayContaining([
        "AssistantPartialCommitted",
        "InferenceFailed",
        "TurnFailed",
        "RunFailed",
        "CommandCancelled",
      ]),
    );
    expect(countEvents(committed, "AssistantPartialCommitted")).toBe(1);
    expect(countEvents(committed, "TurnFailed")).toBe(1);
    expect(indexOfType(committed, "AssistantPartialCommitted")).toBeLessThan(
      indexOfType(committed, "InferenceFailed"),
    );
    expect(indexOfType(committed, "InferenceFailed")).toBeLessThan(
      indexOfType(committed, "TurnFailed"),
    );
    expect(indexOfType(committed, "TurnFailed")).toBeLessThan(indexOfType(committed, "RunFailed"));
    expect(indexOfType(committed, "RunFailed")).toBeLessThan(replacementStarted);
    expect(partial).toMatchObject({
      event: {
        payload: {
          promptParts: [
            { type: "text", text: "partial answer" },
            { type: "reasoning", text: "thinking" },
          ],
          reason: "inference interrupted before completion",
        },
      },
    });
    expect(messages.map((message) => message._tag)).toEqual(["User", "AssistantPartial", "User"]);
    expect(messages[1]).toMatchObject({
      _tag: "AssistantPartial",
      content: { text: "partial answer", reasoning: "thinking" },
    });
    expect(replacementPrompt).toContain("hello");
    expect(replacementPrompt).toContain("partial answer");
    expect(replacementPrompt).toContain("thinking");
    expect(replacementPrompt).toContain("interrupt");
  });

  it("persists assistant partial content on StopTurn and commits one interrupted TurnFailed terminal", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const store = yield* EDASessionStore;
        const query = yield* EDASessionQuery;
        const reasoningSeen = yield* waitForLiveType("ReasoningDelta");

        yield* dispatcher.admitCommand(command);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* Fiber.join(reasoningSeen);
        yield* dispatcher.admitCommand(stopTurnCommand);
        const stopDrain = yield* dispatcher.drainReadyWork({ modelSelection });
        const committed = yield* waitForCommitted(store, hasEventType("StopTurnApplied"));
        const messages = yield* query.messages();
        return { committed, messages, stopDrain };
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: textAndReasoningThenNever,
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const { committed, messages, stopDrain } = await Effect.runPromise(program);
    const partial = committed.find((entry) => entry.event.type === "AssistantPartialCommitted");

    expect(stopDrain.processed).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          outcome: expect.objectContaining({ _tag: "SessionCommandCompleted" }),
        }),
      ]),
    );
    expect(countEvents(committed, "AssistantPartialCommitted")).toBe(1);
    expect(countEvents(committed, "TurnFailed")).toBe(1);
    expect(indexOfType(committed, "StopTurnRequested")).toBeLessThan(
      indexOfType(committed, "AssistantPartialCommitted"),
    );
    expect(indexOfType(committed, "AssistantPartialCommitted")).toBeLessThan(
      indexOfType(committed, "InferenceFailed"),
    );
    expect(indexOfType(committed, "TurnFailed")).toBeLessThan(indexOfType(committed, "RunFailed"));
    expect(indexOfType(committed, "RunFailed")).toBeLessThan(
      indexOfType(committed, "StopTurnApplied"),
    );
    expect(partial).toMatchObject({
      event: {
        payload: {
          promptParts: [
            { type: "text", text: "partial answer" },
            { type: "reasoning", text: "thinking" },
          ],
          reason: "inference interrupted before completion",
        },
      },
    });
    expect(messages.map((message) => message._tag)).toEqual(["User", "AssistantPartial"]);
    expect(messages[1]).toMatchObject({
      _tag: "AssistantPartial",
      content: { text: "partial answer", reasoning: "thinking" },
    });
  });

  it("does not add TurnFailed when StopTurn targets an already completed turn", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "complete answer" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );

    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const store = yield* EDASessionStore;

      yield* dispatcher.admitCommand(command);
      yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(store, hasEventType("TurnCompleted"));
      yield* dispatcher.admitCommand(stopTurnCommand);
      const stopped = yield* drainUntilCommitted(
        dispatcher,
        store,
        hasEventType("StopTurnApplied"),
      );
      return stopped.committed;
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: stream,
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const committed = await Effect.runPromise(program);

    expect(countEvents(committed, "TurnCompleted")).toBe(1);
    expect(countEvents(committed, "TurnFailed")).toBe(0);
    expect(indexOfType(committed, "StopTurnRequested")).toBeGreaterThan(
      indexOfType(committed, "TurnCompleted"),
    );
    expect(indexOfType(committed, "RunFailed")).toBeGreaterThan(
      indexOfType(committed, "StopTurnRequested"),
    );
    expect(indexOfType(committed, "RunCompleted")).toBe(-1);
  });

  it("cancels a running framework-owned tool before stopping the active turn", async () => {
    const BlockingTool = Tool.make("blocking", {
      parameters: Schema.Struct({}),
      success: Schema.Unknown,
    });
    const neverRelease = await Effect.runPromise(Deferred.make<void>());
    const toolkit = Effect.runSync(
      makeEDAToolkit([BlockingTool], {
        blocking: () => Deferred.await(neverRelease).pipe(Effect.as({ ok: true })),
      }),
    );
    const stream = Stream.make(
      Response.makePart("tool-call", {
        id: "tool-call-1",
        name: "blocking",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );

    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const store = yield* EDASessionStore;

        yield* dispatcher.admitCommand(command);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* waitForCommitted(store, hasEventType("ToolCallStarted"));
        yield* dispatcher.admitCommand(stopTurnCommand);
        const stopDrain = yield* dispatcher.drainReadyWork({ modelSelection });
        const committed = yield* waitForCommitted(store, hasEventType("StopTurnApplied"));
        return { committed, stopDrain };
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: stream,
          toolkit,
        }),
      ),
    );

    const { committed, stopDrain } = await Effect.runPromise(program);

    expect(stopDrain.processed).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          outcome: expect.objectContaining({ _tag: "SessionCommandCompleted" }),
        }),
      ]),
    );
    expect(eventTypes(committed)).toEqual(
      expect.arrayContaining([
        "ToolCallStarted",
        "ToolCallFailed",
        "TurnFailed",
        "RunFailed",
        "CommandCancelled",
        "StopTurnApplied",
      ]),
    );
    expect(countEvents(committed, "ToolCallFailed")).toBe(1);
    expect(countEvents(committed, "TurnFailed")).toBe(1);
    expect(indexOfType(committed, "ToolCallFailed")).toBeGreaterThan(
      indexOfType(committed, "StopTurnRequested"),
    );
    expect(indexOfType(committed, "ToolCallFailed")).toBeLessThan(
      indexOfType(committed, "TurnFailed"),
    );
    expect(indexOfType(committed, "TurnFailed")).toBeLessThan(indexOfType(committed, "RunFailed"));
    expect(indexOfType(committed, "ToolCallCompleted")).toBe(-1);
    expect(indexOfType(committed, "TurnCompleted")).toBe(-1);
    expect(
      committed.find((entry) => entry.event.type === "TurnFailed")?.event.payload,
    ).toMatchObject({ error: { code: "turn.interrupted" } });
  });

  it("hydrates the next prompt with an interrupted tool failure after StopTurn", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const BlockingTool = Tool.make("blocking", {
      parameters: Schema.Struct({}),
      success: Schema.Unknown,
    });
    const neverRelease = await Effect.runPromise(Deferred.make<void>());
    const toolkit = Effect.runSync(
      makeEDAToolkit([BlockingTool], {
        blocking: () => Deferred.await(neverRelease).pipe(Effect.as({ ok: true })),
      }),
    );
    const toolStream = Stream.make(
      Response.makePart("tool-call", {
        id: "tool-call-1",
        name: "blocking",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const replacementStream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );

    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const store = yield* EDASessionStore;

        yield* dispatcher.admitCommand(command);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* waitForCommitted(store, hasEventType("ToolCallStarted"));
        yield* dispatcher.admitCommand(stopTurnCommand);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* waitForCommitted(store, hasEventType("StopTurnApplied"));
        yield* dispatcher.admitCommand(secondCommand);
        yield* dispatcher.drainReadyWork({ modelSelection });
        return yield* waitForCommitted(
          store,
          (entries) => countEvents(entries, "InferenceStarted") >= 2,
        );
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: [toolStream, replacementStream],
          toolkit,
          onStreamText: ({ prompt }) => prompts.push(prompt),
        }),
      ),
    );

    const committed = await Effect.runPromise(program);
    const replacementPrompt = prompts[1];

    expect(countEvents(committed, "ToolCallFailed")).toBe(1);
    if (replacementPrompt === undefined) {
      throw new Error("Expected replacement prompt");
    }
    expectEveryToolCallPaired(replacementPrompt);
    expect(JSON.stringify(Prompt.make(replacementPrompt).content)).toContain(
      "tool call interrupted: interrupted",
    );
  });
});
