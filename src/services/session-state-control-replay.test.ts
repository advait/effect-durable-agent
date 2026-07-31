import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";
import { describe, expect, it } from "vite-plus/test";

import {
  CommandId,
  EDASessionStore,
  LiveEventBus,
  SessionId,
  SessionState,
  decideDispatch,
  foldReducedState,
  initialReducedState,
  makeEdaTestLayer,
  pendingCommands,
  SESSION_ID,
  COMMAND_ID,
  SECOND_COMMAND_ID,
  THIRD_COMMAND_ID,
  RUN_ID,
  TURN_ID,
  INFERENCE_ID,
  COMMAND_ADMITTED_EVENT_ID,
  SECOND_COMMAND_ADMITTED_EVENT_ID,
  COMMAND_STARTED_EVENT_ID,
  USER_MESSAGE_ID,
  USER_MESSAGE_EVENT_ID,
  RUN_STARTED_EVENT_ID,
  TURN_STARTED_EVENT_ID,
  INFERENCE_STARTED_EVENT_ID,
  TEXT_EVENT_ID,
  INFERENCE_COMPLETED_EVENT_ID,
  ASSISTANT_MESSAGE_ID,
  ASSISTANT_MESSAGE_EVENT_ID,
  TURN_COMPLETED_EVENT_ID,
  RUN_COMPLETED_EVENT_ID,
  COMMAND_COMPLETED_EVENT_ID,
  generatedId,
  SECOND_COMPLETED_RUN_IDS,
  FIRST_COMPLETED_THEN_SECOND_ADMITTED_IDS,
  NoopParams,
  makeTestLayer,
  modelSelection,
  command,
  secondCommand,
  thirdCommand,
  usage,
  collectCommitted,
  waitForCommitted,
  hasCommandCompleted,
  hasEventType,
} from "./session-state-control-testkit";

describe("SessionState control loop - replay and prompt hydration", () => {
  it("re-pokes dispatch when an active run finishes", async () => {
    const program = Effect.gen(function* () {
      const finishGate = yield* Deferred.make<void>();
      const stream = Stream.make(
        Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
      ).pipe(
        Stream.concat(
          Stream.fromEffect(
            Deferred.await(finishGate).pipe(
              Effect.as(
                Response.makePart("finish", {
                  reason: "stop",
                  usage: usage(),
                  response: undefined,
                }),
              ),
            ),
          ),
        ),
      );

      return yield* Effect.scoped(
        Effect.gen(function* () {
          const dispatcher = yield* SessionState;
          const durableStore = yield* EDASessionStore;
          const liveBus = yield* LiveEventBus;
          const liveStream = yield* liveBus.subscribe();
          const firstTextDelta = yield* liveStream.pipe(
            Stream.filter((event) => event.event.type === "TextDelta"),
            Stream.take(1),
            Stream.runCollect,
            Effect.forkScoped,
          );

          yield* dispatcher.start({ modelSelection });
          yield* dispatcher.admitCommand(command);
          yield* Fiber.join(firstTextDelta);
          yield* dispatcher.admitCommand(secondCommand);
          yield* Deferred.succeed(finishGate, undefined);

          const committed = yield* waitForCommitted(
            durableStore,
            hasCommandCompleted(CommandId.make(SECOND_COMMAND_ID)),
          );
          return committed;
        }),
      ).pipe(
        Effect.provide(
          makeTestLayer(
            [
              COMMAND_ADMITTED_EVENT_ID,
              USER_MESSAGE_ID,
              USER_MESSAGE_EVENT_ID,
              COMMAND_STARTED_EVENT_ID,
              RUN_ID,
              RUN_STARTED_EVENT_ID,
              TURN_ID,
              TURN_STARTED_EVENT_ID,
              INFERENCE_ID,
              INFERENCE_STARTED_EVENT_ID,
              TEXT_EVENT_ID,
              SECOND_COMMAND_ADMITTED_EVENT_ID,
              generatedId(18),
              generatedId(19),
              INFERENCE_COMPLETED_EVENT_ID,
              ASSISTANT_MESSAGE_ID,
              ASSISTANT_MESSAGE_EVENT_ID,
              TURN_COMPLETED_EVENT_ID,
              RUN_COMPLETED_EVENT_ID,
              COMMAND_COMPLETED_EVENT_ID,
              ...SECOND_COMPLETED_RUN_IDS,
            ],
            stream,
          ),
        ),
      );
    });

    const committed = await Effect.runPromise(program);
    const completedCommandIds = committed.flatMap((entry) =>
      entry.event.type === "CommandCompleted" ? [entry.event.payload.commandId] : [],
    );

    expect(completedCommandIds).toEqual([
      CommandId.make(COMMAND_ID),
      CommandId.make(SECOND_COMMAND_ID),
    ]);
  });

  it("folds reduced dispatch state equivalently across incremental prefixes", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const durableStore = yield* EDASessionStore;
      yield* dispatcher.admitCommand(command);
      yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(durableStore, hasEventType("TurnCompleted"));
      yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(durableStore, hasCommandCompleted(CommandId.make(COMMAND_ID)));
      yield* dispatcher.admitCommand(secondCommand);
      return yield* collectCommitted(durableStore);
    }).pipe(Effect.provide(makeTestLayer(FIRST_COMPLETED_THEN_SECOND_ADMITTED_IDS, stream)));

    const committed = await Effect.runPromise(program);
    const allAtOnce = decideDispatch(foldReducedState(initialReducedState, committed));
    const splitPoints = [1, Math.floor(committed.length / 2), committed.length - 1];

    for (const splitPoint of splitPoints) {
      const first = committed.slice(0, splitPoint);
      const second = committed.slice(splitPoint);
      const incrementallyFolded = foldReducedState(
        foldReducedState(initialReducedState, first),
        second,
      );
      expect(decideDispatch(incrementallyFolded)).toEqual(allAtOnce);
    }
    expect(allAtOnce).toMatchObject({
      _tag: "DispatchStartCommand",
      command: { commandId: CommandId.make(SECOND_COMMAND_ID) },
    });
  });

  it("prunes pending admissions once command lifecycle work starts", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const durableStore = yield* EDASessionStore;
      yield* dispatcher.admitCommand(command);
      yield* dispatcher.drainReadyWork({ modelSelection });
      return yield* waitForCommitted(durableStore, hasEventType("CommandStarted"));
    }).pipe(
      Effect.provide(makeTestLayer(FIRST_COMPLETED_THEN_SECOND_ADMITTED_IDS.slice(0, -1), stream)),
    );

    const committed = await Effect.runPromise(program);
    const startedIndex = committed.findIndex((entry) => entry.event.type === "CommandStarted");
    const throughStarted = committed.slice(0, startedIndex + 1);
    const state = foldReducedState(initialReducedState, throughStarted);

    expect(startedIndex).toBeGreaterThanOrEqual(0);
    expect(pendingCommands(state).map((entry) => entry.commandId)).toEqual([]);
    expect(state.commands.get(CommandId.make(COMMAND_ID))?.startedSeq).toBe(
      throughStarted.at(-1)?.position.seq,
    );
    expect(state.lastSeq).toBe(throughStarted.at(-1)?.position.seq);
  });

  it("hydrates subsequent run prompts from the durable transcript", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const firstStream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "first answer" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const secondStream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const durableStore = yield* EDASessionStore;
      yield* dispatcher.admitCommand(command);
      yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(durableStore, hasEventType("TurnCompleted"));
      yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(durableStore, hasCommandCompleted(CommandId.make(COMMAND_ID)));
      yield* dispatcher.admitCommand(secondCommand);
      yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(
        durableStore,
        (entries) => entries.filter((entry) => entry.event.type === "TurnCompleted").length >= 2,
      );
      yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(durableStore, hasCommandCompleted(CommandId.make(SECOND_COMMAND_ID)));
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: [firstStream, secondStream],
          toolSchemas: new Map([["noop", NoopParams]]),
          onStreamText: ({ prompt }) => prompts.push(prompt),
        }),
      ),
    );

    await Effect.runPromise(program);

    const firstPrompt = Prompt.make(prompts[0] ?? "").content;
    const secondPrompt = Prompt.make(prompts[1] ?? "").content;
    expect(firstPrompt.map((message) => message.role)).toEqual(["user"]);
    expect(secondPrompt.map((message) => message.role)).toEqual(["user", "assistant", "user"]);
    expect(JSON.stringify(secondPrompt)).toContain("hello");
    expect(JSON.stringify(secondPrompt)).toContain("first answer");
    expect(JSON.stringify(secondPrompt)).toContain("second");
  });

  it("completes three sequential commands across incremental drain passes", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const durableStore = yield* EDASessionStore;
      yield* dispatcher.admitCommand(command);
      const firstDrain = yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(durableStore, hasEventType("TurnCompleted"));
      yield* dispatcher.admitCommand(secondCommand);
      const secondDrain = yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(
        durableStore,
        (entries) => entries.filter((entry) => entry.event.type === "TurnCompleted").length >= 2,
      );
      yield* dispatcher.admitCommand(thirdCommand);
      const thirdDrain = yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(
        durableStore,
        (entries) => entries.filter((entry) => entry.event.type === "TurnCompleted").length >= 3,
      );
      yield* dispatcher.drainReadyWork({ modelSelection });
      const committed = yield* waitForCommitted(
        durableStore,
        hasCommandCompleted(CommandId.make(THIRD_COMMAND_ID)),
      );
      return { committed, firstDrain, secondDrain, thirdDrain };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          Array.from({ length: 51 }, (_, index) => generatedId(80 + index)),
          stream,
        ),
      ),
    );

    const result = await Effect.runPromise(program);
    const completedCommandIds = result.committed.flatMap((entry) =>
      entry.event.type === "CommandCompleted" ? [entry.event.payload.commandId] : [],
    );

    expect(result.firstDrain).toMatchObject({
      processed: [{ _tag: "SessionForkedCommand", commandId: CommandId.make(COMMAND_ID) }],
      stop: { reason: "active-command" },
    });
    expect(result.secondDrain).toMatchObject({
      processed: [
        { outcome: { _tag: "SessionCommandCompleted" } },
        { _tag: "SessionForkedCommand", commandId: CommandId.make(SECOND_COMMAND_ID) },
      ],
      stop: { reason: "active-command" },
    });
    expect(result.thirdDrain).toMatchObject({
      processed: [
        { outcome: { _tag: "SessionCommandCompleted" } },
        { _tag: "SessionForkedCommand", commandId: CommandId.make(THIRD_COMMAND_ID) },
      ],
      stop: { reason: "active-command" },
    });
    expect(completedCommandIds).toEqual([
      CommandId.make(COMMAND_ID),
      CommandId.make(SECOND_COMMAND_ID),
      CommandId.make(THIRD_COMMAND_ID),
    ]);
  });
});
