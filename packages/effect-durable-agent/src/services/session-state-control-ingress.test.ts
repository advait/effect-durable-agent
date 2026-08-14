import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";
import { describe, expect, it } from "vite-plus/test";
import {
  CancelPendingMessageCommand,
  PromotePendingMessageCommand,
  ResumePendingMessagesCommand,
} from "../types/commands";

import {
  CommandId,
  EDASessionStore,
  EDASessionStoreError,
  EventId,
  LiveEventBus,
  RunId,
  SequenceNumber,
  SessionId,
  SessionState,
  SubmitMessageCommand,
  TurnId,
  makeEdaTestLayer,
  SESSION_ID,
  COMMAND_ID,
  SECOND_COMMAND_ID,
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
  NoopParams,
  makeTestLayer,
  modelSelection,
  command,
  secondCommand,
  steerCommand,
  interruptCommand,
  stopTurnCommand,
  usage,
  waitForCommitted,
  hasCommandCompleted,
  hasEventType,
} from "./session-state-control-testkit";

describe("SessionState control loop - ingress and active command control", () => {
  it("blocks on wakeups and dispatches admitted commands inward", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const liveBus = yield* LiveEventBus;
        const liveStream = yield* liveBus.subscribe();
        const liveFiber = yield* liveStream.pipe(
          Stream.take(12),
          Stream.runCollect,
          Effect.forkScoped,
        );
        yield* dispatcher.start({ modelSelection });
        yield* dispatcher.admitCommand(command);
        return yield* Fiber.join(liveFiber);
      }),
    ).pipe(
      Effect.provide(
        makeTestLayer(
          [
            COMMAND_ADMITTED_EVENT_ID,
            COMMAND_STARTED_EVENT_ID,
            USER_MESSAGE_ID,
            USER_MESSAGE_EVENT_ID,
            RUN_ID,
            RUN_STARTED_EVENT_ID,
            TURN_ID,
            TURN_STARTED_EVENT_ID,
            INFERENCE_ID,
            INFERENCE_STARTED_EVENT_ID,
            TEXT_EVENT_ID,
            ASSISTANT_MESSAGE_ID,
            ASSISTANT_MESSAGE_EVENT_ID,
            INFERENCE_COMPLETED_EVENT_ID,
            TURN_COMPLETED_EVENT_ID,
            RUN_COMPLETED_EVENT_ID,
            COMMAND_COMPLETED_EVENT_ID,
          ],
          stream,
        ),
      ),
    );

    const liveEvents = await Effect.runPromise(program);

    expect(Array.from(liveEvents).map((event) => event.event.type)).toEqual([
      "CommandAdmitted",
      "UserMessageSubmitted",
      "CommandStarted",
      "RunStarted",
      "TurnStarted",
      "InferenceStarted",
      "TextDelta",
      "AssistantMessageCommitted",
      "InferenceCompleted",
      "TurnCompleted",
      "RunCompleted",
      "CommandCompleted",
    ]);
    expect(Array.from(liveEvents)[1]).toMatchObject({
      event: {
        type: "UserMessageSubmitted",
        payload: { commandId: CommandId.make(COMMAND_ID), content: command.content },
      },
    });
    expect(Array.from(liveEvents)[3]).toMatchObject({
      event: {
        type: "RunStarted",
        payload: {
          runId: RunId.make(RUN_ID),
          commandIds: [CommandId.make(COMMAND_ID)],
        },
      },
    });
    expect(Array.from(liveEvents)[4]).toMatchObject({
      event: { type: "TurnStarted", payload: { turnId: TurnId.make(TURN_ID) } },
    });
    expect(Array.from(liveEvents)[11]).toMatchObject({
      event: {
        type: "CommandCompleted",
        eventId: EventId.make(COMMAND_COMPLETED_EVENT_ID),
        payload: { commandId: CommandId.make(COMMAND_ID) },
      },
    });
  });

  it("surfaces durable hydration failures before dispatch drains", async () => {
    let failuresRemaining = 1;
    const wrapStore = (inner: EDASessionStoreShape): EDASessionStoreShape => ({
      ...inner,
      loadReducerCheckpoint: (name) =>
        failuresRemaining > 0
          ? Effect.gen(function* () {
              failuresRemaining -= 1;
              return yield* Effect.fail(
                new EDASessionStoreError({ message: "injected hydration failure" }),
              );
            })
          : inner.loadReducerCheckpoint(name),
    });

    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        return yield* dispatcher.drainReadyWork({ modelSelection });
      }).pipe(Effect.provide(makeTestLayer([], Stream.empty, wrapStore))),
    );

    const exit = await Effect.runPromise(Effect.exit(program));

    expect(failuresRemaining).toBe(0);
    expect(Exit.isFailure(exit)).toBe(true);
    expect(Exit.isFailure(exit) ? Cause.pretty(exit.cause) : "").toContain(
      "injected hydration failure",
    );
  });

  it("serializes concurrently submitted commands one at a time", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const liveBus = yield* LiveEventBus;
        const durableStore = yield* EDASessionStore;
        const liveStream = yield* liveBus.subscribe();
        const liveFiber = yield* liveStream.pipe(
          Stream.take(24),
          Stream.runCollect,
          Effect.forkScoped,
        );

        yield* dispatcher.start({ modelSelection });
        const releaseSubmits = yield* Deferred.make<void>();
        const submitWhenReleased = (submitted: SubmitMessageCommand) =>
          Effect.gen(function* () {
            yield* Deferred.await(releaseSubmits);
            return yield* dispatcher.admitCommand(submitted);
          });
        const firstSubmit = yield* submitWhenReleased(command).pipe(Effect.forkScoped);
        const secondSubmit = yield* submitWhenReleased(secondCommand).pipe(Effect.forkScoped);
        yield* Deferred.succeed(releaseSubmits, undefined);
        yield* Fiber.join(firstSubmit);
        yield* Fiber.join(secondSubmit);

        const liveEvents = yield* Fiber.join(liveFiber);
        const committed = yield* durableStore
          .eventsAfter(SequenceNumber.make(0))
          .pipe(Stream.runCollect);
        return {
          committed: Array.from(committed),
          liveEventTypes: Array.from(liveEvents).map((event) => event.event.type),
        };
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: stream,
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const result = await Effect.runPromise(program);
    const admitted = result.committed.flatMap((entry) =>
      entry.event.type === "CommandAdmitted"
        ? [{ commandId: entry.event.payload.command.commandId, seq: entry.position.seq }]
        : [],
    );
    const started = result.committed.flatMap((entry, index) =>
      entry.event.type === "CommandStarted"
        ? [{ commandId: entry.event.payload.commandId, index }]
        : [],
    );
    const terminals = result.committed.flatMap((entry, index) =>
      entry.event.type === "CommandCompleted" ||
      entry.event.type === "CommandFailed" ||
      entry.event.type === "CommandCancelled"
        ? [{ commandId: entry.event.payload.commandId, index }]
        : [],
    );
    const firstStarted = started[0];
    const secondStarted = started[1];
    const firstTerminal = terminals.find(
      (terminal) => terminal.commandId === firstStarted?.commandId,
    );

    expect(admitted).toHaveLength(2);
    expect(new Set(admitted.map((entry) => entry.seq)).size).toBe(2);
    expect(started).toHaveLength(2);
    expect(terminals).toHaveLength(2);
    expect(result.liveEventTypes.filter((type) => type === "CommandCompleted")).toHaveLength(2);
    expect(firstTerminal?.index).toBeGreaterThan(firstStarted?.index ?? -1);
    expect(secondStarted?.index).toBeGreaterThan(firstTerminal?.index ?? -1);
    for (let index = 1; index < result.committed.length; index += 1) {
      expect(result.committed[index]?.position.seq).toBeGreaterThan(
        result.committed[index - 1]?.position.seq ?? 0,
      );
    }
  });

  it("returns from drain while a submitted run is still streaming", async () => {
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

      return yield* Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const durableStore = yield* EDASessionStore;

        yield* dispatcher.admitCommand(command);
        const firstDrain = yield* dispatcher.drainReadyWork({ modelSelection });
        const secondDrain = yield* dispatcher.drainReadyWork({ modelSelection });
        yield* Deferred.succeed(finishGate, undefined);
        yield* waitForCommitted(durableStore, hasEventType("TurnCompleted"));
        yield* dispatcher.drainReadyWork({ modelSelection });
        const committed = yield* waitForCommitted(
          durableStore,
          hasCommandCompleted(CommandId.make(COMMAND_ID)),
        );

        return { committed, firstDrain, secondDrain };
      }).pipe(
        Effect.provide(
          makeTestLayer(
            [
              COMMAND_ADMITTED_EVENT_ID,
              COMMAND_STARTED_EVENT_ID,
              USER_MESSAGE_ID,
              USER_MESSAGE_EVENT_ID,
              RUN_ID,
              RUN_STARTED_EVENT_ID,
              TURN_ID,
              TURN_STARTED_EVENT_ID,
              INFERENCE_ID,
              INFERENCE_STARTED_EVENT_ID,
              TEXT_EVENT_ID,
              INFERENCE_COMPLETED_EVENT_ID,
              ASSISTANT_MESSAGE_ID,
              ASSISTANT_MESSAGE_EVENT_ID,
              TURN_COMPLETED_EVENT_ID,
              RUN_COMPLETED_EVENT_ID,
              COMMAND_COMPLETED_EVENT_ID,
            ],
            stream,
          ),
        ),
      );
    });

    const result = await Effect.runPromise(program);
    const completedCommandIds = result.committed.flatMap((entry) =>
      entry.event.type === "CommandCompleted" ? [entry.event.payload.commandId] : [],
    );

    expect(result.firstDrain).toMatchObject({
      processed: [{ _tag: "SessionForkedCommand", commandId: CommandId.make(COMMAND_ID) }],
      stop: { reason: "active-command" },
    });
    expect(result.secondDrain).toMatchObject({
      processed: [],
      stop: { reason: "active-command", active: { commandId: CommandId.make(COMMAND_ID) } },
    });
    expect(completedCommandIds).toEqual([CommandId.make(COMMAND_ID)]);
  });

  it("steers an active run without stopping the active command", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
    ).pipe(Stream.concat(Stream.never));

    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const durableStore = yield* EDASessionStore;

        yield* dispatcher.admitCommand(command);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* waitForCommitted(durableStore, hasEventType("InferenceStarted"));
        yield* dispatcher.admitCommand(steerCommand);
        const steerDrain = yield* dispatcher.drainReadyWork({ modelSelection });
        const committed = yield* waitForCommitted(durableStore, (entries) =>
          entries.some(
            (entry) =>
              entry.event.type === "CommandCompleted" &&
              entry.event.payload.commandId === steerCommand.commandId,
          ),
        );
        return { committed, steerDrain };
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: stream,
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const { committed, steerDrain } = await Effect.runPromise(program);
    const steering = committed.find(
      (entry) =>
        entry.event.type === "UserMessageSubmitted" &&
        entry.event.payload.commandId === steerCommand.commandId,
    );
    const cancelledCommandIds = committed.flatMap((entry) =>
      entry.event.type === "CommandCancelled" ? [entry.event.payload.commandId] : [],
    );

    expect(steerDrain.processed).toEqual([
      expect.objectContaining({
        outcome: expect.objectContaining({ _tag: "SessionCommandCompleted" }),
      }),
    ]);
    expect(steerDrain.stop).toMatchObject({ reason: "active-command" });
    expect(steering).toMatchObject({
      event: {
        payload: { commandId: steerCommand.commandId, disposition: "steer" },
      },
    });
    expect(cancelledCommandIds).toEqual([]);
  });

  it("consumes all steering pending at a boundary before terminalizing the active run", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const secondSteerCommand = new SubmitMessageCommand({
      commandId: CommandId.make("018f6bd5-2f2a-7b1e-8f23-1f2e3d4c5b6a"),
      disposition: "steer",
      content: [Prompt.textPart({ text: "steer two" })],
    });
    const finishFirstTurn = await Effect.runPromise(Deferred.make<void>());
    const firstStream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "first answer" }),
    ).pipe(
      Stream.concat(
        Stream.fromEffect(
          Deferred.await(finishFirstTurn).pipe(
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
    const secondStream = Stream.make(
      Response.makePart("text-delta", { id: "text-2", delta: "second answer" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );

    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const durableStore = yield* EDASessionStore;

        yield* dispatcher.start({ modelSelection });
        yield* dispatcher.admitCommand(command);
        yield* waitForCommitted(durableStore, hasEventType("InferenceStarted"));
        yield* dispatcher.admitCommand(steerCommand);
        yield* dispatcher.admitCommand(secondSteerCommand);
        yield* Deferred.succeed(finishFirstTurn, undefined);
        const committed = yield* waitForCommitted(
          durableStore,
          hasCommandCompleted(command.commandId),
        );
        return committed;
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: [firstStream, secondStream],
          toolSchemas: new Map([["noop", NoopParams]]),
          onStreamText: ({ prompt }) => prompts.push(prompt),
        }),
      ),
    );

    const committed = await Effect.runPromise(program);
    const committedTypes = committed.map((entry) => entry.event.type);
    const turnStarted = committed.filter((entry) => entry.event.type === "TurnStarted");
    const originalRunCompleted = committed.filter((entry) => entry.event.type === "RunCompleted");
    const originalCommandCompleted = committed.filter(
      (entry) =>
        entry.event.type === "CommandCompleted" &&
        entry.event.payload.commandId === command.commandId,
    );
    const firstContinuationPrompt = JSON.stringify(Prompt.make(prompts[1] ?? "").content);

    expect(turnStarted).toHaveLength(2);
    expect(turnStarted[1]).toMatchObject({
      event: { payload: { inputMessageIds: [expect.any(String), expect.any(String)] } },
    });
    expect(firstContinuationPrompt).toContain("steer");
    expect(firstContinuationPrompt).toContain("steer two");
    expect(originalRunCompleted).toHaveLength(1);
    expect(originalCommandCompleted).toHaveLength(1);
    expect(committedTypes.lastIndexOf("RunCompleted")).toBeGreaterThan(
      committedTypes.lastIndexOf("TurnCompleted"),
    );
  });

  it("interrupts an active run and starts the interrupting message as replacement work", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const activeStream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
    ).pipe(Stream.concat(Stream.never));
    const replacementStream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );

    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const durableStore = yield* EDASessionStore;

        yield* dispatcher.admitCommand(command);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* waitForCommitted(durableStore, hasEventType("InferenceStarted"));
        yield* dispatcher.admitCommand(interruptCommand);
        const interruptDrain = yield* dispatcher.drainReadyWork({ modelSelection });
        yield* waitForCommitted(durableStore, hasEventType("TurnCompleted"));
        yield* dispatcher.drainReadyWork({ modelSelection });
        const committed = yield* waitForCommitted(durableStore, (entries) =>
          entries.some(
            (entry) =>
              entry.event.type === "CommandCompleted" &&
              entry.event.payload.commandId === interruptCommand.commandId,
          ),
        );
        return { committed, interruptDrain };
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: [activeStream, replacementStream],
          toolSchemas: new Map([["noop", NoopParams]]),
          onStreamText: ({ prompt }) => prompts.push(prompt),
        }),
      ),
    );

    const { committed, interruptDrain } = await Effect.runPromise(program);
    const eventTypes = committed.map((entry) => entry.event.type);
    const indexOfType = (type: string) => eventTypes.indexOf(type);
    const cancelledCommandIds = committed.flatMap((entry) =>
      entry.event.type === "CommandCancelled" ? [entry.event.payload.commandId] : [],
    );
    const completedCommandIds = committed.flatMap((entry) =>
      entry.event.type === "CommandCompleted" ? [entry.event.payload.commandId] : [],
    );
    const replacementStartedIndex = committed.findIndex(
      (entry) =>
        entry.event.type === "CommandStarted" &&
        entry.event.payload.commandId === interruptCommand.commandId,
    );
    const stoppedTurn = committed.find((entry) => entry.event.type === "TurnFailed");

    expect(interruptDrain.processed).toEqual([
      expect.objectContaining({
        _tag: "SessionForkedCommand",
        commandId: interruptCommand.commandId,
      }),
    ]);
    expect(eventTypes).toEqual(
      expect.arrayContaining(["InferenceFailed", "TurnFailed", "RunFailed"]),
    );
    expect(indexOfType("TurnFailed")).toBeGreaterThan(indexOfType("InferenceFailed"));
    expect(indexOfType("TurnFailed")).toBeLessThan(indexOfType("RunFailed"));
    expect(indexOfType("RunFailed")).toBeLessThan(replacementStartedIndex);
    expect(stoppedTurn).toMatchObject({
      event: { payload: { error: { code: "turn.interrupted" } } },
    });
    const replacementPromptJson = JSON.stringify(Prompt.make(prompts[1] ?? "").content);
    expect(replacementPromptJson).toContain("hello");
    expect(replacementPromptJson).toContain("interrupt");
    expect(cancelledCommandIds).toEqual([command.commandId]);
    expect(completedCommandIds).toEqual([interruptCommand.commandId]);
  });

  it("terminalizes abnormal turn failures and continues queued commands", async () => {
    const firstStream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
    ).pipe(Stream.concat(Stream.die(new Error("provider defect"))));
    const secondStream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );

    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const durableStore = yield* EDASessionStore;

        yield* dispatcher.start({ modelSelection });
        yield* dispatcher.admitCommand(command);
        yield* dispatcher.admitCommand(secondCommand);
        const committed = yield* waitForCommitted(
          durableStore,
          hasCommandCompleted(secondCommand.commandId),
        );
        return committed;
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: [firstStream, secondStream],
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const committed = await Effect.runPromise(program);
    const eventTypes = committed.map((entry) => entry.event.type);
    const commandTerminals = committed.flatMap((entry) =>
      entry.event.type === "CommandFailed" || entry.event.type === "CommandCompleted"
        ? [{ type: entry.event.type, commandId: entry.event.payload.commandId }]
        : [],
    );
    const firstFailedIndex = committed.findIndex(
      (entry) =>
        entry.event.type === "CommandFailed" && entry.event.payload.commandId === command.commandId,
    );
    const secondStartedIndex = committed.findIndex(
      (entry) =>
        entry.event.type === "CommandStarted" &&
        entry.event.payload.commandId === secondCommand.commandId,
    );

    expect(eventTypes).toEqual(
      expect.arrayContaining(["InferenceFailed", "TurnFailed", "RunFailed", "CommandFailed"]),
    );
    expect(commandTerminals).toEqual([
      { type: "CommandFailed", commandId: command.commandId },
      { type: "CommandCompleted", commandId: secondCommand.commandId },
    ]);
    expect(secondStartedIndex).toBeGreaterThan(firstFailedIndex);
  });

  it("restarts unconsumed steering in a new run after unexpected failure", async () => {
    const failGate = await Effect.runPromise(Deferred.make<void>());
    const firstStream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "working" }),
    ).pipe(
      Stream.concat(
        Stream.fromEffect(
          Effect.gen(function* () {
            yield* Deferred.await(failGate);
            return yield* Effect.die(new Error("provider defect"));
          }),
        ),
      ),
    );
    const recoveryStream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );

    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const durableStore = yield* EDASessionStore;
        yield* dispatcher.start({ modelSelection });
        yield* dispatcher.admitCommand(command);
        yield* waitForCommitted(durableStore, hasEventType("InferenceStarted"));
        yield* dispatcher.admitCommand(steerCommand);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* Deferred.succeed(failGate, undefined);
        const committed = yield* waitForCommitted(
          durableStore,
          (entries) =>
            entries.filter((entry) => entry.event.type === "RunStarted").length === 2 &&
            entries.filter((entry) => entry.event.type === "RunCompleted").length === 1,
        );
        return committed;
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: [firstStream, recoveryStream],
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const committed = await Effect.runPromise(program);
    const steerSubmitted = committed.find(
      (entry) =>
        entry.event.type === "UserMessageSubmitted" &&
        entry.event.payload.commandId === steerCommand.commandId,
    );
    const recoveryTurn = committed.filter((entry) => entry.event.type === "TurnStarted")[1];
    expect(
      committed.some(
        (entry) =>
          entry.event.type === "CommandAdmitted" &&
          entry.event.payload.command._tag === "ResumePendingMessages",
      ),
    ).toBe(true);
    expect(recoveryTurn).toMatchObject({
      event: { payload: { inputMessageIds: [steerSubmitted?.event.payload.messageId] } },
    });
  });

  it("processes pending StopTurn before steering continuation after a completed turn", async () => {
    const finishGate = await Effect.runPromise(Deferred.make<void>());
    const firstStream = Stream.make(
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

    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const durableStore = yield* EDASessionStore;

        yield* dispatcher.admitCommand(command);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* waitForCommitted(durableStore, hasEventType("InferenceStarted"));
        yield* dispatcher.admitCommand(steerCommand);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* dispatcher.admitCommand(stopTurnCommand);
        yield* Deferred.succeed(finishGate, undefined);
        yield* waitForCommitted(durableStore, hasEventType("TurnCompleted"));
        const stopDrain = yield* dispatcher.drainReadyWork({ modelSelection });
        const committed = yield* waitForCommitted(durableStore, hasEventType("StopTurnApplied"));
        return { committed, stopDrain };
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: [firstStream],
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const { committed, stopDrain } = await Effect.runPromise(program);
    const eventTypes = committed.map((entry) => entry.event.type);
    const turnStarted = committed.filter((entry) => entry.event.type === "TurnStarted");
    const stopApplied = committed.find((entry) => entry.event.type === "StopTurnApplied");

    expect(stopDrain.processed).toEqual([
      expect.objectContaining({
        outcome: expect.objectContaining({ _tag: "SessionCommandCompleted" }),
      }),
    ]);
    expect(eventTypes).toEqual(
      expect.arrayContaining([
        "UserMessageSubmitted",
        "StopTurnRequested",
        "RunFailed",
        "CommandCancelled",
        "PendingMessagesPaused",
        "StopTurnApplied",
      ]),
    );
    expect(turnStarted).toHaveLength(1);
    expect(stopApplied).toMatchObject({
      event: { payload: { turnId: turnStarted[0]?.event.payload.turnId } },
    });
  });

  it("stops the active scoped run before draining queued work", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
    ).pipe(Stream.concat(Stream.never));

    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const durableStore = yield* EDASessionStore;

      yield* dispatcher.admitCommand(command);
      yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(durableStore, hasEventType("InferenceStarted"));
      yield* dispatcher.admitCommand(secondCommand);
      yield* dispatcher.admitCommand(stopTurnCommand);
      const stopDrain = yield* dispatcher.drainReadyWork({ modelSelection });
      const committed = yield* waitForCommitted(
        durableStore,
        hasEventType("PendingMessagesPaused"),
      );
      const snapshot = yield* dispatcher.snapshot();
      return { committed, snapshot, stopDrain };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [
            COMMAND_ADMITTED_EVENT_ID,
            COMMAND_STARTED_EVENT_ID,
            USER_MESSAGE_ID,
            USER_MESSAGE_EVENT_ID,
            RUN_ID,
            RUN_STARTED_EVENT_ID,
            TURN_ID,
            TURN_STARTED_EVENT_ID,
            INFERENCE_ID,
            INFERENCE_STARTED_EVENT_ID,
            TEXT_EVENT_ID,
            SECOND_COMMAND_ADMITTED_EVENT_ID,
            generatedId(30),
            generatedId(31),
            generatedId(32),
            generatedId(33),
            generatedId(34),
            generatedId(35),
            generatedId(36),
            generatedId(37),
            generatedId(38),
            ...SECOND_COMPLETED_RUN_IDS,
          ],
          stream,
        ),
      ),
    );

    const { committed, snapshot, stopDrain } = await Effect.runPromise(program);
    const eventTypes = committed.map((entry) => entry.event.type);
    const indexOfType = (type: string) => eventTypes.indexOf(type);
    const commandStarted = committed.flatMap((entry, index) =>
      entry.event.type === "CommandStarted"
        ? [{ commandId: entry.event.payload.commandId, index }]
        : [],
    );
    const originalCancelledIndex = committed.findIndex(
      (entry) =>
        entry.event.type === "CommandCancelled" &&
        entry.event.payload.commandId === CommandId.make(COMMAND_ID),
    );
    expect(stopDrain.processed).toEqual([
      expect.objectContaining({
        outcome: expect.objectContaining({ _tag: "SessionCommandCompleted" }),
      }),
    ]);
    expect(indexOfType("StopTurnRequested")).toBeGreaterThan(indexOfType("CommandStarted"));
    expect(indexOfType("InferenceFailed")).toBeGreaterThan(indexOfType("StopTurnRequested"));
    expect(indexOfType("TurnFailed")).toBeGreaterThan(indexOfType("StopTurnRequested"));
    expect(indexOfType("TurnFailed")).toBeGreaterThan(indexOfType("InferenceFailed"));
    expect(indexOfType("TurnFailed")).toBeLessThan(indexOfType("RunFailed"));
    expect(indexOfType("TurnFailed")).toBeLessThan(indexOfType("StopTurnApplied"));
    expect(indexOfType("RunFailed")).toBeGreaterThan(indexOfType("StopTurnRequested"));
    expect(originalCancelledIndex).toBeGreaterThan(indexOfType("StopTurnRequested"));
    expect(indexOfType("StopTurnApplied")).toBeGreaterThan(originalCancelledIndex);
    expect(indexOfType("CommandCompleted")).toBeGreaterThan(indexOfType("StopTurnApplied"));
    expect(
      commandStarted.some((entry) => entry.commandId === CommandId.make(SECOND_COMMAND_ID)),
    ).toBe(false);
    expect(snapshot.commandQueues.pausedQueue.map((message) => message.commandId)).toContain(
      CommandId.make(SECOND_COMMAND_ID),
    );
    expect(committed.find((entry) => entry.event.type === "TurnFailed")).toMatchObject({
      event: { payload: { error: { code: "turn.interrupted" } } },
    });
  });

  it("cancels a pending queue message before it can start", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        yield* dispatcher.admitCommand(command);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* dispatcher.admitCommand(secondCommand);
        const before = yield* dispatcher.snapshot();
        const pending = before.commandQueues.pendingQueue[0]!;
        yield* dispatcher.admitCommand(
          new CancelPendingMessageCommand({
            commandId: CommandId.make("018f6bd5-2f2a-7b1e-8f31-1f2e3d4c5b6a"),
            messageId: pending.messageId,
            reason: "user-cancel",
          }),
        );
        yield* dispatcher.drainReadyWork({ modelSelection });
        return yield* dispatcher.snapshot();
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: Stream.never,
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const snapshot = await Effect.runPromise(program);
    expect(snapshot.commandQueues.pendingQueue).toHaveLength(0);
    expect(snapshot.commands.get(CommandId.make(SECOND_COMMAND_ID))?.terminal?._tag).toBe(
      "Cancelled",
    );
  });

  it("promotes a pending queue message and steers the active run", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        yield* dispatcher.admitCommand(command);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* dispatcher.admitCommand(secondCommand);
        const before = yield* dispatcher.snapshot();
        const pending = before.commandQueues.pendingQueue[0]!;
        yield* dispatcher.admitCommand(
          new PromotePendingMessageCommand({
            commandId: CommandId.make("018f6bd5-2f2a-7b1e-8f32-1f2e3d4c5b6a"),
            messageId: pending.messageId,
          }),
        );
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* dispatcher.drainReadyWork({ modelSelection });
        return yield* dispatcher.snapshot();
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: Stream.never,
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const snapshot = await Effect.runPromise(program);
    expect(snapshot.commandQueues.pendingQueue).toHaveLength(0);
    expect(snapshot.commandQueues.pendingSteers).toEqual([
      expect.objectContaining({ commandId: CommandId.make(SECOND_COMMAND_ID) }),
    ]);
    expect(snapshot.commands.get(CommandId.make(SECOND_COMMAND_ID))?.terminal?._tag).toBe(
      "Completed",
    );
  });

  it("interrupts the active run and immediately resumes one pending steer", async () => {
    const resumeCommandId = CommandId.make("018f6bd5-2f2a-7b1e-8f35-1f2e3d4c5b6a");
    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        yield* dispatcher.admitCommand(command);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* dispatcher.admitCommand(steerCommand);
        yield* dispatcher.drainReadyWork({ modelSelection });
        const pending = (yield* dispatcher.snapshot()).commandQueues.pendingSteers[0]!;
        yield* dispatcher.admitCommand(stopTurnCommand);
        yield* dispatcher.admitCommand(
          new ResumePendingMessagesCommand({
            commandId: resumeCommandId,
            messageIds: [pending.messageId],
          }),
        );
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* dispatcher.drainReadyWork({ modelSelection });
        return { messageId: pending.messageId, snapshot: yield* dispatcher.snapshot() };
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: Stream.never,
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const result = await Effect.runPromise(program);
    expect(result.snapshot.commandQueues.pendingSteers).toHaveLength(0);
    expect(result.snapshot.commandQueues.pausedQueue).toHaveLength(0);
    expect(result.snapshot.commandQueues.active?.commandId).toBe(resumeCommandId);
    expect(result.snapshot.messages.get(result.messageId)?.consumedSeq).toBeDefined();
  });

  it("rejects ordinary queue admission while paused and atomically clears an approved set", async () => {
    const replacement = new SubmitMessageCommand({
      commandId: CommandId.make("018f6bd5-2f2a-7b1e-8f34-1f2e3d4c5b6a"),
      disposition: "queue",
      content: [Prompt.textPart({ text: "replacement" })],
    });
    const program = Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        yield* dispatcher.admitCommand(command);
        yield* dispatcher.drainReadyWork({ modelSelection });
        yield* dispatcher.admitCommand(secondCommand);
        yield* dispatcher.admitCommand(stopTurnCommand);
        yield* dispatcher.drainReadyWork({ modelSelection });
        const paused = yield* dispatcher.snapshot();
        const rejected = yield* Effect.exit(dispatcher.admitCommand(replacement));
        const approvedIds = paused.commandQueues.pausedQueue.map((message) => message.messageId);
        const admitted = yield* dispatcher.admitCommand(
          new SubmitMessageCommand({
            commandId: replacement.commandId,
            disposition: "queue",
            content: replacement.content,
            expectedPausedMessageIdsToCancel: approvedIds,
          }),
        );
        return { admitted, approvedIds, rejected, snapshot: yield* dispatcher.snapshot() };
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: Stream.never,
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const result = await Effect.runPromise(program);
    expect(result.rejected).toMatchObject({
      _tag: "Failure",
      cause: {
        reasons: [{ error: { code: "queue_paused", pausedMessageIds: result.approvedIds } }],
      },
    });
    expect(result.admitted.event.type).toBe("CommandAdmitted");
    expect(result.snapshot.commandQueues.pausedQueue).toHaveLength(0);
    expect(result.snapshot.commandQueues.pendingQueue).toEqual([
      expect.objectContaining({ commandId: replacement.commandId }),
    ]);
  });
});
