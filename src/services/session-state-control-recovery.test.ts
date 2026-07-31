import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";
import { describe, expect, it } from "vite-plus/test";

import { ResumePendingMessagesCommand } from "../types/commands";
import { CompactionId } from "../types/core";
import { FailurePayload } from "../types/events";

import {
  InferenceId,
  CommandId,
  EDASessionStore,
  EventFactory,
  EventId,
  MessageId,
  ProviderPartId,
  RunId,
  SessionId,
  SessionState,
  SequenceNumber,
  ToolCallId,
  ToolName,
  TurnId,
  makeEdaTestLayer,
  SESSION_ID,
  COMMAND_ID,
  SECOND_COMMAND_ID,
  STOP_TURN_COMMAND_ID,
  RUN_ID,
  TURN_ID,
  INFERENCE_ID,
  TOOL_CALL_ID,
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
  INFERENCE_FAILED_EVENT_ID,
  TURN_FAILED_EVENT_ID,
  RUN_FAILED_EVENT_ID,
  COMMAND_FAILED_EVENT_ID,
  generatedId,
  SECOND_COMPLETED_RUN_IDS,
  NoopParams,
  makeTestLayer,
  modelSelection,
  command,
  secondCommand,
  stopTurnCommand,
  usage,
  collectCommitted,
  waitForCommitted,
  hasCommandCompleted,
  hasEventType,
  foldReducedState,
  initialReducedState,
} from "./session-state-control-testkit";

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

describe("SessionState control loop - recovery and queue policy", () => {
  it("does not emit RecoveryCompleted when startup finds no work to repair", async () => {
    const program = Effect.scoped(
      Effect.gen(function* () {
        const sessionState = yield* SessionState;
        const durableStore = yield* EDASessionStore;
        yield* sessionState.start({ modelSelection });
        return yield* collectCommitted(durableStore);
      }),
    ).pipe(Effect.provide(makeEdaTestLayer({ sessionId: SessionId.make(SESSION_ID) })));

    const committed = await Effect.runPromise(program);

    expect(committed.some((entry) => entry.event.type === "RecoveryCompleted")).toBe(false);
  });

  it("recovers stale active durable work on session startup before draining queued commands", async () => {
    const stream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const events = yield* EventFactory;
        const sessionState = yield* SessionState;
        const dispatcher = sessionState;
        const durableStore = yield* EDASessionStore;
        const commandId = CommandId.make(COMMAND_ID);
        const runId = RunId.make(RUN_ID);
        const turnId = TurnId.make(TURN_ID);
        const inferenceId = InferenceId.make(INFERENCE_ID);
        const toolCallId = ToolCallId.make(TOOL_CALL_ID);

        yield* sessionState.appendDurableBatch([
          yield* events.commandAdmitted({ command }),
          yield* events.commandStarted({ commandId }),
          yield* events.userMessageCommitted({
            commandId,
            messageId: MessageId.make(USER_MESSAGE_ID),
            content: command.content,
          }),
          yield* events.runStarted({
            runId,
            commandIds: [commandId],
            modelSelection,
          }),
          yield* events.turnStarted({ runId, turnId }),
          yield* events.inferenceStarted({ runId, turnId, inferenceId }),
          yield* events.toolCallCreated({
            runId,
            turnId,
            inferenceId: inferenceId,
            toolCallId,
            promptPart: Prompt.toolCallPart({
              id: ProviderPartId.make("tool-call-1"),
              name: ToolName.make("noop"),
              params: {},
              providerExecuted: false,
            }),
          }),
          yield* events.toolCallStarted({ toolCallId }),
          yield* events.commandAdmitted({ command: secondCommand }),
        ]);

        yield* dispatcher.start({ modelSelection });
        const committed = yield* waitForCommitted(
          durableStore,
          hasCommandCompleted(CommandId.make(SECOND_COMMAND_ID)),
        );
        return { committed };
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

    const { committed } = await Effect.runPromise(program);
    const eventTypes = committed.map((entry) => entry.event.type);
    const cancelledCommandIds = committed.flatMap((entry) =>
      entry.event.type === "CommandCancelled" ? [entry.event.payload.commandId] : [],
    );
    const toolFailed = committed.find((entry) => entry.event.type === "ToolCallFailed");

    expect(eventTypes).toEqual(
      expect.arrayContaining([
        "ToolCallFailed",
        "InferenceFailed",
        "TurnFailed",
        "RunFailed",
        "RunStarted",
        "RecoveryCompleted",
        "CommandCompleted",
      ]),
    );
    expect(eventTypes.indexOf("ToolCallFailed")).toBeLessThan(eventTypes.indexOf("RunFailed"));
    const recoveryCompletedIndex = committed.findIndex(
      (entry) => entry.event.type === "RecoveryCompleted",
    );
    const replacementRun = committed
      .slice(0, recoveryCompletedIndex)
      .reverse()
      .find((entry) => entry.event.type === "RunStarted");
    const recoveryCompleted = committed[recoveryCompletedIndex];
    expect(recoveryCompleted).toMatchObject({
      event: {
        payload: {
          trigger: "runtime-restart",
          continuation: {
            commandId: CommandId.make(COMMAND_ID),
            interruptedRunId: RunId.make(RUN_ID),
            replacementRunId: replacementRun?.event.payload.runId,
          },
        },
      },
    });
    expect(
      Array.from(foldReducedState(initialReducedState, committed).recoveryContinuations.values()),
    ).toEqual([
      expect.objectContaining({
        commandId: CommandId.make(COMMAND_ID),
        interruptedRunId: RunId.make(RUN_ID),
        replacementRunId: replacementRun?.event.payload.runId,
        seq: recoveryCompleted?.position.seq,
      }),
    ]);
    expect(eventTypes.indexOf("RunStarted")).toBeLessThan(eventTypes.indexOf("RecoveryCompleted"));
    expect(eventTypes.indexOf("RecoveryCompleted")).toBeLessThan(
      eventTypes.indexOf("TurnStarted", eventTypes.indexOf("RecoveryCompleted")),
    );
    expect(cancelledCommandIds).toEqual([]);
    expect(toolFailed).toMatchObject({
      event: {
        payload: {
          promptPart: {
            isFailure: true,
            result: {
              message: "tool call interrupted: runtime restarted before lifecycle completed",
            },
          },
        },
      },
    });
    expect(eventTypes.at(-1)).toBe("CommandCompleted");
  });

  it("fails an open completed-inference tool call before building the recovered prompt", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const stream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const events = yield* EventFactory;
        const sessionState = yield* SessionState;
        const dispatcher = sessionState;
        const durableStore = yield* EDASessionStore;
        const commandId = CommandId.make(COMMAND_ID);
        const runId = RunId.make(RUN_ID);
        const turnId = TurnId.make(TURN_ID);
        const inferenceId = InferenceId.make(INFERENCE_ID);
        const toolCallId = ToolCallId.make(TOOL_CALL_ID);
        const toolCallPart = Prompt.toolCallPart({
          id: ProviderPartId.make("tool-call-1"),
          name: ToolName.make("noop"),
          params: {},
          providerExecuted: false,
        });

        yield* sessionState.appendDurableBatch([
          yield* events.commandAdmitted({ command }),
          yield* events.commandStarted({ commandId }),
          yield* events.userMessageCommitted({
            commandId,
            messageId: MessageId.make(USER_MESSAGE_ID),
            content: command.content,
          }),
          yield* events.runStarted({ runId, commandIds: [commandId], modelSelection }),
          yield* events.turnStarted({ runId, turnId }),
          yield* events.inferenceStarted({ runId, turnId, inferenceId }),
          yield* events.inferenceCompleted({
            runId,
            turnId,
            inferenceId,
            finishReason: "tool-calls",
          }),
          yield* events.toolCallCreated({
            runId,
            turnId,
            inferenceId: inferenceId,
            toolCallId,
            promptPart: toolCallPart,
          }),
          yield* events.assistantMessageCommitted({
            messageId: MessageId.make(ASSISTANT_MESSAGE_ID),
            runId,
            turnId,
            inferenceId: inferenceId,
            promptParts: [toolCallPart],
          }),
          yield* events.commandAdmitted({ command: secondCommand }),
        ]);

        yield* dispatcher.start({ modelSelection });
        const committed = yield* waitForCommitted(
          durableStore,
          hasCommandCompleted(CommandId.make(SECOND_COMMAND_ID)),
        );
        return committed;
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: stream,
          toolSchemas: new Map([["noop", NoopParams]]),
          onStreamText: ({ prompt }) => prompts.push(prompt),
        }),
      ),
    );

    const committed = await Effect.runPromise(program);
    const recoveredPrompt = prompts[0];

    expect(committed.map((entry) => entry.event.type)).toEqual(
      expect.arrayContaining(["ToolCallFailed", "TurnFailed", "RunFailed", "RunStarted"]),
    );
    if (recoveredPrompt === undefined) {
      throw new Error("Expected recovered prompt");
    }
    expectEveryToolCallPaired(recoveredPrompt);
    expect(JSON.stringify(Prompt.make(recoveredPrompt).content)).toContain(
      "tool call interrupted: runtime restarted before lifecycle completed",
    );
  });

  it("consumes the originating queue message when recovering before the first turn starts", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const commandId = CommandId.make(COMMAND_ID);
    const messageId = MessageId.make(USER_MESSAGE_ID);
    const sourceRunId = RunId.make(RUN_ID);
    const compactionId = CompactionId.make(generatedId(207));
    const stream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const events = yield* EventFactory;
        const sessionState = yield* SessionState;
        const durableStore = yield* EDASessionStore;
        yield* sessionState.appendDurableBatch([
          yield* events.commandAdmitted({ command }),
          yield* events.userMessageSubmitted({
            commandId,
            messageId,
            disposition: "queue",
            content: command.content,
          }),
          yield* events.commandStarted({ commandId }),
          yield* events.runStarted({
            runId: sourceRunId,
            commandIds: [commandId],
            modelSelection,
          }),
          yield* events.compactionRequested({
            compactionId,
            sourceFromSeq: SequenceNumber.make(1),
            sourceToSeq: SequenceNumber.make(2),
          }),
          yield* events.compactionStarted({ compactionId }),
        ]);

        yield* sessionState.start({ modelSelection });
        const committed = yield* waitForCommitted(durableStore, hasCommandCompleted(commandId));
        const snapshot = yield* sessionState.snapshot();
        return { committed, snapshot };
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: stream,
          onStreamText: ({ prompt }) => prompts.push(prompt),
        }),
      ),
    );

    const { committed, snapshot } = await Effect.runPromise(program);
    expect(committed.filter((entry) => entry.event.type === "TurnStarted")).toHaveLength(1);
    expect(committed.find((entry) => entry.event.type === "TurnStarted")).toMatchObject({
      event: { payload: { inputMessageIds: [messageId] } },
    });
    const compactionFailedIndex = committed.findIndex(
      (entry) => entry.event.type === "CompactionFailed",
    );
    const turnStartedIndex = committed.findIndex((entry) => entry.event.type === "TurnStarted");
    expect(compactionFailedIndex).toBeGreaterThan(-1);
    expect(turnStartedIndex).toBeGreaterThan(-1);
    expect(compactionFailedIndex).toBeLessThan(turnStartedIndex);
    expect(JSON.stringify(Prompt.make(prompts[0] ?? "").content)).toContain("hello");
    expect(snapshot.commandQueues.pendingQueue).toEqual([]);
    expect(snapshot.messages.get(messageId)).toMatchObject({ consumedSeq: expect.any(Number) });
  });

  it("continues resumed-message work while consuming new and migrated steers at the recovery boundary", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const resumeCommandId = CommandId.make(generatedId(200));
    const firstMessageId = MessageId.make(generatedId(201));
    const secondMessageId = MessageId.make(generatedId(202));
    const pendingSteerMessageId = MessageId.make(generatedId(203));
    const legacySteerMessageId = MessageId.make(generatedId(204));
    const pendingSteerCommandId = CommandId.make(generatedId(205));
    const legacySteerCommandId = CommandId.make(generatedId(206));
    const sourceRunId = RunId.make(RUN_ID);
    const sourceTurnId = TurnId.make(TURN_ID);
    const sourceInferenceId = InferenceId.make(INFERENCE_ID);
    const resumeCommand = new ResumePendingMessagesCommand({
      commandId: resumeCommandId,
      messageIds: [firstMessageId, secondMessageId],
    });
    const stream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const events = yield* EventFactory;
        const sessionState = yield* SessionState;
        const durableStore = yield* EDASessionStore;
        yield* sessionState.appendDurableBatch([
          yield* events.userMessageSubmitted({
            commandId: CommandId.make(COMMAND_ID),
            messageId: firstMessageId,
            disposition: "steer",
            content: command.content,
          }),
          yield* events.userMessageSubmitted({
            commandId: CommandId.make(SECOND_COMMAND_ID),
            messageId: secondMessageId,
            disposition: "steer",
            content: secondCommand.content,
          }),
          yield* events.commandAdmitted({ command: resumeCommand }),
          yield* events.commandStarted({ commandId: resumeCommandId }),
          yield* events.runStarted({
            runId: sourceRunId,
            commandIds: [
              resumeCommandId,
              CommandId.make(COMMAND_ID),
              CommandId.make(SECOND_COMMAND_ID),
            ],
            modelSelection,
          }),
          yield* events.turnStarted({
            runId: sourceRunId,
            turnId: sourceTurnId,
            inputMessageIds: [firstMessageId, secondMessageId],
          }),
          yield* events.inferenceStarted({
            runId: sourceRunId,
            turnId: sourceTurnId,
            inferenceId: sourceInferenceId,
          }),
          yield* events.userMessageSubmitted({
            commandId: pendingSteerCommandId,
            messageId: pendingSteerMessageId,
            disposition: "steer",
            content: [Prompt.textPart({ text: "fresh steer" })],
          }),
          yield* events.steeringMessageQueued({
            commandId: legacySteerCommandId,
            messageId: legacySteerMessageId,
            runId: sourceRunId,
            content: [Prompt.textPart({ text: "legacy steer" })],
          }),
        ]);

        yield* sessionState.start({ modelSelection });
        const committed = yield* waitForCommitted(
          durableStore,
          hasCommandCompleted(resumeCommandId),
        );
        return committed;
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: stream,
          onStreamText: ({ prompt }) => prompts.push(prompt),
        }),
      ),
    );

    const committed = await Effect.runPromise(program);
    const reduced = foldReducedState(initialReducedState, committed);
    const runStarts = committed.filter((entry) => entry.event.type === "RunStarted");
    const resumedRun = runStarts.at(-1);
    expect(runStarts).toHaveLength(2);
    expect(resumedRun).toMatchObject({
      event: {
        payload: {
          commandIds: [
            resumeCommandId,
            CommandId.make(COMMAND_ID),
            CommandId.make(SECOND_COMMAND_ID),
          ],
        },
      },
    });
    expect(committed.filter((entry) => entry.event.type === "TurnStarted").at(-1)).toMatchObject({
      event: { payload: { inputMessageIds: [pendingSteerMessageId, legacySteerMessageId] } },
    });
    expect(reduced.messages.get(firstMessageId)).toMatchObject({
      consumedTurnId: sourceTurnId,
    });
    expect(reduced.messages.get(secondMessageId)).toMatchObject({
      consumedTurnId: sourceTurnId,
    });
    expect(
      committed.some(
        (entry) =>
          entry.event.type === "CommandCancelled" &&
          entry.event.payload.commandId === resumeCommandId,
      ),
    ).toBe(false);
    const recoveredPrompt = JSON.stringify(Prompt.make(prompts[0] ?? "").content);
    expect(recoveredPrompt).toContain("hello");
    expect(recoveredPrompt).toContain("second");
    expect(recoveredPrompt).toContain("fresh steer");
    expect(recoveredPrompt).toContain("legacy steer");
  });

  it("preserves targeted legacy steer ownership across a second pre-turn restart", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const commandId = CommandId.make(COMMAND_ID);
    const sourceRunId = RunId.make(RUN_ID);
    const replacementRunId = RunId.make(generatedId(220));
    const legacySteerMessageId = MessageId.make(generatedId(221));
    const legacySteerCommandId = CommandId.make(generatedId(222));
    const stream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const events = yield* EventFactory;
        const sessionState = yield* SessionState;
        const durableStore = yield* EDASessionStore;
        yield* sessionState.appendDurableBatch([
          yield* events.commandAdmitted({ command }),
          yield* events.commandStarted({ commandId }),
          yield* events.userMessageCommitted({
            commandId,
            messageId: MessageId.make(USER_MESSAGE_ID),
            content: command.content,
          }),
          yield* events.runStarted({
            runId: sourceRunId,
            commandIds: [commandId],
            modelSelection,
          }),
          yield* events.runFailed({
            runId: sourceRunId,
            error: FailurePayload.make({
              message: "run interrupted: runtime restarted before lifecycle completed",
              code: "run.interrupted",
            }),
          }),
          yield* events.runStarted({
            runId: replacementRunId,
            commandIds: [commandId],
            modelSelection,
          }),
          yield* events.steeringMessageQueued({
            commandId: legacySteerCommandId,
            messageId: legacySteerMessageId,
            runId: sourceRunId,
            content: [Prompt.textPart({ text: "legacy steer survives" })],
          }),
        ]);

        yield* sessionState.start({ modelSelection });
        return yield* waitForCommitted(durableStore, hasCommandCompleted(commandId));
      }),
    ).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: stream,
          onStreamText: ({ prompt }) => prompts.push(prompt),
        }),
      ),
    );

    const committed = await Effect.runPromise(program);
    expect(
      committed.some(
        (entry) =>
          entry.event.type === "CommandAdmitted" &&
          entry.event.payload.command._tag === "ResumePendingMessages",
      ),
    ).toBe(false);
    expect(committed.filter((entry) => entry.event.type === "TurnStarted").at(-1)).toMatchObject({
      event: {
        payload: {
          inputMessageIds: [MessageId.make(USER_MESSAGE_ID), legacySteerMessageId],
        },
      },
    });
    expect(JSON.stringify(Prompt.make(prompts[0] ?? "").content)).toContain(
      "legacy steer survives",
    );
  });

  it("treats stale active durable work in the post-recovery loop as fatal", async () => {
    const program = Effect.gen(function* () {
      const events = yield* EventFactory;
      const sessionState = yield* SessionState;
      const durableStore = yield* EDASessionStore;
      const commandId = CommandId.make(COMMAND_ID);
      const runId = RunId.make(RUN_ID);
      const turnId = TurnId.make(TURN_ID);
      const inferenceId = InferenceId.make(INFERENCE_ID);
      const toolCallId = ToolCallId.make(TOOL_CALL_ID);

      yield* sessionState.appendDurableBatch([
        yield* events.commandAdmitted({ command }),
        yield* events.commandStarted({ commandId }),
        yield* events.userMessageCommitted({
          commandId,
          messageId: MessageId.make(USER_MESSAGE_ID),
          content: command.content,
        }),
        yield* events.runStarted({ runId, commandIds: [commandId], modelSelection }),
        yield* events.turnStarted({ runId, turnId }),
        yield* events.inferenceStarted({ runId, turnId, inferenceId }),
        yield* events.toolCallCreated({
          runId,
          turnId,
          inferenceId: inferenceId,
          toolCallId,
          promptPart: Prompt.toolCallPart({
            id: ProviderPartId.make("tool-call-1"),
            name: ToolName.make("noop"),
            params: {},
            providerExecuted: true,
          }),
        }),
      ]);

      const drainExit = yield* Effect.exit(sessionState.drainReadyWork({ modelSelection }));
      const laterAdmitExit = yield* Effect.exit(sessionState.admitCommand(secondCommand));
      const committed = yield* collectCommitted(durableStore);
      return { committed, drainExit, laterAdmitExit };
    }).pipe(
      Effect.provide(
        makeEdaTestLayer({
          sessionId: SessionId.make(SESSION_ID),
          parts: Stream.empty,
          toolSchemas: new Map([["noop", NoopParams]]),
        }),
      ),
    );

    const { committed, drainExit, laterAdmitExit } = await Effect.runPromise(program);
    const eventTypes = committed.map((entry) => entry.event.type);

    expect(eventTypes).not.toContain("CommandCancelled");
    expectFailure(drainExit, "SessionState live invariant violated");
    expectFailure(laterAdmitExit, "SessionState live invariant violated");
  });

  it("continues with the next queued command after the active command reaches a terminal boundary", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const durableStore = yield* EDASessionStore;
      yield* dispatcher.admitCommand(command);
      yield* dispatcher.admitCommand(secondCommand);
      const firstDrain = yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(durableStore, hasEventType("TurnCompleted"));
      const secondDrain = yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(
        durableStore,
        (entries) => entries.filter((entry) => entry.event.type === "TurnCompleted").length >= 2,
      );
      yield* dispatcher.drainReadyWork({ modelSelection });
      const committed = yield* waitForCommitted(
        durableStore,
        hasCommandCompleted(CommandId.make(SECOND_COMMAND_ID)),
      );
      return { firstDrain, secondDrain, committed };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [
            COMMAND_ADMITTED_EVENT_ID,
            USER_MESSAGE_ID,
            USER_MESSAGE_EVENT_ID,
            SECOND_COMMAND_ADMITTED_EVENT_ID,
            generatedId(90),
            generatedId(91),
            COMMAND_STARTED_EVENT_ID,
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
            ...SECOND_COMPLETED_RUN_IDS,
          ],
          stream,
        ),
      ),
    );

    const { firstDrain, secondDrain, committed } = await Effect.runPromise(program);
    const startedCommandIds = committed.flatMap((entry) =>
      entry.event.type === "CommandStarted" ? [entry.event.payload.commandId] : [],
    );
    const completedCommandIds = committed.flatMap((entry) =>
      entry.event.type === "CommandCompleted" ? [entry.event.payload.commandId] : [],
    );

    expect(firstDrain).toMatchObject({
      _tag: "SessionDrainResult",
      processed: [{ _tag: "SessionForkedCommand", commandId: CommandId.make(COMMAND_ID) }],
      stop: { reason: "active-command" },
    });
    expect(secondDrain).toMatchObject({
      _tag: "SessionDrainResult",
      processed: [
        { outcome: { _tag: "SessionCommandCompleted" } },
        { _tag: "SessionForkedCommand", commandId: CommandId.make(SECOND_COMMAND_ID) },
      ],
      stop: { reason: "active-command" },
    });
    expect(startedCommandIds).toEqual([
      CommandId.make(COMMAND_ID),
      CommandId.make(SECOND_COMMAND_ID),
    ]);
    expect(completedCommandIds).toEqual([
      CommandId.make(COMMAND_ID),
      CommandId.make(SECOND_COMMAND_ID),
    ]);
  });

  it("leaves queued commands pending while a command is active", async () => {
    const stream = Stream.never;
    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const durableStore = yield* EDASessionStore;
      yield* dispatcher.admitCommand(command);
      yield* dispatcher.admitCommand(secondCommand);
      const firstDrain = yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(durableStore, hasEventType("InferenceStarted"));
      const secondDrain = yield* dispatcher.drainReadyWork({ modelSelection });
      const committed = yield* collectCommitted(durableStore);
      return { firstDrain, secondDrain, committed };
    }).pipe(
      Effect.provide(
        makeTestLayer(
          [
            COMMAND_ADMITTED_EVENT_ID,
            USER_MESSAGE_ID,
            USER_MESSAGE_EVENT_ID,
            SECOND_COMMAND_ADMITTED_EVENT_ID,
            generatedId(90),
            generatedId(91),
            COMMAND_STARTED_EVENT_ID,
            RUN_ID,
            RUN_STARTED_EVENT_ID,
            TURN_ID,
            TURN_STARTED_EVENT_ID,
            INFERENCE_ID,
            INFERENCE_STARTED_EVENT_ID,
          ],
          stream,
        ),
      ),
    );

    const { firstDrain, secondDrain, committed } = await Effect.runPromise(program);
    const startedCommandIds = committed.flatMap((entry) =>
      entry.event.type === "CommandStarted" ? [entry.event.payload.commandId] : [],
    );

    expect(firstDrain).toMatchObject({
      _tag: "SessionDrainResult",
      processed: [{ _tag: "SessionForkedCommand", commandId: CommandId.make(COMMAND_ID) }],
      stop: { reason: "active-command", active: { commandId: CommandId.make(COMMAND_ID) } },
    });
    expect(secondDrain).toMatchObject({
      _tag: "SessionDrainResult",
      processed: [],
      stop: {
        reason: "active-command",
        active: { commandId: CommandId.make(COMMAND_ID) },
      },
    });
    expect(startedCommandIds).toEqual([CommandId.make(COMMAND_ID)]);
  });

  it("cancels StopTurn when no command is active", async () => {
    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const durableStore = yield* EDASessionStore;
      yield* dispatcher.admitCommand(stopTurnCommand);
      const firstDrain = yield* dispatcher.drainReadyWork({ modelSelection });
      const committed = yield* waitForCommitted(durableStore, hasEventType("CommandCancelled"));
      const secondDrain = yield* dispatcher.drainReadyWork({ modelSelection });
      return { firstDrain, secondDrain, committed };
    }).pipe(
      Effect.provide(
        makeTestLayer([generatedId(20), generatedId(21), generatedId(22)], Stream.empty),
      ),
    );

    const { firstDrain, secondDrain, committed } = await Effect.runPromise(program);

    expect(firstDrain).toMatchObject({
      _tag: "SessionDrainResult",
      processed: [
        {
          outcome: {
            _tag: "SessionCommandCancelled",
            committed: {
              event: {
                payload: { commandId: CommandId.make(STOP_TURN_COMMAND_ID) },
              },
            },
          },
        },
      ],
      stop: { reason: "no-pending-command" },
    });
    expect(secondDrain).toMatchObject({
      _tag: "SessionDrainResult",
      processed: [],
      stop: { reason: "no-pending-command" },
    });
    expect(committed.map((entry) => entry.event.type)).toEqual([
      "CommandAdmitted",
      "CommandStarted",
      "CommandCancelled",
    ]);
    expect(committed[2]).toMatchObject({
      event: {
        type: "CommandCancelled",
        payload: { commandId: CommandId.make(STOP_TURN_COMMAND_ID), reason: "no active turn" },
      },
    });
  });

  it("commits CommandFailed when the inward run fails", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
    ).pipe(Stream.concat(Stream.fail(new Error("provider failed"))));
    const program = Effect.gen(function* () {
      const dispatcher = yield* SessionState;
      const durableStore = yield* EDASessionStore;
      yield* dispatcher.admitCommand(command);
      yield* dispatcher.drainReadyWork({ modelSelection });
      yield* waitForCommitted(durableStore, hasEventType("TurnFailed"));
      yield* dispatcher.drainReadyWork({ modelSelection });
      const committed = yield* waitForCommitted(durableStore, hasEventType("CommandFailed"));
      const failed = committed.find((entry) => entry.event.type === "CommandFailed");
      if (failed === undefined) {
        return yield* Effect.die(new Error("Expected CommandFailed event"));
      }
      return failed;
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
            ASSISTANT_MESSAGE_ID,
            ASSISTANT_MESSAGE_EVENT_ID,
            INFERENCE_FAILED_EVENT_ID,
            TURN_FAILED_EVENT_ID,
            RUN_FAILED_EVENT_ID,
            COMMAND_FAILED_EVENT_ID,
          ],
          stream,
        ),
      ),
    );

    const result = await Effect.runPromise(program);

    expect(result).toMatchObject({
      event: {
        type: "CommandFailed",
        eventId: EventId.make(COMMAND_FAILED_EVENT_ID),
        payload: { error: { message: "provider failed" } },
      },
    });
  });
});

const expectFailure = <A, E>(exit: Exit.Exit<A, E>, message: string) => {
  expect(Exit.isFailure(exit)).toBe(true);
  expect(Exit.isFailure(exit) ? Cause.pretty(exit.cause) : "").toContain(message);
};
