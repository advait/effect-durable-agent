import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import {
  InferenceId,
  CommandId,
  EDASessionStore,
  EventFactory,
  MessageId,
  ProviderPartId,
  RunId,
  SessionId,
  SessionState,
  ToolCallId,
  ToolName,
  TurnId,
  makeEdaTestLayer,
  SESSION_ID,
  COMMAND_ID,
  SECOND_COMMAND_ID,
  RUN_ID,
  TURN_ID,
  INFERENCE_ID,
  TOOL_CALL_ID,
  USER_MESSAGE_ID,
  NoopParams,
  modelSelection,
  command,
  secondCommand,
  collectCommitted,
} from "./session-state-control-testkit";
import type { EDADurableEvent } from "../types/events";

interface PartialRecoveryCase {
  readonly name: string;
  readonly staleEventTypes: ReadonlyArray<string>;
  readonly expectedRepairTypes: ReadonlyArray<string>;
}

const cases: ReadonlyArray<PartialRecoveryCase> = [
  {
    name: "CommandStarted without RunStarted",
    staleEventTypes: ["CommandAdmitted", "CommandStarted"],
    expectedRepairTypes: ["CommandCancelled"],
  },
  {
    name: "RunStarted without TurnStarted",
    staleEventTypes: ["CommandAdmitted", "CommandStarted", "UserMessageCommitted", "RunStarted"],
    expectedRepairTypes: ["RunFailed"],
  },
  {
    name: "TurnStarted without InferenceStarted",
    staleEventTypes: [
      "CommandAdmitted",
      "CommandStarted",
      "UserMessageCommitted",
      "RunStarted",
      "TurnStarted",
    ],
    expectedRepairTypes: ["TurnFailed", "RunFailed"],
  },
  {
    name: "InferenceStarted without inference terminal",
    staleEventTypes: [
      "CommandAdmitted",
      "CommandStarted",
      "UserMessageCommitted",
      "RunStarted",
      "TurnStarted",
      "InferenceStarted",
    ],
    expectedRepairTypes: ["InferenceFailed", "TurnFailed", "RunFailed"],
  },
  {
    name: "ToolCallStarted without tool terminal",
    staleEventTypes: [
      "CommandAdmitted",
      "CommandStarted",
      "UserMessageCommitted",
      "RunStarted",
      "TurnStarted",
      "InferenceStarted",
      "ToolCallCreated",
      "ToolCallStarted",
    ],
    expectedRepairTypes: ["ToolCallFailed", "InferenceFailed", "TurnFailed", "RunFailed"],
  },
];

describe("SessionState partial stale lifecycle recovery", () => {
  for (const recoveryCase of cases) {
    it(`repairs ${recoveryCase.name} before resuming or starting queued work`, async () => {
      const program = Effect.gen(function* () {
        const events = yield* EventFactory;
        const sessionState = yield* SessionState;
        const durableStore = yield* EDASessionStore;
        const staleEvents = yield* makeStaleEvents(events, recoveryCase.staleEventTypes);

        yield* sessionState.appendDurableBatch([
          ...staleEvents,
          yield* events.commandAdmitted({ command: secondCommand }),
        ]);
        yield* sessionState.start({ modelSelection });
        yield* sessionState.drainReadyWork({ modelSelection });
        const committed = yield* collectCommitted(durableStore);
        return { committed };
      }).pipe(
        Effect.provide(
          makeEdaTestLayer({
            sessionId: SessionId.make(SESSION_ID),
            parts: Stream.never,
            toolSchemas: new Map([["noop", NoopParams]]),
          }),
        ),
      );

      const { committed } = await Effect.runPromise(program);
      const eventTypes = committed.map((entry) => entry.event.type);
      const recoveryCompleted = committed.filter(
        (entry) => entry.event.type === "RecoveryCompleted",
      );
      const secondCommandStartedIndex = committed.findIndex(
        (entry) =>
          entry.event.type === "CommandStarted" &&
          entry.event.payload.commandId === CommandId.make(SECOND_COMMAND_ID),
      );

      const resumesOriginal = recoveryCase.staleEventTypes.includes("RunStarted");
      expect(recoveryCompleted).toHaveLength(1);
      if (resumesOriginal) {
        expect(secondCommandStartedIndex).toBe(-1);
        expect(eventTypes.filter((type) => type === "RunStarted")).toHaveLength(2);
        expect(recoveryCompleted[0]).toMatchObject({
          event: {
            payload: {
              trigger: "runtime-restart",
              continuation: {
                commandId: CommandId.make(COMMAND_ID),
                interruptedRunId: RunId.make(RUN_ID),
                replacementRunId: expect.any(String),
              },
            },
          },
        });
      } else {
        expect(secondCommandStartedIndex).toBeGreaterThan(-1);
        expect(recoveryCompleted[0]).toMatchObject({
          event: { payload: { trigger: "runtime-restart" } },
        });
        expect(recoveryCompleted[0]?.event.payload).not.toHaveProperty("continuation");
      }
      for (const repairType of recoveryCase.expectedRepairTypes) {
        const repairIndex = eventTypes.indexOf(repairType);
        expect(repairIndex).toBeGreaterThan(-1);
        if (!resumesOriginal) {
          expect(repairIndex).toBeLessThan(secondCommandStartedIndex);
        }
      }
      if (!resumesOriginal) {
        expect(eventTypes.indexOf("CommandCancelled")).toBeLessThan(secondCommandStartedIndex);
        expect(committed.find((entry) => entry.event.type === "CommandCancelled")).toMatchObject({
          event: {
            payload: {
              commandId: CommandId.make(COMMAND_ID),
              reason: "runtime restarted before lifecycle completed",
            },
          },
        });
      }
    });
  }
});

const makeStaleEvents = (
  events: EventFactory,
  eventTypes: ReadonlyArray<string>,
): Effect.Effect<ReadonlyArray<EDADurableEvent>> =>
  Effect.gen(function* () {
    const commandId = CommandId.make(COMMAND_ID);
    const runId = RunId.make(RUN_ID);
    const turnId = TurnId.make(TURN_ID);
    const inferenceId = InferenceId.make(INFERENCE_ID);
    const toolCallId = ToolCallId.make(TOOL_CALL_ID);
    const output: Array<EDADurableEvent> = [];

    for (const eventType of eventTypes) {
      switch (eventType) {
        case "CommandAdmitted":
          output.push(yield* events.commandAdmitted({ command }));
          break;
        case "CommandStarted":
          output.push(yield* events.commandStarted({ commandId }));
          break;
        case "UserMessageCommitted":
          output.push(
            yield* events.userMessageCommitted({
              commandId,
              messageId: MessageId.make(USER_MESSAGE_ID),
              content: command.content,
            }),
          );
          break;
        case "RunStarted":
          output.push(yield* events.runStarted({ runId, commandIds: [commandId], modelSelection }));
          break;
        case "TurnStarted":
          output.push(yield* events.turnStarted({ runId, turnId }));
          break;
        case "InferenceStarted":
          output.push(yield* events.inferenceStarted({ runId, turnId, inferenceId: inferenceId }));
          break;
        case "ToolCallCreated":
          output.push(
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
          );
          break;
        case "ToolCallStarted":
          output.push(yield* events.toolCallStarted({ toolCallId }));
          break;
        default:
          return yield* Effect.die(new Error(`Unsupported stale event ${eventType}`));
      }
    }
    return output;
  });
