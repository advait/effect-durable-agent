import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { deriveCommandQueues } from "./command-queues";
import type { CommandRecord, MessageRecord, RunRecord } from "./reduced-state";
import { StopTurnCommand, SubmitMessageCommand } from "../types/commands";
import { CommandId, MessageId, RunId, SequenceNumber, TurnId } from "../types/core";

const COMMAND_A = CommandId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const COMMAND_B = CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const COMMAND_C = CommandId.make("018f6bd5-2f2a-7b1e-8f1c-1f2e3d4c5b6a");
const COMMAND_D = CommandId.make("018f6bd5-2f2a-7b1e-8f1d-1f2e3d4c5b6a");
const COMMAND_E = CommandId.make("018f6bd5-2f2a-7b1e-8f1e-1f2e3d4c5b6a");
const RUN_A = RunId.make("018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a");
const RUN_B = RunId.make("018f6bd5-2f2a-7b1e-9f1b-1f2e3d4c5b6a");
const MESSAGE_A = MessageId.make("018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a");
const MESSAGE_B = MessageId.make("018f6bd5-2f2a-7b1e-af1b-1f2e3d4c5b6a");
const MESSAGE_C = MessageId.make("018f6bd5-2f2a-7b1e-af1c-1f2e3d4c5b6a");
const MESSAGE_D = MessageId.make("018f6bd5-2f2a-7b1e-af1d-1f2e3d4c5b6a");
const MESSAGE_E = MessageId.make("018f6bd5-2f2a-7b1e-af1e-1f2e3d4c5b6a");
const TURN_A = TurnId.make("018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a");

const seq = (value: number) => SequenceNumber.make(value);
const content = (text: string) => [Prompt.textPart({ text })];

const submit = (commandId: CommandId, disposition: "queue" | "steer" | "interrupt") =>
  new SubmitMessageCommand({
    commandId,
    disposition,
    content: content(`${disposition}-${commandId}`),
  });

const stop = (commandId: CommandId) => new StopTurnCommand({ commandId });

const commandRecord = (
  commandId: CommandId,
  command: CommandRecord["command"],
  admittedSeq: number,
  extras: Partial<CommandRecord> = {},
): CommandRecord => ({ commandId, command, admittedSeq: seq(admittedSeq), ...extras });

const runRecord = (
  runId: RunId,
  commandIds: ReadonlyArray<CommandId>,
  extras: Partial<RunRecord> = {},
): RunRecord => ({ runId, commandIds, startedSeq: seq(10), ...extras });

const steeringRecord = (
  messageId: MessageId,
  runId: RunId,
  queuedSeq: number,
  extras: Partial<Extract<MessageRecord, { readonly _tag: "Steering" }>> = {},
): MessageRecord => ({
  _tag: "Steering",
  messageId,
  commandId: COMMAND_B,
  runId,
  content: content(`steer-${queuedSeq}`),
  seq: seq(queuedSeq),
  ...extras,
});

describe("deriveCommandQueues", () => {
  it("sorts pending commands by admitted sequence and partitions queued vs active-control commands", () => {
    const queueCommand = submit(COMMAND_A, "queue");
    const steerCommand = submit(COMMAND_B, "steer");
    const interruptCommand = submit(COMMAND_C, "interrupt");
    const stopCommand = stop(COMMAND_D);

    const queues = deriveCommandQueues({
      commands: new Map([
        [COMMAND_A, commandRecord(COMMAND_A, queueCommand, 40)],
        [COMMAND_B, commandRecord(COMMAND_B, steerCommand, 10)],
        [COMMAND_C, commandRecord(COMMAND_C, interruptCommand, 30)],
        [COMMAND_D, commandRecord(COMMAND_D, stopCommand, 20)],
      ]),
      runs: new Map(),
      messages: new Map(),
    });

    expect(queues.pendingCommands.map((entry) => entry.commandId)).toEqual([
      COMMAND_B,
      COMMAND_D,
      COMMAND_C,
      COMMAND_A,
    ]);
    expect(queues.queuedCommands.map((entry) => entry.commandId)).toEqual([COMMAND_A]);
    expect(queues.activeControlCommands.map((entry) => entry.commandId)).toEqual([
      COMMAND_B,
      COMMAND_D,
      COMMAND_C,
    ]);
  });

  it("excludes started and terminal commands from pending queues", () => {
    const pending = submit(COMMAND_A, "queue");
    const started = submit(COMMAND_B, "queue");
    const terminal = submit(COMMAND_C, "queue");

    const queues = deriveCommandQueues({
      commands: new Map([
        [COMMAND_A, commandRecord(COMMAND_A, pending, 1)],
        [COMMAND_B, commandRecord(COMMAND_B, started, 2, { startedSeq: seq(5) })],
        [
          COMMAND_C,
          commandRecord(COMMAND_C, terminal, 3, {
            startedSeq: seq(4),
            terminal: { _tag: "Completed", seq: seq(8) },
          }),
        ],
      ]),
      runs: new Map(),
      messages: new Map(),
    });

    expect(queues.pendingCommands.map((entry) => entry.commandId)).toEqual([COMMAND_A]);
    expect(queues.queuedCommands.map((entry) => entry.commandId)).toEqual([COMMAND_A]);
    expect(queues.activeControlCommands).toEqual([]);
  });

  it("derives active command evidence without an open run", () => {
    const queues = deriveCommandQueues({
      commands: new Map([
        [
          COMMAND_A,
          commandRecord(COMMAND_A, submit(COMMAND_A, "queue"), 1, { startedSeq: seq(2) }),
        ],
      ]),
      runs: new Map(),
      messages: new Map(),
    });

    expect(queues.active).toEqual({ commandId: COMMAND_A });
  });

  it("derives active command evidence with the matching open run", () => {
    const queues = deriveCommandQueues({
      commands: new Map([
        [
          COMMAND_A,
          commandRecord(COMMAND_A, submit(COMMAND_A, "queue"), 1, { startedSeq: seq(2) }),
        ],
      ]),
      runs: new Map([[RUN_A, runRecord(RUN_A, [COMMAND_A])]]),
      messages: new Map(),
    });

    expect(queues.active).toEqual({ commandId: COMMAND_A, runId: RUN_A });
  });

  it("ignores terminal runs when deriving active command evidence", () => {
    const queues = deriveCommandQueues({
      commands: new Map([
        [
          COMMAND_A,
          commandRecord(COMMAND_A, submit(COMMAND_A, "queue"), 1, { startedSeq: seq(2) }),
        ],
      ]),
      runs: new Map([
        [RUN_A, runRecord(RUN_A, [COMMAND_A], { terminal: { _tag: "Completed", seq: seq(9) } })],
      ]),
      messages: new Map(),
    });

    expect(queues.active).toEqual({ commandId: COMMAND_A });
  });

  it("groups pending steering by run, sorts by queued sequence, and excludes consumed or cancelled steering", () => {
    const consumed = steeringRecord(MESSAGE_C, RUN_A, 5, {
      consumedSeq: seq(9),
      consumedTurnId: TURN_A,
    });
    const cancelled = steeringRecord(MESSAGE_D, RUN_B, 6, {
      cancelledSeq: seq(10),
      cancellationReason: "stale",
    });

    const queues = deriveCommandQueues({
      commands: new Map(),
      runs: new Map(),
      messages: new Map([
        [MESSAGE_A, steeringRecord(MESSAGE_A, RUN_A, 4)],
        [MESSAGE_B, steeringRecord(MESSAGE_B, RUN_A, 2)],
        [MESSAGE_C, consumed],
        [MESSAGE_D, cancelled],
        [
          MESSAGE_E,
          {
            _tag: "User",
            messageId: MESSAGE_E,
            commandId: COMMAND_E,
            content: content("not-steering"),
            seq: seq(1),
          },
        ],
      ]),
    });

    expect(queues.steeringByRun.get(RUN_A)?.map((entry) => entry.messageId)).toEqual([
      MESSAGE_B,
      MESSAGE_A,
    ]);
    expect(queues.steeringByRun.has(RUN_B)).toBe(false);
  });
});
