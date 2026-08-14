import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { decideDispatch } from "./dispatch-policy";
import { emptyCommandQueues } from "./command-queues";
import type { PendingCommand, ReducedState } from "./reduced-state";
import { initialReducedState } from "./reduced-state";
import { StopTurnCommand, SubmitMessageCommand } from "../types/commands";
import { CommandId, RunId, SequenceNumber } from "../types/core";

const COMMAND_A = CommandId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const COMMAND_B = CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const COMMAND_C = CommandId.make("018f6bd5-2f2a-7b1e-8f1c-1f2e3d4c5b6a");
const RUN_ID = RunId.make("018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a");

const seq = (value: number) => SequenceNumber.make(value);
const content = (text: string) => [Prompt.textPart({ text })];

const submit = (commandId: CommandId, disposition: "queue" | "steer" | "interrupt") =>
  new SubmitMessageCommand({
    commandId,
    disposition,
    content: content(`${disposition}-${commandId}`),
  });

const stop = (commandId: CommandId) => new StopTurnCommand({ commandId });

const pending = (
  commandId: CommandId,
  command: PendingCommand["command"],
  admittedSeq: number,
): PendingCommand => ({ commandId, command, admittedSeq: seq(admittedSeq) });

const state = (queues: Partial<ReducedState["commandQueues"]>): ReducedState => ({
  ...initialReducedState,
  commandQueues: { ...emptyCommandQueues, ...queues },
});

describe("decideDispatch", () => {
  it("returns no-pending-command when idle and no commands are pending", () => {
    expect(decideDispatch(state({}))).toEqual({ _tag: "DispatchNoPendingCommand" });
  });

  it("starts the first pending command when idle", () => {
    const first = pending(COMMAND_B, submit(COMMAND_B, "queue"), 1);
    const second = pending(COMMAND_A, submit(COMMAND_A, "queue"), 2);

    expect(decideDispatch(state({ pendingCommands: [first, second] }))).toEqual({
      _tag: "DispatchStartCommand",
      command: { commandId: COMMAND_B, command: first.command },
    });
  });

  it("blocks queued work while a command is active and no control command is pending", () => {
    const queued = pending(COMMAND_B, submit(COMMAND_B, "queue"), 2);

    expect(
      decideDispatch(
        state({
          active: { commandId: COMMAND_A, runId: RUN_ID },
          pendingCommands: [queued],
          queuedCommands: [queued],
        }),
      ),
    ).toEqual({
      _tag: "DispatchBlockedByActiveCommand",
      active: { _tag: "DispatchActiveCommand", commandId: COMMAND_A, runId: RUN_ID },
    });
  });

  it("selects StopTurn as an active control command", () => {
    const stopCommand = pending(COMMAND_B, stop(COMMAND_B), 2);

    expect(
      decideDispatch(
        state({
          active: { commandId: COMMAND_A, runId: RUN_ID },
          pendingCommands: [stopCommand],
          activeControlCommands: [stopCommand],
        }),
      ),
    ).toEqual({
      _tag: "DispatchStopCommand",
      command: { commandId: COMMAND_B, command: stopCommand.command },
      active: { _tag: "DispatchActiveCommand", commandId: COMMAND_A, runId: RUN_ID },
    });
  });

  it("selects steer as an active control command when the active run is known", () => {
    const steerCommand = pending(COMMAND_B, submit(COMMAND_B, "steer"), 2);

    expect(
      decideDispatch(
        state({
          active: { commandId: COMMAND_A, runId: RUN_ID },
          pendingCommands: [steerCommand],
          activeControlCommands: [steerCommand],
        }),
      ),
    ).toEqual({
      _tag: "DispatchSteerCommand",
      command: { commandId: COMMAND_B, command: steerCommand.command },
      active: { _tag: "DispatchActiveCommand", commandId: COMMAND_A, runId: RUN_ID },
    });
  });

  it("blocks steer when the active command has no open run yet", () => {
    const steerCommand = pending(COMMAND_B, submit(COMMAND_B, "steer"), 2);

    expect(
      decideDispatch(
        state({
          active: { commandId: COMMAND_A },
          pendingCommands: [steerCommand],
          activeControlCommands: [steerCommand],
        }),
      ),
    ).toEqual({
      _tag: "DispatchBlockedByActiveCommand",
      active: { _tag: "DispatchActiveCommand", commandId: COMMAND_A },
    });
  });

  it("selects interrupt as an active control command", () => {
    const interruptCommand = pending(COMMAND_B, submit(COMMAND_B, "interrupt"), 2);

    expect(
      decideDispatch(
        state({
          active: { commandId: COMMAND_A, runId: RUN_ID },
          pendingCommands: [interruptCommand],
          activeControlCommands: [interruptCommand],
        }),
      ),
    ).toEqual({
      _tag: "DispatchInterruptCommand",
      command: { commandId: COMMAND_B, command: interruptCommand.command },
      active: { _tag: "DispatchActiveCommand", commandId: COMMAND_A, runId: RUN_ID },
    });
  });

  it("uses the first active-control command in FIFO order when multiple controls are pending", () => {
    const steerCommand = pending(COMMAND_B, submit(COMMAND_B, "steer"), 2);
    const interruptCommand = pending(COMMAND_C, submit(COMMAND_C, "interrupt"), 3);

    expect(
      decideDispatch(
        state({
          active: { commandId: COMMAND_A, runId: RUN_ID },
          pendingCommands: [steerCommand, interruptCommand],
          activeControlCommands: [steerCommand, interruptCommand],
        }),
      ),
    ).toEqual({
      _tag: "DispatchSteerCommand",
      command: { commandId: COMMAND_B, command: steerCommand.command },
      active: { _tag: "DispatchActiveCommand", commandId: COMMAND_A, runId: RUN_ID },
    });
  });
});
