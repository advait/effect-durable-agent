import * as fc from "fast-check";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { emptyCommandQueues } from "./command-queues";
import { decideDispatch } from "./dispatch-policy";
import type { PendingCommand, ReducedState } from "./reduced-state";
import { initialReducedState } from "./reduced-state";
import { StopTurnCommand, SubmitMessageCommand } from "../types/commands";
import { CommandId, RunId, SequenceNumber } from "../types/core";

const propertyRuns = 100;

type PendingKind = "queue" | "steer" | "interrupt" | "stop";
type ControlKind = "steer" | "interrupt" | "stop";

const seq = (value: number) => SequenceNumber.make(value);

const uuid = (slot: number): string => {
  const fourth = `8${(slot % 0x1000).toString(16).padStart(3, "0")}`;
  const fifth = slot.toString(16).padStart(12, "0").slice(-12);
  return `018f6bd5-2f2a-7b1e-${fourth}-${fifth}`;
};

const commandId = (slot: number) => CommandId.make(uuid(0x500 + slot));
const runId = (slot: number) => RunId.make(uuid(0x600 + slot));
const content = (text: string) => [Prompt.textPart({ text })];

const commandFor = (id: CommandId, kind: PendingKind) =>
  kind === "stop"
    ? new StopTurnCommand({ commandId: id })
    : new SubmitMessageCommand({ commandId: id, disposition: kind, content: content(kind) });

const pending = (kind: PendingKind, index: number): PendingCommand => {
  const id = commandId(index);
  return { commandId: id, command: commandFor(id, kind), admittedSeq: seq(index + 1) };
};

const state = (queues: Partial<ReducedState["commandQueues"]>): ReducedState => ({
  ...initialReducedState,
  commandQueues: { ...emptyCommandQueues, ...queues },
});

const pendingKindArbitrary = fc.constantFrom<PendingKind>("queue", "steer", "interrupt", "stop");
const controlKindArbitrary = fc.constantFrom<ControlKind>("steer", "interrupt", "stop");

describe("decideDispatch properties", () => {
  it("prioritizes stop and interrupt commands when the session is idle", () => {
    fc.assert(
      fc.property(fc.array(pendingKindArbitrary, { maxLength: 20 }), (kinds) => {
        const pendingCommands = kinds.map((kind, index) => pending(kind, index));
        const stopCommands = pendingCommands.filter((entry) => entry.command._tag === "StopTurn");
        const messageCommands = pendingCommands.filter(
          (entry) => entry.command._tag === "SubmitMessage",
        );
        const decision = decideDispatch(
          state({
            pendingCommands,
            activeControlCommands: stopCommands,
          }),
        );
        const expected =
          stopCommands[0] ??
          messageCommands.find(
            (entry) =>
              entry.command._tag === "SubmitMessage" && entry.command.disposition === "interrupt",
          ) ??
          messageCommands[0];

        if (expected === undefined) {
          expect(decision).toEqual({ _tag: "DispatchNoPendingCommand" });
        } else {
          expect(decision).toEqual({
            _tag: "DispatchStartCommand",
            command: {
              commandId: expected.commandId,
              command: expected.command,
            },
          });
        }
      }),
      { numRuns: propertyRuns },
    );
  });

  it("prioritizes stop, then otherwise uses active-control FIFO", () => {
    fc.assert(
      fc.property(
        fc.boolean(),
        fc.array(controlKindArbitrary, { maxLength: 20 }),
        (hasRun, kinds) => {
          const active = hasRun
            ? { commandId: commandId(100), runId: runId(0) }
            : { commandId: commandId(100) };
          const controls = kinds.map((kind, index) => pending(kind, index));
          const decision = decideDispatch(
            state({ active, pendingCommands: controls, activeControlCommands: controls }),
          );

          const expectedActive = hasRun
            ? { _tag: "DispatchActiveCommand", commandId: commandId(100), runId: runId(0) }
            : { _tag: "DispatchActiveCommand", commandId: commandId(100) };

          if (controls.length === 0) {
            expect(decision).toEqual({
              _tag: "DispatchBlockedByActiveCommand",
              active: expectedActive,
            });
            return;
          }

          const first = (controls.find((control) => control.command._tag === "StopTurn") ??
            controls[0]) as PendingCommand;
          if (first.command._tag === "StopTurn") {
            expect(decision).toEqual({
              _tag: "DispatchStopCommand",
              command: { commandId: first.commandId, command: first.command },
              active: expectedActive,
            });
            return;
          }

          if (first.command.disposition === "interrupt") {
            expect(decision).toEqual({
              _tag: "DispatchInterruptCommand",
              command: { commandId: first.commandId, command: first.command },
              active: expectedActive,
            });
            return;
          }

          expect(first.command.disposition).toBe("steer");
          expect(decision).toEqual(
            hasRun
              ? {
                  _tag: "DispatchSteerCommand",
                  command: { commandId: first.commandId, command: first.command },
                  active: expectedActive,
                }
              : { _tag: "DispatchBlockedByActiveCommand", active: expectedActive },
          );
        },
      ),
      { numRuns: propertyRuns },
    );
  });
});
