import * as fc from "fast-check";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { deriveCommandQueues } from "./command-queues";
import type { CommandRecord, MessageRecord, RunRecord } from "./reduced-state";
import { StopTurnCommand, SubmitMessageCommand } from "../types/commands";
import { CommandId, MessageId, RunId, SequenceNumber, TurnId } from "../types/core";

const propertyRuns = 100;

type CommandKind = "queue" | "steer" | "interrupt" | "stop";
type CommandStatus = "pending" | "started" | "terminal";

type CommandSpec = {
  readonly kind: CommandKind;
  readonly admittedSeq: number;
  readonly status: CommandStatus;
};

type SteeringSpec = {
  readonly runSlot: number;
  readonly queuedSeq: number;
  readonly state: "pending" | "consumed" | "cancelled" | "user";
};

const seq = (value: number) => SequenceNumber.make(value);

const uuid = (slot: number): string => {
  const fourth = `8${(slot % 0x1000).toString(16).padStart(3, "0")}`;
  const fifth = slot.toString(16).padStart(12, "0").slice(-12);
  return `018f6bd5-2f2a-7b1e-${fourth}-${fifth}`;
};

const commandId = (slot: number) => CommandId.make(uuid(0x100 + slot));
const runId = (slot: number) => RunId.make(uuid(0x200 + slot));
const messageId = (slot: number) => MessageId.make(uuid(0x300 + slot));
const turnId = (slot: number) => TurnId.make(uuid(0x400 + slot));

const content = (text: string) => [Prompt.textPart({ text })];

const commandFor = (id: CommandId, kind: CommandKind) =>
  kind === "stop"
    ? new StopTurnCommand({ commandId: id })
    : new SubmitMessageCommand({ commandId: id, disposition: kind, content: content(kind) });

const commandRecord = (spec: CommandSpec, index: number): CommandRecord => {
  const id = commandId(index);
  const startedSeq = seq(spec.admittedSeq + 1_000 + index);
  return {
    commandId: id,
    command: commandFor(id, spec.kind),
    admittedSeq: seq(spec.admittedSeq),
    ...(spec.status === "pending" ? {} : { startedSeq }),
    ...(spec.status === "terminal"
      ? { terminal: { _tag: "Completed", seq: seq(spec.admittedSeq + 2_000 + index) } as const }
      : {}),
  };
};

const commandSpecArbitrary = fc.record({
  kind: fc.constantFrom<CommandKind>("queue", "steer", "interrupt", "stop"),
  admittedSeq: fc.integer({ min: 1, max: 10_000 }),
  status: fc.constantFrom<CommandStatus>("pending", "started", "terminal"),
});

const steeringSpecArbitrary = fc.record({
  runSlot: fc.integer({ min: 0, max: 3 }),
  queuedSeq: fc.integer({ min: 1, max: 10_000 }),
  state: fc.constantFrom<SteeringSpec["state"]>("pending", "consumed", "cancelled", "user"),
});

const sortPendingSpecs = (
  entries: ReadonlyArray<{ readonly spec: CommandSpec; readonly index: number }>,
) => [...entries].sort((left, right) => left.spec.admittedSeq - right.spec.admittedSeq);

const isActiveControlKind = (kind: CommandKind) => kind !== "queue";

describe("deriveCommandQueues properties", () => {
  it("keeps pending command partitions consistent for generated command records", () => {
    fc.assert(
      fc.property(fc.array(commandSpecArbitrary, { maxLength: 30 }), (specs) => {
        const commands = new Map(
          specs.map((spec, index) => [commandId(index), commandRecord(spec, index)]),
        );
        const queues = deriveCommandQueues({ commands, runs: new Map(), messages: new Map() });

        const pendingSpecs = sortPendingSpecs(
          specs
            .map((spec, index) => ({ spec, index }))
            .filter(({ spec }) => spec.status === "pending"),
        );
        const queuedSpecs = pendingSpecs.filter(({ spec }) => spec.kind === "queue");
        const activeControlSpecs = pendingSpecs.filter(({ spec }) =>
          isActiveControlKind(spec.kind),
        );

        expect(queues.pendingCommands.map((entry) => entry.commandId)).toEqual(
          pendingSpecs.map(({ index }) => commandId(index)),
        );
        expect(queues.pendingCommands.map((entry) => entry.admittedSeq)).toEqual(
          pendingSpecs.map(({ spec }) => seq(spec.admittedSeq)),
        );
        expect(queues.queuedCommands.map((entry) => entry.commandId)).toEqual(
          queuedSpecs.map(({ index }) => commandId(index)),
        );
        expect(queues.activeControlCommands.map((entry) => entry.commandId)).toEqual(
          activeControlSpecs.map(({ index }) => commandId(index)),
        );
      }),
      { numRuns: propertyRuns },
    );
  });

  it("derives active command evidence only from started non-terminal commands", () => {
    fc.assert(
      fc.property(fc.boolean(), fc.boolean(), (hasMatchingRun, matchingRunIsTerminal) => {
        const activeCommandId = commandId(0);
        const activeRunId = runId(0);
        const runs: ReadonlyMap<RunId, RunRecord> = hasMatchingRun
          ? new Map([
              [
                activeRunId,
                {
                  runId: activeRunId,
                  commandIds: [activeCommandId],
                  startedSeq: seq(3),
                  ...(matchingRunIsTerminal
                    ? { terminal: { _tag: "Completed", seq: seq(4) } as const }
                    : {}),
                },
              ],
            ])
          : new Map();

        const queues = deriveCommandQueues({
          commands: new Map([
            [
              activeCommandId,
              {
                commandId: activeCommandId,
                command: commandFor(activeCommandId, "queue"),
                admittedSeq: seq(1),
                startedSeq: seq(2),
              },
            ],
          ]),
          runs,
          messages: new Map(),
        });

        expect(queues.active).toEqual(
          hasMatchingRun && !matchingRunIsTerminal
            ? { commandId: activeCommandId, runId: activeRunId }
            : { commandId: activeCommandId },
        );
      }),
      { numRuns: propertyRuns },
    );
  });

  it("groups only unconsumed, uncancelled steering by run in queued sequence order", () => {
    fc.assert(
      fc.property(fc.array(steeringSpecArbitrary, { maxLength: 30 }), (specs) => {
        const messages = new Map<MessageId, MessageRecord>(
          specs.map((spec, index) => {
            const id = messageId(index);
            if (spec.state === "user") {
              return [
                id,
                {
                  _tag: "User",
                  messageId: id,
                  commandId: commandId(index),
                  content: content(`user-${index}`),
                  seq: seq(spec.queuedSeq),
                },
              ];
            }

            return [
              id,
              {
                _tag: "Steering",
                messageId: id,
                commandId: commandId(index),
                runId: runId(spec.runSlot),
                content: content(`steer-${index}`),
                seq: seq(spec.queuedSeq),
                ...(spec.state === "consumed"
                  ? { consumedSeq: seq(spec.queuedSeq + 1_000), consumedTurnId: turnId(index) }
                  : {}),
                ...(spec.state === "cancelled"
                  ? { cancelledSeq: seq(spec.queuedSeq + 2_000), cancellationReason: "cancelled" }
                  : {}),
              },
            ];
          }),
        );

        const queues = deriveCommandQueues({ commands: new Map(), runs: new Map(), messages });

        for (let slot = 0; slot <= 3; slot += 1) {
          const expected = specs
            .map((spec, index) => ({ spec, index }))
            .filter(({ spec }) => spec.state === "pending" && spec.runSlot === slot)
            .sort((left, right) => left.spec.queuedSeq - right.spec.queuedSeq);
          const actual = queues.steeringByRun.get(runId(slot)) ?? [];

          expect(actual.map((entry) => entry.messageId)).toEqual(
            expected.map(({ index }) => messageId(index)),
          );
          expect(actual.map((entry) => entry.queuedSeq)).toEqual(
            expected.map(({ spec }) => seq(spec.queuedSeq)),
          );
        }
      }),
      { numRuns: propertyRuns },
    );
  });
});
