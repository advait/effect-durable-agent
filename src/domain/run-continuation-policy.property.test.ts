import * as fc from "fast-check";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import type { PendingSteeringMessage } from "./command-queues";
import { initialReducedState } from "./reduced-state";
import { decideRunContinuation } from "./run-continuation-policy";
import { CommandId, MessageId, RunId, SequenceNumber, TurnId } from "../types/core";

const propertyRuns = 100;

const seq = (value: number) => SequenceNumber.make(value);

const uuid = (slot: number): string => {
  const fourth = `8${(slot % 0x1000).toString(16).padStart(3, "0")}`;
  const fifth = slot.toString(16).padStart(12, "0").slice(-12);
  return `018f6bd5-2f2a-7b1e-${fourth}-${fifth}`;
};

const commandId = (slot: number) => CommandId.make(uuid(0x700 + slot));
const messageId = (slot: number) => MessageId.make(uuid(0x800 + slot));
const runId = (slot: number) => RunId.make(uuid(0x900 + slot));
const turnId = (slot: number) => TurnId.make(uuid(0xa00 + slot));
const content = (text: string) => [Prompt.textPart({ text })];

const steering = (run: RunId, index: number, queuedSeq: number): PendingSteeringMessage => ({
  messageId: messageId(index),
  commandId: commandId(index),
  runId: run,
  content: content(`steer-${queuedSeq}`),
  queuedSeq: seq(queuedSeq),
});

describe("decideRunContinuation properties", () => {
  it("selects all pending steering messages for the target run", () => {
    fc.assert(
      fc.property(
        fc.array(fc.integer({ min: 1, max: 10_000 }), { maxLength: 20 }),
        fc.array(fc.integer({ min: 1, max: 10_000 }), { maxLength: 20 }),
        (targetSeqs, otherSeqs) => {
          const targetRun = runId(0);
          const otherRun = runId(1);
          const target = targetSeqs.map((queuedSeq, index) =>
            steering(targetRun, index, queuedSeq),
          );
          const other = otherSeqs.map((queuedSeq, index) =>
            steering(otherRun, index + target.length, queuedSeq),
          );
          const steeringByRun = new Map<RunId, ReadonlyArray<PendingSteeringMessage>>();
          if (target.length > 0) steeringByRun.set(targetRun, target);
          if (other.length > 0) steeringByRun.set(otherRun, other);

          const decision = decideRunContinuation({
            runId: targetRun,
            turnId: turnId(0),
            state: {
              ...initialReducedState,
              commandQueues: { ...initialReducedState.commandQueues, steeringByRun },
            },
          });

          expect(decision).toEqual(
            target.length === 0
              ? { _tag: "CompleteRun" }
              : {
                  _tag: "ContinueWithSteering",
                  steerings: [...target].sort((left, right) => left.queuedSeq - right.queuedSeq),
                },
          );
        },
      ),
      { numRuns: propertyRuns },
    );
  });
});
