import * as fc from "fast-check";
import { describe, expect, it } from "vite-plus/test";

import type { CommittedDurableEvent } from "../services/session-store";
import { giaLikeTestMappings, transcriptScenarios } from "./transcript-scenarios";
import {
  TestLog,
  reducedStateHistory,
  renderedTranscriptHistory,
  ids,
  normalizedProjection,
  projectBatches,
  testDefinition,
} from "./transcript-testkit";

const propertyRuns = 100;
const plain = testDefinition();
const gia = testDefinition(giaLikeTestMappings);

/** Split a log at the given cut points into consecutive non-empty batches. */
const partition = (
  events: ReadonlyArray<CommittedDurableEvent>,
  cuts: ReadonlyArray<number>,
): ReadonlyArray<ReadonlyArray<CommittedDurableEvent>> => {
  const points = [...new Set(cuts.map((cut) => cut % (events.length + 1)))].sort((a, b) => a - b);
  const batches: Array<ReadonlyArray<CommittedDurableEvent>> = [];
  let start = 0;
  for (const point of [...points, events.length]) {
    if (point > start) batches.push(events.slice(start, point));
    start = Math.max(start, point);
  }
  return batches;
};

/**
 * Plausible logs from a small session simulator: each step picks an action that is valid in the
 * simulated state, so every generated log follows the runtime's event protocol.
 */
const generatedLog = (steps: ReadonlyArray<number>): { log: TestLog; compacted: boolean } => {
  const log = new TestLog().configure();
  let next = 1;
  let compacted = false;
  const pending: Array<number> = [];
  const steering: Array<number> = [];
  let active: { run: number; commands: Array<number>; turns: number; first?: number } | undefined;
  for (const step of steps) {
    const slot = next++;
    switch (step % 11) {
      case 0: {
        const key = slot % 2 === 0 ? `k:${slot}` : undefined;
        if (key !== undefined) {
          log.appendForeign("gia-agent", "GiaUserMessageAuthorCommitted", {
            relatedCommandIdempotencyKey: key,
            userEmail: `u${slot}@example.com`,
          });
        }
        log.submit({
          command: slot,
          message: slot,
          text: `q${slot}`,
          ...(key === undefined ? {} : { idempotencyKey: key }),
        });
        pending.push(slot);
        break;
      }
      case 1:
        if (active !== undefined) {
          log.steer({ command: slot, message: slot, run: active.run, text: `s${slot}` });
          steering.push(slot);
        }
        break;
      case 2: {
        const first = pending.shift();
        if (active === undefined && first !== undefined) {
          log.startRun({ run: slot, commands: [first] });
          active = { run: slot, commands: [first], turns: 0, first };
        } else if (first !== undefined) {
          pending.unshift(first);
        }
        break;
      }
      case 3:
      case 4: {
        if (active === undefined) break;
        const inputs = [
          ...(active.first === undefined ? [] : [active.first]),
          ...steering.splice(0),
        ];
        active.first = undefined;
        active.turns += 1;
        const tools = step % 3 === 0 ? [slot] : [];
        log.assistantTurn({
          run: active.run,
          turn: slot,
          inference: slot,
          message: 10_000 + slot,
          text: `a${slot}`,
          inputs,
          tools,
        });
        break;
      }
      case 5:
        if (active !== undefined && active.first === undefined) {
          log.turn({ run: active.run, turn: slot, inputs: steering.splice(0) });
          log.inferenceStarted({ run: active.run, turn: slot, inference: slot });
          log.assistant({
            run: active.run,
            turn: slot,
            inference: slot,
            message: 10_000 + slot,
            text: "cut",
            partial: "stopped",
          });
          log.inferenceFailed({
            run: active.run,
            turn: slot,
            inference: slot,
            code: "inference.interrupted",
          });
        }
        break;
      case 6:
        if (active !== undefined && active.first === undefined) {
          log.runCompleted(active.run, active.commands);
          active = undefined;
        }
        break;
      case 7:
        if (active !== undefined && active.first === undefined) {
          log.runInterrupted(active.run);
          log.runStarted({ run: slot, commands: active.commands });
          log.append("RecoveryCompleted", {
            trigger: "runtime-restart",
            continuation: {
              commandId: ids.command(active.commands[0] ?? 0),
              interruptedRunId: ids.run(active.run),
              replacementRunId: ids.run(slot),
            },
          });
          active = { ...active, run: slot };
        }
        break;
      case 8:
        if (log.events.length > 4) {
          log.compaction({
            compaction: slot,
            summary: slot,
            retainedFromContextSeq: log.events.length,
            contextVersion: slot,
          });
          compacted = true;
        }
        break;
      case 9: {
        const cancelled = pending.pop();
        if (cancelled !== undefined) {
          log.append("UserMessageCancelled", {
            commandId: ids.command(slot),
            messageId: ids.message(cancelled),
            reason: "user-cancel",
          });
        }
        break;
      }
      case 10: {
        const promoted = pending.shift();
        if (promoted !== undefined && active !== undefined) {
          log.append("UserMessagePromoted", {
            commandId: ids.command(slot),
            messageId: ids.message(promoted),
            from: "queue",
            to: "steer",
          });
          steering.push(promoted);
        } else if (promoted !== undefined) {
          pending.unshift(promoted);
        }
        break;
      }
    }
  }
  return { log, compacted };
};

const stepsArbitrary = fc.array(fc.nat({ max: 10_000 }), { minLength: 1, maxLength: 60 });
const cutsArbitrary = fc.array(fc.nat({ max: 10_000 }), { maxLength: 20 });

describe("transcript interpretation oracle", () => {
  it("projects every scenario identically under every batch partition", () => {
    for (const { name, events } of transcriptScenarios()) {
      const definition = name === "host-decorations" ? gia : plain;
      const projected = projectBatches(definition, [events]);
      expect(projected.meta.status._tag).toBe("Ok");
      const full = normalizedProjection(projected);
      expect(
        normalizedProjection(
          projectBatches(
            definition,
            events.map((event) => [event]),
          ),
        ),
      ).toEqual(full);
      fc.assert(
        fc.property(cutsArbitrary, (cuts) => {
          expect(normalizedProjection(projectBatches(definition, partition(events, cuts)))).toEqual(
            full,
          );
        }),
        { numRuns: propertyRuns },
      );
    }
  });

  it("projects generated logs identically under every batch partition", () => {
    fc.assert(
      fc.property(stepsArbitrary, cutsArbitrary, (steps, cuts) => {
        const { events } = generatedLog(steps).log;
        const full = projectBatches(gia, [events]);
        expect(full.meta.status._tag).toBe("Ok");
        expect(normalizedProjection(projectBatches(gia, partition(events, cuts)))).toEqual(
          normalizedProjection(full),
        );
      }),
      { numRuns: propertyRuns },
    );
  });

  it("renders exactly today's history for uncompacted logs and a superset for compacted ones", () => {
    const check = (events: ReadonlyArray<CommittedDurableEvent>, compacted: boolean) => {
      const rendered = renderedTranscriptHistory(projectBatches(gia, [events]));
      const reduced = reducedStateHistory(events);
      if (compacted) {
        // Everything the pruned state still shows is rendered, in the same order.
        expect(rendered.filter((id) => reduced.includes(id))).toEqual(reduced);
      } else {
        expect(rendered).toEqual(reduced);
      }
    };
    for (const { events, compacted } of transcriptScenarios()) check(events, compacted);
    fc.assert(
      fc.property(stepsArbitrary, (steps) => {
        const { log, compacted } = generatedLog(steps);
        check(log.events, compacted);
      }),
      { numRuns: propertyRuns },
    );
  });
});
