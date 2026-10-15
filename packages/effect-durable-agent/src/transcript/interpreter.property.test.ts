import * as fc from "fast-check";
import { describe, expect, it } from "vite-plus/test";

import type { CommittedDurableEvent } from "../services/session-store";
import { giaLikeTestMappings, transcriptScenarios } from "./transcript-scenarios";
import {
  generatedTestLog as generatedLog,
  reducedStateHistory,
  renderedTranscriptHistory,
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
