import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import * as fc from "fast-check";
import { describe, expect, it } from "vite-plus/test";

import { durableMessageTranscript, durableTranscriptPrompt } from "../domain/message-transcript";
import { reduceCommittedEvents } from "../domain/reduced-state";
import type { CommittedDurableEvent } from "../services/session-store";
import { SequenceNumber, durablePosition } from "../types/core";
import type { TranscriptDefinition } from "./mapping";
import { sha256Hex } from "./hash";
import { canonicalJson, transcriptSemanticRevision } from "./version";
import {
  giaLikeTestMappings,
  transcriptScenarios,
  type TranscriptScenario,
} from "./transcript-scenarios";
import {
  TestLog,
  ids,
  normalizedProjection,
  projectBatches,
  projectionJson,
  testDefinition,
} from "./transcript-testkit";

/**
 * CI guard for the semantic revision (design §7). Each corpus scenario has a recorded transcript
 * fingerprint for the current `transcriptSemanticRevision`. If interpretation output changes, this
 * test fails until the revision is bumped; only then may fingerprints be re-recorded, with
 * `UPDATE_TRANSCRIPT_FINGERPRINTS=1`. A new scenario may be recorded without a bump: adding an event
 * the interpreter ignores changes no existing output, so neither the revision nor any version id
 * changes.
 *
 * The same corpus also records the model-facing prompt and transcript derived from `ReducedState`.
 * Those must never change in this work: transcript history leaves the model path untouched.
 */

const fixturePath = fileURLToPath(new URL("./transcript-fingerprints.json", import.meta.url));

interface ScenarioFingerprints {
  readonly transcript: string;
  readonly prompt: string;
}

interface FingerprintFixture {
  readonly semanticRevision: number;
  readonly scenarios: Readonly<Record<string, ScenarioFingerprints>>;
}

const plain = testDefinition();
const gia = testDefinition(giaLikeTestMappings);
const definitionFor = (name: string): TranscriptDefinition =>
  name === "host-decorations" ? gia : plain;

const json = (value: unknown) => canonicalJson(JSON.parse(JSON.stringify(value)));

const fingerprints = (scenario: TranscriptScenario): ScenarioFingerprints => {
  const projected = projectBatches(definitionFor(scenario.name), [scenario.events]);
  const reduced = reduceCommittedEvents(scenario.events);
  return {
    transcript: sha256Hex(projectionJson(projected.records.values())),
    prompt: sha256Hex(
      json({
        transcript: durableMessageTranscript(reduced),
        prompt: durableTranscriptPrompt(reduced).content,
      }),
    ),
  };
};

const readFixture = (): FingerprintFixture => JSON.parse(readFileSync(fixturePath, "utf8"));

describe("transcript fingerprint", () => {
  it("changes only with the semantic revision, and the model path never changes", () => {
    const fixture = readFixture();
    const actual = Object.fromEntries(
      transcriptScenarios().map((scenario) => [scenario.name, fingerprints(scenario)]),
    );
    const revisionBumped = fixture.semanticRevision !== transcriptSemanticRevision;
    const problems: Array<string> = [];
    for (const [name, current] of Object.entries(actual)) {
      const recorded = fixture.scenarios[name];
      if (recorded === undefined) {
        problems.push(`${name}: no recorded fingerprint`);
        continue;
      }
      if (recorded.prompt !== current.prompt) {
        problems.push(
          `${name}: the model-facing prompt changed; transcript work must not touch it`,
        );
      }
      if (recorded.transcript !== current.transcript && !revisionBumped) {
        problems.push(
          `${name}: interpretation changed without bumping transcriptSemanticRevision (${transcriptSemanticRevision})`,
        );
      }
    }
    for (const name of Object.keys(fixture.scenarios)) {
      if (actual[name] === undefined)
        problems.push(`${name}: recorded but no longer in the corpus`);
    }

    const updatable = problems.every(
      (problem) =>
        problem.endsWith("no recorded fingerprint") || problem.endsWith("no longer in the corpus"),
    );
    if (process.env["UPDATE_TRANSCRIPT_FINGERPRINTS"] === "1" && (updatable || revisionBumped)) {
      const prompts = problems.filter((problem) => problem.includes("prompt changed"));
      expect(prompts).toEqual([]);
      writeFileSync(
        fixturePath,
        `${JSON.stringify({ semanticRevision: transcriptSemanticRevision, scenarios: actual }, null, 2)}\n`,
      );
      return;
    }
    expect(
      revisionBumped
        ? ["semantic revision bumped: re-record with UPDATE_TRANSCRIPT_FINGERPRINTS=1"]
        : problems,
    ).toEqual([]);
  });

  it("is blind to events without transcript meaning", () => {
    const noise = new TestLog();
    noise.append("ContextProjected", { contextVersion: 0, throughSeq: 1 });
    noise.append("BaseStateRequested", { baseStateId: ids.event(990), requestedThroughSeq: 1 });
    noise.append("SystemMessageCommitted", { messageId: ids.message(990), content: "system" });
    noise.append("RunSchedulingDelivered", { requestId: ids.event(991) });
    noise.append("CommandStarted", { commandId: ids.command(990) });
    noise.appendForeign("unmapped-app", "Anything", { relatedCommandIdempotencyKey: "web:1" });

    const renumber = (
      events: ReadonlyArray<CommittedDurableEvent>,
    ): ReadonlyArray<CommittedDurableEvent> =>
      events.map((entry, index) => ({
        ...entry,
        position: durablePosition(SequenceNumber.make(index + 1)),
      }));
    /** Records with every log-position field removed, so only meaning is compared. */
    const meaning = (
      events: ReadonlyArray<CommittedDurableEvent>,
      definition: TranscriptDefinition,
    ) => {
      const { records } = normalizedProjection(projectBatches(definition, [events]));
      return JSON.parse(
        JSON.stringify(records, (key, value) =>
          ["updatedSeq", "anchorSeq", "seq", "submittedSeq"].includes(key) ? undefined : value,
        ),
      );
    };

    for (const scenario of transcriptScenarios()) {
      const definition = definitionFor(scenario.name);
      const expected = meaning(scenario.events, definition);
      fc.assert(
        fc.property(fc.array(fc.nat({ max: 10_000 }), { maxLength: 8 }), (inserts) => {
          const events = [...scenario.events];
          inserts.forEach((at, index) => {
            const extra = noise.events[index % noise.events.length];
            if (extra !== undefined) events.splice(at % (events.length + 1), 0, extra);
          });
          expect(meaning(renumber(events), definition)).toEqual(expected);
        }),
        { numRuns: 25 },
      );
    }
  });
});
