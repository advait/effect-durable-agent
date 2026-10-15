import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { Position, SequenceNumber, SubSequenceNumber } from "../types/core";
import { PositionedEvent, effectDurableAgentNamespace } from "../types/events";
import { durableEventEnvelope } from "../types/events/durable";
import { EDAEphemeralEvent } from "../types/events/ephemeral";
import {
  emptyTranscriptActiveContentState,
  foldTranscriptActiveContent,
  transcriptActiveContentOf,
  type TranscriptActiveContentState,
} from "./active-content";
import { TestLog, ids, testEventTrace, testId, testSessionId } from "./transcript-testkit";

const decodeEphemeral = Schema.decodeUnknownSync(EDAEphemeralEvent);
let ephemeralSlot = 1;

/** A published ephemeral event anchored at `seq`, decoded through the framework schema. */
const ephemeral = (seq: number, type: string, payload: Record<string, unknown>): PositionedEvent =>
  PositionedEvent.make({
    position: Position.make({
      seq: SequenceNumber.make(seq),
      subSeq: SubSequenceNumber.make(ephemeralSlot++),
    }),
    event: decodeEphemeral({
      namespace: effectDurableAgentNamespace,
      type,
      schemaVersion: 1,
      durability: "ephemeral",
      eventId: testId(0x10a, ephemeralSlot),
      sessionId: testSessionId,
      createdAtMs: 1_760_000_000_000,
      trace: testEventTrace,
      payload,
    }),
  });

/** Fold a log's durable events from `from` (0-based) on, as they would be published. */
const publish = (
  state: TranscriptActiveContentState,
  log: TestLog,
  from: number,
): TranscriptActiveContentState =>
  log.events.slice(from).reduce(
    (current, entry) =>
      foldTranscriptActiveContent(
        current,
        PositionedEvent.make({
          position: entry.position,
          event: durableEventEnvelope(entry.event),
        }),
      ),
    state,
  );

/** A started inference with streamed text, reasoning and a tool call. */
const streaming = () => {
  const log = new TestLog().configure();
  log.submit({ command: 1, message: 1, text: "q" });
  log.startRun({ run: 2, commands: [1] });
  log.turn({ run: 2, turn: 3, inputs: [1] });
  log.inferenceStarted({ run: 2, turn: 3, inference: 4 });
  let state = publish(emptyTranscriptActiveContentState, log, 0);
  const head = log.events.length;
  const events = [
    ephemeral(head, "ReasoningDelta", { providerPartId: "r", delta: "think" }),
    ephemeral(head, "TextDelta", { providerPartId: "t", delta: "hel" }),
    ephemeral(head, "TextDelta", { providerPartId: "t", delta: "lo" }),
    ephemeral(head, "ToolParamsStart", {
      providerPartId: "c",
      toolCallId: ids.tool(5),
      toolName: "search",
      providerExecuted: false,
    }),
    ephemeral(head, "ToolParamsDelta", {
      providerPartId: "c",
      toolCallId: ids.tool(5),
      delta: '{"q":',
    }),
    ephemeral(head, "ToolParamsDelta", {
      providerPartId: "c",
      toolCallId: ids.tool(5),
      delta: '"x"}',
    }),
    ephemeral(head, "ToolParamsEnd", { providerPartId: "c", toolCallId: ids.tool(5) }),
  ];
  state = events.reduce(foldTranscriptActiveContent, state);
  return { log, state, published: log.events.length };
};

describe("transcript active content", () => {
  it("accumulates ordered parts under stable identities", () => {
    const { state, published } = streaming();
    expect(state.publishedThroughSeq).toBe(published);
    expect(transcriptActiveContentOf(state)).toEqual([
      {
        assistantId: `assistant:${ids.inference(4)}`,
        parts: [
          { id: "reasoning:r", part: { _tag: "Reasoning", text: "think" } },
          { id: "text:t", part: { _tag: "Text", text: "hello" } },
          {
            id: `tool:${ids.tool(5)}`,
            part: {
              _tag: "ToolCall",
              providerPartId: "c",
              toolName: "search",
              providerExecuted: false,
            },
            paramsText: '{"q":"x"}',
          },
        ],
      },
    ]);
  });

  it("accumulates far more deltas than the raw replay buffer holds", () => {
    const { state, published } = streaming();
    let next = state;
    for (let index = 0; index < 5_000; index += 1) {
      next = foldTranscriptActiveContent(
        next,
        ephemeral(published, "TextDelta", { providerPartId: "long", delta: `${index % 10}` }),
      );
    }
    const long = transcriptActiveContentOf(next)[0]?.parts.find((part) => part.id === "text:long");
    expect(long?.part).toEqual({
      _tag: "Text",
      text: Array.from({ length: 5_000 }, (_, index) => `${index % 10}`).join(""),
    });
  });

  it("retires on every inference terminal and on its turn's terminal", () => {
    const terminals: ReadonlyArray<[string, (log: TestLog) => void]> = [
      [
        "commit",
        (log) => {
          log.assistant({ run: 2, turn: 3, inference: 4, message: 9, text: "hello" });
          log.inferenceCompleted({ run: 2, turn: 3, inference: 4 });
        },
      ],
      [
        "partial plus failure",
        (log) => {
          log.assistant({
            run: 2,
            turn: 3,
            inference: 4,
            message: 9,
            text: "hel",
            partial: "failed",
          });
          log.inferenceFailed({ run: 2, turn: 3, inference: 4, code: "provider.error" });
        },
      ],
      ["empty success", (log) => void log.inferenceCompleted({ run: 2, turn: 3, inference: 4 })],
      [
        "recovery failure",
        (log) =>
          void log.inferenceFailed({
            run: 2,
            turn: 3,
            inference: 4,
            code: "inference.interrupted",
          }),
      ],
      ["turn terminal", (log) => void log.turnCompleted({ run: 2, turn: 3 })],
    ];
    for (const [name, terminate] of terminals) {
      const { log, state, published } = streaming();
      terminate(log);
      const retired = publish(state, log, published);
      expect(transcriptActiveContentOf(retired), name).toEqual([]);
      expect(retired.revision, name).toBeGreaterThan(state.revision);
      expect(retired.publishedThroughSeq, name).toBe(log.events.length);
    }
  });

  it("ignores live parts with no active inference and terminals of other inferences", () => {
    const { log, state, published } = streaming();
    log.inferenceCompleted({ run: 2, turn: 3, inference: 99 });
    const unrelated = publish(state, log, published);
    expect(transcriptActiveContentOf(unrelated)).toHaveLength(1);

    const idle = foldTranscriptActiveContent(
      emptyTranscriptActiveContentState,
      ephemeral(0, "TextDelta", { providerPartId: "t", delta: "stray" }),
    );
    expect(idle).toBe(emptyTranscriptActiveContentState);
  });
});
