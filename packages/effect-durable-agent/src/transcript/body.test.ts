import * as Result from "effect/Result";
import { describe, expect, it } from "vite-plus/test";

import { effectDurableAgentNamespace } from "../types/events/envelope";
import { resolveTranscriptBody } from "./body";
import { inMemoryTranscriptWindowSource } from "./in-memory";
import {
  TranscriptLogIncarnation,
  type TranscriptBody,
  type TranscriptRecordView,
} from "./records";
import { giaLikeTestMappings, transcriptScenarios } from "./transcript-scenarios";
import { projectBatches, testDefinition } from "./transcript-testkit";
import { readTranscriptWindowFrom } from "./window-read";

const event = (
  type: string,
  payload: unknown,
  namespace: string = effectDurableAgentNamespace,
) => ({
  namespace,
  type,
  payload,
});

describe("resolveTranscriptBody", () => {
  it("resolves the interpreter's body sites to the JSON it inlines", () => {
    expect(
      resolveTranscriptBody(event("ToolCallCreated", { promptPart: { params: { q: 1 } } }), [
        "promptPart",
        "params",
      ]),
    ).toEqual({ _tag: "Found", value: { q: 1 } });
    expect(
      resolveTranscriptBody(
        event("ToolCallCompleted", { promptPart: { result: { at: new Date(0) } } }),
        ["promptPart", "result"],
      ),
    ).toEqual({ _tag: "Found", value: { at: "1970-01-01T00:00:00.000Z" } });
    const assistant = {
      promptParts: [
        { type: "text", text: "hi" },
        { type: "tool-result", id: "call_1", name: "search", result: ["row"] },
      ],
    };
    for (const type of [
      "AssistantMessageCommitted",
      "AssistantPartialCommitted",
      "AssistantMessageImported",
    ]) {
      expect(resolveTranscriptBody(event(type, assistant), ["promptParts", 1, "result"])).toEqual({
        _tag: "Found",
        value: ["row"],
      });
    }
    // A value with no JSON text is inlined as null, so its reference resolves to null too.
    expect(
      resolveTranscriptBody(event("ToolCallCompleted", { promptPart: { result: undefined } }), [
        "promptPart",
        "result",
      ]),
    ).toEqual({ _tag: "Found", value: null });
  });

  it("never resolves anything that is not a body site", () => {
    const cases: ReadonlyArray<
      readonly [ReturnType<typeof event>, ReadonlyArray<string | number>]
    > = [
      [event("SystemMessageCommitted", { content: "secret instructions" }), ["content"]],
      [event("ToolCallCreated", { promptPart: { params: {} } }), []],
      [event("ToolCallCreated", { promptPart: { params: {} } }), ["promptPart"]],
      [
        event("ToolCallCreated", { promptPart: { params: { a: 1 } } }),
        ["promptPart", "params", "a"],
      ],
      [event("ToolCallCompleted", { promptPart: { result: 1 } }), ["promptPart", "params"]],
      [event("CommandAdmitted", { command: { content: "x" } }), ["command", "content"]],
      [
        event("ToolCallCompleted", { promptPart: { result: 1 } }, "gia-agent"),
        ["promptPart", "result"],
      ],
      [
        event("AssistantMessageCommitted", { promptParts: [{ type: "text", text: "t" }] }),
        ["promptParts", 0, "result"],
      ],
      [
        event("AssistantMessageCommitted", {
          promptParts: [{ type: "tool-result", id: "", name: "n", result: 1 }],
        }),
        ["promptParts", 0, "result"],
      ],
      [event("AssistantMessageCommitted", { promptParts: [] }), ["promptParts", 3, "result"]],
      [
        event("ToolCallCompleted", Object.create({ promptPart: { result: 1 } })),
        ["promptPart", "result"],
      ],
    ];
    for (const [input, path] of cases) {
      expect(resolveTranscriptBody(input, path)._tag, JSON.stringify([input.type, path])).toBe(
        "Missing",
      );
    }
    expect(
      resolveTranscriptBody(event("ToolCallCompleted", { promptPart: { result: 1n } }), [
        "promptPart",
        "result",
      ])._tag,
    ).toBe("Missing");
  });
});

describe("body references against the scenario corpus", () => {
  it("resolve through the log to exactly what the window inlines", () => {
    const plain = testDefinition();
    const gia = testDefinition(giaLikeTestMappings);
    let resolved = 0;
    for (const { name, events } of transcriptScenarios()) {
      const version = projectBatches(name === "host-decorations" ? gia : plain, [events]);
      const source = inMemoryTranscriptWindowSource({
        incarnation: TranscriptLogIncarnation.make("memory:bodies"),
        config: { current: name === "host-decorations" ? gia : plain },
        versions: [version],
      });
      if (source === undefined) throw new Error("a projected version is servable");
      const read = (maxInlineBodyBytes: number) => {
        const window = readTranscriptWindowFrom(
          source,
          { _tag: "Latest", count: 500 },
          { maxRecords: 500, maxBytes: 64 * 1_048_576, maxInlineBodyBytes },
        );
        if (!Result.isSuccess(window)) throw new Error("a Latest read cannot mismatch");
        return [...window.success.records, ...window.success.dependencies];
      };
      const inline = bodiesOf(read(Number.MAX_SAFE_INTEGER));
      const refs = bodiesOf(read(0));
      expect(refs.length, name).toBe(inline.length);
      refs.forEach((body, index) => {
        const expected = inline[index];
        expect(body._tag, name).toBe("Ref");
        expect(expected?._tag, name).toBe("Inline");
        const event = events.find((entry) => entry.position.seq === body.source.seq)?.event;
        if (event === undefined || expected?._tag !== "Inline") throw new Error(name);
        expect(resolveTranscriptBody(event, body.source.path), name).toEqual({
          _tag: "Found",
          value: expected.value,
        });
        resolved += 1;
      });
    }
    expect(resolved).toBeGreaterThan(5);
  });
});

/** Every body of a window, in record and part order. */
const bodiesOf = (records: ReadonlyArray<TranscriptRecordView>): ReadonlyArray<TranscriptBody> =>
  records.flatMap((record): ReadonlyArray<TranscriptBody> => {
    switch (record.kind) {
      case "tool":
        return [
          ...(record.params === undefined ? [] : [record.params]),
          ...(record.lifecycle._tag === "Completed" ? [record.lifecycle.result] : []),
        ];
      case "assistant":
        return record.parts.flatMap((part) => (part._tag === "ToolResult" ? [part.result] : []));
      default:
        return [];
    }
  });
