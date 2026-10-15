import { describe, expect, it } from "vite-plus/test";

import { CommandId, InferenceId, RunId, SequenceNumber, ToolCallId } from "../types/core";
import type { InMemoryTranscriptVersion } from "./in-memory";
import { planTranscriptBatch } from "./interpreter";
import {
  isPendingTranscriptUserRecord,
  isTopLevelTranscriptRecord,
  runGroupStatus,
  transcriptRecordId,
  type TranscriptRecord,
  type TranscriptRecordId,
} from "./records";
import { giaLikeTestMappings, transcriptScenarios } from "./transcript-scenarios";
import {
  TestLog,
  ids,
  normalizedProjection,
  projectBatches,
  testDefinition,
} from "./transcript-testkit";

const plain = testDefinition();
const gia = testDefinition(giaLikeTestMappings);

const scenario = (name: string) => {
  const found = transcriptScenarios().find((candidate) => candidate.name === name);
  if (found === undefined) throw new Error(`unknown scenario ${name}`);
  return projectBatches(name === "host-decorations" ? gia : plain, [found.events]);
};

const record = (version: InMemoryTranscriptVersion, id: TranscriptRecordId): TranscriptRecord => {
  const found = version.records.get(id);
  if (found === undefined) throw new Error(`missing record ${id}`);
  return found;
};

const user = (version: InMemoryTranscriptVersion, command: number) => {
  const found = record(version, transcriptRecordId.user(CommandId.make(ids.command(command))));
  if (found.kind !== "user") throw new Error("not a user");
  return found;
};

const assistant = (version: InMemoryTranscriptVersion, inference: number) => {
  const found = record(
    version,
    transcriptRecordId.assistant(InferenceId.make(ids.inference(inference))),
  );
  if (found.kind !== "assistant") throw new Error("not an assistant");
  return found;
};

const tool = (version: InMemoryTranscriptVersion, slot: number) => {
  const found = record(version, transcriptRecordId.tool(ToolCallId.make(ids.tool(slot))));
  if (found.kind !== "tool") throw new Error("not a tool");
  return found;
};

/** Rendered history: top-level records in transcript order, excluding cancelled users. */
const history = (version: InMemoryTranscriptVersion) =>
  Array.from(version.records.values())
    .filter(isTopLevelTranscriptRecord)
    .sort(
      (left, right) =>
        (left.position?.anchorSeq ?? 0) - (right.position?.anchorSeq ?? 0) ||
        (left.position?.ordinal ?? 0) - (right.position?.ordinal ?? 0),
    )
    .map((top) => top.id);

describe("transcript interpreter", () => {
  it("orders users by consumption and links assistant tool parts to tool records", () => {
    const version = scenario("queue-run-tools");
    const first = assistant(version, 1);
    expect(history(version)).toEqual([user(version, 1).id, first.id, assistant(version, 2).id]);
    expect(first.parts.map((part) => (part._tag === "ToolCall" ? part.toolId : part._tag))).toEqual(
      ["Text", tool(version, 1).id, tool(version, 2).id],
    );
    expect(tool(version, 1)).toMatchObject({
      assistantId: first.id,
      lifecycle: { _tag: "Completed" },
      params: { _tag: "Inline", value: { q: "tool_1" } },
    });
    expect(user(version, 1).decorations).toEqual({});
  });

  it("treats a committed user as consumed at its commit, admitted or not", () => {
    const version = scenario("legacy-committed-user");
    expect(user(version, 1)).toMatchObject({
      state: { _tag: "Consumed" },
      disposition: "interrupt",
    });
    expect(user(version, 2)).toMatchObject({
      state: { _tag: "Consumed" },
      messageIds: [ids.message(2)],
    });
    expect(history(version).at(-1)).toBe(user(version, 2).id);
  });

  it("consumes steering at the turn that reads it and drops cancelled steering", () => {
    const version = scenario("steering");
    expect(user(version, 2)).toMatchObject({ disposition: "steer", state: { _tag: "Consumed" } });
    expect(user(version, 3)).toMatchObject({
      steering: { runId: ids.run(1) },
      state: { _tag: "Consumed" },
    });
    expect(user(version, 4)).toMatchObject({ state: { _tag: "Cancelled", reason: "run ended" } });
    expect(user(version, 4).position).toBeUndefined();
    const second = history(version).indexOf(assistant(version, 2).id);
    expect(history(version).slice(second - 2, second)).toEqual([
      user(version, 2).id,
      user(version, 3).id,
    ]);
  });

  it("tracks promote, pause and cancel, and withdraws a command that never became a message", () => {
    const version = scenario("promote-pause-cancel");
    expect(user(version, 2)).toMatchObject({ disposition: "steer", state: { _tag: "Consumed" } });
    expect(user(version, 3).state).toEqual({ _tag: "Paused", byCommandId: ids.command(7) });
    expect(user(version, 3).disposition).toBe("queue");
    expect(user(version, 4).state).toMatchObject({ _tag: "Cancelled", reason: "user-cancel" });
    expect(user(version, 5).state).toMatchObject({ _tag: "Cancelled", reason: "edit" });
    expect(user(version, 10)).toMatchObject({
      state: { _tag: "Cancelled", reason: "stopped" },
      messageIds: [],
    });
    expect(
      Array.from(version.records.values())
        .filter(isPendingTranscriptUserRecord)
        .map((pending) => pending.id),
    ).toEqual([user(version, 3).id]);
  });

  it("keeps a partial commit with its failure, an empty success, and an import", () => {
    const version = scenario("partial-failure-empty-success-import");
    expect(assistant(version, 1)).toMatchObject({
      lifecycle: { _tag: "Failed" },
      partial: { reason: "inference failed before completion" },
      parts: [
        { _tag: "Reasoning", text: "hmm" },
        { _tag: "Text", text: "half" },
      ],
    });
    expect(assistant(version, 2)).toMatchObject({ lifecycle: { _tag: "Completed" }, parts: [] });
    const imported = Array.from(version.records.values()).find((candidate) =>
      candidate.id.startsWith("import:"),
    );
    expect(imported).toMatchObject({ source: "import", lifecycle: { _tag: "Completed" } });
    expect(imported !== undefined && "runGroupId" in imported).toBe(false);
    expect(history(version)[0]).toBe(imported?.id);
  });

  it("records rejections as terminal and lets the last tool terminal win", () => {
    const version = scenario("rejected-tool-repeated-terminals");
    expect(tool(version, 1).lifecycle).toMatchObject({ _tag: "Failed" });
    expect(tool(version, 2)).toMatchObject({
      lifecycle: { _tag: "Rejected", reason: "unknown-tool", modelFeedback: "no such tool" },
    });
    expect(tool(version, 2).params).toBeUndefined();
    expect(
      assistant(version, 1).parts.flatMap((part) =>
        part._tag === "ToolCall" ? [part.toolId] : [],
      ),
    ).toEqual([tool(version, 1).id, tool(version, 2).id]);
  });

  it("keeps a recovery continuation in its run group", () => {
    const version = scenario("recovery-continuation");
    const group = record(version, transcriptRecordId.runGroup(RunId.make(ids.run(1))));
    expect(group).toMatchObject({
      kind: "runGroup",
      runs: [
        { runId: ids.run(1), outcome: { _tag: "Interrupted" } },
        { runId: ids.run(2), outcome: { _tag: "Completed" }, continuationOf: ids.run(1) },
      ],
    });
    expect(group.kind === "runGroup" && runGroupStatus(group)).toEqual({ _tag: "Completed" });
    expect(version.records.has(transcriptRecordId.runGroup(RunId.make(ids.run(2))))).toBe(false);
    expect(assistant(version, 2).runGroupId).toBe(group.id);
    expect(tool(version, 1).lifecycle._tag).toBe("Failed");
  });

  it("adds a compaction marker and removes nothing", () => {
    const version = scenario("mid-run-compaction");
    const top = history(version);
    const marker = top.findIndex((id) => id.startsWith("compaction:"));
    expect(marker).toBeGreaterThan(top.indexOf(assistant(version, 2).id));
    expect(top.slice(0, marker)).toEqual([
      user(version, 1).id,
      assistant(version, 1).id,
      user(version, 2).id,
      assistant(version, 2).id,
    ]);
    expect(top.slice(marker + 1)).toEqual([assistant(version, 3).id]);
    expect(tool(version, 1).lifecycle._tag).toBe("Completed");
  });

  it("joins a replacement message id to the same submission", () => {
    const version = scenario("replacement-message-id");
    expect(user(version, 2)).toMatchObject({
      messageIds: [ids.message(2), ids.message(22)],
      state: { _tag: "Consumed" },
    });
    const top = history(version);
    expect(top.indexOf(user(version, 2).id)).toBeGreaterThan(
      top.findIndex((id) => id.startsWith("compaction:")),
    );
  });

  it("applies decorations from every ingress order and degrades only invalid ones", () => {
    const version = scenario("host-decorations");
    expect(user(version, 1).decorations).toEqual({
      author: { _tag: "Value", value: "a@example.com" },
      attachments: {
        _tag: "List",
        entries: [
          {
            key: "att-1",
            value: {
              attachmentId: "att-1",
              relatedCommandIdempotencyKey: "web:1",
              ordinal: 0,
              fileName: "a-renamed.txt",
            },
          },
        ],
      },
      references: {
        _tag: "List",
        entries: [
          { key: 0, value: { kind: "doc", id: "d1" } },
          { key: 1, value: { kind: "doc", id: "d2" } },
        ],
      },
    });
    // A classified notice keeps only its own decorations: no fallback author.
    expect(user(version, 2)).toMatchObject({ classification: "subagentNotice" });
    expect(Object.keys(user(version, 2).decorations)).toEqual(["subagentChildren"]);
    // Parked decorations are drained once applied.
    const buffers = Array.from(version.records.values()).filter(
      (candidate) => candidate.kind === "decorationBuffer",
    );
    expect(
      buffers.map((buffer) => buffer.kind === "decorationBuffer" && buffer.operations.length),
    ).toEqual([0, 0, 1]);
    // No author event of its own: the latest author before it, by the declared fallback.
    expect(user(version, 3).decorations).toEqual({
      author: { _tag: "Value", value: "b@example.com" },
    });
    const session = record(version, transcriptRecordId.session);
    expect(session.kind === "sessionContext" && session.decorations).toEqual({
      owner: { _tag: "Value", value: "owner@example.com" },
      lastAuthor: { _tag: "Value", value: "c@example.com" },
    });
  });

  it("falls back to the session owner when no author was ever seen", () => {
    const log = new TestLog().configure();
    log.appendForeign("gia-agent", "GiaSessionMetadataCommitted", {
      ownerUserEmail: "owner@example.com",
    });
    log.submit({ command: 1, message: 1, text: "hi" });
    expect(user(projectBatches(gia, [log.events]), 1).decorations).toEqual({
      author: { _tag: "Value", value: "owner@example.com" },
    });
  });

  it("drops oversize decoration values without affecting the rest of the event", () => {
    const log = new TestLog();
    log.submit({ command: 1, message: 1, text: "hi", idempotencyKey: "k" });
    log.appendForeign("gia-agent", "GiaFileAttachmentCreated", {
      attachmentId: "big",
      relatedCommandIdempotencyKey: "k",
      blob: "x".repeat(20_000),
    });
    log.appendForeign("gia-agent", "GiaUserMessageAuthorCommitted", {
      relatedCommandIdempotencyKey: "k",
      userEmail: "a@example.com",
    });
    expect(user(projectBatches(gia, [log.events]), 1).decorations).toEqual({
      author: { _tag: "Value", value: "a@example.com" },
    });
  });

  it("withdraws a failed interrupt and counts a compaction's usage once", () => {
    const version = scenario("failed-interrupt-repeated-compaction-completion");
    expect(user(version, 1)).toMatchObject({
      messageIds: [],
      state: { _tag: "Cancelled", reason: "failed: start failed" },
    });
    expect(user(version, 1).position).toBeUndefined();
    const session = record(version, transcriptRecordId.session);
    expect(session.kind === "sessionContext" && session.tokenUsage.byModel[0]?.usage).toMatchObject(
      {
        inputTokens: 110,
        outputTokens: 25,
      },
    );
    const compaction = Array.from(version.records.values()).find(
      (candidate) => candidate.kind === "compaction",
    );
    expect(compaction).toMatchObject({ completedAtMs: expect.any(Number) });
  });

  it("bounds decorations: entry keys, list entries, buffered operations and record bytes", () => {
    const attachments = (log: TestLog, prefix: string, count: number, blob = 0) => {
      for (let index = 0; index < count; index += 1) {
        log.appendForeign("gia-agent", "GiaFileAttachmentCreated", {
          attachmentId: `${prefix}-${index}`,
          relatedCommandIdempotencyKey: "k",
          ...(blob === 0 ? {} : { blob: "x".repeat(blob) }),
        });
      }
    };
    const keysOf = (version: InMemoryTranscriptVersion) => {
      const list = user(version, 1).decorations["attachments"];
      return list?._tag === "List" ? list.entries.map((entry) => entry.key) : [];
    };

    // Parked before the command: an over-long key is skipped, and the buffer keeps 32.
    const early = new TestLog();
    early.appendForeign("gia-agent", "GiaFileAttachmentCreated", {
      attachmentId: "a".repeat(300),
      relatedCommandIdempotencyKey: "k",
    });
    attachments(early, "early", 40);
    early.submit({ command: 1, message: 1, text: "many", idempotencyKey: "k" });
    const parked = projectBatches(gia, [early.events]);
    expect(parked.meta.status._tag).toBe("Ok");
    expect(keysOf(parked)).toHaveLength(32);

    // Applied live: small entries stop at the 64-entry list cap.
    const small = new TestLog();
    small.submit({ command: 1, message: 1, text: "many", idempotencyKey: "k" });
    attachments(small, "small", 80);
    expect(keysOf(projectBatches(gia, [small.events]))).toHaveLength(64);

    // Applied live: large entries stop at the record's decoration byte budget first.
    const large = new TestLog();
    large.submit({ command: 1, message: 1, text: "many", idempotencyKey: "k" });
    attachments(large, "large", 80, 7_900);
    const budgeted = projectBatches(gia, [large.events]);
    expect(keysOf(budgeted).length).toBeLessThan(64);
    expect(
      new TextEncoder().encode(JSON.stringify(user(budgeted, 1).decorations)).length,
    ).toBeLessThanOrEqual(262_144);
  });

  it("normalizes decoration and body values to the JSON a stored log holds", () => {
    const log = new TestLog().configure();
    log.appendForeign("gia-agent", "GiaFileAttachmentCreated", {
      attachmentId: "att",
      relatedCommandIdempotencyKey: "k",
      contentType: undefined,
    });
    log.submit({ command: 1, message: 1, text: "hi", idempotencyKey: "k" });
    log.startRun({ run: 1, commands: [1] });
    log.turn({ run: 1, turn: 1, inputs: [1] });
    log.inferenceStarted({ run: 1, turn: 1, inference: 1 });
    log.toolCreated({ run: 1, turn: 1, inference: 1, tool: 1, providerId: "call_1", name: "t" });
    log.toolCompleted({
      tool: 1,
      providerId: "call_1",
      name: "t",
      result: { ok: true, missing: undefined },
    });
    const live = projectBatches(gia, [log.events]);
    const stored = projectBatches(gia, [JSON.parse(JSON.stringify(log.events))]);
    expect(live.meta.status._tag).toBe("Ok");
    expect(normalizedProjection(live)).toEqual(normalizedProjection(stored));
    expect(user(live, 1).decorations["attachments"]).toMatchObject({ _tag: "List" });
  });

  it("keeps an imported part with an invalid tool name as an unknown part", () => {
    const log = new TestLog();
    log.append("AssistantMessageImported", {
      messageId: ids.message(1),
      runId: ids.run(1),
      turnId: ids.turn(1),
      inferenceId: ids.inference(1),
      promptParts: [
        { type: "text", text: "imported" },
        { type: "tool-call", id: "call_1", name: "", params: {}, providerExecuted: true },
      ],
    });
    const version = projectBatches(plain, [log.events]);
    expect(version.meta.status._tag).toBe("Ok");
    const imported = Array.from(version.records.values()).find(
      (candidate) => candidate.kind === "assistant",
    );
    expect(imported?.kind === "assistant" && imported.parts).toEqual([
      { _tag: "Text", text: "imported" },
      { _tag: "Unknown", partType: "tool-call" },
    ]);
  });

  it("normalizes model settings to JSON instead of storing an unwritable row", () => {
    const log = new TestLog();
    log.append("SessionConfigured", {
      modelSelection: { provider: "test", modelId: "m", settings: { seed: 7n } },
    });
    log.submit({ command: 1, message: 1, text: "hi" });
    log.append("RunStarted", {
      runId: ids.run(1),
      commandIds: [ids.command(1)],
      modelSelection: { provider: "test", modelId: "m", settings: { seed: 7n } },
      trace: {
        root: {
          traceId: "0123456789abcdef0123456789abcdef",
          spanId: "0123456789abcdef",
          sampled: true,
          tracestate: null,
        },
        links: [],
      },
    });
    const version = projectBatches(plain, [log.events]);
    expect(version.meta.status._tag).toBe("Ok");
    const session = record(version, transcriptRecordId.session);
    expect(session.kind === "sessionContext" && session.modelSelection).toEqual({
      provider: "test",
      modelId: "m",
    });
  });

  it("refuses to plan a record no store could write", () => {
    const { events } = transcriptScenarios()[0] ?? { events: [] };
    const version = projectBatches(plain, [events.slice(0, 3)]);
    const pending = user(version, 1);
    // A corrupted stored row: a waiting submission with a position.
    const corrupted = { ...pending, position: { anchorSeq: SequenceNumber.make(1), ordinal: 0 } };
    const promote = new TestLog().append("UserMessagePromoted", {
      commandId: ids.command(9),
      messageId: ids.message(1),
      from: "queue",
      to: "steer",
    }).events;
    expect(() =>
      planTranscriptBatch(
        {
          cursor: version.meta.cursor,
          record: (id) => (id === pending.id ? corrupted : version.records.get(id)),
          alias: (namespace, key) => version.aliases.get(`${namespace}\u0000${key}`),
          nextOrdinal: () => 0,
        },
        promote.map((entry) => ({
          ...entry,
          position: { ...entry.position, seq: SequenceNumber.make(4) },
        })),
        plain,
      ),
    ).toThrow(/does not satisfy the transcript record schema/);

    // A record that matches the schema but whose envelope has no JSON form is refused too,
    // with the cause in the message.
    const session = record(version, transcriptRecordId.session);
    if (session.kind !== "sessionContext") throw new Error("expected the session record");
    const unwritable = {
      ...session,
      modelSelection: { provider: "test", modelId: "m", settings: { seed: 7n } },
    };
    const configure = new TestLog().append("CompactionStarted", {
      compactionId: ids.compaction(1),
    }).events;
    expect(() =>
      planTranscriptBatch(
        {
          cursor: version.meta.cursor,
          record: (id) => (id === session.id ? unwritable : version.records.get(id)),
          alias: () => undefined,
          nextOrdinal: () => 0,
        },
        configure.map((entry) => ({
          ...entry,
          position: { ...entry.position, seq: SequenceNumber.make(4) },
        })),
        plain,
      ),
    ).toThrow(/is not JSON: .*BigInt/);
  });

  it("keeps user fallbacks within the decoration byte budget", () => {
    const log = new TestLog();
    log.appendForeign("gia-agent", "GiaUserMessageAuthorCommitted", {
      relatedCommandIdempotencyKey: "other",
      userEmail: "x".repeat(8_000),
    });
    for (let index = 0; index < 32; index += 1) {
      log.appendForeign("gia-agent", "GiaFileAttachmentCreated", {
        attachmentId: `att-${index}`,
        relatedCommandIdempotencyKey: "k",
        blob: "y".repeat(7_900),
      });
    }
    log.submit({ command: 1, message: 1, text: "full", idempotencyKey: "k" });
    const version = projectBatches(gia, [log.events]);
    const decorations = user(version, 1).decorations;
    expect(decorations["author"]).toBeUndefined();
    expect(new TextEncoder().encode(JSON.stringify(decorations)).length).toBeLessThanOrEqual(
      262_144,
    );
  });

  it("ignores events without transcript meaning", () => {
    const version = scenario("events-without-transcript-meaning");
    expect(
      Array.from(version.records.values())
        .map((candidate) => candidate.kind)
        .sort(),
    ).toEqual(["runGroup", "sessionContext", "user"]);
    const group = record(version, transcriptRecordId.runGroup(RunId.make(ids.run(1))));
    expect(group.kind === "runGroup" && runGroupStatus(group)).toEqual({
      _tag: "Interrupted",
      reason: "legacy stop",
    });
  });

  it("normalizes legacy attempts like the reduced state does", () => {
    const version = scenario("legacy-attempts");
    expect(assistant(version, 1)).toMatchObject({
      lifecycle: { _tag: "Completed", usage: { inputTokens: 3, outputTokens: 4 } },
      parts: [{ _tag: "Text" }, { _tag: "ToolCall", toolId: tool(version, 1).id }],
    });
    expect(tool(version, 1).lifecycle._tag).toBe("Completed");
  });

  it("skips events at or below the cursor, so a re-append changes nothing", () => {
    for (const { name, events } of transcriptScenarios()) {
      const definition = name === "host-decorations" ? gia : plain;
      const once = projectBatches(definition, [events]);
      const replayed = projectBatches(definition, [events, events.slice(3), events]);
      expect(normalizedProjection(replayed)).toEqual(normalizedProjection(once));
    }
  });

  it("returns an empty plan with an unchanged cursor for an already projected batch", () => {
    const { events } = transcriptScenarios()[0] ?? { events: [] };
    const version = projectBatches(plain, [events]);
    const plan = planTranscriptBatch(
      {
        cursor: version.meta.cursor,
        record: (id) => version.records.get(id),
        alias: () => undefined,
        nextOrdinal: () => 0,
      },
      events,
      plain,
    );
    expect(plan).toEqual({ records: [], aliases: [], throughSeq: version.meta.cursor });
  });
});
