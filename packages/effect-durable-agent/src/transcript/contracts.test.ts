import * as Prompt from "effect/ai/Prompt";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  CommandId,
  CompactionId,
  ContextVersion,
  InferenceId,
  MessageId,
  RunId,
  SequenceNumber,
  SummaryId,
  ToolCallId,
  TurnId,
} from "../types/core";
import { ProviderPartId, UnixEpochMillis } from "../types/events/envelope";
import {
  TranscriptDefinitionError,
  emptyTranscriptMappings,
  makeTranscriptDefinition,
  type TranscriptMappings,
} from "./mapping";
import {
  TranscriptLogIncarnation,
  TranscriptOrder,
  TranscriptRecord,
  TranscriptRecordFromEnvelope,
  TranscriptRecordViewFromEnvelope,
  TranscriptVersion,
  UserRecordId,
  isPendingTranscriptUserRecord,
  isTopLevelTranscriptRecord,
  transcriptRecordId,
  type TranscriptRecordEnvelope,
} from "./records";
import { canonicalJson, sha256Hex, transcriptSemanticRevision } from "./version";
import { TranscriptWindow, transcriptCursorOf } from "./window";

const uuid = (suffix: number) => `018f6bd5-2f2a-7b1e-8f1b-${suffix.toString(16).padStart(12, "0")}`;
const seq = SequenceNumber.make;
const at = UnixEpochMillis.make(1_760_000_000_000);
const commandId = CommandId.make(uuid(1));
const runId = RunId.make(uuid(2));
const turnId = TurnId.make(uuid(3));
const inferenceId = InferenceId.make(uuid(4));
const toolCallId = ToolCallId.make(uuid(5));
const compactionId = CompactionId.make(uuid(6));
const version = TranscriptVersion.make("a".repeat(64));
const incarnation = TranscriptLogIncarnation.make("incarnation-1");

const records: ReadonlyArray<TranscriptRecord> = [
  {
    kind: "user",
    id: transcriptRecordId.user(commandId),
    updatedSeq: seq(4),
    position: { anchorSeq: seq(4), ordinal: 0 },
    commandId,
    messageIds: [MessageId.make(uuid(7)), MessageId.make(uuid(8))],
    disposition: "queue",
    content: [Prompt.textPart({ text: "hello" })],
    state: { _tag: "Consumed", turnId, runId },
    decorations: {
      author: { _tag: "Value", value: { email: "a@example.com" } },
      attachments: { _tag: "List", entries: [{ key: "att-1", value: { name: "a.txt" } }] },
    },
    submittedAtMs: at,
  },
  {
    kind: "assistant",
    id: transcriptRecordId.assistant(inferenceId),
    updatedSeq: seq(9),
    position: { anchorSeq: seq(5), ordinal: 0 },
    source: "inference",
    inferenceId,
    runId,
    turnId,
    runGroupId: transcriptRecordId.runGroup(runId),
    lifecycle: { _tag: "Failed", error: { message: "boom" } },
    parts: [
      { _tag: "Reasoning", text: "thinking" },
      { _tag: "Text", text: "partial" },
      {
        _tag: "ToolCall",
        providerPartId: ProviderPartId.make("call_1"),
        toolName: "search",
        providerExecuted: false,
        toolId: transcriptRecordId.tool(toolCallId),
      },
      { _tag: "Unknown", partType: "file" },
    ],
    partial: { reason: "inference failed before completion" },
  },
  {
    kind: "compaction",
    id: transcriptRecordId.compaction(compactionId),
    updatedSeq: seq(12),
    position: { anchorSeq: seq(12), ordinal: 0 },
    compactionId,
    summaryId: SummaryId.make(uuid(9)),
    contextVersion: ContextVersion.make(1),
    retainedFromContextSeq: seq(8),
    createdAtMs: at,
  },
  {
    kind: "tool",
    id: transcriptRecordId.tool(toolCallId),
    updatedSeq: seq(11),
    toolCallId,
    assistantId: transcriptRecordId.assistant(inferenceId),
    providerPartId: ProviderPartId.make("call_1"),
    toolName: "search",
    providerExecuted: false,
    params: {
      _tag: "Inline",
      value: { q: "x" },
      source: { seq: seq(6), path: ["promptPart", "params"] },
    },
    lifecycle: {
      _tag: "Completed",
      result: {
        _tag: "Inline",
        value: [1, 2],
        source: { seq: seq(11), path: ["promptPart", "result"] },
      },
    },
    createdAtMs: at,
    startedAtMs: at,
  },
  {
    kind: "runGroup",
    id: transcriptRecordId.runGroup(runId),
    updatedSeq: seq(10),
    rootRunId: runId,
    runs: [
      {
        runId,
        commandIds: [commandId],
        modelSelection: { provider: "openai", modelId: "gpt" },
        outcome: { _tag: "Interrupted", reason: "restart" },
      },
    ],
    status: { _tag: "Interrupted", reason: "restart" },
  },
  {
    kind: "sessionContext",
    id: transcriptRecordId.session,
    updatedSeq: seq(12),
    tokenUsage: { byModel: [] },
    decorations: {},
  },
  {
    kind: "decorationBuffer",
    id: transcriptRecordId.decorationBuffer("idempotencyKey", "web:1"),
    updatedSeq: seq(2),
    operations: [{ seq: seq(2), mapping: "author", operation: 0, value: "a@example.com" }],
  },
];

/** Index into a fixture list whose length the test controls. */
const nth = <A>(items: ReadonlyArray<A>, index: number): A => {
  const item = items[index];
  if (item === undefined) throw new Error(`fixture ${index} is missing`);
  return item;
};

const toEnvelope = (record: TranscriptRecord): TranscriptRecordEnvelope =>
  JSON.parse(JSON.stringify(Schema.encodeSync(TranscriptRecordFromEnvelope)(record)));

describe("transcript records", () => {
  it("round-trips every record kind through its JSON envelope", () => {
    for (const record of records) {
      const envelope = toEnvelope(record);
      expect(envelope).toMatchObject({ format: 1, kind: record.kind, id: record.id });
      expect(Schema.decodeUnknownSync(TranscriptRecordFromEnvelope)(envelope)).toEqual(record);
      expect(Schema.decodeUnknownSync(TranscriptRecordViewFromEnvelope)(envelope)).toEqual(record);
    }
  });

  it("keeps id and position for kinds and bodies this reader cannot interpret", () => {
    const newerKind = {
      format: 1,
      kind: "poll",
      id: "poll:1",
      updatedSeq: 3,
      position: { anchorSeq: 3, ordinal: 1 },
      body: { question: "?" },
    };
    expect(Schema.decodeUnknownSync(TranscriptRecordViewFromEnvelope)(newerKind)).toEqual({
      kind: "unknown",
      originalKind: "poll",
      id: "poll:1",
      updatedSeq: 3,
      position: { anchorSeq: 3, ordinal: 1 },
    });
    expect(Exit.isFailure(Schema.decodeUnknownExit(TranscriptRecordFromEnvelope)(newerKind))).toBe(
      true,
    );

    const newerLifecycle = toEnvelope(nth(records, 3));
    const changed = {
      ...newerLifecycle,
      body: { ...newerLifecycle.body, lifecycle: { _tag: "Paused" } },
    };
    expect(Schema.decodeUnknownSync(TranscriptRecordViewFromEnvelope)(changed)).toMatchObject({
      kind: "unknown",
      originalKind: "tool",
      id: nth(records, 3).id,
    });
  });

  it("rejects a newer envelope format instead of guessing", () => {
    const envelope = { ...toEnvelope(nth(records, 0)), format: 2 };
    expect(Exit.isFailure(Schema.decodeUnknownExit(TranscriptRecordFromEnvelope)(envelope))).toBe(
      true,
    );
    expect(Schema.decodeUnknownSync(TranscriptRecordViewFromEnvelope)(envelope)).toMatchObject({
      kind: "unknown",
      originalKind: "user",
    });
  });

  it("types identities by their source ids", () => {
    expect(Schema.is(UserRecordId)(`user:${commandId}`)).toBe(true);
    expect(Schema.is(UserRecordId)("user:not-a-command")).toBe(false);
    expect(Schema.is(UserRecordId)(`assistant:${commandId}`)).toBe(false);
  });

  it("separates top-level records from pending users and dependencies", () => {
    const user = nth(records, 0);
    if (user.kind !== "user") throw new Error("fixture 0 is a user record");
    const { position: _position, ...pending } = { ...user, state: { _tag: "Pending" as const } };
    expect(records.slice(0, 3).every(isTopLevelTranscriptRecord)).toBe(true);
    expect(records.slice(3).some(isTopLevelTranscriptRecord)).toBe(false);
    expect(isTopLevelTranscriptRecord(pending)).toBe(false);
    expect(isPendingTranscriptUserRecord(pending)).toBe(true);
    expect(
      isPendingTranscriptUserRecord({ ...user, state: { _tag: "Cancelled", reason: "edit" } }),
    ).toBe(false);
  });

  it("orders by anchor seq, then ordinal, then record id", () => {
    const keys = [
      { anchorSeq: 5, ordinal: 1, recordId: "a" },
      { anchorSeq: 4, ordinal: 9, recordId: "z" },
      { anchorSeq: 5, ordinal: 0, recordId: "b" },
      { anchorSeq: 5, ordinal: 1, recordId: "0" },
    ];
    expect(
      [...keys]
        .sort(TranscriptOrder)
        .map((key) => `${key.anchorSeq}.${key.ordinal}.${key.recordId}`),
    ).toEqual(["4.9.z", "5.0.b", "5.1.0", "5.1.a"]);
  });
});

describe("transcript windows", () => {
  const window = (overrides: Record<string, unknown>) => ({
    incarnation,
    version,
    throughSeq: 12,
    records: records.slice(0, 3).map(toEnvelope),
    dependencies: records.slice(3, 6).map(toEnvelope),
    hasOlder: false,
    hasNewer: false,
    readiness: { _tag: "Ready" },
    ...overrides,
  });
  const decode = Schema.decodeUnknownExit(TranscriptWindow);

  it("decodes a complete cut and mints cursors scoped to it", () => {
    const decoded = Schema.decodeUnknownSync(TranscriptWindow)(
      window({
        activeContent: [
          {
            assistantId: transcriptRecordId.assistant(inferenceId),
            parts: [{ _tag: "Text", text: "…" }],
          },
        ],
      }),
    );
    expect(decoded.records.map((record) => record.id)).toEqual(
      records.slice(0, 3).map((r) => r.id),
    );
    expect(transcriptCursorOf(decoded, nth(decoded.records, 1))).toEqual({
      incarnation,
      version,
      anchorSeq: 5,
      ordinal: 0,
      recordId: transcriptRecordId.assistant(inferenceId),
    });
    expect(transcriptCursorOf(decoded, nth(decoded.dependencies, 0))).toBeUndefined();
  });

  it("rejects any record newer than the cut", () => {
    expect(Exit.isFailure(decode(window({ throughSeq: 11 })))).toBe(true);
  });

  it("never attaches live content to a degraded read", () => {
    const activeContent = [{ assistantId: transcriptRecordId.assistant(inferenceId), parts: [] }];
    for (const readiness of [
      { _tag: "Degraded", failedAtSeq: 13, error: "decode failed" },
      { _tag: "UpdatingHistory" },
      { _tag: "Building" },
    ]) {
      expect(Exit.isSuccess(decode(window({ readiness })))).toBe(true);
      expect(Exit.isFailure(decode(window({ readiness, activeContent })))).toBe(true);
    }
  });

  it("keeps unknown records in place on an older reader", () => {
    const decoded = Schema.decodeUnknownSync(TranscriptWindow)(
      window({
        records: [
          ...records.slice(0, 2).map(toEnvelope),
          {
            format: 1,
            kind: "poll",
            id: "poll:1",
            updatedSeq: 12,
            position: { anchorSeq: 12, ordinal: 1 },
            body: {},
          },
        ],
      }),
    );
    expect(decoded.records.map((record) => record.kind)).toEqual(["user", "assistant", "unknown"]);
  });
});

const giaLikeMappings: TranscriptMappings = {
  mappings: [
    {
      name: "author",
      event: { namespace: "gia-agent", type: "GiaUserMessageAuthorCommitted", schemaVersion: 1 },
      target: { _tag: "UserByIdempotencyKey", path: ["relatedCommandIdempotencyKey"] },
      operations: [{ _tag: "FirstWins", from: ["userEmail"], to: "author" }],
    },
    {
      name: "lastAuthor",
      event: { namespace: "gia-agent", type: "GiaUserMessageAuthorCommitted", schemaVersion: 1 },
      target: { _tag: "Session" },
      operations: [{ _tag: "Copy", from: ["userEmail"], to: "lastAuthor" }],
    },
    {
      name: "attachments",
      event: { namespace: "gia-agent", type: "GiaFileAttachmentCreated", schemaVersion: 1 },
      target: { _tag: "UserByIdempotencyKey", path: ["relatedCommandIdempotencyKey"] },
      operations: [
        {
          _tag: "ReplaceListEntry",
          from: [],
          to: "attachments",
          key: { _tag: "Id", path: ["attachmentId"] },
        },
      ],
    },
    {
      name: "subagentNotice",
      event: { namespace: "gia-agent", type: "GiaSubagentNotificationCommitted", schemaVersion: 1 },
      target: { _tag: "UserByCommandId", path: ["notificationId"] },
      operations: [
        { _tag: "Classify", classification: "subagentNotice" },
        { _tag: "FirstWins", from: ["children"], to: "subagentChildren" },
      ],
    },
  ],
  userFallbacks: [{ to: "author", fromSession: "lastAuthor" }],
};

const definitionExit = (input: unknown) =>
  Effect.runSync(Effect.exit(makeTranscriptDefinition(input)));
const definitionError = (input: unknown): string => {
  const exit = definitionExit(input);
  if (Exit.isSuccess(exit)) throw new Error("expected the definition to be rejected");
  const error = exit.cause.reasons.find((reason) => reason._tag === "Fail");
  if (error === undefined || !(error.error instanceof TranscriptDefinitionError)) {
    throw new Error("expected TranscriptDefinitionError");
  }
  return error.error.message;
};

describe("transcript definitions", () => {
  it("accepts host decorations and derives a stable version from the canonical data", () => {
    const definition = Effect.runSync(makeTranscriptDefinition(giaLikeMappings));
    const reordered = JSON.parse(
      JSON.stringify(giaLikeMappings, (_key, value) =>
        value !== null && typeof value === "object" && !Array.isArray(value)
          ? Object.fromEntries(Object.entries(value).reverse())
          : value,
      ),
    );
    expect(definition.semanticRevision).toBe(transcriptSemanticRevision);
    expect(Effect.runSync(makeTranscriptDefinition(reordered)).version).toBe(definition.version);
    expect(Effect.runSync(makeTranscriptDefinition(emptyTranscriptMappings)).version).not.toBe(
      definition.version,
    );
    expect(
      Effect.runSync(
        makeTranscriptDefinition({
          ...giaLikeMappings,
          mappings: giaLikeMappings.mappings.slice(1),
          userFallbacks: [],
        }),
      ).version,
    ).not.toBe(definition.version);
  });

  it("rejects invalid mapping data at registration", () => {
    const author = nth(giaLikeMappings.mappings, 0);
    const lastAuthor = nth(giaLikeMappings.mappings, 1);
    const attachments = nth(giaLikeMappings.mappings, 2);
    expect(definitionError({ mappings: [author, author], userFallbacks: [] })).toContain(
      "duplicate mapping name",
    );
    expect(
      definitionError({
        mappings: [{ ...author, event: { ...author.event, namespace: "effect-durable-agent" } }],
        userFallbacks: [],
      }),
    ).toContain("may not read namespace");
    expect(
      definitionError({
        mappings: [{ ...lastAuthor, operations: [{ _tag: "Classify", classification: "x" }] }],
        userFallbacks: [],
      }),
    ).toContain("only users can be classified");
    expect(
      definitionError({
        mappings: [
          author,
          { ...attachments, operations: [{ ...attachments.operations[0], to: "author" }] },
        ],
        userFallbacks: [],
      }),
    ).toContain("written as both");
    expect(
      definitionError({
        mappings: [author],
        userFallbacks: [{ to: "author", fromSession: "lastAuthor" }],
      }),
    ).toContain("no mapping writes");
    expect(
      definitionError({
        mappings: [{ ...author, operations: [{ _tag: "Copy", from: ["x"], to: "not a key" }] }],
        userFallbacks: [],
      }),
    ).not.toBe("");
  });
});

describe("transcript version identity", () => {
  it("hashes with SHA-256", () => {
    expect(sha256Hex("")).toBe("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    expect(sha256Hex("abc")).toBe(
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    );
    expect(sha256Hex("a".repeat(1_000))).toBe(
      "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3",
    );
  });

  it("encodes JSON canonically", () => {
    expect(canonicalJson({ b: [1, { d: true, c: null }], a: "x", u: undefined })).toBe(
      '{"a":"x","b":[1,{"c":null,"d":true}]}',
    );
    expect(() => canonicalJson({ n: Number.NaN })).toThrow();
  });
});
