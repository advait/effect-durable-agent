import * as Order from "effect/Order";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as SchemaTransformation from "effect/SchemaTransformation";

import { TokenConsumptionState } from "../domain/model-usage";
import { sha256Hex } from "./hash";
import { JsonUserMessageContent } from "../domain/reduced-state-schema";
import { CommandIdempotencyKey, SubmitMessageDisposition } from "../types/commands";
import {
  CommandId,
  CompactionId,
  ContextVersion,
  EventId,
  InferenceId,
  MessageId,
  RunId,
  SequenceNumber,
  SummaryId,
  ToolCallId,
  TurnId,
} from "../types/core";
import {
  FailurePayload,
  ModelSelectionPayload,
  NonNegativeInt,
  ProviderPartId,
  ToolName,
  UnixEpochMillis,
  UsagePayload,
} from "../types/events/envelope";
import { ToolCallRejectionReason } from "../types/events/durable";

// ---------------------------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------------------------

/** One logical user submission across every lifecycle state: `user:<commandId>`. */
export const UserRecordId = Schema.TemplateLiteral(["user:", CommandId]);
export type UserRecordId = typeof UserRecordId.Type;

/** One model inference's visible output: `assistant:<inferenceId>`. */
export const InferenceAssistantRecordId = Schema.TemplateLiteral(["assistant:", InferenceId]);
export type InferenceAssistantRecordId = typeof InferenceAssistantRecordId.Type;

/** Assistant context imported from another transport, keyed by its import event: `import:<eventId>`. */
export const ImportAssistantRecordId = Schema.TemplateLiteral(["import:", EventId]);
export type ImportAssistantRecordId = typeof ImportAssistantRecordId.Type;

/** Either assistant identity; both are top-level assistant records. */
export const AssistantRecordId = Schema.Union([
  InferenceAssistantRecordId,
  ImportAssistantRecordId,
]);
export type AssistantRecordId = typeof AssistantRecordId.Type;

/** The marker a context rebase leaves in history: `compaction:<compactionId>`. */
export const CompactionRecordId = Schema.TemplateLiteral(["compaction:", CompactionId]);
export type CompactionRecordId = typeof CompactionRecordId.Type;

/** A framework tool call, a dependency of its assistant: `tool:<toolCallId>`. */
export const ToolRecordId = Schema.TemplateLiteral(["tool:", ToolCallId]);
export type ToolRecordId = typeof ToolRecordId.Type;

/**
 * A run plus every startup-recovery continuation of it: `run:<rootRunId>`.
 *
 * The id names the first run of the chain and never changes when a continuation joins.
 */
export const RunGroupRecordId = Schema.TemplateLiteral(["run:", RunId]);
export type RunGroupRecordId = typeof RunGroupRecordId.Type;

/** The session-context singleton. */
export const SessionContextRecordId = Schema.Literal("session");
export type SessionContextRecordId = typeof SessionContextRecordId.Type;

/**
 * Internal holding record for decorations whose target user record does not exist yet.
 *
 * Hosts append decoration events before the command they describe (Gia submits
 * `[author, attachments…, SubmitMessage]` in one batch), so the interpreter parks them under the
 * association key and merges them when the user record is created. Never served by a window read.
 */
export const DecorationBufferRecordId = Schema.TemplateLiteral([
  "decorations:",
  Schema.Literals(["command", "idempotencyKey"]),
  ":",
  Schema.NonEmptyString.check(Schema.isMaxLength(512)),
]);
export type DecorationBufferRecordId = typeof DecorationBufferRecordId.Type;

/** Every transcript record identity this EDA release writes. */
export const TranscriptRecordId = Schema.Union([
  UserRecordId,
  AssistantRecordId,
  CompactionRecordId,
  ToolRecordId,
  RunGroupRecordId,
  SessionContextRecordId,
  DecorationBufferRecordId,
]);
export type TranscriptRecordId = typeof TranscriptRecordId.Type;

/** Constructors for record identities; the only place identities are spelled. */
export const transcriptRecordId = {
  user: (commandId: CommandId): UserRecordId => `user:${commandId}`,
  assistant: (inferenceId: InferenceId): InferenceAssistantRecordId => `assistant:${inferenceId}`,
  import: (eventId: EventId): ImportAssistantRecordId => `import:${eventId}`,
  compaction: (compactionId: CompactionId): CompactionRecordId => `compaction:${compactionId}`,
  tool: (toolCallId: ToolCallId): ToolRecordId => `tool:${toolCallId}`,
  runGroup: (rootRunId: RunId): RunGroupRecordId => `run:${rootRunId}`,
  session: "session",
  /**
   * Keys longer than 256 characters (caller-owned idempotency keys) are hashed to stay bounded;
   * keys starting with `#` are hashed too, so a literal key never collides with a hashed one.
   */
  decorationBuffer: (
    association: "command" | "idempotencyKey",
    key: string,
  ): DecorationBufferRecordId =>
    `decorations:${association}:${
      key.length > 0 && key.length <= 256 && !key.startsWith("#")
        ? key
        : `#sha256:${sha256Hex(key)}`
    }`,
} as const;

// ---------------------------------------------------------------------------------------------
// Position and cursor
// ---------------------------------------------------------------------------------------------

/**
 * History position of a top-level record: the event that placed it and an ordinal among records
 * placed by that same event. Assigned once and never moved.
 */
export const TranscriptPosition = Schema.Struct({
  anchorSeq: SequenceNumber,
  ordinal: NonNegativeInt,
});
export type TranscriptPosition = typeof TranscriptPosition.Type;

/**
 * Adapter-owned identity of one physical event log. A wiped log gets a new incarnation, which
 * invalidates every cursor, chunk and body reference minted against the old one.
 */
export const TranscriptLogIncarnation = Schema.NonEmptyString.pipe(
  Schema.brand("TranscriptLogIncarnation"),
);
export type TranscriptLogIncarnation = typeof TranscriptLogIncarnation.Type;

/**
 * Identity of one transcript interpretation: a hash of the EDA semantic revision and the host's
 * canonical mapping data. Two sessions with equal versions interpret the same log identically.
 */
export const TranscriptVersion = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)).pipe(
  Schema.brand("TranscriptVersion"),
);
export type TranscriptVersion = typeof TranscriptVersion.Type;

/**
 * Paging cursor: a point in the total order `(anchorSeq, ordinal, recordId)`.
 *
 * A cursor is meaningful only for the incarnation and version that minted it; readers reject
 * any other pairing rather than guess an equivalent point.
 */
export const TranscriptCursor = Schema.Struct({
  incarnation: TranscriptLogIncarnation,
  version: TranscriptVersion,
  anchorSeq: SequenceNumber,
  ordinal: NonNegativeInt,
  recordId: Schema.NonEmptyString,
});
export type TranscriptCursor = typeof TranscriptCursor.Type;

/** A point in transcript order, without the incarnation and version scope of a cursor. */
export interface TranscriptOrderKey {
  readonly anchorSeq: number;
  readonly ordinal: number;
  readonly recordId: string;
}

/** Total transcript order over top-level records. */
export const TranscriptOrder: Order.Order<TranscriptOrderKey> = Order.Struct({
  anchorSeq: Order.Number,
  ordinal: Order.Number,
  recordId: Order.String,
});

// ---------------------------------------------------------------------------------------------
// Shared values
// ---------------------------------------------------------------------------------------------

/** A JSON path into an event payload; the empty path is the payload itself. */
export const TranscriptFieldPath = Schema.Array(Schema.Union([Schema.String, NonNegativeInt]));
export type TranscriptFieldPath = typeof TranscriptFieldPath.Type;

/** The immutable log location a body was read from. */
export const TranscriptBodySource = Schema.Struct({
  seq: SequenceNumber,
  path: TranscriptFieldPath,
});
export type TranscriptBodySource = typeof TranscriptBodySource.Type;

/**
 * Largest body a stored record holds inline, in canonical JSON bytes. Larger bodies are stored
 * as references, which keeps every stored row far below a Durable Object's 2 MB value limit no
 * matter how large an event payload is.
 */
export const transcriptStoredInlineBodyLimit = 65_536;

/**
 * A potentially large body (tool parameters and results).
 *
 * Stored records hold a body inline up to `transcriptStoredInlineBodyLimit` and as a reference
 * above it; a window read may also replace a smaller inline body with a reference. A reference
 * points at the body's immutable source in the append-only log, so it stays resolvable after
 * projection rows are rebuilt or deleted. The host authorizes reference resolution.
 */
export const TranscriptBody = Schema.TaggedUnion({
  Inline: { value: Schema.Json, source: TranscriptBodySource },
  Ref: { source: TranscriptBodySource, byteLength: NonNegativeInt },
});
export type TranscriptBody = typeof TranscriptBody.Type;

/** Host-defined decoration name, as written by a mapping operation. */
export const TranscriptDecorationKey = Schema.String.check(
  Schema.isPattern(/^[A-Za-z][A-Za-z0-9_.-]{0,63}$/),
);
export type TranscriptDecorationKey = typeof TranscriptDecorationKey.Type;

/** Key of one decoration list entry: an entry id or a host-assigned ordinal. */
export const TranscriptDecorationEntryKey = Schema.Union([
  Schema.String.check(Schema.isMaxLength(256)),
  NonNegativeInt,
]);
export type TranscriptDecorationEntryKey = typeof TranscriptDecorationEntryKey.Type;

/**
 * A host decoration value: a JSON scalar/object, or a keyed list whose entries are replaced by id
 * or ordinal. EDA treats the JSON as opaque; only the host gives it meaning.
 */
export const TranscriptDecoration = Schema.TaggedUnion({
  Value: { value: Schema.Json },
  List: {
    entries: Schema.Array(Schema.Struct({ key: TranscriptDecorationEntryKey, value: Schema.Json })),
  },
});
export type TranscriptDecoration = typeof TranscriptDecoration.Type;

/** Decorations of one record, keyed by host decoration name. */
export const TranscriptDecorations = Schema.Record(TranscriptDecorationKey, TranscriptDecoration);
export type TranscriptDecorations = typeof TranscriptDecorations.Type;

/** Host classification of a user record, such as an authenticated subagent notice. */
export const TranscriptClassification = TranscriptDecorationKey;
export type TranscriptClassification = typeof TranscriptClassification.Type;

const LifecycleTimingFields = {
  startedAtMs: Schema.optionalKey(UnixEpochMillis),
  terminalAtMs: Schema.optionalKey(UnixEpochMillis),
  durationMs: Schema.optionalKey(NonNegativeInt),
};

// ---------------------------------------------------------------------------------------------
// Record bodies
// ---------------------------------------------------------------------------------------------

/**
 * Where a user submission is in its lifecycle.
 *
 * Only `Consumed` records are rendered in history. `Pending`, `Paused`, `Promoted` and `Steering`
 * are waiting to be read by the agent and are served as dependencies. `Cancelled` records stay
 * stored so their aliases resolve but are never rendered.
 */
export const TranscriptUserState = Schema.TaggedUnion({
  /** Admitted and queued for a later run. */
  Pending: {},
  /** Queued, but held because the user interrupted the run that would have read it. */
  Paused: { byCommandId: CommandId },
  /** Moved from the queue to steer the active run. */
  Promoted: {},
  /** Waiting to steer the active run. */
  Steering: {},
  /** Read by the agent; the record's position is the consumption point. */
  Consumed: {
    turnId: Schema.optionalKey(TurnId),
    runId: Schema.optionalKey(RunId),
  },
  /** Withdrawn before the agent read it, or withdrawn from the model context. */
  Cancelled: {
    reason: Schema.String,
    byCommandId: Schema.optionalKey(CommandId),
  },
});
export type TranscriptUserState = typeof TranscriptUserState.Type;

const UserBodyFields = {
  commandId: CommandId,
  /** Every message id that has carried this submission; a replacement id joins as an alias. */
  messageIds: Schema.Array(MessageId),
  idempotencyKey: Schema.optionalKey(CommandIdempotencyKey),
  disposition: Schema.optionalKey(SubmitMessageDisposition),
  /**
   * JSON projection of the content: file bytes become data URLs, URLs become strings. Content is
   * inline; file data belongs in host attachments rather than message content.
   */
  content: JsonUserMessageContent,
  state: TranscriptUserState,
  /** Present when the submission arrived as steering for a specific run. */
  steering: Schema.optionalKey(Schema.Struct({ runId: RunId })),
  classification: Schema.optionalKey(TranscriptClassification),
  decorations: TranscriptDecorations,
  /** The first event that named this submission; pending users are served in this order. */
  submittedSeq: SequenceNumber,
  submittedAtMs: UnixEpochMillis,
  /** Present exactly when `position` is: the record was consumed at some point. */
  consumedAtMs: Schema.optionalKey(UnixEpochMillis),
};

/** Lifecycle of one assistant record. Imports are `Completed` from the start. */
export const TranscriptAssistantLifecycle = Schema.TaggedUnion({
  Streaming: {},
  Completed: {
    finishReason: Schema.optionalKey(Schema.String),
    usage: Schema.optionalKey(UsagePayload),
  },
  Failed: { error: FailurePayload },
});
export type TranscriptAssistantLifecycle = typeof TranscriptAssistantLifecycle.Type;

/**
 * One ordered part of assistant output. Tool calls are references; their bodies live on the tool
 * dependency. Part types this release does not render are kept as `Unknown` so ordering survives.
 */
const KnownAssistantPart = Schema.TaggedUnion({
  Text: { text: Schema.String },
  Reasoning: { text: Schema.String },
  ToolCall: {
    providerPartId: ProviderPartId,
    toolName: ToolName,
    providerExecuted: Schema.Boolean,
    /** The framework tool record, once the call has been sealed into one. */
    toolId: Schema.optionalKey(ToolRecordId),
  },
  /** Result of a provider-executed tool, which has no framework tool record. */
  ToolResult: {
    providerPartId: ProviderPartId,
    toolName: ToolName,
    isFailure: Schema.Boolean,
    result: TranscriptBody,
  },
  Unknown: { partType: Schema.String },
});

/** A part whose tag or shape this reader does not know keeps its place as `Unknown`. */
const knownAssistantPartTags: ReadonlySet<string> = new Set([
  "Text",
  "Reasoning",
  "ToolCall",
  "ToolResult",
  "Unknown",
]);

const UnrecognizedAssistantPart = Schema.Struct({
  _tag: Schema.String.check(
    Schema.makeFilter(
      (tag) => !knownAssistantPartTags.has(tag) || "a known part tag must decode as that part",
    ),
  ),
}).pipe(
  Schema.decodeTo(Schema.toType(KnownAssistantPart.cases.Unknown), {
    decode: SchemaGetter.transform((part) => ({ _tag: "Unknown" as const, partType: part._tag })),
    encode: SchemaGetter.transform((part) => ({ _tag: part.partType })),
  }),
);

/**
 * One ordered part of assistant output. A part tag this reader does not know decodes as
 * `Unknown`, keeping its place; a malformed part with a known tag is a decode failure.
 */
export const TranscriptAssistantPart = Schema.Union([
  KnownAssistantPart,
  UnrecognizedAssistantPart,
]);
export type TranscriptAssistantPart = typeof TranscriptAssistantPart.Type;

const AssistantBodyFields = {
  source: Schema.Literals(["inference", "import"]),
  inferenceId: InferenceId,
  runId: RunId,
  turnId: TurnId,
  /** Run group of the producing run; absent for imports and for runs the log never started. */
  runGroupId: Schema.optionalKey(RunGroupRecordId),
  messageId: Schema.optionalKey(MessageId),
  lifecycle: TranscriptAssistantLifecycle,
  parts: Schema.Array(TranscriptAssistantPart),
  /** Present when the visible parts were preserved from an interrupted or failed inference. */
  partial: Schema.optionalKey(Schema.Struct({ reason: Schema.String })),
  ...LifecycleTimingFields,
};

const CompactionBodyFields = {
  compactionId: CompactionId,
  /** Reference to the summary artifact; the transcript never copies summary text. */
  summaryId: SummaryId,
  contextVersion: ContextVersion,
  retainedFromContextSeq: SequenceNumber,
  createdAtMs: UnixEpochMillis,
};

/** Lifecycle of one framework tool call. The last terminal event wins. */
export const TranscriptToolLifecycle = Schema.TaggedUnion({
  Created: {},
  Running: {},
  Completed: { result: TranscriptBody },
  Failed: { error: FailurePayload },
  /** The call was not executable; terminal from creation. */
  Rejected: {
    reason: ToolCallRejectionReason,
    message: Schema.String,
    modelFeedback: Schema.String,
  },
});
export type TranscriptToolLifecycle = typeof TranscriptToolLifecycle.Type;

const ToolBodyFields = {
  toolCallId: ToolCallId,
  assistantId: InferenceAssistantRecordId,
  providerPartId: ProviderPartId,
  toolName: ToolName,
  providerExecuted: Schema.Boolean,
  /** Absent for a rejected call: a rejection records only the model-facing feedback. */
  params: Schema.optionalKey(TranscriptBody),
  lifecycle: TranscriptToolLifecycle,
  createdAtMs: UnixEpochMillis,
  ...LifecycleTimingFields,
};

/** Outcome of one run inside a run group. */
export const TranscriptRunOutcome = Schema.TaggedUnion({
  Running: {},
  Completed: {},
  Failed: { error: FailurePayload },
  /** Stopped by the user or by a runtime restart; a continuation may follow. */
  Interrupted: { reason: Schema.String },
});
export type TranscriptRunOutcome = typeof TranscriptRunOutcome.Type;

/** One run in a run group, in start order. */
export const TranscriptRunMember = Schema.Struct({
  runId: RunId,
  commandIds: Schema.Array(CommandId),
  modelSelection: ModelSelectionPayload,
  outcome: TranscriptRunOutcome,
  /** The interrupted run this one continues, as recorded by startup recovery. */
  continuationOf: Schema.optionalKey(RunId),
  ...LifecycleTimingFields,
});
export type TranscriptRunMember = typeof TranscriptRunMember.Type;

const RunGroupBodyFields = {
  rootRunId: RunId,
  /** Every run of the chain in start order; the last member's outcome is the group's status. */
  runs: Schema.NonEmptyArray(TranscriptRunMember),
};

const SessionContextBodyFields = {
  /** The run currently executing, if any. */
  activeRunId: Schema.optionalKey(RunId),
  /** The compaction currently in progress, if any. */
  activeCompactionId: Schema.optionalKey(CompactionId),
  /** Session execution policy: the first configured or started model, as the runtime reads it. */
  modelSelection: Schema.optionalKey(ModelSelectionPayload),
  /** Model of the most recently started run. */
  lastRunModelSelection: Schema.optionalKey(ModelSelectionPayload),
  tokenUsage: TokenConsumptionState,
  decorations: TranscriptDecorations,
};

/** One decoration operation parked until its target exists, in arrival order. */
export const TranscriptBufferedDecoration = Schema.Struct({
  seq: SequenceNumber,
  mapping: TranscriptDecorationKey,
  /** Operation index within the mapping. */
  operation: NonNegativeInt,
  value: Schema.Json,
  entryKey: Schema.optionalKey(TranscriptDecorationEntryKey),
});
export type TranscriptBufferedDecoration = typeof TranscriptBufferedDecoration.Type;

/**
 * Operations one decoration buffer keeps. Later operations for the same key are dropped, as are
 * operations beyond the mapping bounds' decoration byte budget.
 */
export const transcriptDecorationBufferLimit = 32;

const DecorationBufferBodyFields = {
  operations: Schema.Array(TranscriptBufferedDecoration).check(
    Schema.isMaxLength(transcriptDecorationBufferLimit),
  ),
};

// ---------------------------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------------------------

const commonFields = <Kind extends string, Id extends Schema.Top>(kind: Kind, id: Id) => ({
  kind: Schema.tag(kind),
  id,
  /** The last event that contributed to this record. */
  updatedSeq: SequenceNumber,
});

/**
 * User submission, top-level once consumed; waiting states are dependencies.
 *
 * A position is assigned once, when the agent first reads the submission, so a consumed record
 * always has one and a waiting record never does. A cancelled record keeps the position it was
 * consumed at, if any, and is never rendered.
 */
export const TranscriptUserRecord = Schema.Struct({
  ...commonFields("user", UserRecordId),
  position: Schema.optionalKey(TranscriptPosition),
  ...UserBodyFields,
}).check(
  Schema.makeFilter(
    (user) =>
      (user.position === undefined) === (user.consumedAtMs === undefined) ||
      "position and consumedAtMs are assigned together",
  ),
  Schema.makeFilter(
    (user) => user.id === `user:${user.commandId}` || "a user record id names its command",
  ),
  Schema.makeFilter((user) => {
    switch (user.state._tag) {
      case "Consumed":
        return user.position !== undefined || "a consumed user record has a position";
      case "Cancelled":
        return true;
      default:
        return user.position === undefined || "a waiting user record has no position";
    }
  }),
);
export type TranscriptUserRecord = typeof TranscriptUserRecord.Type;

/** Assistant output of one inference or one import; top-level. */
export const TranscriptAssistantRecord = Schema.Struct({
  ...commonFields("assistant", AssistantRecordId),
  position: TranscriptPosition,
  ...AssistantBodyFields,
}).check(
  Schema.makeFilter(
    (assistant) =>
      (assistant.source === "import"
        ? assistant.id.startsWith("import:")
        : assistant.id === `assistant:${assistant.inferenceId}`) ||
      "an assistant record id matches its source and inference",
  ),
);
export type TranscriptAssistantRecord = typeof TranscriptAssistantRecord.Type;

/** Compaction marker; top-level. */
export const TranscriptCompactionRecord = Schema.Struct({
  ...commonFields("compaction", CompactionRecordId),
  position: TranscriptPosition,
  ...CompactionBodyFields,
}).check(
  Schema.makeFilter(
    (compaction) =>
      compaction.id === `compaction:${compaction.compactionId}` ||
      "a compaction record id names its compaction",
  ),
);
export type TranscriptCompactionRecord = typeof TranscriptCompactionRecord.Type;

/** Framework tool call; a dependency of its assistant record. */
export const TranscriptToolRecord = Schema.Struct({
  ...commonFields("tool", ToolRecordId),
  ...ToolBodyFields,
}).check(
  Schema.makeFilter(
    (tool) => tool.id === `tool:${tool.toolCallId}` || "a tool record id names its tool call",
  ),
);
export type TranscriptToolRecord = typeof TranscriptToolRecord.Type;

/** Run and its recovery continuations; a dependency of the records it produced. */
export const TranscriptRunGroupRecord = Schema.Struct({
  ...commonFields("runGroup", RunGroupRecordId),
  ...RunGroupBodyFields,
}).check(
  Schema.makeFilter(
    (group) =>
      (group.id === `run:${group.rootRunId}` && group.runs[0].runId === group.rootRunId) ||
      "a run group is named by, and starts with, its root run",
  ),
);
export type TranscriptRunGroupRecord = typeof TranscriptRunGroupRecord.Type;

/** A run group's status: the outcome of its latest run. */
export const runGroupStatus = (group: TranscriptRunGroupRecord): TranscriptRunOutcome =>
  (group.runs[group.runs.length - 1] ?? group.runs[0]).outcome;

/** Session activity, model selection and token totals; a singleton dependency. */
export const TranscriptSessionContextRecord = Schema.Struct({
  ...commonFields("sessionContext", SessionContextRecordId),
  ...SessionContextBodyFields,
});
export type TranscriptSessionContextRecord = typeof TranscriptSessionContextRecord.Type;

/** Internal decoration holding record; stored, never served. */
export const TranscriptDecorationBufferRecord = Schema.Struct({
  ...commonFields("decorationBuffer", DecorationBufferRecordId),
  ...DecorationBufferBodyFields,
});
export type TranscriptDecorationBufferRecord = typeof TranscriptDecorationBufferRecord.Type;

/** Every record kind this EDA release stores. */
export const TranscriptRecord = Schema.Union([
  TranscriptUserRecord,
  TranscriptAssistantRecord,
  TranscriptCompactionRecord,
  TranscriptToolRecord,
  TranscriptRunGroupRecord,
  TranscriptSessionContextRecord,
  TranscriptDecorationBufferRecord,
]);
export type TranscriptRecord = typeof TranscriptRecord.Type;
export type TranscriptRecordKind = TranscriptRecord["kind"];

/**
 * A record this reader cannot interpret: a newer kind, or a body written by a newer EDA.
 *
 * It keeps the identity, position and revision so a client can hold its place and render a safe
 * placeholder. Storage readers do not produce it: an undecodable stored row makes a read
 * unavailable instead of wrong.
 */
export const TranscriptUnknownRecord = Schema.Struct({
  kind: Schema.tag("unknown"),
  originalKind: Schema.NonEmptyString,
  /** The envelope format it arrived in, kept so it re-encodes exactly as it was read. */
  format: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  /** The uninterpreted body, kept so a relaying reader forwards it unchanged. */
  body: Schema.Record(Schema.String, Schema.Unknown),
  id: Schema.NonEmptyString,
  updatedSeq: SequenceNumber,
  position: Schema.optionalKey(TranscriptPosition),
});
export type TranscriptUnknownRecord = typeof TranscriptUnknownRecord.Type;

/** What a wire reader sees for one record. */
export type TranscriptRecordView = TranscriptRecord | TranscriptUnknownRecord;

/** A user record the agent has read: positioned history. */
export type ConsumedTranscriptUserRecord = TranscriptUserRecord & {
  readonly state: Extract<TranscriptUserState, { readonly _tag: "Consumed" }>;
  readonly position: TranscriptPosition;
};

/** A user record still waiting for the agent. */
export type PendingTranscriptUserRecord = TranscriptUserRecord & {
  readonly state: Extract<
    TranscriptUserState,
    { readonly _tag: "Pending" | "Paused" | "Promoted" | "Steering" }
  >;
};

/** A positioned record that counts toward a window's record limit. */
export type TranscriptTopLevelRecord =
  | ConsumedTranscriptUserRecord
  | TranscriptAssistantRecord
  | TranscriptCompactionRecord;

/** Top-level kinds count toward a window's record limit and carry a position. */
export const isTopLevelTranscriptRecord = (
  record: TranscriptRecord,
): record is TranscriptTopLevelRecord =>
  record.kind === "assistant" ||
  record.kind === "compaction" ||
  (record.kind === "user" && record.state._tag === "Consumed" && record.position !== undefined);

/** A user record still waiting for the agent; served as a dependency, never twice. */
export const isPendingTranscriptUserRecord = (
  record: TranscriptRecord,
): record is PendingTranscriptUserRecord =>
  record.kind === "user" &&
  (record.state._tag === "Pending" ||
    record.state._tag === "Paused" ||
    record.state._tag === "Promoted" ||
    record.state._tag === "Steering");

// ---------------------------------------------------------------------------------------------
// Stored envelope
// ---------------------------------------------------------------------------------------------

/**
 * Current stored-row envelope format. Readers accept every format up to this one.
 *
 * Bump it whenever a known kind gains a value an older reader of this format could not decode
 * (a new lifecycle or state tag, a new required field). Older readers then show such rows as
 * `unknown` records; without a bump, a strict older reader would reject the whole window.
 */
export const transcriptRecordEnvelopeFormat = 1;

/**
 * Backward-readable row and wire shape for one record.
 *
 * Identity, kind, revision and position sit outside the kind-specific `body` so any reader can
 * place a record it cannot interpret.
 */
export const TranscriptRecordEnvelope = Schema.Struct({
  format: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  kind: Schema.NonEmptyString,
  id: Schema.NonEmptyString,
  updatedSeq: SequenceNumber,
  position: Schema.optionalKey(TranscriptPosition),
  body: Schema.Record(Schema.String, Schema.Unknown),
});
export type TranscriptRecordEnvelope = typeof TranscriptRecordEnvelope.Type;

const envelopeToFlat = (envelope: TranscriptRecordEnvelope): Record<string, unknown> => {
  const { kind, id, updatedSeq, position, body } = envelope;
  // Identity, revision and position come only from the envelope, never from the body.
  const { kind: _kind, id: _id, updatedSeq: _updatedSeq, position: _position, ...fields } = body;
  return {
    ...fields,
    kind,
    id,
    updatedSeq,
    ...(position === undefined ? {} : { position }),
  };
};

const flatToEnvelope = (record: Record<string, unknown>): TranscriptRecordEnvelope => {
  const { kind, id, updatedSeq, position, ...body } = record;
  return Schema.decodeUnknownSync(TranscriptRecordEnvelope)({
    format: transcriptRecordEnvelopeFormat,
    kind,
    id,
    updatedSeq,
    ...(position === undefined ? {} : { position }),
    body,
  });
};

const FlatRecordFields = Schema.Record(Schema.String, Schema.Unknown);

/** A known record stored or sent as its envelope. Decoding fails for anything this reader cannot interpret. */
export const TranscriptRecordFromEnvelope = TranscriptRecordEnvelope.check(
  Schema.makeFilter(
    (envelope) =>
      envelope.format <= transcriptRecordEnvelopeFormat ||
      `unsupported transcript record envelope format ${envelope.format}`,
  ),
).pipe(
  Schema.decodeTo(FlatRecordFields, {
    decode: SchemaGetter.transform(envelopeToFlat),
    encode: SchemaGetter.transform(flatToEnvelope),
  }),
  Schema.decodeTo(TranscriptRecord, SchemaTransformation.passthroughSupertype()),
);

const knownRecordKinds: ReadonlySet<string> = new Set([
  "user",
  "assistant",
  "compaction",
  "tool",
  "runGroup",
  "sessionContext",
  "decorationBuffer",
]);

/**
 * Only a kind this reader does not know, or a newer envelope format, becomes `unknown`; a
 * malformed record of a known kind in a known format is a decode failure, never a placeholder.
 */
const TranscriptUnknownRecordFromEnvelope = TranscriptRecordEnvelope.check(
  Schema.makeFilter(
    (envelope) =>
      !knownRecordKinds.has(envelope.kind) ||
      envelope.format > transcriptRecordEnvelopeFormat ||
      `a ${envelope.kind} record in format ${envelope.format} must decode as that kind`,
  ),
).pipe(
  Schema.decodeTo(TranscriptUnknownRecord, {
    decode: SchemaGetter.transform((envelope) => ({
      kind: "unknown" as const,
      originalKind: envelope.kind,
      format: envelope.format,
      body: envelope.body,
      id: envelope.id,
      updatedSeq: envelope.updatedSeq,
      ...(envelope.position === undefined ? {} : { position: envelope.position }),
    })),
    // Every field was validated on the unknown record itself, so this decode cannot fail.
    encode: SchemaGetter.transform((record) =>
      Schema.decodeUnknownSync(TranscriptRecordEnvelope)({
        format: record.format,
        kind: record.originalKind,
        id: record.id,
        updatedSeq: record.updatedSeq,
        ...(record.position === undefined ? {} : { position: record.position }),
        body: record.body,
      }),
    ),
  }),
);

/**
 * Wire codec for one record: a known record when this reader can interpret it, otherwise an
 * explicit unknown record that keeps its id and position.
 */
export const TranscriptRecordViewFromEnvelope = Schema.Union([
  TranscriptRecordFromEnvelope,
  TranscriptUnknownRecordFromEnvelope,
]);
