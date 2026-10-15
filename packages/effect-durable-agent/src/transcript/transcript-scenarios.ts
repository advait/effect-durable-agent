import type { CommittedDurableEvent } from "../services/session-store";
import type { TranscriptMappings } from "./mapping";
import { TestLog, ids } from "./transcript-testkit";

/**
 * The fixed scenario corpus: one named log per interpretation rule the transcript must get
 * right. The oracle, the fingerprint and the prompt-parity check all run over it. Add a scenario
 * for every new rule; never edit an existing one without bumping the semantic revision.
 */
export interface TranscriptScenario {
  readonly name: string;
  readonly events: ReadonlyArray<CommittedDurableEvent>;
  /** Whether the log contains a context rebase (the reduced state is pruned). */
  readonly compacted: boolean;
}

/** Gia-shaped decorations used by the decoration scenarios. */
export const giaLikeTestMappings: TranscriptMappings = {
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
      name: "owner",
      event: { namespace: "gia-agent", type: "GiaSessionMetadataCommitted", schemaVersion: 1 },
      target: { _tag: "Session" },
      operations: [{ _tag: "FirstWins", from: ["ownerUserEmail"], to: "owner" }],
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
      name: "references",
      event: { namespace: "gia-agent", type: "GiaReferenceCreated", schemaVersion: 1 },
      target: { _tag: "UserByIdempotencyKey", path: ["relatedCommandIdempotencyKey"] },
      operations: [
        {
          _tag: "ReplaceListEntry",
          from: ["target"],
          to: "references",
          key: { _tag: "Ordinal", path: ["ordinal"] },
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
  userFallbacks: [
    { to: "author", fromSession: "lastAuthor" },
    { to: "author", fromSession: "owner" },
  ],
};

const author = (key: string, email: string) => ({
  relatedCommandIdempotencyKey: key,
  userEmail: email,
});

const queueRunTools = () => {
  const log = new TestLog().configure();
  log.submit({ command: 1, message: 1, text: "hi", idempotencyKey: "web:1" });
  log.startRun({ run: 1, commands: [1] });
  log.assistantTurn({
    run: 1,
    turn: 1,
    inference: 1,
    message: 101,
    text: "calling",
    inputs: [1],
    tools: [1, 2],
  });
  log.assistantTurn({ run: 1, turn: 2, inference: 2, message: 102, text: "done" });
  return log.runCompleted(1, [1]);
};

const legacyCommitted = () => {
  const log = new TestLog().configure();
  log.admitOnly({ command: 1, text: "interrupting" });
  log.append("CommandStarted", { commandId: ids.command(1) });
  log.commitUser({ command: 1, message: 1, text: "interrupting" });
  log.runStarted({ run: 1, commands: [1] });
  log.assistantTurn({ run: 1, turn: 1, inference: 1, message: 101, text: "ok", inputs: [1] });
  // A committed user without any admission, as very old logs have.
  log.commitUser({ command: 2, message: 2, text: "old" });
  return log.runCompleted(1, [1]);
};

const steering = () => {
  const log = new TestLog().configure();
  log.submit({ command: 1, message: 1, text: "start" });
  log.startRun({ run: 1, commands: [1] });
  log.turn({ run: 1, turn: 1, inputs: [1] });
  log.inferenceStarted({ run: 1, turn: 1, inference: 1 });
  log.submit({ command: 2, message: 2, text: "steer me", disposition: "steer" });
  log.steer({ command: 3, message: 3, run: 1, text: "also this" });
  log.steer({ command: 4, message: 4, run: 1, text: "never mind" });
  log.append("SteeringMessageCancelled", {
    messageId: ids.message(4),
    runId: ids.run(1),
    reason: "run ended",
  });
  log.assistant({ run: 1, turn: 1, inference: 1, message: 101, text: "first" });
  log.inferenceCompleted({ run: 1, turn: 1, inference: 1 });
  log.turnCompleted({ run: 1, turn: 1 });
  log.assistantTurn({
    run: 1,
    turn: 2,
    inference: 2,
    message: 102,
    text: "steered",
    inputs: [2, 3],
  });
  return log.runCompleted(1, [1]);
};

const promotePauseCancel = () => {
  const log = new TestLog().configure();
  log.submit({ command: 1, message: 1, text: "first" });
  log.startRun({ run: 1, commands: [1] });
  log.turn({ run: 1, turn: 1, inputs: [1] });
  log.submit({ command: 2, message: 2, text: "queued a" });
  log.submit({ command: 3, message: 3, text: "queued b" });
  log.submit({ command: 4, message: 4, text: "queued c" });
  log.submit({ command: 5, message: 5, text: "queued d" });
  log.append("UserMessagePromoted", {
    commandId: ids.command(6),
    messageId: ids.message(2),
    from: "queue",
    to: "steer",
  });
  log.append("PendingMessagesPaused", {
    interruptionCommandId: ids.command(7),
    runId: ids.run(1),
    messageIds: [ids.message(3), ids.message(4)],
    reason: "user-interrupted",
  });
  log.append("UserMessageCancelled", {
    commandId: ids.command(8),
    messageId: ids.message(4),
    reason: "user-cancel",
  });
  log.append("UserMessageCancelled", {
    commandId: ids.command(9),
    messageId: ids.message(5),
    reason: "edit",
  });
  log.append("CommandCancelled", { commandId: ids.command(5), reason: "edited" });
  log.admitOnly({ command: 10, text: "admitted then cancelled" });
  log.append("CommandCancelled", { commandId: ids.command(10), reason: "stopped" });
  log.inferenceStarted({ run: 1, turn: 1, inference: 1 });
  log.assistant({ run: 1, turn: 1, inference: 1, message: 101, text: "answer" });
  log.inferenceCompleted({ run: 1, turn: 1, inference: 1 });
  log.turnCompleted({ run: 1, turn: 1 });
  log.assistantTurn({ run: 1, turn: 2, inference: 2, message: 102, text: "promoted", inputs: [2] });
  return log.runCompleted(1, [1]);
};

const partialEmptyImport = () => {
  const log = new TestLog().configure();
  log.importAssistant({ run: 9, turn: 9, inference: 9, message: 99, text: "imported context" });
  log.submit({ command: 1, message: 1, text: "go" });
  log.startRun({ run: 1, commands: [1] });
  log.turn({ run: 1, turn: 1, inputs: [1] });
  log.inferenceStarted({ run: 1, turn: 1, inference: 1 });
  log.assistant({
    run: 1,
    turn: 1,
    inference: 1,
    message: 101,
    text: "half",
    reasoning: "hmm",
    partial: "inference failed before completion",
  });
  log.inferenceFailed({ run: 1, turn: 1, inference: 1 });
  log.inferenceStarted({ run: 1, turn: 1, inference: 2 });
  log.inferenceCompleted({ run: 1, turn: 1, inference: 2, outputTokens: 0 });
  log.turnCompleted({ run: 1, turn: 1 });
  return log.runCompleted(1, [1]);
};

const rejectedAndRepeatedTerminals = () => {
  const log = new TestLog().configure();
  log.submit({ command: 1, message: 1, text: "tools" });
  log.startRun({ run: 1, commands: [1] });
  log.turn({ run: 1, turn: 1, inputs: [1] });
  log.inferenceStarted({ run: 1, turn: 1, inference: 1 });
  // The tool is sealed before the assistant content, the reverse of the usual order.
  log.toolCreated({ run: 1, turn: 1, inference: 1, tool: 1, providerId: "call_1", name: "search" });
  log.toolRejected({ run: 1, turn: 1, inference: 1, tool: 2, providerId: "call_2", name: "nope" });
  log.assistant({
    run: 1,
    turn: 1,
    inference: 1,
    message: 101,
    text: "using tools",
    toolCalls: [
      { providerId: "call_1", name: "search" },
      { providerId: "call_2", name: "nope" },
    ],
  });
  log.inferenceCompleted({ run: 1, turn: 1, inference: 1 });
  log.toolStarted(1);
  log.toolCompleted({ tool: 1, providerId: "call_1", name: "search", result: { hits: 3 } });
  log.toolFailed({ tool: 1, providerId: "call_1", name: "search" });
  log.turnCompleted({ run: 1, turn: 1 });
  return log.runCompleted(1, [1]);
};

const recovery = () => {
  const log = new TestLog().configure();
  log.submit({ command: 1, message: 1, text: "long task" });
  log.startRun({ run: 1, commands: [1] });
  log.turn({ run: 1, turn: 1, inputs: [1] });
  log.inferenceStarted({ run: 1, turn: 1, inference: 1 });
  log.assistant({
    run: 1,
    turn: 1,
    inference: 1,
    message: 101,
    text: "working",
    toolCalls: [{ providerId: "call_1", name: "bash" }],
  });
  log.toolCreated({ run: 1, turn: 1, inference: 1, tool: 1, providerId: "call_1", name: "bash" });
  log.inferenceCompleted({ run: 1, turn: 1, inference: 1 });
  log.toolStarted(1);
  // Startup recovery: one batch that fails everything open and continues the run.
  log.toolFailed({ tool: 1, providerId: "call_1", name: "bash" });
  log.append("TurnFailed", {
    runId: ids.run(1),
    turnId: ids.turn(1),
    error: { message: "turn interrupted", code: "turn.interrupted" },
  });
  log.runInterrupted(1);
  log.runStarted({ run: 2, commands: [1] });
  log.append("RecoveryCompleted", {
    trigger: "runtime-restart",
    continuation: {
      commandId: ids.command(1),
      interruptedRunId: ids.run(1),
      replacementRunId: ids.run(2),
    },
  });
  log.assistantTurn({ run: 2, turn: 2, inference: 2, message: 102, text: "resumed" });
  return log.runCompleted(2, [1]);
};

const midRunCompaction = () => {
  const log = new TestLog().configure();
  log.submit({ command: 1, message: 1, text: "first" });
  log.startRun({ run: 1, commands: [1] });
  log.assistantTurn({
    run: 1,
    turn: 1,
    inference: 1,
    message: 101,
    text: "one",
    inputs: [1],
    tools: [1],
  });
  log.runCompleted(1, [1]);
  log.submit({ command: 2, message: 2, text: "second" });
  log.startRun({ run: 2, commands: [2] });
  log.assistantTurn({ run: 2, turn: 2, inference: 2, message: 102, text: "two", inputs: [2] });
  log.compaction({
    compaction: 1,
    summary: 1,
    retainedFromContextSeq: log.events.length,
    contextVersion: 1,
  });
  log.assistantTurn({ run: 2, turn: 3, inference: 3, message: 103, text: "three" });
  return log.runCompleted(2, [2]);
};

const replacementMessageId = () => {
  // A pending submission pruned by a rebase is re-committed under a new message id when its
  // command starts (session 01a13b26 in the design): one record, two aliases.
  const log = new TestLog().configure();
  log.submit({ command: 1, message: 1, text: "before" });
  log.startRun({ run: 1, commands: [1] });
  log.assistantTurn({ run: 1, turn: 1, inference: 1, message: 101, text: "busy", inputs: [1] });
  log.submit({ command: 2, message: 2, text: "queued during run" });
  log.compaction({
    compaction: 1,
    summary: 1,
    retainedFromContextSeq: log.events.length + 1,
    contextVersion: 1,
  });
  log.runCompleted(1, [1]);
  log.append("CommandStarted", { commandId: ids.command(2) });
  log.commitUser({ command: 2, message: 22, text: "queued during run" });
  log.runStarted({ run: 2, commands: [2] });
  log.assistantTurn({
    run: 2,
    turn: 2,
    inference: 2,
    message: 102,
    text: "answered",
    inputs: [22],
  });
  return log.runCompleted(2, [2]);
};

const decorations = () => {
  const log = new TestLog().configure();
  log.appendForeign("gia-agent", "GiaSessionMetadataCommitted", {
    ownerUserEmail: "owner@example.com",
  });
  // Web follow-up: author, attachments and a reference precede the command in one batch.
  log.appendForeign("gia-agent", "GiaUserMessageAuthorCommitted", author("web:1", "a@example.com"));
  log.appendForeign("gia-agent", "GiaFileAttachmentCreated", {
    attachmentId: "att-1",
    relatedCommandIdempotencyKey: "web:1",
    ordinal: 0,
    fileName: "a.txt",
  });
  log.appendForeign("gia-agent", "GiaReferenceCreated", {
    ordinal: 1,
    referenceId: "ref-2",
    relatedCommandIdempotencyKey: "web:1",
    target: { kind: "doc", id: "d2" },
  });
  log.appendForeign("gia-agent", "GiaReferenceCreated", {
    ordinal: 0,
    referenceId: "ref-1",
    relatedCommandIdempotencyKey: "web:1",
    target: { kind: "doc", id: "d1" },
  });
  log.submit({ command: 1, message: 1, text: "with files", idempotencyKey: "web:1" });
  // A decoration after its command, a replaced attachment, and a second author (first wins).
  log.appendForeign("gia-agent", "GiaFileAttachmentCreated", {
    attachmentId: "att-1",
    relatedCommandIdempotencyKey: "web:1",
    ordinal: 0,
    fileName: "a-renamed.txt",
  });
  log.appendForeign("gia-agent", "GiaUserMessageAuthorCommitted", author("web:1", "b@example.com"));
  // Subagent wakeup: the notice precedes a command whose id is the notification id.
  log.appendForeign("gia-agent", "GiaSubagentNotificationCommitted", {
    notificationId: ids.command(2),
    children: [{ name: "child", sessionId: "s", state: "completed" }],
  });
  log.submit({
    command: 2,
    message: 2,
    text: "children done",
    idempotencyKey: `subagent:notify:${ids.command(2)}`,
  });
  // No author event for this key: the latest author is the fallback.
  log.submit({ command: 3, message: 3, text: "no author", idempotencyKey: "slack:3" });
  // Invalid decorations degrade only themselves; unmapped namespaces are ignored.
  log.appendForeign("gia-agent", "GiaUserMessageAuthorCommitted", {
    relatedCommandIdempotencyKey: "slack:3",
  });
  log.appendForeign("gia-agent", "GiaFileAttachmentCreated", {
    relatedCommandIdempotencyKey: "slack:3",
    fileName: "no id",
  });
  log.appendForeign("gia-agent", "GiaUserMessageAuthorCommitted", {
    relatedCommandIdempotencyKey: 7,
    userEmail: "x",
  });
  log.appendForeign("other-app", "Whatever", { relatedCommandIdempotencyKey: "web:1" });
  // An orphan decoration whose command never arrives.
  log.appendForeign("gia-agent", "GiaUserMessageAuthorCommitted", author("never", "c@example.com"));
  log.startRun({ run: 1, commands: [1, 2, 3] });
  log.assistantTurn({ run: 1, turn: 1, inference: 1, message: 101, text: "ok", inputs: [1, 2, 3] });
  return log.runCompleted(1, [1, 2, 3]);
};

const ignoredEvents = () => {
  const log = new TestLog().configure();
  log.append("SystemMessageCommitted", { messageId: ids.message(90), content: "system prompt" });
  log.append("RunSchedulingRequested", {
    requestId: ids.event(900),
    work: { _tag: "Command", commandId: ids.command(1) },
  });
  log.append("RunSchedulingDelivered", { requestId: ids.event(900) });
  log.append("RunSchedulingGranted", { requestId: ids.event(900), runId: ids.run(1) });
  log.append("RunSchedulingInvalidated", { requestId: ids.event(901), reason: "stopped" });
  log.submit({ command: 1, message: 1, text: "hello" });
  log.startRun({ run: 1, commands: [1] });
  log.turn({ run: 1, turn: 1, inputs: [1] });
  log.append("StopTurnRequested", {
    commandId: ids.command(2),
    runId: ids.run(1),
    turnId: ids.turn(1),
  });
  log.append("StopTurnApplied", {
    commandId: ids.command(2),
    runId: ids.run(1),
    turnId: ids.turn(1),
  });
  log.append("TurnStopped", { runId: ids.run(1), turnId: ids.turn(1), reason: "stop" });
  log.append("ContextProjected", { contextVersion: 0, throughSeq: log.events.length });
  log.append("BaseStateRequested", { baseStateId: ids.event(902), requestedThroughSeq: 1 });
  log.append("BaseStateCreated", { baseStateId: ids.event(902), coversThroughSeq: 1 });
  log.append("BaseStateFailed", { baseStateId: ids.event(903), error: { message: "x" } });
  log.append("CompactionRequested", {
    compactionId: ids.compaction(9),
    sourceFromSeq: 1,
    sourceToSeq: 2,
  });
  log.append("CompactionStarted", { compactionId: ids.compaction(9) });
  log.append("CompactionFailed", { compactionId: ids.compaction(9), error: { message: "no" } });
  log.append("RunInterrupted", { runId: ids.run(1), reason: "legacy stop" });
  log.append("CommandFailed", { commandId: ids.command(1), error: { message: "x" } });
  return log;
};

const legacyAttempts = () => {
  const log = new TestLog().configure();
  log.submit({ command: 1, message: 1, text: "legacy" });
  log.startRun({ run: 1, commands: [1] });
  log.turn({ run: 1, turn: 1, inputs: [1] });
  log.append("TurnAttemptStarted", {
    runId: ids.run(1),
    turnId: ids.turn(1),
    attemptId: ids.inference(1),
  });
  log.append("AssistantMessageCommitted", {
    messageId: ids.message(101),
    runId: ids.run(1),
    turnId: ids.turn(1),
    attemptId: ids.inference(1),
    promptParts: [
      { type: "text", text: "legacy answer" },
      { type: "tool-call", id: "call_1", name: "search", params: {}, providerExecuted: false },
    ],
  });
  log.append("ToolCallCreated", {
    runId: ids.run(1),
    turnId: ids.turn(1),
    attemptId: ids.inference(1),
    toolCallId: ids.tool(1),
    promptPart: {
      type: "tool-call",
      id: "call_1",
      name: "search",
      params: {},
      providerExecuted: false,
    },
  });
  log.append("TurnAttemptCompleted", {
    runId: ids.run(1),
    turnId: ids.turn(1),
    attemptId: ids.inference(1),
    usage: { inputTokens: 3, outputTokens: 4 },
  });
  log.toolStarted(1);
  log.toolCompleted({ tool: 1, providerId: "call_1", name: "search" });
  log.turnCompleted({ run: 1, turn: 1 });
  return log.runCompleted(1, [1]);
};

const failuresAndRepeatedCompletion = () => {
  const log = new TestLog().configure();
  // An interrupt submission whose start fails never becomes a message: it is withdrawn.
  log.admitOnly({ command: 1, text: "stop and do this" });
  log.append("CommandStarted", { commandId: ids.command(1) });
  log.append("CommandFailed", { commandId: ids.command(1), error: { message: "start failed" } });
  log.submit({ command: 2, message: 2, text: "next" });
  log.startRun({ run: 1, commands: [2] });
  log.assistantTurn({ run: 1, turn: 1, inference: 1, message: 101, text: "ok", inputs: [2] });
  log.compaction({
    compaction: 1,
    summary: 1,
    retainedFromContextSeq: log.events.length,
    contextVersion: 1,
  });
  // A repeated completion adds no usage, as in the reduced state.
  log.append("CompactionCompleted", {
    compactionId: ids.compaction(1),
    modelSelection: { provider: "test", modelId: "model-a" },
    usage: { inputTokens: 100, outputTokens: 20 },
  });
  return log.runCompleted(1, [2]);
};

const scenario = (name: string, compacted: boolean, build: () => TestLog): TranscriptScenario => ({
  name,
  compacted,
  events: build().events,
});

/** Build the corpus. Logs are deterministic: ids, timestamps and traces are fixed. */
export const transcriptScenarios = (): ReadonlyArray<TranscriptScenario> => [
  scenario("queue-run-tools", false, queueRunTools),
  scenario("legacy-committed-user", false, legacyCommitted),
  scenario("steering", false, steering),
  scenario("promote-pause-cancel", false, promotePauseCancel),
  scenario("partial-failure-empty-success-import", false, partialEmptyImport),
  scenario("rejected-tool-repeated-terminals", false, rejectedAndRepeatedTerminals),
  scenario("recovery-continuation", false, recovery),
  scenario("mid-run-compaction", true, midRunCompaction),
  scenario("replacement-message-id", true, replacementMessageId),
  scenario("host-decorations", false, decorations),
  scenario("events-without-transcript-meaning", false, ignoredEvents),
  scenario("legacy-attempts", false, legacyAttempts),
  scenario("failed-interrupt-repeated-compaction-completion", true, failuresAndRepeatedCompletion),
];
