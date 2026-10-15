import * as Prompt from "effect/ai/Prompt";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { CommittedDurableEvent } from "../services/session-store";
import { SequenceNumber, SessionId, durablePosition } from "../types/core";
import { effectDurableAgentNamespace, type DurableEventEnvelope } from "../types/events/envelope";
import { decodeDurableEventSync } from "../types/events/durable";
import { makeEDATraceContext } from "../types/tracing";
import { canonicalJson } from "./version";
import {
  advanceTranscriptVersion,
  emptyTranscriptVersion,
  type InMemoryTranscriptVersion,
} from "./in-memory";
import {
  emptyTranscriptMappings,
  makeTranscriptDefinition,
  type TranscriptDefinition,
  type TranscriptMappings,
} from "./mapping";
import { TranscriptRecordFromEnvelope, type TranscriptRecord } from "./records";

/**
 * Test-only builders for transcript interpretation: deterministic ids, a log writer that goes
 * through the real durable decoders, and helpers that project and normalize logs.
 */

const traceContext = makeEDATraceContext({
  traceId: "0123456789abcdef0123456789abcdef",
  spanId: "0123456789abcdef",
});
const eventTrace = { span: traceContext, links: [] };
const runTrace = { root: traceContext, links: [] };

/** Deterministic UUIDv7-shaped id; `space` keeps identities of different kinds apart. */
export const testId = (space: number, slot: number): string =>
  `018f6bd5-2f2a-7${space.toString(16).padStart(3, "0")}-8${(slot >> 48).toString(16).padStart(3, "0")}-${(
    slot %
    2 ** 48
  )
    .toString(16)
    .padStart(12, "0")}`;

export const ids = {
  command: (slot: number) => testId(0x101, slot),
  message: (slot: number) => testId(0x102, slot),
  run: (slot: number) => testId(0x103, slot),
  turn: (slot: number) => testId(0x104, slot),
  inference: (slot: number) => testId(0x105, slot),
  tool: (slot: number) => testId(0x106, slot),
  compaction: (slot: number) => testId(0x107, slot),
  summary: (slot: number) => testId(0x108, slot),
  event: (slot: number) => testId(0x109, slot),
} as const;

export const testSessionId = SessionId.make(testId(0x100, 1));
export const testModel = { provider: "test", modelId: "model-a" } as const;

const summaryPromptMessage = (text: string) =>
  Schema.encodeSync(Prompt.UserMessage)(
    Prompt.makeMessage("user", { content: [Prompt.textPart({ text })] }),
  );

/**
 * Append-only test log. Every event is built as a raw envelope and decoded through the store's
 * durable decoder, so tests exercise the same shapes the interpreter receives in production.
 */
export class TestLog {
  readonly events: Array<CommittedDurableEvent> = [];
  private readonly envelopes: Array<DurableEventEnvelope> = [];
  private nextEvent = 1;

  /** The raw envelopes, for seeding an in-memory store. */
  get durableEnvelopes(): ReadonlyArray<DurableEventEnvelope> {
    return this.envelopes;
  }

  append(type: string, payload: Record<string, unknown>, namespace = effectDurableAgentNamespace) {
    const slot = this.nextEvent++;
    const raw = {
      namespace,
      type,
      schemaVersion: 1,
      durability: "durable",
      eventId: ids.event(slot),
      sessionId: testSessionId,
      createdAtMs: 1_760_000_000_000 + slot * 1_000,
      trace: eventTrace,
      payload,
    };
    const event = decodeDurableEventSync(raw);
    const envelope = event.namespace === "__foreign" ? event.envelope : event;
    this.envelopes.push(envelope);
    this.events.push({
      position: durablePosition(SequenceNumber.make(this.events.length + 1)),
      event,
    });
    return this;
  }

  appendForeign(namespace: string, type: string, payload: Record<string, unknown>) {
    return this.append(type, payload, namespace);
  }

  configure() {
    return this.append("SessionConfigured", { modelSelection: testModel });
  }

  /** Command admission plus the submitted message, as session ingress writes them. */
  submit(input: {
    readonly command: number;
    readonly message: number;
    readonly text: string;
    readonly disposition?: "queue" | "steer";
    readonly idempotencyKey?: string;
  }) {
    const disposition = input.disposition ?? "queue";
    this.append("CommandAdmitted", {
      command: {
        _tag: "SubmitMessage",
        commandId: ids.command(input.command),
        disposition,
        content: input.text,
        ...(input.idempotencyKey === undefined ? {} : { idempotencyKey: input.idempotencyKey }),
      },
    });
    return this.append("UserMessageSubmitted", {
      commandId: ids.command(input.command),
      messageId: ids.message(input.message),
      disposition,
      content: input.text,
    });
  }

  /** A legacy or interrupt-path admission: the message is committed when the command starts. */
  admitOnly(input: {
    readonly command: number;
    readonly text: string;
    readonly idempotencyKey?: string;
  }) {
    return this.append("CommandAdmitted", {
      command: {
        _tag: "SubmitMessage",
        commandId: ids.command(input.command),
        disposition: "interrupt",
        content: input.text,
        ...(input.idempotencyKey === undefined ? {} : { idempotencyKey: input.idempotencyKey }),
      },
    });
  }

  commitUser(input: { readonly command: number; readonly message: number; readonly text: string }) {
    return this.append("UserMessageCommitted", {
      commandId: ids.command(input.command),
      messageId: ids.message(input.message),
      content: input.text,
    });
  }

  startRun(input: { readonly run: number; readonly commands: ReadonlyArray<number> }) {
    for (const command of input.commands) {
      this.append("CommandStarted", { commandId: ids.command(command) });
    }
    return this.runStarted(input);
  }

  runStarted(input: { readonly run: number; readonly commands: ReadonlyArray<number> }) {
    return this.append("RunStarted", {
      runId: ids.run(input.run),
      commandIds: input.commands.map(ids.command),
      modelSelection: testModel,
      trace: runTrace,
    });
  }

  turn(input: {
    readonly run: number;
    readonly turn: number;
    readonly inputs?: ReadonlyArray<number>;
  }) {
    return this.append("TurnStarted", {
      runId: ids.run(input.run),
      turnId: ids.turn(input.turn),
      ...(input.inputs === undefined ? {} : { inputMessageIds: input.inputs.map(ids.message) }),
    });
  }

  inferenceStarted(input: {
    readonly run: number;
    readonly turn: number;
    readonly inference: number;
  }) {
    return this.append("InferenceStarted", {
      runId: ids.run(input.run),
      turnId: ids.turn(input.turn),
      inferenceId: ids.inference(input.inference),
    });
  }

  assistant(input: {
    readonly run: number;
    readonly turn: number;
    readonly inference: number;
    readonly message: number;
    readonly text?: string;
    readonly reasoning?: string;
    readonly toolCalls?: ReadonlyArray<{ readonly providerId: string; readonly name: string }>;
    readonly partial?: string;
  }) {
    const promptParts = [
      ...(input.reasoning === undefined ? [] : [{ type: "reasoning", text: input.reasoning }]),
      ...(input.text === undefined ? [] : [{ type: "text", text: input.text }]),
      ...(input.toolCalls ?? []).map((call) => ({
        type: "tool-call",
        id: call.providerId,
        name: call.name,
        params: { q: call.name },
        providerExecuted: false,
      })),
    ];
    return this.append(
      input.partial === undefined ? "AssistantMessageCommitted" : "AssistantPartialCommitted",
      {
        messageId: ids.message(input.message),
        runId: ids.run(input.run),
        turnId: ids.turn(input.turn),
        inferenceId: ids.inference(input.inference),
        promptParts,
        ...(input.partial === undefined ? {} : { reason: input.partial }),
      },
    );
  }

  importAssistant(input: {
    readonly run: number;
    readonly turn: number;
    readonly inference: number;
    readonly message: number;
    readonly text: string;
  }) {
    return this.append("AssistantMessageImported", {
      messageId: ids.message(input.message),
      runId: ids.run(input.run),
      turnId: ids.turn(input.turn),
      inferenceId: ids.inference(input.inference),
      promptParts: [{ type: "text", text: input.text }],
    });
  }

  inferenceCompleted(input: {
    readonly run: number;
    readonly turn: number;
    readonly inference: number;
    readonly outputTokens?: number;
  }) {
    return this.append("InferenceCompleted", {
      runId: ids.run(input.run),
      turnId: ids.turn(input.turn),
      inferenceId: ids.inference(input.inference),
      finishReason: "stop",
      usage: { inputTokens: 10, outputTokens: input.outputTokens ?? 5 },
    });
  }

  inferenceFailed(input: {
    readonly run: number;
    readonly turn: number;
    readonly inference: number;
    readonly code?: string;
  }) {
    return this.append("InferenceFailed", {
      runId: ids.run(input.run),
      turnId: ids.turn(input.turn),
      inferenceId: ids.inference(input.inference),
      error: {
        message: "inference failed",
        ...(input.code === undefined ? {} : { code: input.code }),
      },
    });
  }

  toolCreated(input: {
    readonly run: number;
    readonly turn: number;
    readonly inference: number;
    readonly tool: number;
    readonly providerId: string;
    readonly name: string;
  }) {
    return this.append("ToolCallCreated", {
      runId: ids.run(input.run),
      turnId: ids.turn(input.turn),
      inferenceId: ids.inference(input.inference),
      toolCallId: ids.tool(input.tool),
      promptPart: {
        type: "tool-call",
        id: input.providerId,
        name: input.name,
        params: { q: input.name },
        providerExecuted: false,
      },
    });
  }

  toolRejected(input: {
    readonly run: number;
    readonly turn: number;
    readonly inference: number;
    readonly tool: number;
    readonly providerId: string;
    readonly name: string;
  }) {
    return this.append("ToolCallRejected", {
      runId: ids.run(input.run),
      turnId: ids.turn(input.turn),
      inferenceId: ids.inference(input.inference),
      toolCallId: ids.tool(input.tool),
      promptPart: {
        type: "tool-result",
        id: input.providerId,
        name: input.name,
        isFailure: true,
        result: { message: "unknown tool", reason: "unknown-tool", modelFeedback: "no such tool" },
      },
    });
  }

  toolStarted(tool: number) {
    return this.append("ToolCallStarted", { toolCallId: ids.tool(tool) });
  }

  toolCompleted(input: {
    readonly tool: number;
    readonly providerId: string;
    readonly name: string;
    readonly result?: unknown;
  }) {
    return this.append("ToolCallCompleted", {
      toolCallId: ids.tool(input.tool),
      promptPart: {
        type: "tool-result",
        id: input.providerId,
        name: input.name,
        isFailure: false,
        result: input.result ?? { ok: true },
      },
    });
  }

  toolFailed(input: { readonly tool: number; readonly providerId: string; readonly name: string }) {
    return this.append("ToolCallFailed", {
      toolCallId: ids.tool(input.tool),
      error: { message: "tool cancelled", code: "tool.cancelled" },
      promptPart: {
        type: "tool-result",
        id: input.providerId,
        name: input.name,
        isFailure: true,
        result: { message: "tool cancelled" },
      },
    });
  }

  turnCompleted(input: { readonly run: number; readonly turn: number }) {
    return this.append("TurnCompleted", {
      runId: ids.run(input.run),
      turnId: ids.turn(input.turn),
    });
  }

  runCompleted(run: number, commands: ReadonlyArray<number> = []) {
    this.append("RunCompleted", { runId: ids.run(run) });
    for (const command of commands)
      this.append("CommandCompleted", { commandId: ids.command(command) });
    return this;
  }

  runInterrupted(run: number) {
    return this.append("RunFailed", {
      runId: ids.run(run),
      error: { message: "run interrupted: restart", code: "run.interrupted" },
    });
  }

  steer(input: {
    readonly command: number;
    readonly message: number;
    readonly run: number;
    readonly text: string;
  }) {
    return this.append("SteeringMessageQueued", {
      commandId: ids.command(input.command),
      messageId: ids.message(input.message),
      runId: ids.run(input.run),
      content: input.text,
    });
  }

  compaction(input: {
    readonly compaction: number;
    readonly summary: number;
    readonly retainedFromContextSeq: number;
    readonly contextVersion: number;
  }) {
    const seq = this.events.length;
    this.append("CompactionRequested", {
      compactionId: ids.compaction(input.compaction),
      sourceFromSeq: 1,
      sourceToSeq: seq,
    });
    this.append("CompactionStarted", { compactionId: ids.compaction(input.compaction) });
    this.append("SummaryCreated", {
      compactionId: ids.compaction(input.compaction),
      summaryId: ids.summary(input.summary),
      sourceFromSeq: 1,
      sourceToSeq: seq,
      summary: {
        compactionId: ids.compaction(input.compaction),
        summaryId: ids.summary(input.summary),
        sourceFromSeq: 1,
        sourceToSeq: seq,
        retainedFromContextSeq: input.retainedFromContextSeq,
        text: "summary",
        promptMessage: summaryPromptMessage("summary"),
        policyId: "test",
      },
    });
    this.append("ContextRebased", {
      compactionId: ids.compaction(input.compaction),
      summaryId: ids.summary(input.summary),
      contextVersion: input.contextVersion,
      retainedFromContextSeq: input.retainedFromContextSeq,
    });
    return this.append("CompactionCompleted", {
      compactionId: ids.compaction(input.compaction),
      modelSelection: testModel,
      usage: { inputTokens: 100, outputTokens: 20 },
    });
  }

  /** One complete assistant turn: start, content, optional tools run to completion, finish. */
  assistantTurn(input: {
    readonly run: number;
    readonly turn: number;
    readonly inference: number;
    readonly message: number;
    readonly text: string;
    readonly inputs?: ReadonlyArray<number>;
    readonly tools?: ReadonlyArray<number>;
  }) {
    this.turn({
      run: input.run,
      turn: input.turn,
      ...(input.inputs === undefined ? {} : { inputs: input.inputs }),
    });
    this.inferenceStarted(input);
    const toolCalls = (input.tools ?? []).map((tool) => ({
      tool,
      providerId: `call_${tool}`,
      name: `tool_${tool}`,
    }));
    this.assistant({ ...input, toolCalls });
    for (const call of toolCalls) this.toolCreated({ ...input, ...call });
    this.inferenceCompleted(input);
    for (const call of toolCalls) {
      this.toolStarted(call.tool);
      this.toolCompleted(call);
    }
    return this.turnCompleted(input);
  }
}

/** A definition, built synchronously for tests. */
export const testDefinition = (
  mappings: TranscriptMappings = emptyTranscriptMappings,
): TranscriptDefinition => Effect.runSync(makeTranscriptDefinition(mappings));

/** Project a log split into the given batches, starting from an empty version. */
export const projectBatches = (
  definition: TranscriptDefinition,
  batches: ReadonlyArray<ReadonlyArray<CommittedDurableEvent>>,
): InMemoryTranscriptVersion =>
  batches.reduce(
    (version, batch) => advanceTranscriptVersion(version, definition, batch),
    emptyTranscriptVersion(definition.version, "active"),
  );

/**
 * A semantic view of a projected version for equality: every record encoded through its stored
 * envelope with `updatedSeq` normalized, every alias, and the cursor and status.
 */
export const normalizedProjection = (version: InMemoryTranscriptVersion) => ({
  meta: version.meta,
  records: Array.from(version.records.values())
    .map((record) => ({
      ...Schema.encodeSync(TranscriptRecordFromEnvelope)(record),
      updatedSeq: 0,
    }))
    .sort((left, right) => (left.id < right.id ? -1 : left.id > right.id ? 1 : 0)),
  aliases: Array.from(version.aliases.entries()).sort(([left], [right]) =>
    left < right ? -1 : left > right ? 1 : 0,
  ),
});

/** Canonical JSON of every record, for fingerprints. */
export const projectionJson = (records: Iterable<TranscriptRecord>): string =>
  canonicalJson(
    Array.from(records)
      .map((record) => Schema.encodeSync(TranscriptRecordFromEnvelope)(record))
      .sort((left, right) => (left.id < right.id ? -1 : left.id > right.id ? 1 : 0)),
  );
