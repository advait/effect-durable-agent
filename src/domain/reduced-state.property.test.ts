import * as fc from "fast-check";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { deriveCommandQueues } from "./command-queues";
import {
  decodeReducedStateCheckpoint,
  encodeReducedStateCheckpoint,
  foldReducedState,
  initialReducedState,
  reduceCommittedEvents,
  reducedStateCheckpointEventSeqs,
} from "./reduced-state";
import { CommittedDurableEvent } from "../services/session-store";
import { StopTurnCommand, SubmitMessageCommand } from "../types/commands";
import {
  InferenceId,
  CommandId,
  EventId,
  MessageId,
  RunId,
  SequenceNumber,
  SessionId,
  ToolCallId,
  TurnId,
  durablePosition,
} from "../types/core";
import type { EDADurableEvent } from "../types/events";
import {
  EventType,
  ProviderPartId,
  ToolName,
  UnixEpochMillis,
  effectDurableAgentNamespace,
  schemaV1,
} from "../types/events";

const propertyRuns = 100;

type CommandKind = "queue" | "steer" | "interrupt" | "stop";
type EventKind =
  | "command-admitted"
  | "command-started"
  | "command-completed"
  | "command-cancelled"
  | "run-started"
  | "run-completed"
  | "run-interrupted"
  | "turn-started"
  | "turn-completed"
  | "turn-stopped"
  | "inference-started"
  | "inference-completed"
  | "inference-failed"
  | "tool-created"
  | "tool-started"
  | "tool-completed"
  | "user-message"
  | "steering-message"
  | "steering-cancelled"
  | "assistant-message"
  | "assistant-partial"
  | "stop-requested"
  | "stop-applied";

type EventSpec = {
  readonly kind: EventKind;
  readonly commandKind: CommandKind;
  readonly commandSlot: number;
  readonly runSlot: number;
  readonly turnSlot: number;
  readonly inferenceSlot: number;
  readonly toolSlot: number;
  readonly messageSlot: number;
  readonly includeInputMessage: boolean;
};

const seq = (value: number) => SequenceNumber.make(value);

const uuid = (slot: number): string => {
  const fourth = `8${(slot % 0x1000).toString(16).padStart(3, "0")}`;
  const fifth = slot.toString(16).padStart(12, "0").slice(-12);
  return `018f6bd5-2f2a-7b1e-${fourth}-${fifth}`;
};

const sessionId = SessionId.make(uuid(0xf00));
const commandId = (slot: number) => CommandId.make(uuid(0x100 + slot));
const runId = (slot: number) => RunId.make(uuid(0x200 + slot));
const turnId = (slot: number) => TurnId.make(uuid(0x300 + slot));
const inferenceId = (slot: number) => InferenceId.make(uuid(0x400 + slot));
const toolCallId = (slot: number) => ToolCallId.make(uuid(0x500 + slot));
const messageId = (slot: number) => MessageId.make(uuid(0x600 + slot));
const eventId = (slot: number) => EventId.make(uuid(0x700 + slot));
const content = (text: string) => [Prompt.textPart({ text })];

const commandFor = (id: CommandId, kind: CommandKind) =>
  kind === "stop"
    ? new StopTurnCommand({ commandId: id })
    : new SubmitMessageCommand({ commandId: id, disposition: kind, content: content(kind) });

const eventSpecArbitrary = fc.record({
  kind: fc.constantFrom<EventKind>(
    "command-admitted",
    "command-started",
    "command-completed",
    "command-cancelled",
    "run-started",
    "run-completed",
    "run-interrupted",
    "turn-started",
    "turn-completed",
    "turn-stopped",
    "inference-started",
    "inference-completed",
    "inference-failed",
    "tool-created",
    "tool-started",
    "tool-completed",
    "user-message",
    "steering-message",
    "steering-cancelled",
    "assistant-message",
    "assistant-partial",
    "stop-requested",
    "stop-applied",
  ),
  commandKind: fc.constantFrom<CommandKind>("queue", "steer", "interrupt", "stop"),
  commandSlot: fc.integer({ min: 0, max: 4 }),
  runSlot: fc.integer({ min: 0, max: 4 }),
  turnSlot: fc.integer({ min: 0, max: 4 }),
  inferenceSlot: fc.integer({ min: 0, max: 4 }),
  toolSlot: fc.integer({ min: 0, max: 4 }),
  messageSlot: fc.integer({ min: 0, max: 4 }),
  includeInputMessage: fc.boolean(),
});

const committed = (position: number, type: string, payload: unknown) => {
  const event: EDADurableEvent = {
    namespace: effectDurableAgentNamespace,
    type: EventType.make(type),
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: eventId(position),
    sessionId,
    createdAtMs: UnixEpochMillis.make(1_715_000_000_000 + position),
    payload,
  } as EDADurableEvent;

  return CommittedDurableEvent.make({ position: durablePosition(seq(position)), event });
};

const committedFromSpec = (spec: EventSpec, position: number): CommittedDurableEvent => {
  const command = commandId(spec.commandSlot);
  const run = runId(spec.runSlot);
  const turn = turnId(spec.turnSlot);
  const inference = inferenceId(spec.inferenceSlot);
  const tool = toolCallId(spec.toolSlot);
  const message = messageId(spec.messageSlot);

  switch (spec.kind) {
    case "command-admitted":
      return committed(position, "CommandAdmitted", {
        command: commandFor(command, spec.commandKind),
      });
    case "command-started":
      return committed(position, "CommandStarted", { commandId: command });
    case "command-completed":
      return committed(position, "CommandCompleted", { commandId: command });
    case "command-cancelled":
      return committed(position, "CommandCancelled", { commandId: command, reason: "cancelled" });
    case "run-started":
      return committed(position, "RunStarted", {
        runId: run,
        commandIds: [command],
        modelSelection: { provider: "test", modelId: "test-model" },
      });
    case "run-completed":
      return committed(position, "RunCompleted", { runId: run });
    case "run-interrupted":
      return committed(position, "RunInterrupted", { runId: run, reason: "interrupted" });
    case "turn-started":
      return committed(position, "TurnStarted", {
        runId: run,
        turnId: turn,
        ...(spec.includeInputMessage ? { inputMessageIds: [message] } : {}),
      });
    case "turn-completed":
      return committed(position, "TurnCompleted", { runId: run, turnId: turn });
    case "turn-stopped":
      return committed(position, "TurnStopped", { runId: run, turnId: turn, reason: "stopped" });
    case "inference-started":
      return committed(position, "InferenceStarted", {
        runId: run,
        turnId: turn,
        inferenceId: inference,
      });
    case "inference-completed":
      return committed(position, "InferenceCompleted", {
        runId: run,
        turnId: turn,
        inferenceId: inference,
      });
    case "inference-failed":
      return committed(position, "InferenceFailed", {
        runId: run,
        turnId: turn,
        inferenceId: inference,
        error: { message: "failed" },
      });
    case "tool-created":
      return committed(position, "ToolCallCreated", {
        runId: run,
        turnId: turn,
        inferenceId: inference,
        toolCallId: tool,
        promptPart: Prompt.toolCallPart({
          id: ProviderPartId.make(`part-${spec.toolSlot}`),
          name: ToolName.make("noop"),
          params: { ok: true },
          providerExecuted: false,
        }),
      });
    case "tool-started":
      return committed(position, "ToolCallStarted", { toolCallId: tool });
    case "tool-completed":
      return committed(position, "ToolCallCompleted", {
        toolCallId: tool,
        promptPart: Prompt.toolResultPart({
          id: ProviderPartId.make(`part-${spec.toolSlot}`),
          name: ToolName.make("noop"),
          isFailure: false,
          result: { ok: true },
        }),
      });
    case "user-message":
      return committed(position, "UserMessageCommitted", {
        commandId: command,
        messageId: message,
        content: content("user"),
      });
    case "steering-message":
      return committed(position, "SteeringMessageQueued", {
        commandId: command,
        messageId: message,
        runId: run,
        content: content("steer"),
      });
    case "steering-cancelled":
      return committed(position, "SteeringMessageCancelled", {
        messageId: message,
        runId: run,
        reason: "cancelled",
      });
    case "assistant-message":
      return committed(position, "AssistantMessageCommitted", {
        messageId: message,
        runId: run,
        turnId: turn,
        inferenceId: inference,
        promptParts: [Prompt.textPart({ text: "assistant" })],
      });
    case "assistant-partial":
      return committed(position, "AssistantPartialCommitted", {
        messageId: message,
        runId: run,
        turnId: turn,
        inferenceId: inference,
        promptParts: [Prompt.textPart({ text: "partial" })],
        reason: "interrupted",
      });
    case "stop-requested":
      return committed(position, "StopTurnRequested", {
        commandId: command,
        runId: run,
        turnId: turn,
      });
    case "stop-applied":
      return committed(position, "StopTurnApplied", {
        commandId: command,
        runId: run,
        turnId: turn,
        inferenceId: inference,
      });
  }
};

describe("reduced-state fold properties", () => {
  it("matches full replay with incremental replay and rematerializes command queues", () => {
    fc.assert(
      fc.property(
        fc.array(eventSpecArbitrary, { maxLength: 40 }),
        fc.integer({ min: 0, max: 40 }),
        (specs, splitSeed) => {
          const events = specs.map((spec, index) => committedFromSpec(spec, index + 1));
          const split = events.length === 0 ? 0 : splitSeed % (events.length + 1);

          const full = reduceCommittedEvents(events);
          const incremental = foldReducedState(
            foldReducedState(initialReducedState, events.slice(0, split)),
            events.slice(split),
          );

          expect(incremental).toEqual(full);
          expect(full.commandQueues).toEqual(
            deriveCommandQueues({
              commands: full.commands,
              runs: full.runs,
              messages: full.messages,
            }),
          );
        },
      ),
      { numRuns: propertyRuns },
    );
  });

  it("matches full replay after pointer-checkpoint hydration plus tail replay", () => {
    fc.assert(
      fc.property(
        fc.array(eventSpecArbitrary, { maxLength: 40 }),
        fc.integer({ min: 0, max: 40 }),
        (specs, splitSeed) => {
          const events = specs.map((spec, index) => committedFromSpec(spec, index + 1));
          const split = events.length === 0 ? 0 : splitSeed % (events.length + 1);
          const prefix = events.slice(0, split);
          const payload = encodeReducedStateCheckpoint(reduceCommittedEvents(prefix));
          const requiredSeqs = new Set(
            reducedStateCheckpointEventSeqs(payload).map((seq) => Number(seq)),
          );
          const referencedEvents = prefix.filter((entry) =>
            requiredSeqs.has(Number(entry.position.seq)),
          );

          const hydrated = foldReducedState(
            decodeReducedStateCheckpoint(payload, referencedEvents),
            events.slice(split),
          );

          expect(hydrated).toEqual(reduceCommittedEvents(events));
        },
      ),
      { numRuns: propertyRuns },
    );
  });
});
