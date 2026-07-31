import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import {
  InferenceId,
  EventId,
  RunId,
  SequenceNumber,
  SessionId,
  ToolCallId,
  TurnId,
  durablePosition,
} from "../../src/types/core";
import {
  DurableEventEnvelope,
  EventType,
  ToolCallCompletedPayload,
  ToolCallCreatedPayload,
  UnixEpochMillis,
  effectDurableAgentNamespace,
  schemaV1,
  toolCallCompletedEventType,
  toolCallCreatedEventType,
} from "../../src/types/events";
import { sequentialUuidV7 } from "../../src/services/id-generator";
import { CommittedDurableEvent } from "../../src/services/session-store";
import {
  ApprovalAction,
  ApproverId,
  PreviewUrl,
  SandboxCommandId,
  SandboxEvents,
  SandboxId,
} from "./events";
import { initialSandboxLifecycleState, reduceSandboxLifecycleState } from "./reducer";

const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const RUN_ID = RunId.make(sequentialUuidV7(31));
const TURN_ID = TurnId.make(sequentialUuidV7(32));
const INFERENCE_ID = InferenceId.make(sequentialUuidV7(33));
const TOOL_CALL_ID = ToolCallId.make(sequentialUuidV7(34));
const SANDBOX_ID = SandboxId.make("sbx_123");
const COMMAND_ID = SandboxCommandId.make("cmd_test_1");
const ACTION = ApprovalAction.make("deploy");

describe("sandbox lifecycle example", () => {
  it("derives one UI model from framework tool events and app lifecycle events", () => {
    const state = sandboxScenarioEvents().reduce(
      reduceSandboxLifecycleState,
      initialSandboxLifecycleState,
    );
    const sandbox = state.sandboxes.get(SANDBOX_ID);
    const tool = state.toolCards.get(TOOL_CALL_ID);

    expect(tool).toMatchObject({ status: "completed", toolName: "runSandboxTask" });
    expect(state.activeSandboxId).toBe(SANDBOX_ID);
    expect(sandbox?.status).toBe("stopped");
    expect(sandbox?.lastCommand).toMatchObject({
      command: "pnpm test",
      wallClockTimeMs: 1412,
    });
    expect(sandbox?.previewUrl).toBe("https://preview.example.dev");
    expect(sandbox?.approval).toEqual({
      _tag: "Granted",
      action: "deploy",
      approverId: "user_456",
    });
    expect(sandbox?.stopReason).toBe("idle");
  });

  it("supports SSR handoff by replaying only events after the server cursor", () => {
    const events = sandboxScenarioEvents();
    const serverCursor = 6;
    const serverState = events
      .slice(0, serverCursor)
      .reduce(reduceSandboxLifecycleState, initialSandboxLifecycleState);
    const clientState = events.slice(serverCursor).reduce(reduceSandboxLifecycleState, serverState);
    const fullReplayState = events.reduce(
      reduceSandboxLifecycleState,
      initialSandboxLifecycleState,
    );

    expect(clientState).toEqual(fullReplayState);
    expect(clientState.sandboxes.get(SANDBOX_ID)?.previewUrl).toBe("https://preview.example.dev");
    expect(clientState.sandboxes.get(SANDBOX_ID)?.lastCommand?.wallClockTimeMs).toBe(1412);
  });
});

const sandboxScenarioEvents = (): ReadonlyArray<CommittedDurableEvent> => [
  frameworkEvent(
    1,
    toolCallCreatedEventType,
    ToolCallCreatedPayload.make({
      runId: RUN_ID,
      turnId: TURN_ID,
      inferenceId: INFERENCE_ID,
      toolCallId: TOOL_CALL_ID,
      promptPart: Prompt.toolCallPart({
        id: "tool-call-1",
        name: "runSandboxTask",
        params: { task: "run tests and prepare preview" },
        providerExecuted: false,
      }),
    }),
  ),
  committed(2, SandboxEvents.starting({ toolCallId: TOOL_CALL_ID, ...context(2) })),
  committed(
    3,
    SandboxEvents.started({ toolCallId: TOOL_CALL_ID, sandboxId: SANDBOX_ID, ...context(3) }),
  ),
  committed(
    4,
    SandboxEvents.commandStarted({
      sandboxId: SANDBOX_ID,
      commandId: COMMAND_ID,
      command: "pnpm test",
      ...context(4),
    }),
  ),
  committed(
    5,
    SandboxEvents.commandCompleted({
      sandboxId: SANDBOX_ID,
      commandId: COMMAND_ID,
      wallClockTimeMs: 1412,
      ...context(5),
    }),
  ),
  committed(
    6,
    SandboxEvents.previewReady({
      sandboxId: SANDBOX_ID,
      url: PreviewUrl.make("https://preview.example.dev"),
      ...context(6),
    }),
  ),
  committed(
    7,
    SandboxEvents.approvalRequested({
      sandboxId: SANDBOX_ID,
      action: ACTION,
      ...context(7),
    }),
  ),
  committed(
    8,
    SandboxEvents.approvalGranted({
      sandboxId: SANDBOX_ID,
      action: ACTION,
      approverId: ApproverId.make("user_456"),
      ...context(8),
    }),
  ),
  frameworkEvent(
    9,
    toolCallCompletedEventType,
    ToolCallCompletedPayload.make({
      toolCallId: TOOL_CALL_ID,
      promptPart: Prompt.toolResultPart({
        id: "tool-call-1",
        name: "runSandboxTask",
        isFailure: false,
        result: { ok: true },
      }),
    }),
  ),
  committed(
    10,
    SandboxEvents.stopped({
      sandboxId: SANDBOX_ID,
      reason: "idle",
      ...context(10),
    }),
  ),
];

const frameworkEvent = (seq: number, type: EventType, payload: unknown): CommittedDurableEvent =>
  committed(
    seq,
    DurableEventEnvelope.make({
      namespace: effectDurableAgentNamespace,
      type,
      schemaVersion: schemaV1,
      durability: "durable",
      eventId: EventId.make(sequentialUuidV7(300 + seq)),
      sessionId: SESSION_ID,
      createdAtMs: UnixEpochMillis.make(1_715_000_000_000 + seq),
      payload,
    }),
  );

const committed = (seq: number, event: DurableEventEnvelope): CommittedDurableEvent =>
  CommittedDurableEvent.make({
    position: durablePosition(SequenceNumber.make(seq)),
    event,
  });

const context = (seq: number) => ({
  createdAtMs: UnixEpochMillis.make(1_715_000_000_000 + seq),
  eventId: EventId.make(sequentialUuidV7(400 + seq)),
  sessionId: SESSION_ID,
});
