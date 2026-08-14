import * as Schema from "effect/Schema";

import type { CommittedDurableEvent } from "effect-durable-agent/services/session-store";
import { EDAReducer } from "effect-durable-agent/services/reducer-registry";
import { ToolCallId } from "effect-durable-agent/types/core";
import {
  ToolCallCompletedPayload,
  ToolCallCreatedPayload,
  toolCallCompletedEventType,
  toolCallCreatedEventType,
} from "effect-durable-agent/types/events";
import {
  ApprovalGrantedPayload,
  ApprovalRequestedPayload,
  PreviewReadyPayload,
  SandboxCommandCompletedPayload,
  SandboxCommandId,
  SandboxCommandStartedPayload,
  SandboxId,
  SandboxStartedPayload,
  SandboxStartingPayload,
  SandboxStoppedPayload,
  approvalGrantedEventType,
  approvalRequestedEventType,
  previewReadyEventType,
  sandboxCommandCompletedEventType,
  sandboxCommandStartedEventType,
  sandboxLifecycleNamespace,
  sandboxStartedEventType,
  sandboxStartingEventType,
  sandboxStoppedEventType,
} from "./events";

export interface SandboxToolCard {
  readonly toolCallId: ToolCallId;
  readonly toolName?: string;
  readonly status: "created" | "completed";
}

export interface SandboxCommandState {
  readonly commandId: SandboxCommandId;
  readonly command: string;
  readonly wallClockTimeMs?: number;
}

export interface SandboxState {
  readonly sandboxId: SandboxId;
  readonly toolCallId: ToolCallId;
  readonly status: "starting" | "running" | "stopped";
  readonly runningCommand?: SandboxCommandState;
  readonly lastCommand?: SandboxCommandState;
  readonly previewUrl?: string;
  readonly approval?:
    | { readonly _tag: "Requested"; readonly action: string }
    | { readonly _tag: "Granted"; readonly action: string; readonly approverId: string };
  readonly stopReason?: string;
}

export interface SandboxLifecycleState {
  readonly activeSandboxId?: SandboxId;
  readonly sandboxIdByToolCallId: ReadonlyMap<ToolCallId, SandboxId>;
  readonly startingToolCalls: ReadonlySet<ToolCallId>;
  readonly toolCards: ReadonlyMap<ToolCallId, SandboxToolCard>;
  readonly sandboxes: ReadonlyMap<SandboxId, SandboxState>;
}

export const initialSandboxLifecycleState: SandboxLifecycleState = {
  activeSandboxId: undefined,
  sandboxIdByToolCallId: new Map(),
  startingToolCalls: new Set(),
  toolCards: new Map(),
  sandboxes: new Map(),
};

const SandboxCommandStateSchema = Schema.Struct({
  commandId: SandboxCommandId,
  command: Schema.String,
  wallClockTimeMs: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
});
const SandboxToolCardSchema = Schema.Struct({
  toolCallId: ToolCallId,
  toolName: Schema.optionalKey(Schema.String),
  status: Schema.Literals(["created", "completed"]),
});
const SandboxStateSchema = Schema.Struct({
  sandboxId: SandboxId,
  toolCallId: ToolCallId,
  status: Schema.Literals(["starting", "running", "stopped"]),
  runningCommand: Schema.optionalKey(SandboxCommandStateSchema),
  lastCommand: Schema.optionalKey(SandboxCommandStateSchema),
  previewUrl: Schema.optionalKey(Schema.String),
  approval: Schema.optionalKey(
    Schema.Union([
      Schema.Struct({ _tag: Schema.Literal("Requested"), action: Schema.String }),
      Schema.Struct({
        _tag: Schema.Literal("Granted"),
        action: Schema.String,
        approverId: Schema.String,
      }),
    ]),
  ),
  stopReason: Schema.optionalKey(Schema.String),
});
const SandboxLifecycleStateSchema = Schema.Struct({
  activeSandboxId: Schema.optionalKey(SandboxId),
  sandboxIdByToolCallId: Schema.ReadonlyMap(ToolCallId, SandboxId),
  startingToolCalls: Schema.ReadonlySet(ToolCallId),
  toolCards: Schema.ReadonlyMap(ToolCallId, SandboxToolCardSchema),
  sandboxes: Schema.ReadonlyMap(SandboxId, SandboxStateSchema),
});

/** Pure UI/application projection over framework tool events and app sandbox events. */
export const SandboxLifecycleReducer = EDAReducer.make<SandboxLifecycleState>({
  name: "example.sandbox",
  initial: initialSandboxLifecycleState,
  stateSchema: SandboxLifecycleStateSchema,
  reduce: (state, entry) => reduceSandboxLifecycleState(state, entry),
});

export const reduceSandboxLifecycleState = (
  state: SandboxLifecycleState,
  entry: CommittedDurableEvent,
): SandboxLifecycleState => {
  const event = entry.event;

  if (event.type === toolCallCreatedEventType && Schema.is(ToolCallCreatedPayload)(event.payload)) {
    return rememberToolCallCreated(state, event.payload);
  }
  if (
    event.type === toolCallCompletedEventType &&
    Schema.is(ToolCallCompletedPayload)(event.payload)
  ) {
    return rememberToolCallCompleted(state, event.payload);
  }

  if (event.namespace !== sandboxLifecycleNamespace) {
    return state;
  }

  if (event.type === sandboxStartingEventType && Schema.is(SandboxStartingPayload)(event.payload)) {
    return rememberSandboxStarting(state, event.payload);
  }
  if (event.type === sandboxStartedEventType && Schema.is(SandboxStartedPayload)(event.payload)) {
    return rememberSandboxStarted(state, event.payload);
  }
  if (
    event.type === sandboxCommandStartedEventType &&
    Schema.is(SandboxCommandStartedPayload)(event.payload)
  ) {
    return rememberCommandStarted(state, event.payload);
  }
  if (
    event.type === sandboxCommandCompletedEventType &&
    Schema.is(SandboxCommandCompletedPayload)(event.payload)
  ) {
    return rememberCommandCompleted(state, event.payload);
  }
  if (event.type === previewReadyEventType && Schema.is(PreviewReadyPayload)(event.payload)) {
    const payload = event.payload;
    return updateSandbox(state, payload.sandboxId, (sandbox) => ({
      ...sandbox,
      previewUrl: payload.url,
    }));
  }
  if (
    event.type === approvalRequestedEventType &&
    Schema.is(ApprovalRequestedPayload)(event.payload)
  ) {
    const payload = event.payload;
    return updateSandbox(state, payload.sandboxId, (sandbox) => ({
      ...sandbox,
      approval: { _tag: "Requested", action: payload.action },
    }));
  }
  if (event.type === approvalGrantedEventType && Schema.is(ApprovalGrantedPayload)(event.payload)) {
    const payload = event.payload;
    return updateSandbox(state, payload.sandboxId, (sandbox) => ({
      ...sandbox,
      approval: {
        _tag: "Granted",
        action: payload.action,
        approverId: payload.approverId,
      },
    }));
  }
  if (event.type === sandboxStoppedEventType && Schema.is(SandboxStoppedPayload)(event.payload)) {
    const payload = event.payload;
    return updateSandbox(state, payload.sandboxId, (sandbox) => ({
      ...sandbox,
      status: "stopped",
      stopReason: payload.reason,
    }));
  }

  return state;
};

const rememberToolCallCreated = (
  state: SandboxLifecycleState,
  payload: ToolCallCreatedPayload,
): SandboxLifecycleState => ({
  ...state,
  toolCards: new Map(state.toolCards).set(payload.toolCallId, {
    toolCallId: payload.toolCallId,
    toolName: payload.promptPart.name,
    status: "created",
  }),
});

const rememberToolCallCompleted = (
  state: SandboxLifecycleState,
  payload: ToolCallCompletedPayload,
): SandboxLifecycleState => {
  const existing = state.toolCards.get(payload.toolCallId);
  return {
    ...state,
    toolCards: new Map(state.toolCards).set(payload.toolCallId, {
      toolCallId: payload.toolCallId,
      toolName: existing?.toolName,
      status: "completed",
    }),
  };
};

const rememberSandboxStarting = (
  state: SandboxLifecycleState,
  payload: SandboxStartingPayload,
): SandboxLifecycleState => ({
  ...state,
  startingToolCalls: new Set(state.startingToolCalls).add(payload.toolCallId),
});

const rememberSandboxStarted = (
  state: SandboxLifecycleState,
  payload: SandboxStartedPayload,
): SandboxLifecycleState => ({
  ...state,
  activeSandboxId: payload.sandboxId,
  sandboxIdByToolCallId: new Map(state.sandboxIdByToolCallId).set(
    payload.toolCallId,
    payload.sandboxId,
  ),
  startingToolCalls: without(state.startingToolCalls, payload.toolCallId),
  sandboxes: new Map(state.sandboxes).set(payload.sandboxId, {
    sandboxId: payload.sandboxId,
    toolCallId: payload.toolCallId,
    status: "running",
  }),
});

const rememberCommandStarted = (
  state: SandboxLifecycleState,
  payload: SandboxCommandStartedPayload,
): SandboxLifecycleState =>
  updateSandbox(state, payload.sandboxId, (sandbox) => ({
    ...sandbox,
    runningCommand: {
      commandId: payload.commandId,
      command: payload.command,
    },
  }));

const rememberCommandCompleted = (
  state: SandboxLifecycleState,
  payload: SandboxCommandCompletedPayload,
): SandboxLifecycleState =>
  updateSandbox(state, payload.sandboxId, (sandbox) => {
    const completed =
      sandbox.runningCommand?.commandId === payload.commandId
        ? {
            ...sandbox.runningCommand,
            wallClockTimeMs: payload.wallClockTimeMs,
          }
        : {
            commandId: payload.commandId,
            command: "(command text was not in this replay window)",
            wallClockTimeMs: payload.wallClockTimeMs,
          };
    return {
      ...sandbox,
      runningCommand: undefined,
      lastCommand: completed,
    };
  });

const updateSandbox = (
  state: SandboxLifecycleState,
  sandboxId: SandboxId,
  update: (sandbox: SandboxState) => SandboxState,
): SandboxLifecycleState => {
  const sandbox = state.sandboxes.get(sandboxId);
  if (sandbox === undefined) {
    return state;
  }
  return {
    ...state,
    sandboxes: new Map(state.sandboxes).set(sandboxId, update(sandbox)),
  };
};

const without = <Value>(set: ReadonlySet<Value>, value: Value): ReadonlySet<Value> => {
  const next = new Set(set);
  next.delete(value);
  return next;
};
