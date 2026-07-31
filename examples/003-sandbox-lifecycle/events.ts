import * as Schema from "effect/Schema";

import { EventId, SessionId, ToolCallId } from "../../src/types/core";
import {
  DurableEventEnvelope,
  EventNamespace,
  EventType,
  NonNegativeInt,
  UnixEpochMillis,
  makeRootEDAEventTrace,
  schemaV1,
} from "../../src/types/events";

export const sandboxLifecycleNamespace = EventNamespace.make("example.sandbox");

export const sandboxStartingEventType = EventType.make("SandboxStarting");
export const sandboxStartedEventType = EventType.make("SandboxStarted");
export const sandboxCommandStartedEventType = EventType.make("CommandStarted");
export const sandboxCommandCompletedEventType = EventType.make("CommandCompleted");
export const previewReadyEventType = EventType.make("PreviewReady");
export const approvalRequestedEventType = EventType.make("ApprovalRequested");
export const approvalGrantedEventType = EventType.make("ApprovalGranted");
export const sandboxStoppedEventType = EventType.make("SandboxStopped");

export const SandboxId = Schema.NonEmptyString.pipe(Schema.brand("SandboxId"));
export type SandboxId = typeof SandboxId.Type;

export const SandboxCommandId = Schema.NonEmptyString.pipe(Schema.brand("SandboxCommandId"));
export type SandboxCommandId = typeof SandboxCommandId.Type;

export const PreviewUrl = Schema.NonEmptyString.pipe(Schema.brand("PreviewUrl"));
export type PreviewUrl = typeof PreviewUrl.Type;

export const ApprovalAction = Schema.NonEmptyString.pipe(Schema.brand("ApprovalAction"));
export type ApprovalAction = typeof ApprovalAction.Type;

export const ApproverId = Schema.NonEmptyString.pipe(Schema.brand("ApproverId"));
export type ApproverId = typeof ApproverId.Type;

export const SandboxStartingPayload = Schema.Struct({ toolCallId: ToolCallId });
export type SandboxStartingPayload = typeof SandboxStartingPayload.Type;

export const SandboxStartedPayload = Schema.Struct({
  toolCallId: ToolCallId,
  sandboxId: SandboxId,
});
export type SandboxStartedPayload = typeof SandboxStartedPayload.Type;

export const SandboxCommandStartedPayload = Schema.Struct({
  sandboxId: SandboxId,
  commandId: SandboxCommandId,
  command: Schema.NonEmptyString,
});
export type SandboxCommandStartedPayload = typeof SandboxCommandStartedPayload.Type;

export const SandboxCommandCompletedPayload = Schema.Struct({
  sandboxId: SandboxId,
  commandId: SandboxCommandId,
  wallClockTimeMs: NonNegativeInt,
});
export type SandboxCommandCompletedPayload = typeof SandboxCommandCompletedPayload.Type;

export const PreviewReadyPayload = Schema.Struct({
  sandboxId: SandboxId,
  url: PreviewUrl,
});
export type PreviewReadyPayload = typeof PreviewReadyPayload.Type;

export const ApprovalRequestedPayload = Schema.Struct({
  sandboxId: SandboxId,
  action: ApprovalAction,
});
export type ApprovalRequestedPayload = typeof ApprovalRequestedPayload.Type;

export const ApprovalGrantedPayload = Schema.Struct({
  sandboxId: SandboxId,
  action: ApprovalAction,
  approverId: ApproverId,
});
export type ApprovalGrantedPayload = typeof ApprovalGrantedPayload.Type;

export const SandboxStoppedPayload = Schema.Struct({
  sandboxId: SandboxId,
  reason: Schema.NonEmptyString,
});
export type SandboxStoppedPayload = typeof SandboxStoppedPayload.Type;

interface AppEventContext {
  readonly createdAtMs: UnixEpochMillis;
  readonly eventId: EventId;
  readonly sessionId: SessionId;
}

export const SandboxEvents = {
  starting: (input: AppEventContext & SandboxStartingPayload): DurableEventEnvelope => {
    const { createdAtMs, eventId, sessionId, ...payload } = input;
    return sandboxDurableEvent({
      createdAtMs,
      eventId,
      payload: SandboxStartingPayload.make(payload),
      sessionId,
      type: sandboxStartingEventType,
    });
  },

  started: (input: AppEventContext & SandboxStartedPayload): DurableEventEnvelope => {
    const { createdAtMs, eventId, sessionId, ...payload } = input;
    return sandboxDurableEvent({
      createdAtMs,
      eventId,
      payload: SandboxStartedPayload.make(payload),
      sessionId,
      type: sandboxStartedEventType,
    });
  },

  commandStarted: (input: AppEventContext & SandboxCommandStartedPayload): DurableEventEnvelope => {
    const { createdAtMs, eventId, sessionId, ...payload } = input;
    return sandboxDurableEvent({
      createdAtMs,
      eventId,
      payload: SandboxCommandStartedPayload.make(payload),
      sessionId,
      type: sandboxCommandStartedEventType,
    });
  },

  commandCompleted: (
    input: AppEventContext & SandboxCommandCompletedPayload,
  ): DurableEventEnvelope => {
    const { createdAtMs, eventId, sessionId, ...payload } = input;
    return sandboxDurableEvent({
      createdAtMs,
      eventId,
      payload: SandboxCommandCompletedPayload.make(payload),
      sessionId,
      type: sandboxCommandCompletedEventType,
    });
  },

  previewReady: (input: AppEventContext & PreviewReadyPayload): DurableEventEnvelope => {
    const { createdAtMs, eventId, sessionId, ...payload } = input;
    return sandboxDurableEvent({
      createdAtMs,
      eventId,
      payload: PreviewReadyPayload.make(payload),
      sessionId,
      type: previewReadyEventType,
    });
  },

  approvalRequested: (input: AppEventContext & ApprovalRequestedPayload): DurableEventEnvelope => {
    const { createdAtMs, eventId, sessionId, ...payload } = input;
    return sandboxDurableEvent({
      createdAtMs,
      eventId,
      payload: ApprovalRequestedPayload.make(payload),
      sessionId,
      type: approvalRequestedEventType,
    });
  },

  approvalGranted: (input: AppEventContext & ApprovalGrantedPayload): DurableEventEnvelope => {
    const { createdAtMs, eventId, sessionId, ...payload } = input;
    return sandboxDurableEvent({
      createdAtMs,
      eventId,
      payload: ApprovalGrantedPayload.make(payload),
      sessionId,
      type: approvalGrantedEventType,
    });
  },

  stopped: (input: AppEventContext & SandboxStoppedPayload): DurableEventEnvelope => {
    const { createdAtMs, eventId, sessionId, ...payload } = input;
    return sandboxDurableEvent({
      createdAtMs,
      eventId,
      payload: SandboxStoppedPayload.make(payload),
      sessionId,
      type: sandboxStoppedEventType,
    });
  },
};

const sandboxDurableEvent = (input: {
  readonly createdAtMs: UnixEpochMillis;
  readonly eventId: EventId;
  readonly payload: unknown;
  readonly sessionId: SessionId;
  readonly type: EventType;
}): DurableEventEnvelope =>
  DurableEventEnvelope.make({
    namespace: sandboxLifecycleNamespace,
    type: input.type,
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: input.eventId,
    sessionId: input.sessionId,
    createdAtMs: input.createdAtMs,
    trace: makeRootEDAEventTrace(),
    payload: input.payload,
  });
