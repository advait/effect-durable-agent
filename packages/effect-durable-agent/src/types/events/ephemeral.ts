import * as Schema from "effect/Schema";

import { ToolCallId } from "../core";
import { ProviderPartId, ToolName, makeEventType } from "./envelope";
import { ephemeralEventSchema } from "./internal";

/** Event type values for ephemeral model stream events. */
export const textDeltaEventType = makeEventType("TextDelta");
export const reasoningDeltaEventType = makeEventType("ReasoningDelta");
export const toolParamsStartEventType = makeEventType("ToolParamsStart");
export const toolParamsDeltaEventType = makeEventType("ToolParamsDelta");
export const toolParamsEndEventType = makeEventType("ToolParamsEnd");

/** Payload for live assistant text deltas. */
export const TextDeltaPayload = Schema.Struct({
  providerPartId: ProviderPartId,
  delta: Schema.String,
});
export type TextDeltaPayload = typeof TextDeltaPayload.Type;
export const TextDeltaEvent = ephemeralEventSchema(textDeltaEventType, TextDeltaPayload);
export type TextDeltaEvent = typeof TextDeltaEvent.Type;

/** Payload for live assistant reasoning deltas. */
export const ReasoningDeltaPayload = Schema.Struct({
  providerPartId: ProviderPartId,
  delta: Schema.String,
});
export type ReasoningDeltaPayload = typeof ReasoningDeltaPayload.Type;
export const ReasoningDeltaEvent = ephemeralEventSchema(
  reasoningDeltaEventType,
  ReasoningDeltaPayload,
);
export type ReasoningDeltaEvent = typeof ReasoningDeltaEvent.Type;

/** Payload for the speculative start of streamed tool parameters. */
export const ToolParamsStartPayload = Schema.Struct({
  providerPartId: ProviderPartId,
  toolCallId: ToolCallId,
  toolName: ToolName,
  providerExecuted: Schema.Boolean,
});
export type ToolParamsStartPayload = typeof ToolParamsStartPayload.Type;
export const ToolParamsStartEvent = ephemeralEventSchema(
  toolParamsStartEventType,
  ToolParamsStartPayload,
);
export type ToolParamsStartEvent = typeof ToolParamsStartEvent.Type;

/** Payload for one speculative streamed tool parameter chunk. */
export const ToolParamsDeltaPayload = Schema.Struct({
  providerPartId: ProviderPartId,
  toolCallId: ToolCallId,
  delta: Schema.String,
});
export type ToolParamsDeltaPayload = typeof ToolParamsDeltaPayload.Type;
export const ToolParamsDeltaEvent = ephemeralEventSchema(
  toolParamsDeltaEventType,
  ToolParamsDeltaPayload,
);
export type ToolParamsDeltaEvent = typeof ToolParamsDeltaEvent.Type;

/** Payload for the speculative end of streamed tool parameters. */
export const ToolParamsEndPayload = Schema.Struct({
  providerPartId: ProviderPartId,
  toolCallId: ToolCallId,
});
export type ToolParamsEndPayload = typeof ToolParamsEndPayload.Type;
export const ToolParamsEndEvent = ephemeralEventSchema(
  toolParamsEndEventType,
  ToolParamsEndPayload,
);
export type ToolParamsEndEvent = typeof ToolParamsEndEvent.Type;

/** Ephemeral event union for live provider stream parts. */
export const ModelStreamEvent = Schema.Union([
  TextDeltaEvent,
  ReasoningDeltaEvent,
  ToolParamsStartEvent,
  ToolParamsDeltaEvent,
  ToolParamsEndEvent,
]);
export type ModelStreamEvent = typeof ModelStreamEvent.Type;

/** Event type values for ephemeral runtime status events. */
export const subscriberStatusEventType = makeEventType("SubscriberStatus");
export const traceStatusEventType = makeEventType("TraceStatus");

/** Status values emitted by the live subscriber pipeline. */
export const SubscriberStatusKind = Schema.Literals([
  "connected",
  "catching-up",
  "live",
  "lagging",
  "closed",
]);
export type SubscriberStatusKind = typeof SubscriberStatusKind.Type;

/** Payload for live subscriber lifecycle and lag notifications. */
export const SubscriberStatusPayload = Schema.Struct({
  status: SubscriberStatusKind,
  reason: Schema.optionalKey(Schema.String),
});
export type SubscriberStatusPayload = typeof SubscriberStatusPayload.Type;
export const SubscriberStatusEvent = ephemeralEventSchema(
  subscriberStatusEventType,
  SubscriberStatusPayload,
);
export type SubscriberStatusEvent = typeof SubscriberStatusEvent.Type;

/** Status values emitted by runtime tracing hooks. */
export const TraceStatusKind = Schema.Literals(["span-started", "span-ended", "span-failed"]);
export type TraceStatusKind = typeof TraceStatusKind.Type;

/** Payload for live trace/status breadcrumbs that are never replayed durably. */
export const TraceStatusPayload = Schema.Struct({
  status: TraceStatusKind,
  spanName: Schema.NonEmptyString,
  details: Schema.optionalKey(Schema.Unknown),
});
export type TraceStatusPayload = typeof TraceStatusPayload.Type;
export const TraceStatusEvent = ephemeralEventSchema(traceStatusEventType, TraceStatusPayload);
export type TraceStatusEvent = typeof TraceStatusEvent.Type;

/** Ephemeral event union for live runtime status updates. */
export const RuntimeStatusEvent = Schema.Union([SubscriberStatusEvent, TraceStatusEvent]);
export type RuntimeStatusEvent = typeof RuntimeStatusEvent.Type;

/** Built-in ephemeral event union for framework-owned live-only updates. */
export const EDAEphemeralEvent = Schema.Union([ModelStreamEvent, RuntimeStatusEvent]);
export type EDAEphemeralEvent = typeof EDAEphemeralEvent.Type;
