import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { CorrelationId, EventId, Position, SessionId } from "../core";
import { EDAEventTrace, makeRootEDAEventTrace } from "../tracing";

/** Event namespace, such as `effect-durable-agent`, `sandbox`, or `gia`. */
export const EventNamespace = Schema.NonEmptyString;
export type EventNamespace = typeof EventNamespace.Type;

/** Event type within a namespace, such as `CommandAdmitted` or `TextDelta`. */
export const EventType = Schema.NonEmptyString;
export type EventType = typeof EventType.Type;

/** Build a non-empty event type while preserving its string literal for exhaustive unions. */
export const makeEventType = <const Value extends string>(
  value: Value extends "" ? never : Value,
) => value;

/** Extract the literal name retained by `makeEventType`. */
export type EventTypeName<Type extends EventType> = Type;

/** Provider-supplied stream part identity, preserved for correlation/debugging. */
export const ProviderPartId = Schema.NonEmptyString.pipe(Schema.brand("ProviderPartId"));
export type ProviderPartId = typeof ProviderPartId.Type;

/** Positive schema version for one namespace/type payload contract. */
export const SchemaVersion = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));
export type SchemaVersion = typeof SchemaVersion.Type;

/** Milliseconds since unix epoch, represented as a non-negative integer. */
export const UnixEpochMillis = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).pipe(
  Schema.brand("UnixEpochMillis"),
);
export type UnixEpochMillis = typeof UnixEpochMillis.Type;

/** Whether an event is persisted or live-only. */
export const EventDurability = Schema.Literals(["durable", "ephemeral"]);
export type EventDurability = typeof EventDurability.Type;

/** Common event envelope decoded before payload-specific schemas. */
export const EventEnvelope = Schema.Struct({
  namespace: EventNamespace,
  type: EventType,
  schemaVersion: SchemaVersion,
  durability: EventDurability,
  eventId: EventId,
  sessionId: SessionId,
  correlationId: Schema.optionalKey(CorrelationId),
  createdAtMs: UnixEpochMillis,
  trace: EDAEventTrace.pipe(Schema.withConstructorDefault(Effect.sync(makeRootEDAEventTrace))),
  payload: Schema.Unknown,
});
export type EventEnvelope = typeof EventEnvelope.Type;

/** Common envelope narrowed to committed durable events. */
export const DurableEventEnvelope = Schema.Struct({
  ...EventEnvelope.fields,
  durability: Schema.Literal("durable"),
});
export type DurableEventEnvelope = typeof DurableEventEnvelope.Type;

/** Common envelope narrowed to live-only ephemeral events. */
export const EphemeralEventEnvelope = Schema.Struct({
  ...EventEnvelope.fields,
  durability: Schema.Literal("ephemeral"),
});
export type EphemeralEventEnvelope = typeof EphemeralEventEnvelope.Type;

/** Event delivered to live subscribers with its durable/ephemeral position. */
export const PositionedEvent = Schema.Struct({
  position: Position,
  event: EventEnvelope,
});
export type PositionedEvent = typeof PositionedEvent.Type;

/** Built-in framework event namespace. */
export const effectDurableAgentNamespace = EventNamespace.make("effect-durable-agent");

/** Initial schema version for built-in event payloads. */
export const schemaV1 = SchemaVersion.make(1);

/** Non-negative integer used by token counts and similar counters. */
export const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
export type NonNegativeInt = typeof NonNegativeInt.Type;

/** Registered tool name; plain non-empty text, not a lifecycle identity. */
export const ToolName = Schema.NonEmptyString;
export type ToolName = typeof ToolName.Type;

/** Error details stored on failed lifecycle events. */
export const FailurePayload = Schema.Struct({
  message: Schema.String,
  code: Schema.optionalKey(Schema.String),
  details: Schema.optionalKey(Schema.Unknown),
});
export type FailurePayload = typeof FailurePayload.Type;

/** Token usage reported by a model provider when available. */
export const UsagePayload = Schema.Struct({
  inputTokens: Schema.optionalKey(NonNegativeInt),
  cachedInputTokens: Schema.optionalKey(NonNegativeInt),
  outputTokens: Schema.optionalKey(NonNegativeInt),
  textTokens: Schema.optionalKey(NonNegativeInt),
  reasoningTokens: Schema.optionalKey(NonNegativeInt),
});
export type UsagePayload = typeof UsagePayload.Type;

/** Intended provider/model selection known before a model request starts. */
export const ModelSelectionPayload = Schema.Struct({
  provider: Schema.NonEmptyString,
  modelId: Schema.NonEmptyString,
  settings: Schema.optionalKey(Schema.Unknown),
});
export type ModelSelectionPayload = typeof ModelSelectionPayload.Type;

/** App-owned instructions inserted into model context as a system message. */
export const SystemPromptText = Schema.NonEmptyString;
export type SystemPromptText = typeof SystemPromptText.Type;
