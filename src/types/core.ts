import * as Order from "effect/Order";
import * as Schema from "effect/Schema";

const UuidString = Schema.String.check(Schema.isUUID(undefined, { identifier: "Uuid" }));
const UuidV7String = Schema.String.check(Schema.isUUID(7));

/** Durable conversation identity; one Durable Object stores one session.
 *
 * New EDA sessions are minted as UUIDv7. Migrated sessions may retain UUIDv4
 * identities, so runtime identity accepts any RFC UUID without changing app
 * URLs or R2 keys.
 */
export const SessionId = UuidString.pipe(Schema.brand("SessionId"));
export type SessionId = typeof SessionId.Type;

/** Durable event identity; `(sessionId, eventId)` is the idempotency key. */
export const EventId = UuidV7String.pipe(Schema.brand("EventId"));
export type EventId = typeof EventId.Type;

/** User input command identity. */
export const CommandId = UuidV7String.pipe(Schema.brand("CommandId"));
export type CommandId = typeof CommandId.Type;

/** Processing lifecycle identity triggered by one or more commands. */
export const RunId = UuidV7String.pipe(Schema.brand("RunId"));
export type RunId = typeof RunId.Type;

/** One LLM invocation within a run. */
export const TurnId = UuidV7String.pipe(Schema.brand("TurnId"));
export type TurnId = typeof TurnId.Type;

/** One concrete model inference for a turn. */
export const InferenceId = UuidV7String.pipe(Schema.brand("InferenceId"));
export type InferenceId = typeof InferenceId.Type;

/** One framework-owned tool invocation requested by the model. */
export const ToolCallId = UuidV7String.pipe(Schema.brand("ToolCallId"));
export type ToolCallId = typeof ToolCallId.Type;

/** Retained message body identity. */
export const MessageId = UuidV7String.pipe(Schema.brand("MessageId"));
export type MessageId = typeof MessageId.Type;

/** Optional live-stream demux identity, such as a `toolCallId`. */
export const CorrelationId = UuidV7String.pipe(Schema.brand("CorrelationId"));
export type CorrelationId = typeof CorrelationId.Type;

/** One compaction workflow identity. */
export const CompactionId = UuidV7String.pipe(Schema.brand("CompactionId"));
export type CompactionId = typeof CompactionId.Type;

/** One retained summary artifact identity. */
export const SummaryId = UuidV7String.pipe(Schema.brand("SummaryId"));
export type SummaryId = typeof SummaryId.Type;

/** One BaseState lifecycle identity. */
export const BaseStateId = UuidV7String.pipe(Schema.brand("BaseStateId"));
export type BaseStateId = typeof BaseStateId.Type;

/** Durable context projection version. */
export const ContextVersion = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).pipe(
  Schema.brand("ContextVersion"),
);
export type ContextVersion = typeof ContextVersion.Type;

/** Durable sequence number; `0` is the genesis/resume-before-first-event position. */
export const SequenceNumber = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).pipe(
  Schema.brand("SequenceNumber"),
);
export type SequenceNumber = typeof SequenceNumber.Type;

/** In-memory ordering slot for ephemeral events anchored to a durable sequence. */
export const SubSequenceNumber = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).pipe(
  Schema.brand("SubSequenceNumber"),
);
export type SubSequenceNumber = typeof SubSequenceNumber.Type;

/** The only valid sub-sequence for durable events. */
export const DurableSubSequenceNumber = Schema.Literal(0).pipe(Schema.brand("SubSequenceNumber"));
export type DurableSubSequenceNumber = typeof DurableSubSequenceNumber.Type;

/** Total live-stream order: durable events are `(seq, 0)`, ephemerals are `(anchorSeq, subSeq > 0)`. */
export const Position = Schema.Struct({
  seq: SequenceNumber,
  subSeq: SubSequenceNumber,
});
export type Position = typeof Position.Type;

/** Position for committed durable events; `subSeq` is literally zero. */
export const DurablePosition = Schema.Struct({
  seq: SequenceNumber,
  subSeq: DurableSubSequenceNumber,
});
export type DurablePosition = typeof DurablePosition.Type;

/** Construct the live position for a committed durable event. */
export const durablePosition = (seq: SequenceNumber): DurablePosition =>
  DurablePosition.make({ seq, subSeq: DurableSubSequenceNumber.make(0) });

/** Total ordering for live positions: compare durable `seq` first, then live `subSeq`. */
export const PositionOrder: Order.Order<Position> = Order.Struct({
  seq: Order.Number,
  subSeq: Order.Number,
});

/** Compare live positions lexicographically. */
export const comparePosition = PositionOrder;
