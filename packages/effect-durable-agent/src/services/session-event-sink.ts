import * as Effect from "effect/Effect";
import type { OpenResumable, ResumableHandle, ResumableOpenError } from "../domain/resumables";
import type { ToolCallId } from "../types/core";

import type {
  DurableEventEnvelope,
  EphemeralEventEnvelope,
  PositionedEvent,
} from "../types/events";
import type { CommittedDurableEvent, EDASessionStoreError } from "./session-store";

/**
 * Narrow write/publish capability passed from `SessionState` into execution subcomponents.
 *
 * This intentionally excludes `snapshot`, admission, and scheduler methods so
 * turns, inferences, and tools can emit events without becoming session-state
 * authorities.
 */
export interface SessionEventSink {
  /** Open one wait per tool call, atomically with app request facts. The callback only constructs events. */
  readonly openToolResumable: (
    toolCallId: ToolCallId,
    input: OpenResumable,
    requestEvents: (handle: ResumableHandle) => Effect.Effect<ReadonlyArray<DurableEventEnvelope>>,
  ) => Effect.Effect<ResumableHandle, EDASessionStoreError | ResumableOpenError>;
  /** Commit one durable event through the authoritative session write path. */
  readonly appendDurable: (
    event: DurableEventEnvelope,
  ) => Effect.Effect<CommittedDurableEvent, EDASessionStoreError>;
  /** Commit an ordered durable batch through the authoritative session write path. */
  readonly appendDurableBatch: (
    events: ReadonlyArray<DurableEventEnvelope>,
  ) => Effect.Effect<ReadonlyArray<CommittedDurableEvent>, EDASessionStoreError>;
  /** Position and publish a live-only event against the session's durable head. */
  readonly publishEphemeral: (
    event: EphemeralEventEnvelope,
  ) => Effect.Effect<PositionedEvent, EDASessionStoreError>;
}
