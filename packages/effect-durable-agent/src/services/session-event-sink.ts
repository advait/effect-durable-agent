import * as Effect from "effect/Effect";

import type {
  DurableEventEnvelope,
  EphemeralEventEnvelope,
  PositionedEvent,
} from "../types/events";
import type { EDADecider, EDADeciderOutcome } from "./decider";
import type { SessionCommandAdmissionConflict } from "./session-admission";
import type { CommittedDurableEvent, EDASessionStoreError } from "./session-store";
import type { SessionDecideError, SessionStateSnapshotData } from "./session-state";

/**
 * Narrow write/publish capability passed from `SessionState` into execution subcomponents.
 *
 * This intentionally excludes admission and scheduler methods so turns, inferences, and tools can
 * emit events without becoming session-state authorities. Tools get two state-aware calls: a
 * read-only snapshot, and `decide`, which reads and appends under the session's own gates.
 */
export interface SessionEventSink {
  /** Commit one durable event through the authoritative session write path. */
  readonly appendDurable: (
    event: DurableEventEnvelope,
  ) => Effect.Effect<CommittedDurableEvent, EDASessionStoreError>;
  /** Commit one app-written durable event after the app event admission rules. */
  readonly appendAppDurable: (
    event: DurableEventEnvelope,
  ) => Effect.Effect<CommittedDurableEvent, EDASessionStoreError | SessionCommandAdmissionConflict>;
  /** Commit an ordered durable batch through the authoritative session write path. */
  readonly appendDurableBatch: (
    events: ReadonlyArray<DurableEventEnvelope>,
  ) => Effect.Effect<ReadonlyArray<CommittedDurableEvent>, EDASessionStoreError>;
  /** Position and publish a live-only event against the session's durable head. */
  readonly publishEphemeral: (
    event: EphemeralEventEnvelope,
  ) => Effect.Effect<PositionedEvent, EDASessionStoreError>;
  /** Run a registered decider against the committed fold, atomically with its append. */
  readonly decide: <Input, Result, Rejection>(
    decider: EDADecider<string, Input, Result, Rejection>,
    input: Input,
  ) => Effect.Effect<EDADeciderOutcome<Result, Rejection>, SessionDecideError>;
  /** Observe the authoritative framework state plus app reducer states. */
  readonly snapshotData: () => Effect.Effect<SessionStateSnapshotData>;
}
