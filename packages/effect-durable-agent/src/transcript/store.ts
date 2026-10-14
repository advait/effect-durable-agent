import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { EDASessionStoreError } from "../services/session-store";
import type { TranscriptDefinition } from "./mapping";
import type {
  TranscriptVersionMeta,
  TranscriptWindow,
  TranscriptWindowLimits,
  TranscriptWindowSelector,
} from "./window";

/**
 * The transcript definitions a store maintains, fixed when the store is constructed.
 *
 * `current` is what new sessions start with and every session converges to. `previous` is
 * carried for one release only, so a session not yet rebuilt keeps its active rows maintained.
 * A stored version that matches neither is frozen.
 */
export interface TranscriptMaintenanceConfig {
  readonly current: TranscriptDefinition;
  readonly previous?: TranscriptDefinition;
}

/** A cursor minted for another log incarnation or transcript version. The client takes `Latest`. */
export class TranscriptCursorMismatch extends Schema.TaggedError<TranscriptCursorMismatch>()(
  "TranscriptCursorMismatch",
  { message: Schema.String },
) {}

/**
 * Storage-level transcript reads for one session.
 *
 * Maintenance has no method here: the store runs it inside its own append transaction for every
 * carried, healthy version, so `EDASessionStore.append` semantics are unchanged. These reads never
 * construct the model runtime and never include live active content, which only the delivery
 * layer can capture.
 */
export interface EDATranscriptStoreShape {
  /** Read one complete window of the served version at a single cut. */
  readonly readTranscriptWindow: (
    selector: TranscriptWindowSelector,
    limits: TranscriptWindowLimits,
  ) => Effect.Effect<TranscriptWindow, EDASessionStoreError | TranscriptCursorMismatch>;
  /** Maintenance state of every version present, for builders, backfill proofs and alerts. */
  readonly readTranscriptMeta: Effect.Effect<
    ReadonlyArray<TranscriptVersionMeta>,
    EDASessionStoreError
  >;
}

/** Transcript read port, provided alongside `EDASessionStore` by stores that maintain transcripts. */
export class EDATranscriptStore extends Context.Service<
  EDATranscriptStore,
  EDATranscriptStoreShape
>()("@effect-durable-agent/EDATranscriptStore") {}
