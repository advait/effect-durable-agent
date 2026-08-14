import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";

import { SequenceNumber } from "../types/core";
import { EDASessionStoreError } from "./session-store";

/** Stable sink name used as the durable checkpoint key. */
export const EDASinkName = Schema.NonEmptyString.pipe(Schema.brand("EDASinkName"));
export type EDASinkName = typeof EDASinkName.Type;

/** Persisted cursor and sink-owned payload for one registered sink. */
export interface StoredSinkCheckpoint {
  readonly afterSeq: SequenceNumber;
  readonly payload: unknown | undefined;
}

/** Durable checkpoint persistence port for at-least-once sink runners. */
export interface SinkCheckpointStoreShape {
  /** Read the latest committed cursor and sink-owned payload. */
  readonly load: (
    sinkName: EDASinkName,
  ) => Effect.Effect<StoredSinkCheckpoint, EDASessionStoreError>;
  /** Save sink-owned state without advancing the durable cursor. */
  readonly saveState: (
    sinkName: EDASinkName,
    payload: unknown,
  ) => Effect.Effect<void, EDASessionStoreError>;
  /** Atomically commit the processed cursor together with current sink-owned state. */
  readonly commit: (
    sinkName: EDASinkName,
    afterSeq: SequenceNumber,
    payload: unknown | undefined,
  ) => Effect.Effect<void, EDASessionStoreError>;
}

/**
 * Semantic checkpoint store for registered EDA sinks.
 *
 * Checkpoints are intentionally exposed as load/save/commit operations rather
 * than SQL. Sink side effects happen outside event append transactions, and the
 * cursor advances only after the sink finishes its own external work.
 */
export class SinkCheckpointStore extends Context.Service<
  SinkCheckpointStore,
  SinkCheckpointStoreShape
>()("@effect-durable-agent/SinkCheckpointStore") {
  /** In-memory checkpoint implementation for store/runtime tests. */
  static readonly InMemory = Layer.effect(
    SinkCheckpointStore,
    Effect.gen(function* () {
      const checkpoints = yield* Ref.make<ReadonlyMap<EDASinkName, StoredSinkCheckpoint>>(
        new Map(),
      );
      return makeInMemorySinkCheckpointStore(checkpoints);
    }),
  );
}

/** Build a ref-backed checkpoint map with the same public semantics as host adapters. */
const makeInMemorySinkCheckpointStore = (
  checkpoints: Ref.Ref<ReadonlyMap<EDASinkName, StoredSinkCheckpoint>>,
): SinkCheckpointStoreShape => ({
  load: (sinkName) =>
    Ref.get(checkpoints).pipe(
      Effect.map(
        (map) =>
          map.get(sinkName) ?? {
            afterSeq: SequenceNumber.make(0),
            payload: undefined,
          },
      ),
    ),
  saveState: (sinkName, payload) =>
    Ref.update(checkpoints, (map) => {
      const next = new Map(map);
      next.set(sinkName, {
        afterSeq: map.get(sinkName)?.afterSeq ?? SequenceNumber.make(0),
        payload,
      });
      return next;
    }),
  commit: (sinkName, afterSeq, payload) =>
    Ref.update(checkpoints, (map) => {
      const next = new Map(map);
      next.set(sinkName, { afterSeq, payload });
      return next;
    }),
});
