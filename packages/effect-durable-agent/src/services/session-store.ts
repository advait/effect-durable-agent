import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { type CommandIdempotencyKey, type EDACommand } from "../types/commands";
import { CompactionSummaryArtifact } from "../domain/context-projection";
import {
  CommandId,
  DurablePosition,
  SequenceNumber,
  SessionId,
  SummaryId,
  durablePosition,
} from "../types/core";
import {
  DurableEventEnvelope,
  commandAdmittedEventType,
  contextRebasedEventType,
  summaryCreatedEventType,
} from "../types/events";

/** A durable event after the store has assigned its replay position. */
export const CommittedDurableEvent = Schema.Struct({
  position: DurablePosition,
  event: DurableEventEnvelope,
});
export type CommittedDurableEvent = typeof CommittedDurableEvent.Type;

/**
 * Store-level failure surfaced by durable append, replay, and hydration APIs.
 *
 * The error intentionally stays storage-semantic rather than SQL-specific so
 * framework services can treat SQL-backed hosts, in-memory tests, and future adapters
 * as the same durability boundary.
 */
export class EDASessionStoreError extends Schema.TaggedErrorClass<EDASessionStoreError>()(
  "EDASessionStoreError",
  {
    message: Schema.String,
  },
) {}

/** True when a Cause contains an escaped durable-store failure. */
export const hasEDASessionStoreError = (cause: Cause.Cause<unknown>): boolean =>
  cause.reasons.some(
    (reason) => Cause.isFailReason(reason) && reason.error instanceof EDASessionStoreError,
  );

/**
 * One logical event to persist in the durable session log.
 *
 * Store adapters may split large logical payload fields into sidecar rows after
 * the event insert assigns `seq`, but callers still provide one fat event.
 */
export const DurableAppendEntry = Schema.Struct({
  event: DurableEventEnvelope,
});
export type DurableAppendEntry = typeof DurableAppendEntry.Type;

/**
 * Ordered durable append intent. Adapters assign positions in entry order.
 *
 * This is the single write shape exposed to framework services. It avoids raw
 * SQL and public transaction callbacks while still expressing the full atomic
 * transition EDA needs for events, normalized bodies, and metadata.
 */
export const DurableAppendBatch = Schema.Struct({
  entries: Schema.Array(DurableAppendEntry),
});
export type DurableAppendBatch = typeof DurableAppendBatch.Type;

/** Durable reducer checkpoint row used to hydrate framework and app projections cheaply. */
export interface EDAReducerCheckpoint {
  readonly name: string;
  readonly schemaVersion: number;
  readonly throughSeq: SequenceNumber;
  readonly payload: unknown;
  readonly updatedAtMs: number;
}

/** Input for writing one durable reducer checkpoint. */
export interface SaveReducerCheckpointInput {
  readonly name: string;
  readonly schemaVersion: number;
  readonly throughSeq: SequenceNumber;
  readonly payload: unknown;
  readonly updatedAtMs: number;
}

/** Indexed command-admission lookup used for idempotent ingress. */
export const FindCommandAdmissionInput = Schema.Struct({
  commandId: Schema.optionalKey(CommandId),
  idempotencyKey: Schema.optionalKey(Schema.String),
});
/** Typed command-admission lookup accepted by the semantic store port. */
export interface FindCommandAdmissionInput {
  readonly commandId?: CommandId;
  readonly idempotencyKey?: CommandIdempotencyKey;
}

/** Semantic durable store contract for one EDA session. */
export interface EDASessionStoreShape {
  /**
   * Append durable logical events atomically, preserving input order.
   *
   * Implementations must be idempotent by `eventId`: duplicates return the
   * original committed position and do not advance the durable head. Live publish
   * remains outside the store and is owned by `SessionState` after this succeeds.
   */
  readonly append: (
    batch: DurableAppendBatch,
  ) => Effect.Effect<ReadonlyArray<CommittedDurableEvent>, EDASessionStoreError>;
  /** Replay committed durable events with `seq > afterSeq`. */
  readonly eventsAfter: (
    afterSeq: SequenceNumber,
  ) => Stream.Stream<CommittedDurableEvent, EDASessionStoreError>;
  /** Load committed durable events by exact sequence number for checkpoint pointer hydration. */
  readonly loadCommittedEventsBySeq: (
    seqs: ReadonlyArray<SequenceNumber>,
  ) => Effect.Effect<ReadonlyArray<CommittedDurableEvent>, EDASessionStoreError>;
  /** Lookup an admitted command without replaying the full journal. */
  readonly findCommandAdmission: (
    input: FindCommandAdmissionInput,
  ) => Effect.Effect<CommittedDurableEvent | undefined, EDASessionStoreError>;
  /** Load one compacted summary artifact body by id, if it has been saved. */
  readonly loadSummaryArtifact: (
    summaryId: SummaryId,
  ) => Effect.Effect<CompactionSummaryArtifact | undefined, EDASessionStoreError>;
  /** Load one framework or app reducer checkpoint, if it has been saved. */
  readonly loadReducerCheckpoint: (
    name: string,
  ) => Effect.Effect<EDAReducerCheckpoint | undefined, EDASessionStoreError>;
  /** Persist one framework or app reducer checkpoint as a derived cache. */
  readonly saveReducerCheckpoint: (
    checkpoint: SaveReducerCheckpointInput,
  ) => Effect.Effect<void, EDASessionStoreError>;
  /** Persist a reducer checkpoint snapshot atomically across framework and app reducers. */
  readonly saveReducerCheckpoints: (
    checkpoints: ReadonlyArray<SaveReducerCheckpointInput>,
  ) => Effect.Effect<void, EDASessionStoreError>;
}

/**
 * SQL-agnostic durable storage port for one EDA session.
 *
 * Framework services depend on this semantic port rather than raw SQL. Host
 * adapters own physical tables and synchronous transaction mechanics; callers
 * only express durable append/replay/lookup intent.
 */
export class EDASessionStore extends Context.Service<EDASessionStore, EDASessionStoreShape>()(
  "@effect-durable-agent/EDASessionStore",
) {
  /** Deterministic in-memory implementation used by runtime and recovery tests. */
  static readonly InMemory = (sessionId: SessionId) =>
    Layer.effect(
      EDASessionStore,
      Effect.gen(function* () {
        const data = yield* Ref.make<InMemoryStoreData>(emptyInMemoryStoreData());
        return makeInMemoryStore(sessionId, data);
      }),
    );

  /** In-memory store seeded with a durable prefix; intended for recovery tests. */
  static readonly InMemorySeeded = (
    sessionId: SessionId,
    events: ReadonlyArray<DurableEventEnvelope>,
  ) =>
    Layer.effect(
      EDASessionStore,
      Effect.gen(function* () {
        const committed = seedCommittedEvents(sessionId, events);
        const data = yield* Ref.make<InMemoryStoreData>({
          ...emptyInMemoryStoreData(),
          committed,
          summaries: summariesFromCommittedEvents(committed),
        });
        return makeInMemoryStore(sessionId, data);
      }),
    );
}

interface InMemoryStoreData {
  readonly committed: ReadonlyArray<CommittedDurableEvent>;
  readonly summaries: ReadonlyMap<SummaryId, CompactionSummaryArtifact>;
  readonly reducerCheckpoints: ReadonlyMap<string, EDAReducerCheckpoint>;
}

const emptyInMemoryStoreData = (): InMemoryStoreData => ({
  committed: [],
  summaries: new Map(),
  reducerCheckpoints: new Map(),
});

const seedCommittedEvents = (
  sessionId: SessionId,
  events: ReadonlyArray<DurableEventEnvelope>,
): ReadonlyArray<CommittedDurableEvent> => {
  const seen = new Set<string>();
  return events.map((event, index) => {
    if (event.sessionId !== sessionId) {
      throw new Error(
        `EDASessionStore.InMemorySeeded is scoped to session ${sessionId}; received ${event.sessionId}`,
      );
    }
    if (seen.has(event.eventId)) {
      throw new Error(`Duplicate seed eventId ${event.eventId}`);
    }
    seen.add(event.eventId);
    return CommittedDurableEvent.make({
      position: durablePosition(SequenceNumber.make(index + 1)),
      event,
    });
  });
};

const summariesFromCommittedEvents = (
  events: ReadonlyArray<CommittedDurableEvent>,
): ReadonlyMap<SummaryId, CompactionSummaryArtifact> => {
  const summaries = new Map<SummaryId, CompactionSummaryArtifact>();
  for (const { event } of events) {
    if (event.type !== summaryCreatedEventType) {
      continue;
    }
    const payload = event.payload as { readonly summary?: unknown };
    if (payload.summary !== undefined) {
      const summary = CompactionSummaryArtifact.make(payload.summary as never);
      summaries.set(summary.summaryId, summary);
    }
  }
  return summaries;
};

/**
 * Build an in-memory store with one `Ref.modify` append critical section.
 *
 * The fake models the semantic atomicity of the production adapter without
 * exposing SQL, so runtime tests exercise the same store contract.
 */
const makeInMemoryStore = (
  sessionId: SessionId,
  data: Ref.Ref<InMemoryStoreData>,
): EDASessionStoreShape => {
  const ensureSession = (event: DurableEventEnvelope): Effect.Effect<void, EDASessionStoreError> =>
    Effect.gen(function* () {
      if (event.sessionId === sessionId) {
        return;
      }
      return yield* new EDASessionStoreError({
        message: `EDASessionStore.InMemory is scoped to session ${sessionId}; received ${event.sessionId}`,
      });
    });

  const append = (batch: DurableAppendBatch) =>
    Effect.gen(function* () {
      for (const entry of batch.entries) {
        yield* ensureSession(entry.event);
      }

      return yield* Ref.modify(data, (current) => {
        const nextEntries: Array<CommittedDurableEvent> = [...current.committed];
        const nextSummaries = new Map(current.summaries);
        const committedEvents: Array<CommittedDurableEvent> = [];

        for (const entry of batch.entries) {
          const { event } = entry;
          const existing = nextEntries.find((entry) => entry.event.eventId === event.eventId);
          if (existing !== undefined) {
            committedEvents.push(existing);
            continue;
          }

          const committed = CommittedDurableEvent.make({
            position: durablePosition(SequenceNumber.make(nextEntries.length + 1)),
            event,
          });
          nextEntries.push(committed);
          committedEvents.push(committed);

          if (event.type === summaryCreatedEventType) {
            const payload = event.payload as {
              readonly summary?: unknown;
              readonly summaryId?: CompactionSummaryArtifact["summaryId"];
            };
            if (payload.summary === undefined) {
              throw new Error("SummaryCreated requires summary payload");
            }
            const summary = CompactionSummaryArtifact.make(payload.summary as never);
            if (payload.summaryId !== undefined && summary.summaryId !== payload.summaryId) {
              throw new Error("SummaryCreated summaryId does not match summary payload");
            }
            nextSummaries.set(summary.summaryId, summary);
          }

          if (event.type === contextRebasedEventType) {
            const payload = event.payload as { readonly summaryId?: SummaryId };
            if (payload.summaryId === undefined || !nextSummaries.has(payload.summaryId)) {
              throw new Error(
                `ContextRebased references unknown summaryId ${String(payload.summaryId)}`,
              );
            }
          }
        }

        return [
          committedEvents,
          {
            committed: nextEntries,
            summaries: nextSummaries,
            reducerCheckpoints: current.reducerCheckpoints,
          },
        ];
      });
    });

  return {
    append,
    eventsAfter: (afterSeq) =>
      Stream.fromIterableEffect(
        Ref.get(data).pipe(
          Effect.map(({ committed }) => committed.filter((entry) => entry.position.seq > afterSeq)),
        ),
      ),
    loadCommittedEventsBySeq: (seqs) =>
      Ref.get(data).pipe(
        Effect.map(({ committed }) => {
          const requested = new Set(seqs.map((seq) => Number(seq)));
          return committed.filter((entry) => requested.has(Number(entry.position.seq)));
        }),
      ),
    findCommandAdmission: (input) =>
      Ref.get(data).pipe(
        Effect.map(({ committed }) =>
          committed.find((entry) => commandAdmissionMatches(entry, input)),
        ),
      ),
    loadSummaryArtifact: (summaryId) =>
      Ref.get(data).pipe(Effect.map(({ summaries }) => summaries.get(summaryId))),
    loadReducerCheckpoint: (name) =>
      Ref.get(data).pipe(Effect.map(({ reducerCheckpoints }) => reducerCheckpoints.get(name))),
    saveReducerCheckpoint: (checkpoint) =>
      Ref.update(data, (current) => ({
        ...current,
        reducerCheckpoints: new Map(current.reducerCheckpoints).set(checkpoint.name, checkpoint),
      })),
    saveReducerCheckpoints: (checkpoints) =>
      Ref.update(data, (current) => {
        const reducerCheckpoints = new Map(current.reducerCheckpoints);
        for (const checkpoint of checkpoints) {
          reducerCheckpoints.set(checkpoint.name, checkpoint);
        }
        return { ...current, reducerCheckpoints };
      }),
  };
};

const commandAdmissionMatches = (
  entry: CommittedDurableEvent,
  input: FindCommandAdmissionInput,
): boolean => {
  if (entry.event.type !== commandAdmittedEventType) {
    return false;
  }
  const admitted = (entry.event.payload as { readonly command?: EDACommand }).command;
  if (admitted === undefined) {
    return false;
  }
  if (input.idempotencyKey !== undefined && admitted.idempotencyKey === input.idempotencyKey) {
    return true;
  }
  return input.commandId !== undefined && admitted.commandId === input.commandId;
};
