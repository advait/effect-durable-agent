import type { CommittedDurableEvent } from "effect-durable-agent/services/session-store";
import type { SequenceNumber } from "effect-durable-agent/types/core";
import {
  planTranscriptBatch,
  type TranscriptDefinition,
  type TranscriptMaintenanceConfig,
  type TranscriptVersion,
} from "effect-durable-agent/transcript";
import type { DurableObjectSessionStorage } from "./durable-object-storage";
import {
  deleteRetiredTranscriptRows,
  insertTranscriptSlot,
  makeTranscriptLookup,
  markTranscriptSlotFailed,
  readTranscriptSlots,
  setTranscriptSlotRole,
  writeTranscriptPlan,
  type AdoptedLogIncarnation,
  type TranscriptSlot,
} from "./durable-object-transcript-tables";

/**
 * Something an operator must look at, raised after the transaction that caused it commits.
 *
 * - `ProjectionFailed`: a version froze at its last good cursor (§5.2). On the active version
 *   readers see a degraded tail until a new definition rebuilds it; on a building version the
 *   active version keeps serving and the backfill reports it.
 * - `MissedSession`: the active version is one this release no longer carries, so its history is
 *   frozen behind the log and `current` is being built at once (§7 safety net).
 */
export type TranscriptAlert =
  | {
      readonly _tag: "ProjectionFailed";
      readonly version: TranscriptVersion;
      readonly role: "active" | "building";
      readonly failedAtSeq: SequenceNumber;
      readonly error: string;
    }
  | {
      readonly _tag: "MissedSession";
      readonly activeVersion: TranscriptVersion;
      readonly currentVersion: TranscriptVersion;
    };

/**
 * Host hooks for transcript maintenance. Every hook runs only after the transaction that caused
 * it has committed, so nothing observed here can be rolled back afterwards.
 */
export interface DurableObjectTranscriptObserver {
  readonly onAlert?: (alert: TranscriptAlert) => void;
  /** A built version became active; connected clients need a window replacement (§9.4). */
  readonly onActivated?: (activation: {
    readonly version: TranscriptVersion;
    readonly previousVersion: TranscriptVersion | undefined;
  }) => void;
  /** Bounded builder or cleanup work is pending; the host schedules a slice. */
  readonly onPendingWork?: () => void;
}

/** Bounds on one builder slice. */
export interface DurableObjectTranscriptSliceLimits {
  /** Log events projected into the building version per slice. */
  readonly maxEvents: number;
  /** Events decoded and planned together; matches the store's bounded event read page. */
  readonly pageEvents: number;
  /** Rows of retired versions deleted per slice. */
  readonly maxDeletedRows: number;
}

export const defaultTranscriptSliceLimits: DurableObjectTranscriptSliceLimits = {
  maxEvents: 512,
  pageEvents: 16,
  maxDeletedRows: 2_000,
};

/** Transcript maintenance configuration of one session store. */
export interface DurableObjectTranscriptOptions {
  /** The definitions this release carries; supplied by the host. */
  readonly definitions: TranscriptMaintenanceConfig;
  /** Adopt a host's existing log incarnation instead of minting a new one. */
  readonly adoptIncarnation?: AdoptedLogIncarnation;
  readonly observer?: DurableObjectTranscriptObserver;
  readonly sliceLimits?: Partial<DurableObjectTranscriptSliceLimits>;
}

/** Synchronous reads of the committed event log a builder needs. */
export interface TranscriptEventLog {
  readonly head: () => SequenceNumber;
  /** Up to `limit` committed events after `afterSeq`, in seq order. */
  readonly after: (afterSeq: SequenceNumber, limit: number) => ReadonlyArray<CommittedDurableEvent>;
}

/** Outcomes collected inside a transaction and published only after it commits. */
export interface TranscriptEffects {
  readonly alerts: Array<TranscriptAlert>;
  activated?: {
    readonly version: TranscriptVersion;
    readonly previousVersion: TranscriptVersion | undefined;
  };
  pendingWork: boolean;
}

export const emptyTranscriptEffects = (): TranscriptEffects => ({
  alerts: [],
  pendingWork: false,
});

/**
 * Publish committed outcomes to the host. Call only after the outer transaction returns. A hook
 * that throws is reported and otherwise ignored: the work it describes is already committed.
 */
export const publishTranscriptEffects = (
  observer: DurableObjectTranscriptObserver | undefined,
  effects: TranscriptEffects,
): void => {
  const notify = (hook: () => void) => {
    try {
      hook();
    } catch (error) {
      console.error("EDA transcript observer failed", error);
    }
  };
  for (const alert of effects.alerts) notify(() => observer?.onAlert?.(alert));
  const activated = effects.activated;
  if (activated !== undefined) notify(() => observer?.onActivated?.(activated));
  if (effects.pendingWork) notify(() => observer?.onPendingWork?.());
};

/** The carried definition for a stored version, if this release still carries it. */
export const carriedDefinition = (
  config: TranscriptMaintenanceConfig,
  version: TranscriptVersion,
): TranscriptDefinition | undefined =>
  config.current.version === version
    ? config.current
    : config.previous?.version === version
      ? config.previous
      : undefined;

const liveSlots = (storage: DurableObjectSessionStorage): ReadonlyArray<TranscriptSlot> =>
  readTranscriptSlots(storage.sql).filter((slot) => slot.role !== "retired");

/**
 * Bring the stored versions in line with this release's definitions (§7 steps 2, 3 and 5),
 * writing only metadata. Rows are never touched here: building and cleanup happen in slices.
 *
 * - No version yet: an empty log starts `current` as active at seq 0; a log written before
 *   transcripts existed builds `current` from the log while nothing is served.
 * - A building version that is not `current` is retired (discarded).
 * - If the active version is not `current` and no build of `current` exists, one starts. A build
 *   of `current` that failed counts as existing, so it is never retried.
 */
export const reconcileTranscriptSlots = (
  storage: DurableObjectSessionStorage,
  config: TranscriptMaintenanceConfig,
  head: SequenceNumber,
  effects: TranscriptEffects,
): void => {
  const { sql } = storage;
  const current = config.current.version;
  const slots = liveSlots(storage);
  if (slots.length === 0) {
    insertTranscriptSlot(sql, current, head === 0 ? "active" : "building");
    if (head > 0) effects.pendingWork = true;
    return;
  }
  for (const slot of slots) {
    if (slot.role === "building" && slot.version !== current) {
      setTranscriptSlotRole(sql, slot.slot, "retired");
      effects.pendingWork = true;
    }
  }
  const active = slots.find((slot) => slot.role === "active");
  const buildingCurrent = slots.some(
    (slot) => slot.role === "building" && slot.version === current,
  );
  if (active?.version !== current && !buildingCurrent) {
    insertTranscriptSlot(sql, current, "building");
    effects.pendingWork = true;
    if (active !== undefined && carriedDefinition(config, active.version) === undefined) {
      effects.alerts.push({
        _tag: "MissedSession",
        activeVersion: active.version,
        currentVersion: current,
      });
    }
  }
};

/**
 * Project committed events into one slot inside an inner transaction, or freeze the slot.
 *
 * Planning, row decoding and every SQL write run inside the inner `transactionSync`. Any throw
 * rolls back only that inner transaction; the failure is recorded outside it, in the caller's
 * transaction, so the events the caller appended still commit (§5.2).
 */
const advanceSlot = (
  storage: DurableObjectSessionStorage,
  slot: TranscriptSlot,
  definition: TranscriptDefinition,
  events: ReadonlyArray<CommittedDurableEvent>,
  effects: TranscriptEffects,
): TranscriptSlot => {
  const first = events.find((entry) => entry.position.seq > slot.cursor);
  if (first === undefined || slot.status._tag === "Failed" || slot.role === "retired") return slot;
  try {
    const throughSeq = storage.transactionSync(() => {
      const plan = planTranscriptBatch(makeTranscriptLookup(storage.sql, slot), events, definition);
      writeTranscriptPlan(storage.sql, slot.slot, plan);
      return plan.throughSeq;
    });
    return { ...slot, cursor: throughSeq };
  } catch (cause) {
    const error = cause instanceof Error ? cause.message : String(cause);
    const failedAtSeq = first.position.seq;
    markTranscriptSlotFailed(storage.sql, slot.slot, failedAtSeq, error);
    effects.alerts.push({
      _tag: "ProjectionFailed",
      version: slot.version,
      role: slot.role,
      failedAtSeq,
      error,
    });
    return { ...slot, status: { _tag: "Failed", failedAtSeq, error } };
  }
};

/**
 * The append hook: one batch-level call inside the append transaction, after every entry has
 * been inserted.
 *
 * A version is maintained here exactly when it is carried, healthy and caught up — its cursor is
 * the head the batch started from — so the new events are precisely what it is missing. Anything
 * behind (a building version) is caught up by builder slices instead, and frozen versions are
 * never touched. Each version fails independently.
 */
export const maintainTranscriptsOnAppend = (
  storage: DurableObjectSessionStorage,
  config: TranscriptMaintenanceConfig,
  headBeforeBatch: SequenceNumber,
  appended: ReadonlyArray<CommittedDurableEvent>,
  effects: TranscriptEffects,
): void => {
  reconcileTranscriptSlots(storage, config, headBeforeBatch, effects);
  if (appended.length === 0) return;
  for (const slot of liveSlots(storage)) {
    const definition = carriedDefinition(config, slot.version);
    if (definition === undefined || slot.status._tag === "Failed") continue;
    if (slot.cursor !== headBeforeBatch) continue;
    advanceSlot(storage, slot, definition, appended, effects);
  }
};

/** The one slot a builder slice advances: the building version, else an active one behind. */
const buildTarget = (
  config: TranscriptMaintenanceConfig,
  slots: ReadonlyArray<TranscriptSlot>,
  head: SequenceNumber,
): TranscriptSlot | undefined => {
  const healthy = (slot: TranscriptSlot) =>
    slot.status._tag === "Ok" && carriedDefinition(config, slot.version) !== undefined;
  return (
    slots.find((slot) => slot.role === "building" && healthy(slot)) ??
    slots.find((slot) => slot.role === "active" && healthy(slot) && slot.cursor < head)
  );
};

/** Whether any builder or cleanup slice has work to do. Read-only. */
export const hasPendingTranscriptWork = (
  storage: DurableObjectSessionStorage,
  config: TranscriptMaintenanceConfig,
  head: SequenceNumber,
): boolean => {
  const slots = readTranscriptSlots(storage.sql);
  if (slots.some((slot) => slot.role === "retired")) return true;
  const live = slots.filter((slot) => slot.role !== "retired");
  if (live.length === 0) return true;
  const current = config.current.version;
  if (live.some((slot) => slot.role === "building" && slot.version !== current)) return true;
  const active = live.find((slot) => slot.role === "active");
  if (
    active?.version !== current &&
    !live.some((slot) => slot.role === "building" && slot.version === current)
  ) {
    return true;
  }
  return buildTarget(config, live, head) !== undefined;
};

/**
 * One bounded builder slice. The caller runs it inside one outer `transactionSync`.
 *
 * It reconciles versions, projects at most `maxEvents` log events into the building version
 * (page by page, each page's rows and cursor committed together, so an eviction resumes from the
 * cursor), activates the build when its cursor equals the head re-read in this transaction, and
 * deletes a bounded number of retired rows. Returns whether more work remains.
 */
export const runTranscriptSlice = (
  storage: DurableObjectSessionStorage,
  config: TranscriptMaintenanceConfig,
  log: TranscriptEventLog,
  limits: DurableObjectTranscriptSliceLimits,
  effects: TranscriptEffects,
): boolean => {
  const { sql } = storage;
  const head = log.head();
  reconcileTranscriptSlots(storage, config, head, effects);
  const slots = liveSlots(storage);
  const target = buildTarget(config, slots, head);
  if (target !== undefined) {
    const definition = carriedDefinition(config, target.version);
    let slot = target;
    let remaining = limits.maxEvents;
    while (
      definition !== undefined &&
      slot.status._tag === "Ok" &&
      slot.cursor < head &&
      remaining > 0
    ) {
      const page = log.after(slot.cursor, Math.min(limits.pageEvents, remaining));
      if (page.length === 0) break;
      remaining -= page.length;
      slot = advanceSlot(storage, slot, definition, page, effects);
    }
    if (slot.role === "building" && slot.status._tag === "Ok" && slot.cursor === log.head()) {
      const previous = slots.find((candidate) => candidate.role === "active");
      if (previous !== undefined) setTranscriptSlotRole(sql, previous.slot, "retired");
      setTranscriptSlotRole(sql, slot.slot, "active");
      effects.activated = { version: slot.version, previousVersion: previous?.version };
    }
  }
  let deleteBudget = limits.maxDeletedRows;
  for (const slot of readTranscriptSlots(sql)) {
    if (deleteBudget <= 0) break;
    if (slot.role === "retired") {
      deleteBudget -= deleteRetiredTranscriptRows(sql, slot.slot, deleteBudget);
    }
  }
  return hasPendingTranscriptWork(storage, config, log.head());
};
