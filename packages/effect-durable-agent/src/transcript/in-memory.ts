import type { CommittedDurableEvent } from "../services/session-store";
import { SequenceNumber } from "../types/core";
import { planTranscriptBatch } from "./interpreter";
import type { TranscriptDefinition } from "./mapping";
import type { TranscriptAliasNamespace, TranscriptChangePlan, TranscriptLookup } from "./plan";
import {
  TranscriptOrder,
  isPendingTranscriptUserRecord,
  isTopLevelTranscriptRecord,
  type TranscriptLogIncarnation,
  type TranscriptOrderKey,
  type TranscriptRecord,
  type TranscriptRecordId,
  type TranscriptTopLevelRecord,
  type TranscriptVersion,
} from "./records";
import type { TranscriptMaintenanceConfig } from "./store";
import type { TranscriptVersionMeta } from "./window";
import type { TranscriptWindowSource } from "./window-read";

/**
 * The in-memory store's transcript state: the semantic model of the adapter's tables
 * (`transcript_records`, `transcript_aliases`, `transcript_meta`), kept inside the store's
 * `Ref.modify` critical section so maintenance commits atomically with the appended events.
 */
export interface InMemoryTranscriptVersion {
  readonly meta: TranscriptVersionMeta;
  readonly records: ReadonlyMap<TranscriptRecordId, TranscriptRecord>;
  readonly aliases: ReadonlyMap<string, TranscriptRecordId>;
  readonly ordinals: ReadonlyMap<number, number>;
}

export interface InMemoryTranscriptState {
  readonly incarnation: TranscriptLogIncarnation;
  readonly config: TranscriptMaintenanceConfig;
  readonly versions: ReadonlyArray<InMemoryTranscriptVersion>;
}

const aliasKey = (namespace: TranscriptAliasNamespace, key: string) => `${namespace}\u0000${key}`;

/** The carried definition for a stored version, if this release still carries it. */
export const carriedTranscriptDefinition = (
  config: TranscriptMaintenanceConfig,
  version: TranscriptVersion,
): TranscriptDefinition | undefined =>
  config.current.version === version
    ? config.current
    : config.previous?.version === version
      ? config.previous
      : undefined;

/** An empty version before any event is projected. */
export const emptyTranscriptVersion = (
  version: TranscriptVersion,
  role: TranscriptVersionMeta["role"],
): InMemoryTranscriptVersion => ({
  meta: { version, role, cursor: SequenceNumber.make(0), status: { _tag: "Ok" } },
  records: new Map(),
  aliases: new Map(),
  ordinals: new Map(),
});

const lookupOf = (version: InMemoryTranscriptVersion): TranscriptLookup => ({
  cursor: version.meta.cursor,
  record: (id) => version.records.get(id),
  alias: (namespace, key) => version.aliases.get(aliasKey(namespace, key)),
  nextOrdinal: (anchorSeq) => version.ordinals.get(anchorSeq) ?? 0,
});

const applyPlan = (
  version: InMemoryTranscriptVersion,
  plan: TranscriptChangePlan,
): InMemoryTranscriptVersion => {
  const records = new Map(version.records);
  const aliases = new Map(version.aliases);
  const ordinals = new Map(version.ordinals);
  for (const record of plan.records) {
    records.set(record.id, record);
    if ("position" in record && record.position !== undefined) {
      const { anchorSeq, ordinal } = record.position;
      ordinals.set(anchorSeq, Math.max(ordinals.get(anchorSeq) ?? 0, ordinal + 1));
    }
  }
  for (const alias of plan.aliases) {
    const key = aliasKey(alias.namespace, alias.key);
    if (!aliases.has(key)) aliases.set(key, alias.recordId);
  }
  return { meta: { ...version.meta, cursor: plan.throughSeq }, records, aliases, ordinals };
};

/**
 * Project committed events into one version, or freeze it.
 *
 * A projection failure marks only this version failed at its unchanged cursor and leaves its
 * rows exactly as they were, so it remains a consistent last-good cut.
 */
export const advanceTranscriptVersion = (
  version: InMemoryTranscriptVersion,
  definition: TranscriptDefinition,
  committed: ReadonlyArray<CommittedDurableEvent>,
): InMemoryTranscriptVersion => {
  const fresh = committed.filter((entry) => entry.position.seq > version.meta.cursor);
  const first = fresh[0];
  if (first === undefined || version.meta.status._tag === "Failed") return version;
  try {
    return applyPlan(version, planTranscriptBatch(lookupOf(version), fresh, definition));
  } catch (error) {
    return {
      ...version,
      meta: {
        ...version.meta,
        status: {
          _tag: "Failed",
          failedAtSeq: first.position.seq,
          error: error instanceof Error ? error.message : String(error),
        },
      },
    };
  }
};

/** Maintain every carried, healthy version for a committed append; others stay frozen. */
export const maintainTranscripts = (
  state: InMemoryTranscriptState,
  committed: ReadonlyArray<CommittedDurableEvent>,
): InMemoryTranscriptState => ({
  ...state,
  versions: state.versions.map((version) => {
    const definition = carriedTranscriptDefinition(state.config, version.meta.version);
    return definition === undefined
      ? version
      : advanceTranscriptVersion(version, definition, committed);
  }),
});

/**
 * Converge a session to the current definition, as the adapter's builder does in slices.
 *
 * A building version that is not `current` is discarded. If the active version is not `current`,
 * `current` is built from the log (never from old rows) and, once it reaches the head, becomes
 * active and the old version is deleted. A failed build leaves the active version serving; only a
 * new definition repairs it. A brand-new log simply initializes `current`.
 */
export const convergeTranscripts = (
  state: InMemoryTranscriptState,
  log: ReadonlyArray<CommittedDurableEvent>,
): InMemoryTranscriptState => {
  const current = state.config.current;
  const versions = state.versions.filter(
    (version) => version.meta.role === "active" || version.meta.version === current.version,
  );
  const active = versions.find((version) => version.meta.role === "active");
  if (active?.meta.version === current.version) return { ...state, versions };

  const building =
    versions.find((version) => version.meta.role === "building") ??
    emptyTranscriptVersion(current.version, "building");
  const built = advanceTranscriptVersion(building, current, log);
  if (built.meta.status._tag === "Failed") {
    return { ...state, versions: [...versions.filter((version) => version !== building), built] };
  }
  return { ...state, versions: [{ ...built, meta: { ...built.meta, role: "active" } }] };
};

/** Initial transcript state for a store whose log already holds `log`. */
export const initialTranscriptState = (
  incarnation: TranscriptLogIncarnation,
  config: TranscriptMaintenanceConfig,
  log: ReadonlyArray<CommittedDurableEvent>,
): InMemoryTranscriptState => convergeTranscripts({ incarnation, config, versions: [] }, log);

const topLevelOrder = (record: TranscriptTopLevelRecord): TranscriptOrderKey => ({
  anchorSeq: record.position?.anchorSeq ?? 0,
  ordinal: record.position?.ordinal ?? 0,
  recordId: record.id,
});

/** Window reads over the active version, or undefined while nothing is servable. */
export const inMemoryTranscriptWindowSource = (
  state: InMemoryTranscriptState,
): TranscriptWindowSource | undefined => {
  const active = state.versions.find((version) => version.meta.role === "active");
  if (active === undefined) return undefined;
  const records = Array.from(active.records.values());
  const ordered = records
    .filter(isTopLevelTranscriptRecord)
    .sort((left, right) => TranscriptOrder(topLevelOrder(left), topLevelOrder(right)));
  const compare = (record: TranscriptTopLevelRecord, key: TranscriptOrderKey) =>
    TranscriptOrder(topLevelOrder(record), key);
  return {
    incarnation: state.incarnation,
    meta: active.meta,
    carried: carriedTranscriptDefinition(state.config, active.meta.version) !== undefined,
    topLevelBefore: (key, limit, inclusive) =>
      ordered
        .filter(
          (record) =>
            key === undefined || (inclusive ? compare(record, key) <= 0 : compare(record, key) < 0),
        )
        .reverse()
        .slice(0, limit),
    topLevelAfter: (key, limit, inclusive) =>
      ordered
        .filter(
          (record) =>
            key === undefined || (inclusive ? compare(record, key) >= 0 : compare(record, key) > 0),
        )
        .slice(0, limit),
    record: (id) => active.records.get(id),
    alias: (namespace, key) => active.aliases.get(aliasKey(namespace, key)),
    toolsOf: (assistantId) =>
      records.flatMap((record) =>
        record.kind === "tool" && record.assistantId === assistantId ? [record] : [],
      ),
    pendingUsers: () => records.filter(isPendingTranscriptUserRecord),
  };
};
