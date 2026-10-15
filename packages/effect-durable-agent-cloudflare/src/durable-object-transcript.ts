import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import { EDASessionStoreError } from "effect-durable-agent/services/session-store";
import { SequenceNumber, type SessionId } from "effect-durable-agent/types/core";
import {
  EDATranscriptStore,
  TranscriptLogIncarnation,
  TranscriptVersion,
  TranscriptVersionMeta,
  buildingTranscriptWindow,
  readTranscriptWindowFrom,
  type EDATranscriptStoreShape,
  type TranscriptCursorMismatch,
  type TranscriptVersionMeta as TranscriptVersionMetaType,
  type TranscriptWindow,
  type TranscriptWindowLimits,
  type TranscriptWindowSelector,
} from "effect-durable-agent/transcript";
import type { DurableObjectSessionStorage } from "./durable-object-storage";
import { makeDurableObjectEventLog } from "./durable-object-store";
import {
  carriedDefinition,
  defaultTranscriptSliceLimits,
  emptyTranscriptEffects,
  hasPendingTranscriptWork,
  missedSessionAlert,
  publishTranscriptEffects,
  reconcileTranscriptSlots,
  recordTranscriptSliceFailure,
  runTranscriptSlice,
  type DurableObjectTranscriptOptions,
  type DurableObjectTranscriptSliceLimits,
  type TranscriptAlert,
  type TranscriptEffects,
  type TranscriptEventLog,
} from "./durable-object-transcript-maintenance";
import {
  makeTranscriptWindowSource,
  readLogIncarnation,
  readOrMintLogIncarnation,
  readTranscriptSlots,
  transcriptVersionMetaOf,
  TranscriptRowDecodeError,
  type TranscriptSlot,
} from "./durable-object-transcript-tables";

/**
 * Maintenance state of one session's transcript, as a host backfill records it (§7).
 *
 * `converged` is the backfill's proof condition: the active version is `currentVersion`, healthy,
 * and no builder or cleanup work remains.
 */
export const TranscriptStorageStatus = Schema.Struct({
  /**
   * Whether the host enabled transcript maintenance for this session. A disabled session is
   * reported read-only: its rows (if any) are listed as they are and nothing is maintained.
   */
  enabled: Schema.Boolean,
  /** Absent only for a disabled session whose log has no incarnation yet; reads never mint one. */
  incarnation: Schema.optionalKey(TranscriptLogIncarnation),
  headSeq: SequenceNumber,
  currentVersion: TranscriptVersion,
  versions: Schema.Array(TranscriptVersionMeta),
  pending: Schema.Boolean,
  converged: Schema.Boolean,
});
export type TranscriptStorageStatus = typeof TranscriptStorageStatus.Type;

/** Construction options for the runtime-free transcript store of one session. */
export interface DurableObjectTranscriptStoreOptions {
  readonly sessionId: SessionId;
  readonly storage: DurableObjectSessionStorage;
  readonly transcript: DurableObjectTranscriptOptions;
}

/**
 * Storage-level transcript service of one session Durable Object: reads, status and builder
 * slices over the same SQLite tables the session store maintains in its appends.
 *
 * It never constructs the model runtime, reducers or sinks, so a cold object can serve SSR and
 * CLI reads and advance a backfill without starting an agent. It holds no cache: every call
 * reads storage, so a storage wipe or another writer is always observed. Every method is
 * synchronous, which makes each window a single cut. Callers must have run the store migration.
 */
export class DurableObjectTranscriptStore {
  readonly #storage: DurableObjectSessionStorage;
  readonly #options: DurableObjectTranscriptOptions;
  readonly #log: TranscriptEventLog;
  readonly #limits: DurableObjectTranscriptSliceLimits;

  constructor(options: DurableObjectTranscriptStoreOptions) {
    this.#storage = options.storage;
    this.#options = options.transcript;
    this.#log = makeDurableObjectEventLog(options.storage.sql, options.sessionId);
    this.#limits = { ...defaultTranscriptSliceLimits, ...options.transcript.sliceLimits };
  }

  /** The `EDATranscriptStore` port over this store. */
  static readonly layer = (options: DurableObjectTranscriptStoreOptions) =>
    Layer.sync(EDATranscriptStore, () => new DurableObjectTranscriptStore(options).port);

  /** Read one complete window of the served version at a single cut. */
  readWindow(
    selector: TranscriptWindowSelector,
    limits: TranscriptWindowLimits,
  ): Result.Result<TranscriptWindow, TranscriptCursorMismatch> {
    const { incarnation, slots } = this.#prepare();
    const active = slots.find((slot) => slot.role === "active");
    if (active === undefined) {
      return Result.succeed(
        buildingTranscriptWindow(incarnation, this.#options.definitions.current.version),
      );
    }
    const carried = carriedDefinition(this.#options.definitions, active.version) !== undefined;
    try {
      const window = readTranscriptWindowFrom(
        makeTranscriptWindowSource(this.#storage.sql, incarnation, active, carried),
        selector,
        limits,
      );
      // An active version behind the head (re-enabled after its session was disabled, or caught
      // up by the builder for any other reason) serves its consistent last cut, but never as
      // current: it reads as updating history until the builder brings it to the head.
      return Result.isSuccess(window) &&
        window.success.readiness._tag === "Ready" &&
        active.cursor < this.#log.head()
        ? Result.succeed({ ...window.success, readiness: { _tag: "UpdatingHistory" } })
        : window;
    } catch (error) {
      // Only a stored row this release cannot decode serves nothing rather than a wrong view (§7);
      // a storage failure is an error.
      if (!(error instanceof TranscriptRowDecodeError)) throw error;
      return Result.succeed({
        incarnation,
        version: active.version,
        throughSeq: active.cursor,
        records: [],
        dependencies: [],
        hasOlder: false,
        hasNewer: false,
        readiness: {
          _tag: "Unreadable",
          error: error instanceof Error ? error.message : String(error),
        },
      });
    }
  }

  /** Maintenance state of every version present. */
  readMeta(): ReadonlyArray<TranscriptVersionMetaType> {
    return this.#prepare().slots.flatMap((slot) => transcriptVersionMetaOf(slot) ?? []);
  }

  /** Status for builders, backfill proofs and alerts. */
  status(): TranscriptStorageStatus {
    const { incarnation, slots } = this.#prepare();
    const headSeq = this.#log.head();
    const config = this.#options.definitions;
    const pending = hasPendingTranscriptWork(this.#storage, config, headSeq);
    const active = slots.find((slot) => slot.role === "active");
    return {
      enabled: true,
      incarnation,
      headSeq,
      currentVersion: config.current.version,
      versions: slots.flatMap((slot) => transcriptVersionMetaOf(slot) ?? []),
      pending,
      converged:
        !pending && active?.version === config.current.version && active.status._tag === "Ok",
    };
  }

  /**
   * Status of a session the host has not enabled, read without writing anything: no version is
   * initialized or reconciled and no incarnation is minted.
   */
  disabledStatus(): TranscriptStorageStatus {
    const incarnation = readLogIncarnation(this.#storage.sql, this.#options.adoptIncarnation);
    return {
      enabled: false,
      ...(incarnation === undefined ? {} : { incarnation }),
      headSeq: this.#log.head(),
      currentVersion: this.#options.definitions.current.version,
      versions: readTranscriptSlots(this.#storage.sql).flatMap(
        (slot) => transcriptVersionMetaOf(slot) ?? [],
      ),
      pending: false,
      converged: false,
    };
  }

  /** Raise the missed-session alert if the active version is no longer carried. */
  auditMissedSession(): void {
    const alert = missedSessionAlert(this.#storage, this.#options.definitions);
    if (alert !== undefined)
      publishTranscriptEffects(this.#options.observer, { alerts: [alert], pendingWork: false });
  }

  /** Whether a builder or cleanup slice has work to do. */
  hasPendingWork(): boolean {
    return hasPendingTranscriptWork(this.#storage, this.#options.definitions, this.#log.head());
  }

  /** Run one bounded builder slice in its own transaction; returns whether work remains. */
  runSlice(): boolean {
    try {
      return this.#transact((effects) =>
        runTranscriptSlice(
          this.#storage,
          this.#options.definitions,
          this.#log,
          this.#limits,
          effects,
        ),
      );
    } catch (error) {
      // The slice rolled back. Count it against its build target, which freezes at the limit.
      if (this.#recordSliceFailure(error)) return this.hasPendingWork();
      throw error;
    }
  }

  /** Record a thrown slice in its own transaction; returns whether a version was frozen. */
  #recordSliceFailure(cause: unknown): boolean {
    try {
      return this.#transact((effects) =>
        recordTranscriptSliceFailure(
          this.#storage,
          this.#options.definitions,
          this.#log,
          this.#limits,
          cause,
          effects,
        ),
      );
    } catch (error) {
      // Storage refuses even the failure record; the caller still sees the original failure,
      // and the builder alarm's own failure count bounds its retries.
      console.error("EDA transcript could not record a failed builder slice", error);
      return false;
    }
  }

  /** Raise an alert through the host observer. */
  alert(alert: TranscriptAlert): void {
    publishTranscriptEffects(this.#options.observer, { alerts: [alert], pendingWork: false });
  }

  /** The `EDATranscriptStore` port: the same reads as Effects. */
  get port(): EDATranscriptStoreShape {
    return {
      readTranscriptWindow: (selector, limits) =>
        Effect.try({
          try: () => this.readWindow(selector, limits),
          catch: (error) => storeError("reading transcript window", error),
        }).pipe(
          Effect.flatMap((window) =>
            Result.isSuccess(window) ? Effect.succeed(window.success) : Effect.fail(window.failure),
          ),
        ),
      readTranscriptMeta: Effect.try({
        try: () => this.readMeta(),
        catch: (error) => storeError("reading transcript meta", error),
      }),
    };
  }

  /**
   * Reconcile versions (a brand-new log initializes `current` at seq 0 before its first read)
   * and read the incarnation and slots, all in one transaction.
   */
  #prepare(): {
    readonly incarnation: TranscriptLogIncarnation;
    readonly slots: ReadonlyArray<TranscriptSlot>;
  } {
    return this.#transact((effects) => {
      reconcileTranscriptSlots(this.#storage, this.#options.definitions, this.#log.head(), effects);
      return {
        incarnation: readOrMintLogIncarnation(this.#storage.sql, this.#options.adoptIncarnation),
        slots: readTranscriptSlots(this.#storage.sql),
      };
    });
  }

  #transact<A>(body: (effects: TranscriptEffects) => A): A {
    const effects = emptyTranscriptEffects();
    const result = this.#storage.transactionSync(() => body(effects));
    publishTranscriptEffects(this.#options.observer, effects);
    return result;
  }
}

const storeError = (operation: string, cause: unknown) =>
  new EDASessionStoreError({
    message: `${operation}: ${cause instanceof Error ? cause.message : String(cause)}`,
  });

/**
 * Drives builder and cleanup slices for one session object (§7 "The builder").
 *
 * Builder state lives entirely in `_eda_transcript_meta`, so whichever entry point notices
 * pending work — the constructor, an alarm, a status request — runs or schedules the next slice;
 * nothing depends on in-memory state surviving eviction. Slices run through the storage service
 * only and never construct the model runtime.
 */
export class DurableObjectTranscriptBuilder {
  constructor(
    private readonly store: DurableObjectTranscriptStore,
    /** Durably request a builder wakeup as soon as possible. */
    private readonly wake: () => Promise<void>,
  ) {}

  /** At object start: raise standing alerts and schedule a slice if any work is pending. */
  async start(): Promise<void> {
    this.store.auditMissedSession();
    await this.kick();
  }

  /** Schedule a slice if any work is pending. */
  async kick(): Promise<void> {
    if (this.store.hasPendingWork()) await this.wake();
  }

  /**
   * Run up to `maxSlices` slices, letting other requests in between them, and schedule a
   * continuation if work remains. Returns whether work remains.
   */
  async run(maxSlices: number): Promise<boolean> {
    const more = await this.runSlices(maxSlices);
    if (more) await this.wake();
    return more;
  }

  /** Run up to `maxSlices` slices without scheduling a continuation; returns whether work remains. */
  async runSlices(maxSlices: number): Promise<boolean> {
    for (let slice = 0; slice < maxSlices; slice += 1) {
      if (slice > 0) await new Promise((resolve) => setTimeout(resolve, 0));
      if (!this.store.runSlice()) return false;
    }
    return true;
  }
}
