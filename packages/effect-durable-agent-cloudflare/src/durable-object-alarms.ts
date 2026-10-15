import type { DurableObjectAlarmStorage } from "./durable-object-keepalive";
import type { DurableObjectSessionStorage } from "./durable-object-storage";

/**
 * The independent owners of a session object's single platform alarm.
 *
 * - `runtime`: the keep-alive heartbeat and run-scheduling redelivery; its wakeup builds the model
 *   runtime.
 * - `transcript`: the transcript builder; its wakeup runs storage-only slices and must never
 *   build the model runtime.
 */
export type DurableObjectAlarmOwner = "runtime" | "transcript";

/**
 * Shares one Durable Object alarm between owners that each keep a durable wakeup.
 *
 * Each owner's wakeup is a row in `_eda_alarm_wakeups`, and the platform alarm is always set to the
 * earliest row, so no owner can delete or postpone another's wakeup, and after an eviction the
 * alarm handler still knows which owners it fired for. An owner's view (`owner(...)`) has the
 * same shape as the platform alarm API, so the keep-alive heartbeat uses it unchanged.
 */
export class DurableObjectAlarmScheduler {
  #tail: Promise<void> = Promise.resolve();

  constructor(
    private readonly storage: DurableObjectSessionStorage & DurableObjectAlarmStorage,
    private readonly now: () => number = () => Date.now(),
  ) {}

  /**
   * One owner's alarm, behaving like the platform alarm restricted to that owner. Like the
   * platform's, it reports no alarm once its time has passed: a wakeup left behind when the
   * platform gave up retrying a failed alarm must not look armed, or its owner would never re-arm.
   */
  owner(owner: DurableObjectAlarmOwner): DurableObjectAlarmStorage {
    return {
      getAlarm: () => {
        const wakeup = this.#wakeup(owner);
        return wakeup === undefined || wakeup < this.now() ? null : wakeup;
      },
      setAlarm: (scheduledTimeMs) => {
        this.#write(owner, scheduledTimeMs);
        return this.sync();
      },
      deleteAlarm: () => {
        this.#write(owner, undefined);
        return this.sync();
      },
    };
  }

  /**
   * Adopt a platform alarm no owner recorded (one armed before wakeups were recorded) as the
   * runtime's, so the first wakeup another owner requests cannot replace it. Call once at startup,
   * before any owner writes a wakeup.
   */
  async adoptUnrecordedAlarm(): Promise<void> {
    if (this.#rows().length > 0) return;
    const armed = await this.storage.getAlarm();
    if (armed !== null && this.#rows().length === 0) this.#write("runtime", armed);
  }

  /**
   * Record `owner`'s wakeup without touching the platform alarm, for use inside `alarm()`: the
   * caller syncs once at the end, or not at all when it rethrows, which leaves the platform's own
   * retry (with its backoff and cap) in force.
   */
  retain(owner: DurableObjectAlarmOwner, atMs: number): void {
    this.#write(owner, atMs);
  }

  /** Wake `owner` no later than `atMs`; an earlier wakeup it already holds is kept. */
  request(owner: DurableObjectAlarmOwner, atMs: number): Promise<void> {
    const current = this.#wakeup(owner);
    if (current !== undefined && current <= atMs) return this.sync();
    this.#write(owner, atMs);
    return this.sync();
  }

  /**
   * Called when the platform alarm fires: the owners it fired for (every wakeup at or before now,
   * and always the earliest one) and the wakeup each held. Nothing is consumed here: each owner
   * `clearThrough`s its wakeup only after its work succeeds, so a handler that throws, or dies
   * mid-work (eviction, deploy, a resource limit), leaves the platform's retry attributed to every
   * owner it fired for. An owner's view already reports a past wakeup as no alarm, so the runtime's
   * keep-alive re-arms during its work as it did with the platform alarm.
   */
  due(nowMs: number): ReadonlyMap<DurableObjectAlarmOwner, number> {
    const rows = this.#rows();
    const earliest = rows.reduce((min, row) => Math.min(min, row.at_ms), Number.POSITIVE_INFINITY);
    const threshold = Math.max(nowMs, earliest);
    const due = new Map<DurableObjectAlarmOwner, number>();
    for (const row of rows) {
      if (row.at_ms <= threshold && isOwner(row.owner)) due.set(row.owner, row.at_ms);
    }
    return due;
  }

  /**
   * Remove `owner`'s wakeup if it is still at or before `atMs`, once the work it fired for has
   * succeeded. A later wakeup the owner re-armed during that work is kept.
   */
  clearThrough(owner: DurableObjectAlarmOwner, atMs: number): void {
    this.storage.sql.exec(
      "DELETE FROM _eda_alarm_wakeups WHERE owner = ? AND at_ms <= ?",
      owner,
      atMs,
    );
  }

  /** Point the platform alarm at the earliest wakeup, or clear it when none remains. */
  sync(): Promise<void> {
    this.#tail = this.#tail
      .catch(() => undefined)
      .then(async () => {
        const earliest = this.storage.sql
          .exec<{ readonly at_ms: number | null }>(
            "SELECT MIN(at_ms) AS at_ms FROM _eda_alarm_wakeups",
          )
          .one().at_ms;
        if (earliest === null) {
          await this.storage.deleteAlarm();
          return;
        }
        if ((await this.storage.getAlarm()) !== earliest) {
          await this.storage.setAlarm(earliest);
        }
      });
    return this.#tail;
  }

  #rows(): ReadonlyArray<{ readonly owner: string; readonly at_ms: number }> {
    return this.storage.sql
      .exec<{ readonly owner: string; readonly at_ms: number }>(
        "SELECT owner, at_ms FROM _eda_alarm_wakeups",
      )
      .toArray();
  }

  #wakeup(owner: DurableObjectAlarmOwner): number | undefined {
    return this.storage.sql
      .exec<{ readonly at_ms: number }>(
        "SELECT at_ms FROM _eda_alarm_wakeups WHERE owner = ?",
        owner,
      )
      .toArray()[0]?.at_ms;
  }

  #write(owner: DurableObjectAlarmOwner, atMs: number | undefined): void {
    if (atMs === undefined) {
      this.storage.sql.exec("DELETE FROM _eda_alarm_wakeups WHERE owner = ?", owner);
      return;
    }
    this.storage.sql.exec(
      `INSERT INTO _eda_alarm_wakeups (owner, at_ms) VALUES (?, ?)
      ON CONFLICT(owner) DO UPDATE SET at_ms = excluded.at_ms`,
      owner,
      atMs,
    );
  }
}

const isOwner = (owner: string): owner is DurableObjectAlarmOwner =>
  owner === "runtime" || owner === "transcript";
