import type { DurableObjectAlarmScheduler } from "./durable-object-alarms";

/** What one session object's alarm fires for, and the policy bounding builder retries. */
export interface EDASessionAlarmParticipants {
  readonly alarms: DurableObjectAlarmScheduler;
  readonly now: () => number;
  /** The runtime's alarm work: keep-alive and run-scheduling redelivery. */
  readonly runRuntime: () => Promise<void>;
  /**
   * Bounded builder slices; resolves whether work remains. Undefined when this object's session
   * maintains no transcript (it is not enabled, or the object is unnamed).
   */
  readonly runBuilderSlices: (() => Promise<boolean>) | undefined;
  /** Consecutive failed builder alarms before the builder stops scheduling itself. */
  readonly maxBuilderFailures: number;
  /** First retry delay after a failed builder alarm; doubled per consecutive failure. */
  readonly builderRetryDelayMs: number;
  readonly onBuilderFailure: (failure: {
    readonly error: unknown;
    readonly consecutiveFailures: number;
    readonly stopped: boolean;
  }) => void;
}

/** Longest delay between builder retries. */
const maxBuilderRetryDelayMs = 3_600_000;

/**
 * Handle one platform alarm shared by the runtime and the transcript builder.
 *
 * - The runtime runs for its own wakeup, or for an alarm no owner recorded (one armed by a
 *   release before wakeups were recorded). The builder runs for its wakeup even when the runtime
 *   failed.
 * - No wakeup is consumed up front: each owner's is cleared only after its work succeeds, so a
 *   handler that throws or dies mid-work leaves the platform's retry attributed to every owner it
 *   fired for.
 * - A failed builder retries with exponential backoff; its consecutive failures persist with its
 *   wakeup, so eviction cannot reset them, and at `maxBuilderFailures` it stops scheduling itself.
 *   No failure re-arms the alarm sooner than the backoff, so no object can loop on its alarm.
 * - A failed runtime rethrows without re-arming the platform alarm, so the platform's own retry
 *   (backoff and retry cap) applies; otherwise the alarm is pointed at the earliest wakeup left.
 */
export const runEDASessionAlarm = async (participants: EDASessionAlarmParticipants) => {
  const { alarms } = participants;
  const now = participants.now();
  const due = alarms.due(now);
  let runtimeFailure: { readonly error: unknown } | undefined;
  if (due.has("runtime") || due.size === 0) {
    try {
      await participants.runRuntime();
      alarms.clearThrough("runtime", due.get("runtime") ?? now);
    } catch (error) {
      runtimeFailure = { error };
    }
  }
  const builderWakeup = due.get("transcript");
  if (builderWakeup !== undefined) {
    const runBuilderSlices = participants.runBuilderSlices;
    if (runBuilderSlices === undefined) {
      alarms.clear("transcript");
    } else {
      try {
        if (await runBuilderSlices()) alarms.retain("transcript", participants.now());
        else alarms.clearThrough("transcript", builderWakeup);
      } catch (error) {
        const consecutiveFailures = alarms.failures("transcript") + 1;
        const stopped = consecutiveFailures >= participants.maxBuilderFailures;
        if (stopped) {
          alarms.clear("transcript");
        } else {
          const delayMs = Math.min(
            participants.builderRetryDelayMs * 2 ** (consecutiveFailures - 1),
            maxBuilderRetryDelayMs,
          );
          alarms.retain("transcript", participants.now() + delayMs, consecutiveFailures);
        }
        participants.onBuilderFailure({ error, consecutiveFailures, stopped });
      }
    }
  }
  if (runtimeFailure !== undefined) throw runtimeFailure.error;
  await alarms.sync();
};
