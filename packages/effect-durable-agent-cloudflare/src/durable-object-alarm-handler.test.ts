import * as Effect from "effect/Effect";
import { describe, expect, it } from "vite-plus/test";

import {
  runEDASessionAlarm,
  type EDASessionAlarmParticipants,
} from "./durable-object-alarm-handler";
import { DurableObjectAlarmScheduler } from "./durable-object-alarms";
import { DurableObjectSessionStore } from "./durable-object-store";
import { SqliteDurableObjectStorage } from "./testing/sqlite-durable-object-storage";

/**
 * One object's alarm over real SQLite. `storage.alarm` is the platform alarm; as inside a real
 * `alarm()` handler, it reads `null` while the handler runs.
 */
const harness = () => {
  const storage = new SqliteDurableObjectStorage();
  Effect.runSync(DurableObjectSessionStore.migrate(storage));
  const clock = { now: 0 };
  const alarms = new DurableObjectAlarmScheduler(storage, () => clock.now);
  const calls: Array<string> = [];
  const failures: Array<{ readonly consecutiveFailures: number; readonly stopped: boolean }> = [];
  const fire = (overrides: Partial<EDASessionAlarmParticipants> = {}) => {
    storage.alarm = null;
    return runEDASessionAlarm({
      alarms,
      now: () => clock.now,
      runRuntime: async () => {
        calls.push("runtime");
      },
      runBuilderSlices: async () => {
        calls.push("builder");
        return false;
      },
      maxBuilderFailures: 3,
      builderRetryDelayMs: 1_000,
      onBuilderFailure: ({ consecutiveFailures, stopped }) =>
        failures.push({ consecutiveFailures, stopped }),
      ...overrides,
    });
  };
  const rows = () =>
    storage.rows<{ owner: string; at_ms: number; failures: number }>(
      "SELECT owner, at_ms, failures FROM _eda_alarm_wakeups ORDER BY owner",
    );
  return { storage, clock, alarms, calls, failures, fire, rows };
};

const failing = (message: string) => async (): Promise<never> => {
  throw new Error(message);
};

describe("runEDASessionAlarm", () => {
  it("runs the runtime for its own wakeup and for an alarm no owner recorded", async () => {
    const recorded = harness();
    await recorded.alarms.request("runtime", 100);
    recorded.clock.now = 100;
    await recorded.fire();
    expect(recorded.calls).toEqual(["runtime"]);
    expect(recorded.rows()).toEqual([]);
    expect(recorded.storage.alarm).toBeNull();

    const legacy = harness();
    await legacy.fire();
    expect(legacy.calls).toEqual(["runtime"]);
  });

  it("runs only the builder for a builder wakeup, never the runtime", async () => {
    const { alarms, clock, calls, fire, rows } = harness();
    await alarms.request("transcript", 100);
    await alarms.request("runtime", 5_000);
    clock.now = 100;
    await fire();
    expect(calls).toEqual(["builder"]);
    expect(rows().map(({ owner }) => owner)).toEqual(["runtime"]);
  });

  it("still runs the builder when the runtime throws, and rethrows without re-arming the alarm", async () => {
    const { storage, alarms, clock, calls, fire, rows } = harness();
    await alarms.request("runtime", 100);
    await alarms.request("transcript", 100);
    clock.now = 100;
    await expect(
      fire({
        runRuntime: failing("runtime down"),
        runBuilderSlices: async () => {
          calls.push("builder");
          return true;
        },
      }),
    ).rejects.toThrow("runtime down");
    expect(calls).toEqual(["builder"]);
    // The platform alarm is left alone so the platform's retry (with backoff) runs it again,
    // and that retry is still the runtime's.
    expect(storage.alarm).toBeNull();
    expect(rows()).toEqual([
      { owner: "runtime", at_ms: 100, failures: 0 },
      { owner: "transcript", at_ms: 100, failures: 0 },
    ]);
    expect([...alarms.due(2_600).keys()].sort()).toEqual(["runtime", "transcript"]);
  });

  it("keeps the builder's backoff and leaves the platform retry alone when both owners fail", async () => {
    const { storage, alarms, clock, fire, rows, failures } = harness();
    await alarms.request("runtime", 100);
    await alarms.request("transcript", 100);
    clock.now = 100;
    await expect(
      fire({ runRuntime: failing("runtime down"), runBuilderSlices: failing("slice failed") }),
    ).rejects.toThrow("runtime down");
    expect(failures).toEqual([{ consecutiveFailures: 1, stopped: false }]);
    expect(rows()).toEqual([
      { owner: "runtime", at_ms: 100, failures: 0 },
      { owner: "transcript", at_ms: 1_100, failures: 1 },
    ]);
    expect(storage.alarm).toBeNull();
    // The platform's retry belongs to the runtime; the builder waits out its backoff.
    expect([...alarms.due(600).keys()]).toEqual(["runtime"]);
  });

  it("keeps the builder's wakeup and continues at once while work remains", async () => {
    const { storage, alarms, clock, fire, rows } = harness();
    await alarms.request("transcript", 100);
    clock.now = 150;
    await fire({ runBuilderSlices: async () => true });
    expect(rows()).toEqual([{ owner: "transcript", at_ms: 150, failures: 0 }]);
    expect(storage.alarm).toBe(150);
  });

  it("backs a failing builder off, keeps its failures across eviction, and stops it at the limit", async () => {
    const first = harness();
    await first.alarms.request("transcript", 0);
    await first.fire({ runBuilderSlices: failing("slice failed") });
    expect(first.rows()).toEqual([{ owner: "transcript", at_ms: 1_000, failures: 1 }]);
    expect(first.storage.alarm).toBe(1_000);

    // A new object instance over the same storage: the count survived.
    const evicted = new DurableObjectAlarmScheduler(first.storage, () => first.clock.now);
    first.clock.now = 1_000;
    await first.fire({ alarms: evicted, runBuilderSlices: failing("slice failed") });
    expect(first.rows()).toEqual([{ owner: "transcript", at_ms: 3_000, failures: 2 }]);
    expect(first.storage.alarm).toBe(3_000);

    first.clock.now = 3_000;
    await first.fire({ runBuilderSlices: failing("slice failed") });
    expect(first.failures).toEqual([
      { consecutiveFailures: 1, stopped: false },
      { consecutiveFailures: 2, stopped: false },
      { consecutiveFailures: 3, stopped: true },
    ]);
    // Stopped: no wakeup, no platform alarm, so nothing loops.
    expect(first.rows()).toEqual([]);
    expect(first.storage.alarm).toBeNull();
  });

  it("resets the builder's failures once a slice succeeds", async () => {
    const { alarms, clock, fire, rows } = harness();
    await alarms.request("transcript", 0);
    await fire({ runBuilderSlices: failing("transient") });
    expect(rows()[0]?.failures).toBe(1);
    clock.now = 1_000;
    await fire({ runBuilderSlices: async () => true });
    expect(rows()).toEqual([{ owner: "transcript", at_ms: 1_000, failures: 0 }]);
  });

  it("clears a builder wakeup without running anything when the session maintains no transcript", async () => {
    const { alarms, calls, fire, rows, storage } = harness();
    await alarms.request("transcript", 0);
    await fire({ runBuilderSlices: undefined });
    expect(calls).toEqual([]);
    expect(rows()).toEqual([]);
    expect(storage.alarm).toBeNull();
  });
});
