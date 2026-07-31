import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import { describe, expect, it } from "vite-plus/test";

import { DurableObjectKeepAlive } from "./durable-object-keepalive";

class FakeAlarmStorage {
  alarm: number | null = null;
  calls: Array<{ readonly _tag: "set"; readonly at: number } | { readonly _tag: "delete" }> = [];

  getAlarm(): number | null {
    return this.alarm;
  }

  setAlarm(scheduledTimeMs: number): void {
    this.alarm = scheduledTimeMs;
    this.calls.push({ _tag: "set", at: scheduledTimeMs });
  }

  deleteAlarm(): void {
    this.alarm = null;
    this.calls.push({ _tag: "delete" });
  }
}

class FakeBackgroundWaiter {
  private readonly promises: Array<Promise<unknown>> = [];

  waitUntil(promise: Promise<unknown>): void {
    this.promises.push(promise);
  }

  async flush(): Promise<void> {
    const pending = this.promises.splice(0);
    await Promise.all(pending);
  }
}

const makeKeepAlive = (
  storage: FakeAlarmStorage,
  background?: FakeBackgroundWaiter,
  now: () => number = () => 1_000,
) =>
  new DurableObjectKeepAlive(storage, background, {
    intervalMs: 5_000,
    now,
  });

describe("DurableObjectKeepAlive", () => {
  it("preserves the earliest alarm across overlapping lease transitions", async () => {
    const storage = new FakeAlarmStorage();
    let now = 1_000;
    const keepAlive = makeKeepAlive(storage, undefined, () => now);

    const first = await keepAlive.acquire();
    expect(keepAlive.activeLeaseCount).toBe(1);
    expect(storage.alarm).toBe(6_000);

    now = 2_000;
    const second = await keepAlive.acquire();
    expect(keepAlive.activeLeaseCount).toBe(2);

    now = 3_000;
    await first.release();
    expect(keepAlive.activeLeaseCount).toBe(1);
    expect(storage.alarm).toBe(6_000);
    expect(storage.calls).toEqual([{ _tag: "set", at: 6_000 }]);

    await second.release();
    expect(keepAlive.activeLeaseCount).toBe(0);
    expect(storage.alarm).toBeNull();
    expect(storage.calls.at(-1)).toEqual({ _tag: "delete" });
  });

  it("re-arms one interval after Cloudflare consumes an alarm", async () => {
    const storage = new FakeAlarmStorage();
    let now = 1_000;
    const keepAlive = makeKeepAlive(storage, undefined, () => now);

    const lease = await keepAlive.acquire();
    expect(storage.alarm).toBe(6_000);

    now = 6_000;
    storage.alarm = null;
    await keepAlive.alarm();

    expect(storage.alarm).toBe(11_000);
    expect(storage.calls).toEqual([
      { _tag: "set", at: 6_000 },
      { _tag: "set", at: 11_000 },
    ]);

    await lease.release();
  });

  it("preserves an earlier durable alarm after keepalive reconstruction", async () => {
    const storage = new FakeAlarmStorage();
    storage.setAlarm(6_000);
    storage.calls = [];
    const keepAlive = makeKeepAlive(storage, undefined, () => 2_000);

    const lease = await keepAlive.acquire();

    expect(storage.alarm).toBe(6_000);
    expect(storage.calls).toEqual([]);

    await lease.release();
  });

  it("pulls an existing later alarm forward", async () => {
    const storage = new FakeAlarmStorage();
    storage.setAlarm(10_000);
    storage.calls = [];
    const keepAlive = makeKeepAlive(storage);

    const lease = await keepAlive.acquire();

    expect(storage.alarm).toBe(6_000);
    expect(storage.calls).toEqual([{ _tag: "set", at: 6_000 }]);

    await lease.release();
  });

  it("makes lease release idempotent", async () => {
    const storage = new FakeAlarmStorage();
    const keepAlive = makeKeepAlive(storage);
    const lease = await keepAlive.acquire();

    await lease.release();
    await lease.release();

    expect(keepAlive.activeLeaseCount).toBe(0);
    expect(storage.calls.filter((call) => call._tag === "delete")).toHaveLength(1);
  });

  it("does not re-arm when a stale lease releases after shutdown", async () => {
    const storage = new FakeAlarmStorage();
    const keepAlive = makeKeepAlive(storage);
    const lease = await keepAlive.acquire();

    expect(storage.alarm).toBe(6_000);

    await keepAlive.shutdown();
    await lease.release();

    expect(keepAlive.activeLeaseCount).toBe(0);
    expect(storage.alarm).toBeNull();
    expect(storage.calls.at(-1)).toEqual({ _tag: "delete" });
  });

  it("releases the heartbeat when host-wrapped promise work throws", async () => {
    const storage = new FakeAlarmStorage();
    const keepAlive = makeKeepAlive(storage);

    await expect(
      keepAlive.runWhileActive(async () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");

    expect(keepAlive.activeLeaseCount).toBe(0);
    expect(storage.alarm).toBeNull();
  });

  it("releases the heartbeat when a host-forked Effect fiber settles", async () => {
    const storage = new FakeAlarmStorage();
    const background = new FakeBackgroundWaiter();
    const keepAlive = makeKeepAlive(storage, background);
    const deferred = Effect.runSync(Deferred.make<void>());

    const fiber = await keepAlive.fork(Deferred.await(deferred));
    expect(keepAlive.activeLeaseCount).toBe(1);
    expect(storage.alarm).toBe(6_000);

    await Effect.runPromise(Deferred.succeed(deferred, undefined));
    const exit = await Effect.runPromise(Fiber.await(fiber));
    await background.flush();

    expect(Exit.isSuccess(exit)).toBe(true);
    expect(keepAlive.activeLeaseCount).toBe(0);
    expect(storage.alarm).toBeNull();
  });

  it("releases the heartbeat when a host-forked Effect fiber settles immediately", async () => {
    const storage = new FakeAlarmStorage();
    const background = new FakeBackgroundWaiter();
    const keepAlive = makeKeepAlive(storage, background);

    const fiber = await keepAlive.fork(Effect.succeed("done"));
    const exit = await Effect.runPromise(Fiber.await(fiber));
    await background.flush();

    expect(exit).toEqual(Exit.succeed("done"));
    expect(keepAlive.activeLeaseCount).toBe(0);
    expect(storage.alarm).toBeNull();
  });
});
