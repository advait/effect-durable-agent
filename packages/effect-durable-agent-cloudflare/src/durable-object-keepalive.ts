import * as Effect from "effect/Effect";
import type * as Fiber from "effect/Fiber";

/** Default alarm cadence for keeping an active Durable Object warm. */
export const defaultDurableObjectKeepAliveIntervalMs = 30_000;

/** Minimal Durable Object storage alarm surface needed by the EDA host keepalive. */
export interface DurableObjectAlarmStorage {
  readonly getAlarm: () => number | null | Promise<number | null>;
  readonly setAlarm: (scheduledTimeMs: number) => void | Promise<void>;
  readonly deleteAlarm: () => void | Promise<void>;
}

/**
 * Optional host hook for background alarm writes started from synchronous observers.
 * Durable Objects already track their own async work; this hook does not keep an
 * accepted WebSocket active or prevent WebSocket hibernation.
 */
export interface DurableObjectBackgroundWaiter {
  readonly waitUntil: (promise: Promise<unknown>) => void;
}

/** Configuration for the in-memory active-work heartbeat. */
export interface DurableObjectKeepAliveOptions {
  /** Alarm interval while at least one host-tracked fiber is still unsettled. */
  readonly intervalMs?: number;
  /** Clock used for scheduling; tests can inject a deterministic clock. */
  readonly now?: () => number;
  /** Receives best-effort release/reschedule failures that cannot be awaited by the caller. */
  readonly onBackgroundError?: (error: unknown) => void;
}

/** Idempotent lease for one host-tracked active Effect fiber/root. */
export interface DurableObjectKeepAliveLease {
  readonly release: () => Promise<void>;
}

/**
 * Alarm-backed in-memory keepalive for a raw Durable Object host.
 *
 * The active lease count is intentionally not durable: if the object is evicted
 * or crashes, the fibers the leases represented are gone too, and normal EDA
 * durable recovery must explain any escaped lifecycle boundaries.
 */
export class DurableObjectKeepAlive {
  private readonly intervalMs: number;
  private readonly now: () => number;
  private readonly onBackgroundError: (error: unknown) => void;
  private activeLeases = 0;
  private generation = 0;
  private scheduleTail: Promise<void> = Promise.resolve();
  private shutdownRequested = false;

  constructor(
    private readonly storage: DurableObjectAlarmStorage,
    private readonly background: DurableObjectBackgroundWaiter | undefined = undefined,
    options: DurableObjectKeepAliveOptions = {},
  ) {
    this.intervalMs = options.intervalMs ?? defaultDurableObjectKeepAliveIntervalMs;
    this.now = options.now ?? (() => Date.now());
    this.onBackgroundError = options.onBackgroundError ?? (() => undefined);
  }

  /** Number of active unsettled host roots currently holding the heartbeat. */
  get activeLeaseCount(): number {
    return this.activeLeases;
  }

  /** Acquire one active-work lease and arm the heartbeat if this is the first lease. */
  acquire(): Promise<DurableObjectKeepAliveLease> {
    if (this.shutdownRequested) {
      return Promise.reject(new Error("DurableObjectKeepAlive is shut down"));
    }
    this.activeLeases += 1;
    const generation = this.generation;
    let released = false;

    return this.reschedule().then(
      () => ({
        release: async () => {
          if (released) {
            return;
          }
          released = true;
          if (generation !== this.generation) {
            return;
          }
          this.activeLeases = Math.max(0, this.activeLeases - 1);
          await this.reschedule();
        },
      }),
      async (error) => {
        released = true;
        if (generation !== this.generation) {
          throw error;
        }
        this.activeLeases = Math.max(0, this.activeLeases - 1);
        await this.reschedule().catch(this.onBackgroundError);
        throw error;
      },
    );
  }

  /** Run a promise while holding a lease. Intended for host adapters, not EDA internals. */
  async runWhileActive<A>(fn: () => Promise<A>): Promise<A> {
    const lease = await this.acquire();
    try {
      return await fn();
    } finally {
      await lease.release();
    }
  }

  /**
   * Track an already-forked Effect fiber as active host work until it settles.
   *
   * Use this with `ManagedRuntime.runFork(...)` when the fiber needs an already
   * built runtime context. The returned lease can be released manually, but it
   * is normally released by the fiber observer.
   */
  async trackFiber<A, E>(fiber: Fiber.Fiber<A, E>): Promise<DurableObjectKeepAliveLease> {
    const lease = await this.acquire();
    fiber.addObserver(() => {
      this.waitUntil(
        lease.release().catch((error) => {
          this.onBackgroundError(error);
        }),
      );
    });

    if (fiber.pollUnsafe() !== undefined) {
      await lease.release();
    }

    return lease;
  }

  /**
   * Fork an Effect as host-tracked active work.
   *
   * This mirrors Effect's host-runner pattern: observe the root fiber and release
   * the alarm lease when it settles. It deliberately does not patch Effect
   * internals or try to classify runnable vs suspended fibers.
   */
  async fork<A, E>(effect: Effect.Effect<A, E>): Promise<Fiber.Fiber<A, E>> {
    const fiber = Effect.runFork(effect);
    await this.trackFiber(fiber);
    return fiber;
  }

  /** Handle the Durable Object `alarm()` callback and re-arm while work remains. */
  alarm(): Promise<void> {
    return this.reschedule();
  }

  /** Permanently stop the heartbeat and prevent later lease releases from re-arming it. */
  shutdown(): Promise<void> {
    this.shutdownRequested = true;
    this.generation += 1;
    this.activeLeases = 0;
    return this.reschedule();
  }

  /** Start a fresh lease generation after the host intentionally resets its durable state. */
  restart(): void {
    this.shutdownRequested = false;
  }

  private waitUntil(promise: Promise<unknown>): void {
    if (this.background !== undefined) {
      this.background.waitUntil(promise);
      return;
    }
    void promise;
  }

  private reschedule(): Promise<void> {
    this.scheduleTail = this.scheduleTail.catch(() => undefined).then(() => this.syncAlarm());
    return this.scheduleTail;
  }

  private async syncAlarm(): Promise<void> {
    if (this.shutdownRequested || this.activeLeases === 0) {
      await this.storage.deleteAlarm();
      return;
    }

    const desiredAt = this.now() + this.intervalMs;
    const currentAt = await this.storage.getAlarm();
    if (currentAt === null || currentAt > desiredAt) {
      await this.storage.setAlarm(desiredAt);
    }
  }
}

/** Hold a Durable Object heartbeat lease for the lifetime of an Effect. */
export const withDurableObjectKeepAlive =
  (keepAlive: DurableObjectKeepAlive) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.acquireUseRelease(
      Effect.promise(() => keepAlive.acquire()),
      () => effect,
      (lease) => Effect.promise(() => lease.release()),
    );
