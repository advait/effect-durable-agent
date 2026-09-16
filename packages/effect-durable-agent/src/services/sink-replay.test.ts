import { makeMethods } from "@effect/vitest";
import { describe, expect, it } from "vite-plus/test";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";

import {
  reduceCommittedEvents,
  encodeReducedStateCheckpoint,
  frameworkReducedStateReducerName,
  frameworkReducedStateReducerSchemaVersion,
} from "../domain/reduced-state";
import { makeEdaTestLayer } from "../testkit/layers";
import { SessionState } from "./session-state";
import { durablePosition, EventId, SequenceNumber, SessionId } from "../types/core";
import {
  DurableEventEnvelope,
  EventType,
  UnixEpochMillis,
  effectDurableAgentNamespace,
  schemaV1,
} from "../types/events";
import { EventFactory } from "./event-factory";
import { IdGenerator, sequentialUuidV7 } from "./id-generator";
import { EDAKeepAlive } from "./keep-alive";
import { LiveEventBus } from "./live-event-bus";
import { EDAReducer, EDAReducerRegistry } from "./reducer-registry";
import { SessionContext } from "./session-context";
import { EDASessionStore, EDASessionStoreError, type EDASessionStoreShape } from "./session-store";
import { EDASinkName, SinkCheckpointStore } from "./sink-checkpoint-store";
import { EDASinkRegistry, type EDASink } from "./sink-registry";

const sessionId = SessionId.make(sequentialUuidV7(1));
const event = (seq: number) =>
  DurableEventEnvelope.make({
    namespace: effectDurableAgentNamespace,
    type: EventType.make(seq % 4 === 3 ? "ContextRebased" : "UserMessageCommitted"),
    schemaVersion: schemaV1,
    durability: "durable",
    sessionId,
    eventId: EventId.make(sequentialUuidV7(seq + 100)),
    createdAtMs: UnixEpochMillis.make(1715000000000),
    payload:
      seq % 4 === 3
        ? {
            compactionId: sequentialUuidV7(seq + 1000),
            summaryId: sequentialUuidV7(seq + 2000),
            contextVersion: seq,
            retainedFromContextSeq: seq,
          }
        : {
            commandId: sequentialUuidV7(2),
            messageId: sequentialUuidV7(seq + 3000),
            content: [Prompt.textPart({ text: `message ${seq}` })],
          },
  });
const counter = EDAReducer.make({
  name: "replay.count",
  initial: 0,
  stateSchema: Schema.Number,
  reduce: (n) => n + 1,
});
const unexpected = () => Effect.die(new Error("unexpected mutation"));

const harness = (
  sinks: ReadonlyArray<EDASink>,
  wrap: (store: EDASessionStoreShape) => EDASessionStoreShape = (store) => store,
) => {
  const store = Layer.effect(
    EDASessionStore,
    Effect.gen(function* () {
      const inner = yield* EDASessionStore;
      return wrap({
        ...inner,
        eventsAfter: (seq) => inner.eventsAfter(seq).pipe(Stream.rechunk(16)),
      });
    }),
  ).pipe(
    Layer.provide(
      EDASessionStore.InMemorySeeded(
        sessionId,
        Array.from({ length: 132 }, (_, i) => event(i + 1)),
      ),
    ),
  );
  const session = SessionContext.Live(sessionId);
  const ids = IdGenerator.Sequential;
  const dependencies = Layer.mergeAll(
    store,
    session,
    ids,
    LiveEventBus.Noop,
    SinkCheckpointStore.InMemory,
    EDAKeepAlive.Noop,
    EDAReducerRegistry.Live([counter]),
    EventFactory.Live.pipe(Layer.provide(Layer.merge(session, ids))),
  );
  return EDASinkRegistry.Live(sinks).pipe(Layer.provideMerge(dependencies));
};

const waitFor = (test: () => Effect.Effect<boolean, EDASessionStoreError>) =>
  Effect.gen(function* () {
    for (let i = 0; i < 2000; i++) {
      if (yield* test()) return;
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(new Error("sink did not catch up"));
  });

describe("paged sink startup replay", () => {
  makeMethods(it).effect(
    "preserves projections and delivery at zero, page-boundary, and lagging checkpoints",
    () =>
      Effect.gen(function* () {
        const cursors = [0, 1, 15, 16, 17, 63, 64];
        const delivered = cursors.map(() => new Array<number>());
        const failures: Array<string> = [];
        const sinks = cursors.map(
          (_, i): EDASink => ({
            name: `replay.${i}`,
            durable: {
              batchSize: 5,
              interests: ["UserMessageCommitted"],
              process: (batch) =>
                Effect.sync(() => {
                  // Record assertions outside the runner: sink defects are intentionally caught by its contract.
                  try {
                    expect(batch.stateAfter).toEqual(
                      reduceCommittedEvents(all.slice(0, batch.throughSeq)),
                    );
                    expect(batch.reducerStates.get(counter.name)).toBe(batch.throughSeq);
                    expect(
                      batch.events.every((entry) => entry.event.type === "UserMessageCommitted"),
                    ).toBe(true);
                    delivered[i]?.push(
                      ...batch.allEvents.map((entry) => Number(entry.position.seq)),
                    );
                  } catch (error) {
                    failures.push(String(error));
                  }
                }),
            },
          }),
        );
        let all: ReadonlyArray<import("./session-store").CommittedDurableEvent> = [];
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* EDASessionStore;
            all = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
            const checkpoints = yield* SinkCheckpointStore;
            for (const [i, cursor] of cursors.entries())
              yield* checkpoints.commit(
                EDASinkName.make(`replay.${i}`),
                SequenceNumber.make(cursor),
                { preserved: i },
              );
            const registry = yield* EDASinkRegistry;
            yield* registry.startSinkRunners({
              initialHead: SequenceNumber.make(132),
              scope: yield* Effect.scope,
              appendDurableBatch: unexpected,
              publishEphemeral: unexpected,
            });
            yield* waitFor(() =>
              Effect.gen(function* () {
                for (let i = 0; i < cursors.length; i++)
                  if ((yield* checkpoints.load(EDASinkName.make(`replay.${i}`))).afterSeq !== 132)
                    return false;
                return true;
              }),
            );
            expect(failures).toEqual([]);
            for (const [i, cursor] of cursors.entries()) {
              expect(delivered[i]).toEqual(
                Array.from({ length: 132 - cursor }, (_, j) => cursor + j + 1),
              );
              expect((yield* checkpoints.load(EDASinkName.make(`replay.${i}`))).payload).toEqual({
                preserved: i,
              });
            }
          }).pipe(Effect.provide(harness(sinks))),
        );
      }),
  );

  makeMethods(it).effect("does not scan history for a caught-up zero cursor", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* EDASinkRegistry;
        yield* registry.startSinkRunners({
          initialHead: SequenceNumber.make(0),
          scope: yield* Effect.scope,
          appendDurableBatch: unexpected,
          publishEphemeral: unexpected,
        });
      }).pipe(
        Effect.provide(
          harness([{ name: "zero" }], (store) => ({
            ...store,
            eventsAfter: () => Stream.die("zero cursor read history"),
          })),
        ),
      ),
    ),
  );

  makeMethods(it).effect(
    "fails initialization without committing cursors or delivering partially reconstructed state",
    () =>
      Effect.gen(function* () {
        let delivered = 0;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const checkpoints = yield* SinkCheckpointStore;
            yield* checkpoints.commit(EDASinkName.make("failure"), SequenceNumber.make(64), {
              saved: true,
            });
            const registry = yield* EDASinkRegistry;
            const result = yield* registry
              .startSinkRunners({
                initialHead: SequenceNumber.make(132),
                scope: yield* Effect.scope,
                appendDurableBatch: unexpected,
                publishEphemeral: unexpected,
              })
              .pipe(Effect.exit);
            expect(Exit.isFailure(result)).toBe(true);
            expect(delivered).toBe(0);
            expect(yield* checkpoints.load(EDASinkName.make("failure"))).toEqual({
              afterSeq: 64,
              payload: { saved: true },
            });
          }).pipe(
            Effect.provide(
              harness(
                [
                  {
                    name: "failure",
                    durable: {
                      process: () =>
                        Effect.sync(() => {
                          delivered++;
                        }),
                    },
                  },
                ],
                (store) => ({
                  ...store,
                  eventsAfter: (seq) =>
                    store
                      .eventsAfter(seq)
                      .pipe(
                        Stream.take(16),
                        Stream.concat(
                          Stream.fail(
                            new EDASessionStoreError({ message: "injected page failure" }),
                          ),
                        ),
                      ),
                }),
              ),
            ),
          ),
        );
      }),
  );

  makeMethods(it).effect("interrupts page replay without advancing the persisted cursor", () =>
    Effect.gen(function* () {
      const reading = yield* Deferred.make<void>();
      yield* Effect.scoped(
        Effect.gen(function* () {
          const checkpoints = yield* SinkCheckpointStore;
          yield* checkpoints.commit(
            EDASinkName.make("interrupted"),
            SequenceNumber.make(64),
            undefined,
          );
          const registry = yield* EDASinkRegistry;
          const fiber = yield* registry
            .startSinkRunners({
              initialHead: SequenceNumber.make(132),
              scope: yield* Effect.scope,
              appendDurableBatch: unexpected,
              publishEphemeral: unexpected,
            })
            .pipe(Effect.forkChild);
          yield* Deferred.await(reading);
          yield* Fiber.interrupt(fiber);
          expect((yield* checkpoints.load(EDASinkName.make("interrupted"))).afterSeq).toBe(64);
        }).pipe(
          Effect.provide(
            harness([{ name: "interrupted" }], (store) => ({
              ...store,
              eventsAfter: (seq) =>
                store.eventsAfter(seq).pipe(
                  Stream.take(16),
                  Stream.concat(
                    Stream.fromEffect(
                      Effect.gen(function* () {
                        yield* Deferred.succeed(reading, undefined);
                        return yield* Effect.never;
                      }),
                    ),
                  ),
                ),
            })),
          ),
        ),
      );
    }),
  );

  makeMethods(it).effect(
    "subscribes later sinks before an earlier sink appends during startup catch-up",
    () =>
      Effect.gen(function* () {
        let appended = false;
        const sinks: ReadonlyArray<EDASink> = [
          {
            name: "writer",
            durable: {
              process: (_, ctx) =>
                Effect.gen(function* () {
                  if (!appended) {
                    appended = true;
                    yield* ctx.stageDurable(event(133));
                  }
                }),
            },
          },
          { name: "reader", durable: { process: () => Effect.void } },
        ];
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* EDASessionStore;
            const bus = yield* LiveEventBus;
            const checkpoints = yield* SinkCheckpointStore;
            yield* checkpoints.commit(
              EDASinkName.make("writer"),
              SequenceNumber.make(131),
              undefined,
            );
            yield* checkpoints.commit(
              EDASinkName.make("reader"),
              SequenceNumber.make(64),
              undefined,
            );
            const registry = yield* EDASinkRegistry;
            yield* registry.startSinkRunners({
              initialHead: SequenceNumber.make(132),
              scope: yield* Effect.scope,
              publishEphemeral: unexpected,
              appendDurableBatch: (events) =>
                Effect.gen(function* () {
                  const committed = yield* store.append({
                    entries: events.map((event) => ({ event })),
                  });
                  for (const entry of committed) yield* bus.publish(entry);
                  return committed;
                }),
            });
            yield* waitFor(() =>
              Effect.gen(function* () {
                return (yield* checkpoints.load(EDASinkName.make("reader"))).afterSeq === 133;
              }),
            );
            expect(appended).toBe(true);
          }).pipe(
            Effect.provide(
              harness(sinks, (store) => ({
                ...store,
                eventsAfter: (seq) =>
                  store.eventsAfter(seq).pipe(Stream.tap(() => Effect.yieldNow)),
              })),
            ),
          ),
        );
      }),
  );
});

describe("paged session recovery", () => {
  for (const pageSize of [1, 16, 31]) {
    for (const checkpointKind of ["missing", "valid", "stale"] as const) {
      makeMethods(it).effect(
        `matches full replay with ${pageSize}-event pages and ${checkpointKind} checkpoints`,
        () => {
          const events = Array.from({ length: 132 }, (_, i) => event(i + 1));
          const committed = events.map((event, i) => ({
            event,
            position: durablePosition(SequenceNumber.make(i + 1)),
          }));
          const reads: Array<number> = [];
          return Effect.scoped(
            Effect.gen(function* () {
              const state = yield* SessionState;
              const snapshot = yield* state.snapshotData();
              expect(snapshot.reduced).toEqual(reduceCommittedEvents(committed));
              expect(snapshot.reducerStates.get(counter.name)).toBe(132);
              expect(reads).toEqual([
                checkpointKind === "valid" ? 17 : 0,
                checkpointKind === "valid" ? 17 : 0,
              ]);
            }).pipe(
              Effect.provide(
                makeEdaTestLayer({
                  sessionId,
                  seedEvents: events,
                  reducers: [counter],
                  wrapStore: (store) => ({
                    ...store,
                    eventsAfter: (seq) => {
                      reads.push(seq);
                      return store.eventsAfter(seq).pipe(Stream.rechunk(pageSize));
                    },
                    loadReducerCheckpoint: (name) =>
                      Effect.succeed(
                        checkpointKind === "missing"
                          ? undefined
                          : {
                              name,
                              throughSeq: SequenceNumber.make(17),
                              updatedAtMs: 1715000000000,
                              schemaVersion:
                                checkpointKind === "stale"
                                  ? 0
                                  : name === frameworkReducedStateReducerName
                                    ? frameworkReducedStateReducerSchemaVersion
                                    : 1,
                              payload:
                                name === frameworkReducedStateReducerName
                                  ? encodeReducedStateCheckpoint(
                                      reduceCommittedEvents(committed.slice(0, 17)),
                                    )
                                  : 17,
                            },
                      ),
                  }),
                }),
              ),
            ),
          );
        },
      );
    }
  }
});
