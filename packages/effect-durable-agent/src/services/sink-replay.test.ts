import { assert, makeMethods } from "@effect/vitest";
import { describe, it } from "vite-plus/test";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import * as Prompt from "effect/unstable/ai/Prompt";

import {
  reduceCommittedEvents,
  encodeReducedStateCheckpoint,
  frameworkReducedStateReducerName,
  frameworkReducedStateReducerSchemaVersion,
} from "../domain/reduced-state";
import { makeEdaTestLayer } from "../testkit/layers";
import { ModelResolver } from "./model-resolver";
import { EDARuntime } from "./runtime";
import { makeEDARuntimeLayer } from "./runtime-layer";
import { SessionState } from "./session-state";
import { makeEdaExportingTracer, type EDAExportedSpan } from "./tracing";
import { durablePosition, EventId, SequenceNumber, SessionId } from "../types/core";
import {
  DurableEventEnvelope,
  EventType,
  EventNamespace,
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
import {
  EDASinkRegistry,
  type EDASink,
  type EDASinkDurableBatch,
  type EDASinkRawDurableBatch,
} from "./sink-registry";

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
const history = EDAReducer.make({
  name: "replay.history",
  initial: Schema.decodeUnknownSync(Schema.Array(Schema.Number))([]),
  stateSchema: Schema.Array(Schema.Number),
  reduce: (state, entry) => [...state, Number(entry.position.seq)],
});
const projectionAt = (count: number) => ({
  reduced: reduceCommittedEvents(
    Array.from({ length: count }, (_, i) => ({
      event: event(i + 1),
      position: durablePosition(SequenceNumber.make(i + 1)),
    })),
  ),
  reducerStates: new Map<string, unknown>([
    [counter.name, count],
    [history.name, Array.from({ length: count }, (_, i) => i + 1)],
  ]),
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
    EDAReducerRegistry.Live([counter, history]),
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
  for (const cursor of [0, 16, 127, 132]) {
    makeMethods(it).effect(
      `delivers raw wildcard events after cursor ${cursor} without hydration`,
      () =>
        Effect.gen(function* () {
          const batches: EDASinkRawDurableBatch[] = [];
          const reads: number[] = [];
          const name = EDASinkName.make(`raw.${cursor}`);
          const sink: EDASink = {
            name,
            state: "none",
            durable: {
              interests: "*",
              batchSize: 16,
              process: (batch) =>
                Effect.sync(() => {
                  batches.push(batch);
                }),
            },
          };
          yield* Effect.scoped(
            Effect.gen(function* () {
              const checkpoints = yield* SinkCheckpointStore;
              yield* checkpoints.commit(name, SequenceNumber.make(cursor), { preserved: true });
              const registry = yield* EDASinkRegistry;
              yield* registry.startSinkRunners({
                initialProjection: projectionAt(132),
                scope: yield* Effect.scope,
                appendDurableBatch: unexpected,
                publishEphemeral: unexpected,
              });
              const store = yield* EDASessionStore;
              yield* store.append({ entries: [{ event: event(133) }] });
              yield* registry.notifyDurableHeadAdvanced(SequenceNumber.make(133));
              yield* waitFor(() =>
                Effect.gen(function* () {
                  return (yield* checkpoints.load(name)).afterSeq === 133;
                }),
              );
              assert.deepStrictEqual(
                batches.flatMap((batch) => batch.events.map((entry) => Number(entry.position.seq))),
                Array.from({ length: 133 - cursor }, (_, i) => cursor + i + 1),
              );
              assert.strictEqual(reads[0], cursor);
              assert.strictEqual(
                reads.every((after) => after >= cursor),
                true,
              );
              for (const batch of batches) {
                assert.strictEqual("stateAfter" in batch, false);
                assert.strictEqual("reducerStates" in batch, false);
                assert.deepStrictEqual(batch.events, batch.allEvents);
              }
              assert.deepStrictEqual((yield* checkpoints.load(name)).payload, { preserved: true });
            }).pipe(
              Effect.provide(
                harness([sink], (store) => ({
                  ...store,
                  eventsAfter: (after) => {
                    reads.push(after);
                    return store.eventsAfter(after);
                  },
                })),
              ),
            ),
          );
        }),
    );
  }

  makeMethods(it).effect(
    "reuses caught-up projections without reads and isolates later sink folds",
    () =>
      Effect.gen(function* () {
        const deliveries: Array<Array<EDASinkDurableBatch>> = [[], []];
        const reads: Array<number> = [];
        const sinks = deliveries.map(
          (batches, i): EDASink => ({
            name: `caught-up.${i}`,
            durable: {
              process: (batch) =>
                Effect.sync(() => {
                  batches.push(batch);
                }),
            },
          }),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* EDASessionStore;
            const all = yield* store.eventsAfter(SequenceNumber.make(0)).pipe(Stream.runCollect);
            const initialProjection = projectionAt(132);
            const checkpoints = yield* SinkCheckpointStore;
            for (const sink of sinks)
              yield* checkpoints.commit(EDASinkName.make(sink.name), SequenceNumber.make(132), {
                preserved: sink.name,
              });
            reads.length = 0;
            const registry = yield* EDASinkRegistry;
            yield* registry.startSinkRunners({
              initialProjection,
              scope: yield* Effect.scope,
              appendDurableBatch: unexpected,
              publishEphemeral: unexpected,
            });
            assert.deepStrictEqual(reads, []);
            for (const seq of [133, 134]) {
              const committed = yield* store.append({ entries: [{ event: event(seq) }] });
              for (const entry of committed)
                yield* registry.notifyDurableHeadAdvanced(entry.position.seq);
              yield* waitFor(() =>
                Effect.gen(function* () {
                  for (const sink of sinks)
                    if ((yield* checkpoints.load(EDASinkName.make(sink.name))).afterSeq !== seq)
                      return false;
                  return true;
                }),
              );
            }
            const complete = yield* store
              .eventsAfter(SequenceNumber.make(0))
              .pipe(Stream.runCollect);
            for (const batches of deliveries) {
              assert.deepStrictEqual(
                batches.map((b) => b.throughSeq),
                [133, 134],
              );
              for (const batch of batches) {
                assert.deepStrictEqual(
                  batch.stateAfter,
                  reduceCommittedEvents(complete.slice(0, batch.throughSeq)),
                );
                assert.strictEqual(batch.reducerStates.get(counter.name), batch.throughSeq);
                assert.deepStrictEqual(
                  batch.reducerStates.get(history.name),
                  Array.from({ length: batch.throughSeq }, (_, i) => i + 1),
                );
              }
            }
            assert.deepStrictEqual(
              initialProjection.reducerStates,
              projectionAt(132).reducerStates,
            );
            assert.deepStrictEqual(initialProjection.reduced, reduceCommittedEvents(all));
            assert.strictEqual(initialProjection.reducerStates.get(counter.name), 132);
            for (const sink of sinks)
              assert.deepStrictEqual(
                (yield* checkpoints.load(EDASinkName.make(sink.name))).payload,
                { preserved: sink.name },
              );
          }).pipe(
            Effect.provide(
              harness(sinks, (store) => ({
                ...store,
                eventsAfter: (seq) => {
                  reads.push(seq);
                  return store.eventsAfter(seq);
                },
              })),
            ),
          ),
        );
      }),
  );

  makeMethods(it).effect(
    "preserves projections and delivery at zero, page-boundary, and lagging checkpoints",
    () =>
      Effect.gen(function* () {
        const spans: EDAExportedSpan[] = [];
        const cursors = [0, 1, 15, 16, 17, 63, 64, 132];
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
                    assert.deepStrictEqual(
                      batch.stateAfter,
                      reduceCommittedEvents(all.slice(0, batch.throughSeq)),
                    );
                    assert.strictEqual(batch.reducerStates.get(counter.name), batch.throughSeq);
                    assert.strictEqual(
                      batch.events.every((entry) => entry.event.type === "UserMessageCommitted"),
                      true,
                    );
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
              initialProjection: projectionAt(132),
              scope: yield* Effect.scope,
              appendDurableBatch: unexpected,
              publishEphemeral: unexpected,
            });
            const initialization = spans.find((span) => span.name === "agent.sinks.initialize");
            assert.strictEqual(initialization?.attributes["eda.sinks.reused"], 1);
            assert.strictEqual(initialization?.attributes["eda.sinks.deferred"], 6);
            assert.strictEqual(initialization?.attributes["eda.sinks.empty"], 1);
            yield* waitFor(() =>
              Effect.gen(function* () {
                for (let i = 0; i < cursors.length; i++)
                  if ((yield* checkpoints.load(EDASinkName.make(`replay.${i}`))).afterSeq !== 132)
                    return false;
                return true;
              }),
            );
            assert.deepStrictEqual(failures, []);
            for (const [i, cursor] of cursors.entries()) {
              assert.deepStrictEqual(
                delivered[i],
                Array.from({ length: 132 - cursor }, (_, j) => cursor + j + 1),
              );
              assert.deepStrictEqual(
                (yield* checkpoints.load(EDASinkName.make(`replay.${i}`))).payload,
                {
                  preserved: i,
                },
              );
            }
          }).pipe(
            Effect.provide(harness(sinks)),
            Effect.provideService(
              Tracer.Tracer,
              makeEdaExportingTracer((span) => spans.push(span)),
            ),
          ),
        );
      }),
  );

  makeMethods(it).effect("does not scan history for a caught-up zero cursor", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* EDASinkRegistry;
        yield* registry.startSinkRunners({
          initialProjection: projectionAt(0),
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
    "isolates background hydration failure without committing cursors or delivering partial state",
    () =>
      Effect.gen(function* () {
        let delivered = 0;
        const failedRead = yield* Deferred.make<void>();
        yield* Effect.scoped(
          Effect.gen(function* () {
            const checkpoints = yield* SinkCheckpointStore;
            yield* checkpoints.commit(EDASinkName.make("failure"), SequenceNumber.make(64), {
              saved: true,
            });
            const registry = yield* EDASinkRegistry;
            const result = yield* registry
              .startSinkRunners({
                initialProjection: projectionAt(132),
                scope: yield* Effect.scope,
                appendDurableBatch: unexpected,
                publishEphemeral: unexpected,
              })
              .pipe(Effect.exit);
            assert.strictEqual(Exit.isSuccess(result), true);
            yield* Deferred.await(failedRead);
            yield* Effect.yieldNow;
            assert.strictEqual(delivered, 0);
            assert.deepStrictEqual(yield* checkpoints.load(EDASinkName.make("failure")), {
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
                    store.eventsAfter(seq).pipe(
                      Stream.take(16),
                      Stream.concat(
                        Stream.fromEffect(
                          Effect.gen(function* () {
                            yield* Deferred.succeed(failedRead, undefined);
                            return yield* new EDASessionStoreError({
                              message: "injected page failure",
                            });
                          }),
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
          const runnerScope = yield* Scope.make();
          yield* registry.startSinkRunners({
            initialProjection: projectionAt(132),
            scope: runnerScope,
            appendDurableBatch: unexpected,
            publishEphemeral: unexpected,
          });
          yield* Deferred.await(reading);
          yield* Scope.close(runnerScope, Exit.void);
          assert.strictEqual(
            (yield* checkpoints.load(EDASinkName.make("interrupted"))).afterSeq,
            64,
          );
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

  for (const readerCursor of [64, 132])
    makeMethods(it).effect(
      `subscribes reader at ${readerCursor} before an earlier sink appends during startup catch-up`,
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
              const checkpoints = yield* SinkCheckpointStore;
              yield* checkpoints.commit(
                EDASinkName.make("writer"),
                SequenceNumber.make(131),
                undefined,
              );
              yield* checkpoints.commit(
                EDASinkName.make("reader"),
                SequenceNumber.make(readerCursor),
                undefined,
              );
              const registry = yield* EDASinkRegistry;
              yield* registry.startSinkRunners({
                initialProjection: projectionAt(132),
                scope: yield* Effect.scope,
                publishEphemeral: unexpected,
                appendDurableBatch: (events) =>
                  Effect.gen(function* () {
                    const committed = yield* store.append({
                      entries: events.map((event) => ({ event })),
                    });
                    for (const entry of committed)
                      yield* registry.notifyDurableHeadAdvanced(entry.position.seq);
                    return committed;
                  }),
              });
              yield* waitFor(() =>
                Effect.gen(function* () {
                  return (yield* checkpoints.load(EDASinkName.make("reader"))).afterSeq === 133;
                }),
              );
              assert.strictEqual(appended, true);
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
  makeMethods(it).effect(
    "loads a checkpointed runtime with caught-up sinks without reading genesis",
    () => {
      const spans: EDAExportedSpan[] = [];
      const head = SequenceNumber.make(131);
      const seedEvents = Array.from({ length: head }, (_, i) =>
        DurableEventEnvelope.make({
          ...event(i + 1),
          namespace: EventNamespace.make("test.replay"),
          type: EventType.make("TestRecorded"),
          payload: { index: i },
        }),
      );
      const projection = {
        reduced: reduceCommittedEvents(
          seedEvents.map((event, i) => ({
            event,
            position: durablePosition(SequenceNumber.make(i + 1)),
          })),
        ),
      };
      const reads: Array<number> = [];
      const sinks = Array.from(
        { length: 7 },
        (_, i): EDASink => ({ name: `runtime.${i}`, durable: { process: unexpected } }),
      );
      const storeLayer = Layer.effect(
        EDASessionStore,
        Effect.gen(function* () {
          const store = yield* EDASessionStore;
          yield* store.saveReducerCheckpoints([
            {
              name: frameworkReducedStateReducerName,
              schemaVersion: frameworkReducedStateReducerSchemaVersion,
              throughSeq: head,
              payload: encodeReducedStateCheckpoint(projection.reduced),
              updatedAtMs: 1715000000000,
            },
            {
              name: counter.name,
              schemaVersion: 1,
              throughSeq: head,
              payload: head,
              updatedAtMs: 1715000000000,
            },
          ]);
          return {
            ...store,
            eventsAfter: (seq: SequenceNumber) => {
              reads.push(seq);
              return seq === 0
                ? Stream.die("checkpointed runtime replayed genesis")
                : store.eventsAfter(seq);
            },
          };
        }),
      ).pipe(Layer.provide(EDASessionStore.InMemorySeeded(sessionId, seedEvents)));
      const checkpointLayer = Layer.effect(
        SinkCheckpointStore,
        Effect.gen(function* () {
          const checkpoints = yield* SinkCheckpointStore;
          for (const sink of sinks)
            yield* checkpoints.commit(EDASinkName.make(sink.name), head, undefined);
          return checkpoints;
        }),
      ).pipe(Layer.provide(SinkCheckpointStore.InMemory));
      return Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* EDARuntime;
          const snapshot = yield* runtime.snapshot();
          assert.strictEqual(snapshot.state.lastSeq, head);
          assert.deepStrictEqual(snapshot.state.messages, projection.reduced.messages);
          assert.strictEqual(snapshot.reducerStates.get(counter.name), head);
          assert.deepStrictEqual(reads, [head, head]);
          yield* runtime.snapshot();
          const hydration = spans.filter((span) => span.name === "agent.session.hydrate");
          const initialization = spans.filter((span) => span.name === "agent.sinks.initialize");
          assert.strictEqual(hydration.length, 1);
          assert.strictEqual(initialization.length, 1);
          assert.strictEqual(hydration[0]?.attributes["sessionId"], sessionId);
          assert.strictEqual(hydration[0]?.attributes["eda.checkpoint.framework_seq"], head);
          assert.strictEqual(initialization[0]?.attributes["eda.sinks.reused"], 7);
          assert.strictEqual(initialization[0]?.attributes["eda.sinks.deferred"], 0);
          assert.strictEqual(initialization[0]?.attributes["eda.sinks.empty"], 0);
        }).pipe(
          Effect.provide(
            makeEDARuntimeLayer({
              config: { modelSelection: { provider: "test", modelId: "test" } },
              sessionId,
              sessionStoreLayer: storeLayer,
              sinkCheckpointStoreLayer: checkpointLayer,
              tracer: makeEdaExportingTracer((span) => spans.push(span)),
              modelResolverLayer: Layer.succeed(ModelResolver, { resolve: unexpected }),
              reducerRegistryLayer: EDAReducerRegistry.Live([counter]),
              sinks,
            }),
          ),
        ),
      );
    },
  );

  for (const stage of ["hydrate", "sinks"] as const) {
    makeMethods(it).effect(`exports a failed ${stage} startup span`, () =>
      Effect.gen(function* () {
        const spans: EDAExportedSpan[] = [];
        const failure = new EDASessionStoreError({ message: "storage unavailable" });
        const storeLayer = Layer.effect(
          EDASessionStore,
          Effect.gen(function* () {
            const store = yield* EDASessionStore;
            return { ...store, loadReducerCheckpoint: () => Effect.fail(failure) };
          }),
        ).pipe(Layer.provide(EDASessionStore.InMemorySeeded(sessionId, [])));
        const checkpointLayer = Layer.succeed(SinkCheckpointStore, {
          load: () => Effect.fail(failure),
          commit: unexpected,
          saveState: unexpected,
        });
        const result = yield* Effect.scoped(
          EDARuntime.pipe(
            Effect.provide(
              makeEDARuntimeLayer({
                config: { modelSelection: { provider: "test", modelId: "test" } },
                sessionId,
                sessionStoreLayer:
                  stage === "hydrate" ? storeLayer : EDASessionStore.InMemorySeeded(sessionId, []),
                sinkCheckpointStoreLayer: checkpointLayer,
                modelResolverLayer: Layer.succeed(ModelResolver, { resolve: unexpected }),
                sinks: [{ name: "failure" }],
                tracer: makeEdaExportingTracer((span) => spans.push(span)),
              }),
            ),
          ),
        ).pipe(Effect.exit);
        assert.ok(Exit.isFailure(result));
        const failed = spans.find(
          (span) =>
            span.name ===
            (stage === "hydrate" ? "agent.session.hydrate" : "agent.sinks.initialize"),
        );
        assert.strictEqual(failed?.statusCode, "ERROR");
        assert.strictEqual(failed?.statusMessage, "storage unavailable");
        assert.strictEqual(failed?.attributes["sessionId"], sessionId);
        assert.strictEqual(
          spans.filter((span) => span.name.startsWith("agent.sinks.initialize")).length,
          stage === "hydrate" ? 0 : 1,
        );
      }),
    );
  }

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
              assert.deepStrictEqual(snapshot.reduced, reduceCommittedEvents(committed));
              assert.strictEqual(snapshot.reducerStates.get(counter.name), 132);
              assert.deepStrictEqual(reads, [
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
