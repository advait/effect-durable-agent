import * as fc from "fast-check";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/ai/Prompt";
import * as Response from "effect/ai/Response";
import { describe, expect, it } from "vite-plus/test";

import * as Schema from "effect/Schema";

import { classifyRecoverableWork, reduceCommittedEvents } from "../domain/reduced-state";
import { durableMessageTranscript } from "../domain/message-transcript";
import { CommandIdempotencyKey, StopTurnCommand, SubmitMessageCommand } from "../types/commands";
import type { EDACommand } from "../types/commands";
import { CommandId, EventId, SequenceNumber, SessionId } from "../types/core";
import {
  DeciderAcceptedEvent,
  DurableEventEnvelope,
  EventDedupeKey,
  UnixEpochMillis,
  schemaV1,
} from "../types/events";
import { durableEventEnvelope } from "../types/events/durable";
import { EDADecider, type EDADeciderOutcome } from "./decider";
import { EDAReducer, getEDAReducerState } from "./reducer-registry";
import { EDASink } from "./sink-registry";
import { EDASessionStore } from "./session-store";
import type { CommittedDurableEvent, EDASessionStoreShape } from "./session-store";
import { sequentialUuidV7 } from "./id-generator";
import { EDASessionQuery } from "./session-query";
import { SessionState } from "./session-state";
import type { SessionStateShape } from "./session-state";
import type { InferenceRunnerStreamPart } from "./inference-runner";
import { makeEdaTestLayer } from "../testkit/layers";
import { NoopParams, SESSION_ID, modelSelection, usage } from "./session-state-control-testkit";

const propertyRuns = 25;
const maxActions = 12;

type ModelAction =
  | "submitQueue"
  | "submitSteer"
  | "submitInterrupt"
  | "stop"
  | "finish"
  | "decide"
  | "retryDecide";

type CommandAdmission = {
  readonly commandId: string;
  readonly command: EDACommand;
};

const actionArbitrary = fc
  .tuple(
    fc.constant<ModelAction>("submitQueue"),
    fc.array(
      fc.constantFrom<ModelAction>(
        "submitQueue",
        "submitSteer",
        "submitInterrupt",
        "stop",
        "finish",
        "decide",
        "retryDecide",
      ),
      { maxLength: maxActions - 1 },
    ),
  )
  .map(([first, rest]) => [first, ...rest] as ReadonlyArray<ModelAction>);

const commandId = (index: number) => CommandId.make(sequentialUuidV7(10_000 + index));

/**
 * App facts the decider reads and writes: `Fact` from the decider and from tools, `Echo` staged by
 * a sink in reaction to each `Fact`. Both land concurrently with decisions and run lifecycles.
 */
const modelFields = { ...DurableEventEnvelope.fields, namespace: Schema.Literal("test.model") };
const ModelFact = Schema.Struct({
  ...modelFields,
  type: Schema.Literal("Fact"),
  payload: Schema.Struct({ count: Schema.Number, head: Schema.Number, origin: Schema.String }),
});
const ModelEcho = Schema.Struct({
  ...modelFields,
  type: Schema.Literal("Echo"),
  payload: Schema.Struct({ of: Schema.String }),
});
const ModelEvents = Schema.Union([ModelFact, ModelEcho]);
const factCounter = EDAReducer.forEvents(ModelEvents, {
  name: "test.model.facts",
  initial: 0,
  stateSchema: Schema.Number,
  reduce: (count) => count + 1,
});

/** Reads the head and the fact count; on an even count it also admits a steer. */
const DecideInput = Schema.Struct({ key: Schema.String, text: Schema.String });
type DecideInput = typeof DecideInput.Type;
const modelDecider = EDADecider.define({
  name: "test.model.Decide",
  input: DecideInput,
  events: ModelEvents,
  result: Schema.Struct({ count: Schema.Number, head: Schema.Number }),
  rejection: Schema.Never,
  idempotencyKey: (input) => input.key,
  decide: (view, input) => {
    const count = getEDAReducerState(view.reducerStates, factCounter);
    const fact = {
      namespace: "test.model",
      type: "Fact",
      schemaVersion: schemaV1,
      payload: { count, head: view.state.lastSeq, origin: input.key },
    } as const;
    return {
      _tag: "Accept",
      items:
        count % 2 === 0
          ? [
              fact,
              new SubmitMessageCommand({
                idempotencyKey: CommandIdempotencyKey.make(`decide:${input.key}`),
                disposition: "steer",
                content: [Prompt.textPart({ text: input.text })],
              }),
            ]
          : [fact],
      result: { count, head: view.state.lastSeq },
    };
  },
});

/** Stages one `Echo` per `Fact`, keyed by the fact, as a sink should. */
const echoSink = EDASink.make({
  name: "test.model.echo",
  state: "none",
  durable: {
    interests: ModelFact,
    process: (batch, ctx) =>
      Effect.forEach(batch.events, (entry) =>
        Effect.gen(function* () {
          yield* ctx.stageDurable(
            ModelEcho.make({
              namespace: "test.model",
              type: "Echo",
              schemaVersion: schemaV1,
              durability: "durable",
              eventId: yield* ctx.makeEventId(),
              sessionId: SessionId.make(SESSION_ID),
              createdAtMs: UnixEpochMillis.make(0),
              dedupeKey: EventDedupeKey.make(entry.event.eventId),
              payload: { of: entry.event.eventId },
            }),
          );
        }),
      ).pipe(Effect.asVoid),
  },
});

/** A tool-emitted fact, through the path `ctx.emitDurable` takes. */
const toolFact = (index: number) =>
  ModelFact.make({
    namespace: "test.model",
    type: "Fact",
    schemaVersion: schemaV1,
    durability: "durable",
    eventId: EventId.make(sequentialUuidV7(50_000 + index)),
    sessionId: SessionId.make(SESSION_ID),
    createdAtMs: UnixEpochMillis.make(0),
    payload: { count: -1, head: -1, origin: `tool ${index}` },
  });

/** A store append the scenario holds open, inside the append gate, until it releases it. */
interface HeldAppend {
  readonly eventId: string;
  readonly reached: Deferred.Deferred<void>;
  readonly release: Deferred.Deferred<void>;
}

/** Holds the append that carries `held.current`'s event at the store, the boundary the test owns. */
const holdingStore =
  (held: { current: HeldAppend | undefined }) =>
  (inner: EDASessionStoreShape): EDASessionStoreShape => ({
    ...inner,
    append: (batch) =>
      Effect.gen(function* () {
        const hold = held.current;
        if (
          hold !== undefined &&
          batch.entries.some((entry) => entry.event.eventId === hold.eventId)
        ) {
          yield* Deferred.succeed(hold.reached, undefined);
          yield* Deferred.await(hold.release);
        }
        return yield* inner.append(batch);
      }),
  });

interface DecideRecord {
  readonly key: string;
  readonly input: DecideInput;
  readonly outcome: EDADeciderOutcome<{ readonly count: number; readonly head: number }, never>;
}

const makeCommand = (action: Exclude<ModelAction, "finish">, index: number): EDACommand => {
  const id = commandId(index);
  switch (action) {
    case "submitQueue":
      return new SubmitMessageCommand({
        commandId: id,
        disposition: "queue",
        content: [Prompt.textPart({ text: `queue ${index}` })],
      });
    case "submitSteer":
      return new SubmitMessageCommand({
        commandId: id,
        disposition: "steer",
        content: [Prompt.textPart({ text: `steer ${index}` })],
      });
    case "submitInterrupt":
      return new SubmitMessageCommand({
        commandId: id,
        disposition: "interrupt",
        content: [Prompt.textPart({ text: `interrupt ${index}` })],
      });
    case "stop":
      return new StopTurnCommand({ commandId: id });
  }
};

const modelStream = (
  index: number,
  gate: Deferred.Deferred<void>,
): Stream.Stream<InferenceRunnerStreamPart> =>
  Stream.make(
    Response.makePart("text-delta", { id: `text-${index}`, delta: `answer ${index}` }),
  ).pipe(
    Stream.concat(
      Stream.fromEffect(
        Deferred.await(gate).pipe(
          Effect.as(
            Response.makePart("finish", {
              reason: "stop",
              usage: usage(),
              response: undefined,
            }),
          ),
        ),
      ),
    ),
  );

const collectCommitted = (store: EDASessionStore) =>
  store.eventsAfter(SequenceNumber.make(0)).pipe(
    Stream.runCollect,
    Effect.map((entries) => Array.from(entries)),
  );

const drainOnce = (dispatcher: SessionStateShape) => dispatcher.drainReadyWork({ modelSelection });

const settleAfterAction = (dispatcher: SessionStateShape) =>
  Effect.gen(function* () {
    yield* Effect.yieldNow;
    yield* drainOnce(dispatcher);
  });

const releaseStartedStreams = (
  gates: ReadonlyArray<Deferred.Deferred<void>>,
  startedStreams: ReadonlyArray<number>,
  released: Set<number>,
) =>
  Effect.forEach(startedStreams, (index) => {
    if (released.has(index)) {
      return Effect.void;
    }
    released.add(index);
    const gate = gates[index];
    return gate === undefined ? Effect.void : Deferred.succeed(gate, undefined).pipe(Effect.ignore);
  });

const finalFlush = (
  dispatcher: SessionStateShape,
  gates: ReadonlyArray<Deferred.Deferred<void>>,
  startedStreams: ReadonlyArray<number>,
  released: Set<number>,
) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      yield* releaseStartedStreams(gates, startedStreams, released);
      yield* Effect.yieldNow;
      yield* drainOnce(dispatcher);
      const snapshot = yield* dispatcher.snapshot();
      const recoverable = classifyRecoverableWork(snapshot);
      if (
        snapshot.commandQueues.pendingSteers.length === 0 &&
        snapshot.commandQueues.activeControlCommands.length === 0 &&
        snapshot.commandQueues.active === undefined &&
        recoverable.activeCommands.length === 0 &&
        recoverable.activeRuns.length === 0 &&
        recoverable.activeTurns.length === 0 &&
        recoverable.activeInferences.length === 0 &&
        recoverable.openToolCalls.length === 0 &&
        recoverable.runningToolCalls.length === 0 &&
        recoverable.pendingStopRequests.length === 0
      ) {
        return;
      }
    }
    return yield* Effect.die(new Error("Timed out flushing generated SessionState scenario"));
  });

const interpretAction = (input: {
  readonly dispatcher: SessionStateShape;
  readonly action: ModelAction;
  readonly actionIndex: number;
  readonly commandCounter: { value: number };
  readonly admissions: Array<CommandAdmission>;
  readonly gates: ReadonlyArray<Deferred.Deferred<void>>;
  readonly startedStreams: ReadonlyArray<number>;
  readonly released: Set<number>;
  readonly decisions: Array<DecideRecord>;
  readonly held: { current: HeldAppend | undefined };
}) =>
  Effect.gen(function* () {
    if (input.action === "decide" || input.action === "retryDecide") {
      const earlier = input.decisions.at(input.actionIndex % Math.max(1, input.decisions.length));
      const key =
        input.action === "retryDecide" && earlier !== undefined
          ? earlier.key
          : `key-${input.actionIndex}`;
      const decideInput = { key, text: `decide ${input.actionIndex}` };
      // A tool's fact is mid-append, inside the gate, when the decision starts. The decision must
      // read the state that append commits, never the state from before it.
      const fact = toolFact(input.actionIndex);
      const hold: HeldAppend = {
        eventId: fact.eventId,
        reached: yield* Deferred.make<void>(),
        release: yield* Deferred.make<void>(),
      };
      input.held.current = hold;
      const emitting = yield* Effect.forkChild(input.dispatcher.appendAppDurable(fact));
      yield* Deferred.await(hold.reached);
      const deciding = yield* Effect.forkChild(input.dispatcher.decide(modelDecider, decideInput));
      // Let the decision run until it waits on the gate the held append owns.
      for (let step = 0; step < 10; step += 1) yield* Effect.yieldNow;
      yield* Deferred.succeed(hold.release, undefined);
      yield* Fiber.join(emitting);
      const outcome = yield* Fiber.join(deciding);
      input.held.current = undefined;
      input.decisions.push({ key, input: decideInput, outcome });
      yield* settleAfterAction(input.dispatcher);
      return;
    }
    if (input.action === "finish") {
      yield* releaseStartedStreams(input.gates, input.startedStreams, input.released);
      yield* settleAfterAction(input.dispatcher);
      return;
    }

    input.commandCounter.value += 1;
    const command = makeCommand(input.action, input.commandCounter.value);
    input.admissions.push({ commandId: command.commandId, command });
    yield* input.dispatcher
      .admitCommand(command)
      .pipe(Effect.catchTag("SessionCommandAdmissionConflict", () => Effect.void));
    yield* settleAfterAction(input.dispatcher);
  });

const runScenario = (actions: ReadonlyArray<ModelAction>) =>
  Effect.gen(function* () {
    const streamCount = actions.length * 3 + 10;
    const gates = yield* Effect.forEach(
      Array.from({ length: streamCount }, () => undefined),
      () => Deferred.make<void>(),
    );
    const startedStreams: Array<number> = [];
    const released = new Set<number>();
    const admissions: Array<CommandAdmission> = [];
    const decisions: Array<DecideRecord> = [];
    const commandCounter = { value: 0 };
    const held: { current: HeldAppend | undefined } = { current: undefined };
    const streams = gates.map((gate, index) => modelStream(index, gate));

    return yield* Effect.scoped(
      Effect.gen(function* () {
        const dispatcher = yield* SessionState;
        const store = yield* EDASessionStore;
        const query = yield* EDASessionQuery;

        for (let actionIndex = 0; actionIndex < actions.length; actionIndex += 1) {
          yield* interpretAction({
            dispatcher,
            action: actions[actionIndex] as ModelAction,
            actionIndex,
            commandCounter,
            admissions,
            gates,
            startedStreams,
            released,
            decisions,
            held,
          });
        }

        yield* finalFlush(dispatcher, gates, startedStreams, released);
        // The echo sink may still append, so read the snapshots before the log.
        const snapshot = yield* dispatcher.snapshot();
        const messages = durableMessageTranscript((yield* query.snapshot()).state);
        const committed = yield* collectCommitted(store);
        return { admissions, committed, decisions, messages, snapshot };
      }).pipe(
        Effect.provide(
          makeEdaTestLayer({
            sessionId: SessionId.make(SESSION_ID),
            parts: streams,
            toolSchemas: new Map([["noop", NoopParams]]),
            onStreamText: ({ index }) => startedStreams.push(index),
            appEvents: ModelEvents,
            reducers: [factCounter],
            deciders: [modelDecider],
            sinks: [echoSink],
            wrapStore: holdingStore(held),
          }),
        ),
      ),
    );
  });

describe("SessionState control loop model", () => {
  it("preserves durable lifecycle invariants across generated command-control sequences", async () => {
    await fc.assert(
      fc.asyncProperty(actionArbitrary, async (actions) => {
        const result = await Effect.runPromise(runScenario(actions));
        assertDurableSequences(result.committed);
        assertLifecycleTerminals(result.committed);
        assertQueuedCommandSerialization(result.committed);
        assertFinalReducedState(result.committed);
        assertDecisionsReplay(result.committed, result.decisions);
        const throughSnapshot = result.committed.filter(
          (entry) => entry.position.seq <= result.snapshot.lastSeq,
        );
        expect(result.snapshot).toEqual(reduceCommittedEvents(throughSnapshot));
        expect(result.messages).toEqual(durableMessageTranscript(result.snapshot));
      }),
      { numRuns: propertyRuns },
    );
  });
});

/**
 * The atomicity claim as a property. For every marker at seq s, deciding again on the fold of the
 * log through s - 1, with the recorded input and the marker's clock, yields exactly the events
 * that follow the marker. One marker per key, and every replay returned the first result.
 */
const assertDecisionsReplay = (
  committed: ReadonlyArray<CommittedDurableEvent>,
  decisions: ReadonlyArray<DecideRecord>,
) => {
  const markerEntries = committed.filter((entry) => entry.event.type === "DeciderAccepted");
  const keys = markerEntries.map(
    (entry) => Schema.decodeUnknownSync(DeciderAcceptedEvent)(entry.event).payload.idempotencyKey,
  );
  expect(new Set(keys).size).toBe(keys.length);
  for (const marker of markerEntries) {
    const envelope = Schema.decodeUnknownSync(DeciderAcceptedEvent)(marker.event);
    const accepted = decisions.find(
      (decision) =>
        decision.key === envelope.payload.idempotencyKey &&
        decision.outcome._tag === "Accepted" &&
        !decision.outcome.replayed,
    );
    if (accepted === undefined)
      throw new Error(`no accepted call for ${envelope.payload.idempotencyKey}`);
    const index = committed.indexOf(marker);
    const prefix = committed.slice(0, index);
    const view = {
      sessionId: SessionId.make(SESSION_ID),
      nowMs: envelope.createdAtMs,
      state: reduceCommittedEvents(prefix),
      reducerStates: new Map([
        [factCounter.name, prefix.reduce(factCounter.reduce, factCounter.initial)],
      ]),
    };
    const decision = modelDecider.decide(view, accepted.input);
    if (decision._tag !== "Accept") throw new Error("model decider never rejects");
    expect(envelope.payload.result).toEqual(decision.result);
    let next = index + 1;
    for (const item of decision.items) {
      if ("_tag" in item) {
        const admitted = committed[next];
        expect(admitted?.event.type).toBe("CommandAdmitted");
        expect(JSON.stringify(admitted?.event.payload)).toContain(
          `"idempotencyKey":"decide:${envelope.payload.idempotencyKey}"`,
        );
        expect(committed[next + 1]?.event.type).toBe("UserMessageSubmitted");
        next += 2;
        continue;
      }
      const written = committed[next];
      expect(written === undefined ? undefined : durableEventEnvelope(written.event)).toMatchObject(
        { ...item, createdAtMs: envelope.createdAtMs },
      );
      next += 1;
    }
  }
  for (const decision of decisions) {
    if (decision.outcome._tag !== "Accepted" || !decision.outcome.replayed) continue;
    const first = decisions.find(
      (other) =>
        other.key === decision.key && other.outcome._tag === "Accepted" && !other.outcome.replayed,
    );
    expect(first?.outcome).toEqual({ ...decision.outcome, replayed: false });
  }
};

const assertDurableSequences = (committed: ReadonlyArray<CommittedDurableEvent>) => {
  committed.forEach((entry, index) => {
    expect(entry.position).toEqual({ seq: index + 1, subSeq: 0 });
  });
};

const assertFinalReducedState = (committed: ReadonlyArray<CommittedDurableEvent>) => {
  const state = reduceCommittedEvents(committed);
  const recoverable = classifyRecoverableWork(state);

  expect(state.commandQueues.pendingSteers).toEqual([]);
  expect(state.commandQueues.active).toBeUndefined();
  expect(recoverable.activeCommands).toEqual([]);
  expect(recoverable.activeRuns).toEqual([]);
  expect(recoverable.activeTurns).toEqual([]);
  expect(recoverable.activeInferences).toEqual([]);
  expect(recoverable.openToolCalls).toEqual([]);
  expect(recoverable.runningToolCalls).toEqual([]);
  expect(recoverable.pendingStopRequests).toEqual([]);
};

const assertLifecycleTerminals = (committed: ReadonlyArray<CommittedDurableEvent>) => {
  assertExactlyOneTerminal(committed, {
    startedType: "CommandStarted",
    startedId: (entry) => entry.event.payload.commandId,
    terminalTypes: ["CommandCompleted", "CommandFailed", "CommandCancelled"],
    terminalId: (entry) => entry.event.payload.commandId,
  });
  assertExactlyOneTerminal(committed, {
    startedType: "RunStarted",
    startedId: (entry) => entry.event.payload.runId,
    terminalTypes: ["RunCompleted", "RunFailed", "RunInterrupted"],
    terminalId: (entry) => entry.event.payload.runId,
  });
  assertExactlyOneTerminal(committed, {
    startedType: "TurnStarted",
    startedId: (entry) => entry.event.payload.turnId,
    terminalTypes: ["TurnCompleted", "TurnFailed", "TurnStopped"],
    terminalId: (entry) => entry.event.payload.turnId,
  });
  assertExactlyOneTerminal(committed, {
    startedType: "InferenceStarted",
    startedId: (entry) => entry.event.payload.inferenceId,
    terminalTypes: ["InferenceCompleted", "InferenceFailed"],
    terminalId: (entry) => entry.event.payload.inferenceId,
  });
};

const assertExactlyOneTerminal = (
  committed: ReadonlyArray<CommittedDurableEvent>,
  input: {
    readonly startedType: string;
    readonly terminalTypes: ReadonlyArray<string>;
    readonly startedId: (entry: CommittedDurableEvent) => string;
    readonly terminalId: (entry: CommittedDurableEvent) => string;
  },
) => {
  const started = new Map<string, number>();
  const terminals = new Map<string, Array<number>>();

  committed.forEach((entry, index) => {
    if (entry.event.type === input.startedType) {
      const id = input.startedId(entry);
      expect(started.has(id)).toBe(false);
      started.set(id, index);
    }
    if (input.terminalTypes.includes(entry.event.type)) {
      const id = input.terminalId(entry);
      terminals.set(id, [...(terminals.get(id) ?? []), index]);
    }
  });

  for (const [id, startedIndex] of started) {
    const terminalIndexes = terminals.get(id) ?? [];
    expect(terminalIndexes).toHaveLength(1);
    expect(terminalIndexes[0]).toBeGreaterThan(startedIndex);
  }
  for (const id of terminals.keys()) {
    expect(started.has(id)).toBe(true);
  }
};

const assertQueuedCommandSerialization = (committed: ReadonlyArray<CommittedDurableEvent>) => {
  const admitted = new Map<string, EDACommand>();
  const starts = new Map<string, number>();
  const terminals = new Map<string, number>();

  committed.forEach((entry, index) => {
    switch (entry.event.type) {
      case "CommandAdmitted":
        admitted.set(entry.event.payload.command.commandId, entry.event.payload.command);
        break;
      case "CommandStarted":
        starts.set(entry.event.payload.commandId, index);
        break;
      case "CommandCompleted":
      case "CommandFailed":
      case "CommandCancelled":
        terminals.set(entry.event.payload.commandId, index);
        break;
    }
  });

  for (const [commandId, command] of admitted) {
    if (command._tag !== "SubmitMessage" || command.disposition !== "queue") {
      continue;
    }
    const startIndex = starts.get(commandId);
    if (startIndex === undefined) {
      continue;
    }
    for (const [otherId, otherStartIndex] of starts) {
      if (otherId === commandId || otherStartIndex >= startIndex) {
        continue;
      }
      expect(terminals.get(otherId)).toBeLessThan(startIndex);
    }
  }
};
