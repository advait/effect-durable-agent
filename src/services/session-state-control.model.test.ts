import * as fc from "fast-check";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";
import { describe, expect, it } from "vite-plus/test";

import { classifyRecoverableWork, reduceCommittedEvents } from "../domain/reduced-state";
import { durableMessageTranscript } from "../domain/message-transcript";
import { StopTurnCommand, SubmitMessageCommand } from "../types/commands";
import type { EDACommand } from "../types/commands";
import { CommandId, SequenceNumber, SessionId } from "../types/core";
import { EDASessionStore } from "./session-store";
import type { CommittedDurableEvent } from "./session-store";
import { sequentialUuidV7 } from "./id-generator";
import { EDASessionQuery } from "./session-query";
import { SessionState } from "./session-state";
import type { SessionStateShape } from "./session-state";
import type { InferenceRunnerStreamPart } from "./inference-runner";
import { makeEdaTestLayer } from "../testkit/layers";
import { NoopParams, SESSION_ID, modelSelection, usage } from "./session-state-control-testkit";

const propertyRuns = 25;
const maxActions = 12;

type ModelAction = "submitQueue" | "submitSteer" | "submitInterrupt" | "stop" | "finish";

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
      ),
      { maxLength: maxActions - 1 },
    ),
  )
  .map(([first, rest]) => [first, ...rest] as ReadonlyArray<ModelAction>);

const commandId = (index: number) => CommandId.make(sequentialUuidV7(10_000 + index));

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
}) =>
  Effect.gen(function* () {
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
    const commandCounter = { value: 0 };
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
          });
        }

        yield* finalFlush(dispatcher, gates, startedStreams, released);
        const committed = yield* collectCommitted(store);
        const snapshot = yield* dispatcher.snapshot();
        const messages = yield* query.messages();
        return { admissions, committed, messages, snapshot };
      }).pipe(
        Effect.provide(
          makeEdaTestLayer({
            sessionId: SessionId.make(SESSION_ID),
            parts: streams,
            toolSchemas: new Map([["noop", NoopParams]]),
            onStreamText: ({ index }) => startedStreams.push(index),
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
        expect(result.snapshot).toEqual(reduceCommittedEvents(result.committed));
        expect(result.messages).toEqual(durableMessageTranscript(result.snapshot));
      }),
      { numRuns: propertyRuns },
    );
  });
});

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
