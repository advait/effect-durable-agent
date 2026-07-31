import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Response from "effect/unstable/ai/Response";
import * as Tool from "effect/unstable/ai/Tool";
import { describe, expect, it } from "vite-plus/test";

import {
  classifyRecoverableWork,
  reduceCommittedEvents,
  type ReducedState,
} from "../domain/reduced-state";
import { CommandId, SequenceNumber, SessionId } from "../types/core";
import type { EDADurableEvent } from "../types/events";
import {
  EDASessionStore,
  type CommittedDurableEvent,
  type EDASessionStoreShape,
} from "./session-store";
import { sequentialUuidV7 } from "./id-generator";
import { LiveEventBus } from "./live-event-bus";
import { SessionState } from "./session-state";
import type { SessionStateShape } from "./session-state";
import { makeEDAToolkit, type EDAModelToolkit } from "./tool-registry";
import type { InferenceRunnerStreamPart } from "./inference-runner";
import { makeEdaTestLayer, type TestModelParts } from "../testkit/layers";
import {
  ASSISTANT_MESSAGE_EVENT_ID,
  ASSISTANT_MESSAGE_ID,
  INFERENCE_COMPLETED_EVENT_ID,
  INFERENCE_ID,
  INFERENCE_STARTED_EVENT_ID,
  COMMAND_ADMITTED_EVENT_ID,
  COMMAND_COMPLETED_EVENT_ID,
  COMMAND_ID,
  COMMAND_STARTED_EVENT_ID,
  SECOND_COMMAND_ID,
  RUN_COMPLETED_EVENT_ID,
  RUN_ID,
  RUN_STARTED_EVENT_ID,
  SESSION_ID,
  TEXT_EVENT_ID,
  TURN_COMPLETED_EVENT_ID,
  TURN_ID,
  TURN_STARTED_EVENT_ID,
  USER_MESSAGE_EVENT_ID,
  USER_MESSAGE_ID,
  collectCommitted,
  command,
  hasCommandCompleted,
  hasEventType,
  interruptCommand,
  NoopParams,
  modelSelection,
  secondCommand,
  steerCommand,
  stopTurnCommand,
  usage,
} from "./session-state-control-testkit";

interface DurableCheckpoint {
  readonly operation: string;
  readonly committed: ReadonlyArray<CommittedDurableEvent>;
}

interface RecoveredSession {
  readonly committed: ReadonlyArray<CommittedDurableEvent>;
  readonly snapshot: ReducedState;
}

interface CrashGoldenResult<Context> {
  readonly committed: ReadonlyArray<CommittedDurableEvent>;
  readonly context: Context;
}

interface CrashRecoveryResult<Context> extends RecoveredSession {
  readonly context: Context;
}

interface CrashPrefixScenario<GoldenContext, RecoveryContext> {
  readonly name: string;
  readonly runGolden: (
    wrapStore: (inner: EDASessionStoreShape) => EDASessionStoreShape,
  ) => Promise<CrashGoldenResult<GoldenContext>>;
  readonly recover: (
    checkpoint: DurableCheckpoint,
    context: GoldenContext,
  ) => Effect.Effect<CrashRecoveryResult<RecoveryContext>>;
  readonly assertGolden?: (input: {
    readonly committed: ReadonlyArray<CommittedDurableEvent>;
    readonly checkpoints: ReadonlyArray<DurableCheckpoint>;
    readonly context: GoldenContext;
  }) => void;
  readonly assertCheckpoint?: (input: {
    readonly checkpoint: DurableCheckpoint;
    readonly recovered: ReadonlyArray<CommittedDurableEvent>;
    readonly snapshot: ReducedState;
    readonly goldenContext: GoldenContext;
    readonly recoveryContext: RecoveryContext;
  }) => void;
}

const plainSubmitIds = [
  COMMAND_ADMITTED_EVENT_ID,
  COMMAND_STARTED_EVENT_ID,
  USER_MESSAGE_ID,
  USER_MESSAGE_EVENT_ID,
  RUN_ID,
  RUN_STARTED_EVENT_ID,
  TURN_ID,
  TURN_STARTED_EVENT_ID,
  INFERENCE_ID,
  INFERENCE_STARTED_EVENT_ID,
  TEXT_EVENT_ID,
  INFERENCE_COMPLETED_EVENT_ID,
  ASSISTANT_MESSAGE_ID,
  ASSISTANT_MESSAGE_EVENT_ID,
  TURN_COMPLETED_EVENT_ID,
  RUN_COMPLETED_EVENT_ID,
  COMMAND_COMPLETED_EVENT_ID,
];

const toolSubmitIds = Array.from({ length: 40 }, (_, index) => sequentialUuidV7(1_000 + index));
const stopTurnIds = Array.from({ length: 80 }, (_, index) => sequentialUuidV7(2_000 + index));
const controlIds = Array.from({ length: 140 }, (_, index) => sequentialUuidV7(3_000 + index));
const NoopTool = Tool.make("noop", { parameters: NoopParams, success: Schema.Unknown });

const plainSubmitStream = (): Stream.Stream<InferenceRunnerStreamPart, unknown> =>
  Stream.make(
    Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
    Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
  );

const stopTurnStream = (): Stream.Stream<InferenceRunnerStreamPart, unknown> =>
  Stream.make(Response.makePart("text-delta", { id: "text-stop", delta: "partial answer" })).pipe(
    Stream.concat(Stream.never),
  );

const controlActiveStream = (): Stream.Stream<InferenceRunnerStreamPart, unknown> =>
  Stream.make(
    Response.makePart("text-delta", { id: "text-control", delta: "active partial" }),
  ).pipe(Stream.concat(Stream.never));

const completedTextStream = (
  id: string,
  delta: string,
): Stream.Stream<InferenceRunnerStreamPart, unknown> =>
  Stream.make(
    Response.makePart("text-delta", { id, delta }),
    Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
  );

const controlStreams = (): TestModelParts => [
  controlActiveStream(),
  completedTextStream("text-interrupt", "replacement answer"),
  completedTextStream("text-queued", "queued answer"),
  completedTextStream("text-steer-recovered", "steer recovered answer"),
  completedTextStream("text-interrupt-recovered", "interrupt recovered answer"),
];

const controlRecoveryStreams = (): TestModelParts => [
  completedTextStream("text-recovery-1", "recovered answer 1"),
  completedTextStream("text-recovery-2", "recovered answer 2"),
  completedTextStream("text-recovery-3", "recovered answer 3"),
  completedTextStream("text-recovery-4", "recovered answer 4"),
  completedTextStream("text-recovery-5", "recovered answer 5"),
];

const toolSubmitStreams = (): TestModelParts => [
  Stream.make(
    Response.makePart("tool-call", {
      id: "tool-call-1",
      name: "noop",
      params: {},
      providerExecuted: false,
    }),
    Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
  ),
  Stream.make(
    Response.makePart("text-delta", { id: "text-2", delta: "done" }),
    Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
  ),
];

const countingNoopToolkit = (calls: { value: number }): EDAModelToolkit =>
  Effect.runSync(
    makeEDAToolkit([NoopTool], {
      noop: () =>
        Effect.sync(() => {
          calls.value += 1;
          return { ok: true, call: calls.value };
        }),
    }),
  );

describe("SessionState crash-point simulation", () => {
  it("recovers from every batch-aligned durable prefix of a plain submit run", async () => {
    await runCrashPrefixSimulation({
      name: "plain submit",
      runGolden: async (wrapStore) => ({
        committed: await Effect.runPromise(runPlainSubmit({ wrapStore })),
        context: undefined,
      }),
      recover: (checkpoint) =>
        recoverWithoutContext(recoverFromCheckpoint(checkpoint, { parts: plainSubmitStream() })),
      assertGolden: ({ committed, checkpoints }) => {
        expect(committed.map((entry) => entry.event.type)).toEqual([
          "CommandAdmitted",
          "UserMessageSubmitted",
          "CommandStarted",
          "RunStarted",
          "TurnStarted",
          "InferenceStarted",
          "AssistantMessageCommitted",
          "InferenceCompleted",
          "TurnCompleted",
          "RunCompleted",
          "CommandCompleted",
        ]);
        expect(checkpoints.map(eventTypesAtCheckpoint)).toEqual([
          ["CommandAdmitted", "UserMessageSubmitted"],
          ["CommandAdmitted", "UserMessageSubmitted", "CommandStarted", "RunStarted"],
          [
            "CommandAdmitted",
            "UserMessageSubmitted",
            "CommandStarted",
            "RunStarted",
            "TurnStarted",
          ],
          [
            "CommandAdmitted",
            "UserMessageSubmitted",
            "CommandStarted",
            "RunStarted",
            "TurnStarted",
            "InferenceStarted",
          ],
          [
            "CommandAdmitted",
            "UserMessageSubmitted",
            "CommandStarted",
            "RunStarted",
            "TurnStarted",
            "InferenceStarted",
            "AssistantMessageCommitted",
            "InferenceCompleted",
          ],
          [
            "CommandAdmitted",
            "UserMessageSubmitted",
            "CommandStarted",
            "RunStarted",
            "TurnStarted",
            "InferenceStarted",
            "AssistantMessageCommitted",
            "InferenceCompleted",
            "TurnCompleted",
          ],
          [
            "CommandAdmitted",
            "UserMessageSubmitted",
            "CommandStarted",
            "RunStarted",
            "TurnStarted",
            "InferenceStarted",
            "AssistantMessageCommitted",
            "InferenceCompleted",
            "TurnCompleted",
            "RunCompleted",
            "CommandCompleted",
          ],
        ]);
      },
    });
  });

  it("recovers tool execution without rerunning completed or abandoned tool calls", async () => {
    await runCrashPrefixSimulation({
      name: "tool execution",
      runGolden: async (wrapStore) => {
        const goldenToolCalls = { value: 0 };
        return {
          committed: await Effect.runPromise(
            runToolSubmit({ wrapStore, toolkit: countingNoopToolkit(goldenToolCalls) }),
          ),
          context: { goldenToolCalls },
        };
      },
      recover: (checkpoint) => {
        const recoveredToolCalls = { value: 0 };
        return recoverFromCheckpoint(checkpoint, {
          parts: plainSubmitStream(),
          toolkit: countingNoopToolkit(recoveredToolCalls),
        }).pipe(Effect.map((recovered) => ({ ...recovered, context: { recoveredToolCalls } })));
      },
      assertGolden: ({ committed, checkpoints, context }) => {
        const operations = checkpoints.map((checkpoint) => checkpoint.operation);
        expect(context.goldenToolCalls.value).toBe(1);
        expect(countEventType(committed, "ToolCallCreated")).toBe(1);
        expect(countEventType(committed, "ToolCallStarted")).toBe(1);
        expect(countEventType(committed, "ToolCallCompleted")).toBe(1);
        expect(countEventType(committed, "InferenceStarted")).toBeGreaterThanOrEqual(2);
        expect(indexOfFirstType(committed, "ToolCallCreated")).toBeLessThan(
          indexOfFirstType(committed, "ToolCallStarted"),
        );
        expect(indexOfFirstType(committed, "ToolCallStarted")).toBeLessThan(
          indexOfFirstType(committed, "ToolCallCompleted"),
        );
        expect(operations).toEqual(
          expect.arrayContaining([
            "CommandAdmitted+UserMessageSubmitted",
            "CommandStarted+RunStarted",
            "TurnStarted",
            "InferenceStarted",
            "AssistantMessageCommitted+ToolCallCreated+InferenceCompleted",
            "ToolCallStarted",
            "ToolCallCompleted",
            "RunCompleted+CommandCompleted",
          ]),
        );
      },
      assertCheckpoint: ({ checkpoint, recovered, recoveryContext }) => {
        assertToolRecoveryPolicy({
          checkpoint,
          recovered,
          recoveredToolCalls: recoveryContext.recoveredToolCalls,
        });
      },
    });
  });

  it("recovers stopped in-flight turns and preserves durable partials once committed", async () => {
    await runCrashPrefixSimulation({
      name: "StopTurn",
      runGolden: async (wrapStore) => ({
        committed: await Effect.runPromise(runStoppedSubmit({ wrapStore })),
        context: undefined,
      }),
      recover: (checkpoint) => recoverWithoutContext(recoverStoppedCheckpoint(checkpoint)),
      assertGolden: ({ committed, checkpoints }) => {
        expect(committed.map((entry) => entry.event.type)).toEqual([
          "CommandAdmitted",
          "UserMessageSubmitted",
          "CommandStarted",
          "RunStarted",
          "TurnStarted",
          "InferenceStarted",
          "CommandAdmitted",
          "CommandStarted",
          "StopTurnRequested",
          "AssistantPartialCommitted",
          "InferenceFailed",
          "TurnFailed",
          "RunFailed",
          "CommandCancelled",
          "StopTurnApplied",
          "CommandCompleted",
        ]);
        expect(checkpoints.map((checkpoint) => checkpoint.operation)).toEqual([
          "CommandAdmitted+UserMessageSubmitted",
          "CommandStarted+RunStarted",
          "TurnStarted",
          "InferenceStarted",
          "CommandAdmitted",
          "CommandStarted",
          "StopTurnRequested",
          "AssistantPartialCommitted+InferenceFailed",
          "TurnFailed+RunFailed+CommandCancelled",
          "StopTurnApplied+CommandCompleted",
        ]);
      },
      assertCheckpoint: ({ checkpoint, recovered }) => {
        assertStopRecoveryPolicy({ checkpoint, recovered });
      },
    });
  });

  it("recovers queued, steering, and interrupt command-control prefixes", async () => {
    await runCrashPrefixSimulation({
      name: "command-control",
      runGolden: async (wrapStore) => ({
        committed: await Effect.runPromise(runControlSubmit({ wrapStore })),
        context: undefined,
      }),
      recover: (checkpoint) => recoverWithoutContext(recoverControlCheckpoint(checkpoint)),
      assertGolden: ({ committed, checkpoints }) => {
        expect(
          indexOfCommandEvent(committed, "CommandStarted", steerCommand.commandId),
        ).toBeGreaterThan(indexOfFirstType(committed, "InferenceStarted"));
        expect(
          indexOfCommandEvent(committed, "CommandCompleted", steerCommand.commandId),
        ).toBeGreaterThan(indexOfCommandEvent(committed, "CommandStarted", steerCommand.commandId));
        expect(
          indexOfCommandEvent(committed, "CommandStarted", interruptCommand.commandId),
        ).toBeGreaterThan(
          indexOfCommandEvent(committed, "CommandCompleted", steerCommand.commandId),
        );
        expect(
          indexOfCommandEvent(committed, "CommandStarted", CommandId.make(SECOND_COMMAND_ID)),
        ).toBe(-1);
        expect(countEventType(committed, "PendingMessagesPaused")).toBe(1);
        expect(countEventType(committed, "SteeringMessageQueued")).toBe(0);
        expect(countEventType(committed, "SteeringMessageCancelled")).toBe(0);
        expect(countEventType(committed, "AssistantPartialCommitted")).toBe(1);
        expect(committed.map((entry) => entry.event.type)).toEqual(
          expect.arrayContaining([
            "CommandAdmitted",
            "UserMessageSubmitted",
            "InferenceFailed",
            "RunFailed",
            "CommandCancelled",
            "RunCompleted",
            "CommandCompleted",
          ]),
        );
        expect(
          checkpoints.some((checkpoint) => checkpoint.operation.includes("PendingMessagesPaused")),
        ).toBe(true);
      },
      assertCheckpoint: ({ checkpoint, recovered }) => {
        assertControlRecoveryPolicy({ checkpoint, recovered });
      },
    });
  });
});

const runCrashPrefixSimulation = async <GoldenContext, RecoveryContext>(
  scenario: CrashPrefixScenario<GoldenContext, RecoveryContext>,
): Promise<void> => {
  const recorder = makeDurableCheckpointRecorder();
  const golden = await scenario.runGolden(recorder.wrapStore);

  scenario.assertGolden?.({
    committed: golden.committed,
    checkpoints: recorder.checkpoints,
    context: golden.context,
  });

  for (const checkpoint of recorder.checkpoints) {
    const recovered = await recoverWithCheckpointContext(scenario.name, checkpoint, () =>
      scenario.recover(checkpoint, golden.context),
    );

    assertCommonRecovery({ scenarioName: scenario.name, checkpoint, recovered });
    scenario.assertCheckpoint?.({
      checkpoint,
      recovered: recovered.committed,
      snapshot: recovered.snapshot,
      goldenContext: golden.context,
      recoveryContext: recovered.context,
    });
  }
};

const assertCommonRecovery = (input: {
  readonly scenarioName: string;
  readonly checkpoint: DurableCheckpoint;
  readonly recovered: RecoveredSession;
}) => {
  assertRecoveredPrefix(input.checkpoint, input.recovered.committed);
  assertDurableSequences(input.recovered.committed);
  assertNoRecoverableWork(input.recovered.snapshot);
  assertLifecycleTerminals(input.recovered.committed);
  expect(input.recovered.snapshot).toEqual(reduceCommittedEvents(input.recovered.committed));
};

const recoverWithoutContext = (
  effect: Effect.Effect<RecoveredSession>,
): Effect.Effect<CrashRecoveryResult<undefined>> =>
  effect.pipe(Effect.map((recovered) => ({ ...recovered, context: undefined })));

const recoverWithCheckpointContext = async <A>(
  scenarioName: string,
  checkpoint: DurableCheckpoint,
  recover: () => Effect.Effect<A, never, never>,
): Promise<A> => {
  try {
    return await Effect.runPromise(recover());
  } catch (error) {
    throw new Error(
      `Recovery failed in ${scenarioName} after ${checkpoint.operation}: ${eventTypesAtCheckpoint(checkpoint).join(" -> ")}`,
      { cause: error },
    );
  }
};

const runPlainSubmit = (input: {
  readonly wrapStore?: (inner: EDASessionStoreShape) => EDASessionStoreShape;
}) =>
  runSubmitScenario({
    ids: plainSubmitIds,
    parts: plainSubmitStream(),
    expectedTurnCompletions: 1,
    wrapStore: input.wrapStore,
  });

const runToolSubmit = (input: {
  readonly toolkit: EDAModelToolkit;
  readonly wrapStore?: (inner: EDASessionStoreShape) => EDASessionStoreShape;
}) =>
  runSubmitScenario({
    ids: toolSubmitIds,
    parts: toolSubmitStreams(),
    expectedTurnCompletions: 2,
    toolkit: input.toolkit,
    wrapStore: input.wrapStore,
  });

const runStoppedSubmit = (input: {
  readonly wrapStore?: (inner: EDASessionStoreShape) => EDASessionStoreShape;
}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const sessionState = yield* SessionState;
      const durableStore = yield* EDASessionStore;
      const textSeen = yield* waitForLiveType("TextDelta");

      yield* sessionState.admitCommand(command);
      yield* sessionState.drainReadyWork({ modelSelection });
      yield* Fiber.join(textSeen);
      yield* sessionState.admitCommand(stopTurnCommand);
      yield* sessionState.drainReadyWork({ modelSelection });
      return yield* waitForDurableStore(durableStore, hasEventType("StopTurnApplied"));
    }),
  ).pipe(
    Effect.provide(
      makeEdaTestLayer({
        sessionId: SessionId.make(SESSION_ID),
        ids: stopTurnIds,
        parts: stopTurnStream(),
        wrapStore: input.wrapStore,
      }),
    ),
  );

const runControlSubmit = (input: {
  readonly wrapStore?: (inner: EDASessionStoreShape) => EDASessionStoreShape;
}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const sessionState = yield* SessionState;
      const durableStore = yield* EDASessionStore;
      const textSeen = yield* waitForLiveType("TextDelta");

      yield* sessionState.admitCommand(command);
      yield* sessionState.drainReadyWork({ modelSelection });
      yield* Fiber.join(textSeen);
      yield* sessionState.admitCommand(secondCommand);
      yield* sessionState.admitCommand(steerCommand);
      yield* sessionState.admitCommand(interruptCommand);
      yield* sessionState.drainReadyWork({ modelSelection });
      yield* waitForDurableStore(
        durableStore,
        (entries) => countEventType(entries, "TurnCompleted") >= 1,
      );
      yield* sessionState.drainReadyWork({ modelSelection });
      return yield* waitForDurableStore(
        durableStore,
        hasCommandCompleted(interruptCommand.commandId),
      );
    }),
  ).pipe(
    Effect.provide(
      makeEdaTestLayer({
        sessionId: SessionId.make(SESSION_ID),
        ids: controlIds,
        parts: controlStreams(),
        wrapStore: input.wrapStore,
      }),
    ),
  );

const runSubmitScenario = (input: {
  readonly ids: ReadonlyArray<string>;
  readonly parts: TestModelParts;
  readonly expectedTurnCompletions: number;
  readonly toolkit?: EDAModelToolkit;
  readonly wrapStore?: (inner: EDASessionStoreShape) => EDASessionStoreShape;
}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const sessionState = yield* SessionState;
      const durableStore = yield* EDASessionStore;

      yield* sessionState.admitCommand(command);
      yield* sessionState.drainReadyWork({ modelSelection });
      for (let count = 1; count <= input.expectedTurnCompletions; count += 1) {
        yield* waitForDurableStore(
          durableStore,
          (entries) => countEventType(entries, "TurnCompleted") >= count,
        );
        yield* sessionState.drainReadyWork({ modelSelection });
        const committed = yield* collectCommitted(durableStore);
        if (hasCommandCompleted(CommandId.make(COMMAND_ID))(committed)) {
          return committed;
        }
      }
      return yield* waitForDurableStore(
        durableStore,
        hasCommandCompleted(CommandId.make(COMMAND_ID)),
      );
    }),
  ).pipe(
    Effect.provide(
      makeEdaTestLayer({
        sessionId: SessionId.make(SESSION_ID),
        ids: input.ids,
        parts: input.parts,
        ...(input.toolkit === undefined ? {} : { toolkit: input.toolkit }),
        wrapStore: input.wrapStore,
      }),
    ),
  );

const recoverStoppedCheckpoint = (checkpoint: DurableCheckpoint) =>
  recoverFromCheckpoint(checkpoint, {
    parts: plainSubmitStream(),
  });

const recoverControlCheckpoint = (checkpoint: DurableCheckpoint) =>
  recoverFromCheckpoint(checkpoint, {
    parts: checkpoint.committed.length === 2 ? plainSubmitStream() : controlRecoveryStreams(),
  });

const recoverFromCheckpoint = (
  checkpoint: DurableCheckpoint,
  input: { readonly parts: TestModelParts; readonly toolkit?: EDAModelToolkit },
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const sessionState = yield* SessionState;
      const durableStore = yield* EDASessionStore;
      yield* sessionState.start({ modelSelection });
      return yield* waitForRecoveredStable(sessionState, durableStore);
    }),
  ).pipe(
    Effect.provide(
      makeEdaTestLayer({
        sessionId: SessionId.make(SESSION_ID),
        seedEvents: checkpoint.committed.map((entry) => entry.event),
        parts: input.parts,
        ...(input.toolkit === undefined ? {} : { toolkit: input.toolkit }),
      }),
    ),
  );

const waitForLiveType = (type: string) =>
  Effect.gen(function* () {
    const liveBus = yield* LiveEventBus;
    const liveStream = yield* liveBus.subscribe();
    return yield* liveStream.pipe(
      Stream.filter((event) => event.event.type === type),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  });

const waitForDurableStore = (
  store: EDASessionStoreShape,
  predicate: (committed: ReadonlyArray<CommittedDurableEvent>) => boolean,
) =>
  Effect.gen(function* () {
    let lastCommitted: ReadonlyArray<CommittedDurableEvent> = [];
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const committed = yield* collectCommitted(store);
      lastCommitted = committed;
      if (predicate(committed)) {
        return committed;
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(
      new Error(
        `Timed out waiting for durable events: ${lastCommitted
          .map((entry) => entry.event.type)
          .join(", ")}`,
      ),
    );
  });

const waitForRecoveredStable = (sessionState: SessionStateShape, store: EDASessionStoreShape) =>
  Effect.gen(function* () {
    let lastCommitted: ReadonlyArray<CommittedDurableEvent> = [];
    let lastSnapshot: ReducedState | undefined;
    for (let attempt = 0; attempt < 200; attempt += 1) {
      const committed = yield* collectCommitted(store);
      const snapshot = yield* sessionState.snapshot();
      lastCommitted = committed;
      lastSnapshot = snapshot;
      if (isStable(snapshot)) {
        return { committed, snapshot };
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(
      new Error(
        `Timed out waiting for recovered session to stabilize: ${summarizeRecoveredState(
          lastCommitted,
          lastSnapshot,
        )}`,
      ),
    );
  });

const makeDurableCheckpointRecorder = () => {
  const checkpoints: Array<DurableCheckpoint> = [];
  const record = (inner: EDASessionStoreShape, operation: string) =>
    collectCommitted(inner).pipe(
      Effect.tap((committed) =>
        Effect.sync(() => {
          checkpoints.push({ operation, committed });
        }),
      ),
    );

  const wrapStore = (inner: EDASessionStoreShape): EDASessionStoreShape => ({
    ...inner,
    append: (batch) =>
      inner
        .append(batch)
        .pipe(
          Effect.tap(() => record(inner, batch.entries.map((entry) => entry.event.type).join("+"))),
        ),
  });

  return { checkpoints, wrapStore };
};

const eventTypesAtCheckpoint = (checkpoint: DurableCheckpoint): ReadonlyArray<string> =>
  checkpoint.committed.map((entry) => entry.event.type);

const summarizeRecoveredState = (
  committed: ReadonlyArray<CommittedDurableEvent>,
  snapshot: ReducedState | undefined,
): string => {
  if (snapshot === undefined) {
    return JSON.stringify({ committedTypes: committed.map((entry) => entry.event.type) });
  }
  const recoverable = classifyRecoverableWork(snapshot);
  return JSON.stringify({
    committedTypes: committed.map((entry) => entry.event.type),
    pendingCommands: snapshot.commandQueues.pendingCommands.map((entry) => entry.commandId),
    active: snapshot.commandQueues.active,
    queuedCommands: snapshot.commandQueues.queuedCommands.map((entry) => entry.commandId),
    activeControlCommands: snapshot.commandQueues.activeControlCommands.map(
      (entry) => entry.commandId,
    ),
    steeringByRun: Array.from(snapshot.commandQueues.steeringByRun, ([runId, messages]) => ({
      runId,
      messageIds: messages.map((message) => message.messageId),
    })),
    recoverable: {
      activeCommands: recoverable.activeCommands.map((entry) => entry.commandId),
      activeRuns: recoverable.activeRuns.map((entry) => entry.runId),
      activeTurns: recoverable.activeTurns.map((entry) => entry.turnId),
      activeInferences: recoverable.activeInferences.map((entry) => entry.inferenceId),
      openToolCalls: recoverable.openToolCalls.map((entry) => entry.toolCallId),
      runningToolCalls: recoverable.runningToolCalls.map((entry) => entry.toolCallId),
      pendingStopRequests: recoverable.pendingStopRequests.map((entry) => entry.commandId),
    },
  });
};

const isStable = (snapshot: ReducedState): boolean => {
  const recoverable = classifyRecoverableWork(snapshot);
  const pendingCommandIds = new Set(
    snapshot.commandQueues.pendingCommands.map((command) => command.commandId),
  );
  return (
    snapshot.commandQueues.pendingQueue.every(
      (message) => !pendingCommandIds.has(message.commandId),
    ) &&
    snapshot.commandQueues.pendingSteers.length === 0 &&
    snapshot.commandQueues.active === undefined &&
    Array.from(snapshot.commandQueues.steeringByRun.values()).every(
      (messages) => messages.length === 0,
    ) &&
    recoverable.activeCommands.length === 0 &&
    recoverable.activeRuns.length === 0 &&
    recoverable.activeTurns.length === 0 &&
    recoverable.activeInferences.length === 0 &&
    recoverable.openToolCalls.length === 0 &&
    recoverable.runningToolCalls.length === 0 &&
    recoverable.pendingStopRequests.length === 0
  );
};

const assertRecoveredPrefix = (
  checkpoint: DurableCheckpoint,
  recovered: ReadonlyArray<CommittedDurableEvent>,
) => {
  expect(recovered.length).toBeGreaterThanOrEqual(checkpoint.committed.length);
  checkpoint.committed.forEach((entry, index) => {
    expect(recovered[index]).toEqual(entry);
  });
};

const assertNoRecoverableWork = (snapshot: ReducedState) => {
  expect(isStable(snapshot)).toBe(true);
};

const assertDurableSequences = (committed: ReadonlyArray<CommittedDurableEvent>) => {
  committed.forEach((entry, index) => {
    expect(entry.position).toEqual({ seq: SequenceNumber.make(index + 1), subSeq: 0 });
  });
};

const assertLifecycleTerminals = (committed: ReadonlyArray<CommittedDurableEvent>) => {
  assertExactlyOneTerminal(committed, {
    startedType: "CommandStarted",
    terminalTypes: ["CommandCompleted", "CommandFailed", "CommandCancelled"],
    idField: "commandId",
  });
  assertExactlyOneTerminal(committed, {
    startedType: "RunStarted",
    terminalTypes: ["RunCompleted", "RunFailed", "RunInterrupted"],
    idField: "runId",
  });
  assertExactlyOneTerminal(committed, {
    startedType: "TurnStarted",
    terminalTypes: ["TurnCompleted", "TurnFailed", "TurnStopped"],
    idField: "turnId",
  });
  assertExactlyOneTerminal(committed, {
    startedType: "InferenceStarted",
    terminalTypes: ["InferenceCompleted", "InferenceFailed"],
    idField: "inferenceId",
  });
  assertExactlyOneTerminal(committed, {
    startedType: "ToolCallCreated",
    terminalTypes: ["ToolCallCompleted", "ToolCallFailed"],
    idField: "toolCallId",
  });
};

const assertToolRecoveryPolicy = (input: {
  readonly checkpoint: DurableCheckpoint;
  readonly recovered: ReadonlyArray<CommittedDurableEvent>;
  readonly recoveredToolCalls: { readonly value: number };
}) => {
  const prefixTypes = eventTypesAtCheckpoint(input.checkpoint);
  const recoveredTypes = input.recovered.map((entry) => entry.event.type);
  const prefixHasCompletedTool = prefixTypes.includes("ToolCallCompleted");
  const prefixHasOpenTool =
    prefixTypes.includes("ToolCallCreated") && !hasToolTerminal(input.checkpoint.committed);
  expect(input.recoveredToolCalls.value).toBe(0);

  if (prefixHasCompletedTool) {
    expect(countEventType(input.recovered, "ToolCallCompleted")).toBe(1);
  }
  if (prefixHasOpenTool) {
    expect(recoveredTypes).toContain("ToolCallFailed");
    expect(indexOfFirstType(input.recovered, "ToolCallFailed")).toBeGreaterThan(
      input.checkpoint.committed.length - 1,
    );
  }
};

const assertStopRecoveryPolicy = (input: {
  readonly checkpoint: DurableCheckpoint;
  readonly recovered: ReadonlyArray<CommittedDurableEvent>;
}) => {
  const prefixTypes = eventTypesAtCheckpoint(input.checkpoint);
  const recoveredTypes = input.recovered.map((entry) => entry.event.type);
  const prefixHasStopRequest = prefixTypes.includes("StopTurnRequested");
  const prefixHasPartial = prefixTypes.includes("AssistantPartialCommitted");

  if (prefixHasStopRequest) {
    expect(recoveredTypes).toContain("StopTurnApplied");
  }
  if (prefixHasPartial) {
    expect(countEventType(input.recovered, "AssistantPartialCommitted")).toBe(1);
    const partial = input.recovered.find(
      (entry) => entry.event.type === "AssistantPartialCommitted",
    );
    expect(partial).toMatchObject({
      event: {
        payload: {
          promptParts: [{ type: "text", text: "partial answer" }],
          reason: "inference interrupted before completion",
        },
      },
    });
    expect(indexOfFirstType(input.recovered, "AssistantPartialCommitted")).toBeLessThan(
      indexOfFirstType(input.recovered, "InferenceFailed"),
    );
  }
  if (prefixHasStopRequest && !prefixHasPartial) {
    expect(indexOfFirstType(input.recovered, "StopTurnApplied")).toBeGreaterThan(
      input.checkpoint.committed.length - 1,
    );
  }
};

const assertControlRecoveryPolicy = (input: {
  readonly checkpoint: DurableCheckpoint;
  readonly recovered: ReadonlyArray<CommittedDurableEvent>;
}) => {
  const prefixTypes = eventTypesAtCheckpoint(input.checkpoint);
  const recoveredTypes = input.recovered.map((entry) => entry.event.type);
  const prefixHasSteering = prefixTypes.includes("SteeringMessageQueued");
  const prefixHasInterruptedRun = prefixTypes.includes("RunFailed");
  const prefixHasReplacementStarted =
    indexOfCommandEvent(input.checkpoint.committed, "CommandStarted", interruptCommand.commandId) >=
    0;

  if (prefixHasSteering) {
    expect(recoveredTypes).toContain("SteeringMessageCancelled");
    expect(countEventType(input.recovered, "SteeringMessageQueued")).toBe(1);
    expect(countEventType(input.recovered, "SteeringMessageCancelled")).toBe(1);
  }
  if (prefixHasInterruptedRun) {
    expect(countEventType(input.recovered, "RunFailed")).toBeGreaterThanOrEqual(1);
  }
  if (prefixHasReplacementStarted) {
    expect(
      indexOfCommandEvent(input.recovered, "CommandStarted", CommandId.make(SECOND_COMMAND_ID)),
    ).toBe(-1);
  }
};

const hasToolTerminal = (committed: ReadonlyArray<CommittedDurableEvent>): boolean =>
  committed.some((entry) => ["ToolCallCompleted", "ToolCallFailed"].includes(entry.event.type));

const countEventType = (committed: ReadonlyArray<CommittedDurableEvent>, type: string): number =>
  committed.filter((entry) => entry.event.type === type).length;

const indexOfFirstType = (committed: ReadonlyArray<CommittedDurableEvent>, type: string): number =>
  committed.findIndex((entry) => entry.event.type === type);

const indexOfCommandEvent = (
  committed: ReadonlyArray<CommittedDurableEvent>,
  type: string,
  commandId: CommandId,
): number =>
  committed.findIndex(
    (entry) =>
      entry.event.type === type &&
      "commandId" in entry.event.payload &&
      entry.event.payload.commandId === commandId,
  );

const assertExactlyOneTerminal = (
  committed: ReadonlyArray<CommittedDurableEvent>,
  input: {
    readonly startedType: string;
    readonly terminalTypes: ReadonlyArray<string>;
    readonly idField: string;
  },
) => {
  const started = new Map<string, number>();
  const terminals = new Map<string, Array<number>>();

  committed.forEach((entry, index) => {
    const id = payloadField(entry.event, input.idField);
    if (id === undefined) {
      return;
    }
    if (entry.event.type === input.startedType) {
      expect(started.has(id)).toBe(false);
      started.set(id, index);
    }
    if (input.terminalTypes.includes(entry.event.type)) {
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

const payloadField = (event: EDADurableEvent, field: string): string | undefined => {
  const payload = event.payload as Record<string, unknown>;
  const value = payload[field];
  return typeof value === "string" ? value : undefined;
};
