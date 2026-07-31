import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import type * as Tracer from "effect/Tracer";
import * as Prompt from "effect/unstable/ai/Prompt";

import { assertNeverError } from "../domain/assert-never";
import {
  DispatchActiveCommand,
  decideDispatch,
  type DispatchCommandCandidate,
} from "../domain/dispatch-policy";
import {
  activeTurnIdentityForCommand,
  classifyRecoverableWork,
  decodeReducedStateCheckpoint,
  encodeReducedStateCheckpoint,
  foldReducedState,
  frameworkReducedStateReducerName,
  frameworkReducedStateReducerSchemaVersion,
  initialReducedState,
  reducedStateCheckpointEventSeqs,
  type CommandRecord,
  type ReducedState,
  type ToolCallRecord,
} from "../domain/reduced-state";
import { ContextProjection, emptyContextProjection } from "../domain/context-projection";
import { decideRunContinuation } from "../domain/run-continuation-policy";
import { isSessionRecoveryPlanEmpty, planSessionRecovery } from "../domain/recovery-policy";
import { planRunFailure, planUserInterruption } from "../domain/run-terminal-policy";
import {
  CancelPendingMessageCommand,
  PromotePendingMessageCommand,
  ResumePendingMessagesCommand,
  StopTurnCommand,
  SubmitMessageCommand,
  UserMessageContent,
  type EDACommand,
} from "../types/commands";
import {
  CommandId,
  MessageId,
  Position,
  RunId,
  SequenceNumber,
  SubSequenceNumber,
  TurnId,
} from "../types/core";
import {
  DurableEventEnvelope,
  EphemeralEventEnvelope,
  FailurePayload,
  ModelSelectionPayload,
  NonNegativeInt,
  PositionedEvent,
  SystemPromptText,
  commandAdmittedEventType,
  turnFailedEventType,
  type ToolCallFailedPayload,
  type TurnFailedEvent,
} from "../types/events";
import {
  CommittedDurableEvent,
  DurableAppendEntry,
  EDASessionStore,
  EDASessionStoreError,
  hasEDASessionStoreError,
  type EDAReducerCheckpoint,
  type EDASessionStoreShape,
  type SaveReducerCheckpointInput,
} from "./session-store";
import { CompactionRunner } from "./compaction";
import { EventFactory } from "./event-factory";
import { IdGenerator } from "./id-generator";
import { EDAKeepAlive } from "./keep-alive";
import { LiveEventBus } from "./live-event-bus";
import {
  buildEDAPrompt,
  EDAPromptProjector,
  type EDAPromptProjectionInput,
} from "./prompt-projector";
import {
  decodeReducerState,
  encodeReducerState,
  EDAReducerRegistry,
  reducerSchemaVersion,
  type EDAReducer,
  type EDAReducerStateSnapshot,
} from "./reducer-registry";
import { EDASinkRegistry } from "./sink-registry";
import { SessionContext } from "./session-context";
import type { SessionEventSink } from "./session-event-sink";
import { failurePayloadFromCause, makeStartedBoundaryGuard } from "./started-boundary-guard";
import {
  annotateEdaSpan,
  committedBatchAttributes,
  edaTraceContextFromSpan,
  eventBatchAttributes,
  makeEDARunTrace,
  toEdaExternalSpan,
} from "./tracing";
import type { EDARunTrace, EDATraceLink } from "../types/tracing";
import { TurnRunner, type TurnRunResult } from "./turn-runner";
import type { TurnRunnerError } from "./turn-runner";

interface EphemeralAnchorState {
  readonly anchorSeq: SequenceNumber;
  readonly lastSubSeq: SubSequenceNumber;
}

interface SessionStateData {
  readonly reduced: ReducedState;
  readonly reducerStates: EDAReducerStateSnapshot;
  readonly ephemeral: EphemeralAnchorState;
}

interface HydratedFrameworkReducedState {
  readonly state: ReducedState;
  readonly checkpointSeq: SequenceNumber;
}

interface HydratedAppReducerStates {
  readonly states: EDAReducerStateSnapshot;
  readonly checkpointSeq?: SequenceNumber;
}

interface ReducerCheckpointSnapshot {
  readonly reason: "turn-boundary" | "startup-recovery-complete";
  readonly reduced: ReducedState;
  readonly reducerStates: EDAReducerStateSnapshot;
  readonly updatedAtMs: number;
}

interface PublishedDurables {
  readonly head: SequenceNumber;
}

/** Authoritative framework and app reducer state snapshot. */
export interface SessionStateSnapshotData {
  readonly reduced: ReducedState;
  readonly reducerStates: EDAReducerStateSnapshot;
}

interface StartTurnInput {
  readonly commandId: CommandId;
  readonly commandStarted: CommittedDurableEvent;
  readonly runId: RunId;
  readonly modelSelection: ModelSelectionPayload;
  readonly maxToolCallsPerRun?: NonNegativeInt;
  readonly promptInput: EDAPromptProjectionInput;
  readonly runScope: Scope.Closeable;
  readonly runSpan: Tracer.Span;
}

interface ActiveRunTrace {
  readonly runScope: Scope.Closeable;
  readonly runSpan: Tracer.Span;
  readonly runTrace: EDARunTrace;
}

/** Input for one session control pass or long-lived control process. */
export const SessionRunInput = Schema.Struct({
  modelSelection: ModelSelectionPayload,
  maxToolCallsPerRun: Schema.optionalKey(NonNegativeInt),
  systemPrompt: Schema.optionalKey(SystemPromptText),
});
export type SessionRunInput = typeof SessionRunInput.Type;

/** Command outcome when the command's run completed and the command was completed. */
export const SessionCommandCompleted = Schema.TaggedStruct("SessionCommandCompleted", {
  committed: CommittedDurableEvent,
});
export type SessionCommandCompleted = typeof SessionCommandCompleted.Type;

/** Command outcome when the command's run failed and the command was failed. */
export const SessionCommandFailed = Schema.TaggedStruct("SessionCommandFailed", {
  committed: CommittedDurableEvent,
});
export type SessionCommandFailed = typeof SessionCommandFailed.Type;

/** Command outcome when command work was cancelled by stop, interrupt, or recovery. */
export const SessionCommandCancelled = Schema.TaggedStruct("SessionCommandCancelled", {
  committed: CommittedDurableEvent,
});
export type SessionCommandCancelled = typeof SessionCommandCancelled.Type;

/** Terminal command-processing variants returned from one control action. */
export const SessionCommandOutcome = Schema.Union([
  SessionCommandCompleted,
  SessionCommandFailed,
  SessionCommandCancelled,
]);
export type SessionCommandOutcome = typeof SessionCommandOutcome.Type;

/** Summary returned after processing one admitted command. */
export interface SessionCommandResult {
  readonly started: CommittedDurableEvent;
  readonly outcome: SessionCommandOutcome;
}

/** Why a control drain stopped without starting more command work. */
export const SessionNoRunnableReason = Schema.Literals(["active-command", "no-pending-command"]);
export type SessionNoRunnableReason = typeof SessionNoRunnableReason.Type;

/** Session control result when no admitted command is runnable. */
export const SessionNoRunnableCommand = Schema.TaggedStruct("SessionNoRunnableCommand", {
  reason: SessionNoRunnableReason,
  active: Schema.optionalKey(DispatchActiveCommand),
});
export type SessionNoRunnableCommand = typeof SessionNoRunnableCommand.Type;

/** Control result when command execution was started in an active child scope. */
export const SessionForkedCommand = Schema.TaggedStruct("SessionForkedCommand", {
  commandId: CommandId,
});
export type SessionForkedCommand = typeof SessionForkedCommand.Type;

/** One processed control-loop action from a drain pass. */
export type SessionDrainProcessed = SessionCommandResult | SessionForkedCommand;

/** Summary of a control-loop drain pass and the reason it stopped. */
export interface SessionDrainResult {
  readonly _tag: "SessionDrainResult";
  readonly processed: ReadonlyArray<SessionDrainProcessed>;
  readonly stop: SessionNoRunnableCommand;
}

type ExecutionState =
  | { readonly _tag: "Idle" }
  | {
      readonly _tag: "Running";
      readonly commandId: CommandId;
      readonly commandStarted: CommittedDurableEvent;
      readonly runId: RunId;
      readonly turnId: TurnId;
      readonly modelSelection: ModelSelectionPayload;
      readonly maxToolCallsPerRun?: NonNegativeInt;
      readonly runScope: Scope.Closeable;
      readonly runSpan: Tracer.Span;
      readonly turnScope: Scope.Closeable;
      readonly fiber: Fiber.Fiber<TurnRunResult, SessionStateError>;
    }
  | {
      readonly _tag: "Completed";
      readonly commandId: CommandId;
      readonly commandStarted: CommittedDurableEvent;
      readonly runId: RunId;
      readonly turnId: TurnId;
      readonly modelSelection: ModelSelectionPayload;
      readonly maxToolCallsPerRun?: NonNegativeInt;
      readonly runScope: Scope.Closeable;
      readonly runSpan: Tracer.Span;
      readonly turn: TurnRunResult;
    };

type RunningExecution = Extract<ExecutionState, { readonly _tag: "Running" }>;
type CompletedExecution = Extract<ExecutionState, { readonly _tag: "Completed" }>;

type ExecutionInspection =
  | { readonly _tag: "RunningTurn"; readonly active: RunningExecution }
  | {
      readonly _tag: "CompletedTurn";
      readonly active: CompletedExecution;
      readonly turn: TurnRunResult;
    }
  | {
      readonly _tag: "FailedTurn";
      readonly result: SessionCommandResult;
    };

/** Error surface for session control-loop execution. */
export type SessionStateError = EDASessionStoreError | TurnRunnerError;

export class SessionCommandAdmissionConflict extends Schema.TaggedErrorClass<SessionCommandAdmissionConflict>()(
  "SessionCommandAdmissionConflict",
  {
    code: Schema.Literals(["queue_paused", "paused_queue_changed"]),
    message: Schema.String,
    pausedMessageIds: Schema.Array(MessageId),
  },
) {}

/** Error surface for durable command admission and batched submit. */
export type SessionCommandAdmissionError = EDASessionStoreError | SessionCommandAdmissionConflict;

/** Ordered item admitted through the public runtime submit batch. */
export type EDASubmittable = EDACommand | DurableEventEnvelope;

/** Internal live-process authority for commands, durable writes, and scheduling. */
export interface SessionStateShape extends SessionEventSink {
  /** Commit session-initial context, such as app-owned system prompt, before command work starts. */
  readonly initialize: (
    input: SessionRunInput,
  ) => Effect.Effect<void, SessionCommandAdmissionError>;
  /** Submit commands and/or app durable events as one ordered durable batch. */
  readonly submitBatch: (
    input: ReadonlyArray<EDASubmittable>,
  ) => Effect.Effect<ReadonlyArray<CommittedDurableEvent>, SessionCommandAdmissionError>;
  /** Admit an external command durably and wake the control loop. */
  readonly admitCommand: (
    command: EDACommand,
  ) => Effect.Effect<CommittedDurableEvent, SessionCommandAdmissionError>;
  /** Observe the authoritative live reduced state. Use for reads only, not detached mutation. */
  readonly snapshot: () => Effect.Effect<ReducedState>;
  /** Observe the authoritative framework state plus app reducer states. */
  readonly snapshotData: () => Effect.Effect<SessionStateSnapshotData>;
  /**
   * Process runnable work until blocked or empty after startup recovery has completed.
   *
   * This is a post-recovery dispatch helper. If durable replay says a command is
   * already active while no in-memory execution owner exists, that is a fatal
   * live-runtime invariant violation rather than an inline recovery case.
   */
  readonly drainReadyWork: (
    input: SessionRunInput,
  ) => Effect.Effect<SessionDrainResult, SessionStateError>;
  /** Initialize, recover exactly once, then enter the long-lived live control loop. */
  readonly start: (input: SessionRunInput) => Effect.Effect<void, SessionCommandAdmissionError>;
}

/** Internal authoritative live state and root control loop for one session. */
export class SessionState extends Context.Service<SessionState, SessionStateShape>()(
  "@effect-durable-agent/SessionState",
) {
  static readonly Live = Layer.effect(
    SessionState,
    Effect.suspend(() => makeLiveSessionState),
  );
}

const defaultMaxToolCallsPerRun = NonNegativeInt.make(20);
const maxToolRejectionCorrections = 1;

const toolCancelledPromptPartFromRecord = (
  tool: ToolCallRecord,
  error: FailurePayload,
): ToolCallFailedPayload["promptPart"] => {
  const decision = tool.decision;
  if (decision?._tag !== "Created") {
    throw new Error(
      "Cannot build cancellation failure prompt part without a created tool decision",
    );
  }
  return {
    ...Prompt.toolResultPart({
      id: decision.providerPartId,
      name: decision.toolName,
      isFailure: true,
      result: error,
    }),
    id: decision.providerPartId,
    name: decision.toolName,
    isFailure: true,
    result: error,
  };
};

const toolCancelledFailureFromRecord = (tool: ToolCallRecord, reason: string): FailurePayload =>
  FailurePayload.make({
    message: `tool call interrupted: ${reason}`,
    code: "tool.interrupted",
    details: { reason, toolCallId: tool.toolCallId },
  });

/** Build the canonical failed-terminal payload for an interrupted lifecycle. */
const interruptionFailure = (
  lifecycle: "turn" | "run",
  reason: string,
  details: Readonly<Record<string, unknown>>,
  recoverable = reason === startupRecoveryReason,
): FailurePayload =>
  FailurePayload.make({
    message: `${lifecycle} interrupted: ${reason}`,
    code: `${lifecycle}.interrupted`,
    details: { ...details, reason, recoverable },
  });

const startupRecoveryReason = "runtime restarted before lifecycle completed";
const makeLiveSessionState = Effect.gen(function* () {
  const store = yield* EDASessionStore;
  const liveBus = yield* LiveEventBus;
  const keepAlive = yield* EDAKeepAlive;
  const ids = yield* IdGenerator;
  const events = yield* EventFactory;
  const turnRunner = yield* TurnRunner;
  const compactionRunner = yield* CompactionRunner;
  const promptProjector = yield* EDAPromptProjector;
  const sessionContext = yield* SessionContext;
  const reducerRegistry = yield* EDAReducerRegistry;
  const sinkRegistry = yield* EDASinkRegistry;
  const hydrated = yield* hydrateFrameworkReducedState(store);
  const hydratedReducerStates = yield* hydrateAppReducerStates(store, reducerRegistry.reducers);
  const startupRecoveryCheckpointSeq =
    hydratedReducerStates.checkpointSeq === undefined
      ? hydrated.checkpointSeq
      : SequenceNumber.make(Math.min(hydrated.checkpointSeq, hydratedReducerStates.checkpointSeq));
  const state = yield* Ref.make<SessionStateData>({
    reduced: hydrated.state,
    reducerStates: hydratedReducerStates.states,
    ephemeral: {
      anchorSeq: SequenceNumber.make(0),
      lastSubSeq: SubSequenceNumber.make(0),
    },
  });
  const gate = yield* Semaphore.make(1);
  const ingressSignals = yield* Queue.sliding<void>(1);
  // Checkpoints are derived caches: keep only the latest pending boundary snapshot
  // so slow storage writes cannot back up the session engine. Dropping older
  // pending snapshots is intentional; replay from the durable event log repairs gaps.
  const reducerCheckpointSnapshots = yield* Queue.sliding<ReducerCheckpointSnapshot>(1);
  const executionState = yield* Ref.make<ExecutionState>({ _tag: "Idle" });
  const startupRecoveryCheckpointRequested = yield* Ref.make(false);
  const fatalCause = yield* Ref.make<Cause.Cause<EDASessionStoreError> | undefined>(undefined);
  const fatalInvariantViolation = yield* Ref.make<string | undefined>(undefined);
  const runtimeScope = yield* Effect.acquireRelease(Scope.make("sequential"), (scope) =>
    Scope.close(scope, Exit.void),
  );

  const failIfFatal = Effect.gen(function* () {
    const invariantViolation = yield* Ref.get(fatalInvariantViolation);
    if (invariantViolation !== undefined) {
      return yield* Effect.die(invariantViolation);
    }
    const cause = yield* Ref.get(fatalCause);
    if (cause !== undefined) {
      return yield* Effect.failCause(cause);
    }
  });

  const markFatalIfDurable = (cause: Cause.Cause<unknown>) =>
    hasEDASessionStoreError(cause)
      ? Ref.set(fatalCause, cause as Cause.Cause<EDASessionStoreError>)
      : Effect.void;

  const withAppendGate = <A, E>(effect: Effect.Effect<A, E>) =>
    gate.withPermits(1)(
      Effect.uninterruptible(
        Effect.gen(function* () {
          yield* failIfFatal;
          return yield* effect;
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              yield* markFatalIfDurable(cause);
              return yield* Effect.failCause(cause);
            }),
          ),
        ),
      ),
    );

  const withFatalRecording = <A, E extends SessionStateError, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.catchCause(effect, (cause) =>
      Effect.gen(function* () {
        yield* markFatalIfDurable(cause);
        return yield* Effect.failCause(cause);
      }),
    );

  const buildReducerCheckpointSnapshot = (
    snapshot: ReducerCheckpointSnapshot,
  ): ReadonlyArray<SaveReducerCheckpointInput> => [
    {
      name: frameworkReducedStateReducerName,
      schemaVersion: frameworkReducedStateReducerSchemaVersion,
      throughSeq: snapshot.reduced.lastSeq,
      payload: encodeReducedStateCheckpoint(snapshot.reduced),
      updatedAtMs: snapshot.updatedAtMs,
    },
    ...reducerRegistry.reducers.map((reducer) => ({
      name: reducer.name,
      schemaVersion: reducerSchemaVersion(reducer),
      throughSeq: snapshot.reduced.lastSeq,
      payload: encodeReducerState(
        reducer,
        snapshot.reducerStates.get(reducer.name) ?? reducer.initial,
      ),
      updatedAtMs: snapshot.updatedAtMs,
    })),
  ];

  const writeReducerCheckpointSnapshot = Effect.fnUntraced(function* (
    snapshot: ReducerCheckpointSnapshot,
  ) {
    const checkpoints = yield* Effect.sync(() => buildReducerCheckpointSnapshot(snapshot));
    yield* store.saveReducerCheckpoints(checkpoints).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("EDA reducer checkpoint snapshot skipped", {
          cause: Cause.pretty(cause),
          reducerNames: checkpoints.map((checkpoint) => checkpoint.name),
          reason: snapshot.reason,
          throughSeq: snapshot.reduced.lastSeq,
        }),
      ),
    );
  });

  yield* Effect.forever(
    Effect.gen(function* () {
      const snapshot = yield* Queue.take(reducerCheckpointSnapshots);
      yield* Effect.yieldNow;
      yield* writeReducerCheckpointSnapshot(snapshot).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("EDA reducer checkpoint snapshot worker recovered from defect", {
            cause: Cause.pretty(cause),
            reason: snapshot.reason,
            throughSeq: snapshot.reduced.lastSeq,
          }),
        ),
      );
    }),
  ).pipe(Effect.forkIn(runtimeScope), Effect.asVoid);

  const requestReducerCheckpointSnapshot = (snapshot: ReducerCheckpointSnapshot) =>
    Queue.offer(reducerCheckpointSnapshots, snapshot).pipe(Effect.asVoid);

  const publishFreshDurables = (
    committed: ReadonlyArray<CommittedDurableEvent>,
  ): Effect.Effect<PublishedDurables | undefined> =>
    Effect.gen(function* () {
      const current = yield* Ref.get(state);
      const fresh = Array.from(
        committed.filter((entry) => entry.position.seq > current.reduced.lastSeq),
      ).sort((a, b) => a.position.seq - b.position.seq);

      if (fresh.length === 0) {
        return undefined;
      }

      const nextReduced = foldReducedState(current.reduced, fresh);
      const nextReducerStates = reducerRegistry.reduce(current.reducerStates, fresh);
      yield* Ref.set(state, {
        reduced: nextReduced,
        reducerStates: nextReducerStates,
        ephemeral: current.ephemeral,
      });
      for (const entry of fresh) {
        yield* liveBus.publish(PositionedEvent.make(entry));
      }
      return { head: nextReduced.lastSeq } satisfies PublishedDurables;
    });

  const snapshot = () => Ref.get(state).pipe(Effect.map((state) => state.reduced));
  const snapshotData = () =>
    Ref.get(state).pipe(
      Effect.map((state) => ({
        reduced: state.reduced,
        reducerStates: state.reducerStates,
      })),
    );

  /**
   * Append, fold, and publish one durable batch under the session append gate.
   *
   * `EDASessionStore` owns durable atomicity; `SessionState` deliberately owns
   * the post-commit fold/live-publish step so storage never depends on the live
   * bus and a crash after commit can be recovered by replay.
   */
  const commitDurableEntriesWithinGate = Effect.fn(function* (
    entries: ReadonlyArray<DurableAppendEntry>,
  ) {
    if (entries.length === 0) {
      return [];
    }
    yield* annotateEdaSpan(eventBatchAttributes(entries.map((entry) => entry.event)));
    const committed = yield* store.append({ entries });
    yield* annotateEdaSpan(committedBatchAttributes(committed));
    const published = yield* publishFreshDurables(committed);
    if (published !== undefined) {
      yield* sinkRegistry.notifyDurableHeadAdvanced(published.head);
    }
    return committed;
  });

  const appendDurableEntries = (entries: ReadonlyArray<DurableAppendEntry>) =>
    withAppendGate(commitDurableEntriesWithinGate(entries));

  const appendDurableBatch = (durableEvents: ReadonlyArray<DurableEventEnvelope>) =>
    appendDurableEntries(durableEvents.map((event) => ({ event })));

  const appendDurable = (event: DurableEventEnvelope) =>
    Effect.gen(function* () {
      const committed = yield* appendDurableBatch([event]);
      return committed[0] as CommittedDurableEvent;
    });

  const commitInitialSystemPrompt = (systemPrompt: SystemPromptText | undefined) =>
    Effect.gen(function* () {
      if (systemPrompt === undefined) {
        return;
      }
      const current = yield* Ref.get(state);
      if (current.reduced.lastSeq > SequenceNumber.make(0)) {
        return;
      }
      const event = yield* events.systemMessageCommitted({
        messageId: yield* ids.makeMessageId(),
        content: systemPrompt,
      });
      yield* appendDurable(event);
    });

  const publishEphemeralCore = (event: EphemeralEventEnvelope) =>
    withAppendGate(
      Effect.gen(function* () {
        const current = yield* Ref.get(state);
        const head = current.reduced.lastSeq;
        const nextSubSeq = SubSequenceNumber.make(
          current.ephemeral.anchorSeq === head ? current.ephemeral.lastSubSeq + 1 : 1,
        );
        const positioned = PositionedEvent.make({
          position: Position.make({ seq: head, subSeq: nextSubSeq }),
          event,
        });
        yield* Ref.set(state, {
          reduced: current.reduced,
          reducerStates: current.reducerStates,
          ephemeral: { anchorSeq: head, lastSubSeq: nextSubSeq },
        });
        yield* liveBus.publish(positioned);
        return positioned;
      }),
    );

  const publishEphemeral = (event: EphemeralEventEnvelope) =>
    Effect.gen(function* () {
      const positioned = yield* publishEphemeralCore(event);
      yield* sinkRegistry.publishEphemeralToSinks(positioned, publishEphemeralCore);
      return positioned;
    });

  const wakeAfterCommandAdmission = () => Queue.offer(ingressSignals, undefined);

  /** Use indexed store lookup for command idempotency instead of genesis replay. */
  const findExistingCommandAdmission = (command: EDACommand) =>
    Effect.gen(function* () {
      if (command.idempotencyKey === undefined && command.commandId === undefined) {
        return undefined;
      }
      return yield* store.findCommandAdmission({
        ...(command.commandId === undefined ? {} : { commandId: command.commandId }),
        ...(command.idempotencyKey === undefined ? {} : { idempotencyKey: command.idempotencyKey }),
      });
    });

  const prepareCommandAdmission = (
    command: EDACommand,
    preparedByIdempotencyKey: Map<string, DurableEventEnvelope>,
    preparedByCommandId: Map<string, DurableEventEnvelope>,
  ) =>
    Effect.gen(function* () {
      if (command.idempotencyKey !== undefined) {
        const prepared = preparedByIdempotencyKey.get(command.idempotencyKey);
        if (prepared !== undefined) {
          return [prepared];
        }
      }
      if (command.commandId !== undefined) {
        const prepared = preparedByCommandId.get(command.commandId);
        if (prepared !== undefined) {
          return [prepared];
        }
      }

      const existing = yield* findExistingCommandAdmission(command);
      if (existing !== undefined) {
        rememberPreparedCommand(existing.event, preparedByIdempotencyKey, preparedByCommandId);
        return [existing.event];
      }

      const commandId = command.commandId ?? (yield* ids.makeCommandId());
      const admitted = commandWithId(command, commandId);
      const event = yield* events.commandAdmitted({ command: admitted });
      rememberPreparedCommand(event, preparedByIdempotencyKey, preparedByCommandId);
      if (admitted._tag !== "SubmitMessage" || admitted.disposition === "interrupt") {
        return [event];
      }
      const current = yield* Ref.get(state);
      const pausedMessageIds = current.reduced.commandQueues.pausedQueue.map(
        (message) => message.messageId,
      );
      const requestedClear = admitted.expectedPausedMessageIdsToCancel;
      if (admitted.disposition === "queue" && pausedMessageIds.length > 0) {
        if (requestedClear === undefined) {
          return yield* new SessionCommandAdmissionConflict({
            code: "queue_paused",
            message: "Queue paused because you interrupted",
            pausedMessageIds,
          });
        }
        if (!sameMessageIdSet(requestedClear, pausedMessageIds)) {
          return yield* new SessionCommandAdmissionConflict({
            code: "paused_queue_changed",
            message: "Paused queue changed; confirm the current messages before clearing",
            pausedMessageIds,
          });
        }
      } else if (
        requestedClear !== undefined &&
        !sameMessageIdSet(requestedClear, pausedMessageIds)
      ) {
        return yield* new SessionCommandAdmissionConflict({
          code: "paused_queue_changed",
          message: "Paused queue changed; confirm the current messages before clearing",
          pausedMessageIds,
        });
      }
      const messageId = yield* ids.makeMessageId();
      const submitted = yield* events.userMessageSubmitted({
        commandId,
        messageId,
        disposition: admitted.disposition,
        content: admitted.content,
      });
      const clearEvents: Array<DurableEventEnvelope> = [];
      if (requestedClear !== undefined) {
        for (const pausedMessageId of requestedClear) {
          const pausedMessage = current.reduced.messages.get(pausedMessageId);
          if (pausedMessage?._tag !== "User" && pausedMessage?._tag !== "Steering") {
            continue;
          }
          clearEvents.push(
            yield* events.userMessageCancelled({
              commandId,
              messageId: pausedMessageId,
              reason: "clear-paused-queue",
            }),
          );
          const origin = current.reduced.commands.get(pausedMessage.commandId);
          if (
            origin !== undefined &&
            origin.startedSeq === undefined &&
            origin.terminal === undefined
          ) {
            clearEvents.push(
              yield* events.commandCancelled({
                commandId: origin.commandId,
                reason: "paused queue cleared by replacement message",
              }),
            );
          }
        }
      }
      return [event, ...clearEvents, submitted];
    });

  const submitBatch = Effect.fn(function* (input: ReadonlyArray<EDASubmittable>) {
    const commandCount = input.filter(isEDACommand).length;
    yield* annotateEdaSpan({
      "eda.command.submittable_count": input.length,
      "eda.command.count": commandCount,
      "eda.event.app_owned_count": input.length - commandCount,
    });
    const committed = yield* withAppendGate(
      Effect.gen(function* () {
        const preparedByIdempotencyKey = new Map<string, DurableEventEnvelope>();
        const preparedByCommandId = new Map<string, DurableEventEnvelope>();
        const durableEventGroups = yield* Effect.forEach(input, (item) =>
          isEDACommand(item)
            ? prepareCommandAdmission(item, preparedByIdempotencyKey, preparedByCommandId)
            : Effect.succeed([item]),
        );
        const durableEvents = durableEventGroups.flat();
        return yield* commitDurableEntriesWithinGate(durableEvents.map((event) => ({ event })));
      }),
    );
    yield* annotateEdaSpan(committedBatchAttributes(committed));
    if (commandCount > 0) {
      yield* wakeAfterCommandAdmission();
    }
    return committed;
  });

  const admitCommand = (command: EDACommand) =>
    Effect.gen(function* () {
      const committed = yield* submitBatch([command]);
      return committed[0] as CommittedDurableEvent;
    });

  const initialize = (input: SessionRunInput) => commitInitialSystemPrompt(input.systemPrompt);

  const sessionApi: SessionStateShape = {
    initialize,
    submitBatch,
    admitCommand,
    snapshot,
    snapshotData,
    appendDurable,
    appendDurableBatch,
    publishEphemeral,
    drainReadyWork: (input) => drainReadyWork(input),
    start: (input) => start(input),
  };

  const startedBoundaryGuard = makeStartedBoundaryGuard(sessionApi);

  const currentReduced = () => Ref.get(state).pipe(Effect.map((state) => state.reduced));
  const currentData = () => Ref.get(state);
  const contextProjectionFor = (reduced: ReducedState) =>
    Effect.gen(function* () {
      const summaryId = reduced.context.currentSummaryId;
      if (summaryId === undefined) {
        return emptyContextProjection;
      }
      const summary = yield* store.loadSummaryArtifact(summaryId);
      if (summary === undefined) {
        return yield* new EDASessionStoreError({
          message: `ReducedState references missing summary ${summaryId}`,
        });
      }
      return ContextProjection.make({
        contextVersion: reduced.context.version,
        currentSummary: summary,
      });
    });

  const requestCurrentReducerCheckpointSnapshot = (reason: ReducerCheckpointSnapshot["reason"]) =>
    Effect.gen(function* () {
      const current = yield* currentData();
      if (current.reduced.lastSeq <= SequenceNumber.make(0)) {
        return;
      }
      yield* requestReducerCheckpointSnapshot({
        reason,
        reduced: current.reduced,
        reducerStates: current.reducerStates,
        updatedAtMs: yield* Clock.currentTimeMillis,
      });
    });

  const requestStartupRecoveryCheckpointIfNeeded = () =>
    Effect.gen(function* () {
      if (yield* Ref.get(startupRecoveryCheckpointRequested)) {
        return;
      }
      const current = yield* currentData();
      if (current.reduced.lastSeq <= startupRecoveryCheckpointSeq) {
        return;
      }
      yield* Ref.set(startupRecoveryCheckpointRequested, true);
      yield* requestCurrentReducerCheckpointSnapshot("startup-recovery-complete");
    });

  const maybeCompactAtBoundary = () =>
    Effect.gen(function* () {
      const current = yield* currentReduced();
      const committed = yield* compactionRunner.maybeCompact({
        state: current,
        context: yield* contextProjectionFor(current),
        appendDurableEntries,
      });
      const next = committed.length === 0 ? current : yield* currentReduced();
      yield* requestCurrentReducerCheckpointSnapshot("turn-boundary");
      return next;
    });

  const cancelCommand = (commandId: CommandId, reason: string) =>
    Effect.gen(function* () {
      const event = yield* events.commandCancelled({ commandId, reason });
      const committed = yield* appendDurable(event);
      return SessionCommandCancelled.make({ committed });
    });

  const failInterruptedTurnIfOpen = (
    active: { readonly runId: RunId; readonly turnId: TurnId },
    reason: string,
  ) =>
    Effect.gen(function* () {
      const current = yield* currentReduced();
      const turn = current.turns.get(active.turnId);
      if (turn?.terminal !== undefined) {
        return undefined;
      }
      const event = yield* events.turnFailed({
        runId: active.runId,
        turnId: active.turnId,
        error: interruptionFailure("turn", reason, {
          runId: active.runId,
          turnId: active.turnId,
        }),
      });
      return yield* appendDurable(event);
    });

  const completeRunAndCommand = (commandId: CommandId, runId: RunId) =>
    Effect.gen(function* () {
      const runCompleted = yield* events.runCompleted({ runId });
      const commandCompleted = yield* events.commandCompleted({ commandId });
      const committed = yield* appendDurableBatch([runCompleted, commandCompleted]);
      return SessionCommandCompleted.make({ committed: committed.at(-1)! });
    });

  const failRunAndCommandWithError = (
    commandId: CommandId,
    runId: RunId,
    error: TurnFailedEvent["payload"]["error"],
  ) =>
    Effect.gen(function* () {
      const runFailed = yield* events.runFailed({ runId, error });
      const commandFailed = yield* events.commandFailed({ commandId, error });
      const current = yield* currentReduced();
      const failurePlan = planRunFailure(current);
      const resumeEvents: Array<DurableEventEnvelope> = [];
      if (failurePlan.messageIdsToResume.length > 0) {
        const resumeCommandId = yield* ids.makeCommandId();
        resumeEvents.push(
          yield* events.commandAdmitted({
            command: new ResumePendingMessagesCommand({
              commandId: resumeCommandId,
              messageIds: failurePlan.messageIdsToResume as [MessageId, ...MessageId[]],
            }),
          }),
        );
      }
      const committed = yield* appendDurableBatch([runFailed, commandFailed, ...resumeEvents]);
      return SessionCommandFailed.make({ committed: committed.at(-1)! });
    });

  const failRunAndCommand = (commandId: CommandId, runId: RunId, turn: TurnRunResult) =>
    Effect.gen(function* () {
      if (turn.outcome._tag !== "TurnRunFailed") {
        return yield* Effect.die(new Error("Cannot fail run from a completed turn"));
      }
      const failed = yield* requireTurnFailed(turn.outcome.committed);
      return yield* failRunAndCommandWithError(commandId, runId, failed.event.payload.error);
    });

  const interruptRunAndCommand = (
    active: { readonly commandId: CommandId; readonly runId: RunId },
    reason: string,
  ) =>
    Effect.gen(function* () {
      const failurePlan = planRunFailure(yield* currentReduced());
      const runFailed = yield* events.runFailed({
        runId: active.runId,
        error: interruptionFailure("run", reason, { runId: active.runId }),
      });
      const commandCancelled = yield* events.commandCancelled({
        commandId: active.commandId,
        reason,
      });
      const resume =
        failurePlan.messageIdsToResume.length === 0
          ? []
          : [
              yield* events.commandAdmitted({
                command: new ResumePendingMessagesCommand({
                  commandId: yield* ids.makeCommandId(),
                  messageIds: failurePlan.messageIdsToResume as [MessageId, ...MessageId[]],
                }),
              }),
            ];
      const committed = yield* appendDurableBatch([runFailed, commandCancelled, ...resume]);
      return SessionCommandCancelled.make({ committed: committed.at(-1)! });
    });

  const userInterruptionEvents = (
    active: {
      readonly commandId: CommandId;
      readonly runId: RunId;
      readonly turnId: TurnId;
    },
    interruptionCommandId: CommandId,
    reason: string,
  ) =>
    Effect.gen(function* () {
      const current = yield* currentReduced();
      const plan = planUserInterruption(current);
      const turn = current.turns.get(active.turnId);
      const terminalEvents: Array<DurableEventEnvelope> = [];
      if (turn?.terminal === undefined) {
        terminalEvents.push(
          yield* events.turnFailed({
            runId: active.runId,
            turnId: active.turnId,
            error: interruptionFailure("turn", reason, {
              runId: active.runId,
              turnId: active.turnId,
            }),
          }),
        );
      }
      terminalEvents.push(
        yield* events.runFailed({
          runId: active.runId,
          error: interruptionFailure("run", reason, { runId: active.runId }),
        }),
        yield* events.commandCancelled({ commandId: active.commandId, reason }),
      );
      if (plan.messageIdsToPause.length > 0) {
        terminalEvents.push(
          yield* events.pendingMessagesPaused({
            interruptionCommandId,
            runId: active.runId,
            messageIds: plan.messageIdsToPause as [MessageId, ...MessageId[]],
            reason: "user-interrupted",
          }),
        );
      }
      return terminalEvents;
    });

  const appendUserInterruption = (
    active: {
      readonly commandId: CommandId;
      readonly runId: RunId;
      readonly turnId: TurnId;
    },
    interruptionCommandId: CommandId,
    reason: string,
  ) =>
    Effect.gen(function* () {
      yield* appendDurableBatch(
        yield* userInterruptionEvents(active, interruptionCommandId, reason),
      );
    });

  const startRunTrace = Effect.fn(function* (input: {
    readonly admissionTrace?: DispatchCommandCandidate["admissionTrace"];
    readonly commandId: CommandId;
    readonly modelSelection: ModelSelectionPayload;
    readonly runId: RunId;
  }) {
    const runScope = yield* Scope.fork(runtimeScope, "sequential");
    return yield* Effect.gen(function* () {
      const links: ReadonlyArray<EDATraceLink> =
        input.admissionTrace === undefined
          ? []
          : [
              {
                context: input.admissionTrace.span,
                attributes: {
                  "eda.trace.link_type": "command_admission",
                  "eda.command.id": input.commandId,
                },
              },
            ];
      const runSpan = yield* Effect.makeSpanScoped("agent.run", {
        root: true,
        attributes: {
          "eda.session.id": sessionContext.sessionId,
          "eda.command.id": input.commandId,
          "eda.run.id": input.runId,
          "eda.model.provider": input.modelSelection.provider,
          "eda.model.id": input.modelSelection.modelId,
        },
        links: links.map((link) => ({
          span: toEdaExternalSpan(link.context),
          attributes: link.attributes,
        })),
      }).pipe(Scope.provide(runScope));
      return {
        runScope,
        runSpan,
        runTrace: makeEDARunTrace(edaTraceContextFromSpan(runSpan), links),
      } satisfies ActiveRunTrace;
    }).pipe(
      Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(runScope, exit) : Effect.void)),
    );
  });

  const closeRunTraceOnFailure = <A, E, R>(
    trace: Pick<ActiveRunTrace, "runScope">,
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E, R> =>
    effect.pipe(
      Effect.onExit((exit) =>
        Exit.isFailure(exit) ? Scope.close(trace.runScope, exit) : Effect.void,
      ),
    );

  const startTurn = Effect.fn(function* (input: StartTurnInput) {
    const inputMessageIds = input.promptInput.selectedUserMessages.map(
      (message) => message.messageId,
    );
    const prompt = yield* buildEDAPrompt(promptProjector, input.promptInput);
    const turnId = yield* ids.makeTurnId();
    const current = yield* currentReduced();
    const remainingToolCalls = remainingToolCallsForRun(
      current,
      input.runId,
      input.maxToolCallsPerRun ?? defaultMaxToolCallsPerRun,
    );
    const remainingToolRejectionCorrections = remainingToolRejectionCorrectionsForRun(
      current,
      input.runId,
      NonNegativeInt.make(maxToolRejectionCorrections),
    );
    yield* annotateEdaSpan({
      "eda.command.id": input.commandId,
      "eda.run.id": input.runId,
      "eda.turn.id": turnId,
      "eda.message.input_count": inputMessageIds.length,
    });
    const started = yield* appendDurable(
      yield* events.turnStarted({
        runId: input.runId,
        turnId,
        ...(inputMessageIds.length === 0 ? {} : { inputMessageIds }),
      }),
    );
    const turnScope = yield* Scope.fork(input.runScope, "sequential");
    const fiber = yield* keepAlive
      .withActiveWork(
        `turn:${turnId}`,
        turnRunner.runTurn({
          runId: input.runId,
          turnId,
          prompt,
          eventSink: sessionApi,
          started,
          modelSelection: input.modelSelection,
          maxToolCallsPerRun: remainingToolCalls,
          remainingToolRejectionCorrections,
          maxToolRejectionCorrections: NonNegativeInt.make(maxToolRejectionCorrections),
        }),
      )
      .pipe(Effect.withParentSpan(input.runSpan), Effect.forkIn(turnScope));
    yield* Ref.set(executionState, {
      _tag: "Running",
      commandId: input.commandId,
      commandStarted: input.commandStarted,
      runId: input.runId,
      turnId,
      modelSelection: input.modelSelection,
      ...(input.maxToolCallsPerRun === undefined
        ? {}
        : { maxToolCallsPerRun: input.maxToolCallsPerRun }),
      runScope: input.runScope,
      runSpan: input.runSpan,
      turnScope,
      fiber,
    });
    return SessionForkedCommand.make({ commandId: input.commandId });
  });

  const startSubmitCommand = Effect.fn(function* (
    command: DispatchCommandCandidate,
    input: SessionRunInput,
  ) {
    if (command.command._tag !== "SubmitMessage") {
      return yield* Effect.die(new Error("startSubmitCommand requires SubmitMessage"));
    }
    const submit = command.command;
    const commandId = command.commandId;
    yield* annotateEdaSpan({
      "eda.command.id": commandId,
      "eda.command.disposition": submit.disposition,
      "eda.model.provider": input.modelSelection.provider,
      "eda.model.id": input.modelSelection.modelId,
    });
    const commandStarted = yield* events.commandStarted({ commandId });
    const beforeStart = yield* currentReduced();
    const submittedMessage = Array.from(beforeStart.messages.values()).find(
      (message) => message._tag === "User" && message.commandId === commandId,
    );
    const messageId = submittedMessage?.messageId ?? (yield* ids.makeMessageId());
    const userMessage =
      submittedMessage === undefined
        ? yield* events.userMessageCommitted({
            commandId,
            messageId,
            content: submit.content,
          })
        : undefined;
    const runId = yield* ids.makeRunId();
    const activeRunTrace = yield* startRunTrace({
      admissionTrace: command.admissionTrace,
      commandId,
      modelSelection: input.modelSelection,
      runId,
    });
    return yield* closeRunTraceOnFailure(
      activeRunTrace,
      Effect.gen(function* () {
        const runStarted = yield* events.runStarted({
          runId,
          commandIds: [commandId],
          modelSelection: input.modelSelection,
          trace: activeRunTrace.runTrace,
        });
        const startedEvents = yield* appendDurableBatch([
          commandStarted,
          ...(userMessage === undefined ? [] : [userMessage]),
          runStarted,
        ]);
        const selectedUserContent =
          submittedMessage === undefined
            ? submit.content
            : Schema.decodeUnknownSync(UserMessageContent)(submittedMessage.content);
        const promptState = yield* currentData();
        return yield* startTurn({
          commandId,
          commandStarted: startedEvents[0]!,
          runId,
          modelSelection: input.modelSelection,
          ...(input.maxToolCallsPerRun === undefined
            ? {}
            : { maxToolCallsPerRun: input.maxToolCallsPerRun }),
          promptInput: {
            sessionId: sessionContext.sessionId,
            state: promptState.reduced,
            reducerStates: promptState.reducerStates,
            context: yield* contextProjectionFor(promptState.reduced),
            selectedUserMessages: [{ commandId, content: selectedUserContent, messageId }],
          },
          runScope: activeRunTrace.runScope,
          runSpan: activeRunTrace.runSpan,
        });
      }),
    );
  });

  const startResumePendingMessages = Effect.fn(function* (
    command: DispatchCommandCandidate,
    input: SessionRunInput,
  ) {
    if (command.command._tag !== "ResumePendingMessages") {
      return yield* Effect.die(new Error("Resume dispatch requires ResumePendingMessages"));
    }
    const current = yield* currentData();
    const pending = command.command.messageIds.flatMap((messageId) => {
      const message = current.reduced.messages.get(messageId);
      return (message?._tag === "User" || message?._tag === "Steering") &&
        message.consumedSeq === undefined &&
        message.cancelledSeq === undefined
        ? [message]
        : [];
    });
    if (pending.length === 0) {
      return yield* processInactiveStopCommand(command);
    }
    const commandStarted = yield* events.commandStarted({ commandId: command.commandId });
    const runId = yield* ids.makeRunId();
    const activeRunTrace = yield* startRunTrace({
      admissionTrace: command.admissionTrace,
      commandId: command.commandId,
      modelSelection: input.modelSelection,
      runId,
    });
    return yield* closeRunTraceOnFailure(
      activeRunTrace,
      Effect.gen(function* () {
        const runStarted = yield* events.runStarted({
          runId,
          commandIds: [command.commandId, ...pending.map((message) => message.commandId)],
          modelSelection: input.modelSelection,
          trace: activeRunTrace.runTrace,
        });
        const started = yield* appendDurableBatch([commandStarted, runStarted]);
        return yield* startTurn({
          commandId: command.commandId,
          commandStarted: started[0]!,
          runId,
          modelSelection: input.modelSelection,
          ...(input.maxToolCallsPerRun === undefined
            ? {}
            : { maxToolCallsPerRun: input.maxToolCallsPerRun }),
          promptInput: {
            sessionId: sessionContext.sessionId,
            state: current.reduced,
            reducerStates: current.reducerStates,
            context: yield* contextProjectionFor(current.reduced),
            selectedUserMessages: pending.map((message) => ({
              commandId: message.commandId,
              content: message.content,
              messageId: message.messageId,
            })),
          },
          runScope: activeRunTrace.runScope,
          runSpan: activeRunTrace.runSpan,
        });
      }),
    );
  });

  const processInactiveStopCommand = Effect.fn(function* (command: DispatchCommandCandidate) {
    const commandId = command.commandId;
    const guarded = yield* startedBoundaryGuard({
      start: events.commandStarted({ commandId }),
      body: (started) =>
        Effect.gen(function* () {
          const outcome = yield* cancelCommand(commandId, "no active turn");
          return { started, outcome } satisfies SessionCommandResult;
        }),
      onFailure: (error) => events.commandFailed({ commandId, error }),
      onInterrupt: events.commandCancelled({ commandId, reason: "interrupted" }),
    });
    return guarded.value;
  });

  const inspectExecutionState = () =>
    Effect.gen(function* () {
      const current = yield* Ref.get(executionState);
      if (current._tag === "Idle") {
        return undefined;
      }
      if (current._tag === "Completed") {
        return {
          _tag: "CompletedTurn",
          active: current,
          turn: current.turn,
        } satisfies ExecutionInspection;
      }

      const exit = current.fiber.pollUnsafe();
      if (exit === undefined) {
        return { _tag: "RunningTurn", active: current } satisfies ExecutionInspection;
      }

      yield* Scope.close(current.turnScope, exit);
      if (Exit.isFailure(exit)) {
        yield* Ref.set(executionState, { _tag: "Idle" });
        if (hasEDASessionStoreError(exit.cause)) {
          yield* Scope.close(current.runScope, exit);
          return yield* Effect.failCause(exit.cause);
        }
        if (Cause.hasInterruptsOnly(exit.cause)) {
          yield* failInterruptedTurnIfOpen(current, "interrupted");
          const outcome = yield* interruptRunAndCommand(current, "interrupted");
          yield* Scope.close(current.runScope, exit);
          return {
            _tag: "FailedTurn",
            result: { started: current.commandStarted, outcome } satisfies SessionCommandResult,
          } satisfies ExecutionInspection;
        }
        const outcome = yield* failRunAndCommandWithError(
          current.commandId,
          current.runId,
          failurePayloadFromCause(exit.cause),
        );
        yield* Scope.close(current.runScope, exit);
        return {
          _tag: "FailedTurn",
          result: { started: current.commandStarted, outcome } satisfies SessionCommandResult,
        } satisfies ExecutionInspection;
      }
      const completed: CompletedExecution = {
        _tag: "Completed",
        commandId: current.commandId,
        commandStarted: current.commandStarted,
        runId: current.runId,
        turnId: current.turnId,
        modelSelection: current.modelSelection,
        ...(current.maxToolCallsPerRun === undefined
          ? {}
          : { maxToolCallsPerRun: current.maxToolCallsPerRun }),
        runScope: current.runScope,
        runSpan: current.runSpan,
        turn: exit.value,
      };
      yield* Ref.set(executionState, completed);
      return {
        _tag: "CompletedTurn",
        active: completed,
        turn: exit.value,
      } satisfies ExecutionInspection;
    });

  const recoverSession = Effect.fnUntraced(function* (
    reduced: ReducedState,
    reason: string,
    input: SessionRunInput,
  ) {
    const plan = planSessionRecovery(reduced);
    yield* annotateEdaSpan({
      "eda.recovery.reason": reason,
      "eda.recovery.open_tool_calls": plan.openToolCalls.length,
      "eda.recovery.active_inferences": plan.activeInferences.length,
      "eda.recovery.active_turns": plan.activeTurns.length,
      "eda.recovery.active_runs": plan.activeRuns.length,
      "eda.recovery.active_commands": plan.activeCommands.length,
      "eda.recovery.pending_stop_requests": plan.pendingStopRequests.length,
      "eda.recovery.open_compactions": plan.openCompactions.length,
      "eda.recovery.messages_to_pause": plan.messageIdsToPause.length,
      "eda.recovery.messages_to_resume": plan.messageIdsToResume.length,
    });
    const recoveryEvents: Array<DurableEventEnvelope> = [];

    for (const tool of plan.openToolCalls) {
      const error = toolCancelledFailureFromRecord(tool, reason);
      recoveryEvents.push(
        yield* events.toolCallFailed({
          toolCallId: tool.toolCallId,
          error,
          promptPart: toolCancelledPromptPartFromRecord(tool, error),
        }),
      );
    }
    for (const inference of plan.activeInferences) {
      recoveryEvents.push(
        yield* events.inferenceFailed({
          runId: inference.runId,
          turnId: inference.turnId,
          inferenceId: inference.inferenceId,
          error: FailurePayload.make({
            message: `inference interrupted: ${reason}`,
            code: "inference.interrupted",
            details: { reason },
          }),
        }),
      );
    }
    const stoppedRunIds = new Set(
      plan.pendingStopRequests.flatMap((request) =>
        request.requestedRunId === undefined ? [] : [request.requestedRunId],
      ),
    );
    for (const turn of plan.activeTurns) {
      recoveryEvents.push(
        yield* events.turnFailed({
          runId: turn.runId,
          turnId: turn.turnId,
          error: interruptionFailure(
            "turn",
            reason,
            { runId: turn.runId, turnId: turn.turnId },
            !stoppedRunIds.has(turn.runId),
          ),
        }),
      );
    }
    for (const run of plan.activeRuns) {
      recoveryEvents.push(
        yield* events.runFailed({
          runId: run.runId,
          error: interruptionFailure(
            "run",
            reason,
            { runId: run.runId },
            !stoppedRunIds.has(run.runId),
          ),
        }),
      );
    }

    const completedStopCommandIds = new Set<CommandId>();
    for (const request of plan.pendingStopRequests) {
      if (request.requestedRunId === undefined || request.requestedTurnId === undefined) {
        continue;
      }
      completedStopCommandIds.add(request.commandId);
    }

    for (const command of plan.activeCommands) {
      if (completedStopCommandIds.has(command.commandId)) {
        continue;
      }
      if (command.commandId === plan.continuation?.command.commandId) {
        continue;
      }
      recoveryEvents.push(yield* events.commandCancelled({ commandId: command.commandId, reason }));
    }

    for (const request of plan.pendingStopRequests) {
      if (request.requestedRunId === undefined || request.requestedTurnId === undefined) {
        continue;
      }
      recoveryEvents.push(
        yield* events.stopTurnApplied({
          commandId: request.commandId,
          runId: request.requestedRunId,
          turnId: request.requestedTurnId,
        }),
      );
      recoveryEvents.push(yield* events.commandCompleted({ commandId: request.commandId }));
    }

    const applicableStop = plan.pendingStopRequests.find(
      (request) => request.requestedRunId !== undefined && request.requestedTurnId !== undefined,
    );
    if (applicableStop !== undefined && plan.messageIdsToPause.length > 0) {
      recoveryEvents.push(
        yield* events.pendingMessagesPaused({
          interruptionCommandId: applicableStop.commandId,
          runId: applicableStop.requestedRunId!,
          messageIds: plan.messageIdsToPause as [MessageId, ...MessageId[]],
          reason: "user-interrupted",
        }),
      );
    }

    for (const compaction of plan.openCompactions) {
      recoveryEvents.push(
        compaction.rebasedSeq === undefined
          ? yield* events.compactionFailed({
              compactionId: compaction.compactionId,
              error: FailurePayload.make({
                message: "runtime restarted before compaction completed",
              }),
            })
          : yield* events.compactionCompleted({ compactionId: compaction.compactionId }),
      );
    }

    if (plan.messageIdsToResume.length > 0) {
      recoveryEvents.push(
        yield* events.commandAdmitted({
          command: new ResumePendingMessagesCommand({
            commandId: yield* ids.makeCommandId(),
            messageIds: plan.messageIdsToResume as [MessageId, ...MessageId[]],
          }),
        }),
      );
    }

    const continuationRunId = plan.continuation === undefined ? undefined : yield* ids.makeRunId();
    const continuationRunTrace =
      plan.continuation === undefined || continuationRunId === undefined
        ? undefined
        : yield* startRunTrace({
            admissionTrace: plan.continuation.command.admissionTrace,
            commandId: plan.continuation.command.commandId,
            modelSelection: input.modelSelection,
            runId: continuationRunId,
          });
    const completeRecovery = Effect.gen(function* () {
      if (
        plan.continuation !== undefined &&
        continuationRunId !== undefined &&
        continuationRunTrace !== undefined
      ) {
        recoveryEvents.push(
          yield* events.runStarted({
            runId: continuationRunId,
            commandIds: plan.continuation.run.commandIds,
            modelSelection: input.modelSelection,
            trace: continuationRunTrace.runTrace,
          }),
        );
      }

      if (recoveryEvents.length > 0) {
        recoveryEvents.push(
          yield* events.recoveryCompleted({
            trigger: "runtime-restart",
            ...(plan.continuation === undefined || continuationRunId === undefined
              ? {}
              : {
                  continuation: {
                    commandId: plan.continuation.command.commandId,
                    interruptedRunId: plan.continuation.run.runId,
                    replacementRunId: continuationRunId,
                  },
                }),
          }),
        );
      }

      const committed =
        recoveryEvents.length === 0 ? [] : yield* appendDurableBatch(recoveryEvents);
      const nextPlan = planSessionRecovery(yield* currentReduced());
      const reachedFixedPoint =
        plan.continuation === undefined || continuationRunId === undefined
          ? isSessionRecoveryPlanEmpty(nextPlan)
          : nextPlan.activeCommands.length === 1 &&
            nextPlan.activeCommands[0]?.commandId === plan.continuation.command.commandId &&
            nextPlan.activeRuns.length === 1 &&
            nextPlan.activeRuns[0]?.runId === continuationRunId &&
            nextPlan.activeTurns.length === 0 &&
            nextPlan.activeInferences.length === 0 &&
            nextPlan.openToolCalls.length === 0 &&
            nextPlan.pendingStopRequests.length === 0 &&
            nextPlan.openCompactions.length === 0 &&
            nextPlan.messageIdsToPause.length === 0 &&
            nextPlan.messageIdsToResume.length === 0 &&
            nextPlan.continuation?.inputMessageIds.length ===
              plan.continuation.inputMessageIds.length &&
            nextPlan.continuation.inputMessageIds.every((messageId) =>
              plan.continuation?.inputMessageIds.includes(messageId),
            );
      if (!reachedFixedPoint) {
        return yield* Effect.die(
          new Error("Startup recovery did not reach a fixed point before live execution"),
        );
      }
      return {
        committed,
        continuation:
          plan.continuation === undefined ||
          continuationRunId === undefined ||
          continuationRunTrace === undefined
            ? undefined
            : {
                command: plan.continuation.command,
                runId: continuationRunId,
                inputMessageIds: plan.continuation.inputMessageIds,
                runScope: continuationRunTrace.runScope,
                runSpan: continuationRunTrace.runSpan,
              },
      } as const;
    });
    return continuationRunTrace === undefined
      ? yield* completeRecovery
      : yield* closeRunTraceOnFailure(continuationRunTrace, completeRecovery);
  });

  /** Launch the turn for a replacement run already scheduled atomically by startup recovery. */
  const resumeRecoveredCommand = Effect.fnUntraced(function* (
    continuation: {
      readonly command: CommandRecord;
      readonly runId: RunId;
      readonly inputMessageIds: ReadonlyArray<MessageId>;
      readonly runScope: Scope.Closeable;
      readonly runSpan: Tracer.Span;
    },
    input: SessionRunInput,
  ) {
    const [commandStarted] = yield* store.loadCommittedEventsBySeq([
      continuation.command.startedSeq!,
    ]);
    if (commandStarted?.event.type !== "CommandStarted") {
      return yield* Effect.die(
        new Error(
          `Recovered command ${continuation.command.commandId} is missing its CommandStarted event`,
        ),
      );
    }
    const promptState = yield* currentData();
    const pendingInputs = continuation.inputMessageIds.flatMap((messageId) => {
      const message = promptState.reduced.messages.get(messageId);
      return (message?._tag === "User" || message?._tag === "Steering") &&
        message.consumedSeq === undefined &&
        message.cancelledSeq === undefined
        ? [message]
        : [];
    });
    yield* startTurn({
      commandId: continuation.command.commandId,
      commandStarted,
      runId: continuation.runId,
      modelSelection: input.modelSelection,
      ...(input.maxToolCallsPerRun === undefined
        ? {}
        : { maxToolCallsPerRun: input.maxToolCallsPerRun }),
      promptInput: {
        sessionId: sessionContext.sessionId,
        state: promptState.reduced,
        reducerStates: promptState.reducerStates,
        context: yield* contextProjectionFor(promptState.reduced),
        selectedUserMessages: pendingInputs.map((message) => ({
          commandId: message.commandId,
          content: message.content,
          messageId: message.messageId,
        })),
      },
      runScope: continuation.runScope,
      runSpan: continuation.runSpan,
    });
  });

  const dieLiveInvariantViolation = (reduced: ReducedState, active: DispatchActiveCommand) =>
    Effect.gen(function* () {
      const recoverable = classifyRecoverableWork(reduced);
      const message =
        "SessionState live invariant violated: durable replay has active work but no in-memory execution owner.";
      yield* Effect.logFatal(message, {
        "eda.command.id": active.commandId,
        "eda.recovery.active_inferences": recoverable.activeInferences.length,
        "eda.recovery.active_runs": recoverable.activeRuns.length,
        "eda.recovery.active_turns": recoverable.activeTurns.length,
        "eda.recovery.open_tool_calls": recoverable.openToolCalls.length,
        "eda.recovery.pending_stop_requests": recoverable.pendingStopRequests.length,
        ...(active.runId === undefined ? {} : { "eda.run.id": active.runId }),
      });
      yield* Ref.set(fatalInvariantViolation, message);
      return yield* Effect.die(message);
    });

  const steerActiveCommand = Effect.fn(function* (command: DispatchCommandCandidate, runId: RunId) {
    yield* annotateEdaSpan({ "eda.command.id": command.commandId, "eda.run.id": runId });
    if (command.command._tag !== "SubmitMessage") {
      return yield* Effect.die(new Error("DispatchSteerCommand must contain SubmitMessage"));
    }

    const commandId = command.commandId;
    const guarded = yield* startedBoundaryGuard({
      start: events.commandStarted({ commandId }),
      body: (started) =>
        Effect.gen(function* () {
          const completed = yield* events.commandCompleted({ commandId });
          const committed = yield* appendDurableBatch([completed]);
          return {
            started,
            outcome: SessionCommandCompleted.make({ committed: committed.at(-1)! }),
          } satisfies Pick<SessionCommandResult, "started" | "outcome">;
        }),
      onFailure: (error) => events.commandFailed({ commandId, error }),
      onInterrupt: events.commandCancelled({ commandId, reason: "interrupted" }),
    });
    return guarded.value;
  });

  const failControlCommand = (commandId: CommandId, code: string, message: string) =>
    Effect.gen(function* () {
      const failed = yield* events.commandFailed({
        commandId,
        error: FailurePayload.make({ code, message }),
      });
      const committed = yield* appendDurable(failed);
      return SessionCommandFailed.make({ committed });
    });

  const cancelPendingMessage = Effect.fn(function* (command: DispatchCommandCandidate) {
    if (command.command._tag !== "CancelPendingMessage") {
      return yield* Effect.die(
        new Error("CancelPendingMessage dispatch requires matching command"),
      );
    }
    const cancel = command.command;
    const controlCommandId = command.commandId;
    const guarded = yield* startedBoundaryGuard({
      start: events.commandStarted({ commandId: controlCommandId }),
      body: (started) =>
        Effect.gen(function* () {
          const current = yield* currentReduced();
          const target = current.messages.get(cancel.messageId);
          if (
            (target?._tag !== "User" && target?._tag !== "Steering") ||
            (target._tag === "User" && target.disposition === undefined) ||
            target.consumedSeq !== undefined ||
            target.cancelledSeq !== undefined
          ) {
            return {
              started,
              outcome: yield* failControlCommand(
                controlCommandId,
                "message_not_pending",
                "Message is no longer pending",
              ),
            } satisfies SessionCommandResult;
          }
          const cancelled = yield* events.userMessageCancelled({
            commandId: controlCommandId,
            messageId: target.messageId,
            reason: cancel.reason,
          });
          const origin = current.commands.get(target.commandId);
          const originCancelled =
            origin !== undefined && origin.terminal === undefined && origin.startedSeq === undefined
              ? [
                  yield* events.commandCancelled({
                    commandId: target.commandId,
                    reason: "pending message cancelled",
                  }),
                ]
              : [];
          const completed = yield* events.commandCompleted({ commandId: controlCommandId });
          const committed = yield* appendDurableBatch([cancelled, ...originCancelled, completed]);
          return {
            started,
            outcome: SessionCommandCompleted.make({ committed: committed.at(-1)! }),
          } satisfies SessionCommandResult;
        }),
      onFailure: (error) => events.commandFailed({ commandId: controlCommandId, error }),
      onInterrupt: events.commandCancelled({ commandId: controlCommandId, reason: "interrupted" }),
    });
    return guarded.value;
  });

  const promotePendingMessage = Effect.fn(function* (command: DispatchCommandCandidate) {
    if (command.command._tag !== "PromotePendingMessage") {
      return yield* Effect.die(
        new Error("PromotePendingMessage dispatch requires matching command"),
      );
    }
    const promotion = command.command;
    const controlCommandId = command.commandId;
    const guarded = yield* startedBoundaryGuard({
      start: events.commandStarted({ commandId: controlCommandId }),
      body: (started) =>
        Effect.gen(function* () {
          const current = yield* currentReduced();
          const target = current.messages.get(promotion.messageId);
          if (
            (target?._tag !== "User" && target?._tag !== "Steering") ||
            (target._tag === "User"
              ? target.disposition !== "queue"
              : target.pausedByCommandId === undefined) ||
            target.consumedSeq !== undefined ||
            target.cancelledSeq !== undefined
          ) {
            return {
              started,
              outcome: yield* failControlCommand(
                controlCommandId,
                "message_not_queue",
                "Message is no longer a pending queue message",
              ),
            } satisfies SessionCommandResult;
          }
          const promoted = yield* events.userMessagePromoted({
            commandId: controlCommandId,
            messageId: target.messageId,
            from: "queue",
            to: "steer",
          });
          const completed = yield* events.commandCompleted({ commandId: controlCommandId });
          const committed = yield* appendDurableBatch([promoted, completed]);
          return {
            started,
            outcome: SessionCommandCompleted.make({ committed: committed.at(-1)! }),
          } satisfies SessionCommandResult;
        }),
      onFailure: (error) => events.commandFailed({ commandId: controlCommandId, error }),
      onInterrupt: events.commandCancelled({ commandId: controlCommandId, reason: "interrupted" }),
    });
    return guarded.value;
  });

  const stopActiveCommand = Effect.fn(function* (
    command: DispatchCommandCandidate,
    active: RunningExecution,
  ) {
    const stopCommandId = command.commandId;
    yield* annotateEdaSpan({
      "eda.command.id": active.commandId,
      "eda.control_command.id": stopCommandId,
      "eda.run.id": active.runId,
      "eda.turn.id": active.turnId,
    });
    const guarded = yield* startedBoundaryGuard({
      start: events.commandStarted({ commandId: stopCommandId }),
      body: (started) =>
        Effect.uninterruptible(
          Effect.gen(function* () {
            const beforeStop = yield* currentReduced();
            const requested = activeTurnIdentityForCommand(beforeStop, active.commandId);
            const requestedEvent = yield* events.stopTurnRequested({
              commandId: stopCommandId,
              ...(requested === undefined
                ? {}
                : { runId: requested.runId, turnId: requested.turnId }),
            });
            yield* appendDurable(requestedEvent);

            yield* Scope.close(active.turnScope, Exit.interrupt());
            yield* Ref.set(executionState, { _tag: "Idle" });
            yield* appendUserInterruption(active, stopCommandId, "interrupted");
            yield* Scope.close(active.runScope, Exit.interrupt());

            const afterStop = yield* currentReduced();
            const applied = activeTurnIdentityForCommand(afterStop, active.commandId);
            const appliedEvent = yield* events.stopTurnApplied({
              commandId: stopCommandId,
              runId: active.runId,
              turnId: active.turnId,
              ...(applied?.turnId === active.turnId && applied.inferenceId !== undefined
                ? { inferenceId: applied.inferenceId }
                : {}),
            });
            const completedEvent = yield* events.commandCompleted({ commandId: stopCommandId });
            const committed = yield* appendDurableBatch([appliedEvent, completedEvent]);
            return {
              started,
              outcome: SessionCommandCompleted.make({ committed: committed.at(-1)! }),
            } satisfies Pick<SessionCommandResult, "started" | "outcome">;
          }),
        ),
      onFailure: (error) => events.commandFailed({ commandId: stopCommandId, error }),
      onInterrupt: events.commandCancelled({ commandId: stopCommandId, reason: "interrupted" }),
    });
    return guarded.value;
  });

  const stopCompletedCommand = Effect.fn(function* (
    command: DispatchCommandCandidate,
    active: CompletedExecution,
  ) {
    const stopCommandId = command.commandId;
    yield* annotateEdaSpan({
      "eda.command.id": active.commandId,
      "eda.control_command.id": stopCommandId,
      "eda.run.id": active.runId,
      "eda.turn.id": active.turnId,
    });
    const guarded = yield* startedBoundaryGuard({
      start: events.commandStarted({ commandId: stopCommandId }),
      body: (started) =>
        Effect.uninterruptible(
          Effect.gen(function* () {
            const requestedEvent = yield* events.stopTurnRequested({
              commandId: stopCommandId,
              runId: active.runId,
              turnId: active.turnId,
            });
            yield* appendDurable(requestedEvent);
            yield* Ref.set(executionState, { _tag: "Idle" });
            yield* appendUserInterruption(active, stopCommandId, "interrupted");
            yield* Scope.close(active.runScope, Exit.interrupt());
            const appliedEvent = yield* events.stopTurnApplied({
              commandId: stopCommandId,
              runId: active.runId,
              turnId: active.turnId,
            });
            const completedEvent = yield* events.commandCompleted({ commandId: stopCommandId });
            const committed = yield* appendDurableBatch([appliedEvent, completedEvent]);
            return {
              started,
              outcome: SessionCommandCompleted.make({ committed: committed.at(-1)! }),
            } satisfies Pick<SessionCommandResult, "started" | "outcome">;
          }),
        ),
      onFailure: (error) => events.commandFailed({ commandId: stopCommandId, error }),
      onInterrupt: events.commandCancelled({ commandId: stopCommandId, reason: "interrupted" }),
    });
    return guarded.value;
  });

  const continueOrFinishRunAfterTurn = Effect.fnUntraced(function* (
    active: CompletedExecution,
    turn: TurnRunResult,
  ) {
    switch (turn.outcome._tag) {
      case "TurnRunFailed": {
        const outcome = yield* failRunAndCommand(active.commandId, active.runId, turn);
        yield* Ref.set(executionState, { _tag: "Idle" });
        yield* maybeCompactAtBoundary();
        yield* Scope.close(active.runScope, Exit.fail(new Error("agent turn failed")));
        return { started: active.commandStarted, outcome } satisfies SessionCommandResult;
      }
      case "TurnRunCompleted": {
        const current = yield* currentReduced();
        const continuation = decideRunContinuation({
          runId: active.runId,
          turnId: active.turnId,
          state: current,
          maxToolRejectionCorrections,
        });
        switch (continuation._tag) {
          case "ContinueWithSteering":
            return yield* startTurn({
              commandId: active.commandId,
              commandStarted: active.commandStarted,
              runId: active.runId,
              modelSelection: active.modelSelection,
              ...(active.maxToolCallsPerRun === undefined
                ? {}
                : { maxToolCallsPerRun: active.maxToolCallsPerRun }),
              promptInput: {
                sessionId: sessionContext.sessionId,
                state: current,
                reducerStates: (yield* currentData()).reducerStates,
                context: yield* contextProjectionFor(current),
                selectedUserMessages: continuation.steerings.map((steering) => ({
                  commandId: steering.commandId,
                  content: steering.content,
                  messageId: steering.messageId,
                })),
              },
              runScope: active.runScope,
              runSpan: active.runSpan,
            });
          case "ContinueWithToolFeedback":
            return yield* startTurn({
              commandId: active.commandId,
              commandStarted: active.commandStarted,
              runId: active.runId,
              modelSelection: active.modelSelection,
              ...(active.maxToolCallsPerRun === undefined
                ? {}
                : { maxToolCallsPerRun: active.maxToolCallsPerRun }),
              promptInput: {
                sessionId: sessionContext.sessionId,
                state: current,
                reducerStates: (yield* currentData()).reducerStates,
                context: yield* contextProjectionFor(current),
                selectedUserMessages: [],
              },
              runScope: active.runScope,
              runSpan: active.runSpan,
            });
          case "FailRun": {
            const outcome = yield* failRunAndCommandWithError(
              active.commandId,
              active.runId,
              continuation.error,
            );
            yield* Ref.set(executionState, { _tag: "Idle" });
            yield* maybeCompactAtBoundary();
            yield* Scope.close(active.runScope, Exit.fail(new Error(continuation.error.message)));
            return { started: active.commandStarted, outcome } satisfies SessionCommandResult;
          }
          case "CompleteRun":
            break;
          default:
            return yield* Effect.die(assertNeverError(continuation, "run continuation decision"));
        }
        yield* maybeCompactAtBoundary();
        const outcome = yield* completeRunAndCommand(active.commandId, active.runId);
        yield* Ref.set(executionState, { _tag: "Idle" });
        yield* Scope.close(active.runScope, Exit.void);
        return { started: active.commandStarted, outcome } satisfies SessionCommandResult;
      }
      default:
        return yield* Effect.die(assertNeverError(turn.outcome, "turn outcome"));
    }
  });

  const drainOneReadyCommand = Effect.fnUntraced(function* (input: SessionRunInput) {
    const execution = yield* inspectExecutionState();
    const current = yield* currentReduced();

    if (execution?._tag === "FailedTurn") {
      return execution.result;
    }

    if (execution?._tag === "CompletedTurn") {
      const decision = decideDispatch(current);
      switch (decision._tag) {
        case "DispatchCancelPendingMessage":
          return yield* cancelPendingMessage(decision.command);
        case "DispatchPromotePendingMessage":
          return yield* promotePendingMessage(decision.command);
        case "DispatchStopCommand": {
          const result = yield* stopCompletedCommand(decision.command, execution.active);
          yield* maybeCompactAtBoundary();
          return result;
        }
        case "DispatchSteerCommand": {
          const result = yield* steerActiveCommand(decision.command, decision.active.runId);
          yield* maybeCompactAtBoundary();
          return result;
        }
        case "DispatchInterruptCommand": {
          yield* appendUserInterruption(
            execution.active,
            decision.command.commandId,
            "interrupted",
          );
          yield* Scope.close(execution.active.runScope, Exit.interrupt());
          yield* Ref.set(executionState, { _tag: "Idle" });
          yield* maybeCompactAtBoundary();
          return yield* startSubmitCommand(decision.command, input);
        }
        default:
          return yield* continueOrFinishRunAfterTurn(execution.active, execution.turn);
      }
    }

    if (execution?._tag === "RunningTurn") {
      const decision = decideDispatch(current);
      switch (decision._tag) {
        case "DispatchCancelPendingMessage":
          return yield* cancelPendingMessage(decision.command);
        case "DispatchPromotePendingMessage":
          return yield* promotePendingMessage(decision.command);
        case "DispatchStopCommand": {
          const result = yield* stopActiveCommand(decision.command, execution.active);
          yield* maybeCompactAtBoundary();
          return result;
        }
        case "DispatchSteerCommand":
          return yield* steerActiveCommand(decision.command, decision.active.runId);
        case "DispatchInterruptCommand": {
          yield* Scope.close(execution.active.turnScope, Exit.interrupt());
          yield* Ref.set(executionState, { _tag: "Idle" });
          yield* appendUserInterruption(
            execution.active,
            decision.command.commandId,
            "interrupted",
          );
          yield* Scope.close(execution.active.runScope, Exit.interrupt());
          yield* maybeCompactAtBoundary();
          return yield* startSubmitCommand(decision.command, input);
        }
        default:
          return SessionNoRunnableCommand.make({
            reason: "active-command",
            active: DispatchActiveCommand.make({
              commandId: execution.active.commandId,
              runId: execution.active.runId,
            }),
          });
      }
    }

    const decision = decideDispatch(current);
    switch (decision._tag) {
      case "DispatchBlockedByActiveCommand":
      case "DispatchStopCommand":
      case "DispatchSteerCommand":
      case "DispatchInterruptCommand":
        return yield* dieLiveInvariantViolation(current, decision.active);
      case "DispatchCancelPendingMessage":
        return yield* cancelPendingMessage(decision.command);
      case "DispatchPromotePendingMessage":
        return yield* promotePendingMessage(decision.command);
      case "DispatchNoPendingCommand":
        return SessionNoRunnableCommand.make({ reason: "no-pending-command" });
      case "DispatchInvariantViolation":
        return yield* Effect.die(
          new Error(
            `SessionState live invariant violated: pending messages have no execution owner (${decision.messageIds.join(", ")})`,
          ),
        );
      case "DispatchStartCommand":
        switch (decision.command.command._tag) {
          case "StopTurn":
            return yield* processInactiveStopCommand(decision.command);
          case "ResumePendingMessages":
            return yield* startResumePendingMessages(decision.command, input);
          case "SubmitMessage":
            return yield* startSubmitCommand(decision.command, input);
          case "CancelPendingMessage":
            return yield* cancelPendingMessage(decision.command);
          case "PromotePendingMessage":
            return yield* promotePendingMessage(decision.command);
          default:
            return yield* Effect.die(
              assertNeverError(decision.command.command, "start dispatch command"),
            );
        }
      default:
        return yield* Effect.die(assertNeverError(decision, "dispatch decision"));
    }
  });

  const drainReadyWorkPostRecovery = Effect.fnUntraced(function* (input: SessionRunInput) {
    yield* annotateEdaSpan({
      "eda.model.provider": input.modelSelection.provider,
      "eda.model.id": input.modelSelection.modelId,
    });
    yield* failIfFatal;
    const processed: Array<SessionDrainProcessed> = [];
    while (true) {
      const result = yield* withFatalRecording(drainOneReadyCommand(input));
      if (isSessionNoRunnableCommand(result)) {
        yield* annotateEdaSpan({
          "eda.command.processed_count": processed.length,
          "eda.dispatch.stop_reason": result.reason,
        });
        return { _tag: "SessionDrainResult", processed, stop: result } as const;
      }
      processed.push(result);
    }
  });

  const runStartupRecoveryPass = Effect.fnUntraced(function* (input: SessionRunInput) {
    yield* annotateEdaSpan({
      "eda.recovery.reason": startupRecoveryReason,
      "eda.model.provider": input.modelSelection.provider,
      "eda.model.id": input.modelSelection.modelId,
    });
    yield* failIfFatal;
    const initial = yield* currentReduced();
    const recovery = yield* withFatalRecording(
      recoverSession(initial, startupRecoveryReason, input),
    );
    yield* requestStartupRecoveryCheckpointIfNeeded();
    if (recovery.continuation !== undefined) {
      yield* closeRunTraceOnFailure(
        recovery.continuation,
        withFatalRecording(resumeRecoveredCommand(recovery.continuation, input)),
      );
    }
  });

  const drainReadyWork = (input: SessionRunInput) => drainReadyWorkPostRecovery(input);

  const drainAndLog = (input: SessionRunInput) =>
    keepAlive.withActiveWork(
      "session-control",
      drainReadyWorkPostRecovery(input).pipe(
        Effect.asVoid,
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            yield* markFatalIfDurable(cause);
            yield* Effect.logError("session control drain pass failed", {
              cause: Cause.pretty(cause),
            });
            return yield* Effect.failCause(cause);
          }),
        ),
      ),
    );

  const waitForIngressSignal = () =>
    Stream.fromQueue(ingressSignals).pipe(Stream.take(1), Stream.runDrain);

  const waitForNextSignal = () =>
    Effect.gen(function* () {
      const current = yield* Ref.get(executionState);
      if (current._tag === "Running") {
        yield* Effect.raceFirst(
          waitForIngressSignal(),
          Fiber.await(current.fiber).pipe(Effect.asVoid),
        );
        return;
      }
      if (current._tag === "Completed") {
        return;
      }
      yield* waitForIngressSignal();
    });

  const annotateRunInput = (input: SessionRunInput) =>
    annotateEdaSpan({
      "eda.model.provider": input.modelSelection.provider,
      "eda.model.id": input.modelSelection.modelId,
    });

  const runStartupRecovery = Effect.fnUntraced(function* (input: SessionRunInput) {
    yield* annotateRunInput(input);
    yield* initialize(input);
    yield* keepAlive.withActiveWork("session-recovery", runStartupRecoveryPass(input));
  });

  const runLiveControlLoop = Effect.fn(function* (input: SessionRunInput) {
    yield* annotateRunInput(input);
    while (true) {
      yield* drainAndLog(input);
      yield* waitForNextSignal();
    }
  });

  const start = Effect.fn(function* (input: SessionRunInput) {
    yield* runStartupRecovery(input);
    yield* runLiveControlLoop(input).pipe(Effect.forkIn(runtimeScope));
  });

  yield* sinkRegistry.startSinkRunners({
    appendDurableBatch,
    initialHead: (yield* Ref.get(state)).reduced.lastSeq,
    publishEphemeral: publishEphemeralCore,
    scope: runtimeScope,
  });

  return sessionApi;
});

const hydrateReducedStateCheckpoint = (
  store: EDASessionStoreShape,
  checkpoint: EDAReducerCheckpoint,
): Effect.Effect<ReducedState, EDASessionStoreError> =>
  Effect.gen(function* () {
    const referencedEvents = yield* store.loadCommittedEventsBySeq(
      reducedStateCheckpointEventSeqs(checkpoint.payload),
    );
    const state = yield* Effect.try({
      try: () => decodeReducedStateCheckpoint(checkpoint.payload, referencedEvents),
      catch: (error) =>
        new EDASessionStoreError({
          message: `hydrating framework reducer checkpoint: ${String(error)}`,
        }),
    });
    if (state.lastSeq !== checkpoint.throughSeq) {
      return yield* new EDASessionStoreError({
        message: `hydrating framework reducer checkpoint: payload lastSeq ${state.lastSeq} does not match row throughSeq ${checkpoint.throughSeq}`,
      });
    }
    return state;
  });

const hydrateFrameworkReducedState = (
  store: EDASessionStoreShape,
): Effect.Effect<HydratedFrameworkReducedState, EDASessionStoreError> =>
  Effect.gen(function* () {
    const checkpoint = yield* store.loadReducerCheckpoint(frameworkReducedStateReducerName);
    const checkpointState =
      checkpoint === undefined ||
      checkpoint.schemaVersion !== frameworkReducedStateReducerSchemaVersion
        ? initialReducedState
        : yield* hydrateReducedStateCheckpoint(store, checkpoint);
    const checkpointSeq =
      checkpoint === undefined ||
      checkpoint.schemaVersion !== frameworkReducedStateReducerSchemaVersion
        ? SequenceNumber.make(0)
        : checkpoint.throughSeq;
    const tail = yield* store.eventsAfter(checkpointSeq).pipe(
      Stream.runCollect,
      Effect.map((events) => Array.from(events)),
    );
    return { state: foldReducedState(checkpointState, tail), checkpointSeq };
  });

const hydrateAppReducerStates = (
  store: EDASessionStoreShape,
  reducers: ReadonlyArray<EDAReducer<any>>,
): Effect.Effect<HydratedAppReducerStates, EDASessionStoreError> =>
  Effect.gen(function* () {
    const entries = yield* Effect.forEach(reducers, (reducer) =>
      Effect.gen(function* () {
        const checkpoint = yield* store.loadReducerCheckpoint(reducer.name);
        const schemaVersion = reducerSchemaVersion(reducer);
        const checkpointState =
          checkpoint === undefined || checkpoint.schemaVersion !== schemaVersion
            ? reducer.initial
            : decodeReducerState(reducer, checkpoint.payload);
        const checkpointSeq =
          checkpoint === undefined || checkpoint.schemaVersion !== schemaVersion
            ? SequenceNumber.make(0)
            : checkpoint.throughSeq;
        const tail = yield* store.eventsAfter(checkpointSeq).pipe(
          Stream.runCollect,
          Effect.map((events) => Array.from(events)),
        );
        return [reducer.name, tail.reduce(reducer.reduce, checkpointState), checkpointSeq] as const;
      }),
    );
    const checkpointSeq = entries.reduce<SequenceNumber | undefined>(
      (min, [, , seq]) => (min === undefined || seq < min ? seq : min),
      undefined,
    );
    return {
      states: new Map(entries.map(([name, state]) => [name, state])),
      ...(checkpointSeq === undefined ? {} : { checkpointSeq }),
    };
  });

const isEDACommand = (input: EDASubmittable): input is EDACommand => "_tag" in input;

const commandWithId = (command: EDACommand, commandId: CommandId): EDACommand => {
  switch (command._tag) {
    case "SubmitMessage":
      return new SubmitMessageCommand({
        commandId,
        ...(command.idempotencyKey === undefined ? {} : { idempotencyKey: command.idempotencyKey }),
        disposition: command.disposition,
        content: command.content,
        ...(command.expectedPausedMessageIdsToCancel === undefined
          ? {}
          : { expectedPausedMessageIdsToCancel: command.expectedPausedMessageIdsToCancel }),
      });
    case "StopTurn":
      return new StopTurnCommand({
        commandId,
        ...(command.idempotencyKey === undefined ? {} : { idempotencyKey: command.idempotencyKey }),
      });
    case "CancelPendingMessage":
      return new CancelPendingMessageCommand({
        commandId,
        ...(command.idempotencyKey === undefined ? {} : { idempotencyKey: command.idempotencyKey }),
        messageId: command.messageId,
        reason: command.reason,
      });
    case "PromotePendingMessage":
      return new PromotePendingMessageCommand({
        commandId,
        ...(command.idempotencyKey === undefined ? {} : { idempotencyKey: command.idempotencyKey }),
        messageId: command.messageId,
      });
    case "ResumePendingMessages":
      return new ResumePendingMessagesCommand({
        commandId,
        ...(command.idempotencyKey === undefined ? {} : { idempotencyKey: command.idempotencyKey }),
        messageIds: command.messageIds,
      });
    default:
      throw assertNeverError(command, "EDA command");
  }
};

const rememberPreparedCommand = (
  event: DurableEventEnvelope,
  preparedByIdempotencyKey: Map<string, DurableEventEnvelope>,
  preparedByCommandId: Map<string, DurableEventEnvelope>,
): void => {
  if (event.type !== commandAdmittedEventType) {
    return;
  }
  const command = (event.payload as { readonly command?: EDACommand }).command;
  if (command?.idempotencyKey !== undefined) {
    preparedByIdempotencyKey.set(command.idempotencyKey, event);
  }
  if (command?.commandId !== undefined) {
    preparedByCommandId.set(command.commandId, event);
  }
};

const sameMessageIdSet = (
  left: ReadonlyArray<MessageId>,
  right: ReadonlyArray<MessageId>,
): boolean => {
  if (left.length !== right.length) {
    return false;
  }
  const rightIds = new Set(right);
  return left.every((messageId) => rightIds.has(messageId));
};

const remainingToolCallsForRun = (
  state: ReducedState,
  runId: RunId,
  maxToolCallsPerRun: NonNegativeInt,
): NonNegativeInt => {
  const used = Array.from(state.toolCalls.values()).filter(
    (tool) =>
      tool.decision?._tag === "Created" &&
      tool.decision.runId === runId &&
      !tool.decision.providerExecuted,
  ).length;
  return NonNegativeInt.make(Math.max(0, maxToolCallsPerRun - used));
};

const remainingToolRejectionCorrectionsForRun = (
  state: ReducedState,
  runId: RunId,
  limit: NonNegativeInt,
): NonNegativeInt => {
  const rejectedTurnIds = new Set<TurnId>();
  for (const tool of state.toolCalls.values()) {
    if (tool.decision?._tag === "Rejected" && tool.decision.runId === runId) {
      rejectedTurnIds.add(tool.decision.turnId);
    }
  }
  return NonNegativeInt.make(Math.max(0, limit - rejectedTurnIds.size));
};

type CommittedTurnFailed = CommittedDurableEvent & { readonly event: TurnFailedEvent };

const requireTurnFailed = (committed: CommittedDurableEvent) =>
  isTurnFailed(committed)
    ? Effect.succeed(committed)
    : Effect.die(new Error("TurnRunFailed did not include TurnFailed"));

const isTurnFailed = (committed: CommittedDurableEvent): committed is CommittedTurnFailed =>
  committed.event.type === turnFailedEventType;

const isSessionNoRunnableCommand = (
  result: SessionDrainProcessed | SessionNoRunnableCommand,
): result is SessionNoRunnableCommand =>
  "_tag" in result && result._tag === "SessionNoRunnableCommand";
