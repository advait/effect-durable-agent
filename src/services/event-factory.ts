import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import {
  CommandAdmittedEvent,
  CommandAdmittedPayload,
  CommandCancelledEvent,
  CommandCancelledPayload,
  CommandCompletedEvent,
  CommandCompletedPayload,
  CommandFailedEvent,
  CommandFailedPayload,
  CommandStartedEvent,
  CommandStartedPayload,
  CompactionCompletedEvent,
  CompactionCompletedPayload,
  CompactionFailedEvent,
  CompactionFailedPayload,
  CompactionRequestedEvent,
  CompactionRequestedPayload,
  CompactionStartedEvent,
  CompactionStartedPayload,
  ContextRebasedEvent,
  ContextRebasedPayload,
  AssistantMessageCommittedEvent,
  AssistantMessageCommittedPayload,
  AssistantPartialCommittedEvent,
  AssistantPartialCommittedPayload,
  EventType,
  InferenceCompletedEvent,
  InferenceCompletedPayload,
  InferenceFailedEvent,
  InferenceFailedPayload,
  InferenceStartedEvent,
  InferenceStartedPayload,
  SummaryCreatedEvent,
  SummaryCreatedPayload,
  SystemMessageCommittedEvent,
  SystemMessageCommittedPayload,
  ReasoningDeltaEvent,
  ReasoningDeltaPayload,
  RecoveryCompletedEvent,
  RecoveryCompletedPayload,
  RunCompletedEvent,
  RunCompletedPayload,
  RunFailedEvent,
  RunFailedPayload,
  RunInterruptedEvent,
  RunInterruptedPayload,
  RunStartedEvent,
  RunStartedPayload,
  StopTurnAppliedEvent,
  StopTurnAppliedPayload,
  StopTurnRequestedEvent,
  StopTurnRequestedPayload,
  SteeringMessageCancelledEvent,
  SteeringMessageCancelledPayload,
  SteeringMessageQueuedEvent,
  SteeringMessageQueuedPayload,
  TextDeltaEvent,
  TextDeltaPayload,
  ToolCallCompletedEvent,
  ToolCallCompletedPayload,
  ToolCallCreatedEvent,
  ToolCallCreatedPayload,
  ToolCallFailedEvent,
  ToolCallFailedPayload,
  ToolCallRejectedEvent,
  ToolCallRejectedPayload,
  ToolCallStartedEvent,
  ToolCallStartedPayload,
  ToolParamsDeltaEvent,
  ToolParamsDeltaPayload,
  ToolParamsEndEvent,
  ToolParamsEndPayload,
  ToolParamsStartEvent,
  ToolParamsStartPayload,
  TurnCompletedEvent,
  TurnCompletedPayload,
  TurnFailedEvent,
  TurnFailedPayload,
  TurnStartedEvent,
  TurnStartedPayload,
  TurnStoppedEvent,
  TurnStoppedPayload,
  UnixEpochMillis,
  UserMessageCommittedEvent,
  UserMessageCommittedPayload,
  UserMessageSubmittedEvent,
  UserMessageSubmittedPayload,
  UserMessagePromotedEvent,
  UserMessagePromotedPayload,
  UserMessageCancelledEvent,
  UserMessageCancelledPayload,
  PendingMessagesPausedEvent,
  PendingMessagesPausedPayload,
  commandAdmittedEventType,
  commandCancelledEventType,
  commandCompletedEventType,
  commandFailedEventType,
  commandStartedEventType,
  compactionCompletedEventType,
  compactionFailedEventType,
  compactionRequestedEventType,
  compactionStartedEventType,
  contextRebasedEventType,
  assistantMessageCommittedEventType,
  assistantPartialCommittedEventType,
  effectDurableAgentNamespace,
  inferenceCompletedEventType,
  inferenceFailedEventType,
  inferenceStartedEventType,
  reasoningDeltaEventType,
  recoveryCompletedEventType,
  runCompletedEventType,
  runFailedEventType,
  runInterruptedEventType,
  runStartedEventType,
  schemaV1,
  steeringMessageCancelledEventType,
  summaryCreatedEventType,
  systemMessageCommittedEventType,
  steeringMessageQueuedEventType,
  stopTurnAppliedEventType,
  stopTurnRequestedEventType,
  textDeltaEventType,
  toolCallCompletedEventType,
  toolCallCreatedEventType,
  toolCallFailedEventType,
  toolCallRejectedEventType,
  toolCallStartedEventType,
  toolParamsDeltaEventType,
  toolParamsEndEventType,
  toolParamsStartEventType,
  turnCompletedEventType,
  turnFailedEventType,
  turnStartedEventType,
  turnStoppedEventType,
  userMessageCommittedEventType,
  userMessageSubmittedEventType,
  userMessagePromotedEventType,
  userMessageCancelledEventType,
  pendingMessagesPausedEventType,
} from "../types/events";
import { IdGenerator } from "./id-generator";
import { SessionContext } from "./session-context";
import { currentOrRootEDAEventTrace } from "./tracing";

/** Central envelope factory that stamps session id, event id, and current clock time. */
export interface EventFactoryShape {
  // Durable lifecycle events.
  readonly commandAdmitted: (
    payload: CommandAdmittedPayload,
  ) => Effect.Effect<CommandAdmittedEvent>;
  readonly commandStarted: (payload: CommandStartedPayload) => Effect.Effect<CommandStartedEvent>;
  readonly commandCompleted: (
    payload: CommandCompletedPayload,
  ) => Effect.Effect<CommandCompletedEvent>;
  readonly commandFailed: (payload: CommandFailedPayload) => Effect.Effect<CommandFailedEvent>;
  readonly commandCancelled: (
    payload: CommandCancelledPayload,
  ) => Effect.Effect<CommandCancelledEvent>;
  readonly systemMessageCommitted: (
    payload: SystemMessageCommittedPayload,
  ) => Effect.Effect<SystemMessageCommittedEvent>;
  readonly userMessageCommitted: (
    payload: UserMessageCommittedPayload,
  ) => Effect.Effect<UserMessageCommittedEvent>;
  readonly userMessageSubmitted: (
    payload: UserMessageSubmittedPayload,
  ) => Effect.Effect<UserMessageSubmittedEvent>;
  readonly userMessagePromoted: (
    payload: UserMessagePromotedPayload,
  ) => Effect.Effect<UserMessagePromotedEvent>;
  readonly userMessageCancelled: (
    payload: UserMessageCancelledPayload,
  ) => Effect.Effect<UserMessageCancelledEvent>;
  readonly pendingMessagesPaused: (
    payload: PendingMessagesPausedPayload,
  ) => Effect.Effect<PendingMessagesPausedEvent>;
  readonly steeringMessageQueued: (
    payload: SteeringMessageQueuedPayload,
  ) => Effect.Effect<SteeringMessageQueuedEvent>;
  readonly steeringMessageCancelled: (
    payload: SteeringMessageCancelledPayload,
  ) => Effect.Effect<SteeringMessageCancelledEvent>;
  readonly assistantMessageCommitted: (
    payload: AssistantMessageCommittedPayload,
  ) => Effect.Effect<AssistantMessageCommittedEvent>;
  readonly assistantPartialCommitted: (
    payload: AssistantPartialCommittedPayload,
  ) => Effect.Effect<AssistantPartialCommittedEvent>;
  readonly runStarted: (payload: RunStartedPayload) => Effect.Effect<RunStartedEvent>;
  readonly runCompleted: (payload: RunCompletedPayload) => Effect.Effect<RunCompletedEvent>;
  readonly runFailed: (payload: RunFailedPayload) => Effect.Effect<RunFailedEvent>;
  readonly recoveryCompleted: (
    payload: RecoveryCompletedPayload,
  ) => Effect.Effect<RecoveryCompletedEvent>;
  /** @deprecated Emit `runFailed` with `error.code = "run.interrupted"`. Retained for legacy event construction only. */
  readonly runInterrupted: (payload: RunInterruptedPayload) => Effect.Effect<RunInterruptedEvent>;
  readonly turnStarted: (payload: TurnStartedPayload) => Effect.Effect<TurnStartedEvent>;
  readonly turnCompleted: (payload: TurnCompletedPayload) => Effect.Effect<TurnCompletedEvent>;
  readonly turnFailed: (payload: TurnFailedPayload) => Effect.Effect<TurnFailedEvent>;
  /** @deprecated Emit `turnFailed` with `error.code = "turn.interrupted"`. Retained for legacy event construction only. */
  readonly turnStopped: (payload: TurnStoppedPayload) => Effect.Effect<TurnStoppedEvent>;
  readonly inferenceStarted: (
    payload: InferenceStartedPayload,
  ) => Effect.Effect<InferenceStartedEvent>;
  readonly inferenceCompleted: (
    payload: InferenceCompletedPayload,
  ) => Effect.Effect<InferenceCompletedEvent>;
  readonly inferenceFailed: (
    payload: InferenceFailedPayload,
  ) => Effect.Effect<InferenceFailedEvent>;
  readonly toolCallCreated: (
    payload: ToolCallCreatedPayload,
  ) => Effect.Effect<ToolCallCreatedEvent>;
  readonly toolCallRejected: (
    payload: ToolCallRejectedPayload,
  ) => Effect.Effect<ToolCallRejectedEvent>;
  readonly toolCallStarted: (
    payload: ToolCallStartedPayload,
  ) => Effect.Effect<ToolCallStartedEvent>;
  readonly toolCallCompleted: (
    payload: ToolCallCompletedPayload,
  ) => Effect.Effect<ToolCallCompletedEvent>;
  readonly toolCallFailed: (payload: ToolCallFailedPayload) => Effect.Effect<ToolCallFailedEvent>;
  readonly stopTurnRequested: (
    payload: StopTurnRequestedPayload,
  ) => Effect.Effect<StopTurnRequestedEvent>;
  readonly stopTurnApplied: (
    payload: StopTurnAppliedPayload,
  ) => Effect.Effect<StopTurnAppliedEvent>;
  readonly compactionRequested: (
    payload: CompactionRequestedPayload,
  ) => Effect.Effect<CompactionRequestedEvent>;
  readonly compactionStarted: (
    payload: CompactionStartedPayload,
  ) => Effect.Effect<CompactionStartedEvent>;
  readonly summaryCreated: (payload: SummaryCreatedPayload) => Effect.Effect<SummaryCreatedEvent>;
  readonly contextRebased: (payload: ContextRebasedPayload) => Effect.Effect<ContextRebasedEvent>;
  readonly compactionCompleted: (
    payload: CompactionCompletedPayload,
  ) => Effect.Effect<CompactionCompletedEvent>;
  readonly compactionFailed: (
    payload: CompactionFailedPayload,
  ) => Effect.Effect<CompactionFailedEvent>;
  // Ephemeral live-only events.
  readonly textDelta: (payload: TextDeltaPayload) => Effect.Effect<TextDeltaEvent>;
  readonly reasoningDelta: (payload: ReasoningDeltaPayload) => Effect.Effect<ReasoningDeltaEvent>;
  readonly toolParamsStart: (
    payload: ToolParamsStartPayload,
  ) => Effect.Effect<ToolParamsStartEvent>;
  readonly toolParamsDelta: (
    payload: ToolParamsDeltaPayload,
  ) => Effect.Effect<ToolParamsDeltaEvent>;
  readonly toolParamsEnd: (payload: ToolParamsEndPayload) => Effect.Effect<ToolParamsEndEvent>;
}

/**
 * The one construction path for built-in event envelopes. It closes over the
 * session identity, mints `eventId`s, and stamps `createdAtMs` from the
 * ambient `Clock`, so lifecycle code provides only the payload.
 */
export class EventFactory extends Context.Service<EventFactory, EventFactoryShape>()(
  "@effect-durable-agent/EventFactory",
) {
  static readonly Live = Layer.effect(
    EventFactory,
    Effect.gen(function* () {
      const { sessionId } = yield* SessionContext;
      const ids = yield* IdGenerator;

      const envelope = (durability: "durable" | "ephemeral") =>
        Effect.gen(function* () {
          const eventId = yield* ids.makeEventId();
          const nowMs = yield* Clock.currentTimeMillis;
          const trace = yield* currentOrRootEDAEventTrace;
          return {
            namespace: effectDurableAgentNamespace,
            schemaVersion: schemaV1,
            durability,
            eventId,
            sessionId,
            createdAtMs: UnixEpochMillis.make(nowMs),
            trace,
          };
        });

      const event =
        (durability: "durable" | "ephemeral") =>
        <Payload, Event>(schema: { readonly make: (input: never) => Event }, type: EventType) =>
        (payload: Payload): Effect.Effect<Event> =>
          Effect.map(envelope(durability), (fields) =>
            schema.make({ ...fields, type, payload } as never),
          );
      const durable = event("durable");
      const ephemeral = event("ephemeral");

      return {
        commandAdmitted: durable<CommandAdmittedPayload, CommandAdmittedEvent>(
          CommandAdmittedEvent,
          commandAdmittedEventType,
        ),
        commandStarted: durable<CommandStartedPayload, CommandStartedEvent>(
          CommandStartedEvent,
          commandStartedEventType,
        ),
        commandCompleted: durable<CommandCompletedPayload, CommandCompletedEvent>(
          CommandCompletedEvent,
          commandCompletedEventType,
        ),
        commandFailed: durable<CommandFailedPayload, CommandFailedEvent>(
          CommandFailedEvent,
          commandFailedEventType,
        ),
        commandCancelled: durable<CommandCancelledPayload, CommandCancelledEvent>(
          CommandCancelledEvent,
          commandCancelledEventType,
        ),
        systemMessageCommitted: durable<SystemMessageCommittedPayload, SystemMessageCommittedEvent>(
          SystemMessageCommittedEvent,
          systemMessageCommittedEventType,
        ),
        userMessageCommitted: durable<UserMessageCommittedPayload, UserMessageCommittedEvent>(
          UserMessageCommittedEvent,
          userMessageCommittedEventType,
        ),
        userMessageSubmitted: durable<UserMessageSubmittedPayload, UserMessageSubmittedEvent>(
          UserMessageSubmittedEvent,
          userMessageSubmittedEventType,
        ),
        userMessagePromoted: durable<UserMessagePromotedPayload, UserMessagePromotedEvent>(
          UserMessagePromotedEvent,
          userMessagePromotedEventType,
        ),
        userMessageCancelled: durable<UserMessageCancelledPayload, UserMessageCancelledEvent>(
          UserMessageCancelledEvent,
          userMessageCancelledEventType,
        ),
        pendingMessagesPaused: durable<PendingMessagesPausedPayload, PendingMessagesPausedEvent>(
          PendingMessagesPausedEvent,
          pendingMessagesPausedEventType,
        ),
        steeringMessageQueued: durable<SteeringMessageQueuedPayload, SteeringMessageQueuedEvent>(
          SteeringMessageQueuedEvent,
          steeringMessageQueuedEventType,
        ),
        steeringMessageCancelled: durable<
          SteeringMessageCancelledPayload,
          SteeringMessageCancelledEvent
        >(SteeringMessageCancelledEvent, steeringMessageCancelledEventType),
        assistantMessageCommitted: durable<
          AssistantMessageCommittedPayload,
          AssistantMessageCommittedEvent
        >(AssistantMessageCommittedEvent, assistantMessageCommittedEventType),
        assistantPartialCommitted: durable<
          AssistantPartialCommittedPayload,
          AssistantPartialCommittedEvent
        >(AssistantPartialCommittedEvent, assistantPartialCommittedEventType),
        runStarted: durable<RunStartedPayload, RunStartedEvent>(
          RunStartedEvent,
          runStartedEventType,
        ),
        runCompleted: durable<RunCompletedPayload, RunCompletedEvent>(
          RunCompletedEvent,
          runCompletedEventType,
        ),
        runFailed: durable<RunFailedPayload, RunFailedEvent>(RunFailedEvent, runFailedEventType),
        recoveryCompleted: durable<RecoveryCompletedPayload, RecoveryCompletedEvent>(
          RecoveryCompletedEvent,
          recoveryCompletedEventType,
        ),
        runInterrupted: durable<RunInterruptedPayload, RunInterruptedEvent>(
          RunInterruptedEvent,
          runInterruptedEventType,
        ),
        turnStarted: durable<TurnStartedPayload, TurnStartedEvent>(
          TurnStartedEvent,
          turnStartedEventType,
        ),
        turnCompleted: durable<TurnCompletedPayload, TurnCompletedEvent>(
          TurnCompletedEvent,
          turnCompletedEventType,
        ),
        turnFailed: durable<TurnFailedPayload, TurnFailedEvent>(
          TurnFailedEvent,
          turnFailedEventType,
        ),
        turnStopped: durable<TurnStoppedPayload, TurnStoppedEvent>(
          TurnStoppedEvent,
          turnStoppedEventType,
        ),
        inferenceStarted: durable<InferenceStartedPayload, InferenceStartedEvent>(
          InferenceStartedEvent,
          inferenceStartedEventType,
        ),
        inferenceCompleted: durable<InferenceCompletedPayload, InferenceCompletedEvent>(
          InferenceCompletedEvent,
          inferenceCompletedEventType,
        ),
        inferenceFailed: durable<InferenceFailedPayload, InferenceFailedEvent>(
          InferenceFailedEvent,
          inferenceFailedEventType,
        ),
        toolCallCreated: durable<ToolCallCreatedPayload, ToolCallCreatedEvent>(
          ToolCallCreatedEvent,
          toolCallCreatedEventType,
        ),
        toolCallRejected: durable<ToolCallRejectedPayload, ToolCallRejectedEvent>(
          ToolCallRejectedEvent,
          toolCallRejectedEventType,
        ),
        toolCallStarted: durable<ToolCallStartedPayload, ToolCallStartedEvent>(
          ToolCallStartedEvent,
          toolCallStartedEventType,
        ),
        toolCallCompleted: durable<ToolCallCompletedPayload, ToolCallCompletedEvent>(
          ToolCallCompletedEvent,
          toolCallCompletedEventType,
        ),
        toolCallFailed: durable<ToolCallFailedPayload, ToolCallFailedEvent>(
          ToolCallFailedEvent,
          toolCallFailedEventType,
        ),
        stopTurnRequested: durable<StopTurnRequestedPayload, StopTurnRequestedEvent>(
          StopTurnRequestedEvent,
          stopTurnRequestedEventType,
        ),
        stopTurnApplied: durable<StopTurnAppliedPayload, StopTurnAppliedEvent>(
          StopTurnAppliedEvent,
          stopTurnAppliedEventType,
        ),
        compactionRequested: durable<CompactionRequestedPayload, CompactionRequestedEvent>(
          CompactionRequestedEvent,
          compactionRequestedEventType,
        ),
        compactionStarted: durable<CompactionStartedPayload, CompactionStartedEvent>(
          CompactionStartedEvent,
          compactionStartedEventType,
        ),
        summaryCreated: durable<SummaryCreatedPayload, SummaryCreatedEvent>(
          SummaryCreatedEvent,
          summaryCreatedEventType,
        ),
        contextRebased: durable<ContextRebasedPayload, ContextRebasedEvent>(
          ContextRebasedEvent,
          contextRebasedEventType,
        ),
        compactionCompleted: durable<CompactionCompletedPayload, CompactionCompletedEvent>(
          CompactionCompletedEvent,
          compactionCompletedEventType,
        ),
        compactionFailed: durable<CompactionFailedPayload, CompactionFailedEvent>(
          CompactionFailedEvent,
          compactionFailedEventType,
        ),
        textDelta: ephemeral<TextDeltaPayload, TextDeltaEvent>(TextDeltaEvent, textDeltaEventType),
        reasoningDelta: ephemeral<ReasoningDeltaPayload, ReasoningDeltaEvent>(
          ReasoningDeltaEvent,
          reasoningDeltaEventType,
        ),
        toolParamsStart: ephemeral<ToolParamsStartPayload, ToolParamsStartEvent>(
          ToolParamsStartEvent,
          toolParamsStartEventType,
        ),
        toolParamsDelta: ephemeral<ToolParamsDeltaPayload, ToolParamsDeltaEvent>(
          ToolParamsDeltaEvent,
          toolParamsDeltaEventType,
        ),
        toolParamsEnd: ephemeral<ToolParamsEndPayload, ToolParamsEndEvent>(
          ToolParamsEndEvent,
          toolParamsEndEventType,
        ),
      };
    }),
  );
}
