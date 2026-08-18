import type {
  ActiveCommandEvidence,
  CommandRecord,
  MessageRecord,
  PendingCommand,
  RunRecord,
} from "./reduced-state";
import type { MessageId, RunId, SequenceNumber, TurnId } from "../types/core";
import type { UserMessageContent } from "../types/commands";

/**
 * A steering message admitted for an active run but not necessarily consumed
 * by a later turn yet.
 */
export interface PendingSteeringMessage {
  /** Durable message identity used by `TurnStarted.inputMessageIds` when consumed. */
  readonly messageId: MessageId;
  /** Command that admitted this steering message. */
  readonly commandId: PendingCommand["commandId"];
  /** Active run this steering message may continue. */
  readonly runId: RunId;
  /** User content to append to the next continuation prompt when selected. */
  readonly content: UserMessageContent;
  /** Sequence where `SteeringMessageQueued` committed; used for one-by-one ordering. */
  readonly queuedSeq: SequenceNumber;
  /** Sequence where a later `TurnStarted` consumed this steering message. */
  readonly consumedSeq?: SequenceNumber;
  /** Turn that consumed this steering message. */
  readonly consumedTurnId?: TurnId;
}

/**
 * Materialized command scheduler view derived from durable `ReducedState`.
 *
 * `pendingCommands` means admitted commands with no `CommandStarted` yet.
 * `queuedCommands` is the FIFO subset that should wait for idle session work
 * (`SubmitMessage{disposition:"queue"}`). `activeControlCommands` is the
 * subset that may affect an active run (`StopTurn`, `steer`, `interrupt`).
 * `steeringByRun` is not a command queue; it is queued steering content already
 * admitted/completed as commands but not yet consumed by a turn.
 */
export interface CommandQueues {
  /** Started command without terminal command evidence, plus active run when known. */
  readonly active?: ActiveCommandEvidence;
  /** All admitted commands that have not yet crossed `CommandStarted`. */
  readonly pendingCommands: ReadonlyArray<PendingCommand>;
  /** FIFO user-message work that waits until no command/run is active. */
  readonly queuedCommands: ReadonlyArray<PendingCommand>;
  /** Stop/steer/interrupt work that can be interpreted while a run is active. */
  readonly activeControlCommands: ReadonlyArray<PendingCommand>;
  /** Unconsumed steering messages grouped by target run and sorted by queued seq. */
  readonly steeringByRun: ReadonlyMap<RunId, ReadonlyArray<PendingSteeringMessage>>;
  /** Admission-time queue messages eligible for separate work. */
  readonly pendingQueue: ReadonlyArray<PendingUserMessage>;
  /** Admission-time steer messages eligible at the next boundary. */
  readonly pendingSteers: ReadonlyArray<PendingUserMessage>;
  /** Queue messages held by an explicit interruption pause. */
  readonly pausedQueue: ReadonlyArray<PendingUserMessage>;
}

export interface PendingUserMessage {
  readonly messageId: MessageId;
  readonly commandId: PendingCommand["commandId"];
  readonly content: UserMessageContent;
  readonly submittedSeq: SequenceNumber;
  readonly effectiveSeq: SequenceNumber;
  readonly disposition: "queue" | "steer";
  readonly targetRunId?: RunId;
  readonly pausedByCommandId?: PendingCommand["commandId"];
}

/** Durable records needed to derive `CommandQueues` without services or refs. */
export interface CommandQueueSource {
  readonly commands: ReadonlyMap<PendingCommand["commandId"], CommandRecord>;
  readonly runs: ReadonlyMap<RunId, RunRecord>;
  readonly messages: ReadonlyMap<MessageId, MessageRecord>;
}

/** Empty queue view for `initialReducedState` before any durable events exist. */
export const emptyCommandQueues: CommandQueues = {
  pendingCommands: [],
  queuedCommands: [],
  activeControlCommands: [],
  steeringByRun: new Map(),
  pendingQueue: [],
  pendingSteers: [],
  pausedQueue: [],
};

/**
 * Derive scheduler queues from durable replay state.
 *
 * These queues are cached inside `ReducedState` for clarity and fast scheduler
 * reads, but durable command/message/run records remain the source of truth.
 * Never mutate a queue directly; fold durable events and derive again.
 */
export const deriveCommandQueues = (state: CommandQueueSource): CommandQueues => {
  const pendingCommands = Array.from(state.commands.values())
    .filter(isPendingCommandRecord)
    .map((record) => ({
      commandId: record.commandId,
      command: record.command,
      admittedSeq: record.admittedSeq,
      ...(record.admissionTrace === undefined ? {} : { admissionTrace: record.admissionTrace }),
    }))
    .sort((a, b) => a.admittedSeq - b.admittedSeq);

  const pendingMessages = derivePendingUserMessages(state.messages);
  const pendingMessageByCommandId = new Map(
    pendingMessages.map((message) => [message.commandId, message] as const),
  );
  return {
    active: deriveActiveCommand(state),
    pendingCommands,
    queuedCommands: pendingCommands.filter((command) => {
      if (command.command._tag !== "SubmitMessage") return false;
      const message = pendingMessageByCommandId.get(command.commandId);
      return (
        message?.disposition === "queue" ||
        (message === undefined && command.command.disposition === "queue")
      );
    }),
    activeControlCommands: pendingCommands.filter((command) =>
      isActiveControlCommand(command, pendingMessageByCommandId),
    ),
    steeringByRun: derivePendingSteeringByRun(state.messages),
    pendingQueue: pendingMessages.filter(
      (message) => message.disposition === "queue" && message.pausedByCommandId === undefined,
    ),
    pendingSteers: pendingMessages.filter((message) => message.disposition === "steer"),
    pausedQueue: pendingMessages.filter(
      (message) => message.disposition === "queue" && message.pausedByCommandId !== undefined,
    ),
  };
};

const derivePendingUserMessages = (
  messages: ReadonlyMap<MessageId, MessageRecord>,
): ReadonlyArray<PendingUserMessage> =>
  Array.from(messages.values())
    .flatMap((message): ReadonlyArray<PendingUserMessage> => {
      if (
        (message._tag !== "User" && message._tag !== "Steering") ||
        message.consumedSeq !== undefined ||
        message.cancelledSeq !== undefined
      ) {
        return [];
      }
      return [
        {
          messageId: message.messageId,
          commandId: message.commandId,
          content: message.content,
          submittedSeq: message.seq,
          effectiveSeq: message.promotedSeq ?? message.seq,
          disposition:
            message.pausedByCommandId !== undefined
              ? "queue"
              : message._tag === "Steering"
                ? "steer"
                : (message.disposition ?? "queue"),
          ...(message._tag === "Steering" ? { targetRunId: message.runId } : {}),
          ...(message.pausedByCommandId === undefined
            ? {}
            : { pausedByCommandId: message.pausedByCommandId }),
        },
      ];
    })
    .sort((left, right) => left.effectiveSeq - right.effectiveSeq);

const isPendingCommandRecord = (
  record: CommandRecord,
): record is CommandRecord & {
  readonly command: NonNullable<CommandRecord["command"]>;
  readonly admittedSeq: SequenceNumber;
} =>
  record.command !== undefined &&
  record.admittedSeq !== undefined &&
  record.startedSeq === undefined;

const isActiveControlCommand = (
  command: PendingCommand,
  pendingMessageByCommandId: ReadonlyMap<PendingCommand["commandId"], PendingUserMessage>,
): boolean => {
  switch (command.command._tag) {
    case "StopTurn":
    case "CancelPendingMessage":
    case "PromotePendingMessage":
      return true;
    case "ResumePendingMessages":
      return false;
    case "SubmitMessage":
      return (
        command.command.disposition === "interrupt" ||
        pendingMessageByCommandId.get(command.commandId)?.disposition === "steer" ||
        (pendingMessageByCommandId.get(command.commandId) === undefined &&
          command.command.disposition === "steer")
      );
  }
};

const deriveActiveCommand = (state: CommandQueueSource): ActiveCommandEvidence | undefined => {
  for (const command of state.commands.values()) {
    if (command.startedSeq !== undefined && command.terminal === undefined) {
      const activeRun = Array.from(state.runs.values()).find(
        (run) => run.commandIds.includes(command.commandId) && run.terminal === undefined,
      );
      return activeRun === undefined
        ? { commandId: command.commandId }
        : { commandId: command.commandId, runId: activeRun.runId };
    }
  }
  return undefined;
};

const derivePendingSteeringByRun = (
  messages: ReadonlyMap<MessageId, MessageRecord>,
): ReadonlyMap<RunId, ReadonlyArray<PendingSteeringMessage>> => {
  const grouped = new Map<RunId, Array<PendingSteeringMessage>>();
  for (const message of messages.values()) {
    if (
      message._tag !== "Steering" ||
      message.consumedSeq !== undefined ||
      message.cancelledSeq !== undefined
    ) {
      continue;
    }
    const existing = grouped.get(message.runId) ?? [];
    existing.push({
      messageId: message.messageId,
      commandId: message.commandId,
      runId: message.runId,
      content: message.content,
      queuedSeq: message.seq,
      ...(message.consumedSeq === undefined ? {} : { consumedSeq: message.consumedSeq }),
      ...(message.consumedTurnId === undefined ? {} : { consumedTurnId: message.consumedTurnId }),
    });
    grouped.set(message.runId, existing);
  }
  for (const [runId, entries] of grouped) {
    grouped.set(
      runId,
      entries.sort((a, b) => a.queuedSeq - b.queuedSeq),
    );
  }
  return grouped;
};
