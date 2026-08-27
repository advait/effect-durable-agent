import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { EDACommand } from "../types/commands";
import { CommandId, SequenceNumber, durablePosition } from "../types/core";
import {
  ModelSelectionPayload,
  NonNegativeInt,
  SystemPromptText,
  commandCancelledEventType,
  commandCompletedEventType,
  commandFailedEventType,
} from "../types/events";
import type {
  CommandCancelledEvent,
  CommandCompletedEvent,
  CommandFailedEvent,
  PositionedEvent,
} from "../types/events";
import { SessionState } from "./session-state";
import type { EDASubmittable, SessionCommandAdmissionError } from "./session-state";
import { CommittedDurableEvent } from "./session-store";
import { EDASessionQuery } from "./session-query";
import { annotateEdaSpan, committedBatchAttributes } from "./tracing";
import type { EDASessionQueryShape } from "./session-query";

/** Configuration for one scoped session runtime instance. */
export const EDARuntimeConfig = Schema.Struct({
  modelSelection: ModelSelectionPayload,
  /** Maximum framework-owned tool calls one model run may execute before failing safely. */
  maxToolCallsPerRun: Schema.optionalKey(NonNegativeInt),
  systemPrompt: Schema.optionalKey(SystemPromptText),
});
export type EDARuntimeConfig = typeof EDARuntimeConfig.Type;

/** Public runtime error surface for command admission helpers. */
export type EDARuntimeError = SessionCommandAdmissionError;

/** Command terminal events observed by blocking runtime helpers. */
export type EDACommandTerminalEvent =
  | CommandCompletedEvent
  | CommandFailedEvent
  | CommandCancelledEvent;

/** Committed durable event narrowed to a command terminal boundary. */
export type CommittedCommandTerminalEvent = CommittedDurableEvent & {
  readonly event: EDACommandTerminalEvent;
};

/** Overloaded public submit API for single commands or ordered durable batches. */
export interface EDARuntimeSubmit {
  /** Submit one command and return its committed CommandAdmitted event. */
  (command: EDACommand): Effect.Effect<CommittedDurableEvent, EDARuntimeError>;
  /** Submit one ordered durable batch of commands and/or app events. */
  (
    input: ReadonlyArray<EDASubmittable>,
  ): Effect.Effect<ReadonlyArray<CommittedDurableEvent>, EDARuntimeError>;
}

/** Public facade exposed to hosts and route handlers for one live session runtime. */
export interface EDARuntimeShape {
  /** Submit commands and/or app durable events durably and wake the dispatch process if needed. */
  readonly submit: EDARuntimeSubmit;
  /** Block until the command reaches CommandCompleted, CommandFailed, or CommandCancelled. */
  readonly blockOnCommand: (
    commandId: CommandId,
    afterSeq?: SequenceNumber,
  ) => Effect.Effect<CommittedCommandTerminalEvent, EDARuntimeError>;
  /** Admit one command and block until that command reaches a terminal command boundary. */
  readonly submitAndBlock: (
    command: EDACommand,
  ) => Effect.Effect<CommittedCommandTerminalEvent, EDARuntimeError>;
  /** Read the authoritative live session snapshot. */
  readonly snapshot: EDASessionQueryShape["snapshot"];
  /** Read durable user/assistant messages in committed order. */
  readonly messages: EDASessionQueryShape["messages"];
  /** Backfill durable events after a committed sequence, then follow live events. */
  readonly eventsAfter: EDASessionQueryShape["eventsAfter"];
  /** Read one bounded page of committed durable events plus the committed head. */
  readonly readEventPage: EDASessionQueryShape["readEventPage"];
}

/** Top-level public facade for one effect-durable-agent session runtime. */
export class EDARuntime extends Context.Service<EDARuntime, EDARuntimeShape>()(
  "@effect-durable-agent/EDARuntime",
) {
  static readonly Live = (config: EDARuntimeConfig) =>
    Layer.effect(EDARuntime, makeLiveRuntime(config));
}

const makeLiveRuntime = (config: EDARuntimeConfig) =>
  Effect.gen(function* () {
    const sessionState = yield* SessionState;
    const query = yield* EDASessionQuery;

    const runInput = {
      modelSelection: config.modelSelection,
      ...(config.maxToolCallsPerRun === undefined
        ? {}
        : { maxToolCallsPerRun: config.maxToolCallsPerRun }),
      ...(config.systemPrompt === undefined ? {} : { systemPrompt: config.systemPrompt }),
    };
    yield* sessionState.start(runInput);

    const blockOnCommand = Effect.fn(function* (
      commandId: CommandId,
      afterSeq: SequenceNumber = SequenceNumber.make(0),
    ) {
      yield* annotateEdaSpan({ "eda.command.id": commandId, "eda.seq.after": afterSeq });
      return yield* Effect.scoped(
        Effect.gen(function* () {
          const events = yield* query.eventsAfter(afterSeq);
          const terminal = yield* events.pipe(
            Stream.filter(isCommandTerminalFor(commandId)),
            Stream.take(1),
            Stream.map(toCommittedCommandTerminal),
            Stream.runCollect,
            Effect.map((events) => events[0]),
          );
          return terminal === undefined
            ? yield* Effect.die(new Error("Expected command terminal event"))
            : terminal;
        }),
      );
    });

    const submit = Effect.fn(function* (input: EDACommand | ReadonlyArray<EDASubmittable>) {
      const items = Array.isArray(input) ? input : [input as EDACommand];
      yield* annotateEdaSpan({
        "eda.command.submittable_count": items.length,
        "eda.command.count": items.filter((item) => "_tag" in item).length,
      });
      const committed = Array.isArray(input)
        ? yield* sessionState.submitBatch(input)
        : [yield* sessionState.admitCommand(input as EDACommand)];
      yield* annotateEdaSpan(committedBatchAttributes(committed));
      return Array.isArray(input) ? committed : committed[0]!;
    }) as EDARuntimeSubmit;

    return {
      submit,
      blockOnCommand,
      submitAndBlock: Effect.fn(function* (command: EDACommand) {
        yield* annotateEdaSpan({
          "eda.command.kind": command._tag,
          "eda.command.id": command.commandId,
          "eda.command.disposition": "disposition" in command ? command.disposition : undefined,
        });
        const admitted = yield* sessionState.admitCommand(command);
        return yield* blockOnCommand(commandIdFromCommandAdmitted(admitted), admitted.position.seq);
      }),
      snapshot: () => query.snapshot(),
      messages: () => query.messages(),
      eventsAfter: (afterSeq: SequenceNumber) => query.eventsAfter(afterSeq),
      readEventPage: (afterSeq: SequenceNumber, limit: number) =>
        query.readEventPage(afterSeq, limit),
    };
  });

const isCommandTerminalFor =
  (commandId: CommandId) =>
  (event: PositionedEvent): boolean => {
    if (
      event.event.type !== commandCompletedEventType &&
      event.event.type !== commandFailedEventType &&
      event.event.type !== commandCancelledEventType
    ) {
      return false;
    }
    return (event.event.payload as { readonly commandId: CommandId }).commandId === commandId;
  };

const toCommittedCommandTerminal = (event: PositionedEvent): CommittedCommandTerminalEvent =>
  CommittedDurableEvent.make({
    position: durablePosition(event.position.seq),
    event: event.event as EDACommandTerminalEvent,
  }) as CommittedCommandTerminalEvent;

const commandIdFromCommandAdmitted = (committed: CommittedDurableEvent): CommandId => {
  const commandId = (committed.event.payload as { readonly command?: EDACommand }).command
    ?.commandId;
  if (commandId === undefined) {
    throw new Error("CommandAdmitted event is missing a framework commandId");
  }
  return commandId;
};
