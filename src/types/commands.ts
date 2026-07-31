import * as Prompt from "effect/unstable/ai/Prompt";
import * as Schema from "effect/Schema";

import { CommandId, MessageId } from "./core";

/** Caller-owned stable retry/correlation key for a submitted command. */
export const CommandIdempotencyKey = Schema.NonEmptyString.pipe(
  Schema.brand("CommandIdempotencyKey"),
);
export type CommandIdempotencyKey = typeof CommandIdempotencyKey.Type;

const CommandSubmissionFields = {
  /**
   * Optional low-level lifecycle id. Normal app code should omit this and let EDA mint it.
   */
  commandId: Schema.optionalKey(CommandId),
  /** Caller-owned stable retry/correlation key. Not required to be a UUID. */
  idempotencyKey: Schema.optionalKey(CommandIdempotencyKey),
};

/** How a submitted user message interacts with an active run. */
export const SubmitMessageDisposition = Schema.Literals(["queue", "steer", "interrupt"]);
export type SubmitMessageDisposition = typeof SubmitMessageDisposition.Type;

/** User-authored content parts accepted into the durable command stream. */
export const UserMessagePart = Schema.Union([Prompt.TextPart, Prompt.FilePart]);
export type UserMessagePart = typeof UserMessagePart.Type;

/** Non-empty user-authored content, aligned to Effect AI `Prompt.UserMessage` content. */
export const UserMessageContent = Schema.Union([
  Prompt.ContentFromString,
  Schema.NonEmptyArray(UserMessagePart),
]);
export type UserMessageContent = typeof UserMessageContent.Type;

/** Submit a user message to the session command stream. */
export class SubmitMessageCommand extends Schema.TaggedClass<SubmitMessageCommand>()(
  "SubmitMessage",
  {
    ...CommandSubmissionFields,
    disposition: SubmitMessageDisposition,
    content: UserMessageContent,
    expectedPausedMessageIdsToCancel: Schema.optionalKey(Schema.Array(MessageId)),
  },
) {}

export const PendingMessageCancellationReason = Schema.Literals(["edit", "user-cancel"]);
export type PendingMessageCancellationReason = typeof PendingMessageCancellationReason.Type;

export class CancelPendingMessageCommand extends Schema.TaggedClass<CancelPendingMessageCommand>()(
  "CancelPendingMessage",
  {
    ...CommandSubmissionFields,
    messageId: MessageId,
    reason: PendingMessageCancellationReason,
  },
) {}

export class PromotePendingMessageCommand extends Schema.TaggedClass<PromotePendingMessageCommand>()(
  "PromotePendingMessage",
  {
    ...CommandSubmissionFields,
    messageId: MessageId,
  },
) {}

/** Framework-internal owner for restarting unconsumed messages after unexpected failure. */
export class ResumePendingMessagesCommand extends Schema.TaggedClass<ResumePendingMessagesCommand>()(
  "ResumePendingMessages",
  {
    ...CommandSubmissionFields,
    messageIds: Schema.NonEmptyArray(MessageId),
  },
) {}

/** Request interruption/finalization of the active turn. */
export class StopTurnCommand extends Schema.TaggedClass<StopTurnCommand>()("StopTurn", {
  ...CommandSubmissionFields,
}) {}

/** Mutating input accepted by effect-durable-agent before runtime processing. */
export const EDACommand = Schema.Union([
  SubmitMessageCommand,
  StopTurnCommand,
  CancelPendingMessageCommand,
  PromotePendingMessageCommand,
  ResumePendingMessagesCommand,
]);
export type EDACommand = typeof EDACommand.Type;
