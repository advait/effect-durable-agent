import * as Effect from "effect/Effect";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { MessageId } from "./core";
import {
  CancelPendingMessageCommand,
  CommandIdempotencyKey,
  EDACommand,
  PromotePendingMessageCommand,
  StopTurnCommand,
  SubmitMessageCommand,
  UserMessageContent,
} from "./commands";

const COMMAND_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const MESSAGE_ID = "018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a";

describe("command types", () => {
  it("decodes pending-message control commands and conditional clear submissions", () => {
    const cancel = Effect.runSync(
      Schema.decodeUnknownEffect(EDACommand)({
        _tag: "CancelPendingMessage",
        messageId: MESSAGE_ID,
        reason: "edit",
      }),
    );
    const promote = Effect.runSync(
      Schema.decodeUnknownEffect(EDACommand)({
        _tag: "PromotePendingMessage",
        messageId: MESSAGE_ID,
      }),
    );
    const submit = Effect.runSync(
      Schema.decodeUnknownEffect(EDACommand)({
        _tag: "SubmitMessage",
        disposition: "queue",
        content: "replace",
        expectedPausedMessageIdsToCancel: [MESSAGE_ID],
      }),
    );

    expect(cancel).toBeInstanceOf(CancelPendingMessageCommand);
    expect(promote).toBeInstanceOf(PromotePendingMessageCommand);
    expect(submit).toMatchObject({
      expectedPausedMessageIdsToCancel: [MessageId.make(MESSAGE_ID)],
    });
  });
  it("decodes text SubmitMessage commands as Effect AI user content", () => {
    const command = Effect.runSync(
      Schema.decodeUnknownEffect(EDACommand)({
        _tag: "SubmitMessage",
        commandId: COMMAND_ID,
        disposition: "queue",
        content: "hello",
      }),
    );

    expect(command).toBeInstanceOf(SubmitMessageCommand);
    expect(command).toEqual({
      _tag: "SubmitMessage",
      commandId: COMMAND_ID,
      disposition: "queue",
      content: [expect.objectContaining({ type: "text", text: "hello" })],
    });
  });

  it("accepts multimodal user content aligned to Effect AI Prompt.UserMessage", () => {
    const command = Effect.runSync(
      Schema.decodeUnknownEffect(EDACommand)({
        _tag: "SubmitMessage",
        commandId: COMMAND_ID,
        disposition: "queue",
        content: [
          { type: "text", text: "describe this" },
          {
            type: "file",
            mediaType: "image/png",
            fileName: "screenshot.png",
            data: "data:image/png;base64,abcd",
          },
        ],
      }),
    );

    expect(command).toBeInstanceOf(SubmitMessageCommand);
    if (command._tag !== "SubmitMessage") throw new Error("expected SubmitMessage");
    expect(command.content).toHaveLength(2);
    expect(command.content[0]).toEqual(expect.objectContaining({ type: "text" }));
    expect(command.content[1]).toEqual(
      expect.objectContaining({ type: "file", mediaType: "image/png" }),
    );
    expect(command.content.every(Prompt.isPart)).toBe(true);
    expect(Schema.is(UserMessageContent)(command.content)).toBe(true);
  });

  it("accepts caller idempotency keys without requiring commandId", () => {
    const command = Effect.runSync(
      Schema.decodeUnknownEffect(EDACommand)({
        _tag: "SubmitMessage",
        idempotencyKey: "web:create_session:example",
        disposition: "queue",
        content: "hello",
      }),
    );

    expect(command).toBeInstanceOf(SubmitMessageCommand);
    expect(command).toEqual({
      _tag: "SubmitMessage",
      idempotencyKey: "web:create_session:example",
      disposition: "queue",
      content: [expect.objectContaining({ type: "text", text: "hello" })],
    });
    expect(Schema.is(CommandIdempotencyKey)(command.idempotencyKey)).toBe(true);
  });

  it("decodes StopTurn commands as tagged class instances", () => {
    const command = Effect.runSync(
      Schema.decodeUnknownEffect(EDACommand)({
        _tag: "StopTurn",
        commandId: COMMAND_ID,
        idempotencyKey: "web:stop:example",
      }),
    );

    expect(command).toBeInstanceOf(StopTurnCommand);
    expect(command).toEqual({
      _tag: "StopTurn",
      commandId: COMMAND_ID,
      idempotencyKey: "web:stop:example",
    });
  });

  it("rejects invalid command boundaries", () => {
    expect(() =>
      Effect.runSync(
        Schema.decodeUnknownEffect(EDACommand)({
          _tag: "SubmitMessage",
          commandId: COMMAND_ID,
          disposition: "queue",
          content: [],
        }),
      ),
    ).toThrow();
    expect(() =>
      Effect.runSync(
        Schema.decodeUnknownEffect(EDACommand)({
          _tag: "SubmitMessage",
          commandId: COMMAND_ID,
          disposition: "merge",
          content: "hello",
        }),
      ),
    ).toThrow();
    expect(() =>
      Effect.runSync(
        Schema.decodeUnknownEffect(EDACommand)({
          _tag: "StopTurn",
          commandId: COMMAND_ID.replace("7", "4"),
        }),
      ),
    ).toThrow();
    expect(() =>
      Effect.runSync(
        Schema.decodeUnknownEffect(EDACommand)({
          _tag: "UnknownCommand",
          commandId: COMMAND_ID,
        }),
      ),
    ).toThrow();
  });
});
