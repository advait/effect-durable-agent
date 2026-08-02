import * as Effect from "effect/Effect";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { initialReducedState, type MessageRecord } from "../domain/reduced-state";
import { CommandId, MessageId, RunId, SequenceNumber, SessionId } from "../types/core";
import {
  buildEDAPrompt,
  EDAPromptProjector,
  type EDAPromptProjectorShape,
  type SelectedPromptUserMessage,
} from "./prompt-projector";

const COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const RUN_ID = RunId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a");

const selectedMessage = (index: number): SelectedPromptUserMessage => ({
  commandId: CommandId.make(`018f6bd5-2f2a-7b1e-8f1${index}-1f2e3d4c5b6a`),
  messageId: MessageId.make(`018f6bd5-2f2a-7b1e-9f1${index}-1f2e3d4c5b6a`),
  content: [Prompt.textPart({ text: `selected ${index}` })],
});

const project = (
  selectedUserMessages: ReadonlyArray<SelectedPromptUserMessage>,
  input: {
    readonly projector?: EDAPromptProjectorShape;
    readonly state?: typeof initialReducedState;
  } = {},
) =>
  Effect.gen(function* () {
    const projector = input.projector ?? (yield* EDAPromptProjector);
    return yield* buildEDAPrompt(projector, {
      commandId: COMMAND_ID,
      runId: RUN_ID,
      sessionId: SESSION_ID,
      state: input.state ?? initialReducedState,
      reducerStates: new Map(),
      selectedUserMessages,
    });
  }).pipe(Effect.provide(EDAPromptProjector.Default), Effect.runPromise);

describe("EDAPromptProjector", () => {
  it("projects an empty selected-message collection as transcript-only context", async () => {
    expect(Prompt.make(await project([])).content).toEqual([]);
  });

  it("preserves selected-message cardinality and order", async () => {
    const prompt = Prompt.make(await project([selectedMessage(1), selectedMessage(2)])).content;

    expect(prompt).toMatchObject([
      { role: "user", content: [{ type: "text", text: "selected 1" }] },
      { role: "user", content: [{ type: "text", text: "selected 2" }] },
    ]);
  });

  it("places projected instructions and data around durable transcript history", async () => {
    const system: MessageRecord = {
      _tag: "System",
      content: "durable instructions",
      messageId: MessageId.make("018f6bd5-2f2a-7b1e-8f21-1f2e3d4c5b6a"),
      seq: SequenceNumber.make(1),
    };
    const priorUser: MessageRecord = {
      _tag: "User",
      commandId: COMMAND_ID,
      content: [Prompt.textPart({ text: "durable user" })],
      messageId: MessageId.make("018f6bd5-2f2a-7b1e-8f22-1f2e3d4c5b6a"),
      seq: SequenceNumber.make(2),
    };
    const projector: EDAPromptProjectorShape = {
      projectContext: (input) => {
        expect(input.commandId).toBe(COMMAND_ID);
        expect(input.runId).toBe(RUN_ID);
        return Effect.succeed({
          instructions: [Prompt.systemMessage({ content: "projected instructions" })],
          messages: [
            Prompt.userMessage({ content: [Prompt.textPart({ text: "projected data" })] }),
          ],
        });
      },
      projectState: (input) => Effect.succeed(input.state),
      projectUserMessageContent: (_input, message) => Effect.succeed(message.content),
    };

    const prompt = await project([selectedMessage(1)], {
      projector,
      state: {
        ...initialReducedState,
        messages: new Map([
          [system.messageId, system],
          [priorUser.messageId, priorUser],
        ]),
      },
    });

    expect(Prompt.make(prompt).content).toMatchObject([
      { role: "system", content: "durable instructions" },
      { role: "system", content: "projected instructions" },
      { role: "user", content: [{ text: "durable user" }] },
      { role: "user", content: [{ text: "projected data" }] },
      { role: "user", content: [{ text: "selected 1" }] },
    ]);
  });
});
