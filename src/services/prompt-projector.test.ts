import * as Effect from "effect/Effect";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { initialReducedState, type MessageRecord } from "../domain/reduced-state";
import { CommandId, MessageId, SequenceNumber, SessionId, TurnId } from "../types/core";
import {
  buildEDAPrompt,
  EDAPromptProjector,
  type EDAPromptProjectorShape,
} from "./prompt-projector";

const COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a");
const TURN_ID = TurnId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");

const project = (
  input: {
    readonly projector?: EDAPromptProjectorShape;
    readonly state?: typeof initialReducedState;
  } = {},
) =>
  Effect.gen(function* () {
    const projector = input.projector ?? (yield* EDAPromptProjector);
    return yield* buildEDAPrompt(projector, {
      sessionId: SESSION_ID,
      state: input.state ?? initialReducedState,
      reducerStates: new Map(),
    });
  }).pipe(Effect.provide(EDAPromptProjector.Default), Effect.runPromise);

describe("EDAPromptProjector", () => {
  it("projects an empty durable state as empty context", async () => {
    expect(Prompt.make(await project()).content).toEqual([]);
  });

  it("includes admission-time user content only after state records turn consumption", async () => {
    const pending: MessageRecord = {
      _tag: "User",
      commandId: COMMAND_ID,
      content: [Prompt.textPart({ text: "selected from state" })],
      disposition: "queue",
      messageId: MessageId.make("018f6bd5-2f2a-7b1e-9f11-1f2e3d4c5b6a"),
      requestedDisposition: "queue",
      seq: SequenceNumber.make(1),
    };
    const pendingState = {
      ...initialReducedState,
      messages: new Map([[pending.messageId, pending]]),
    };

    expect(Prompt.make(await project({ state: pendingState })).content).toEqual([]);

    const consumed: MessageRecord = {
      ...pending,
      consumedSeq: SequenceNumber.make(2),
      consumedTurnId: TURN_ID,
    };
    const prompt = Prompt.make(
      await project({
        state: { ...pendingState, messages: new Map([[consumed.messageId, consumed]]) },
      }),
    ).content;
    expect(prompt).toMatchObject([
      { role: "user", content: [{ type: "text", text: "selected from state" }] },
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
      projectContext: () =>
        Effect.succeed({
          instructions: [Prompt.systemMessage({ content: "projected instructions" })],
          messages: [
            Prompt.userMessage({ content: [Prompt.textPart({ text: "projected data" })] }),
          ],
        }),
      projectState: (input) => Effect.succeed(input.state),
    };

    const prompt = await project({
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
    ]);
  });
});
