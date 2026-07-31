import * as Effect from "effect/Effect";
import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { initialReducedState } from "../domain/reduced-state";
import { CommandId, MessageId, SessionId } from "../types/core";
import {
  buildEDAPrompt,
  EDAPromptProjector,
  type SelectedPromptUserMessage,
} from "./prompt-projector";

const selectedMessage = (index: number): SelectedPromptUserMessage => ({
  commandId: CommandId.make(`018f6bd5-2f2a-7b1e-8f1${index}-1f2e3d4c5b6a`),
  messageId: MessageId.make(`018f6bd5-2f2a-7b1e-9f1${index}-1f2e3d4c5b6a`),
  content: [Prompt.textPart({ text: `selected ${index}` })],
});

const project = (selectedUserMessages: ReadonlyArray<SelectedPromptUserMessage>) =>
  Effect.gen(function* () {
    const projector = yield* EDAPromptProjector;
    return yield* buildEDAPrompt(projector, {
      sessionId: SessionId.make("018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a"),
      state: initialReducedState,
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
});
