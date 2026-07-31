import * as Prompt from "effect/unstable/ai/Prompt";

import { durableTranscriptPrompt } from "../../../src/domain/message-transcript";
import { reduceCommittedEvents } from "../../../src/domain/reduced-state";
import type { CommittedDurableEvent } from "../../../src/services/session-store";
import { toJsonValue, type JsonValue } from "../json";

/** Durable replay summary used to inspect hydrated prompt shape. */
export interface ReplayIntrospection {
  readonly messageCount: number;
  readonly prompt: JsonValue;
}

/** Replay durable events into ReducedState and expose the prompt history it hydrates. */
export const replayDurablePrompt = (
  events: ReadonlyArray<CommittedDurableEvent>,
): ReplayIntrospection => {
  const state = reduceCommittedEvents(events);
  const prompt = durableTranscriptPrompt(state);
  return {
    messageCount: state.messages.size,
    prompt: toJsonValue(Prompt.make(prompt).content),
  };
};
