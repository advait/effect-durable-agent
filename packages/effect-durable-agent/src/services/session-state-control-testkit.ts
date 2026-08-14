import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";

import { StopTurnCommand, SubmitMessageCommand } from "../types/commands";
import {
  InferenceId,
  CommandId,
  EventId,
  MessageId,
  RunId,
  SequenceNumber,
  SessionId,
  ToolCallId,
  TurnId,
} from "../types/core";
import { ProviderPartId, ToolName } from "../types/events";
import { decideDispatch } from "../domain/dispatch-policy";
import { foldReducedState, initialReducedState, pendingCommands } from "../domain/reduced-state";
import { SessionState } from "./session-state";
import { EDASessionStore, EDASessionStoreError } from "./session-store";
import { EventFactory } from "./event-factory";
import type { CommittedDurableEvent, EDASessionStoreShape } from "./session-store";
import { LiveEventBus } from "./live-event-bus";
import type { InferenceRunnerStreamPart } from "./inference-runner";
import { makeEdaTestLayer } from "../testkit/layers";

export {
  InferenceId,
  CommandId,
  EDASessionStore,
  EDASessionStoreError,
  EventFactory,
  EventId,
  LiveEventBus,
  MessageId,
  ProviderPartId,
  RunId,
  SequenceNumber,
  SessionId,
  SessionState,
  StopTurnCommand,
  SubmitMessageCommand,
  ToolCallId,
  ToolName,
  TurnId,
  decideDispatch,
  foldReducedState,
  initialReducedState,
  makeEdaTestLayer,
  pendingCommands,
};

/** Stable UUIDv7 fixture values shared by session-state control tests. */
export const SESSION_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
export const COMMAND_ID = "018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a";
export const SECOND_COMMAND_ID = "018f6bd5-2f2a-7b1e-8f1c-1f2e3d4c5b6a";
export const THIRD_COMMAND_ID = "018f6bd5-2f2a-7b1e-8f1e-1f2e3d4c5b6a";
export const STOP_TURN_COMMAND_ID = "018f6bd5-2f2a-7b1e-8f1d-1f2e3d4c5b6a";
export const RUN_ID = "018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a";
export const TURN_ID = "018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a";
export const INFERENCE_ID = "018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a";
export const TOOL_CALL_ID = "018f6bd5-2f2a-7b1e-8f20-1f2e3d4c5b6a";
export const COMMAND_ADMITTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f2a-1f2e3d4c5b6a";
export const SECOND_COMMAND_ADMITTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f2b-1f2e3d4c5b6a";
export const COMMAND_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f3a-1f2e3d4c5b6a";
export const USER_MESSAGE_ID = "018f6bd5-2f2a-7b1e-8f3b-1f2e3d4c5b6a";
export const USER_MESSAGE_EVENT_ID = "018f6bd5-2f2a-7b1e-8f3c-1f2e3d4c5b6a";
export const RUN_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f4a-1f2e3d4c5b6a";
export const TURN_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f5a-1f2e3d4c5b6a";
export const INFERENCE_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f6a-1f2e3d4c5b6a";
export const TEXT_EVENT_ID = "018f6bd5-2f2a-7b1e-8f7a-1f2e3d4c5b6a";
export const INFERENCE_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f8a-1f2e3d4c5b6a";
export const ASSISTANT_MESSAGE_ID = "018f6bd5-2f2a-7b1e-8f8b-1f2e3d4c5b6a";
export const ASSISTANT_MESSAGE_EVENT_ID = "018f6bd5-2f2a-7b1e-8f8c-1f2e3d4c5b6a";
export const TURN_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f9a-1f2e3d4c5b6a";
export const RUN_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8faa-1f2e3d4c5b6a";
export const COMMAND_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8fab-1f2e3d4c5b6a";
export const INFERENCE_FAILED_EVENT_ID = "018f6bd5-2f2a-7b1e-8fac-1f2e3d4c5b6a";
export const TURN_FAILED_EVENT_ID = "018f6bd5-2f2a-7b1e-8fad-1f2e3d4c5b6a";
export const RUN_FAILED_EVENT_ID = "018f6bd5-2f2a-7b1e-8fae-1f2e3d4c5b6a";
export const COMMAND_FAILED_EVENT_ID = "018f6bd5-2f2a-7b1e-8faf-1f2e3d4c5b6a";
/** Deterministically derive additional UUIDv7-looking fixtures for long scenarios. */
export const generatedId = (offset: number) =>
  `018f6bd5-2f2a-7b1e-${(0x8fb2 + offset).toString(16)}-1f2e3d4c5b6a`;
/** ID sequence for tests that drive a complete second command/run. */
export const SECOND_COMPLETED_RUN_IDS = Array.from({ length: 16 }, (_, index) =>
  generatedId(index),
);
/** ID sequence for tests where one completed run is followed by a queued command. */
export const FIRST_COMPLETED_THEN_SECOND_ADMITTED_IDS = [
  COMMAND_ADMITTED_EVENT_ID,
  USER_MESSAGE_ID,
  USER_MESSAGE_EVENT_ID,
  COMMAND_STARTED_EVENT_ID,
  RUN_ID,
  RUN_STARTED_EVENT_ID,
  TURN_ID,
  TURN_STARTED_EVENT_ID,
  INFERENCE_ID,
  INFERENCE_STARTED_EVENT_ID,
  TEXT_EVENT_ID,
  INFERENCE_COMPLETED_EVENT_ID,
  ASSISTANT_MESSAGE_ID,
  ASSISTANT_MESSAGE_EVENT_ID,
  TURN_COMPLETED_EVENT_ID,
  RUN_COMPLETED_EVENT_ID,
  COMMAND_COMPLETED_EVENT_ID,
  SECOND_COMMAND_ADMITTED_EVENT_ID,
  generatedId(18),
  generatedId(19),
];

export const NoopParams = Schema.Struct({});

/** Build a single-session test layer with the common noop tool schema. */
export const makeTestLayer = (
  ids: ReadonlyArray<string>,
  stream: Stream.Stream<InferenceRunnerStreamPart, unknown>,
  wrapStore?: (inner: EDASessionStoreShape) => EDASessionStoreShape,
) =>
  makeEdaTestLayer({
    sessionId: SessionId.make(SESSION_ID),
    ids,
    parts: stream,
    toolSchemas: new Map([["noop", NoopParams]]),
    wrapStore,
  });

export const modelSelection = { provider: "test", modelId: "test-model" };

export const command = new SubmitMessageCommand({
  commandId: CommandId.make(COMMAND_ID),
  disposition: "queue",
  content: [Prompt.textPart({ text: "hello" })],
});

export const secondCommand = new SubmitMessageCommand({
  commandId: CommandId.make(SECOND_COMMAND_ID),
  disposition: "queue",
  content: [Prompt.textPart({ text: "second" })],
});

export const thirdCommand = new SubmitMessageCommand({
  commandId: CommandId.make(THIRD_COMMAND_ID),
  disposition: "queue",
  content: [Prompt.textPart({ text: "third" })],
});

export const steerCommand = new SubmitMessageCommand({
  commandId: CommandId.make("018f6bd5-2f2a-7b1e-8f21-1f2e3d4c5b6a"),
  disposition: "steer",
  content: [Prompt.textPart({ text: "steer" })],
});

export const interruptCommand = new SubmitMessageCommand({
  commandId: CommandId.make("018f6bd5-2f2a-7b1e-8f22-1f2e3d4c5b6a"),
  disposition: "interrupt",
  content: [Prompt.textPart({ text: "interrupt" })],
});

export const stopTurnCommand = new StopTurnCommand({
  commandId: CommandId.make(STOP_TURN_COMMAND_ID),
});

/** Provider usage fixture used when finishing fake inference streams. */
export const usage = () =>
  new Response.Usage({
    inputTokens: {
      uncached: undefined,
      total: 10,
      cacheRead: undefined,
      cacheWrite: undefined,
    },
    outputTokens: { total: 5, text: undefined, reasoning: undefined },
  });

/** Replay every committed durable event from the supplied test store. */
export const collectCommitted = (store: EDASessionStoreShape) =>
  store.eventsAfter(SequenceNumber.make(0)).pipe(
    Stream.runCollect,
    Effect.map((committed) => Array.from(committed)),
  );

/** Yield until a committed-event predicate becomes true or fail the test. */
export const waitForCommitted = (
  store: EDASessionStoreShape,
  predicate: (committed: ReadonlyArray<CommittedDurableEvent>) => boolean,
) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const committed = yield* collectCommitted(store);
      if (predicate(committed)) {
        return committed;
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(new Error("Timed out waiting for committed durable events"));
  });

/** Predicate for waiting on one CommandCompleted event. */
export const hasCommandCompleted =
  (commandId: CommandId) => (committed: ReadonlyArray<CommittedDurableEvent>) =>
    committed.some((entry) => {
      const payload = entry.event.payload as { readonly commandId?: CommandId };
      return entry.event.type === "CommandCompleted" && payload.commandId === commandId;
    });

/** Predicate for waiting on any committed event with the requested type. */
export const hasEventType = (type: string) => (committed: ReadonlyArray<CommittedDurableEvent>) =>
  committed.some((entry) => entry.event.type === type);
