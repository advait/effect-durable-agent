import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import { initialReducedState } from "./reduced-state";
import { decideRunContinuation } from "./run-continuation-policy";
import { CommandId, MessageId, RunId, SequenceNumber, TurnId } from "../types/core";

const RUN_ID = RunId.make("018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a");
const TURN_ID = TurnId.make("018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a");
const COMMAND_ID = CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const FIRST_MESSAGE_ID = MessageId.make("018f6bd5-2f2a-7b1e-8f2a-1f2e3d4c5b6a");
const SECOND_MESSAGE_ID = MessageId.make("018f6bd5-2f2a-7b1e-8f2b-1f2e3d4c5b6a");

const steering = (messageId: MessageId, queuedSeq: number) => ({
  messageId,
  commandId: COMMAND_ID,
  runId: RUN_ID,
  content: [Prompt.textPart({ text: `steer ${queuedSeq}` })],
  queuedSeq: SequenceNumber.make(queuedSeq),
});

const stateWithSteering = (
  steeringByRun: ReadonlyMap<RunId, ReadonlyArray<ReturnType<typeof steering>>>,
) => ({
  ...initialReducedState,
  commandQueues: { ...initialReducedState.commandQueues, steeringByRun },
});

describe("run-continuation-policy", () => {
  it("batches legacy pending steering messages in FIFO order", () => {
    const first = steering(FIRST_MESSAGE_ID, 2);
    const second = steering(SECOND_MESSAGE_ID, 3);

    expect(
      decideRunContinuation({
        runId: RUN_ID,
        turnId: TURN_ID,
        state: stateWithSteering(new Map([[RUN_ID, [first, second]]])),
      }),
    ).toEqual({ _tag: "ContinueWithSteering", steerings: [first, second] });
  });

  it("prefers new lifecycle pending steers and batches them in effective order", () => {
    const first = {
      messageId: FIRST_MESSAGE_ID,
      commandId: COMMAND_ID,
      content: [Prompt.textPart({ text: "first" })],
      submittedSeq: SequenceNumber.make(2),
      effectiveSeq: SequenceNumber.make(2),
      disposition: "steer" as const,
    };
    const second = { ...first, messageId: SECOND_MESSAGE_ID, effectiveSeq: SequenceNumber.make(3) };
    const base = stateWithSteering(new Map([[RUN_ID, [steering(FIRST_MESSAGE_ID, 1)]]]));
    const state = {
      ...base,
      commandQueues: { ...base.commandQueues, pendingSteers: [first, second] },
    };

    expect(decideRunContinuation({ runId: RUN_ID, turnId: TURN_ID, state })).toEqual({
      _tag: "ContinueWithSteering",
      steerings: [first, second],
    });
  });

  it("completes the run when no steering is pending", () => {
    expect(
      decideRunContinuation({
        runId: RUN_ID,
        turnId: TURN_ID,
        state: stateWithSteering(new Map()),
      }),
    ).toEqual({ _tag: "CompleteRun" });
  });
});
