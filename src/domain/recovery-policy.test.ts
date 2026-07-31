import * as Prompt from "effect/unstable/ai/Prompt";
import { describe, expect, it } from "vite-plus/test";

import type { PendingUserMessage } from "./command-queues";
import { initialReducedState } from "./reduced-state";
import { isSessionRecoveryPlanEmpty, planSessionRecovery } from "./recovery-policy";
import { ResumePendingMessagesCommand, SubmitMessageCommand } from "../types/commands";
import { CommandId, CompactionId, MessageId, RunId, SequenceNumber, TurnId } from "../types/core";
import { makeEDARunTrace } from "../types/tracing";

const commandId = CommandId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const resumeCommandId = CommandId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const messageId = MessageId.make("018f6bd5-2f2a-7b1e-8f2a-1f2e3d4c5b6a");
const queuedMessageId = MessageId.make("018f6bd5-2f2a-7b1e-8f2b-1f2e3d4c5b6a");
const runId = RunId.make("018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a");
const turnId = TurnId.make("018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a");
const compactionId = CompactionId.make("018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a");
const seq = (value: number) => SequenceNumber.make(value);

const steer: PendingUserMessage = {
  messageId,
  commandId,
  content: [Prompt.textPart({ text: "steer" })],
  submittedSeq: seq(1),
  effectiveSeq: seq(1),
  disposition: "steer",
};

const queued: PendingUserMessage = {
  ...steer,
  messageId: queuedMessageId,
  content: [Prompt.textPart({ text: "queue" })],
  disposition: "queue",
};

describe("planSessionRecovery", () => {
  it("is empty for an idle session", () => {
    expect(isSessionRecoveryPlanEmpty(planSessionRecovery(initialReducedState))).toBe(true);
  });

  it("assigns one recovery owner to an ownerless steer", () => {
    const plan = planSessionRecovery({
      ...initialReducedState,
      commandQueues: { ...initialReducedState.commandQueues, pendingSteers: [steer] },
    });

    expect(plan.messageIdsToResume).toEqual([messageId]);
  });

  it("does not duplicate an existing resume owner", () => {
    const resume = new ResumePendingMessagesCommand({
      commandId: resumeCommandId,
      messageIds: [messageId],
    });
    const plan = planSessionRecovery({
      ...initialReducedState,
      commandQueues: {
        ...initialReducedState.commandQueues,
        pendingSteers: [steer],
        pendingCommands: [{ commandId: resumeCommandId, command: resume, admittedSeq: seq(2) }],
      },
    });

    expect(plan.messageIdsToResume).toEqual([]);
  });

  it("continues only still-pending inputs from an active resume command", () => {
    const resume = new ResumePendingMessagesCommand({
      commandId: resumeCommandId,
      messageIds: [messageId, queuedMessageId],
    });
    const plan = planSessionRecovery({
      ...initialReducedState,
      commands: new Map([
        [resumeCommandId, { commandId: resumeCommandId, command: resume, startedSeq: seq(2) }],
      ]),
      runs: new Map([
        [
          runId,
          {
            runId,
            commandIds: [resumeCommandId],
            startedSeq: seq(3),
            trace: makeEDARunTrace(),
          },
        ],
      ]),
      commandQueues: {
        ...initialReducedState.commandQueues,
        active: { commandId: resumeCommandId, runId },
        pendingSteers: [steer],
      },
    });

    expect(plan.continuation?.inputMessageIds).toEqual([messageId]);
  });

  it("continues active submit work and owns its unconsumed source message", () => {
    const command = new SubmitMessageCommand({
      commandId,
      disposition: "queue",
      content: [Prompt.textPart({ text: "queue" })],
    });
    const plan = planSessionRecovery({
      ...initialReducedState,
      commands: new Map([[commandId, { commandId, command, startedSeq: seq(2) }]]),
      runs: new Map([
        [
          runId,
          {
            runId,
            commandIds: [commandId],
            startedSeq: seq(3),
            trace: makeEDARunTrace(),
          },
        ],
      ]),
      commandQueues: {
        ...initialReducedState.commandQueues,
        active: { commandId, runId },
        pendingQueue: [queued],
      },
    });

    expect(plan.continuation).toMatchObject({
      command: { commandId },
      run: { runId },
      inputMessageIds: [queuedMessageId],
    });
    expect(plan.messageIdsToResume).toEqual([]);
  });

  it("does not continue work targeted by a pending stop", () => {
    const command = new SubmitMessageCommand({
      commandId,
      disposition: "queue",
      content: [Prompt.textPart({ text: "queue" })],
    });
    const plan = planSessionRecovery({
      ...initialReducedState,
      commands: new Map([[commandId, { commandId, command, startedSeq: seq(2) }]]),
      runs: new Map([
        [
          runId,
          {
            runId,
            commandIds: [commandId],
            startedSeq: seq(3),
            trace: makeEDARunTrace(),
          },
        ],
      ]),
      stopRequests: new Map([
        [
          resumeCommandId,
          {
            commandId: resumeCommandId,
            requestedSeq: seq(4),
            requestedRunId: runId,
            requestedTurnId: turnId,
          },
        ],
      ]),
      commandQueues: {
        ...initialReducedState.commandQueues,
        active: { commandId, runId },
        pendingQueue: [queued],
      },
    });

    expect(plan.continuation).toBeUndefined();
    expect(plan.messageIdsToPause).toEqual([queuedMessageId]);
  });

  it("plans pending queue and steer messages into the interruption pause", () => {
    const plan = planSessionRecovery({
      ...initialReducedState,
      stopRequests: new Map([
        [
          commandId,
          { commandId, requestedSeq: seq(3), requestedRunId: runId, requestedTurnId: turnId },
        ],
      ]),
      commandQueues: {
        ...initialReducedState.commandQueues,
        pendingQueue: [queued],
        pendingSteers: [steer],
      },
    });

    expect(plan.messageIdsToPause).toEqual([queuedMessageId, messageId]);
    expect(plan.messageIdsToResume).toEqual([]);
  });

  it("includes unfinished compaction in the same recovery plan", () => {
    const plan = planSessionRecovery({
      ...initialReducedState,
      compactions: new Map([[compactionId, { compactionId, requestedSeq: seq(1) }]]),
    });

    expect(plan.openCompactions).toEqual([{ compactionId, requestedSeq: seq(1) }]);
  });

  it("clears an untargeted stop from recovery when its command terminalizes", () => {
    const active = planSessionRecovery({
      ...initialReducedState,
      commands: new Map([[commandId, { commandId, startedSeq: seq(1) }]]),
      stopRequests: new Map([[commandId, { commandId, requestedSeq: seq(2) }]]),
    });
    expect(active.pendingStopRequests).toHaveLength(1);

    const terminal = planSessionRecovery({
      ...initialReducedState,
      commands: new Map([
        [
          commandId,
          {
            commandId,
            startedSeq: seq(1),
            terminal: { _tag: "Cancelled", seq: seq(3), reason: "restart" },
          },
        ],
      ]),
      stopRequests: new Map([[commandId, { commandId, requestedSeq: seq(2) }]]),
    });
    expect(terminal.pendingStopRequests).toEqual([]);
  });
});
