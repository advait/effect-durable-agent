import * as Prompt from "effect/unstable/ai/Prompt";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { initialReducedState } from "effect-durable-agent/domain/reduced-state";
import { EDASessionSnapshot } from "effect-durable-agent/services/session-query";
import { SubmitMessageCommand, EDACommand } from "effect-durable-agent/types/commands";
import {
  CommandId,
  InferenceId,
  ToolCallId,
  TurnId,
  MessageId,
  RunId,
  SequenceNumber,
  SessionId,
} from "effect-durable-agent/types/core";
import { makeRootEDATraceMetadata } from "effect-durable-agent/types/tracing";
import { edaSessionRpc } from "./rpc-spec";

const command = new SubmitMessageCommand({
  commandId: CommandId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a"),
  content: [Prompt.textPart({ text: "historical prompt" })],
  disposition: "queue",
});

describe("deployed EDA RPC formats", () => {
  it.effect("encodes command classes into the existing admission payload", () =>
    Effect.sync(() => {
      const input = {
        command,
        sessionId: SessionId.make("018f6bd5-2f2a-7b1e-8f0a-1f2e3d4c5b6a"),
        trace: makeRootEDATraceMetadata(),
      };
      const encoded = Schema.encodeSync(edaSessionRpc.submit.input)(input);
      assert.deepEqual(encoded, { ...input, command: Schema.encodeSync(EDACommand)(command) });
      const decoded = Schema.decodeUnknownSync(edaSessionRpc.submit.input)(
        structuredClone(encoded),
      );
      assert.instanceOf(decoded.command, SubmitMessageCommand);
    }),
  );

  it.effect("retains snapshot Maps and the exact previous command carrier encoding", () =>
    Effect.sync(() => {
      const pending = {
        commandId: command.commandId,
        command,
        admittedSeq: SequenceNumber.make(1),
      };
      const messageId = MessageId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6b");
      const runId = RunId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6c");
      const content = [
        Prompt.textPart({ text: "first", options: { provider: { marker: true } } }),
        Prompt.textPart({ text: "second" }),
      ];
      const files = [
        ...content,
        Prompt.filePart({
          mediaType: "application/octet-stream",
          data: new Uint8Array([1, 2, 255]),
        }),
        Prompt.filePart({
          mediaType: "text/plain",
          data: "data:text/plain;base64,aGlzdG9yaWNhbA==",
        }),
      ];
      const user = {
        _tag: "User",
        messageId,
        commandId: command.commandId,
        content,
        seq: SequenceNumber.make(1),
      } satisfies (typeof EDASessionSnapshot.Type.messages)[number];
      const steering = {
        _tag: "Steering",
        messageId,
        commandId: command.commandId,
        runId,
        content: files,
        seq: SequenceNumber.make(2),
        consumedSeq: SequenceNumber.make(3),
      } satisfies (typeof EDASessionSnapshot.Type.messages)[number];
      const assistantId = MessageId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6d");
      const toolCallId = ToolCallId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6e");
      const toolResult = Prompt.toolResultPart({
        id: toolCallId,
        name: "fixture-tool",
        isFailure: false,
        result: { historical: [1, 2] },
        options: { provider: { marker: true } },
      });
      const assistant = {
        _tag: "Assistant",
        messageId: assistantId,
        inferenceId: InferenceId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6f"),
        turnId: TurnId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b70"),
        runId,
        seq: SequenceNumber.make(4),
        content: { text: "firstsecond", reasoning: "historical reasoning" },
        promptParts: [...files, Prompt.reasoningPart({ text: "historical reasoning" }), toolResult],
      } satisfies (typeof EDASessionSnapshot.Type.messages)[number];
      const queued = {
        messageId,
        commandId: command.commandId,
        content,
        submittedSeq: SequenceNumber.make(1),
        effectiveSeq: SequenceNumber.make(1),
        disposition: "queue",
      } satisfies (typeof EDASessionSnapshot.Type.state.commandQueues.pendingQueue)[number];
      const snapshot: EDASessionSnapshot = {
        state: {
          ...initialReducedState,
          commands: new Map([[command.commandId, pending]]),
          messages: new Map([
            [messageId, steering],
            [assistantId, assistant],
          ]),
          toolCalls: new Map([
            [
              toolCallId,
              {
                toolCallId,
                terminal: {
                  _tag: "Completed",
                  result: toolResult.result,
                  seq: SequenceNumber.make(4),
                  promptPart: toolResult,
                },
              },
            ],
          ]),
          commandQueues: {
            ...initialReducedState.commandQueues,
            active: undefined,
            pendingCommands: [pending],
            queuedCommands: [pending],
            pendingQueue: [queued],
            pendingSteers: [{ ...queued, content: files, disposition: "steer" }],
            pausedQueue: [queued],
            steeringByRun: new Map([
              [
                runId,
                [
                  {
                    messageId,
                    commandId: command.commandId,
                    runId,
                    content: files,
                    queuedSeq: SequenceNumber.make(2),
                  },
                ],
              ],
            ]),
          },
        },
        reducerStates: new Map([["gia.persisted", { version: 1, facts: ["historical"] }]]),
        messages: [user, steering, assistant],
      };
      const carrier = { ...pending, command: Schema.encodeSync(EDACommand)(command) };
      const oldWire = {
        ...snapshot,
        reducerStates: new Map(snapshot.reducerStates),
        state: {
          ...snapshot.state,
          commands: new Map([[command.commandId, carrier]]),
          commandQueues: {
            ...snapshot.state.commandQueues,
            pendingCommands: [carrier],
            queuedCommands: [carrier],
          },
        },
      };
      const encoded = Schema.encodeSync(edaSessionRpc.snapshot.output)(snapshot);
      assert.deepEqual(encoded, oldWire);
      assert.deepEqual(structuredClone(encoded), structuredClone(oldWire));
      const decoded = Schema.decodeUnknownSync(edaSessionRpc.snapshot.output)(
        structuredClone(oldWire),
      );
      assert.instanceOf(
        decoded.state.commands.get(command.commandId)?.command,
        SubmitMessageCommand,
      );
      assert.deepEqual(decoded.reducerStates, snapshot.reducerStates);
      assert.deepEqual(decoded.messages, snapshot.messages);
      assert.deepEqual(decoded.state.messages, snapshot.state.messages);
      assert.deepEqual(
        decoded.state.commandQueues.pendingQueue,
        snapshot.state.commandQueues.pendingQueue,
      );
      assert.deepEqual(
        structuredClone(Schema.encodeSync(edaSessionRpc.messages.output)(snapshot.messages)),
        structuredClone(snapshot.messages),
      );
      assert.throws(() =>
        Schema.decodeUnknownSync(edaSessionRpc.snapshot.output)({
          ...oldWire,
          state: { ...oldWire.state, lastSeq: "invalid" },
        }),
      );
    }),
  );
});
