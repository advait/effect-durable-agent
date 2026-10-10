import * as Prompt from "effect/unstable/ai/Prompt";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { initialReducedState } from "effect-durable-agent/domain/reduced-state";
import { EDASessionSnapshot } from "effect-durable-agent/services/session-query";
import { SubmitMessageCommand, EDACommand } from "effect-durable-agent/types/commands";
import { CommandId, SequenceNumber, SessionId } from "effect-durable-agent/types/core";
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
      const snapshot: EDASessionSnapshot = {
        state: {
          ...initialReducedState,
          commands: new Map([[command.commandId, pending]]),
          commandQueues: {
            ...initialReducedState.commandQueues,
            active: undefined,
            pendingCommands: [pending],
            queuedCommands: [pending],
          },
        },
        reducerStates: new Map([["gia.persisted", { version: 1, facts: ["historical"] }]]),
        messages: [],
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
      assert.deepEqual(structuredClone(encoded), structuredClone(oldWire));
      const decoded = Schema.decodeUnknownSync(edaSessionRpc.snapshot.output)(
        structuredClone(oldWire),
      );
      assert.instanceOf(
        decoded.state.commands.get(command.commandId)?.command,
        SubmitMessageCommand,
      );
      assert.deepEqual(decoded.reducerStates, snapshot.reducerStates);
      assert.throws(() =>
        Schema.decodeUnknownSync(edaSessionRpc.snapshot.output)({
          ...oldWire,
          state: { ...oldWire.state, lastSeq: "invalid" },
        }),
      );
    }),
  );
});
