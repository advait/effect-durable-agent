import * as Prompt from "effect/unstable/ai/Prompt";
import { createClient } from "rivetkit/client";

import { CommandIdempotencyKey, SubmitMessageCommand } from "effect-durable-agent/types/commands";
import { SessionId } from "effect-durable-agent/types/core";
import { decodeEDARivetCommittedEvent, encodeEDARivetCommand } from "effect-durable-agent-rivet";
import type { registry } from "./server";

const endpoint = process.env.RIVET_PUBLIC_ENDPOINT ?? "http://127.0.0.1:6420";
const sessionId = SessionId.make(
  process.env.EDA_SESSION_ID ?? "018f6bd5-2f2a-7b1e-8f1d-1f2e3d4c5b6a",
);
const session = createClient<typeof registry>(endpoint).edaSession.getOrCreate([sessionId]);

const terminal = decodeEDARivetCommittedEvent(
  await session.submitAndBlock({
    command: encodeEDARivetCommand(
      new SubmitMessageCommand({
        idempotencyKey: CommandIdempotencyKey.make("rivet-example:first-message"),
        disposition: "queue",
        content: [Prompt.textPart({ text: "Say hello from a durable Rivet Actor." })],
      }),
    ),
  }),
);

process.stdout.write(
  `${JSON.stringify({ terminal, messages: await session.messages({}) }, null, 2)}\n`,
);
