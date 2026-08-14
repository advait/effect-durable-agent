import * as Prompt from "effect/unstable/ai/Prompt";

import { CommandIdempotencyKey, SubmitMessageCommand } from "effect-durable-agent/types/commands";
import { SessionId } from "effect-durable-agent/types/core";
import { makeRootEDATraceMetadata } from "effect-durable-agent/types/tracing";
import {
  EDASessionDurableObject,
  getEDASessionDurableObjectByName,
} from "../../src/durable-object";
import { encodeEdaRpcCommand } from "../../src/durable-object-runtime";
import { json, parseJsonObject, pathParam, requiredString } from "../_shared/http";
import { makeExampleOpenAiOptions, type ExampleOpenAiEnv } from "../_shared/openai";

/** Bindings used by the no-tools EDA example Worker. */
export interface NoToolsEDAEnv extends ExampleOpenAiEnv {
  readonly NoToolsEDASession: DurableObjectNamespace<NoToolsEDASession>;
}

/** Concrete no-tools session object; register this subclass, never the EDA base class. */
export class NoToolsEDASession extends EDASessionDurableObject<NoToolsEDAEnv> {
  constructor(ctx: DurableObjectState, env: NoToolsEDAEnv) {
    super(ctx, env, makeExampleOpenAiOptions(env));
  }
}

/**
 * Tiny HTTP facade for the baseline example.
 *
 * The important line is `session.submit(...)`: the user message becomes a
 * durable `CommandAdmitted` fact before model execution begins.
 */
export default {
  async fetch(request, env): Promise<Response> {
    const url = new URL(request.url);
    const rawSessionId = pathParam(url.pathname, /^\/sessions\/([^/]+)\/messages\/?$/);
    if (rawSessionId === null) {
      return json({ error: "Use /sessions/:sessionId/messages" }, 404);
    }

    const sessionId = SessionId.make(rawSessionId);
    const session = getEDASessionDurableObjectByName(env.NoToolsEDASession, sessionId);

    if (request.method === "GET") {
      return json(await session.messages({ sessionId, trace: makeRootEDATraceMetadata() }));
    }

    if (request.method !== "POST") {
      return json({ error: "Method not allowed" }, 405);
    }

    const body = await parseJsonObject(request);
    const text = requiredString(body, "text");
    if (text === undefined) {
      return json({ error: 'Expected JSON body: { "text": "..." }' }, 400);
    }

    const idempotencyKey =
      requiredString(body, "idempotencyKey") ?? `http:message:${crypto.randomUUID()}`;
    const admitted = await session.submit({
      command: encodeEdaRpcCommand(
        new SubmitMessageCommand({
          idempotencyKey: CommandIdempotencyKey.make(idempotencyKey),
          disposition: "queue",
          content: [Prompt.textPart({ text })],
        }),
      ),
      sessionId,
      trace: makeRootEDATraceMetadata(),
    });

    return json(
      {
        admitted,
        messages: await session.messages({ sessionId, trace: makeRootEDATraceMetadata() }),
      },
      202,
    );
  },
} satisfies ExportedHandler<NoToolsEDAEnv>;

// Session ids are externally supplied in this tiny example. EDA accepts UUIDv4
// for migrated callers and mints new lifecycle ids as UUIDv7.
