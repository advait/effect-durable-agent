import * as Prompt from "effect/unstable/ai/Prompt";

import { CommandIdempotencyKey, SubmitMessageCommand } from "effect-durable-agent/types/commands";
import { SessionId } from "effect-durable-agent/types/core";
import { makeRootEDATraceMetadata } from "effect-durable-agent/types/tracing";
import {
  EDASessionCell,
  edaRuntimeConfig,
  encodeEdaRpcCommand,
  getEDASessionCellByName,
  makeEDACelldOpenAiModelLayer,
} from "effect-durable-agent-celld";

/** Minimal celld-hosted EDA session with no application tools. */
export class CelldNoToolsSession extends EDASessionCell<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    const modelId = env.EDA_OPENAI_MODEL;
    super(ctx, env, {
      config: edaRuntimeConfig({
        modelId,
        provider: "openai",
        systemPrompt: "Answer briefly and clearly.",
      }),
      modelLayer: makeEDACelldOpenAiModelLayer({
        apiKey: env.OPENAI_API_KEY,
        modelId,
      }),
    });
  }
}

const json = (body: unknown, status = 200): Response =>
  Response.json(body, { headers: { "Cache-Control": "no-store" }, status });

/** HTTP facade demonstrating durable command admission through a named celld cell. */
export default {
  async fetch(request, env): Promise<Response> {
    const url = new URL(request.url);
    const match = /^\/sessions\/([^/]+)\/messages\/?$/.exec(url.pathname);
    const rawSessionId = match?.[1];
    if (rawSessionId === undefined) {
      return json({ error: "Use /sessions/:sessionId/messages" }, 404);
    }

    const sessionId = SessionId.make(decodeURIComponent(rawSessionId));
    const session = getEDASessionCellByName(env.EDA_SESSION, sessionId);
    const scoped = { sessionId, trace: makeRootEDATraceMetadata() };

    if (request.method === "GET") {
      return json(await session.messages(scoped));
    }
    if (request.method !== "POST") {
      return json({ error: "Method not allowed" }, 405);
    }

    const body: unknown = await request.json().catch(() => undefined);
    const text =
      typeof body === "object" && body !== null && "text" in body && typeof body.text === "string"
        ? body.text.trim()
        : "";
    const providedKey =
      typeof body === "object" &&
      body !== null &&
      "idempotencyKey" in body &&
      typeof body.idempotencyKey === "string"
        ? body.idempotencyKey.trim()
        : "";
    if (text.length === 0) {
      return json({ error: 'Expected JSON body: { "text": "..." }' }, 400);
    }

    const idempotencyKey =
      providedKey.length > 0 ? providedKey : `http:message:${crypto.randomUUID()}`;
    const admitted = await session.submit({
      ...scoped,
      command: encodeEdaRpcCommand(
        new SubmitMessageCommand({
          idempotencyKey: CommandIdempotencyKey.make(idempotencyKey),
          disposition: "queue",
          content: [Prompt.textPart({ text })],
        }),
      ),
    });

    return json({ admitted, messages: await session.messages(scoped) }, 202);
  },
} satisfies ExportedHandler<Env>;
