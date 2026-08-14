import * as Prompt from "effect/unstable/ai/Prompt";
import * as AIResponse from "effect/unstable/ai/Response";
import * as Stream from "effect/Stream";

import { makeLanguageModelLayer } from "effect-durable-agent/testkit/layers";
import { CommandIdempotencyKey, SubmitMessageCommand } from "effect-durable-agent/types/commands";
import { SessionId } from "effect-durable-agent/types/core";
import { makeRootEDATraceMetadata } from "effect-durable-agent/types/tracing";
import type {
  EDASessionDurableObject,
  EDASessionDurableObjectOptions,
} from "../../packages/cloudflare/dist/index.js";
import { encodeEdaRpcCommand } from "../../packages/cloudflare/dist/index.js";

declare const EDA_CONFORMANCE_BLOCK_MODEL: boolean;

interface ConformanceEnv {
  readonly EDA_SESSION: DurableObjectNamespace<EDASessionDurableObject>;
}

const usage = () =>
  new AIResponse.Usage({
    inputTokens: { total: 1, uncached: undefined, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: undefined, reasoning: undefined },
  });

const finishedStream = (text: string) =>
  Stream.make(
    AIResponse.makePart("text-delta", { id: "conformance-text", delta: text }),
    AIResponse.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
  );

/** Deterministic model and runtime configuration shared by both real-host fixtures. */
export const conformanceHostOptions = (): EDASessionDurableObjectOptions => ({
  config: { modelSelection: { provider: "conformance", modelId: "fixed-pong" } },
  modelLayer: makeLanguageModelLayer(
    EDA_CONFORMANCE_BLOCK_MODEL ? Stream.never : finishedStream("pong"),
  ),
});

const json = (value: unknown, status = 200): Response => Response.json(value, { status });

const snapshotSummary = (snapshot: {
  readonly state: { readonly lastSeq: number };
  readonly reducerStates: ReadonlyMap<string, unknown>;
}) => ({
  lastSeq: snapshot.state.lastSeq,
  reducerNames: Array.from(snapshot.reducerStates.keys()).sort(),
});

/** HTTP/WebSocket facade used unchanged by the workerd and celld fixtures. */
export const conformanceWorker = {
  async fetch(request, env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      return json({ ok: true });
    }

    const match = /^\/sessions\/([^/]+)\/(messages|snapshot|events|destroy)\/?$/.exec(url.pathname);
    if (match === null) {
      return json({ error: "Not found" }, 404);
    }

    const [, rawSessionId, operation] = match;
    if (rawSessionId === undefined || operation === undefined) {
      return json({ error: "Invalid session route" }, 400);
    }
    const sessionId = SessionId.make(rawSessionId);
    const session = env.EDA_SESSION.getByName(sessionId);
    const scoped = { sessionId, trace: makeRootEDATraceMetadata() };

    if (operation === "events") {
      if (request.method !== "GET") {
        return json({ error: "Method not allowed" }, 405);
      }
      const eventsUrl = new URL("https://eda.invalid/events");
      eventsUrl.searchParams.set("sessionId", sessionId);
      eventsUrl.searchParams.set("afterSeq", url.searchParams.get("afterSeq") ?? "0");
      return session.fetch(new Request(eventsUrl, request));
    }

    if (operation === "destroy") {
      if (request.method !== "DELETE") {
        return json({ error: "Method not allowed" }, 405);
      }
      await session.destroySession(scoped);
      return new Response(null, { status: 204 });
    }

    if (operation === "snapshot") {
      if (request.method !== "GET") {
        return json({ error: "Method not allowed" }, 405);
      }
      return json(snapshotSummary(await session.snapshot(scoped)));
    }

    if (request.method === "GET") {
      return json(await session.messages(scoped));
    }
    if (request.method !== "POST") {
      return json({ error: "Method not allowed" }, 405);
    }

    const body: unknown = await request.json();
    if (typeof body !== "object" || body === null) {
      return json({ error: "Expected an object body" }, 400);
    }
    const text = "text" in body && typeof body.text === "string" ? body.text : undefined;
    const idempotencyKey =
      "idempotencyKey" in body && typeof body.idempotencyKey === "string"
        ? body.idempotencyKey
        : undefined;
    if (text === undefined || idempotencyKey === undefined) {
      return json({ error: "Expected text and idempotencyKey strings" }, 400);
    }

    const terminal = await session.submitAndBlock({
      ...scoped,
      command: encodeEdaRpcCommand(
        new SubmitMessageCommand({
          idempotencyKey: CommandIdempotencyKey.make(idempotencyKey),
          disposition: "queue",
          content: [Prompt.textPart({ text })],
        }),
      ),
    });
    const snapshot = await session.snapshot(scoped);
    return json({
      messages: await session.messages(scoped),
      snapshot: snapshotSummary(snapshot),
      terminal,
    });
  },
} satisfies ExportedHandler<ConformanceEnv>;
