import * as Prompt from "effect/ai/Prompt";
import * as AIResponse from "effect/ai/Response";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Layer from "effect/Layer";
import { ModelResolver } from "effect-durable-agent/services/model-resolver";

import { makeLanguageModelLayer } from "effect-durable-agent/testkit/layers";
import * as Schema from "effect/Schema";
import * as Tool from "effect/ai/Tool";
import {
  CompactionExecutor,
  CompactionPolicy,
  type CompactionPolicyInput,
} from "effect-durable-agent/services/compaction";
import { makeEDAToolkit } from "effect-durable-agent/services/tool-registry";
import { CommandIdempotencyKey, SubmitMessageCommand } from "effect-durable-agent/types/commands";
import { SessionId } from "effect-durable-agent/types/core";
import { makeRootEDATraceMetadata } from "effect-durable-agent/types/tracing";
import type {
  EDASessionDurableObject,
  EDASessionDurableObjectOptions,
} from "../../packages/effect-durable-agent-cloudflare/dist/index.js";
import { encodeEdaRpcCommand } from "../../packages/effect-durable-agent-cloudflare/dist/index.js";

declare const EDA_CONFORMANCE_BLOCK_MODEL: boolean;
/** The one session this launch leaves out of the transcript rollout ring ("" for none). */
declare const EDA_CONFORMANCE_TRANSCRIPT_DISABLED: string;

/** A conformance session object that also reports whether its model runtime was built. */
type ConformanceSession = EDASessionDurableObject & {
  readonly runtimeBuilds: () => Promise<number>;
  readonly armUnrecordedAlarm: (delayMs: number) => Promise<void>;
  readonly kvIncarnation: () => Promise<unknown>;
};

/** Storage-level transcript probe object; see `transcript-probe.ts`. */
interface TranscriptProbeObject {
  readonly probe: (step: string) => Promise<{
    readonly failures: ReadonlyArray<string>;
    readonly info: Record<string, number>;
  }>;
}

interface ConformanceEnv {
  readonly EDA_SESSION: DurableObjectNamespace<ConformanceSession>;
  readonly TRANSCRIPT_PROBE: DurableObjectNamespace<
    TranscriptProbeObject & Rpc.DurableObjectBranded
  >;
}

/**
 * Model runtimes built in this isolate. The model resolver layer is part of every runtime, so a
 * storage-only entry point that leaves this unchanged did not start the runtime or its sinks.
 */
export const conformanceRuntimeBuilds = { count: 0 };

/** Host key/value the conformance hosts adopt their log incarnation from, as Gia does. */
export const conformanceIncarnationKey = "conformance.log_incarnation";

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

const EchoTool = Tool.make("echo", {
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.Unknown,
});
const conformanceToolkit = Effect.runSync(
  makeEDAToolkit([EchoTool], { echo: ({ text }) => Effect.succeed({ echoed: text }) }),
);

/**
 * The conformance model: `pong` to everything, except a request to echo, which calls the `echo`
 * tool once and then answers, so a session holds a tool body.
 */
const conformanceModel = ({ prompt }: { readonly prompt: Prompt.RawInput }) => {
  const text = JSON.stringify(prompt);
  return text.includes("please echo") && !text.includes("tool-result")
    ? Stream.make(
        AIResponse.makePart("tool-call", {
          id: "conformance-echo",
          name: "echo",
          params: { text: "hello" },
          providerExecuted: false,
        }),
        AIResponse.makePart("finish", {
          reason: "tool-calls",
          usage: usage(),
          response: undefined,
        }),
      )
    : finishedStream("pong");
};

/** A session that asks to compact compacts after its turn; every other session never does. */
const compactsOnRequest = (state: CompactionPolicyInput["state"]): boolean =>
  Array.from(state.messages.values()).some(
    (message) =>
      message._tag === "User" && JSON.stringify(message.content).includes("please compact"),
  );
const everyTurnPolicy = Effect.runSync(
  Effect.service(CompactionPolicy).pipe(
    Effect.provide(
      CompactionPolicy.ApproximateTokenThreshold({
        policyId: "conformance.every-turn",
        thresholdTokens: 1,
        retainTailTokens: 0,
        minSummarizableTokens: 1,
        charsPerToken: 1,
      }),
    ),
  ),
);

/** Deterministic model and runtime configuration shared by both real-host fixtures. */
export const conformanceHostOptions = (): EDASessionDurableObjectOptions => ({
  config: { modelSelection: { provider: "conformance", modelId: "fixed-pong" } },
  transcript: {
    adoptIncarnationKey: conformanceIncarnationKey,
    // A rollout ring: every session except the one this launch disables.
    enabled: (sessionId) => sessionId !== EDA_CONFORMANCE_TRANSCRIPT_DISABLED,
  },
  // Transcript sockets read the object's own Durable Object transcript store.
  transcriptSockets: {},
  toolkit: conformanceToolkit,
  compactionPolicyLayer: CompactionPolicy.FromFunction("conformance.on-request", (input) =>
    compactsOnRequest(input.state) ? everyTurnPolicy.plan(input) : Effect.succeed(undefined),
  ),
  compactionExecutorLayer: CompactionExecutor.LanguageModelSummary({
    executorId: "conformance.summary",
  }),
  modelResolverLayer: Layer.merge(
    ModelResolver.Fixed.pipe(
      Layer.provide(
        makeLanguageModelLayer({
          parts: EDA_CONFORMANCE_BLOCK_MODEL ? Stream.never : conformanceModel,
          generateText: "Conformance summary",
        }),
      ),
    ),
    Layer.effectDiscard(
      Effect.sync(() => {
        conformanceRuntimeBuilds.count += 1;
      }),
    ),
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

    const probe = /^\/transcript-probe\/([^/]+)\/([^/]+)$/.exec(url.pathname);
    if (probe !== null) {
      const [, objectName = "", step = ""] = probe;
      return json(await env.TRANSCRIPT_PROBE.getByName(objectName).probe(decodeURIComponent(step)));
    }

    const match =
      /^\/sessions\/([^/]+)\/(messages|snapshot|events|destroy|transcript|transcript-status|transcript-body|transcript-summary|runtime-builds|arm-unrecorded-alarm|kv-incarnation|transcript-upgrade)\/?$/.exec(
        url.pathname,
      );
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
      const stream = url.searchParams.get("stream");
      if (stream !== null) eventsUrl.searchParams.set("stream", stream);
      const eventsRequest = new Request(eventsUrl, request);
      if (url.searchParams.get("projection") === "conformance") {
        eventsRequest.headers.set("x-eda-websocket-projection", "conformance-projection-v1");
      }
      return session.fetch(eventsRequest);
    }

    if (operation === "destroy") {
      if (request.method !== "DELETE") {
        return json({ error: "Method not allowed" }, 405);
      }
      await session.destroySession(scoped);
      return new Response(null, { status: 204 });
    }

    if (operation === "transcript-status") {
      return json(await session.transcriptStatus(scoped));
    }
    if (operation === "transcript") {
      return json(
        await session.readTranscriptWindow({ ...scoped, selector: { _tag: "Latest", count: 100 } }),
      );
    }
    if (operation === "transcript-body") {
      return json(
        await session.readTranscriptBody({
          ...scoped,
          source: {
            seq: Number(url.searchParams.get("seq") ?? "0"),
            path: JSON.parse(url.searchParams.get("path") ?? "[]"),
          },
          incarnation: url.searchParams.get("incarnation") ?? "",
        }),
      );
    }
    if (operation === "transcript-summary") {
      return json(
        await session.readTranscriptSummary({
          ...scoped,
          summaryId: url.searchParams.get("summaryId") ?? "",
          incarnation: url.searchParams.get("incarnation") ?? "",
        }),
      );
    }
    if (operation === "arm-unrecorded-alarm") {
      await session.armUnrecordedAlarm(Number(url.searchParams.get("delayMs") ?? "0"));
      return json({ ok: true });
    }
    if (operation === "transcript-upgrade") {
      // Ask the object to upgrade a transcript socket for `as` (default: its own session).
      const upgradeUrl = new URL("https://eda.invalid/events");
      upgradeUrl.searchParams.set("sessionId", url.searchParams.get("as") ?? sessionId);
      upgradeUrl.searchParams.set("stream", "transcript");
      const response = await session.fetch(
        new Request(upgradeUrl, { headers: { Upgrade: "websocket" } }),
      );
      response.webSocket?.accept();
      response.webSocket?.close(1000, "conformance probe");
      return json({ status: response.status });
    }
    if (operation === "kv-incarnation") {
      return json({ incarnation: await session.kvIncarnation() });
    }
    if (operation === "runtime-builds") {
      return json({ count: await session.runtimeBuilds() });
    }

    if (operation === "snapshot") {
      if (request.method !== "GET") {
        return json({ error: "Method not allowed" }, 405);
      }
      return json(snapshotSummary(await session.snapshot(scoped)));
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
    return json({ snapshot: snapshotSummary(snapshot), terminal });
  },
} satisfies ExportedHandler<ConformanceEnv>;
