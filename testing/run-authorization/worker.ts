import { DurableObject } from "cloudflare:workers";
import { timingSafeEqual } from "node:crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  RunScheduler,
  RunSchedulingDeliveryError,
} from "effect-durable-agent/services/run-scheduler";
import { EDACommand, GrantRunCommand } from "effect-durable-agent/types/commands";
import { RunRequestId, SessionId } from "effect-durable-agent/types/core";
import { makeRootEDATraceMetadata } from "effect-durable-agent/types/tracing";
import {
  EDASessionDurableObject,
  encodeEdaRpcCommand,
  type EDASessionScopedRpcInput,
} from "../../packages/effect-durable-agent-cloudflare/dist/index.js";
import { conformanceHostOptions } from "../host-conformance/fixture";
import { AcceptedAuthorization, AuthorizationHandoff, AuthorizationSnapshot } from "./protocol";

declare const EDA_AUTHORIZER_OFFLINE: boolean;

/** Per-session production adapter with a deterministic provider and a separate test authorizer. */
export class AuthorizationSession extends EDASessionDurableObject<RunAuthorizationEnv> {
  constructor(ctx: DurableObjectState, env: RunAuthorizationEnv) {
    super(ctx, env, {
      ...conformanceHostOptions(),
      runSchedulerLayer: RunScheduler.Deferred((handoff) =>
        Effect.tryPromise({
          try: () => env.AUTHORIZER.getByName(handoff.request.requestId).accept(handoff),
          catch: (error) => new RunSchedulingDeliveryError({ message: String(error) }),
        }).pipe(Effect.asVoid),
      ),
    });
  }

  /** Project before the RPC boundary so the test wire contract contains only clone-safe values. */
  summary(input: EDASessionScopedRpcInput): Promise<AuthorizationSnapshot> {
    const read = () => super.snapshot(input);
    return Effect.runPromise(
      Effect.gen(function* () {
        const { state } = yield* Effect.promise(read);
        return {
          lastSeq: state.lastSeq,
          ...(state.runSchedulingRequest === undefined
            ? {}
            : { request: state.runSchedulingRequest }),
          runCount: state.runs.size,
          completedRunCount: Array.from(state.runs.values()).filter(
            (run) => run.terminal?._tag === "Completed",
          ).length,
          cancelledCommandIds: Array.from(state.commands.values())
            .filter((command) => command.terminal?._tag === "Cancelled")
            .map((command) => command.commandId),
          pendingMessageCount:
            state.commandQueues.pendingQueue.length + state.commandQueues.pendingSteers.length,
          pendingMessageIds: [
            ...state.commandQueues.pendingQueue,
            ...state.commandQueues.pendingSteers,
          ].map((message) => message.messageId),
          pausedMessageCount: state.commandQueues.pausedQueue.length,
          recovery: Array.from(state.recoveryContinuations.values()),
        };
      }),
    );
  }
}

/**
 * Manual test authorizer, one object per request. Successful accept persists the
 * handoff; the verification driver owns when to grant and retries its RPC on failure.
 * This fixture deliberately has no capacity, fairness, or lease policy.
 */
export class ManualAuthorizer extends DurableObject<RunAuthorizationEnv> {
  constructor(ctx: DurableObjectState, env: RunAuthorizationEnv) {
    super(ctx, env);
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS accepted_request (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), payload TEXT NOT NULL, deliveries INTEGER NOT NULL)",
    );
  }

  accept(input: unknown): AcceptedAuthorization {
    if (EDA_AUTHORIZER_OFFLINE) throw new Error("Test authorizer is intentionally offline");
    const handoff = Schema.decodeUnknownSync(AuthorizationHandoff)(input);
    const payload = JSON.stringify(Schema.encodeSync(AuthorizationHandoff)(handoff));
    const previous = this.inspect();
    if (
      previous !== null &&
      JSON.stringify(Schema.encodeSync(AuthorizationHandoff)(previous.handoff)) !== payload
    ) {
      throw new Error("A request ID cannot identify different authorization work");
    }
    this.ctx.storage.sql.exec(
      "INSERT INTO accepted_request VALUES (1, ?, 1) ON CONFLICT(singleton) DO UPDATE SET deliveries = deliveries + 1",
      payload,
    );
    return { handoff, deliveries: (previous?.deliveries ?? 0) + 1 };
  }

  inspect(): AcceptedAuthorization | null {
    const row = this.ctx.storage.sql
      .exec<{ payload: string; deliveries: number }>(
        "SELECT payload, deliveries FROM accepted_request WHERE singleton = 1",
      )
      .toArray()[0];
    if (row === undefined) return null;
    return {
      handoff: Schema.decodeUnknownSync(AuthorizationHandoff)(JSON.parse(row.payload)),
      deliveries: row.deliveries,
    };
  }

  grant() {
    const accepted = this.inspect();
    if (accepted === null) throw new Error("No durable authorization request was accepted");
    const { sessionId, request } = accepted.handoff;
    return this.env.SESSION.getByName(sessionId).grantRun({
      sessionId,
      trace: makeRootEDATraceMetadata(),
      command: Schema.encodeSync(GrantRunCommand)(
        new GrantRunCommand({ requestId: request.requestId }),
      ),
    });
  }
}

/** Protect every fixture route, including manual scheduler controls, with a required secret. */
const authenticated = (request: Request, env: RunAuthorizationEnv) =>
  Effect.gen(function* () {
    if (!env.STAGING_TOKEN) return false;
    const encoder = new TextEncoder();
    const supplied = yield* Effect.promise(() =>
      crypto.subtle.digest("SHA-256", encoder.encode(request.headers.get("authorization") ?? "")),
    );
    const expected = yield* Effect.promise(() =>
      crypto.subtle.digest("SHA-256", encoder.encode(`Bearer ${env.STAGING_TOKEN}`)),
    );
    return timingSafeEqual(new Uint8Array(supplied), new Uint8Array(expected));
  });

/** Authenticated, test-only control surface for exercising the real RPC and persistence contract. */
export default {
  fetch(request, env): Promise<Response> {
    return Effect.runPromise(
      Effect.gen(function* () {
        if (!(yield* authenticated(request, env)))
          return new Response("Unauthorized", { status: 401 });
        const url = new URL(request.url);
        if (url.pathname === "/health") return Response.json({ ok: true });
        const authorizerRoute = /^\/authorizers\/([^/]+)\/(inspect|grant)$/.exec(url.pathname);
        if (authorizerRoute !== null) {
          const requestId = Schema.decodeUnknownSync(RunRequestId)(authorizerRoute[1]);
          const authorizer = env.AUTHORIZER.getByName(requestId);
          if (authorizerRoute[2] === "inspect" && request.method === "GET")
            return Response.json(yield* Effect.promise(() => authorizer.inspect()));
          if (authorizerRoute[2] === "grant" && request.method === "POST")
            return Response.json(yield* Effect.promise(() => authorizer.grant()));
          return new Response("Method not allowed", { status: 405 });
        }
        const route = /^\/sessions\/([^/]+)\/(submit|snapshot|events)$/.exec(url.pathname);
        if (route === null) return new Response("Not found", { status: 404 });
        const sessionId = Schema.decodeUnknownSync(SessionId)(route[1]);
        const session = env.SESSION.getByName(sessionId);
        const scoped = { sessionId, trace: makeRootEDATraceMetadata() };
        if (route[2] === "submit" && request.method === "POST") {
          const command = Schema.decodeUnknownSync(EDACommand)(
            yield* Effect.promise(() => request.json()),
          );
          return Response.json(
            yield* Effect.promise(() =>
              session.submit({ ...scoped, command: encodeEdaRpcCommand(command) }),
            ),
          );
        }
        if (route[2] === "snapshot" && request.method === "GET") {
          const snapshot = Schema.decodeUnknownSync(AuthorizationSnapshot)(
            yield* Effect.promise(() => session.summary(scoped)),
          );
          return Response.json(Schema.encodeSync(AuthorizationSnapshot)(snapshot));
        }
        if (route[2] === "events" && request.method === "GET") {
          const eventsUrl = new URL("https://eda.invalid/events");
          eventsUrl.searchParams.set("sessionId", sessionId);
          eventsUrl.searchParams.set("afterSeq", url.searchParams.get("afterSeq") ?? "0");
          return yield* Effect.promise(() => session.fetch(new Request(eventsUrl, request)));
        }
        return new Response("Method not allowed", { status: 405 });
      }),
    );
  },
} satisfies ExportedHandler<RunAuthorizationEnv>;
