import assert from "node:assert/strict";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { setTimeout as delay } from "node:timers/promises";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import WebSocket from "ws";
import { IdGenerator } from "effect-durable-agent/services/id-generator";
import { RunGrantResult } from "effect-durable-agent/domain/run-scheduling";
import { EDAWebSocketServerFrame } from "effect-durable-agent/websocket";
import { AcceptedAuthorization, AuthorizationSnapshot } from "./protocol.ts";

const [mode, base, tokenFile, stateFile] = process.argv.slice(2);
assert(
  mode && base && tokenFile && stateFile,
  "Usage: node testing/run-authorization/verify.mjs MODE BASE_URL TOKEN_FILE STATE_FILE",
);
const headers = {
  authorization: `Bearer ${readFileSync(tokenFile, "utf8").trim()}`,
  "content-type": "application/json",
};
const state = existsSync(stateFile) ? JSON.parse(readFileSync(stateFile, "utf8")) : {};
const call = async (path, body) => {
  const response = await fetch(`${base}${path}`, {
    headers,
    signal: AbortSignal.timeout(20_000),
    ...(body === undefined ? {} : { method: "POST", body: JSON.stringify(body) }),
  });
  assert.equal(
    response.status,
    200,
    `${path}: HTTP ${response.status}: ${await response.clone().text()}`,
  );
  return response.json();
};
const snapshot = async (sessionId) =>
  Schema.decodeUnknownSync(AuthorizationSnapshot)(await call(`/sessions/${sessionId}/snapshot`));
const inspect = async (requestId) =>
  Schema.decodeUnknownSync(Schema.NullOr(AcceptedAuthorization))(
    await call(`/authorizers/${requestId}/inspect`),
  );
const grant = async (requestId) =>
  Schema.decodeUnknownSync(RunGrantResult)(await call(`/authorizers/${requestId}/grant`, {}));
const submit = (sessionId, text, disposition = "queue") =>
  call(`/sessions/${sessionId}/submit`, {
    _tag: "SubmitMessage",
    content: text,
    disposition,
    idempotencyKey: text,
  });
const newSession = () =>
  Effect.runPromise(
    Effect.gen(function* () {
      return yield* (yield* IdGenerator).makeSessionId();
    }).pipe(Effect.provide(IdGenerator.Live)),
  );
const until = async (read, predicate) => {
  for (let attempt = 0; attempt < 150; attempt += 1) {
    const result = await read();
    if (predicate(result)) return result;
    await delay(400);
  }
  throw new Error("Verification condition did not become true within 60 seconds");
};
const waiting = (sessionId) =>
  until(
    () => snapshot(sessionId),
    (value) => value.request?.deliveredSeq !== undefined,
  );
const replay = async (sessionId, throughSeq, afterSeq = 0) => {
  const ws = new WebSocket(
    `${base.replace(/^http/, "ws")}/sessions/${sessionId}/events?afterSeq=${afterSeq}`,
    { headers },
  );
  const events = [];
  await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      ws.terminate();
      reject(new Error("WebSocket replay timed out"));
    }, 20_000);
    ws.on("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    ws.on("message", (data) => {
      try {
        const frame = Schema.decodeUnknownSync(EDAWebSocketServerFrame)(
          JSON.parse(data.toString()),
        );
        if (frame._tag !== "events") return;
        events.push(...frame.events);
        ws.send(
          JSON.stringify({
            _tag: "ack",
            frameId: frame.frameId,
            durableThroughSeq: frame.durableThroughSeq,
          }),
        );
        if (frame.durableThroughSeq >= throughSeq) {
          clearTimeout(timeout);
          ws.close(1000, "verification complete");
          resolve();
        }
      } catch (error) {
        clearTimeout(timeout);
        ws.terminate();
        reject(error);
      }
    });
  });
  ws.terminate();
  assert.deepEqual(
    events.map((item) => item.position.seq),
    Array.from({ length: throughSeq - afterSeq }, (_, i) => afterSeq + i + 1),
  );
  return events;
};

if (mode === "offline") {
  assert.equal((await fetch(`${base}/health`)).status, 401);
  state.sessionId = await newSession();
  await submit(state.sessionId, "deferred:first");
  const first = await until(
    () => snapshot(state.sessionId),
    (value) => value.request !== undefined,
  );
  assert.equal(first.request.deliveredSeq, undefined);
  assert.equal(first.runCount, 0);
  assert.equal(await inspect(first.request.requestId), null);
  state.first = first;
} else if (mode === "handoff") {
  // Inspect only the other object until the session alarm delivers after redeployment.
  const accepted = await until(
    () => inspect(state.first.request.requestId),
    (value) => value !== null,
  );
  assert.equal(accepted.handoff.sessionId, state.sessionId);
  const handedOff = await waiting(state.sessionId);
  assert.equal(handedOff.request.requestId, state.first.request.requestId);
  assert.equal(handedOff.runCount, 0);
  state.handedOff = handedOff;
  state.accepted = accepted;
} else if (mode === "hydration") {
  const restored = await snapshot(state.sessionId);
  assert.deepEqual(restored, state.handedOff);
  assert.deepEqual(await inspect(restored.request.requestId), state.accepted);
  assert.equal(restored.runCount, 0);
} else if (mode === "queue") {
  await Promise.all([
    submit(state.sessionId, "deferred:second"),
    submit(state.sessionId, "deferred:third"),
  ]);
  assert.equal((await snapshot(state.sessionId)).request.requestId, state.first.request.requestId);
  for (let expected = 1; expected <= 3; expected += 1) {
    const before = await waiting(state.sessionId);
    assert.equal((await grant(before.request.requestId))._tag, "Granted");
    assert.deepEqual(await grant(before.request.requestId), { _tag: "Stale" });
    await until(
      () => snapshot(state.sessionId),
      (value) => value.completedRunCount === expected,
    );
  }
  const complete = await snapshot(state.sessionId);
  assert.equal(complete.runCount, 3);
  assert.equal(complete.request, undefined);
  const events = await replay(state.sessionId, complete.lastSeq);
  for (const type of [
    "RunSchedulingRequested",
    "RunSchedulingDelivered",
    "RunSchedulingGranted",
    "RunStarted",
    "CommandCompleted",
  ]) {
    assert.equal(events.filter((item) => item.event.type === type).length, 3, type);
  }
  const resumed = await replay(state.sessionId, complete.lastSeq, state.handedOff.lastSeq);
  assert(resumed.length > 0);
  state.complete = complete;
} else if (mode === "controls") {
  state.controls = [];
  for (const control of ["stop", "cancel", "interrupt"]) {
    const sessionId = await newSession();
    await submit(sessionId, `deferred:${control}:first`);
    const first = await waiting(sessionId);
    if (control === "stop") {
      await submit(sessionId, `deferred:${control}:queued`);
      await call(`/sessions/${sessionId}/submit`, { _tag: "StopTurn" });
    } else if (control === "cancel") {
      await call(`/sessions/${sessionId}/submit`, {
        _tag: "CancelPendingMessage",
        messageId: first.pendingMessageIds[0],
        reason: "user-cancel",
      });
    } else {
      await submit(sessionId, `deferred:${control}:replacement`, "interrupt");
    }
    const changed = await until(
      () => snapshot(sessionId),
      (value) => value.request?.requestId !== first.request.requestId,
    );
    assert.deepEqual(await grant(first.request.requestId), { _tag: "Stale" });
    assert.equal(changed.runCount, 0);
    assert(changed.cancelledCommandIds.includes(first.request.work.commandId));
    if (control === "stop") assert.equal(changed.pausedMessageCount, 2);
    if (control === "interrupt") {
      const replacement = await waiting(sessionId);
      assert.notEqual(replacement.request.requestId, first.request.requestId);
      await grant(replacement.request.requestId);
      await until(
        () => snapshot(sessionId),
        (value) => value.completedRunCount === 1,
      );
    }
    state.controls.push({ control, sessionId, firstRequestId: first.request.requestId });
  }
} else if (mode === "block") {
  state.recoverySessionId = await newSession();
  await submit(state.recoverySessionId, "deferred:recover");
  const before = await waiting(state.recoverySessionId);
  const granted = await grant(before.request.requestId);
  assert.equal(granted._tag, "Granted");
  const running = await until(
    () => snapshot(state.recoverySessionId),
    (value) => value.runCount === 1,
  );
  assert.equal(running.completedRunCount, 0);
  assert.equal(running.request, undefined);
  state.interrupted = {
    requestId: before.request.requestId,
    runId: granted.runId,
    snapshot: running,
  };
} else if (mode === "recover") {
  const restored = await waiting(state.recoverySessionId);
  assert.notEqual(restored.request.requestId, state.interrupted.requestId);
  assert.equal(restored.request.work._tag, "Recovery");
  assert.equal(restored.request.work.interruptedRunId, state.interrupted.runId);
  assert.deepEqual(await grant(state.interrupted.requestId), { _tag: "Stale" });
  const granted = await grant(restored.request.requestId);
  assert.equal(granted._tag, "Granted");
  const complete = await until(
    () => snapshot(state.recoverySessionId),
    (value) => value.completedRunCount === 1,
  );
  assert.equal(complete.runCount, 2);
  assert.equal(complete.request, undefined);
  assert.equal(complete.recovery.length, 1);
  assert.equal(complete.recovery[0].interruptedRunId, state.interrupted.runId);
  assert.equal(complete.recovery[0].replacementRunId, granted.runId);
  const events = await replay(state.recoverySessionId, complete.lastSeq);
  assert.equal(events.filter((item) => item.event.type === "RunSchedulingRequested").length, 2);
  assert.equal(events.filter((item) => item.event.type === "CommandCompleted").length, 1);
  state.recovered = complete;
} else {
  throw new Error(`Unknown verification mode: ${mode}`);
}

writeFileSync(stateFile, JSON.stringify(state, null, 2), { mode: 0o600 });
console.log(
  JSON.stringify({
    mode,
    result: "passed",
    sessionId: state.sessionId,
    recoverySessionId: state.recoverySessionId,
    stateFile,
  }),
);
