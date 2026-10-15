import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { build } from "esbuild";
import { describe, expect, it } from "vite-plus/test";

import {
  applyEDATranscriptServerMessage,
  edaTranscriptSubscribeRequest,
  emptyEDATranscriptClientState,
  type EDATranscriptClientState,
} from "effect-durable-agent/websocket";

import { installCelld } from "../../packages/effect-durable-agent-celld/scripts/install-celld.mjs";

/** Runtime variants required to satisfy the shared EDA host contract. */
export type HostKind = "celld" | "cloudflare";

interface HostProcess {
  readonly baseUrl: string;
  restart(options?: { readonly blockModel?: boolean; readonly hard?: boolean }): Promise<void>;
  stop(): Promise<void>;
}

interface MessageResult {
  readonly messages: ReadonlyArray<{
    readonly _tag: string;
    readonly content?: { readonly text?: string };
  }>;
  readonly snapshot: { readonly lastSeq: number };
  readonly terminal: {
    readonly event: { readonly type: string };
    readonly position: { readonly seq: number };
  };
}

interface TranscriptProbeResult {
  readonly failures: ReadonlyArray<string>;
  readonly info: Record<string, number>;
}

interface TranscriptStatus {
  readonly incarnation: string;
  readonly headSeq: number;
  readonly converged: boolean;
  readonly pending: boolean;
  readonly versions: ReadonlyArray<{ readonly cursor: number }>;
}

interface TranscriptRead {
  readonly _tag: string;
  readonly window: {
    readonly readiness: { readonly _tag: string };
    readonly records: ReadonlyArray<{ readonly kind: string }>;
  };
}

interface EventsFrame {
  readonly _tag: "events";
  readonly durableThroughSeq: number;
  readonly frameId: number;
  readonly events: ReadonlyArray<{
    readonly event: { readonly type: string };
    readonly position: { readonly seq: number };
  }>;
}

const repositoryRoot = resolve(import.meta.dirname, "../..");

const availablePort = async (): Promise<number> =>
  await new Promise((resolvePort, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        server.close();
        reject(new Error("Unable to allocate a conformance-test port"));
        return;
      }
      server.close((error) => {
        if (error === undefined) {
          resolvePort(address.port);
        } else {
          reject(error);
        }
      });
    });
  });

const killHostProcess = (child: ChildProcess, signal: NodeJS.Signals): void => {
  if (child.pid === undefined) {
    return;
  }
  try {
    if (process.platform === "win32") {
      child.kill(signal);
    } else {
      process.kill(-child.pid, signal);
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") {
      throw error;
    }
  }
};

const waitForExit = async (child: ChildProcess, timeoutMs: number): Promise<void> => {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  await new Promise<void>((resolveExit) => {
    const timeout = setTimeout(() => {
      killHostProcess(child, "SIGKILL");
    }, timeoutMs);
    child.once("exit", () => {
      clearTimeout(timeout);
      resolveExit();
    });
  });
};

const startHost = async (kind: HostKind): Promise<HostProcess> => {
  const scratch = await mkdtemp(join(tmpdir(), `eda-${kind}-conformance-`));
  const dataDirectory = join(scratch, "data");
  const port = await availablePort();
  const internalPort = await availablePort();
  const baseUrl = `http://127.0.0.1:${port}`;
  let child: ChildProcess | undefined;
  let logs = "";

  let celldBinary: string | undefined;
  const celldBundle = kind === "celld" ? join(scratch, "celld-worker.mjs") : undefined;
  if (kind === "celld") {
    const installedCelldBinary = await installCelld();
    celldBinary = installedCelldBinary;
    const deployment: ChildProcess = spawn(
      installedCelldBinary,
      [
        "deploy",
        resolve(
          repositoryRoot,
          "packages/effect-durable-agent-celld/testing/host-conformance/wrangler.jsonc",
        ),
        "--dry-run",
      ],
      {
        cwd: repositoryRoot,
        env: {
          ...process.env,
          PATH: `${resolve(repositoryRoot, "node_modules/.bin")}:${process.env.PATH ?? ""}`,
        },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    let deployLogs = "";
    deployment.stdout?.on("data", (chunk: Uint8Array) => {
      deployLogs += new TextDecoder().decode(chunk);
    });
    deployment.stderr?.on("data", (chunk: Uint8Array) => {
      deployLogs += new TextDecoder().decode(chunk);
    });
    await waitForExit(deployment, 90_000);
    if (deployment.exitCode !== 0) {
      throw new Error(`celld deploy --dry-run failed (${deployment.exitCode})\n${deployLogs}`);
    }
  }

  const launch = async (options: { readonly blockModel?: boolean } = {}): Promise<void> => {
    logs = "";
    const blockModel = options.blockModel === true;
    let launchedChild: ChildProcess;
    if (kind === "cloudflare") {
      launchedChild = spawn(
        resolve(repositoryRoot, "node_modules/.bin/wrangler"),
        [
          "dev",
          resolve(
            repositoryRoot,
            "packages/effect-durable-agent-cloudflare/testing/host-conformance/worker.ts",
          ),
          "--config",
          resolve(
            repositoryRoot,
            "packages/effect-durable-agent-cloudflare/testing/host-conformance/wrangler.jsonc",
          ),
          "--ip",
          "127.0.0.1",
          "--port",
          String(port),
          "--define",
          `EDA_CONFORMANCE_BLOCK_MODEL:${String(blockModel)}`,
          "--persist-to",
          dataDirectory,
          "--log-level",
          "error",
          "--no-show-interactive-dev-session",
        ],
        {
          cwd: repositoryRoot,
          detached: process.platform !== "win32",
          env: { ...process.env, CI: "1" },
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
    } else {
      await build({
        bundle: true,
        conditions: ["workerd", "worker", "browser"],
        define: { EDA_CONFORMANCE_BLOCK_MODEL: String(blockModel) },
        entryPoints: [
          resolve(
            repositoryRoot,
            "packages/effect-durable-agent-celld/testing/host-conformance/worker.ts",
          ),
        ],
        external: ["cloudflare:workers"],
        format: "esm",
        outfile: celldBundle!,
        platform: "browser",
        sourcemap: true,
        target: "es2022",
      });
      launchedChild = spawn(celldBinary!, [], {
        cwd: repositoryRoot,
        detached: process.platform !== "win32",
        env: {
          ...process.env,
          CELLD_ADDR: `127.0.0.1:${port}`,
          CELLD_INTERNAL_ADDR: `127.0.0.1:${internalPort}`,
          CELLD_TEST_DATA_DIR: dataDirectory,
          CELLD_TEST_DO_BINDINGS:
            "EDA_SESSION=EDAConformanceSession,TRANSCRIPT_PROBE=EDATranscriptProbe",
          CELLD_TEST_DO_CLASSES: "EDAConformanceSession,EDATranscriptProbe",
          CELLD_TEST_SCRIPT_PATH: celldBundle!,
        },
        stdio: ["ignore", "pipe", "pipe"],
      });
    }
    child = launchedChild;

    const appendLog = (chunk: Uint8Array) => {
      logs = `${logs}${new TextDecoder().decode(chunk)}`.slice(-20_000);
    };
    launchedChild.stdout?.on("data", appendLog);
    launchedChild.stderr?.on("data", appendLog);

    const deadline = Date.now() + 90_000;
    while (Date.now() < deadline) {
      if (launchedChild.exitCode !== null) {
        throw new Error(`${kind} exited during startup (${launchedChild.exitCode})\n${logs}`);
      }
      try {
        const response = await fetch(`${baseUrl}/health`, { signal: AbortSignal.timeout(1_000) });
        if (response.ok) {
          return;
        }
      } catch {
        // The runtime is still binding its listener.
      }
      await new Promise((resolveWait) => setTimeout(resolveWait, 100));
    }
    throw new Error(`Timed out starting ${kind}\n${logs}`);
  };

  const stopChild = async (hard = false): Promise<void> => {
    if (child === undefined) {
      return;
    }
    killHostProcess(child, hard ? "SIGKILL" : "SIGTERM");
    await waitForExit(child, 10_000);
    child = undefined;
  };

  try {
    await launch();
  } catch (error) {
    await stopChild();
    await rm(scratch, { force: true, recursive: true });
    throw error;
  }
  return {
    baseUrl,
    restart: async (options) => {
      await stopChild(options?.hard);
      await launch(options);
    },
    stop: async () => {
      await stopChild();
      await rm(scratch, { force: true, recursive: true });
    },
  };
};

const decodeJson = async <A>(response: Response): Promise<A> => {
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}: ${await response.text()}`);
  }
  return (await response.json()) as A;
};

const submitMessage = async (
  host: HostProcess,
  sessionId: string,
  input: { readonly idempotencyKey: string; readonly text: string },
): Promise<MessageResult> =>
  await decodeJson<MessageResult>(
    await fetch(`${host.baseUrl}/sessions/${sessionId}/messages`, {
      body: JSON.stringify(input),
      headers: { "content-type": "application/json" },
      method: "POST",
      signal: AbortSignal.timeout(30_000),
    }),
  );

const getMessages = async (
  host: HostProcess,
  sessionId: string,
): Promise<MessageResult["messages"]> =>
  await decodeJson(
    await fetch(`${host.baseUrl}/sessions/${sessionId}/messages`, {
      signal: AbortSignal.timeout(30_000),
    }),
  );

class EventSocket {
  readonly #messages: string[] = [];
  readonly #waiters: Array<(message: string) => void> = [];
  readonly #socket: WebSocket;

  private constructor(socket: WebSocket) {
    this.#socket = socket;
    socket.addEventListener("message", (event) => {
      const message = typeof event.data === "string" ? event.data : String(event.data);
      const waiter = this.#waiters.shift();
      if (waiter === undefined) {
        this.#messages.push(message);
      } else {
        waiter(message);
      }
    });
  }

  static async open(
    host: HostProcess,
    sessionId: string,
    afterSeq: number,
    options: { readonly projected?: boolean } = {},
  ): Promise<EventSocket> {
    const url = new URL(`${host.baseUrl}/sessions/${sessionId}/events`);
    url.protocol = "ws:";
    url.searchParams.set("afterSeq", String(afterSeq));
    if (options.projected === true) url.searchParams.set("projection", "conformance");
    const socket = new WebSocket(url);
    const eventSocket = new EventSocket(socket);
    await new Promise<void>((resolveOpen, reject) => {
      socket.addEventListener("open", () => resolveOpen(), { once: true });
      socket.addEventListener("error", () => reject(new Error("WebSocket failed to open")), {
        once: true,
      });
    });
    const hello = JSON.parse(await eventSocket.nextMessage()) as {
      readonly _tag?: string;
      readonly type?: string;
    };
    expect(options.projected === true ? hello.type : hello._tag).toBe("hello");
    eventSocket.projected = options.projected === true;
    return eventSocket;
  }

  async eventsUntil(eventType: string): Promise<ReadonlyArray<EventsFrame["events"][number]>> {
    const collected: Array<EventsFrame["events"][number]> = [];
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      const frame = JSON.parse(await this.nextMessage()) as
        | EventsFrame
        | (Omit<EventsFrame, "_tag"> & { readonly type: "events" })
        | { readonly _tag?: "heartbeat"; readonly type?: "heartbeat" };
      const frameType = "type" in frame ? frame.type : frame._tag;
      if (frameType === "heartbeat") {
        continue;
      }
      expect(frameType).toBe("events");
      if (!("events" in frame)) continue;
      collected.push(...frame.events);
      this.#socket.send(
        JSON.stringify({
          durableThroughSeq: frame.durableThroughSeq,
          frameId: frame.frameId,
          ...(this.projected ? { type: "ack" } : { _tag: "ack" }),
        }),
      );
      if (frame.events.some((event) => event.event.type === eventType)) {
        return collected;
      }
    }
    throw new Error(`Timed out waiting for ${eventType}`);
  }

  close(): void {
    this.#socket.close(1000, "conformance step complete");
  }

  private projected = false;

  private async nextMessage(): Promise<string> {
    const existing = this.#messages.shift();
    if (existing !== undefined) {
      return existing;
    }
    return await new Promise((resolveMessage, reject) => {
      const timeout = setTimeout(
        () => reject(new Error("Timed out waiting for WebSocket frame")),
        30_000,
      );
      this.#waiters.push((message) => {
        clearTimeout(timeout);
        resolveMessage(message);
      });
    });
  }
}

/** Open a transcript-window socket, subscribe to the latest window and return the client state. */
const subscribeTranscript = async (
  host: HostProcess,
  sessionId: string,
): Promise<EDATranscriptClientState> => {
  const url = new URL(`${host.baseUrl}/sessions/${sessionId}/events`);
  url.protocol = "ws:";
  url.searchParams.set("stream", "transcript");
  const socket = new WebSocket(url);
  let client = emptyEDATranscriptClientState;
  try {
    return await new Promise<EDATranscriptClientState>((resolveClient, reject) => {
      const timeout = setTimeout(() => reject(new Error("no transcript window")), 30_000);
      socket.addEventListener("error", () => reject(new Error("transcript socket failed")));
      socket.addEventListener("open", () => {
        socket.send(JSON.stringify(edaTranscriptSubscribeRequest(client)));
      });
      socket.addEventListener("message", (event) => {
        const step = applyEDATranscriptServerMessage(client, String(event.data));
        client = step.state;
        if (step.ack !== undefined) socket.send(JSON.stringify(step.ack));
        if (step.outcome._tag === "Resubscribe") reject(new Error(step.outcome.reason));
        if (step.outcome._tag === "Applied" && client.window !== undefined) {
          clearTimeout(timeout);
          resolveClient(client);
        }
      });
    });
  } finally {
    socket.close(1000, "conformance step complete");
  }
};

/** Register the identical durable-session behavior suite for one concrete host package. */
export const defineHostConformanceSuite = (kind: HostKind): void => {
  describe(`${kind} host conformance`, () => {
    it("preserves command, event-stream, restart, idempotency, and destruction semantics", async () => {
      const host = await startHost(kind);
      const sessionId =
        kind === "cloudflare"
          ? "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a"
          : "018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a";
      try {
        const initialSocket = await EventSocket.open(host, sessionId, 0);
        const first = await submitMessage(host, sessionId, {
          idempotencyKey: "conformance:first",
          text: "ping one",
        });
        const firstEvents = await initialSocket.eventsUntil("CommandCompleted");
        initialSocket.close();

        expect(first.terminal.event.type).toBe("CommandCompleted");
        expect(first.messages.map((message) => message._tag)).toEqual(["User", "Assistant"]);
        expect(first.messages[1]?.content?.text).toBe("pong");
        expect(first.snapshot.lastSeq).toBeGreaterThan(0);
        expect(firstEvents.some((event) => event.event.type === "CommandAdmitted")).toBe(true);
        expect(firstEvents.some((event) => event.event.type === "CommandCompleted")).toBe(true);

        const duplicate = await submitMessage(host, sessionId, {
          idempotencyKey: "conformance:first",
          text: "this duplicate must not run",
        });
        expect(duplicate.terminal.position.seq).toBe(first.terminal.position.seq);
        expect(duplicate.messages).toEqual(first.messages);
        expect(duplicate.snapshot.lastSeq).toBe(first.snapshot.lastSeq);

        await host.restart({ hard: true });
        expect(await getMessages(host, sessionId)).toEqual(first.messages);
        const durableDuplicate = await submitMessage(host, sessionId, {
          idempotencyKey: "conformance:first",
          text: "this post-restart duplicate must not run",
        });
        expect(durableDuplicate.terminal.position.seq).toBe(first.terminal.position.seq);
        expect(durableDuplicate.messages).toEqual(first.messages);
        expect(durableDuplicate.snapshot.lastSeq).toBe(first.snapshot.lastSeq);

        const resumedSocket = await EventSocket.open(host, sessionId, first.snapshot.lastSeq);
        const second = await submitMessage(host, sessionId, {
          idempotencyKey: "conformance:second",
          text: "ping two",
        });
        const resumedEvents = await resumedSocket.eventsUntil("CommandCompleted");
        resumedSocket.close();

        expect(second.messages.map((message) => message._tag)).toEqual([
          "User",
          "Assistant",
          "User",
          "Assistant",
        ]);
        expect(second.snapshot.lastSeq).toBeGreaterThan(first.snapshot.lastSeq);
        expect(resumedEvents.every((event) => event.position.seq > first.snapshot.lastSeq)).toBe(
          true,
        );

        await host.restart({ blockModel: true });
        const recoverySocket = await EventSocket.open(host, sessionId, second.snapshot.lastSeq);
        const interruptedSubmission = submitMessage(host, sessionId, {
          idempotencyKey: "conformance:crash-recovery",
          text: "ping through crash",
        }).catch(() => undefined);
        const interruptedEvents = await recoverySocket.eventsUntil("TurnStarted");
        expect(interruptedEvents.some((event) => event.event.type === "CommandAdmitted")).toBe(
          true,
        );
        recoverySocket.close();

        await host.restart({ hard: true });
        const projectedRecoverySocket =
          kind === "cloudflare"
            ? await EventSocket.open(host, sessionId, second.snapshot.lastSeq, { projected: true })
            : undefined;
        await interruptedSubmission;
        const recovered = await submitMessage(host, sessionId, {
          idempotencyKey: "conformance:crash-recovery",
          text: "this recovered duplicate must not run twice",
        });
        expect(recovered.terminal.event.type).toBe("CommandCompleted");
        expect(recovered.messages.map((message) => message._tag)).toEqual([
          "User",
          "Assistant",
          "User",
          "Assistant",
          "User",
          "Assistant",
        ]);
        expect(recovered.messages[5]?.content?.text).toBe("pong");
        if (projectedRecoverySocket !== undefined) {
          const projectedRecoveryEvents =
            await projectedRecoverySocket.eventsUntil("CommandCompleted");
          expect(
            projectedRecoveryEvents.some((event) => event.event.type === "CommandCompleted"),
          ).toBe(true);
          projectedRecoverySocket.close();
        }

        const destroyResponse = await fetch(`${host.baseUrl}/sessions/${sessionId}/destroy`, {
          method: "DELETE",
        });
        expect(destroyResponse.status).toBe(204);
        expect(await getMessages(host, sessionId)).toEqual([]);

        const recreated = await submitMessage(host, sessionId, {
          idempotencyKey: "conformance:after-destroy",
          text: "ping after destroy",
        });
        expect(recreated.messages.map((message) => message._tag)).toEqual(["User", "Assistant"]);
        expect(recreated.messages[1]?.content?.text).toBe("pong");

        const secondDestroyResponse = await fetch(`${host.baseUrl}/sessions/${sessionId}/destroy`, {
          method: "DELETE",
        });
        expect(secondDestroyResponse.status).toBe(204);
        await host.restart();
        expect(await getMessages(host, sessionId)).toEqual([]);
      } finally {
        await host.stop();
      }
    }, 240_000);

    it("maintains transcripts in the real platform's transactions and serves them storage-only", async () => {
      const host = await startHost(kind);
      const get = async <A>(path: string): Promise<A> =>
        await decodeJson<A>(
          await fetch(`${host.baseUrl}${path}`, { signal: AbortSignal.timeout(60_000) }),
        );
      const probe = (object: string, step: string) =>
        get<TranscriptProbeResult>(
          `/transcript-probe/${kind}-${object}/${encodeURIComponent(step)}`,
        );
      const sessionId =
        kind === "cloudflare"
          ? "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b7a"
          : "018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b7a";
      try {
        // The interpretation oracle, through the real nested transactions.
        const { info: oracle } = await probe("oracle-count", "oracle-count");
        expect(oracle.scenarios).toBeGreaterThan(10);
        for (let index = 0; index < (oracle.scenarios ?? 0); index += 1) {
          expect((await probe(`oracle-${index}`, `oracle:${index}`)).failures).toEqual([]);
        }

        // A projection failure at every statement of the inner transaction, each in a fresh
        // object: events commit, the projection rolls back, and the version freezes for good.
        // Where the platform nests transactions (workerd) even a failing write only freezes the
        // version; where it does not (celld) a failing write fails the append atomically.
        const { info: faults } = await probe("fault-count", "fault-count");
        expect(faults.projectionStatements).toBeGreaterThan(10);
        if (kind === "cloudflare") expect(faults.innerStatements).toBeGreaterThan(10);
        else expect(faults.innerStatements).toBe(0);
        for (let failAt = 1; failAt <= (faults.projectionStatements ?? 0); failAt += 1) {
          const result = await probe(`fault-${failAt}`, `fault:${failAt}`);
          expect(result.failures).toEqual([]);
          expect(result.info.appendFailed ?? 0).toBe(
            kind === "celld" && result.info.writeFault === 1 ? 1 : 0,
          );
        }

        for (const step of ["activate", "discard-and-repair", "missed-session", "init"]) {
          expect((await probe(step, step)).failures).toEqual([]);
        }

        // A build resumes from its committed cursor after the process dies.
        const started = await probe("build", "build-start");
        expect(started).toEqual({ failures: [], info: { cursor: 5 } });
        await host.restart({ hard: true });
        const resumed = await probe("build", "build-resume");
        expect(resumed.failures).toEqual([]);
        expect(resumed.info.cursorAtResume).toBe(5);

        // A real session: the runtime maintains the transcript in its appends.
        await submitMessage(host, sessionId, { idempotencyKey: "transcript:first", text: "ping" });
        const status = await get<TranscriptStatus>(`/sessions/${sessionId}/transcript-status`);
        expect(status.converged).toBe(true);
        expect(status.versions).toHaveLength(1);
        expect(status.versions[0]?.cursor).toBe(status.headSeq);
        // The adapter writes the incarnation it minted to the host's key, so both agree.
        expect(await get(`/sessions/${sessionId}/kv-incarnation`)).toEqual({
          incarnation: status.incarnation,
        });
        const live = await get<TranscriptRead>(`/sessions/${sessionId}/transcript`);
        expect(live._tag).toBe("Window");
        expect(live.window.readiness._tag).toBe("Ready");
        expect(live.window.records.map((record) => record.kind)).toEqual(["user", "assistant"]);

        // The object serves transcript sockets from its own transcript store, without the runtime.
        const subscribed = await subscribeTranscript(host, sessionId);
        expect(subscribed.window?.readiness._tag).toBe("Ready");
        expect(subscribed.window?.records.map((record) => record.kind)).toEqual([
          "user",
          "assistant",
        ]);
        expect(subscribed.window?.incarnation).toBe(status.incarnation);

        // Cold reads and backfill status never build the model runtime.
        await host.restart({ hard: true });
        const cold = await get<TranscriptStatus>(`/sessions/${sessionId}/transcript-status`);
        expect(cold).toEqual(status);
        expect(await get<TranscriptRead>(`/sessions/${sessionId}/transcript`)).toEqual(live);
        expect(await get(`/sessions/${sessionId}/runtime-builds`)).toEqual({ count: 0 });

        // An alarm armed by an earlier release stays the runtime's even when the builder also
        // wants the alarm: after the restart (a deploy), the builder's wakeup runs storage-only
        // and the earlier alarm still builds the runtime when it fires.
        await get(`/sessions/${sessionId}/arm-unrecorded-alarm?delayMs=4000`);
        await host.restart({ hard: true });
        const afterDeploy = await get<TranscriptStatus>(`/sessions/${sessionId}/transcript-status`);
        expect(afterDeploy.pending).toBe(false);
        expect(await get(`/sessions/${sessionId}/runtime-builds`)).toEqual({ count: 0 });
        const deadline = Date.now() + 30_000;
        let runtimeBuilds = 0;
        while (runtimeBuilds === 0 && Date.now() < deadline) {
          await new Promise((resolveWait) => setTimeout(resolveWait, 500));
          ({ count: runtimeBuilds } = await get<{ readonly count: number }>(
            `/sessions/${sessionId}/runtime-builds`,
          ));
        }
        expect(runtimeBuilds).toBeGreaterThan(0);

        // A wiped log is a new incarnation.
        const destroyed = await fetch(`${host.baseUrl}/sessions/${sessionId}/destroy`, {
          method: "DELETE",
        });
        expect(destroyed.status).toBe(204);
        const wiped = await get<TranscriptStatus>(`/sessions/${sessionId}/transcript-status`);
        expect(wiped.incarnation).not.toBe(status.incarnation);
        expect(await get(`/sessions/${sessionId}/kv-incarnation`)).toEqual({
          incarnation: wiped.incarnation,
        });
        expect(wiped.headSeq).toBe(0);
        expect(wiped.converged).toBe(true);
      } finally {
        await host.stop();
      }
    }, 600_000);

    // Opt-in measurement (design §13 gate 6): EDA_TRANSCRIPT_PERF=true.
    it.skipIf(process.env.EDA_TRANSCRIPT_PERF !== "true")(
      "measures append overhead and build cost at 10k events",
      async () => {
        const host = await startHost(kind);
        const timed = async (object: string, step: string) => {
          const started = performance.now();
          const result = await decodeJson<TranscriptProbeResult>(
            await fetch(
              `${host.baseUrl}/transcript-probe/${kind}-${object}/${encodeURIComponent(step)}`,
              {
                signal: AbortSignal.timeout(600_000),
              },
            ),
          );
          expect(result.failures).toEqual([]);
          return { ms: performance.now() - started, info: result.info };
        };
        try {
          await timed("perf-warm", "perf-append:on");
          const off = await timed("perf-off", "perf-append:off");
          const on = await timed("perf-on", "perf-append:on");
          const build = await timed("perf-off", "perf-build");
          const appends = on.info.appends ?? 1;
          process.stderr.write(
            `\n[${kind}] 10k events in ${appends} appends: without transcript ${off.ms.toFixed(0)} ms, ` +
              `with ${on.ms.toFixed(0)} ms (+${(((on.ms - off.ms) / appends) * 1000).toFixed(0)} us/append); ` +
              `build ${build.ms.toFixed(0)} ms in ${build.info.slices} slices\n`,
          );
        } finally {
          await host.stop();
        }
      },
      1_200_000,
    );
  });
};
