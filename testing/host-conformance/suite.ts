import { spawn, type ChildProcess } from "node:child_process";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { build } from "esbuild";
import * as Prompt from "effect/unstable/ai/Prompt";
import { getEnginePath } from "@rivetkit/engine-cli";
import { createClient } from "rivetkit/client";
import { describe, expect, it } from "vite-plus/test";

import { installCelld } from "../../packages/effect-durable-agent-celld/scripts/install-celld.mjs";
import { encodeEDARivetCommand } from "../../packages/effect-durable-agent-rivet/src/actor";
import type { registry as rivetConformanceRegistry } from "../../packages/effect-durable-agent-rivet/testing/host-conformance/server";
import { CommandIdempotencyKey, SubmitMessageCommand } from "effect-durable-agent/types/commands";

/** Runtime variants required to satisfy the shared EDA host contract. */
export type HostKind = "celld" | "cloudflare" | "rivet";

const rivetConformanceAuthorization = "eda-rivet-conformance-authorized";

const rivetConnectionParams = (afterSeq?: number) => ({
  authorization: rivetConformanceAuthorization,
  ...(afterSeq === undefined ? {} : { afterSeq }),
});

interface HostProcess {
  readonly baseUrl: string;
  readonly kind: HostKind;
  diagnostics(): string;
  restart(options?: { readonly blockModel?: boolean; readonly hard?: boolean }): Promise<void>;
  stop(): Promise<void>;
}

interface MessageResult {
  readonly messages: ReadonlyArray<{
    readonly _tag: string;
    readonly content?: { readonly text?: string } | ReadonlyArray<unknown>;
  }>;
  readonly snapshot: { readonly lastSeq: number };
  readonly terminal: {
    readonly event: { readonly type: string };
    readonly position: { readonly seq: number };
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

const messageText = (
  content: { readonly text?: string } | ReadonlyArray<unknown> | undefined,
): string | undefined =>
  content !== undefined && "text" in content && typeof content.text === "string"
    ? content.text
    : undefined;

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
  await mkdir(dataDirectory, { recursive: true });
  // Rivet Engine 2.3 advertises its local canonical endpoint as :6420 to
  // serverless runners, even when Guard binds elsewhere. Use that canonical
  // local endpoint while still allocating the runner's private ports.
  const port = kind === "rivet" ? 6420 : await availablePort();
  const internalPort = await availablePort();
  const rivetPeerPort = kind === "rivet" ? await availablePort() : undefined;
  const rivetMetricsPort = kind === "rivet" ? await availablePort() : undefined;
  const baseUrl = `http://127.0.0.1:${port}`;
  let child: ChildProcess | undefined;
  let rivetEngine: ChildProcess | undefined;
  let logs = "";
  let rivetEngineLogs = "";

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

  const startRivetEngine = (): void => {
    if (kind !== "rivet") {
      return;
    }
    const launchedEngine = spawn(getEnginePath(), ["start"], {
      cwd: repositoryRoot,
      detached: process.platform !== "win32",
      env: {
        ...process.env,
        RIVET_INSPECTOR_DISABLE: "1",
        RIVET_LOG_LEVEL: "error",
        RIVET__API_PEER__HOST: "127.0.0.1",
        RIVET__API_PEER__PORT: String(rivetPeerPort),
        RIVET__FILE_SYSTEM__PATH: join(dataDirectory, "rivet-engine"),
        RIVET__GUARD__HOST: "127.0.0.1",
        RIVET__GUARD__PORT: String(port),
        RIVET__METRICS__HOST: "127.0.0.1",
        RIVET__METRICS__PORT: String(rivetMetricsPort),
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    const appendEngineLog = (chunk: Uint8Array) => {
      rivetEngineLogs = `${rivetEngineLogs}${new TextDecoder().decode(chunk)}`.slice(-20_000);
    };
    launchedEngine.stdout?.on("data", appendEngineLog);
    launchedEngine.stderr?.on("data", appendEngineLog);
    rivetEngine = launchedEngine;
  };

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
    } else if (kind === "celld") {
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
          CELLD_TEST_DO_BINDINGS: "EDA_SESSION=EDAConformanceSession",
          CELLD_TEST_DO_CLASSES: "EDAConformanceSession",
          CELLD_TEST_SCRIPT_PATH: celldBundle!,
        },
        stdio: ["ignore", "pipe", "pipe"],
      });
    } else {
      launchedChild = spawn(
        resolve(repositoryRoot, "node_modules/.bin/tsx"),
        [
          resolve(
            repositoryRoot,
            "packages/effect-durable-agent-rivet/testing/host-conformance/server.ts",
          ),
        ],
        {
          cwd: repositoryRoot,
          detached: process.platform !== "win32",
          env: {
            ...process.env,
            EDA_CONFORMANCE_BLOCK_MODEL: blockModel ? "1" : "0",
            EDA_RIVET_AUTHORIZATION: rivetConformanceAuthorization,
            EDA_RIVET_ENGINE_ENDPOINT: baseUrl,
            EDA_RIVET_HTTP_PORT: String(internalPort),
            RIVET_INSPECTOR_DISABLE: "1",
            RIVETKIT_RUNTIME_MODE: "serverless",
            RIVET_LOG_LEVEL: "error",
            XDG_DATA_HOME: join(dataDirectory, "xdg"),
          },
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
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
        throw new Error(
          `${kind} exited during startup (${launchedChild.exitCode})\n${logs}\n${rivetEngineLogs}`,
        );
      }
      if (rivetEngine?.exitCode !== null && rivetEngine?.exitCode !== undefined) {
        throw new Error(
          `rivet engine exited during startup (${rivetEngine.exitCode})\n${rivetEngineLogs}`,
        );
      }
      if (kind === "rivet") {
        if (logs.includes("EDA_RIVET_READY")) {
          return;
        }
      } else {
        try {
          const response = await fetch(`${baseUrl}/health`, {
            signal: AbortSignal.timeout(1_000),
          });
          if (response.ok) {
            return;
          }
        } catch {
          // The runtime is still binding its listener.
        }
      }
      await new Promise((resolveWait) => setTimeout(resolveWait, 100));
    }
    throw new Error(`Timed out starting ${kind}\n${logs}\n${rivetEngineLogs}`);
  };

  const stopChild = async (hard = false): Promise<void> => {
    if (child === undefined) {
      return;
    }
    killHostProcess(child, hard ? "SIGKILL" : "SIGTERM");
    await waitForExit(child, 10_000);
    child = undefined;
  };

  const stopRivetEngine = async (): Promise<void> => {
    if (rivetEngine === undefined) {
      return;
    }
    killHostProcess(rivetEngine, "SIGTERM");
    await waitForExit(rivetEngine, 10_000);
    rivetEngine = undefined;
  };

  try {
    startRivetEngine();
    await launch();
  } catch (error) {
    await stopChild();
    await stopRivetEngine();
    await rm(scratch, { force: true, recursive: true });
    throw error;
  }
  return {
    baseUrl,
    diagnostics: () => `${logs}\n${rivetEngineLogs}`,
    kind,
    restart: async (options) => {
      await stopChild(options?.hard);
      await launch(options);
    },
    stop: async () => {
      await stopChild();
      await stopRivetEngine();
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
): Promise<MessageResult> => {
  if (host.kind === "rivet") {
    const handle = createClient<typeof rivetConformanceRegistry>(
      host.baseUrl,
    ).edaConformanceSession.getOrCreate([sessionId], { params: rivetConnectionParams() });
    const command = encodeEDARivetCommand(
      new SubmitMessageCommand({
        idempotencyKey: CommandIdempotencyKey.make(input.idempotencyKey),
        disposition: "queue",
        content: [Prompt.textPart({ text: input.text })],
      }),
    );
    const terminal = await handle.submitAndBlock({ command });
    const messages = await handle.messages({});
    const snapshot = await handle.snapshot({});
    return {
      messages,
      snapshot: { lastSeq: snapshot.state.lastSeq },
      terminal: terminal as MessageResult["terminal"],
    };
  }
  return await decodeJson<MessageResult>(
    await fetch(`${host.baseUrl}/sessions/${sessionId}/messages`, {
      body: JSON.stringify(input),
      headers: { "content-type": "application/json" },
      method: "POST",
      signal: AbortSignal.timeout(30_000),
    }),
  );
};

const getMessages = async (
  host: HostProcess,
  sessionId: string,
): Promise<MessageResult["messages"]> => {
  if (host.kind === "rivet") {
    return await createClient<typeof rivetConformanceRegistry>(host.baseUrl)
      .edaConformanceSession.getOrCreate([sessionId], { params: rivetConnectionParams() })
      .messages({});
  }
  return await decodeJson(
    await fetch(`${host.baseUrl}/sessions/${sessionId}/messages`, {
      signal: AbortSignal.timeout(30_000),
    }),
  );
};

const destroySession = async (host: HostProcess, sessionId: string): Promise<number> => {
  if (host.kind === "rivet") {
    await createClient<typeof rivetConformanceRegistry>(host.baseUrl)
      .edaConformanceSession.getOrCreate([sessionId], { params: rivetConnectionParams() })
      .destroySession({});
    return 204;
  }
  return (
    await fetch(`${host.baseUrl}/sessions/${sessionId}/destroy`, {
      method: "DELETE",
    })
  ).status;
};

interface ConformanceWebSocket {
  readonly readyState: number;
  addEventListener(
    type: string,
    listener: (event: { readonly data?: unknown }) => void,
    options?: {
      readonly once?: boolean;
    },
  ): void;
  close(code?: number, reason?: string): void;
  send(data: string): void;
}

const expectRejectedWebSocket = async (socket: ConformanceWebSocket): Promise<void> => {
  if (socket.readyState === 3) {
    return;
  }
  await new Promise<void>((resolveClose, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("Timed out waiting for unauthorized WebSocket rejection")),
      10_000,
    );
    socket.addEventListener(
      "close",
      () => {
        clearTimeout(timeout);
        resolveClose();
      },
      { once: true },
    );
    socket.addEventListener(
      "message",
      () => {
        clearTimeout(timeout);
        reject(new Error("Unauthorized WebSocket received an EDA frame"));
      },
      { once: true },
    );
  });
};

class EventSocket {
  readonly #closed: Promise<void>;
  readonly #messages: string[] = [];
  readonly #waiters: Array<(message: string) => void> = [];
  readonly #socket: ConformanceWebSocket;
  readonly #diagnostics: () => string;

  private constructor(socket: ConformanceWebSocket, diagnostics: () => string) {
    this.#socket = socket;
    this.#diagnostics = diagnostics;
    this.#closed = new Promise((resolveClose) => {
      socket.addEventListener("close", () => resolveClose(), { once: true });
    });
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

  static async open(host: HostProcess, sessionId: string, afterSeq: number): Promise<EventSocket> {
    let socket: ConformanceWebSocket;
    if (host.kind === "rivet") {
      const session = createClient<typeof rivetConformanceRegistry>(
        host.baseUrl,
      ).edaConformanceSession.getOrCreate([sessionId], {
        params: rivetConnectionParams(afterSeq),
      });
      // Resolve and wake the dynamic actor before opening its raw gateway socket.
      // The local engine can accept the WebSocket upgrade before a newly allocated
      // runner has installed onWebSocket, which would otherwise lose the hello frame.
      await session.messages({});
      socket = await session.webSocket();
    } else {
      socket = new WebSocket(eventSocketUrl(host, sessionId, afterSeq));
    }
    const eventSocket = new EventSocket(socket, host.diagnostics);
    if (socket.readyState === 1) {
      const hello = JSON.parse(await eventSocket.nextMessage()) as { readonly _tag?: string };
      expect(hello._tag).toBe("hello");
      return eventSocket;
    }
    await new Promise<void>((resolveOpen, reject) => {
      socket.addEventListener("open", () => resolveOpen(), { once: true });
      socket.addEventListener("error", () => reject(new Error("WebSocket failed to open")), {
        once: true,
      });
    });
    const hello = JSON.parse(await eventSocket.nextMessage()) as { readonly _tag?: string };
    expect(hello._tag).toBe("hello");
    return eventSocket;
  }

  async eventsUntil(eventType: string): Promise<ReadonlyArray<EventsFrame["events"][number]>> {
    const collected: Array<EventsFrame["events"][number]> = [];
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      const frame = JSON.parse(await this.nextMessage()) as
        | EventsFrame
        | { readonly _tag: "heartbeat" };
      if (frame._tag === "heartbeat") {
        continue;
      }
      expect(frame._tag).toBe("events");
      collected.push(...frame.events);
      this.#socket.send(
        JSON.stringify({
          _tag: "ack",
          durableThroughSeq: frame.durableThroughSeq,
          frameId: frame.frameId,
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

  async waitForClose(): Promise<void> {
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        this.#closed,
        new Promise<never>((_, reject) => {
          timeout = setTimeout(
            () =>
              reject(new Error(`Timed out waiting for WebSocket close\n${this.#diagnostics()}`)),
            30_000,
          );
        }),
      ]);
    } finally {
      clearTimeout(timeout);
    }
  }

  private async nextMessage(): Promise<string> {
    const existing = this.#messages.shift();
    if (existing !== undefined) {
      return existing;
    }
    return await new Promise((resolveMessage, reject) => {
      const timeout = setTimeout(
        () => reject(new Error(`Timed out waiting for WebSocket frame\n${this.#diagnostics()}`)),
        30_000,
      );
      this.#waiters.push((message) => {
        clearTimeout(timeout);
        resolveMessage(message);
      });
    });
  }
}

const eventSocketUrl = (host: HostProcess, sessionId: string, afterSeq: number): URL => {
  const url = new URL(`${host.baseUrl}/sessions/${sessionId}/events`);
  url.protocol = "ws:";
  url.searchParams.set("afterSeq", String(afterSeq));
  return url;
};

/** Register the identical durable-session behavior suite for one concrete host package. */
export const defineHostConformanceSuite = (kind: HostKind): void => {
  describe(`${kind} host conformance`, () => {
    it("preserves command, event-stream, restart, idempotency, and destruction semantics", async () => {
      const host = await startHost(kind);
      const sessionId =
        kind === "cloudflare"
          ? "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a"
          : kind === "celld"
            ? "018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a"
            : "018f6bd5-2f2a-7b1e-8f1c-1f2e3d4c5b6a";
      try {
        if (kind === "rivet") {
          const unauthorizedClient = createClient<typeof rivetConformanceRegistry>(host.baseUrl);
          await expect(
            unauthorizedClient.edaConformanceSession
              .getOrCreate([sessionId], {
                params: { authorization: "wrong" },
              })
              .messages({}),
          ).rejects.toBeDefined();
          const unauthorizedSocket = await unauthorizedClient.edaConformanceSession
            .getOrCreate([sessionId], {
              params: { authorization: "wrong" },
            })
            .webSocket();
          await expectRejectedWebSocket(unauthorizedSocket);
        }
        const initialSocket = await EventSocket.open(host, sessionId, 0);
        const first = await submitMessage(host, sessionId, {
          idempotencyKey: "conformance:first",
          text: "ping one",
        });
        const firstEvents = await initialSocket.eventsUntil("CommandCompleted");
        initialSocket.close();

        expect(first.terminal.event.type).toBe("CommandCompleted");
        expect(first.messages.map((message) => message._tag)).toEqual(["User", "Assistant"]);
        expect(messageText(first.messages[1]?.content)).toBe("pong");
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
        expect(messageText(recovered.messages[5]?.content)).toBe("pong");

        const destroyedSocket =
          kind === "rivet"
            ? await EventSocket.open(host, sessionId, recovered.terminal.position.seq)
            : undefined;
        expect(await destroySession(host, sessionId)).toBe(204);
        await destroyedSocket?.waitForClose();
        expect(await getMessages(host, sessionId)).toEqual([]);

        const recreated = await submitMessage(host, sessionId, {
          idempotencyKey: "conformance:after-destroy",
          text: "ping after destroy",
        });
        expect(recreated.messages.map((message) => message._tag)).toEqual(["User", "Assistant"]);
        expect(messageText(recreated.messages[1]?.content)).toBe("pong");

        expect(await destroySession(host, sessionId)).toBe(204);
        await host.restart();
        expect(await getMessages(host, sessionId)).toEqual([]);
      } catch (error) {
        throw new Error(`${kind} host conformance failed\n${host.diagnostics()}`, { cause: error });
      } finally {
        await host.stop();
      }
    }, 240_000);
  });
};
