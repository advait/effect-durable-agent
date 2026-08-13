import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { build } from "esbuild";
import { describe, expect, it } from "vite-plus/test";

import { installCelld } from "../../scripts/install-celld.mjs";

type HostKind = "celld" | "cloudflare";

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
    celldBinary = await installCelld();
    const deployment = spawn(
      celldBinary,
      ["deploy", resolve(import.meta.dirname, "celld.wrangler.jsonc"), "--dry-run"],
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
    deployment.stdout?.on("data", (chunk: Buffer) => {
      deployLogs += chunk.toString("utf8");
    });
    deployment.stderr?.on("data", (chunk: Buffer) => {
      deployLogs += chunk.toString("utf8");
    });
    await waitForExit(deployment, 90_000);
    if (deployment.exitCode !== 0) {
      throw new Error(`celld deploy --dry-run failed (${deployment.exitCode})\n${deployLogs}`);
    }
  }

  const launch = async (options: { readonly blockModel?: boolean } = {}): Promise<void> => {
    logs = "";
    const blockModel = options.blockModel === true;
    if (kind === "cloudflare") {
      child = spawn(
        resolve(repositoryRoot, "node_modules/.bin/wrangler"),
        [
          "dev",
          resolve(import.meta.dirname, "cloudflare-worker.ts"),
          "--config",
          resolve(import.meta.dirname, "wrangler.jsonc"),
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
        entryPoints: [resolve(import.meta.dirname, "celld-worker.ts")],
        external: ["cloudflare:workers"],
        format: "esm",
        outfile: celldBundle!,
        platform: "browser",
        sourcemap: true,
        target: "es2022",
      });
      child = spawn(celldBinary!, [], {
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
    }

    const appendLog = (chunk: Buffer) => {
      logs = `${logs}${chunk.toString("utf8")}`.slice(-20_000);
    };
    child.stdout?.on("data", appendLog);
    child.stderr?.on("data", appendLog);

    const deadline = Date.now() + 90_000;
    while (Date.now() < deadline) {
      if (child.exitCode !== null) {
        throw new Error(`${kind} exited during startup (${child.exitCode})\n${logs}`);
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

  static async open(host: HostProcess, sessionId: string, afterSeq: number): Promise<EventSocket> {
    const url = new URL(`${host.baseUrl}/sessions/${sessionId}/events`);
    url.protocol = "ws:";
    url.searchParams.set("afterSeq", String(afterSeq));
    const socket = new WebSocket(url);
    const eventSocket = new EventSocket(socket);
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

describe.each(["cloudflare", "celld"] as const)("%s host conformance", (kind) => {
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
      expect(interruptedEvents.some((event) => event.event.type === "CommandAdmitted")).toBe(true);
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
      expect(recovered.messages[5]?.content?.text).toBe("pong");

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
});
