import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { DatabaseSync } from "node:sqlite";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Tool from "effect/unstable/ai/Tool";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import { describe, expect, it } from "vite-plus/test";

import { EDASessionDurableObjectHost } from "../../src/durable-object-runtime";
import type { EDASessionDurableObjectStorage } from "../../src/durable-object-runtime";
import { EDAWebSocketAckFrame, FrameId } from "../../src/websocket-protocol";
import { SubmitMessageCommand } from "effect-durable-agent/types/commands";
import { CommandId, SequenceNumber, SessionId } from "effect-durable-agent/types/core";
import { sequentialUuidV7 } from "effect-durable-agent/services/id-generator";
import { makeEDAToolkit } from "effect-durable-agent/services/tool-registry";
import type { PositionedEvent } from "effect-durable-agent/types/events";

const shouldRunRealIntegration =
  process.env.EDA_REAL_INTEGRATION === "true" && process.env.OPENAI_API_KEY !== undefined;
const describeRealIntegration = shouldRunRealIntegration ? describe : describe.skip;
const modelId = process.env.EDA_OPENAI_MODEL ?? "gpt-4.1-mini";
const sessionId = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");

interface RealPromptCase {
  readonly commandId: CommandId;
  readonly expectedText?: RegExp;
  readonly minToolCalls?: number;
  readonly name: string;
  readonly requiredEventTypes?: ReadonlyArray<string>;
  readonly text: string;
}

interface PromptResult {
  readonly events: ReadonlyArray<PositionedEvent>;
  readonly text: string;
}

const ReadSkillParams = Schema.Struct({ name: Schema.String });
const CalculateParams = Schema.Struct({ expression: Schema.String });
const LookupAccountParams = Schema.Struct({ district: Schema.String });

const ReadSkillTool = Tool.make("readSkill", {
  description:
    "Read a named internal skill package before answering. Available names: competitive, revenue, implementation.",
  parameters: ReadSkillParams,
  success: Schema.Unknown,
});

const CalculateTool = Tool.make("calculate", {
  description: "Evaluate a small arithmetic expression for integration testing.",
  parameters: CalculateParams,
  success: Schema.Unknown,
});

const LookupAccountTool = Tool.make("lookupAccount", {
  description: "Look up a deterministic district account profile for integration testing.",
  parameters: LookupAccountParams,
  success: Schema.Unknown,
});

const realIntegrationToolkit = Effect.runSync(
  makeEDAToolkit([ReadSkillTool, CalculateTool, LookupAccountTool], {
    readSkill: ({ name }) =>
      Effect.succeed({
        name,
        markdown:
          name === "competitive"
            ? "# Competitive\nLead with buyer pain, cite sources, and contrast switching costs."
            : name === "revenue"
              ? "# Revenue\nTie every recommendation to pipeline stage, renewal risk, and next action."
              : "# Implementation\nPrefer phased rollout, success criteria, and owner/date/accountability.",
      }),
    calculate: ({ expression }) =>
      Effect.succeed({ expression, value: arithmeticFixtures[expression] ?? "unsupported" }),
    lookupAccount: ({ district }) =>
      Effect.succeed({
        district,
        renewalRisk: "medium",
        products: ["GoGuardian Admin", "GoGuardian Teacher"],
        nextBestAction: "schedule implementation-health review",
      }),
  }),
);

const arithmeticFixtures: Record<string, number> = {
  "2+2": 4,
  "3+4": 7,
  "6*7": 42,
  "12+30": 42,
};

function commandId(index: number): CommandId {
  return CommandId.make(sequentialUuidV7(30_000 + index));
}

const realPromptCases: ReadonlyArray<RealPromptCase> = [
  {
    commandId: commandId(1),
    name: "simple marker response",
    text: 'Reply with exactly one short sentence containing the marker "EDA-PONG".',
    expectedText: /EDA-PONG/i,
  },
  {
    commandId: commandId(2),
    name: "structured rollout plan",
    text: "Give a three-bullet rollout plan for a district admin evaluating classroom visibility.",
    expectedText: /rollout|district|admin/i,
  },
  {
    commandId: commandId(3),
    name: "markdown with sources section",
    text: 'Return markdown with a heading and a "Sources" section. Use placeholder source https://example.com/source.',
    expectedText: /sources/i,
  },
  {
    commandId: commandId(4),
    name: "memory seed",
    text: 'Remember the code word "river" for later. Acknowledge in one sentence.',
    expectedText: /river/i,
  },
  {
    commandId: commandId(5),
    name: "memory recall",
    text: "What code word did I ask you to remember? Answer with only that word if possible.",
    expectedText: /river/i,
  },
  {
    commandId: commandId(6),
    minToolCalls: 1,
    name: "skill package lookup",
    requiredEventTypes: ["ToolCallCreated", "ToolCallStarted", "ToolCallCompleted"],
    text: 'You must call readSkill exactly once with name "competitive" before answering. Then write one sentence that includes "competitive".',
    expectedText: /competitive/i,
  },
  {
    commandId: commandId(7),
    minToolCalls: 1,
    name: "calculator tool",
    requiredEventTypes: ["ToolCallCreated", "ToolCallStarted", "ToolCallCompleted"],
    text: 'You must call calculate exactly once with expression "6*7" before answering. Include the numeric result.',
    expectedText: /42/,
  },
  {
    commandId: commandId(8),
    minToolCalls: 1,
    name: "account lookup tool",
    requiredEventTypes: ["ToolCallCreated", "ToolCallStarted", "ToolCallCompleted"],
    text: 'You must call lookupAccount exactly once for district "Lakota SD" before answering with the next best action.',
    expectedText: /implementation-health|Lakota|review/i,
  },
  {
    commandId: commandId(9),
    minToolCalls: 2,
    name: "multi-tool account synthesis",
    requiredEventTypes: ["ToolCallCreated", "ToolCallCompleted"],
    text: 'Before answering, call readSkill with name "revenue" and lookupAccount for district "Lakota SD". Then give one revenue next step.',
    expectedText: /revenue|pipeline|renewal|next/i,
  },
  {
    commandId: commandId(10),
    minToolCalls: 2,
    name: "parallel arithmetic tool calls",
    requiredEventTypes: ["ToolCallCreated", "ToolCallCompleted"],
    text: 'Before answering, call calculate for expression "2+2" and calculate for expression "3+4". Include both results.',
    expectedText: /4|7/,
  },
  {
    commandId: commandId(11),
    name: "long-context synthesis",
    text: "Summarize the prior tool-assisted work in exactly two sentences.",
    expectedText: /tool|skill|account|calculate|prior/i,
  },
  {
    commandId: commandId(12),
    minToolCalls: 2,
    name: "advanced skill plus calculation",
    requiredEventTypes: ["ToolCallCreated", "ToolCallCompleted"],
    text: 'Call readSkill with name "implementation" and calculate with expression "12+30" before answering. Include result 42 and one rollout criterion.',
    expectedText: /42|rollout|criterion|implementation/i,
  },
];

describeRealIntegration("EDA Durable Object host real local integration", () => {
  it("runs a dozen real prompts through DO storage, the EDA runtime, real model streaming, tools, and transcript hydration", async () => {
    const apiKey = process.env.OPENAI_API_KEY;
    if (apiKey === undefined) {
      throw new Error("OPENAI_API_KEY is required when EDA_REAL_INTEGRATION=true");
    }

    const context = createIntegrationDurableObjectContext();
    const host = makeRealIntegrationHost(apiKey, context.storage);

    try {
      await Effect.runPromise(EDASessionDurableObjectHost.migrate(context.storage));
      let afterSeq = SequenceNumber.make(0);
      const results: PromptResult[] = [];

      for (const promptCase of realPromptCases) {
        const events = await runOnePrompt(host, promptCase, afterSeq);
        afterSeq = SequenceNumber.make(Math.max(...events.map((event) => event.position.seq)));
        results.push({ events, text: assistantText(events) });
      }

      const messages = await host.messages({ sessionId });
      await context.waitUntilSettled();

      expect(results).toHaveLength(realPromptCases.length);
      expect(messages.filter((message) => message._tag === "User")).toHaveLength(
        realPromptCases.length,
      );
      expect(
        messages.filter((message) => message._tag === "Assistant").length,
      ).toBeGreaterThanOrEqual(realPromptCases.length);

      for (const [index, promptCase] of realPromptCases.entries()) {
        const result = results[index];
        if (result === undefined) {
          throw new Error(`Missing result for ${promptCase.name}`);
        }
        const types = eventTypes(result.events);
        expect(types, promptCase.name).toEqual(
          expect.arrayContaining([
            "CommandAdmitted",
            "CommandStarted",
            "UserMessageCommitted",
            "RunStarted",
            "TurnStarted",
            "InferenceStarted",
            "CommandCompleted",
          ]),
        );
        for (const eventType of promptCase.requiredEventTypes ?? []) {
          expect(types, promptCase.name).toContain(eventType);
        }
        if (promptCase.minToolCalls !== undefined) {
          expect(
            result.events.filter((event) => event.event.type === "ToolCallCreated").length,
            promptCase.name,
          ).toBeGreaterThanOrEqual(promptCase.minToolCalls);
        }
        if (promptCase.expectedText !== undefined) {
          expect(result.text, promptCase.name).toMatch(promptCase.expectedText);
        }
      }
    } finally {
      await host.dispose();
      context.close();
    }
  }, 600_000);
});

const makeRealIntegrationHost = (
  apiKey: string,
  storage: EDASessionDurableObjectStorage,
): EDASessionDurableObjectHost => {
  const client = OpenAiClient.layer({
    apiKey: Redacted.make(apiKey),
    ...(process.env.OPENAI_API_URL === undefined ? {} : { apiUrl: process.env.OPENAI_API_URL }),
  }).pipe(Layer.provide(FetchHttpClient.layer));
  const modelLayer = OpenAiLanguageModel.layer({ model: modelId }).pipe(Layer.provide(client));

  return new EDASessionDurableObjectHost({
    config: {
      modelSelection: { provider: "openai", modelId },
      systemPrompt:
        "You are validating effect-durable-agent integration locally. When a user says you must call a provided tool before answering, call that tool first. Keep answers concise and deterministic.",
    },
    modelLayer,
    storage,
    toolkit: realIntegrationToolkit,
  });
};

const runOnePrompt = async (
  host: EDASessionDurableObjectHost,
  promptCase: RealPromptCase,
  afterSeq: SequenceNumber,
): Promise<ReadonlyArray<PositionedEvent>> => {
  const webSocket = new TestWebSocket();
  await host.acceptEventWebSocket({ sessionId, afterSeq, webSocket: webSocket.asWebSocket() });
  const eventsPromise = collectWebSocketEventsUntil(host, webSocket, (event) =>
    isTerminalFor(event, promptCase.commandId),
  );

  await host.submit({
    sessionId,
    command: new SubmitMessageCommand({
      commandId: promptCase.commandId,
      disposition: "queue",
      content: [Prompt.textPart({ text: promptCase.text })],
    }),
  });

  return await eventsPromise;
};

const collectWebSocketEventsUntil = async (
  host: EDASessionDurableObjectHost,
  webSocket: TestWebSocket,
  predicate: (event: PositionedEvent) => boolean,
): Promise<ReadonlyArray<PositionedEvent>> => {
  const events: PositionedEvent[] = [];
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeout = setTimeout(
      () => reject(new Error("Timed out waiting for EDA WebSocket event")),
      120_000,
    );
  });

  const readPromise = (async () => {
    while (true) {
      const raw = await webSocket.nextMessage();
      const frame = JSON.parse(raw) as {
        readonly _tag: string;
        readonly durableThroughSeq?: number;
        readonly events?: ReadonlyArray<PositionedEvent>;
        readonly frameId?: number;
        readonly message?: string;
      };
      if (frame._tag === "hello" || frame._tag === "heartbeat") {
        continue;
      }
      if (frame._tag === "lagged" || frame._tag === "error") {
        throw new Error(
          `EDA WebSocket closed before expected event: ${frame.message ?? frame._tag}`,
        );
      }
      if (frame._tag !== "events" || frame.frameId === undefined || frame.events === undefined) {
        throw new Error(`Unexpected EDA WebSocket frame: ${raw}`);
      }

      for (const event of frame.events) {
        events.push(event);
      }
      await host.webSocketMessage(
        webSocket.asWebSocket(),
        JSON.stringify(
          EDAWebSocketAckFrame.make({
            _tag: "ack",
            frameId: FrameId.make(frame.frameId),
            durableThroughSeq: SequenceNumber.make(frame.durableThroughSeq ?? 0),
          }),
        ),
      );
      if (frame.events.some(predicate)) {
        return events;
      }
    }
  })();

  try {
    return await Promise.race([readPromise, timeoutPromise]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
    webSocket.close(1000, "test complete");
    await host.webSocketClose(webSocket.asWebSocket());
  }
};

class TestWebSocket {
  private attachment: unknown;
  private closed = false;
  private readonly messages: string[] = [];
  private readonly waiters: Array<(message: string) => void> = [];
  readonly readyState = 1;
  closeCode: number | undefined;
  closeReason: string | undefined;

  asWebSocket(): WebSocket {
    return this as unknown as WebSocket;
  }

  send(message: string | ArrayBuffer | ArrayBufferView): void {
    if (this.closed) {
      throw new Error("WebSocket is closed");
    }
    if (typeof message !== "string") {
      throw new Error("Expected text frame");
    }
    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter(message);
      return;
    }
    this.messages.push(message);
  }

  close(code?: number, reason?: string): void {
    this.closed = true;
    this.closeCode = code;
    this.closeReason = reason;
  }

  serializeAttachment(attachment: unknown): void {
    this.attachment = attachment;
  }

  deserializeAttachment(): unknown {
    return this.attachment;
  }

  nextMessage(): Promise<string> {
    const message = this.messages.shift();
    if (message !== undefined) {
      return Promise.resolve(message);
    }
    return new Promise((resolve) => this.waiters.push(resolve));
  }
}

const isTerminalFor = (event: PositionedEvent, commandIdValue: CommandId): boolean => {
  if (
    event.event.type !== "CommandCompleted" &&
    event.event.type !== "CommandFailed" &&
    event.event.type !== "CommandCancelled"
  ) {
    return false;
  }
  return (event.event.payload as { readonly commandId?: unknown }).commandId === commandIdValue;
};

const assistantText = (events: ReadonlyArray<PositionedEvent>): string =>
  events
    .filter((event) => event.event.type === "TextDelta")
    .map((event) => (event.event.payload as { readonly delta: string }).delta)
    .join("");

const eventTypes = (events: ReadonlyArray<PositionedEvent>): ReadonlyArray<string> =>
  events.map((event) => event.event.type);

type SqliteValue = null | number | string | Uint8Array;

interface IntegrationDurableObjectContext {
  readonly close: () => void;
  readonly storage: EDASessionDurableObjectStorage;
  readonly waitUntilSettled: () => Promise<void>;
}

interface DurableObjectSqlCursor<TRow extends Record<string, unknown>> extends Iterable<TRow> {
  readonly rowsRead: number;
  readonly rowsWritten: number;
  next(): IteratorResult<TRow>;
  one(): TRow;
  raw(): { toArray(): unknown[][] };
  toArray(): TRow[];
}

const createIntegrationDurableObjectContext = (): IntegrationDurableObjectContext => {
  const sqlite = new DatabaseSync(":memory:");
  const waitUntilPromises: Promise<unknown>[] = [];
  const storage = new IntegrationDurableObjectStorage(sqlite);
  return {
    close: () => sqlite.close(),
    storage: storage as unknown as EDASessionDurableObjectStorage,
    waitUntilSettled: async () => {
      await Promise.all(waitUntilPromises.splice(0));
    },
  };
};

const normalizeParam = (value: unknown): SqliteValue => {
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }
  if (typeof value === "number" || typeof value === "string") {
    return value;
  }
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  throw new TypeError(`Unsupported Durable Object SQLite test value: ${typeof value}`);
};

const isReadStatement = (sql: string): boolean =>
  /^\s*(?:select|with|pragma)\b/i.test(sql) || /\breturning\b/i.test(sql);

const hasMultipleStatements = (sql: string): boolean =>
  sql
    .split(";")
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0).length > 1;

class IntegrationSqlCursor<
  TRow extends Record<string, unknown>,
> implements DurableObjectSqlCursor<TRow> {
  readonly rowsRead: number;
  readonly rowsWritten: number;
  readonly #iterator: Iterator<TRow>;

  constructor(
    private readonly rows: TRow[],
    rowsWritten: number,
  ) {
    this.rowsRead = rows.length;
    this.rowsWritten = rowsWritten;
    this.#iterator = rows[Symbol.iterator]();
  }

  [Symbol.iterator](): Iterator<TRow> {
    return this.rows[Symbol.iterator]();
  }

  next(): IteratorResult<TRow> {
    return this.#iterator.next();
  }

  one(): TRow {
    const row = this.rows[0];
    if (!row) {
      throw new Error("Expected Durable Object SQLite query to return one row.");
    }
    return row;
  }

  raw(): { toArray(): unknown[][] } {
    return {
      toArray: () => this.rows.map((row) => Object.values(row)),
    };
  }

  toArray(): TRow[] {
    return [...this.rows];
  }
}

class IntegrationDurableObjectSql {
  constructor(private readonly sqlite: DatabaseSync) {}

  exec<TRow extends Record<string, unknown>>(
    query: string,
    ...params: SqliteValue[]
  ): DurableObjectSqlCursor<TRow> {
    const normalizedParams = params.map(normalizeParam);
    if (normalizedParams.length === 0 && !isReadStatement(query) && hasMultipleStatements(query)) {
      this.sqlite.exec(query);
      return new IntegrationSqlCursor<TRow>([], 0);
    }

    const statement = this.sqlite.prepare(query);
    if (isReadStatement(query)) {
      return new IntegrationSqlCursor(statement.all(...normalizedParams) as TRow[], 0);
    }

    const result = statement.run(...normalizedParams);
    return new IntegrationSqlCursor<TRow>([], Number(result.changes));
  }
}

class IntegrationDurableObjectStorage {
  readonly sql: IntegrationDurableObjectSql;
  readonly #kv = new Map<string, unknown>();
  #alarm: null | number = null;

  constructor(private readonly sqlite: DatabaseSync) {
    this.sql = new IntegrationDurableObjectSql(sqlite);
  }

  async delete(key: string): Promise<boolean> {
    return this.#kv.delete(key);
  }

  async deleteAlarm(): Promise<void> {
    this.#alarm = null;
  }

  async get<T = unknown>(key: string): Promise<T | undefined> {
    return this.#kv.get(key) as T | undefined;
  }

  async getAlarm(): Promise<null | number> {
    return this.#alarm;
  }

  async put(key: string, value: unknown): Promise<void> {
    this.#kv.set(key, value);
  }

  async setAlarm(scheduledTime: number | Date): Promise<void> {
    this.#alarm = scheduledTime instanceof Date ? scheduledTime.getTime() : scheduledTime;
  }

  transactionSync<T>(callback: () => T): T {
    this.sqlite.exec("BEGIN");
    try {
      const result = callback();
      this.sqlite.exec("COMMIT");
      return result;
    } catch (error) {
      this.sqlite.exec("ROLLBACK");
      throw error;
    }
  }
}
