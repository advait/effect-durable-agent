import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Prompt from "effect/unstable/ai/Prompt";
import * as Response from "effect/unstable/ai/Response";
import * as LanguageModel from "effect/unstable/ai/LanguageModel";
import { ModelResolver } from "./model-resolver";
import { SessionConfiguredEvent, type ModelSelectionPayload } from "../types/events";
import * as Tool from "effect/unstable/ai/Tool";
import { describe, expect, it } from "vite-plus/test";
import { makeMethods } from "@effect/vitest";

import { SubmitMessageCommand } from "../types/commands";
import {
  CommandId,
  EventId,
  Position,
  RunId,
  SequenceNumber,
  SessionId,
  SubSequenceNumber,
} from "../types/core";
import {
  EventType,
  PositionedEvent,
  UnixEpochMillis,
  effectDurableAgentNamespace,
  schemaV1,
} from "../types/events";
import { LiveEventBus } from "./live-event-bus";
import { EDARuntime } from "./runtime";
import { makeEDAToolkit } from "./tool-registry";
import type { InferenceRunnerStreamPart } from "./inference-runner";
import { makeEdaTestLayer, makeLanguageModelLayer } from "../testkit/layers";

const SESSION_ID = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const COMMAND_ID = "018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a";
const SECOND_COMMAND_ID = "018f6bd5-2f2a-7b1e-8f1c-1f2e3d4c5b6a";
const INTERRUPT_COMMAND_ID = "018f6bd5-2f2a-7b1e-8f1d-1f2e3d4c5b6a";
const RUN_ID = "018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a";
const TURN_ID = "018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a";
const INFERENCE_ID = "018f6bd5-2f2a-7b1e-bf1a-1f2e3d4c5b6a";
const COMMAND_ADMITTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f2a-1f2e3d4c5b6a";
const COMMAND_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f3a-1f2e3d4c5b6a";
const USER_MESSAGE_ID = "018f6bd5-2f2a-7b1e-8f3b-1f2e3d4c5b6a";
const USER_MESSAGE_EVENT_ID = "018f6bd5-2f2a-7b1e-8f3c-1f2e3d4c5b6a";
const RUN_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f4a-1f2e3d4c5b6a";
const TURN_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f5a-1f2e3d4c5b6a";
const INFERENCE_STARTED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f6a-1f2e3d4c5b6a";
const TEXT_EVENT_ID = "018f6bd5-2f2a-7b1e-8f7a-1f2e3d4c5b6a";
const INFERENCE_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f8a-1f2e3d4c5b6a";
const ASSISTANT_MESSAGE_ID = "018f6bd5-2f2a-7b1e-8f8b-1f2e3d4c5b6a";
const ASSISTANT_MESSAGE_EVENT_ID = "018f6bd5-2f2a-7b1e-8f8c-1f2e3d4c5b6a";
const TURN_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8f9a-1f2e3d4c5b6a";
const RUN_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8faa-1f2e3d4c5b6a";
const COMMAND_COMPLETED_EVENT_ID = "018f6bd5-2f2a-7b1e-8fab-1f2e3d4c5b6a";
const LIVE_EPHEMERAL_EVENT_ID = "018f6bd5-2f2a-7b1e-8fac-1f2e3d4c5b6a";

const NoopParams = Schema.Struct({});
const OrderedParams = Schema.Struct({ label: Schema.String });
const OrderedTool = Tool.make("ordered", { parameters: OrderedParams, success: Schema.Unknown });

const defaultRuntimeIds = [
  COMMAND_ADMITTED_EVENT_ID,
  COMMAND_STARTED_EVENT_ID,
  USER_MESSAGE_ID,
  USER_MESSAGE_EVENT_ID,
  RUN_ID,
  RUN_STARTED_EVENT_ID,
  TURN_ID,
  TURN_STARTED_EVENT_ID,
  INFERENCE_ID,
  INFERENCE_STARTED_EVENT_ID,
  TEXT_EVENT_ID,
  ASSISTANT_MESSAGE_ID,
  ASSISTANT_MESSAGE_EVENT_ID,
  INFERENCE_COMPLETED_EVENT_ID,
  TURN_COMPLETED_EVENT_ID,
  RUN_COMPLETED_EVENT_ID,
  COMMAND_COMPLETED_EVENT_ID,
] as const;

const makeTestLayer = (
  stream: Stream.Stream<InferenceRunnerStreamPart, unknown>,
  ids: ReadonlyArray<string> | "sequential" = defaultRuntimeIds,
) =>
  EDARuntime.Live({
    modelSelection: { provider: "test", modelId: "test-model" },
  }).pipe(
    Layer.provideMerge(
      makeEdaTestLayer({
        sessionId: SessionId.make(SESSION_ID),
        ids: ids === "sequential" ? undefined : ids,
        parts: stream,
        toolSchemas: new Map([["noop", NoopParams]]),
      }),
    ),
  );

const command = new SubmitMessageCommand({
  commandId: CommandId.make(COMMAND_ID),
  disposition: "queue",
  content: [Prompt.textPart({ text: "hello" })],
});

const secondCommand = new SubmitMessageCommand({
  commandId: CommandId.make(SECOND_COMMAND_ID),
  disposition: "queue",
  content: [Prompt.textPart({ text: "second" })],
});

const interruptCommand = new SubmitMessageCommand({
  commandId: CommandId.make(INTERRUPT_COMMAND_ID),
  disposition: "interrupt",
  content: [Prompt.textPart({ text: "interrupt" })],
});

const usage = () =>
  new Response.Usage({
    inputTokens: {
      uncached: undefined,
      total: 10,
      cacheRead: undefined,
      cacheWrite: undefined,
    },
    outputTokens: { total: 5, text: undefined, reasoning: undefined },
  });

describe("EDARuntime", () => {
  makeMethods(it).effect(
    "executes with the persisted creation selection instead of the process default",
    () => {
      const selected = {
        provider: "test",
        modelId: "selected",
        settings: { thinkingLevel: "high" },
      };
      const observed: Array<ModelSelectionPayload | undefined> = [];
      const models = Layer.effect(
        ModelResolver,
        Effect.gen(function* () {
          const model = yield* LanguageModel.LanguageModel;
          return {
            resolve: (selection: ModelSelectionPayload | undefined) =>
              Effect.sync(() => {
                observed.push(selection);
                return model;
              }),
          };
        }),
      ).pipe(
        Layer.provide(
          makeLanguageModelLayer(
            Stream.make(
              Response.makePart("text-delta", { id: "text", delta: "hello" }),
              Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
            ),
          ),
        ),
      );
      const runtimeLayer = EDARuntime.Live({
        modelSelection: { provider: "test", modelId: "process-default" },
      }).pipe(
        Layer.provide(
          makeEdaTestLayer({
            sessionId: SessionId.make(SESSION_ID),
            modelResolverLayer: models,
            seedEvents: [
              SessionConfiguredEvent.make({
                namespace: effectDurableAgentNamespace,
                type: "SessionConfigured",
                schemaVersion: schemaV1,
                durability: "durable",
                eventId: EventId.make(TEXT_EVENT_ID),
                sessionId: SessionId.make(SESSION_ID),
                createdAtMs: UnixEpochMillis.make(1),
                payload: { modelSelection: selected },
              }),
            ],
          }),
        ),
      );
      return Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        yield* runtime.submitAndBlock(command);
        yield* runtime.submitAndBlock(secondCommand);
        const snapshot = yield* runtime.snapshot();
        expect(snapshot.state.modelSelection).toEqual(selected);
        expect(snapshot.state.tokenConsumption.byModel).toMatchObject([
          { modelId: "selected", usage: { inputTokens: 20 } },
        ]);
        expect(observed).toEqual([selected, selected]);
      }).pipe(Effect.provide(runtimeLayer));
    },
  );
  it("owns command submission and the blocking dispatch process", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const liveBus = yield* LiveEventBus;
        const runtime = yield* EDARuntime;
        const liveStream = yield* liveBus.subscribe();
        const liveFiber = yield* liveStream.pipe(
          Stream.take(12),
          Stream.runCollect,
          Effect.forkScoped,
        );
        const admitted = yield* runtime.submit(command);
        const liveEvents = yield* Fiber.join(liveFiber);
        const messages = yield* runtime.messages();
        const snapshot = yield* runtime.snapshot();
        return { admitted, liveEvents, messages, snapshot };
      }),
    ).pipe(Effect.provide(makeTestLayer(stream)));

    const { admitted, liveEvents, messages, snapshot } = await Effect.runPromise(program);

    expect(admitted.event).toMatchObject({
      type: "CommandAdmitted",
      eventId: EventId.make(COMMAND_ADMITTED_EVENT_ID),
    });
    expect(Array.from(liveEvents).map((event) => event.event.type)).toEqual([
      "CommandAdmitted",
      "UserMessageSubmitted",
      "CommandStarted",
      "RunStarted",
      "TurnStarted",
      "InferenceStarted",
      "TextDelta",
      "AssistantMessageCommitted",
      "InferenceCompleted",
      "TurnCompleted",
      "RunCompleted",
      "CommandCompleted",
    ]);
    expect(Array.from(liveEvents)[3]).toMatchObject({
      event: { type: "RunStarted", payload: { runId: RunId.make(RUN_ID) } },
    });
    expect(messages.map((message) => message._tag)).toEqual(["User", "Assistant"]);
    expect(messages[0]).toMatchObject({ _tag: "User", content: command.content });
    expect(messages[1]).toMatchObject({ _tag: "Assistant", content: { text: "hello" } });
    expect(snapshot.messages).toEqual(messages);
  });

  it("continues a tool-producing inference as a new turn in the same run", async () => {
    const prompts: Array<Prompt.RawInput> = [];
    const slowStarted = await Effect.runPromise(Deferred.make<void>());
    const fastStarted = await Effect.runPromise(Deferred.make<void>());
    const slowRelease = await Effect.runPromise(Deferred.make<void>());
    const fastRelease = await Effect.runPromise(Deferred.make<void>());
    const firstStream = Stream.make(
      Response.makePart("tool-call", {
        id: "tool-slow",
        name: "ordered",
        params: { label: "slow" },
        providerExecuted: false,
      }),
      Response.makePart("tool-call", {
        id: "tool-fast",
        name: "ordered",
        params: { label: "fast" },
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const secondStream = Stream.make(
      Response.makePart("text-delta", { id: "text-2", delta: "done" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const toolkit = Effect.runSync(
      makeEDAToolkit([OrderedTool], {
        ordered: (params) =>
          Effect.gen(function* () {
            const label = (params as { readonly label: "slow" | "fast" }).label;
            if (label === "slow") {
              yield* Deferred.succeed(slowStarted, undefined);
              yield* Deferred.await(slowRelease);
              return { label };
            }
            yield* Deferred.succeed(fastStarted, undefined);
            yield* Deferred.await(fastRelease);
            return { label };
          }),
      }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const liveBus = yield* LiveEventBus;
        const runtime = yield* EDARuntime;
        const liveStream = yield* liveBus.subscribe();
        const firstToolCompleted = yield* liveStream.pipe(
          Stream.filter((event) => event.event.type === "ToolCallCompleted"),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );
        const submitted = yield* runtime.submit(command);
        yield* Deferred.await(slowStarted);
        yield* Deferred.await(fastStarted);
        yield* Deferred.succeed(fastRelease, undefined);
        yield* Fiber.join(firstToolCompleted);
        yield* Deferred.succeed(slowRelease, undefined);
        yield* runtime.blockOnCommand(command.commandId, submitted.position.seq);
        const snapshot = yield* runtime.snapshot();
        const replay = yield* runtime.eventsAfter(SequenceNumber.make(0));
        const events = yield* replay.pipe(Stream.take(snapshot.state.lastSeq), Stream.runCollect);
        return Array.from(events);
      }),
    ).pipe(
      Effect.provide(
        EDARuntime.Live({
          modelSelection: { provider: "test", modelId: "test-model" },
        }).pipe(
          Layer.provideMerge(
            makeEdaTestLayer({
              sessionId: SessionId.make(SESSION_ID),
              ids: undefined,
              parts: [firstStream, secondStream],
              toolkit,
              onStreamText: ({ prompt }) => prompts.push(prompt),
            }),
          ),
        ),
      ),
    );

    const events = await Effect.runPromise(program);
    const eventTypes = events.map((entry) => entry.event.type);
    const firstTurnCompleted = eventTypes.indexOf("TurnCompleted");
    const secondTurnStarted = eventTypes.indexOf("TurnStarted", firstTurnCompleted + 1);
    const secondPrompt = Prompt.make(prompts[1] ?? []).content;
    const toolResultMessages = secondPrompt.filter((message) => message.role === "tool");
    const toolCompletions = events.filter((entry) => entry.event.type === "ToolCallCompleted");

    expect(eventTypes).toEqual([
      "CommandAdmitted",
      "UserMessageSubmitted",
      "CommandStarted",
      "RunStarted",
      "TurnStarted",
      "InferenceStarted",
      "AssistantMessageCommitted",
      "ToolCallCreated",
      "ToolCallCreated",
      "InferenceCompleted",
      "ToolCallStarted",
      "ToolCallStarted",
      "ToolCallCompleted",
      "ToolCallCompleted",
      "TurnCompleted",
      "TurnStarted",
      "InferenceStarted",
      "AssistantMessageCommitted",
      "InferenceCompleted",
      "TurnCompleted",
      "RunCompleted",
      "CommandCompleted",
    ]);
    expect(eventTypes.filter((type) => type === "TurnStarted")).toHaveLength(2);
    expect(eventTypes.filter((type) => type === "InferenceStarted")).toHaveLength(2);
    expect(eventTypes.indexOf("AssistantMessageCommitted")).toBeLessThan(
      eventTypes.indexOf("ToolCallCreated"),
    );
    expect(eventTypes.indexOf("ToolCallCreated")).toBeLessThan(
      eventTypes.indexOf("InferenceCompleted"),
    );
    expect(eventTypes.indexOf("InferenceCompleted")).toBeLessThan(
      eventTypes.indexOf("ToolCallStarted"),
    );
    expect(eventTypes.indexOf("ToolCallCompleted")).toBeLessThan(firstTurnCompleted);
    expect(secondTurnStarted).toBeGreaterThan(firstTurnCompleted);
    expect(eventTypes.lastIndexOf("RunCompleted")).toBeGreaterThan(
      eventTypes.lastIndexOf("TurnCompleted"),
    );
    expect(secondPrompt.map((message) => message.role)).toEqual([
      "user",
      "assistant",
      "tool",
      "tool",
    ]);
    expect(
      toolCompletions.map(
        (entry) => (entry.event.payload.promptPart.result as { readonly label: string }).label,
      ),
    ).toEqual(["fast", "slow"]);
    expect(JSON.stringify(toolResultMessages[0])).toContain("tool-slow");
    expect(JSON.stringify(toolResultMessages[0])).toContain('"label":"slow"');
    expect(JSON.stringify(toolResultMessages[1])).toContain("tool-fast");
    expect(JSON.stringify(toolResultMessages[1])).toContain('"label":"fast"');
  });

  it("fails the exhausting rejected-tool correction turn consistently with the run", async () => {
    const firstRejectedStream = Stream.make(
      Response.makePart("tool-call", {
        id: "bad-tool-1",
        name: "missing",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const secondRejectedStream = Stream.make(
      Response.makePart("tool-call", {
        id: "bad-tool-2",
        name: "missing",
        params: {},
        providerExecuted: false,
      }),
      Response.makePart("finish", { reason: "tool-calls", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        const terminal = yield* runtime.submitAndBlock(command);
        const snapshot = yield* runtime.snapshot();
        const replay = yield* runtime.eventsAfter(SequenceNumber.make(0));
        const events = yield* replay.pipe(Stream.take(snapshot.state.lastSeq), Stream.runCollect);
        return { events: Array.from(events), terminal };
      }),
    ).pipe(
      Effect.provide(
        EDARuntime.Live({
          modelSelection: { provider: "test", modelId: "test-model" },
        }).pipe(
          Layer.provideMerge(
            makeEdaTestLayer({
              sessionId: SessionId.make(SESSION_ID),
              ids: undefined,
              parts: [firstRejectedStream, secondRejectedStream],
              toolSchemas: new Map([["noop", NoopParams]]),
            }),
          ),
        ),
      ),
    );

    const { events, terminal } = await Effect.runPromise(program);
    const eventTypes = events.map((entry) => entry.event.type);
    const rejected = events.filter((entry) => entry.event.type === "ToolCallRejected");
    const exhaustingTurnId = rejected.at(-1)?.event.payload.turnId;
    const exhaustingTurnTerminals = events
      .filter(
        (entry) =>
          (entry.event.type === "TurnCompleted" || entry.event.type === "TurnFailed") &&
          entry.event.payload.turnId === exhaustingTurnId,
      )
      .map((entry) => entry.event.type);
    const terminalErrorCodes = events.flatMap((entry) =>
      entry.event.type === "TurnFailed" ||
      entry.event.type === "RunFailed" ||
      entry.event.type === "CommandFailed"
        ? [entry.event.payload.error.code]
        : [],
    );

    expect(rejected).toHaveLength(2);
    expect(terminal.event.type).toBe("CommandFailed");
    expect(eventTypes).toContain("RunFailed");
    expect(exhaustingTurnTerminals).toEqual(["TurnFailed"]);
    expect(terminalErrorCodes).toEqual([
      "tool.rejection_correction_exhausted",
      "tool.rejection_correction_exhausted",
      "tool.rejection_correction_exhausted",
    ]);
    expect(eventTypes.indexOf("TurnFailed")).toBeLessThan(eventTypes.indexOf("RunFailed"));
    expect(eventTypes.indexOf("RunFailed")).toBeLessThan(eventTypes.indexOf("CommandFailed"));
  });

  it("submitAndBlock admits a command and returns its terminal command event", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        const terminal = yield* runtime.submitAndBlock(command);
        const messages = yield* runtime.messages();
        return { terminal, messages };
      }),
    ).pipe(Effect.provide(makeTestLayer(stream)));

    const { terminal, messages } = await Effect.runPromise(program);

    expect(terminal).toMatchObject({
      event: {
        type: "CommandCompleted",
        eventId: EventId.make(COMMAND_COMPLETED_EVENT_ID),
        payload: { commandId: command.commandId },
      },
    });
    expect(messages.map((message) => message._tag)).toEqual(["User", "Assistant"]);
    expect(messages[1]).toMatchObject({ _tag: "Assistant", content: { text: "hello" } });
  });

  it("commits an app-provided system prompt once and hydrates model context", async () => {
    const systemPrompt = "You are Gia's concise no-tools assistant.";
    const prompts: Array<Prompt.RawInput> = [];
    const firstStream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const secondStream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        yield* runtime.submitAndBlock(command);
        yield* runtime.submitAndBlock(secondCommand);
        const snapshot = yield* runtime.snapshot();
        const replay = yield* runtime.eventsAfter(SequenceNumber.make(0));
        const events = yield* replay.pipe(Stream.take(snapshot.state.lastSeq), Stream.runCollect);
        return Array.from(events);
      }),
    ).pipe(
      Effect.provide(
        EDARuntime.Live({
          modelSelection: { provider: "test", modelId: "test-model" },
          systemPrompt,
        }).pipe(
          Layer.provideMerge(
            makeEdaTestLayer({
              sessionId: SessionId.make(SESSION_ID),
              parts: [firstStream, secondStream],
              toolSchemas: new Map([["noop", NoopParams]]),
              onStreamText: ({ prompt }) => prompts.push(prompt),
            }),
          ),
        ),
      ),
    );

    const events = await Effect.runPromise(program);
    const promptMessages = prompts.map((prompt) => Prompt.make(prompt).content);
    const systemCommitted = events.filter((event) => event.event.type === "SystemMessageCommitted");

    expect(promptMessages).toHaveLength(2);
    expect(promptMessages[0]?.map((message) => message.role)).toEqual(["system", "user"]);
    expect(promptMessages[1]?.map((message) => message.role)).toEqual(["system", "user", "user"]);
    expect(promptMessages[0]?.[0]).toMatchObject({ role: "system", content: systemPrompt });
    expect(promptMessages[1]?.[0]).toMatchObject({ role: "system", content: systemPrompt });
    expect(systemCommitted).toHaveLength(1);
    expect(systemCommitted[0]).toMatchObject({
      event: { type: "SystemMessageCommitted", payload: { content: systemPrompt } },
    });
  });

  it("blockOnCommand observes an already committed terminal through durable replay", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        const terminal = yield* runtime.submitAndBlock(command);
        const replayed = yield* runtime.blockOnCommand(command.commandId, SequenceNumber.make(0));
        return { terminal, replayed };
      }),
    ).pipe(Effect.provide(makeTestLayer(stream)));

    const { terminal, replayed } = await Effect.runPromise(program);

    expect(replayed).toEqual(terminal);
  });

  it("submitAndBlock returns CommandFailed when the inward run fails", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "partial" }),
    ).pipe(Stream.concat(Stream.fail(new Error("provider failed"))));
    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* EDARuntime;
        const terminal = yield* runtime.submitAndBlock(command);
        const messages = yield* runtime.messages();
        return { terminal, messages };
      }),
    ).pipe(Effect.provide(makeTestLayer(stream, "sequential")));

    const { terminal, messages } = await Effect.runPromise(program);

    expect(terminal).toMatchObject({
      event: {
        type: "CommandFailed",
        payload: { commandId: command.commandId, error: { message: "provider failed" } },
      },
    });
    expect(messages.map((message) => message._tag)).toEqual(["User", "AssistantPartial"]);
    expect(messages[1]).toMatchObject({ _tag: "AssistantPartial", content: { text: "partial" } });
  });

  it("blockOnCommand returns cancellation and replacement terminals for an interrupt", async () => {
    const activeStream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "partial" }),
    ).pipe(Stream.concat(Stream.never));
    const replacementStream = Stream.make(
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const liveBus = yield* LiveEventBus;
        const runtime = yield* EDARuntime;
        const liveStream = yield* liveBus.subscribe();
        const textSeen = yield* liveStream.pipe(
          Stream.filter((event) => event.event.type === "TextDelta"),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );

        const admitted = yield* runtime.submit(command);
        yield* Fiber.join(textSeen);
        const interruptAdmitted = yield* runtime.submit(interruptCommand);
        const originalTerminal = yield* runtime.blockOnCommand(
          command.commandId,
          admitted.position.seq,
        );
        const interruptTerminal = yield* runtime.blockOnCommand(
          interruptCommand.commandId,
          interruptAdmitted.position.seq,
        );
        const messages = yield* runtime.messages();
        return { originalTerminal, interruptTerminal, messages };
      }),
    ).pipe(Effect.provide(makeTestLayer([activeStream, replacementStream], "sequential")));

    const { originalTerminal, interruptTerminal, messages } = await Effect.runPromise(program);

    expect(originalTerminal).toMatchObject({
      event: { type: "CommandCancelled", payload: { commandId: command.commandId } },
    });
    expect(interruptTerminal).toMatchObject({
      event: { type: "CommandCompleted", payload: { commandId: interruptCommand.commandId } },
    });
    expect(messages.map((message) => message._tag)).toEqual(["User", "AssistantPartial", "User"]);
    expect(messages[1]).toMatchObject({ _tag: "AssistantPartial", content: { text: "partial" } });
  });

  it("backfills durable events before following live events", async () => {
    const stream = Stream.make(
      Response.makePart("text-delta", { id: "text-1", delta: "hello" }),
      Response.makePart("finish", { reason: "stop", usage: usage(), response: undefined }),
    );
    const program = Effect.scoped(
      Effect.gen(function* () {
        const liveBus = yield* LiveEventBus;
        const runtime = yield* EDARuntime;
        const liveStream = yield* liveBus.subscribe();
        const completed = yield* liveStream.pipe(
          Stream.filter((event) => event.event.type === "CommandCompleted"),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );

        yield* runtime.submit(command);
        yield* Fiber.join(completed);

        const snapshot = yield* runtime.snapshot();
        const resume = yield* runtime.eventsAfter(SequenceNumber.make(0));
        const liveEphemeral = PositionedEvent.make({
          position: Position.make({
            seq: snapshot.state.lastSeq,
            subSeq: SubSequenceNumber.make(1),
          }),
          event: {
            namespace: effectDurableAgentNamespace,
            type: EventType.make("TextDelta"),
            schemaVersion: schemaV1,
            durability: "ephemeral",
            eventId: EventId.make(LIVE_EPHEMERAL_EVENT_ID),
            sessionId: SessionId.make(SESSION_ID),
            createdAtMs: UnixEpochMillis.make(1_715_000_000_000),
            payload: { delta: "live" },
          },
        });
        const replayThenLive = yield* resume.pipe(
          Stream.take(snapshot.state.lastSeq + 2),
          Stream.runCollect,
          Effect.forkScoped,
        );

        yield* liveBus.publish(liveEphemeral);
        yield* runtime.submit(secondCommand);
        return {
          events: Array.from(yield* Fiber.join(replayThenLive)),
          replayHead: snapshot.state.lastSeq,
        };
      }),
    ).pipe(Effect.provide(makeTestLayer(stream, "sequential")));

    const { events, replayHead } = await Effect.runPromise(program);
    const replayed = events.slice(0, replayHead);
    const live = events.slice(replayHead);

    expect(replayed.map((event) => event.event.durability)).toEqual(
      Array.from({ length: replayHead }, () => "durable"),
    );
    expect(replayed.map((event) => event.position.seq)).toEqual(
      Array.from({ length: replayHead }, (_, index) => SequenceNumber.make(index + 1)),
    );
    expect(replayed.at(-1)?.event.type).toBe("CommandCompleted");
    expect(live.map((event) => event.event.type)).toEqual(["TextDelta", "CommandAdmitted"]);
    expect(live[0]).toMatchObject({
      event: { durability: "ephemeral", payload: { delta: "live" } },
    });
    expect(live[1]).toMatchObject({
      event: {
        type: "CommandAdmitted",
        payload: { command: { commandId: CommandId.make(SECOND_COMMAND_ID) } },
      },
    });
  });
});
