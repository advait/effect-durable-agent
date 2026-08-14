import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Tool from "effect/unstable/ai/Tool";
import { describe, expect, it } from "vite-plus/test";

import { CommittedDurableEvent } from "./session-store";
import {
  EDAToolRegistry,
  makeEDAToolkit,
  type EDAModelToolkit,
  type EDAToolExecutionContext,
} from "./tool-registry";
import { EventId, SequenceNumber, SessionId, ToolCallId, durablePosition } from "../types/core";
import { ToolName } from "../types/events";

const SESSION_ID = SessionId.make("018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a");
const TOOL_CALL_ID = ToolCallId.make("018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a");
const EVENT_ID = EventId.make("018f6bd5-2f2a-7b1e-8f1c-1f2e3d4c5b6a");

const toolContext: EDAToolExecutionContext = {
  toolCallId: TOOL_CALL_ID,
  sessionId: SESSION_ID,
  makeEventId: () => Effect.succeed(EVENT_ID),
  emitDurable: (event) =>
    Effect.succeed(
      CommittedDurableEvent.make({
        position: durablePosition(SequenceNumber.make(1)),
        event,
      }),
    ),
};

const LookupParams = Schema.Struct({ query: Schema.String });
const LookupTool = Tool.make("lookup", {
  description: "Look up project facts.",
  parameters: LookupParams,
  success: Schema.Unknown,
});

describe("EDAToolRegistry", () => {
  it("uses an Effect Toolkit as the provider-visible and executable tool source", async () => {
    const toolkit = Effect.runSync(
      makeEDAToolkit([LookupTool], {
        lookup: (params) => Effect.succeed({ echoed: params.query }),
      }),
    );
    const program = Effect.gen(function* () {
      const registry = yield* EDAToolRegistry;
      const modelToolkit = yield* registry.getModelToolkit();
      const paramsSchema = yield* registry.getParamsSchema(ToolName.make("lookup"));
      const result = yield* registry.execute(
        ToolName.make("lookup"),
        { query: "docs" },
        toolContext,
      );
      return { modelToolkit, paramsSchema, result };
    }).pipe(Effect.provide(EDAToolRegistry.FromToolkit(toolkit)));

    const { modelToolkit, paramsSchema, result } = await Effect.runPromise(program);

    expect(Object.keys(modelToolkit?.tools ?? {})).toEqual(["lookup"]);
    expect(modelToolkit?.tools.lookup).toMatchObject({
      name: "lookup",
      description: "Look up project facts.",
    });
    expect(paramsSchema).toBe(LookupParams);
    expect(result).toEqual({ echoed: "docs" });
  });

  it("fails when a handler stream completes without a final result", async () => {
    const toolkit = fakeToolkit(
      Stream.make({
        result: { progress: "still working" },
        encodedResult: { progress: "still working" },
        isFailure: false,
        preliminary: true,
      }),
    );
    const program = Effect.gen(function* () {
      const registry = yield* EDAToolRegistry;
      return yield* registry.execute(ToolName.make("lookup"), { query: "docs" }, toolContext);
    }).pipe(Effect.provide(EDAToolRegistry.FromToolkit(toolkit)));

    const exit = await Effect.runPromise(Effect.exit(program));

    expectFailure(exit, "Tool lookup completed without a final result");
  });

  it("fails with the final failure result from a handler stream", async () => {
    const toolkit = fakeToolkit(
      Stream.make({
        result: { message: "not allowed" },
        encodedResult: { message: "not allowed" },
        isFailure: true,
        preliminary: false,
      }),
    );
    const program = Effect.gen(function* () {
      const registry = yield* EDAToolRegistry;
      return yield* registry.execute(ToolName.make("lookup"), { query: "docs" }, toolContext);
    }).pipe(Effect.provide(EDAToolRegistry.FromToolkit(toolkit)));

    const exit = await Effect.runPromise(Effect.exit(program));

    expectFailure(exit, "not allowed");
  });

  it("exposes schema-only tools to the model but fails execution without handlers", async () => {
    const program = Effect.gen(function* () {
      const registry = yield* EDAToolRegistry;
      const modelToolkit = yield* registry.getModelToolkit();
      const paramsSchema = yield* registry.getParamsSchema(ToolName.make("lookup"));
      const unknownParamsSchema = yield* registry.getParamsSchema(ToolName.make("missing"));
      const executeExit = yield* Effect.exit(
        registry.execute(ToolName.make("lookup"), { query: "docs" }, toolContext),
      );
      return { modelToolkit, paramsSchema, unknownParamsSchema, executeExit };
    }).pipe(Effect.provide(EDAToolRegistry.FromSchemas(new Map([["lookup", LookupParams]]))));

    const { modelToolkit, paramsSchema, unknownParamsSchema, executeExit } =
      await Effect.runPromise(program);

    expect(Object.keys(modelToolkit?.tools ?? {})).toEqual(["lookup"]);
    expect(paramsSchema).toBe(LookupParams);
    expect(unknownParamsSchema).toBeUndefined();
    expectFailure(executeExit, "No test handler registered for schema-only tool lookup");
  });

  it("fails unknown tool execution before reaching any handler", async () => {
    const toolkit = Effect.runSync(
      makeEDAToolkit([LookupTool], {
        lookup: () => Effect.die(new Error("should not execute")),
      }),
    );
    const program = Effect.gen(function* () {
      const registry = yield* EDAToolRegistry;
      return yield* registry.execute(ToolName.make("missing"), {}, toolContext);
    }).pipe(Effect.provide(EDAToolRegistry.FromToolkit(toolkit)));

    const exit = await Effect.runPromise(Effect.exit(program));

    expectFailure(exit, "No handler registered for tool missing");
  });

  it("omits model toolkit when no tools are registered", async () => {
    const program = Effect.gen(function* () {
      const registry = yield* EDAToolRegistry;
      return yield* registry.getModelToolkit();
    }).pipe(Effect.provide(EDAToolRegistry.Empty));

    await expect(Effect.runPromise(program)).resolves.toBeUndefined();
  });
});

type FakeToolResult = {
  readonly result: unknown;
  readonly encodedResult: unknown;
  readonly isFailure: boolean;
  readonly preliminary: boolean;
};

const fakeToolkit = (stream: Stream.Stream<FakeToolResult>): EDAModelToolkit =>
  ({
    tools: { lookup: LookupTool },
    handle: () => Effect.succeed(stream),
  }) as unknown as EDAModelToolkit;

const expectFailure = <A, E>(exit: Exit.Exit<A, E>, message: string) => {
  expect(Exit.isFailure(exit)).toBe(true);
  expect(Exit.isFailure(exit) ? Cause.pretty(exit.cause) : "").toContain(message);
};
